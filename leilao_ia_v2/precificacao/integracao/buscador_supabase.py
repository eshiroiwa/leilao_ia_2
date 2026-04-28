"""
Adapter Supabase: ``ImovelAlvo`` + (raio, área_relax, tipo_próximo)
→ lista de :class:`Amostra` filtradas.

A consulta é feita em **dois passos** porque o PostgREST do Supabase não
expõe Haversine nativo:

1. SQL "barato" — filtra pelas colunas indexadas:
   ``estado``, ``cidade`` (ilike), ``tipo_imovel`` (ou IN para próximos),
   ``transacao='venda'``, área dentro de ``[A·(1-r), A·(1+r)]``.

2. Filtro Haversine em memória — descarta os fora do raio.

Como o filtro 1 já é seletivo (cidade + tipo + área), a quantidade de
linhas que chegam ao passo 2 é tipicamente pequena (dezenas a centenas).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from supabase import Client

from leilao_ia_v2.constants import TABELA_ANUNCIOS_MERCADO
from leilao_ia_v2.precificacao.dominio import Amostra, ImovelAlvo
from leilao_ia_v2.precificacao.integracao.conversores import (
    anuncio_row_para_amostra,
)
from leilao_ia_v2.services.geo_medicao import haversine_km

logger = logging.getLogger(__name__)


# Mapeamento de tipos "aproximados" — usado quando o motor pede
# ``permitir_tipo_proximo=True`` no último degrau de expansão.
# Mantém-se alinhado com ``_TIPOS_RESIDENCIAIS_POOL`` em
# ``services/cache_media_leilao.py`` para coerência de comportamento.
_TIPOS_PROXIMOS: dict[str, tuple[str, ...]] = {
    "casa": ("casa", "sobrado", "casa_condominio"),
    "sobrado": ("casa", "sobrado", "casa_condominio"),
    "casa_condominio": ("casa", "sobrado", "casa_condominio"),
    # Apartamento/cobertura/kitnet ficam no próprio (não há equivalência
    # útil — não misturamos com casa).
}


# Limite duro de linhas trazidas do banco em uma única query — proteção
# contra cidades enormes. 1500 já cobre folgadamente bairros densos.
_LIMITE_QUERY: int = 1500


@dataclass(frozen=True)
class BuscaSupabaseConfig:
    """Parâmetros de configuração do adapter (independentes do motor).

    - ``incluir_sem_geo``: se ``True``, devolve também amostras sem
      ``latitude``/``longitude`` (com ``distancia_km=999``); útil em
      cidades pequenas. Default ``False`` (rigoroso).
    - ``limite_por_query``: cap de linhas SQL — proteção operacional.
    """

    incluir_sem_geo: bool = False
    limite_por_query: int = _LIMITE_QUERY


def _tipos_para_query(tipo: str, *, permitir_proximo: bool) -> list[str]:
    """Devolve os tipos a usar no filtro IN do SQL.

    Sempre inclui o próprio tipo. Quando ``permitir_proximo=True`` e há
    mapeamento conhecido em :data:`_TIPOS_PROXIMOS`, expande.
    """
    base = (tipo or "").strip().lower() or "desconhecido"
    if not permitir_proximo:
        return [base]
    return list(_TIPOS_PROXIMOS.get(base, (base,)))


def _faixa_area(area_alvo: float, area_relax_pct: float) -> tuple[float, float]:
    """Calcula a faixa ``[A·(1-r), A·(1+r)]``. Não-negativa."""
    a = max(0.0, float(area_alvo))
    r = max(0.0, float(area_relax_pct))
    return max(0.0, a * (1.0 - r)), a * (1.0 + r)


def _consultar_anuncios(
    client: Client,
    *,
    cidade: str,
    estado_uf: str,
    tipos: list[str],
    area_min: float,
    area_max: float,
    limite: int,
) -> list[dict[str, Any]]:
    """Query barata: cidade + UF + tipo IN + faixa de área.

    Faz log resumido em DEBUG. Falhas devolvem lista vazia (logs warning),
    nunca propagam.
    """
    cid = (cidade or "").strip()
    uf = (estado_uf or "").strip().upper()[:2]
    if not cid or len(uf) != 2 or not tipos:
        return []
    try:
        q = (
            client.table(TABELA_ANUNCIOS_MERCADO)
            .select(
                "url_anuncio, tipo_imovel, cidade, estado, bairro, "
                "area_construida_m2, valor_venda, latitude, longitude, "
                "metadados_json, logradouro"
            )
            .eq("transacao", "venda")
            .eq("estado", uf)
            .ilike("cidade", f"%{cid}%")
            .gte("area_construida_m2", float(area_min))
            .lte("area_construida_m2", float(area_max))
        )
        if len(tipos) == 1:
            q = q.eq("tipo_imovel", tipos[0])
        else:
            q = q.in_("tipo_imovel", tipos)
        resp = q.limit(int(limite)).execute()
        return list(getattr(resp, "data", None) or [])
    except Exception:
        logger.exception(
            "buscador_supabase: consulta falhou (cidade=%r uf=%r tipos=%r)",
            cidade, estado_uf, tipos,
        )
        return []


def _filtrar_por_raio(
    rows: list[dict[str, Any]],
    *,
    lat_alvo: Optional[float],
    lon_alvo: Optional[float],
    raio_m: int,
    incluir_sem_geo: bool,
) -> list[tuple[dict[str, Any], float]]:
    """Aplica Haversine row-a-row.

    Devolve lista de ``(row, distancia_km)``. Quando o alvo não tem
    coordenadas, devolve **tudo** (a precificação ainda é útil — só
    perde o filtro espacial).
    """
    if lat_alvo is None or lon_alvo is None:
        return [(r, float("nan")) for r in rows]
    raio_km = max(0.0, raio_m / 1000.0)
    out: list[tuple[dict[str, Any], float]] = []
    for r in rows:
        try:
            la = float(r.get("latitude")) if r.get("latitude") is not None else None
            lo = float(r.get("longitude")) if r.get("longitude") is not None else None
        except (TypeError, ValueError):
            la, lo = None, None
        if la is None or lo is None:
            if incluir_sem_geo:
                out.append((r, 999.0))
            continue
        d = haversine_km(lat_alvo, lon_alvo, la, lo)
        if d <= raio_km:
            out.append((r, d))
    return out


def construir_buscador(
    *,
    client: Client,
    alvo: ImovelAlvo,
    config: Optional[BuscaSupabaseConfig] = None,
):
    """Devolve o callable que o motor consome.

    A assinatura do callable corresponde ao protocolo
    :data:`leilao_ia_v2.precificacao.expansao.BuscadorAmostras`:

        ``fn(*, raio_m, area_relax_pct, permitir_tipo_proximo) -> list[Amostra]``

    Cada chamada é isolada (sem estado entre invocações) — apropriado
    para a expansão progressiva, que pode chamar até 5 vezes com
    parâmetros diferentes.
    """
    cfg = config or BuscaSupabaseConfig()

    def _buscar(
        *,
        raio_m: int,
        area_relax_pct: float,
        permitir_tipo_proximo: bool,
    ) -> list[Amostra]:
        if alvo.area_m2 <= 0:
            return []
        a_min, a_max = _faixa_area(alvo.area_m2, area_relax_pct)
        tipos = _tipos_para_query(alvo.tipo_imovel, permitir_proximo=permitir_tipo_proximo)
        rows = _consultar_anuncios(
            client,
            cidade=alvo.cidade,
            estado_uf=alvo.estado_uf,
            tipos=tipos,
            area_min=a_min,
            area_max=a_max,
            limite=cfg.limite_por_query,
        )
        if not rows:
            return []
        rows_dist = _filtrar_por_raio(
            rows,
            lat_alvo=alvo.latitude,
            lon_alvo=alvo.longitude,
            raio_m=raio_m,
            incluir_sem_geo=cfg.incluir_sem_geo,
        )
        amostras: list[Amostra] = []
        for row, dist_km in rows_dist:
            am = anuncio_row_para_amostra(
                row,
                distancia_km=dist_km if dist_km == dist_km else 0.0,  # NaN→0
                raio_origem_m=int(raio_m),
            )
            if am is not None:
                amostras.append(am)
        logger.info(
            "buscador_supabase: alvo=%s/%s tipo=%s raio=%dm area_relax=%.0f%% "
            "tipo_proximo=%s rows_sql=%d amostras=%d",
            alvo.cidade, alvo.estado_uf, alvo.tipo_imovel,
            raio_m, area_relax_pct * 100, permitir_tipo_proximo,
            len(rows), len(amostras),
        )
        return amostras

    return _buscar
