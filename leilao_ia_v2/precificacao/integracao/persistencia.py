"""
Serializa :class:`ResultadoPrecificacao` para JSON estável e grava em
``leilao_imoveis.leilao_extra_json.precificacao_v2``.

Decisões:

- **Sem migração de coluna**: ``leilao_extra_json`` (jsonb) já existe e
  está indexado (GIN) — basta merge da chave ``precificacao_v2``.
- **Snapshot completo, não só veredito**: gravamos faixa, estatísticas,
  amostras (URLs + R$/m² ajustado), confiança, alerta de liquidez e
  expansão. Permite auditoria sem reconsultar amostras.
- **Limite de amostras** (``MAX_AMOSTRAS_PERSISTIDAS``): JSON no DB não
  deve crescer indefinidamente. Persistimos só as ``N`` mais úteis (com
  preço ajustado > 0), ordenadas por menor distância.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from supabase import Client

from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.precificacao.dominio import (
    AmostraHomogeneizada,
    ResultadoPrecificacao,
)

logger = logging.getLogger(__name__)


# Chave dentro de ``leilao_extra_json`` onde o snapshot é gravado.
METADADO_KEY: str = "precificacao_v2"

# Cap de amostras serializadas. JSON no Supabase deve ficar enxuto;
# ainda dá para auditar a média/mediana e ver os 30 mais próximos.
MAX_AMOSTRAS_PERSISTIDAS: int = 30

# Versão do schema de payload — incrementar se a forma do JSON mudar
# (consumidores podem checar antes de parsear).
SCHEMA_VERSAO: int = 1


def _amostra_para_dict(h: AmostraHomogeneizada) -> dict[str, Any]:
    """Serializa uma amostra homogeneizada de forma compacta."""
    return {
        "url": h.origem.url,
        "valor": round(float(h.origem.valor_anuncio), 2),
        "area_m2": round(float(h.origem.area_m2), 2),
        "tipo": h.origem.tipo_imovel,
        "distancia_km": round(float(h.origem.distancia_km), 3),
        "precisao_geo": h.origem.precisao_geo,
        "raio_origem_m": int(h.origem.raio_origem_m),
        "preco_m2_bruto": round(float(h.preco_m2_bruto), 2),
        "preco_m2_ajustado": round(float(h.preco_m2_ajustado), 2),
        "fator_oferta": round(float(h.fator_oferta), 4),
        "fator_area": round(float(h.fator_area), 4),
    }


def _selecionar_amostras_para_persistir(
    homogs: tuple[AmostraHomogeneizada, ...],
    *,
    cap: int = MAX_AMOSTRAS_PERSISTIDAS,
) -> list[AmostraHomogeneizada]:
    """Devolve até ``cap`` amostras válidas, ordenadas por distância (asc).

    Critério: ``preco_m2_ajustado > 0`` (ignora as descartadas) e quando
    há empate de distância, mantém a ordem de chegada (estável).
    """
    validas = [h for h in homogs if h.preco_m2_ajustado > 0]
    validas.sort(key=lambda h: float(h.origem.distancia_km))
    return validas[: max(0, int(cap))]


def resultado_para_payload(resultado: ResultadoPrecificacao) -> dict[str, Any]:
    """Converte um :class:`ResultadoPrecificacao` em dict JSON-friendly."""
    alvo = resultado.alvo
    estat = resultado.estatistica

    payload: dict[str, Any] = {
        "schema_versao": SCHEMA_VERSAO,
        "calculado_em": datetime.now(timezone.utc).isoformat(),
        "alvo": {
            "cidade": alvo.cidade,
            "estado_uf": alvo.estado_uf,
            "bairro": alvo.bairro,
            "tipo_imovel": alvo.tipo_imovel,
            "area_m2": float(alvo.area_m2),
            "lance_minimo": (float(alvo.lance_minimo) if alvo.lance_minimo else None),
            "latitude": alvo.latitude,
            "longitude": alvo.longitude,
        },
        "valor_estimado": resultado.valor_estimado,
        "p20_total": resultado.p20_total,
        "p80_total": resultado.p80_total,
        "estatistica": (
            {
                "n_total": estat.n_total,
                "n_uteis": estat.n_uteis,
                "n_descartados_outlier": estat.n_descartados_outlier,
                "mediana_r_m2": estat.mediana_r_m2,
                "p20_r_m2": estat.p20_r_m2,
                "p80_r_m2": estat.p80_r_m2,
                "iqr_r_m2": estat.iqr_r_m2,
                "cv_pct": estat.cv_pct,
            }
            if estat is not None
            else None
        ),
        "confianca": {
            "nivel": resultado.confianca.nivel,
            "motivo": resultado.confianca.motivo,
            "score": resultado.confianca.score,
        },
        "veredito": {
            "nivel": resultado.veredito.nivel,
            "descricao": resultado.veredito.descricao,
            "rebaixado": resultado.veredito.rebaixado,
            "desconto_vs_p20_pct": resultado.veredito.desconto_vs_p20_pct,
        },
        "alerta_liquidez": {
            "razao_area": resultado.alerta_liquidez.razao_area,
            "severidade": resultado.alerta_liquidez.severidade,
            "mensagem": resultado.alerta_liquidez.mensagem,
            "fator_aplicado": resultado.alerta_liquidez.fator_aplicado,
            "rebaixa_niveis": resultado.alerta_liquidez.rebaixa_niveis,
        },
        "expansao": {
            "raio_final_m": resultado.expansao.raio_final_m,
            "area_relax_aplicada": resultado.expansao.area_relax_aplicada,
            "tipo_relax_aplicado": resultado.expansao.tipo_relax_aplicado,
            "niveis_expansao_aplicados": resultado.expansao.niveis_expansao_aplicados,
            "n_amostras_capturadas": len(resultado.expansao.amostras),
        },
        "amostras": [
            _amostra_para_dict(h)
            for h in _selecionar_amostras_para_persistir(resultado.amostras_homogeneizadas)
        ],
    }
    return payload


def gravar_resultado(
    client: Client,
    leilao_imovel_id: str,
    resultado: ResultadoPrecificacao,
    *,
    leilao_extra_json_atual: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Persiste o snapshot em ``leilao_extra_json[METADADO_KEY]``.

    Mescla com o JSON atual da linha (preserva outras chaves). Quando o
    chamador já tem o ``leilao_extra_json`` carregado, pode passá-lo via
    ``leilao_extra_json_atual`` para economizar uma leitura no banco.

    Devolve o ``leilao_extra_json`` completo gravado.
    """
    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        raise ValueError("leilao_imovel_id vazio")

    if leilao_extra_json_atual is None:
        row = leilao_imoveis_repo.buscar_por_id(lid, client)
        atual = (row or {}).get("leilao_extra_json") or {}
    else:
        atual = leilao_extra_json_atual or {}
    if not isinstance(atual, dict):
        atual = {}

    payload = resultado_para_payload(resultado)
    novo = dict(atual)
    novo[METADADO_KEY] = payload

    leilao_imoveis_repo.atualizar_leilao_imovel(lid, {"leilao_extra_json": novo}, client)
    logger.info(
        "precificacao_v2 gravada (leilao=%s veredito=%s confianca=%s n_uteis=%s)",
        lid[:12],
        payload["veredito"]["nivel"],
        payload["confianca"]["nivel"],
        (payload.get("estatistica") or {}).get("n_uteis"),
    )
    return novo
