"""
Persistência de cards validados em ``anuncios_mercado`` — **sem fallback de cidade**.

Princípios:

- Recebe a tupla ``(card, validacao)`` onde a :class:`ResultadoValidacaoMunicipio`
  *já confirmou* que o anúncio pertence ao município alvo. A cidade gravada é
  **a do reverse-geocode** (município real), nunca a cidade do leilão de origem.
- Lat/Lon vêm da própria validação (já temos as coordenadas do geocode forward).
- Tipo de imóvel é passado pelo caller (corresponde ao tipo do leilão alvo:
  comparáveis fazem sentido entre tipos iguais; nunca inferimos tipo do
  título do anúncio — fonte ruidosa que mistura "Casa térrea de 2 pisos" com
  "Apartamento garden", etc.).
- Bairro vem do extrator (``bairro_inferido`` do próprio anúncio); se vazio,
  fica vazio — preferimos um campo vazio a um bairro inventado.

**Política de precisão geográfica** (decisão A do plano A+B):

- ``rooftop`` / ``rua``: lat/lon vão directos (geocode preciso).
- ``bairro``: lat/lon vão com **jitter determinístico ±80 m** baseado no
  hash da URL — evita pile-up de N cards no centroide e mantém o haversine
  útil. Marca ``metadados.precisao_geo='bairro_centroide'``.
- ``cidade`` (centroide do município): grava lat/lon **sem jitter**
  (queremos manter agrupado para o cache poder identificar e descartar) e
  marca ``metadados.precisao_geo='cidade_centroide'``. Em cidades muito
  pequenas pode ser a única opção.
- ``desconhecido`` ou sem coords: NÃO grava lat/lon (fica NULL); marca
  ``metadados.precisao_geo='desconhecido'``.

Esta função NÃO consulta Firecrawl, NÃO consulta geocoder. É puro I/O com Supabase.
"""

from __future__ import annotations

import hashlib
import logging
import math
import struct
from dataclasses import dataclass
from typing import Any, Optional

from leilao_ia_v2.comparaveis.extrator import CardExtraido
from leilao_ia_v2.comparaveis.validacao_cidade import (
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_DESCONHECIDA,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    ResultadoValidacaoMunicipio,
)
from leilao_ia_v2.persistence import anuncios_mercado_repo

logger = logging.getLogger(__name__)


# Jitter aplicado quando a coord é centroide de bairro: ~80 m em latitude
# (1 grau lat ≈ 111 km), aplicado deterministicamente a partir do hash da URL
# para que cards iguais tenham sempre a mesma coord (idempotência do upsert).
JITTER_BAIRRO_METROS = 80.0
_GRAUS_POR_METRO_LAT = 1.0 / 111_000.0

# Mapa precisao_geo (do validacao_cidade) → marcador gravado em metadados.
# O cache_media_leilao usa este marcador para penalizar/descartar amostras.
_MARCADOR_POR_PRECISAO: dict[str, str] = {
    PRECISAO_ROOFTOP: PRECISAO_ROOFTOP,
    PRECISAO_RUA: PRECISAO_RUA,
    PRECISAO_BAIRRO: "bairro_centroide",
    PRECISAO_CIDADE: "cidade_centroide",
    PRECISAO_DESCONHECIDA: PRECISAO_DESCONHECIDA,
}


@dataclass(frozen=True)
class LinhaPersistir:
    """Estrutura intermediária — útil em testes para inspeccionar antes do upsert.

    ``logradouro`` é gravado como **coluna do banco** (existe em
    ``leilao_ia_v2/sql/006_anuncios_mercado.sql`` como ``text default ''``)
    para permitir queries SQL diretas sem ter que extrair de
    ``metadados_json``. Vazio quando não foi possível extrair (não
    inventamos rua).
    """

    url_anuncio: str
    portal: str
    tipo_imovel: str
    logradouro: str
    bairro: str
    cidade: str
    estado: str
    valor_venda: float
    area_construida_m2: float
    transacao: str
    latitude: Optional[float]
    longitude: Optional[float]
    metadados_json: dict[str, Any]

    def para_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "url_anuncio": self.url_anuncio,
            "portal": self.portal,
            "tipo_imovel": self.tipo_imovel,
            "logradouro": self.logradouro,
            "bairro": self.bairro,
            "cidade": self.cidade,
            "estado": self.estado,
            "valor_venda": self.valor_venda,
            "area_construida_m2": self.area_construida_m2,
            "transacao": self.transacao,
            "metadados_json": dict(self.metadados_json),
        }
        if self.latitude is not None and self.longitude is not None:
            d["latitude"] = float(self.latitude)
            d["longitude"] = float(self.longitude)
        return d


class PersistenciaInvalida(ValueError):
    """Levantada quando a validação não comprova a cidade alvo (defesa em profundidade)."""


def _jitter_deterministico(url: str, latitude: float) -> tuple[float, float]:
    """Gera (delta_lat, delta_lon) em graus, ±~80 m, determinístico por URL.

    A ideia é distribuir cards diferentes que caíram no mesmo centroide do
    bairro em pontos próximos mas distintos, evitando pile-up sem mover o
    anúncio para fora do bairro.

    O hash garante reprodutibilidade — o mesmo URL produz sempre o mesmo
    deslocamento, o que é crítico para que o ``upsert(on_conflict=url_anuncio)``
    seja idempotente (re-ingestões não mexem nas coordenadas).

    Args:
        url: URL do anúncio (chave do hash).
        latitude: latitude do centroide (usada para corrigir o factor cosseno
            quando convertemos metros para graus de longitude).
    """
    if not url:
        return (0.0, 0.0)
    digest = hashlib.sha256(url.encode("utf-8")).digest()
    # Dois floats em [-1, 1] a partir de 16 bytes do digest.
    raw_x, raw_y = struct.unpack(">QQ", digest[:16])
    nx = (raw_x / float(2**64 - 1)) * 2.0 - 1.0
    ny = (raw_y / float(2**64 - 1)) * 2.0 - 1.0

    delta_lat_graus = nx * JITTER_BAIRRO_METROS * _GRAUS_POR_METRO_LAT
    cos_lat = math.cos(math.radians(latitude)) if -90 <= latitude <= 90 else 1.0
    if abs(cos_lat) < 1e-6:
        cos_lat = 1e-6
    delta_lon_graus = ny * JITTER_BAIRRO_METROS * _GRAUS_POR_METRO_LAT / cos_lat
    return (delta_lat_graus, delta_lon_graus)


def _classificar_logradouro_origem(card: CardExtraido) -> str:
    """Devolve a "origem" textual do ``logradouro_inferido`` final do card.

    Valores possíveis (gravados em ``metadados_json.logradouro_origem``):

    - ``"pagina_individual"`` — refino top-N com sucesso e a página individual
      forneceu rua nova (``refino_status == "ok_pagina"``).
    - ``"titulo"`` — o card tem ``logradouro_inferido`` mas veio do título do
      próprio card de listagem (sem refino, ou refino com extracção vazia).
    - ``"none"`` — não há logradouro inferido em lado nenhum.

    Esta separação permite, em SQL, identificar quanta da precisão "rua"/
    "rooftop" gravada vem efectivamente do scrape individual versus do
    geocode primário.
    """
    status = (card.refino_status or "").strip()
    if status == "ok_pagina":
        return "pagina_individual"
    if (card.logradouro_inferido or "").strip():
        return "titulo"
    return "none"


def _aplicar_politica_precisao(
    *,
    coords: Optional[tuple[float, float]],
    precisao: str,
    url_anuncio: str,
) -> tuple[Optional[float], Optional[float], str]:
    """Decide (lat, lon, marcador) finais a partir do par (coords, precisao).

    Aplica a política A+C descrita no docstring do módulo:

    - rooftop/rua  → coord directa.
    - bairro       → coord + jitter ±80 m, marcador ``bairro_centroide``.
    - cidade       → coord directa, marcador ``cidade_centroide``.
    - desconhecido → sem coord, marcador ``desconhecido``.
    """
    marcador = _MARCADOR_POR_PRECISAO.get(precisao or "", PRECISAO_DESCONHECIDA)

    if coords is None:
        return (None, None, marcador)

    lat, lon = float(coords[0]), float(coords[1])

    if precisao == PRECISAO_BAIRRO:
        dlat, dlon = _jitter_deterministico(url_anuncio, lat)
        return (lat + dlat, lon + dlon, marcador)

    if precisao == PRECISAO_DESCONHECIDA:
        # Decisão: não confiamos em coords sem precisão classificável.
        return (None, None, marcador)

    return (lat, lon, marcador)


def montar_linha(
    card: CardExtraido,
    validacao: ResultadoValidacaoMunicipio,
    *,
    tipo_imovel: str,
    estado_uf: str,
    fonte_busca: str = "",
) -> LinhaPersistir:
    """Constrói a linha pronta para upsert a partir do card + validação.

    Args:
        card: card extraído do markdown (já passou pelos filtros do extrator).
        validacao: resultado da validação por geocode — DEVE ter ``valido=True``.
            A função levanta :class:`PersistenciaInvalida` se vier inválida (defesa
            em profundidade contra calls de pipeline com bug).
        tipo_imovel: tipo canónico do leilão (apartamento/casa/terreno/...).
            Comparáveis devem ser do mesmo tipo do leilão alvo.
        estado_uf: UF de 2 letras do leilão (consistente, vem do edital).
        fonte_busca: opcional — string identificando a query/origem
            (vai para `metadados_json.fonte_busca`).

    Returns:
        :class:`LinhaPersistir` imutável.

    Raises:
        PersistenciaInvalida: se ``validacao.valido`` for False ou faltar cidade.
    """
    if not validacao.valido:
        raise PersistenciaInvalida(
            f"validação reprovada (motivo={validacao.motivo!r}) — não persistir."
        )
    cidade_real = (validacao.municipio_real or "").strip()
    if not cidade_real:
        raise PersistenciaInvalida("validação OK mas sem município_real — bug.")
    uf = (estado_uf or "").strip().upper()[:2]
    if not uf or len(uf) != 2:
        raise PersistenciaInvalida(f"estado_uf inválido: {estado_uf!r}")

    lat, lon, marcador_precisao = _aplicar_politica_precisao(
        coords=validacao.coordenadas,
        precisao=validacao.precisao_geo,
        url_anuncio=card.url_anuncio,
    )

    metadados: dict[str, Any] = {
        "fonte": "comparaveis_v2",
        "validacao_municipio": {
            "alvo_slug": validacao.municipio_alvo_slug,
            "real_slug": validacao.municipio_real_slug,
            "real_nome": cidade_real,
        },
        "logradouro_inferido": card.logradouro_inferido,
        "titulo_anuncio": card.titulo,
        "precisao_geo": marcador_precisao,
        # Auditoria do refino top-N (gravado mesmo quando False/"" para que
        # consultas SQL possam contar/filtrar sem precisar de COALESCE):
        "refinado_top_n": bool(card.refinado_top_n),
        "refino_status": card.refino_status or "",
        "logradouro_origem": _classificar_logradouro_origem(card),
    }
    if fonte_busca:
        metadados["fonte_busca"] = fonte_busca

    return LinhaPersistir(
        url_anuncio=card.url_anuncio,
        portal=card.portal,
        tipo_imovel=(tipo_imovel or "desconhecido").strip().lower(),
        logradouro=(card.logradouro_inferido or "").strip(),
        bairro=(card.bairro_inferido or "").strip(),
        cidade=cidade_real,
        estado=uf,
        valor_venda=float(card.valor_venda),
        area_construida_m2=float(card.area_m2),
        transacao="venda",
        latitude=lat,
        longitude=lon,
        metadados_json=metadados,
    )


def persistir_lote(
    client: Any,
    linhas: list[LinhaPersistir],
) -> int:
    """Persiste uma lista de linhas validadas usando o repositório existente.

    Args:
        client: instância do Supabase ``Client``.
        linhas: lista produzida por :func:`montar_linha`. Pode ser vazia.

    Returns:
        Número de linhas efectivamente persistidas (relatório do
        :func:`anuncios_mercado_repo.upsert_lote`).
    """
    if not linhas:
        return 0
    payload = [l.para_dict() for l in linhas]
    persistidas = anuncios_mercado_repo.upsert_lote(client, payload)
    logger.info(
        "Comparaveis v2: persistidas %s/%s linhas em anuncios_mercado",
        persistidas,
        len(linhas),
    )
    return persistidas
