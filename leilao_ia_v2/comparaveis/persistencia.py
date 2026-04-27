"""
Persistência de cards validados em ``anuncios_mercado`` — **sem fallback de cidade**.

Diferenças em relação ao caminho antigo
(``services/anuncios_mercado_coleta.py``):

- Recebe a tupla ``(card, validacao)`` onde a ``ResultadoValidacaoMunicipio``
  *já confirmou* que o anúncio pertence ao município alvo. A cidade gravada é
  **a do reverse-geocode** (município real), nunca a cidade do leilão de origem.
- Lat/Lon vêm da própria validação (já temos as coordenadas do geocode forward).
- Tipo de imóvel é passado pelo caller (corresponde ao tipo do leilão alvo:
  comparáveis fazem sentido entre tipos iguais; nunca inferimos tipo do
  título do anúncio — fonte ruidosa que mistura "Casa térrea de 2 pisos" com
  "Apartamento garden", etc.).
- Bairro vem do extrator (`bairro_inferido` do próprio anúncio); se vazio,
  fica vazio — preferimos um campo vazio a um bairro inventado.

Esta função NÃO consulta Firecrawl, NÃO consulta geocoder. É puro I/O com Supabase.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from leilao_ia_v2.comparaveis.extrator import CardExtraido
from leilao_ia_v2.comparaveis.validacao_cidade import ResultadoValidacaoMunicipio
from leilao_ia_v2.persistence import anuncios_mercado_repo

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LinhaPersistir:
    """Estrutura intermediária — útil em testes para inspeccionar antes do upsert."""

    url_anuncio: str
    portal: str
    tipo_imovel: str
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

    coords = validacao.coordenadas
    lat = coords[0] if coords else None
    lon = coords[1] if coords else None

    metadados: dict[str, Any] = {
        "fonte": "comparaveis_v2",
        "validacao_municipio": {
            "alvo_slug": validacao.municipio_alvo_slug,
            "real_slug": validacao.municipio_real_slug,
            "real_nome": cidade_real,
        },
        "logradouro_inferido": card.logradouro_inferido,
        "titulo_anuncio": card.titulo,
    }
    if fonte_busca:
        metadados["fonte_busca"] = fonte_busca

    return LinhaPersistir(
        url_anuncio=card.url_anuncio,
        portal=card.portal,
        tipo_imovel=(tipo_imovel or "desconhecido").strip().lower(),
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
