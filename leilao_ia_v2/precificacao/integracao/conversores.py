"""
Conversores entre rows do Supabase e tipos de domínio do pacote
``precificacao``.

Mantemos esta camada pura (sem rede) para que o motor possa ser testado
com fixtures de dicionários sem depender da forma exata do schema.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from leilao_ia_v2.precificacao.dominio import (
    Amostra,
    ImovelAlvo,
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_DESCONHECIDA,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
)

logger = logging.getLogger(__name__)


# Mapeamento dos marcadores gravados em ``anuncios_mercado.metadados_json.precisao_geo``
# (ver ``leilao_ia_v2/comparaveis/persistencia.py``) para as constantes do
# pacote precificação. Identidade na maioria dos casos, mas isolar via map
# evita acoplamento futuro.
_MAPA_PRECISAO_DB: dict[str, str] = {
    "rooftop": PRECISAO_ROOFTOP,
    "rua": PRECISAO_RUA,
    "bairro_centroide": PRECISAO_BAIRRO,
    "cidade_centroide": PRECISAO_CIDADE,
    "desconhecido": PRECISAO_DESCONHECIDA,
    "desconhecida": PRECISAO_DESCONHECIDA,  # tolerância a typos antigos
}


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _lance_de_referencia(row: dict[str, Any]) -> Optional[float]:
    """Devolve o lance mínimo do leilão.

    Preferência:
    1. ``valor_lance_2_praca`` (mais barato, geralmente o que o usuário avalia).
    2. ``valor_lance_1_praca``.
    3. ``valor_arrematacao`` se já houve venda (apenas como referência).

    Considera apenas valores positivos. Devolve ``None`` se nada elegível.
    """
    for chave in ("valor_lance_2_praca", "valor_lance_1_praca", "valor_arrematacao"):
        v = _to_float(row.get(chave))
        if v is not None and v > 0:
            return v
    return None


def _area_alvo(row: dict[str, Any]) -> Optional[float]:
    """Lê área útil do leilão.

    Preferência: ``area_util`` → ``area_construida`` → ``area_total``.
    Apenas valores positivos.
    """
    for chave in ("area_util", "area_construida", "area_total"):
        v = _to_float(row.get(chave))
        if v is not None and v > 0:
            return v
    return None


def leilao_row_para_alvo(row: dict[str, Any]) -> ImovelAlvo:
    """Constrói :class:`ImovelAlvo` a partir de uma row de ``leilao_imoveis``.

    Campos obrigatórios mínimos: ``cidade`` + ``estado`` + ``tipo_imovel``
    + uma área positiva (``area_util`` ou ``area_construida``).

    ``lance_minimo`` e coordenadas são opcionais — se ausentes, o motor
    devolve veredito ``SEM_LANCE``/``INSUFICIENTE`` conforme o caso, sem
    levantar exceção.
    """
    cidade = str(row.get("cidade") or "").strip()
    estado = str(row.get("estado") or "").strip().upper()[:2]
    bairro = str(row.get("bairro") or "").strip()
    tipo = str(row.get("tipo_imovel") or "").strip().lower() or "desconhecido"

    area = _area_alvo(row) or 0.0
    lance = _lance_de_referencia(row)
    lat = _to_float(row.get("latitude"))
    lon = _to_float(row.get("longitude"))

    return ImovelAlvo(
        cidade=cidade,
        estado_uf=estado,
        bairro=bairro,
        tipo_imovel=tipo,
        area_m2=area,
        latitude=lat,
        longitude=lon,
        lance_minimo=lance,
    )


def anuncio_row_para_amostra(
    row: dict[str, Any],
    *,
    distancia_km: float,
    raio_origem_m: int,
) -> Optional[Amostra]:
    """Converte uma linha de ``anuncios_mercado`` em :class:`Amostra`.

    Devolve ``None`` (com log) quando faltam dados essenciais (área ou
    valor não positivos, sem URL). Não levanta — o motor precisa tolerar
    bases sujas.
    """
    url = str(row.get("url_anuncio") or "").strip()
    area = _to_float(row.get("area_construida_m2"))
    valor = _to_float(row.get("valor_venda"))
    if not url or area is None or valor is None or area <= 0 or valor <= 0:
        logger.debug(
            "anuncio_row_para_amostra: descartado (url=%r area=%s valor=%s)",
            url[:80] if url else "",
            area,
            valor,
        )
        return None

    tipo = str(row.get("tipo_imovel") or "").strip().lower() or "desconhecido"
    meta = row.get("metadados_json") or {}
    if not isinstance(meta, dict):
        meta = {}
    precisao_db = str(meta.get("precisao_geo") or "").strip().lower()
    precisao = _MAPA_PRECISAO_DB.get(precisao_db, PRECISAO_DESCONHECIDA)

    return Amostra(
        url=url,
        valor_anuncio=float(valor),
        area_m2=float(area),
        tipo_imovel=tipo,
        distancia_km=float(distancia_km),
        precisao_geo=precisao,
        raio_origem_m=int(raio_origem_m),
    )
