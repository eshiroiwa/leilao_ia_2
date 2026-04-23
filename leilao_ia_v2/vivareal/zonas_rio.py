"""
Inferência de zona na URL do Viva Real para a capital (rio-de-janeiro).

Fonte: regras do projeto + lista de bairros principais (Zona Sul, Norte, Oeste, Centro).
Novos bairros: acrescentar ao mapa em ``_BAIRRO_SLUG_PARA_ZONA``.
"""

from __future__ import annotations

from typing import Optional

from leilao_ia_v2.vivareal.slug import slug_vivareal

RIO_CAPITAL_CIDADE_SLUG = "rio-de-janeiro"

# Bairro (slug) → segmento de zona no path (ex.: zona-sul).
_BAIRRO_SLUG_PARA_ZONA: dict[str, str] = {
    # Zona Sul
    "ipanema": "zona-sul",
    "copacabana": "zona-sul",
    "leblon": "zona-sul",
    "botafogo": "zona-sul",
    "flamengo": "zona-sul",
    "lagoa": "zona-sul",
    "laranjeiras": "zona-sul",
    "jardim-botanico": "zona-sul",
    "gavea": "zona-sul",
    "sao-conrado": "zona-sul",
    # Zona Norte
    "tijuca": "zona-norte",
    "vila-isabel": "zona-norte",
    "grajau": "zona-norte",
    "meier": "zona-norte",
    "cachambi": "zona-norte",
    "madureira": "zona-norte",
    "vila-da-penha": "zona-norte",
    "ilha-do-governador": "zona-norte",
    # Zona Oeste
    "barra-da-tijuca": "zona-oeste",
    "recreio-dos-bandeirantes": "zona-oeste",
    "jacarepagua": "zona-oeste",
    "freguesia": "zona-oeste",
    "freguesia-jacarepagua": "zona-oeste",
    "taquara": "zona-oeste",
    "pechincha": "zona-oeste",
    "campo-grande": "zona-oeste",
    "santa-cruz": "zona-oeste",
    # Centro e adjacências
    "centro": "centro",
    "santa-teresa": "centro",
    "lapa": "centro",
    "gamboa": "centro",
}


def _normalizar_chave_bairro(bairro: str) -> str:
    return slug_vivareal(bairro)


def inferir_zona_rio_por_bairro(bairro: Optional[str]) -> Optional[str]:
    """
    Retorna o segmento de zona (ex.: 'zona-sul') ou None se não houver mapeamento.
    """
    if not bairro or not str(bairro).strip():
        return None
    s = _normalizar_chave_bairro(bairro)
    if not s:
        return None
    return _BAIRRO_SLUG_PARA_ZONA.get(s)


def rio_capital_cidade_slug() -> str:
    return RIO_CAPITAL_CIDADE_SLUG
