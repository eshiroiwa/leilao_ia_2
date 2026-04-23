"""
Zonas do Viva Real na capital paulista (path ``/venda/sp/sao-paulo/{zona}/{bairro}/``).

Fonte de classificação: listas do projeto (guia por zona — ver ficheiros Markdown na raiz do pacote).
Bairros não mapeados: ``inferir_zona_sao_paulo_por_bairro`` devolve ``None`` — usar URL manual ou
ampliar o mapa.
"""

from __future__ import annotations

from typing import Optional

from leilao_ia_v2.vivareal.slug import slug_vivareal

SAO_PAULO_CIDADE_SLUG = "sao-paulo"

# Slug do bairro (como no path VR) → slug da zona no path (zona-sul, zona-norte, …).
_BAIRRO_SLUG_PARA_ZONA: dict[str, str] = {
    # Centro
    "bela-vista": "centro",
    "bom-retiro": "centro",
    "cambuci": "centro",
    "consolacao": "centro",
    "higienopolis": "centro",
    "liberdade": "centro",
    "republica": "centro",
    "santa-cecilia": "centro",
    "se": "centro",
    # Zona Norte
    "anhanguera": "zona-norte",
    "brasilandia": "zona-norte",
    "cachoeirinha": "zona-norte",
    "casa-verde": "zona-norte",
    "freguesia-do-o": "zona-norte",
    "jacana": "zona-norte",
    "jaragua": "zona-norte",
    "limao": "zona-norte",
    "mandaqui": "zona-norte",
    "perus": "zona-norte",
    "pirituba": "zona-norte",
    "santana": "zona-norte",
    "sao-domingos": "zona-norte",
    "tremembe": "zona-norte",
    "tucuruvi": "zona-norte",
    "vila-guilherme": "zona-norte",
    "vila-maria": "zona-norte",
    "vila-medeiros": "zona-norte",
    # Zona Sul
    "campo-belo": "zona-sul",
    "campo-limpo": "zona-sul",
    "capao-redondo": "zona-sul",
    "cidade-ademar": "zona-sul",
    "cidade-dutra": "zona-sul",
    "cursino": "zona-sul",
    "grajau": "zona-sul",
    "ipiranga": "zona-sul",
    "jabaquara": "zona-sul",
    "jardim-angela": "zona-sul",
    "jardim-sao-luis": "zona-sul",
    "marsilac": "zona-sul",
    "moema": "zona-sul",
    "parelheiros": "zona-sul",
    "pedreira": "zona-sul",
    "sacoma": "zona-sul",
    "socorro": "zona-sul",
    "santo-amaro": "zona-sul",
    "saude": "zona-sul",
    "vila-andrade": "zona-sul",
    "vila-mariana": "zona-sul",
    "vila-olimpia": "zona-sul",
    # Zona Leste
    "agua-rasa": "zona-leste",
    "aricanduva": "zona-leste",
    "artur-alvim": "zona-leste",
    "belem": "zona-leste",
    "bras": "zona-leste",
    "cangaiba": "zona-leste",
    "carrao": "zona-leste",
    "cidade-lider": "zona-leste",
    "cidade-tiradentes": "zona-leste",
    "ermelino-matarazzo": "zona-leste",
    "guaianases": "zona-leste",
    "itaim-paulista": "zona-leste",
    "itaquera": "zona-leste",
    "jardim-helena": "zona-leste",
    "jose-bonifacio": "zona-leste",
    "lajeado": "zona-leste",
    "mooca": "zona-leste",
    "pari": "zona-leste",
    "parque-do-carmo": "zona-leste",
    "penha": "zona-leste",
    "ponte-rasa": "zona-leste",
    "sao-lucas": "zona-leste",
    "sao-mateus": "zona-leste",
    "sao-miguel": "zona-leste",
    "sao-rafael": "zona-leste",
    "sapopemba": "zona-leste",
    "tatuape": "zona-leste",
    "vila-curuca": "zona-leste",
    "vila-formosa": "zona-leste",
    "vila-jacui": "zona-leste",
    "vila-matilde": "zona-leste",
    "vila-prudente": "zona-leste",
    # Zona Oeste
    "alto-de-pinheiros": "zona-oeste",
    "barra-funda": "zona-oeste",
    "butanta": "zona-oeste",
    "jaguara": "zona-oeste",
    "itaim-bibi": "zona-oeste",
    "jardim-america": "zona-oeste",
    "jardim-europa": "zona-oeste",
    "jardim-paulista": "zona-oeste",
    "jardim-paulistano": "zona-oeste",
    "lapa": "zona-oeste",
    "morumbi": "zona-oeste",
    "perdizes": "zona-oeste",
    "pinheiros": "zona-oeste",
    "raposo-tavares": "zona-oeste",
    "rio-pequeno": "zona-oeste",
    "vila-leopoldina": "zona-oeste",
    "vila-madalena": "zona-oeste",
    "vila-sonia": "zona-oeste",
}


def _normalizar_chave_bairro(bairro: str) -> str:
    return slug_vivareal(bairro)


def inferir_zona_sao_paulo_por_bairro(bairro: Optional[str]) -> Optional[str]:
    """Segmento de zona no path (ex.: ``zona-oeste``) ou ``None``."""
    if not bairro or not str(bairro).strip():
        return None
    s = _normalizar_chave_bairro(bairro)
    if not s:
        return None
    return _BAIRRO_SLUG_PARA_ZONA.get(s)


def sao_paulo_capital_cidade_slug() -> str:
    return SAO_PAULO_CIDADE_SLUG
