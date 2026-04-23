"""Slugs, UF, zonas e parser de cards em markdown (usado pelo Firecrawl Search e por testes)."""

from leilao_ia_v2.vivareal.parser_cards_listagem import extrair_cards_anuncios_vivareal_markdown
from leilao_ia_v2.vivareal.slug import slug_vivareal
from leilao_ia_v2.vivareal.tipo_path import SEGMENTOS_TIPO_PATH_VIVAREAL, tipo_imovel_para_segmento_vivareal
from leilao_ia_v2.vivareal.uf_segmento import (
    estado_livre_para_sigla_uf,
    estado_para_uf_segmento_vivareal,
    segmentos_uf_urls_listagem_vivareal,
)
from leilao_ia_v2.vivareal.zonas_rio import inferir_zona_rio_por_bairro, rio_capital_cidade_slug
from leilao_ia_v2.vivareal.zonas_sao_paulo import inferir_zona_sao_paulo_por_bairro, sao_paulo_capital_cidade_slug

__all__ = [
    "SEGMENTOS_TIPO_PATH_VIVAREAL",
    "estado_livre_para_sigla_uf",
    "estado_para_uf_segmento_vivareal",
    "extrair_cards_anuncios_vivareal_markdown",
    "inferir_zona_rio_por_bairro",
    "inferir_zona_sao_paulo_por_bairro",
    "rio_capital_cidade_slug",
    "sao_paulo_capital_cidade_slug",
    "segmentos_uf_urls_listagem_vivareal",
    "slug_vivareal",
    "tipo_imovel_para_segmento_vivareal",
]
