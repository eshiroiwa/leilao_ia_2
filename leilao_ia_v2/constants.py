"""Constantes alinhadas ao vocabulário do sistema legado (sem importar o legado)."""

from __future__ import annotations

STATUS_PENDENTE = "pendente"

TABELA_LEILAO_IMOVEIS = "leilao_imoveis"
TABELA_ANUNCIOS_MERCADO = "anuncios_mercado"
TABELA_CACHE_MEDIA_BAIRRO = "cache_media_bairro"

# Vocabulário canónico de tipo_imovel (slug ASCII; ver normalizacao.normalizar_tipo_imovel)
TIPOS_IMOVEL_VALIDOS = frozenset({
    # residenciais
    "apartamento",
    "casa",
    "kitnet",
    "casa_condominio",
    "chacara",
    "cobertura",
    "duplex",
    "flat",
    "lote",
    "terreno",
    "sobrado",
    "predio",
    "edificio",
    "fazenda",
    "sitio",
    # comerciais
    "consultorio",
    "galpao",
    "deposito",
    "armazem",
    "imovel_comercial",
    "ponto_comercial",
    "loja",
    "box",
    "sala",
    "conjunto",
    "desconhecido",
})

CONSERVACAO_VALIDAS = frozenset({"novo", "usado", "desconhecido"})

TIPO_CASA_VALIDOS = frozenset({"terrea", "sobrado", "desconhecido", "-"})
