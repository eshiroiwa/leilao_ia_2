"""Parser genérico Firecrawl: URLs em texto plano, JSON e exclusão de hubs de listagem."""

from __future__ import annotations

from leilao_ia_v2.fc_search.parser import extrair_anuncios_markdown_generico


def test_bare_url_zap_com_preco_e_area():
    md = """
    Condomínio XYZ
    https://www.zapimoveis.com.br/imovel/venda-apartamento-centro-campinas-99001234
    Área útil 95 m² — R$ 1.250.000
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="Campinas",
        estado_ref="SP",
        bairro_ref="Centro",
    )
    assert len(cards) == 1
    assert cards[0]["url_anuncio"].startswith("https://www.zapimoveis.com.br/imovel/")
    assert cards[0]["area_m2"] == 95.0
    assert cards[0]["valor_venda"] == 1_250_000.0


def test_url_hub_zap_listagem_ignorada_mesmo_com_preco_na_pagina():
    md = """
    Resultados na rua
    https://www.zapimoveis.com.br/venda/imoveis/sp+campinas/rua-teste/
    R$ 500.000  120 m²
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="Campinas",
        estado_ref="SP",
        bairro_ref="Centro",
    )
    assert cards == []


def test_quintoandar_hub_comprar_imovel_brasil_casa_ignorado():
    md = """
    https://www.quintoandar.com.br/comprar/imovel/rua-teste-campinas-sp-brasil/casa
    R$ 800.000  200 m²
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="Campinas",
        estado_ref="SP",
        bairro_ref="Centro",
    )
    assert cards == []


def test_quintoandar_ficha_com_id_numerico():
    md = """
    **Apartamento à venda**
    https://www.quintoandar.com.br/imovel/8234567890123456/
    72 m²  R$ 450.000
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="São Paulo",
        estado_ref="SP",
        bairro_ref="Centro",
    )
    assert len(cards) == 1
    assert "/imovel/" in cards[0]["url_anuncio"]
    assert cards[0]["valor_venda"] == 450_000.0


def test_json_url_zap():
    md = """
    {"@type":"Product","url":"https://www.zapimoveis.com.br/imovel/venda-casa-jardim-1","name":"Casa"}
    Valores a partir de R$ 990.000 e metragens de 180 m².
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="São Paulo",
        estado_ref="SP",
        bairro_ref="Jardim",
    )
    assert len(cards) == 1
    assert "990" in str(cards[0]["valor_venda"]) or cards[0]["valor_venda"] == 990_000.0


def test_imovelweb_oferta_bare():
    md = """
    Ver oferta https://www.imovelweb.com.br/oferta/apartamento-centro-abc-123.html
    88 m² por R$ 620.000
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="Curitiba",
        estado_ref="PR",
        bairro_ref="Centro",
    )
    assert len(cards) == 1
    assert "/oferta/" in cards[0]["url_anuncio"]


def test_imovelweb_imovel_slug_construida():
    """Ficha ``/imovel/…`` costuma trazer 'área construída' no markdown."""
    md = """
    https://www.imovelweb.com.br/imovel/apartamento-vila-mariana-12345678.html
    Área construída 102 m²
    R$ 715.000
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="São Paulo",
        estado_ref="SP",
        bairro_ref="Vila Mariana",
    )
    assert len(cards) == 1
    assert "/imovel/" in cards[0]["url_anuncio"]
    assert cards[0]["area_m2"] == 102.0
    assert cards[0]["valor_venda"] == 715_000.0


def test_chavesnamao_imovel_markdown_link_classico_ainda_funciona():
    md = "[Casa térrea](https://www.chavesnamao.com.br/imovel/venda-casa-xyz-999/) 110 m² R$ 780.000"
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="São Paulo",
        estado_ref="SP",
        bairro_ref="Vila",
    )
    assert len(cards) == 1


def test_chavesnamao_logradouro_de_endereco_rotulo_ou_rua_nao_slug():
    md = """
    [Sobrado](https://www.chavesnamao.com.br/imovel/venda-sobrado-moema-sp-12345/)
    Localização: Rua Canário, 890 - Moema
    210 m²  R$ 1.890.000
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="São Paulo",
        estado_ref="SP",
        bairro_ref="Moema",
    )
    assert len(cards) == 1
    logr = (cards[0].get("logradouro") or "").lower()
    assert "canário" in logr or "canario" in logr
    assert "moema" in logr or "890" in logr


def test_prefere_preco_perto_do_link_quando_ha_r_mais_cedo_na_janela():
    """Evita usar R$ de 'similares' antes do URL quando o valor certo vem depois com a área."""
    md = """
    Imóveis similares a partir de R$ 750.000
    https://www.zapimoveis.com.br/imovel/venda-apartamento-centro-campinas-99001234
    95 m² por R$ 1.100.000
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="Campinas",
        estado_ref="SP",
        bairro_ref="Centro",
    )
    assert len(cards) == 1
    assert cards[0]["valor_venda"] == 1_100_000.0
    assert cards[0]["area_m2"] == 95.0


def test_logradouro_fallback_sem_prefixar_preco_do_titulo():
    md = """
    [Anúncio](https://www.zapimoveis.com.br/imovel/venda-apartamento-z-123)
    R$ 330.000  72 m²
    """
    cards = extrair_anuncios_markdown_generico(
        md,
        cidade_ref="Campinas",
        estado_ref="SP",
        bairro_ref="Centro",
    )
    assert len(cards) == 1
    assert not str(cards[0].get("logradouro") or "").strip().startswith("R$")


def test_zap_area_util_decimal_e_nbsp():
    md = (
        "https://www.zapimoveis.com.br/imovel/venda-apartamento-centro-campinas-990099\n"
        "Área útil\u00a095,5\u00a0m² — R$ 890.000"
    )
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Campinas", estado_ref="SP", bairro_ref="Centro"
    )
    assert len(cards) == 1
    assert cards[0]["area_m2"] == 95.5
    assert cards[0]["valor_venda"] == 890_000.0


def test_zap_metragem_sem_espaco_antes_de_m2():
    md = "https://www.zapimoveis.com.br/imovel/venda-casa-jardim-111 120m² R$ 640.000"
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Campinas", estado_ref="SP", bairro_ref="Jardim"
    )
    assert len(cards) == 1
    assert cards[0]["area_m2"] == 120.0


def test_chavesnamao_url_com_querystring_e_metragem():
    md = (
        "[Ver imóvel](https://www.chavesnamao.com.br/imovel/venda-sobrado-abc-12/?utm=x) "
        "Metragem total 140 m² — R$ 920.000"
    )
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Porto Alegre", estado_ref="RS", bairro_ref="Cidade Baixa"
    )
    assert len(cards) == 1
    assert "chavesnamao.com.br" in cards[0]["url_anuncio"]
    assert cards[0]["area_m2"] == 140.0
    assert cards[0]["valor_venda"] == 920_000.0


def test_json_canonical_url_quintoandar():
    md = """
    {"canonicalUrl":"https://www.quintoandar.com.br/imovel/9123456789012345/"}
    55 m²  R$ 410.000
    """
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="São Paulo", estado_ref="SP", bairro_ref="Pinheiros"
    )
    assert len(cards) == 1
    assert "quintoandar.com.br/imovel/" in cards[0]["url_anuncio"]


def test_olx_vi_anuncio():
    md = "Detalhe https://www.olx.com.br/vi-9988776655 68 m² R$ 295.000"
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Belo Horizonte", estado_ref="MG", bairro_ref="Savassi"
    )
    assert len(cards) == 1
    assert "/vi-" in cards[0]["url_anuncio"]


def test_loft_imovel_markdown():
    md = "### Loft compacto\nhttps://www.loft.com.br/imovel/venda-loft-centro-xyz-99/\n42 m² R$ 380.000"
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="São Paulo", estado_ref="SP", bairro_ref="Centro"
    )
    assert len(cards) == 1
    assert "loft.com.br" in cards[0]["url_anuncio"]
    assert cards[0]["area_m2"] == 42.0


def test_loft_ignora_preco_similares_antes_do_link():
    md = """
    Similares a partir de R$ 620.000
    https://www.loft.com.br/imovel/venda-apartamento-pinheiros-abc-3/
    78 m²
    R$ 910.000
    """
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="São Paulo", estado_ref="SP", bairro_ref="Pinheiros"
    )
    assert len(cards) == 1
    assert cards[0]["valor_venda"] == 910_000.0
    assert cards[0]["area_m2"] == 78.0


def test_kenlo_imovel_bare_url_com_preco_area():
    md = "https://portal.kenlo.com.br/imovel/taubate/casa-condominio-villagio 120 m² R$ 780.000"
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Taubaté", estado_ref="SP", bairro_ref="Chácara do Visconde"
    )
    assert len(cards) == 1
    assert "kenlo.com.br" in cards[0]["url_anuncio"]
    assert cards[0]["area_m2"] == 120.0
    assert cards[0]["valor_venda"] == 780_000.0


def test_parser_titulo_fallback_quando_link_mensagem():
    md = """
R$ 530.000
[Mensagem](https://www.zapimoveis.com.br/imovel/venda-casa-taubate-sp-120m2-id-1/)
![Casa de condomínio com 3 quartos em Taubaté](https://img.exemplo/foto.jpg)
120 m²
"""
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Taubaté", estado_ref="SP", bairro_ref="Centro"
    )
    assert len(cards) == 1
    assert "mensagem" not in str(cards[0].get("titulo") or "").lower()


def test_zap_usa_linha_rua_no_markdown_nao_slug_seo():
    md = """
    https://www.zapimoveis.com.br/imovel/venda-apartamento-jardim-paulista-campinas-990011
    Rua Argentina, 1500
    Área útil 88 m²
    R$ 1.350.000
    """
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Campinas", estado_ref="SP", bairro_ref="Jardim Paulista"
    )
    assert len(cards) == 1
    assert "Argentina" in cards[0]["logradouro"] or "argentina" in cards[0]["logradouro"].lower()
    assert cards[0]["valor_venda"] == 1_350_000.0


def test_zap_ficha_aluguel_nao_entra_como_comparavel():
    md = "https://www.zapimoveis.com.br/imovel/aluguel-apartamento-centro-123/ 55 m² R$ 2.800"
    cards = extrair_anuncios_markdown_generico(
        md, cidade_ref="Campinas", estado_ref="SP", bairro_ref="Centro"
    )
    assert cards == []
