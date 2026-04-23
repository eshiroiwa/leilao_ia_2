from leilao_ia_v2.vivareal.parser_cards_listagem import extrair_cards_anuncios_vivareal_markdown

_MARKDOWN_CARD = """
Tamanho do imóvel 90 m²
Quantidade de quartos 2
Quantidade de banheiros 2
Quantidade de vagas 1

R$ 550.000

Rua das Acácias, 42
**Apartamento à venda**
Contatar](https://www.vivareal.com.br/imovel/apartamento-rua-acacias-moema-abc123/)
"""


def test_extrair_um_card_campos_basicos():
    rows = extrair_cards_anuncios_vivareal_markdown(
        _MARKDOWN_CARD,
        cidade_ref="São Paulo",
        estado_ref="SP",
        bairro_ref="Moema",
    )
    assert len(rows) == 1
    a = rows[0]
    assert a["portal"] == "vivareal.com.br"
    assert a["cidade"] == "São Paulo"
    assert a["estado"] == "SP"
    assert a["bairro"] == "Moema"
    assert a["area_m2"] == 90.0
    assert a["valor_venda"] == 550_000.0
    assert a["quartos"] == 2
    assert "acácias" in (a.get("logradouro") or "").lower() or "acacias" in (a.get("logradouro") or "").lower()
    assert "vivareal.com.br" in a["url_anuncio"]


def test_extrair_vazio_sem_contatar():
    assert (
        extrair_cards_anuncios_vivareal_markdown(
            "só texto sem padrão de card",
            cidade_ref="X",
            estado_ref="SP",
            bairro_ref="",
        )
        == []
    )


def test_prefere_preco_depois_da_area_quando_ha_varios_r():
    """Último R$ válido após 'Tamanho do imóvel' costuma ser o de venda."""
    md = """
Tamanho do imóvel 80 m²
Financiamento simulado R$ 200.000
R$ 890.000

Rua Beta, 10
**Apartamento**
Contatar](https://www.vivareal.com.br/imovel/apartamento-rua-beta-moema-xyz/)
"""
    rows = extrair_cards_anuncios_vivareal_markdown(
        md,
        cidade_ref="São Paulo",
        estado_ref="SP",
        bairro_ref="Moema",
    )
    assert len(rows) == 1
    assert rows[0]["valor_venda"] == 890_000.0
