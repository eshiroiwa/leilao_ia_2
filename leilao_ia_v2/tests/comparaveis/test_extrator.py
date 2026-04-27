"""Testes do extrator de cards SEM inventar cidade (PR2)."""

from __future__ import annotations

import pytest

from leilao_ia_v2.comparaveis.extrator import (
    CardExtraido,
    extrair_cards,
    url_eh_anuncio_aproveitavel,
)


# -----------------------------------------------------------------------------
# url_eh_anuncio_aproveitavel
# -----------------------------------------------------------------------------

class TestUrlAproveitavel:
    @pytest.mark.parametrize(
        "url",
        [
            "https://www.zapimoveis.com.br/imovel/venda-apartamento-2-quartos-vila-mariana-sao-paulo-sp/12345/",
            "https://www.vivareal.com.br/imovel/casa-3-quartos-centro-pindamonhangaba-sp-id-87/",
            "https://www.quintoandar.com.br/imovel/abc123",
            "https://www.olx.com.br/d-imoveis/iid-12345",
            "https://chavesnamao.com.br/imovel/sp-pinda-casa-id",
            "https://www.imovelweb.com.br/imovel/casa-venda-1234",
            "https://www.loft.com.br/imovel/venda-apartamento-bairro-x-sp",
        ],
    )
    def test_anuncios_individuais_aceites(self, url):
        assert url_eh_anuncio_aproveitavel(url)

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.zapimoveis.com.br/venda/apartamentos/sp+pindamonhangaba/",
            "https://www.quintoandar.com.br/comprar/imovel/sao-paulo-sp-brasil/casa",
            "https://www.olx.com.br/imoveis/estado-sp/sao-paulo-e-regiao",
            "https://www.vivareal.com.br/venda/sp/pindamonhangaba/",
            "https://www.imovelweb.com.br/casas-venda-sao-paulo.html",
            "https://www.loft.com.br/comprar/sp/sao-paulo",
        ],
    )
    def test_listagens_rejeitadas(self, url):
        assert not url_eh_anuncio_aproveitavel(url)

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.zapimoveis.com.br/imovel/aluguel-apto-x/",
            "https://www.olx.com.br/imoveis/aluguel/d-12345",
            "https://www.quintoandar.com.br/imovel/aluguel-de-casa-x",
            "https://www.vivareal.com.br/imovel/aluguel-apto-sp",
        ],
    )
    def test_alugueis_rejeitados(self, url):
        assert not url_eh_anuncio_aproveitavel(url)

    def test_dominio_aleatorio_rejeitado(self):
        assert not url_eh_anuncio_aproveitavel(
            "https://blogimoveis.com.br/dicas/comprar-imovel"
        )

    def test_string_vazia_rejeitada(self):
        assert not url_eh_anuncio_aproveitavel("")


# -----------------------------------------------------------------------------
# extrair_cards — markdown sintético
# -----------------------------------------------------------------------------

def _md_card(url: str, titulo: str, preco: str, area: str) -> str:
    """Constrói uma janela de markdown que simula um card de portal.

    Estrutura inspirada nos markdowns reais devolvidos pelo Firecrawl em
    Zap/QuintoAndar/Loft: thumbnail (imagem), título em negrito, link
    "Ver detalhes" para a ficha, e linha com preço + área.
    """
    return (
        f"![{titulo}](https://cdn.portal.example/{abs(hash(url)) % 99999}.jpg)\n\n"
        f"**{titulo}**\n\n"
        f"[Ver detalhes]({url})\n\n"
        f"R$ {preco} · {area} m²\n\n"
        "---\n\n"
    )


class TestExtrairCardsBasico:
    def test_markdown_vazio(self):
        assert extrair_cards("") == []

    def test_markdown_so_espacos(self):
        assert extrair_cards("   \n\n  ") == []

    def test_um_card_simples(self):
        md = _md_card(
            "https://www.zapimoveis.com.br/imovel/venda-apto-x/",
            "Apartamento 2 quartos no Centro",
            "350.000",
            "65",
        )
        cards = extrair_cards(md)
        assert len(cards) == 1
        c = cards[0]
        assert isinstance(c, CardExtraido)
        assert c.url_anuncio == "https://www.zapimoveis.com.br/imovel/venda-apto-x/"
        assert c.portal == "zapimoveis.com.br"
        assert c.valor_venda == 350_000.0
        assert c.area_m2 == 65.0
        assert c.preco_m2 == round(350_000.0 / 65.0, 2)
        assert "Apartamento 2 quartos" in c.titulo

    def test_multiplos_cards_sem_duplicacao(self):
        md = (
            _md_card("https://www.zapimoveis.com.br/imovel/a1/", "Casa Vila X", "500.000", "80")
            + _md_card("https://www.zapimoveis.com.br/imovel/a2/", "Casa Vila Y", "620.000", "95")
            + _md_card("https://www.zapimoveis.com.br/imovel/a3/", "Casa Vila Z", "780.000", "120")
        )
        cards = extrair_cards(md)
        assert len(cards) == 3
        urls = {c.url_anuncio for c in cards}
        assert urls == {
            "https://www.zapimoveis.com.br/imovel/a1/",
            "https://www.zapimoveis.com.br/imovel/a2/",
            "https://www.zapimoveis.com.br/imovel/a3/",
        }

    def test_url_repetida_dedup(self):
        url = "https://www.zapimoveis.com.br/imovel/x/"
        md = _md_card(url, "T1", "100.000", "30") + _md_card(url, "T2", "100.000", "30")
        cards = extrair_cards(md)
        assert len(cards) == 1


class TestNenhumCidadeNoCard:
    """Garantia da decisão arquitetural: o extrator não devolve cidade/UF/bairro
    confirmado. Esses campos são preenchidos pela validação por geocode no
    pipeline (PR4)."""

    def test_dataclass_nao_tem_cidade(self):
        md = _md_card(
            "https://www.zapimoveis.com.br/imovel/x/",
            "Apto Centro Pindamonhangaba SP",
            "300.000",
            "55",
        )
        cards = extrair_cards(md)
        assert cards
        c = cards[0]
        # Campos definitivos NÃO existem no CardExtraido.
        for campo_proibido in ("cidade", "estado", "estado_uf", "bairro"):
            assert not hasattr(c, campo_proibido)

    def test_inferencia_de_bairro_no_titulo(self):
        md = _md_card(
            "https://www.vivareal.com.br/imovel/x/",
            "Casa no bairro Vila São José com 2 quartos",
            "400.000",
            "70",
        )
        cards = extrair_cards(md)
        assert cards
        # bairro_inferido vem do anúncio em si, não do leilão.
        assert "Vila São José" in cards[0].bairro_inferido


class TestParPrecoArea:
    def test_preco_proximo_de_taxa_descartado(self):
        """'R$ 700/mês' (condomínio) não deve virar preço de venda."""
        md = (
            "**Apto 50m² Vila X**\n\n"
            "Condomínio R$ 700/mês\n\n"
            "[Ver]({url})\n\n"
            "R$ 320.000 · 50 m²\n\n"
        ).replace("{url}", "https://www.zapimoveis.com.br/imovel/abc/")
        cards = extrair_cards(md)
        assert len(cards) == 1
        assert cards[0].valor_venda == 320_000.0
        assert cards[0].area_m2 == 50.0

    def test_preco_e_area_implausiveis_descartados(self):
        # Preço 100 reais → fora de [30k, 120M] → descarta o card.
        md = _md_card("https://www.zapimoveis.com.br/imovel/x/", "T", "100", "60")
        assert extrair_cards(md) == []

    def test_area_grande_descartada(self):
        # Área 1.234.567 m² (com pontos como milhar) está fora de [12, 50000] → descarta.
        md = _md_card("https://www.zapimoveis.com.br/imovel/x/", "T", "300.000", "1.234.567")
        assert extrair_cards(md) == []

    def test_area_pequena_descartada(self):
        # Área 5 m² fora do limite inferior (12) → descarta.
        md = _md_card("https://www.zapimoveis.com.br/imovel/x/", "T", "300.000", "5")
        assert extrair_cards(md) == []


class TestFormatosBR:
    def test_preco_com_pontos_e_virgula(self):
        md = _md_card(
            "https://www.zapimoveis.com.br/imovel/x/", "T", "1.250.000,00", "180"
        )
        cards = extrair_cards(md)
        assert cards and cards[0].valor_venda == 1_250_000.0

    def test_area_decimal_br(self):
        md = _md_card(
            "https://www.zapimoveis.com.br/imovel/x/", "T", "450.000", "67,5"
        )
        cards = extrair_cards(md)
        assert cards and cards[0].area_m2 == 67.5


class TestExtracaoLogradouro:
    def test_logradouro_no_titulo(self):
        md = _md_card(
            "https://www.zapimoveis.com.br/imovel/x/",
            "Apartamento na Rua Barão do Rio Branco 1500",
            "500.000",
            "80",
        )
        cards = extrair_cards(md)
        assert cards
        assert "Barão do Rio Branco" in cards[0].logradouro_inferido


class TestOrdemDosCards:
    def test_cards_em_ordem_textual(self):
        md = (
            _md_card("https://www.zapimoveis.com.br/imovel/a/", "T1", "300.000", "50")
            + _md_card("https://www.zapimoveis.com.br/imovel/b/", "T2", "400.000", "60")
            + _md_card("https://www.zapimoveis.com.br/imovel/c/", "T3", "500.000", "70")
        )
        cards = extrair_cards(md)
        assert [c.url_anuncio.rsplit("/", 2)[-2] for c in cards] == ["a", "b", "c"]
