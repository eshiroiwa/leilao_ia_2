"""Testes do wrapper Firecrawl Scrape com orçamento e cache (PR3)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from leilao_ia_v2.comparaveis.orcamento import (
    OrcamentoExcedido,
    OrcamentoFirecrawl,
)
from leilao_ia_v2.comparaveis.scrape import (
    FirecrawlScrapeIndisponivel,
    ResultadoScrape,
    scrape_url,
)


# -----------------------------------------------------------------------------
# Cliente fake
# -----------------------------------------------------------------------------

class _ClienteFake:
    def __init__(self, markdown: str = "", *, exception: Exception | None = None):
        self.markdown = markdown
        self.exception = exception
        self.chamadas: list[tuple[str, list[str]]] = []

    def scrape(self, url: str, *, formats):
        self.chamadas.append((url, list(formats)))
        if self.exception is not None:
            raise self.exception
        return {"markdown": self.markdown}


# -----------------------------------------------------------------------------
# Pré-condições
# -----------------------------------------------------------------------------

class TestPreCondicoes:
    def test_url_vazia_nao_executa(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="x")
        r = scrape_url("", orcamento=o, cliente=cli)
        assert isinstance(r, ResultadoScrape)
        assert not r.executado and r.motivo_nao_executado == "url_vazia"
        assert cli.chamadas == []
        assert o.gasto == 0

    def test_url_so_espacos(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="x")
        r = scrape_url("   \n", orcamento=o, cliente=cli)
        assert not r.executado


# -----------------------------------------------------------------------------
# Cache
# -----------------------------------------------------------------------------

class TestCache:
    def test_cache_hit_nao_consome_credito(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="da api")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value="markdown do cache",
        ):
            r = scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
            )
        assert r.executado and r.teve_sucesso
        assert r.fonte == "cache"
        assert r.markdown == "markdown do cache"
        assert r.custo_creditos == 0
        assert o.gasto == 0
        assert cli.chamadas == []  # API não é chamada

    def test_cache_miss_chama_api_e_grava(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="md fresco")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ), patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.gravar_markdown_cache",
        ) as mock_grava:
            r = scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
            )
        assert r.executado and r.teve_sucesso
        assert r.fonte == "firecrawl"
        assert r.markdown == "md fresco"
        assert r.custo_creditos == 1
        assert o.gasto == 1
        assert len(cli.chamadas) == 1
        mock_grava.assert_called_once()

    def test_ignorar_cache_forca_api_mesmo_com_hit(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="fresco")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value="velho",
        ), patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.gravar_markdown_cache",
        ):
            r = scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
                ignorar_cache=True,
            )
        assert r.fonte == "firecrawl"
        assert r.markdown == "fresco"
        assert o.gasto == 1

    def test_gravar_cache_false_nao_grava(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="md")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ), patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.gravar_markdown_cache",
        ) as mock_grava:
            scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
                gravar_cache=False,
            )
        mock_grava.assert_not_called()


# -----------------------------------------------------------------------------
# Orçamento
# -----------------------------------------------------------------------------

class TestOrcamentoBloqueia:
    def test_cap_zero_efetivo_nao_chama_api(self):
        # cap=1, gasto=1 → restante 0 → não cabe scrape (1 cr).
        o = OrcamentoFirecrawl(cap=1, gasto=1)
        cli = _ClienteFake(markdown="x")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ):
            r = scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
            )
        assert not r.executado
        assert "orcamento_insuficiente" in r.motivo_nao_executado
        assert cli.chamadas == []
        assert o.gasto == 1

    def test_cache_hit_funciona_mesmo_sem_orcamento(self):
        """Cache é gratuito — devolve sucesso mesmo com orçamento esgotado."""
        o = OrcamentoFirecrawl(cap=1, gasto=1)
        cli = _ClienteFake(markdown="api")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value="cache",
        ):
            r = scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
            )
        assert r.teve_sucesso and r.fonte == "cache"

    def test_consome_exatamente_um_credito(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="md")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ), patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.gravar_markdown_cache",
        ):
            scrape_url("https://www.vivareal.com.br/imovel/y/", orcamento=o, cliente=cli)
            scrape_url("https://www.vivareal.com.br/imovel/z/", orcamento=o, cliente=cli)
        assert o.gasto == 2 and o.restante == 18


# -----------------------------------------------------------------------------
# Erros e respostas degeneradas
# -----------------------------------------------------------------------------

class TestErros:
    def test_excecao_api_devolve_executado_false(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(exception=RuntimeError("boom"))
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ):
            r = scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
            )
        assert not r.executado
        assert "erro_api" in r.motivo_nao_executado
        assert o.gasto == 0

    def test_markdown_vazio_devolve_executado_true_sem_sucesso(self):
        o = OrcamentoFirecrawl(cap=20)
        cli = _ClienteFake(markdown="   ")
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ), patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.gravar_markdown_cache",
        ):
            r = scrape_url(
                "https://www.zapimoveis.com.br/imovel/x/",
                orcamento=o,
                cliente=cli,
            )
        assert r.executado and not r.teve_sucesso
        assert r.motivo_nao_executado == "markdown_vazio"
        assert r.custo_creditos == 1  # API foi chamada → consome
        assert o.gasto == 1

    def test_orcamento_excedido_propaga(self):
        class _CliRaiseOrc:
            def scrape(self, url, *, formats):
                raise OrcamentoExcedido("race")

        o = OrcamentoFirecrawl(cap=20)
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ):
            with pytest.raises(OrcamentoExcedido):
                scrape_url(
                    "https://www.zapimoveis.com.br/imovel/x/",
                    orcamento=o,
                    cliente=_CliRaiseOrc(),
                )

    def test_sem_api_key_e_sem_cliente_levanta(self, monkeypatch):
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        o = OrcamentoFirecrawl(cap=20)
        with patch(
            "leilao_ia_v2.comparaveis.scrape.disk_cache.ler_markdown_cache",
            return_value=None,
        ):
            with pytest.raises(FirecrawlScrapeIndisponivel):
                scrape_url("https://www.zapimoveis.com.br/imovel/x/", orcamento=o)
