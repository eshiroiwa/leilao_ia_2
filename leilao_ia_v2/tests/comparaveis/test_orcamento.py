"""Testes do contador de créditos Firecrawl (PR1 do plano de reescrita)."""

from __future__ import annotations

import pytest

from leilao_ia_v2.comparaveis.orcamento import (
    CAP_PADRAO_CREDITOS,
    OrcamentoExcedido,
    OrcamentoFirecrawl,
    custo_scrape,
    custo_search,
)


class TestCustoSearch:
    """Custo de search corresponde a 2 créditos por bloco de até 10 resultados."""

    @pytest.mark.parametrize(
        "limit,esperado",
        [
            (1, 2),
            (5, 2),
            (10, 2),
            (11, 4),
            (15, 4),
            (20, 4),
            (21, 6),
            (30, 6),
        ],
    )
    def test_custos_em_blocos_de_10(self, limit, esperado):
        assert custo_search(limit) == esperado

    def test_limit_zero_arredonda_para_um_bloco(self):
        # Defensivo: nunca deve devolver 0 (Firecrawl cobra mesmo com limit baixo).
        assert custo_search(0) == 2

    def test_limit_negativo_tratado_como_um(self):
        assert custo_search(-5) == 2


class TestCustoScrape:
    def test_um_credito_por_chamada(self):
        assert custo_scrape() == 1


class TestOrcamentoFirecrawl:
    def test_cap_padrao(self):
        o = OrcamentoFirecrawl()
        assert o.cap == CAP_PADRAO_CREDITOS == 15
        assert o.gasto == 0
        assert o.restante == 15

    def test_cap_customizado(self):
        o = OrcamentoFirecrawl(cap=10)
        assert o.cap == 10 and o.restante == 10

    def test_cap_invalido_levanta(self):
        with pytest.raises(ValueError):
            OrcamentoFirecrawl(cap=0)
        with pytest.raises(ValueError):
            OrcamentoFirecrawl(cap=-3)

    def test_gasto_inicial_negativo_levanta(self):
        with pytest.raises(ValueError):
            OrcamentoFirecrawl(cap=10, gasto=-1)

    def test_gasto_inicial_excede_cap_levanta(self):
        with pytest.raises(ValueError):
            OrcamentoFirecrawl(cap=10, gasto=11)

    def test_pode_search_dentro_do_orcamento(self):
        o = OrcamentoFirecrawl(cap=15)
        assert o.pode_search(limit=10) is True
        assert o.pode_search(limit=20) is True

    def test_pode_search_excede_orcamento(self):
        # custo de search com limit=10 é 2 → cap=1 não cabe.
        o = OrcamentoFirecrawl(cap=1)
        assert o.pode_search(limit=10) is False

    def test_consumir_search_atualiza_gasto_e_restante(self):
        o = OrcamentoFirecrawl(cap=15)
        custo = o.consumir_search(limit=10, query="apartamento Pindamonhangaba SP")
        assert custo == 2
        assert o.gasto == 2 and o.restante == 13
        assert len(o.eventos) == 1
        assert o.eventos[0].tipo == "search"
        assert "apartamento" in o.eventos[0].detalhe

    def test_consumir_search_acima_do_cap_levanta(self):
        o = OrcamentoFirecrawl(cap=1)
        with pytest.raises(OrcamentoExcedido) as exc:
            o.consumir_search(limit=10)
        assert "search" in str(exc.value).lower()
        # Estado não muda em erro.
        assert o.gasto == 0

    def test_consumir_scrape_atualiza_gasto(self):
        o = OrcamentoFirecrawl(cap=15)
        o.consumir_scrape(url="https://exemplo.com/x")
        assert o.gasto == 1 and o.restante == 14
        assert o.eventos[-1].tipo == "scrape"
        assert "exemplo.com" in o.eventos[-1].detalhe

    def test_consumir_scrape_acima_do_cap_levanta(self):
        o = OrcamentoFirecrawl(cap=2, gasto=2)
        assert o.pode_scrape() is False
        with pytest.raises(OrcamentoExcedido):
            o.consumir_scrape(url="x")

    def test_cenario_realista_um_search_e_varios_scrapes(self):
        """Simula o orçamento alvo (decisão 2-C): 15 créditos = 1 search 10 + ~13 scrapes."""
        o = OrcamentoFirecrawl(cap=15)
        o.consumir_search(limit=10, query="casa Taubaté SP")
        scrapes = 0
        while o.pode_scrape():
            o.consumir_scrape(url=f"https://portal.example/{scrapes}")
            scrapes += 1
        assert scrapes == 13
        assert o.gasto == 15 and o.restante == 0

    def test_cenario_dois_searches_e_scrapes(self):
        """2 searches (4 créditos) + scrapes até esgotar (11)."""
        o = OrcamentoFirecrawl(cap=15)
        o.consumir_search(limit=10, query="q1")
        o.consumir_search(limit=10, query="q2")
        assert o.gasto == 4 and o.restante == 11
        scrapes = 0
        while o.pode_scrape():
            o.consumir_scrape(url=f"u{scrapes}")
            scrapes += 1
        assert scrapes == 11
        assert o.restante == 0

    def test_resumo_serializavel(self):
        o = OrcamentoFirecrawl(cap=15)
        o.consumir_search(limit=10, query="q")
        o.consumir_scrape(url="https://x.com/a")
        r = o.resumo()
        assert r["cap"] == 15 and r["gasto"] == 3 and r["restante"] == 12
        assert r["n_search"] == 1 and r["n_scrape"] == 1
        assert isinstance(r["eventos"], list) and len(r["eventos"]) == 2
        assert r["eventos"][0]["tipo"] == "search"
        assert r["eventos"][1]["tipo"] == "scrape"
