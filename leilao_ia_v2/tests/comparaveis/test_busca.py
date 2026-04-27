"""Testes do wrapper Firecrawl Search com orçamento (PR3)."""

from __future__ import annotations

import pytest

from leilao_ia_v2.comparaveis.busca import (
    FirecrawlSearchIndisponivel,
    ResultadoBusca,
    executar_search,
)
from leilao_ia_v2.comparaveis.orcamento import (
    OrcamentoExcedido,
    OrcamentoFirecrawl,
)


# -----------------------------------------------------------------------------
# Cliente fake (Mock manual, para visibilidade explícita do que aconteceu)
# -----------------------------------------------------------------------------

class _ClienteFake:
    """Substitui ``Firecrawl`` em testes. Regista chamadas e devolve respostas planeadas."""

    def __init__(self, resposta=None, *, exception: Exception | None = None):
        self.resposta = resposta if resposta is not None else {"web": []}
        self.exception = exception
        self.chamadas: list[tuple[str, int]] = []

    def search(self, query: str, *, limit: int):
        self.chamadas.append((query, limit))
        if self.exception is not None:
            raise self.exception
        return self.resposta


def _resp_web(*urls: str) -> dict:
    return {"web": [{"url": u} for u in urls]}


# -----------------------------------------------------------------------------
# Pré-condições / argumentos
# -----------------------------------------------------------------------------

class TestPreCondicoes:
    def test_query_vazia_nao_executa(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake()
        r = executar_search("", limit=10, orcamento=o, cliente=cli)
        assert isinstance(r, ResultadoBusca)
        assert not r.executada and r.motivo_nao_executada == "query_vazia"
        assert cli.chamadas == []
        assert o.gasto == 0

    def test_query_so_espacos_nao_executa(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake()
        r = executar_search("   \n  ", limit=10, orcamento=o, cliente=cli)
        assert not r.executada
        assert o.gasto == 0


class TestOrcamentoBloqueia:
    def test_orcamento_insuficiente_nao_chama_api(self):
        # cap=1 → custo de search com limit=10 é 2 → não cabe.
        o = OrcamentoFirecrawl(cap=1)
        cli = _ClienteFake(_resp_web("https://www.zapimoveis.com.br/imovel/x/"))
        r = executar_search("apartamento taubate sp", limit=10, orcamento=o, cliente=cli)
        assert not r.executada
        assert "orcamento_insuficiente" in r.motivo_nao_executada
        assert cli.chamadas == []
        assert o.gasto == 0

    def test_orcamento_apertado_executa_e_consome(self):
        o = OrcamentoFirecrawl(cap=2)
        cli = _ClienteFake(_resp_web("https://www.zapimoveis.com.br/imovel/x/"))
        r = executar_search("q", limit=10, orcamento=o, cliente=cli)
        assert r.executada
        assert o.gasto == 2 and o.restante == 0
        assert r.custo_creditos == 2

    def test_segunda_search_acima_do_cap_devolve_nao_executada(self):
        o = OrcamentoFirecrawl(cap=2)
        cli = _ClienteFake(_resp_web("https://www.zapimoveis.com.br/imovel/x/"))
        executar_search("q1", limit=10, orcamento=o, cliente=cli)
        r2 = executar_search("q2", limit=10, orcamento=o, cliente=cli)
        assert not r2.executada
        assert "orcamento_insuficiente" in r2.motivo_nao_executada
        # Segunda chamada não deve aparecer no fake.
        assert len(cli.chamadas) == 1


class TestLimit:
    def test_limit_maior_que_max_clampado(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake(_resp_web())
        executar_search("q", limit=999, orcamento=o, cliente=cli)
        assert cli.chamadas[0][1] == 20  # _LIMIT_MAX

    def test_limit_menor_que_um_clampado(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake(_resp_web())
        executar_search("q", limit=0, orcamento=o, cliente=cli)
        assert cli.chamadas[0][1] >= 1

    def test_limit_default_quando_omitido(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake(_resp_web())
        executar_search("q", orcamento=o, cliente=cli)
        assert cli.chamadas[0][1] == 10


# -----------------------------------------------------------------------------
# Filtragem das URLs devolvidas
# -----------------------------------------------------------------------------

class TestFiltragemUrls:
    def test_urls_aceites_e_descartadas_separadas(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake(
            _resp_web(
                "https://www.zapimoveis.com.br/imovel/casa-x/",       # aceite
                "https://www.zapimoveis.com.br/venda/sp/sao-paulo/",  # listagem → descarta
                "https://blogimoveis.com/dicas",                       # domínio fora → descarta
                "https://www.vivareal.com.br/imovel/aluguel-x/",       # aluguel → descarta
            )
        )
        r = executar_search("q", limit=10, orcamento=o, cliente=cli)
        assert r.executada
        assert r.urls_aceites == ("https://www.zapimoveis.com.br/imovel/casa-x/",)
        assert len(r.urls_descartadas) == 3
        assert r.total_resultados == 4

    def test_dedup_por_url(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake(
            _resp_web(
                "https://www.zapimoveis.com.br/imovel/x/",
                "https://www.zapimoveis.com.br/imovel/x/#section",
                "https://www.zapimoveis.com.br/imovel/x/",
            )
        )
        r = executar_search("q", limit=10, orcamento=o, cliente=cli)
        assert len(r.urls_aceites) == 1

    def test_resposta_sem_web_devolve_listas_vazias(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake({})  # sem 'web'
        r = executar_search("q", limit=10, orcamento=o, cliente=cli)
        assert r.executada
        assert r.urls_aceites == () and r.urls_descartadas == ()


class TestRespostaModelo:
    """Algumas SDKs devolvem objetos com `.url`/`.link` em vez de dicts."""

    def test_objetos_com_atributo_url(self):
        class _Item:
            def __init__(self, url):
                self.url = url

        class _CliObj:
            chamadas = []

            def search(self, query, *, limit):
                self.chamadas.append((query, limit))
                return {"web": [_Item("https://www.zapimoveis.com.br/imovel/y/")]}

        o = OrcamentoFirecrawl(cap=15)
        r = executar_search("q", orcamento=o, cliente=_CliObj())
        assert r.urls_aceites == ("https://www.zapimoveis.com.br/imovel/y/",)


# -----------------------------------------------------------------------------
# Erros
# -----------------------------------------------------------------------------

class TestErros:
    def test_excecao_da_api_devolve_executada_false(self):
        o = OrcamentoFirecrawl(cap=15)
        cli = _ClienteFake(exception=RuntimeError("boom"))
        r = executar_search("q", orcamento=o, cliente=cli)
        assert not r.executada
        assert "erro_api" in r.motivo_nao_executada
        # Não consome créditos quando a chamada falha (custo ainda não foi consumido).
        assert o.gasto == 0

    def test_orcamento_excedido_propaga(self):
        """Se o orçamento for esgotado *durante* o consumir_search (ex.: race
        em multi-thread), a OrcamentoExcedido deve propagar — é um erro de
        programação, não de runtime esperado."""

        class _CliRaiseOrc:
            def search(self, query, *, limit):
                raise OrcamentoExcedido("inesperado")

        o = OrcamentoFirecrawl(cap=15)
        with pytest.raises(OrcamentoExcedido):
            executar_search("q", orcamento=o, cliente=_CliRaiseOrc())

    def test_sem_api_key_e_sem_cliente_levanta(self, monkeypatch):
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        o = OrcamentoFirecrawl(cap=15)
        with pytest.raises(FirecrawlSearchIndisponivel):
            executar_search("q", orcamento=o)
