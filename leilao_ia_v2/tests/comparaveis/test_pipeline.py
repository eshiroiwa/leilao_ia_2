"""Testes integrados do orquestrador `comparaveis.pipeline` (PR4).

Todos os efeitos colaterais são injectados como hooks — nenhum teste toca
em rede, Firecrawl, geocoder ou Supabase reais.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from leilao_ia_v2.comparaveis.busca import ResultadoBusca
from leilao_ia_v2.comparaveis.extrator import CardExtraido
from leilao_ia_v2.comparaveis.frase import FraseBusca
from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.pagina_filtro import (
    ResultadoFiltroPagina,
    StatusPagina,
)
from leilao_ia_v2.comparaveis.persistencia import LinhaPersistir
from leilao_ia_v2.comparaveis.pipeline import (
    EstatisticasPipeline,
    LeilaoAlvo,
    ResultadoPipeline,
    executar_pipeline,
)
from leilao_ia_v2.comparaveis.refino_individual import ResultadoRefino
from leilao_ia_v2.comparaveis.scrape import ResultadoScrape
from leilao_ia_v2.comparaveis.validacao_cidade import ResultadoValidacaoMunicipio


def _refino_noop(cards_validados, **kw) -> ResultadoRefino:
    """Mock de refino que devolve os cards inalterados.

    Os testes do pipeline focam-se nos passos search → scrape → filtro → extract
    → validar → persistir. O refino tem testes próprios em
    ``test_refino_individual.py``; aqui injectamos um no-op para isolar.
    """
    return ResultadoRefino(cards_finais=list(cards_validados))


@pytest.fixture(autouse=True)
def _patch_refino_default(monkeypatch):
    """Substitui o default de :func:`refinar_cards_top_n` no módulo pipeline.

    Sem isto, os testes que não passam ``fn_refino=`` invocariam o refino real
    (que chama Firecrawl). Como cada teste injecta os outros hooks à mão,
    fazemos o mesmo para o refino — automaticamente — para não poluir cada
    chamada com ``fn_refino=_refino_noop``.
    """
    import leilao_ia_v2.comparaveis.pipeline as pl

    monkeypatch.setattr(pl, "refinar_cards_top_n", _refino_noop)


# -----------------------------------------------------------------------------
# Builders / fakes
# -----------------------------------------------------------------------------

def _leilao(cidade="Pindamonhangaba", uf="SP", tipo="apartamento", bairro="Centro", area=65.0):
    return LeilaoAlvo(
        cidade=cidade,
        estado_uf=uf,
        tipo_imovel=tipo,
        bairro=bairro,
        area_m2=area,
    )


def _frase(texto="apartamento 65 m² Centro Pindamonhangaba SP") -> FraseBusca:
    return FraseBusca(
        texto=texto,
        componentes={"cidade": "Pindamonhangaba", "uf": "SP", "tipo": "apartamento"},
    )


def _busca_ok(urls=("https://www.zapimoveis.com.br/imovel/1/", "https://www.vivareal.com.br/imovel/2/")):
    return ResultadoBusca(
        urls_aceites=tuple(urls),
        urls_descartadas=(),
        custo_creditos=2,
        executada=True,
    )


def _scrape_ok(url, md="# Apartamento 65m² Centro Pindamonhangaba SP\nEm Pindamonhangaba SP."):
    return ResultadoScrape(
        url=url,
        markdown=md,
        executado=True,
        custo_creditos=1,
        fonte="firecrawl",
    )


def _scrape_cache(url, md="# Apartamento Pindamonhangaba"):
    return ResultadoScrape(
        url=url,
        markdown=md,
        executado=True,
        custo_creditos=0,
        fonte="cache",
    )


def _scrape_falhou(url, motivo="vazio"):
    return ResultadoScrape(url=url, executado=False, motivo_nao_executado=motivo)


def _filtro(status=StatusPagina.CONFIRMADA, motivo="ok", concorrentes=()):
    return ResultadoFiltroPagina(
        status=status,
        cidade_alvo_slug="pindamonhangaba",
        motivo=motivo,
        cidades_concorrentes=tuple(concorrentes),
    )


def _card(url, valor=350_000.0, area=65.0, bairro="Centro", cidade_no_markdown=""):
    return CardExtraido(
        url_anuncio=url,
        portal="zapimoveis.com.br",
        valor_venda=valor,
        area_m2=area,
        titulo=f"Apto {area}m² {bairro}",
        logradouro_inferido="Rua Tal 100",
        bairro_inferido=bairro,
        cidade_no_markdown=cidade_no_markdown,
    )


def _val_ok(municipio="Pindamonhangaba"):
    return ResultadoValidacaoMunicipio(
        valido=True,
        motivo="ok",
        municipio_real=municipio,
        coordenadas=(-22.92, -45.46),
        municipio_alvo_slug="pindamonhangaba",
        municipio_real_slug="pindamonhangaba",
    )


def _val_reprovado(motivo="municipio_diferente", real="São Bernardo do Campo", real_slug="saobernardodocampo"):
    return ResultadoValidacaoMunicipio(
        valido=False,
        motivo=motivo,
        municipio_real=real,
        municipio_alvo_slug="pindamonhangaba",
        municipio_real_slug=real_slug,
    )


# -----------------------------------------------------------------------------
# Aborts: frase vazia, busca não executada
# -----------------------------------------------------------------------------

class TestAborts:
    def test_frase_vazia_aborta_sem_consumir_creditos(self):
        orc = OrcamentoFirecrawl(cap=20)
        fn_search = MagicMock()
        fn_persistir = MagicMock(return_value=0)
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: FraseBusca(texto="", componentes={}),
            fn_search=fn_search,
            fn_persistir=fn_persistir,
        )
        assert isinstance(r, ResultadoPipeline)
        assert r.estatisticas.abortado is True
        assert r.estatisticas.motivo_aborto == "frase_vazia"
        assert r.linhas_persistidas == ()
        assert orc.gasto == 0
        fn_search.assert_not_called()
        fn_persistir.assert_not_called()

    def test_busca_nao_executada_propaga_motivo(self):
        orc = OrcamentoFirecrawl(cap=20)
        fn_search = MagicMock(return_value=ResultadoBusca(
            executada=False,
            motivo_nao_executada="orcamento_insuficiente",
        ))
        fn_scrape = MagicMock()
        fn_persistir = MagicMock(return_value=0)
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=fn_search,
            fn_scrape=fn_scrape,
            fn_persistir=fn_persistir,
        )
        assert r.estatisticas.abortado is True
        assert r.estatisticas.motivo_aborto.startswith("busca_nao_executada:")
        fn_scrape.assert_not_called()
        fn_persistir.assert_not_called()


# -----------------------------------------------------------------------------
# Fluxo completo: search → scrape → filtro → extract → validar → persistir
# -----------------------------------------------------------------------------

class TestFluxoCompleto:
    def test_pipeline_feliz_persiste_apenas_aprovados(self):
        """Cenário: 2 URLs, 2 scrapes, 2 cards por página = 4 cards;
        1 deles fica em São Bernardo (reprovado), 3 em Pindamonhangaba."""
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        url2 = "https://www.vivareal.com.br/imovel/2/"
        cards_por_url = {
            url1: [
                _card(url=f"{url1}#a", valor=350_000),
                _card(url=f"{url1}#b", valor=400_000),
            ],
            url2: [
                _card(url=f"{url2}#a", valor=380_000),  # reprovado
                _card(url=f"{url2}#b", valor=420_000),
            ],
        }
        # 1 reprovação (segundo url2#a)
        seq_validacoes = iter([
            _val_ok(),         # url1#a → ok
            _val_ok(),         # url1#b → ok
            _val_reprovado(),  # url2#a → reprovado (foi para SBC)
            _val_ok(),         # url2#b → ok
        ])

        fn_search = MagicMock(return_value=_busca_ok((url1, url2)))
        fn_scrape = MagicMock(side_effect=lambda u, **kw: _scrape_ok(u))
        fn_filtro = MagicMock(return_value=_filtro())
        # Simplificado: associamos extrair_cards ao url visto no scrape via closure
        chamadas_extracao = {"i": 0}
        ordem_urls = [url1, url2]

        def extrai(md, **kw):
            u = ordem_urls[chamadas_extracao["i"]]
            chamadas_extracao["i"] += 1
            return cards_por_url[u]

        fn_extrai = MagicMock(side_effect=extrai)
        fn_valida = MagicMock(side_effect=lambda **kw: next(seq_validacoes))
        fn_persistir = MagicMock(return_value=3)

        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=fn_search,
            fn_scrape=fn_scrape,
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=fn_valida,
            fn_persistir=fn_persistir,
        )
        s = r.estatisticas
        assert s.abortado is False
        assert s.urls_busca == 2
        assert s.urls_aceites_busca == 2
        assert s.paginas_scrapadas == 2
        assert s.paginas_filtro_rejeitado == 0
        assert s.cards_extraidos == 4
        assert s.cards_aprovados_validacao == 3
        assert s.cards_descartados_validacao == 1
        assert s.motivos_descarte_validacao.get("municipio_diferente") == 1
        assert s.persistidos == 3

        # 3 linhas a persistir (não 4)
        assert len(r.linhas_persistidas) == 3
        # Todas com cidade do reverse-geocode
        for l in r.linhas_persistidas:
            assert l.cidade == "Pindamonhangaba"
            assert l.estado == "SP"
            assert l.tipo_imovel == "apartamento"

        # Persistir foi chamado uma única vez
        fn_persistir.assert_called_once()

    def test_pindamonhangaba_para_sao_bernardo_zero_persistidos(self):
        """Regressão dura do bug original: TODOS os cards extraídos do
        markdown caem em SBC após reverse-geocode → nada persiste."""
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        cards = [_card(url=f"{url1}#x"), _card(url=f"{url1}#y")]
        fn_search = MagicMock(return_value=_busca_ok((url1,)))
        fn_scrape = MagicMock(return_value=_scrape_ok(url1))
        fn_filtro = MagicMock(return_value=_filtro())
        fn_extrai = MagicMock(return_value=cards)
        fn_valida = MagicMock(return_value=_val_reprovado())
        fn_persistir = MagicMock(return_value=0)

        r = executar_pipeline(
            _leilao(cidade="Pindamonhangaba"),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=fn_search,
            fn_scrape=fn_scrape,
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=fn_valida,
            fn_persistir=fn_persistir,
        )
        assert r.linhas_persistidas == ()
        assert r.estatisticas.cards_extraidos == 2
        assert r.estatisticas.cards_aprovados_validacao == 0
        assert r.estatisticas.cards_descartados_validacao == 2
        # NÃO deve ter chamado persistir (lista vazia)
        fn_persistir.assert_not_called()


# -----------------------------------------------------------------------------
# Filtro de página rejeita
# -----------------------------------------------------------------------------

class TestFiltroPaginaRejeita:
    def test_paginas_rejeitadas_pulam_extracao(self):
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        fn_search = MagicMock(return_value=_busca_ok((url1,)))
        fn_scrape = MagicMock(return_value=_scrape_ok(url1))
        fn_filtro = MagicMock(return_value=_filtro(
            status=StatusPagina.REJEITADA,
            motivo="cidade_ausente",
            concorrentes=("São Paulo",),
        ))
        fn_extrai = MagicMock()
        fn_valida = MagicMock()
        fn_persistir = MagicMock()
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=fn_search,
            fn_scrape=fn_scrape,
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=fn_valida,
            fn_persistir=fn_persistir,
        )
        assert r.estatisticas.paginas_filtro_rejeitado == 1
        assert r.estatisticas.cards_extraidos == 0
        fn_extrai.assert_not_called()
        fn_valida.assert_not_called()
        fn_persistir.assert_not_called()

    def test_paginas_apenas_mencionada_continuam(self):
        """Status MENCIONADA NÃO é REJEITADA — pipeline continua."""
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        fn_filtro = MagicMock(return_value=_filtro(status=StatusPagina.MENCIONADA))
        fn_extrai = MagicMock(return_value=[_card(url=f"{url1}#a")])
        fn_valida = MagicMock(return_value=_val_ok())
        fn_persistir = MagicMock(return_value=1)
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_ok(url1)),
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=fn_valida,
            fn_persistir=fn_persistir,
        )
        assert r.estatisticas.paginas_filtro_rejeitado == 0
        assert r.estatisticas.cards_aprovados_validacao == 1
        fn_extrai.assert_called_once()


# -----------------------------------------------------------------------------
# Orçamento esgotado a meio
# -----------------------------------------------------------------------------

class TestOrcamentoEsgotaMeio:
    def test_loop_para_quando_orcamento_acaba(self):
        """Cap=4: search custa 2 cr, sobram 2 → só 2 scrapes possíveis,
        mesmo que a busca tenha trazido 5 URLs. Pipeline interrompe limpo."""
        orc = OrcamentoFirecrawl(cap=4)
        urls = tuple(f"https://www.zapimoveis.com.br/imovel/{i}/" for i in range(5))
        # Busca: a função simula consumo via closure
        def fake_search(query, *, limit, orcamento, cliente=None):
            orcamento.consumir_search(limit=limit, query=query)
            return _busca_ok(urls)

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url)

        fn_filtro = MagicMock(return_value=_filtro())
        fn_extrai = MagicMock(return_value=[_card(url="https://www.zapimoveis.com.br/imovel/x/")])
        fn_valida = MagicMock(return_value=_val_ok())
        fn_persistir = MagicMock(return_value=2)
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=fake_search,
            fn_scrape=fake_scrape,
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=fn_valida,
            fn_persistir=fn_persistir,
        )
        assert orc.gasto <= orc.cap == 4
        assert r.estatisticas.paginas_scrapadas == 2  # 5 URLs, mas só 2 cabem
        assert r.estatisticas.creditos_gastos == 4

    def test_scrape_falhou_nao_quebra_loop(self):
        orc = OrcamentoFirecrawl(cap=20)
        urls = ("https://www.zapimoveis.com.br/imovel/1/", "https://www.zapimoveis.com.br/imovel/2/")
        def scrape(u, *, orcamento, cliente=None):
            if "1" in u:
                return _scrape_falhou(u, motivo="vazio")
            orcamento.consumir_scrape(url=u)
            return _scrape_ok(u)
        fn_persistir = MagicMock(return_value=1)
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok(urls)),
            fn_scrape=scrape,
            fn_filtro_pagina=MagicMock(return_value=_filtro()),
            fn_extrai_cards=MagicMock(return_value=[_card(url="https://www.zapimoveis.com.br/imovel/2/#a")]),
            fn_valida_municipio=MagicMock(return_value=_val_ok()),
            fn_persistir=fn_persistir,
        )
        assert r.estatisticas.paginas_scrapadas == 1
        assert r.estatisticas.cards_aprovados_validacao == 1


# -----------------------------------------------------------------------------
# Cache hits são contabilizados
# -----------------------------------------------------------------------------

class TestCacheHits:
    def test_cache_hits_contabilizados(self):
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        fn_persistir = MagicMock(return_value=1)
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_cache(url1)),
            fn_filtro_pagina=MagicMock(return_value=_filtro()),
            fn_extrai_cards=MagicMock(return_value=[_card(url=f"{url1}#a")]),
            fn_valida_municipio=MagicMock(return_value=_val_ok()),
            fn_persistir=fn_persistir,
        )
        assert r.estatisticas.paginas_scrapadas == 1
        assert r.estatisticas.paginas_cache_hit == 1


# -----------------------------------------------------------------------------
# Modo dry-run (persistir=False)
# -----------------------------------------------------------------------------

class TestDryRun:
    def test_persistir_false_calcula_mas_nao_persiste(self):
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        fn_persistir = MagicMock()
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=None,  # ok porque persistir=False
            persistir=False,
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_ok(url1)),
            fn_filtro_pagina=MagicMock(return_value=_filtro()),
            fn_extrai_cards=MagicMock(return_value=[_card(url=f"{url1}#a")]),
            fn_valida_municipio=MagicMock(return_value=_val_ok()),
            fn_persistir=fn_persistir,
        )
        fn_persistir.assert_not_called()
        assert len(r.linhas_persistidas) == 1
        assert r.estatisticas.persistidos == 0

    def test_persistir_true_sem_client_levanta(self):
        with pytest.raises(ValueError, match="supabase_client"):
            executar_pipeline(
                _leilao(),
                orcamento=OrcamentoFirecrawl(cap=20),
                supabase_client=None,
                persistir=True,
            )


# -----------------------------------------------------------------------------
# Persistência tolera exceções (não derruba o pipeline)
# -----------------------------------------------------------------------------

class TestPersistenciaResiliente:
    def test_excecao_em_persistencia_nao_quebra_pipeline(self):
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        fn_persistir = MagicMock(side_effect=RuntimeError("supabase down"))
        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_ok(url1)),
            fn_filtro_pagina=MagicMock(return_value=_filtro()),
            fn_extrai_cards=MagicMock(return_value=[_card(url=f"{url1}#a")]),
            fn_valida_municipio=MagicMock(return_value=_val_ok()),
            fn_persistir=fn_persistir,
        )
        # Mesmo com persistir falhando, pipeline devolve resultado coerente
        assert r.estatisticas.persistidos == 0
        assert r.estatisticas.cards_aprovados_validacao == 1
        assert len(r.linhas_persistidas) == 1


# -----------------------------------------------------------------------------
# cidades_concorrentes propaga ao filtro
# -----------------------------------------------------------------------------

class TestCidadesConcorrentes:
    def test_lista_passada_ao_filtro(self):
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        fn_filtro = MagicMock(return_value=_filtro())
        executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            cidades_conhecidas=["São Paulo", "São Bernardo do Campo"],
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_ok(url1)),
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=MagicMock(return_value=[]),
            fn_valida_municipio=MagicMock(),
            fn_persistir=MagicMock(return_value=0),
        )
        # Verifica que cidades_conhecidas foi propagado
        kwargs = fn_filtro.call_args.kwargs
        assert kwargs["cidade_alvo"] == "Pindamonhangaba"
        assert kwargs["cidades_conhecidas"] == ["São Paulo", "São Bernardo do Campo"]


# -----------------------------------------------------------------------------
# Propagação de cidade_alvo / cidade_no_markdown / pagina_confirmada
# -----------------------------------------------------------------------------

class TestPropagacaoSinaisCidade:
    def test_cidade_alvo_passada_ao_extrator(self):
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        fn_extrai = MagicMock(return_value=[])
        executar_pipeline(
            _leilao(cidade="Pindamonhangaba"),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_ok(url1)),
            fn_filtro_pagina=MagicMock(return_value=_filtro()),
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=MagicMock(),
            fn_persistir=MagicMock(return_value=0),
        )
        # cidade_alvo é passada como kwarg
        kwargs = fn_extrai.call_args.kwargs
        assert kwargs["cidade_alvo"] == "Pindamonhangaba"

    def test_cidade_no_markdown_e_pagina_confirmada_propagados(self):
        """Pipeline encaminha (a) cidade_no_markdown do card e
        (b) pagina_confirmada do filtro à validação."""
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        card = _card(url=f"{url1}#a", cidade_no_markdown="Pindamonhangaba")
        fn_valida = MagicMock(return_value=_val_ok())
        executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_ok(url1)),
            fn_filtro_pagina=MagicMock(return_value=_filtro(status=StatusPagina.CONFIRMADA)),
            fn_extrai_cards=MagicMock(return_value=[card]),
            fn_valida_municipio=fn_valida,
            fn_persistir=MagicMock(return_value=1),
        )
        kwargs = fn_valida.call_args.kwargs
        assert kwargs["cidade_no_markdown"] == "Pindamonhangaba"
        assert kwargs["pagina_confirmada"] is True
        assert kwargs["cidade_alvo"] == "Pindamonhangaba"

    def test_pagina_mencionada_NAO_eh_confirmada(self):
        """Status MENCIONADA não dá rescue (apenas CONFIRMADA dá)."""
        orc = OrcamentoFirecrawl(cap=20)
        url1 = "https://www.zapimoveis.com.br/imovel/1/"
        card = _card(url=f"{url1}#a")
        fn_valida = MagicMock(return_value=_val_ok())
        executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=MagicMock(return_value=_busca_ok((url1,))),
            fn_scrape=MagicMock(return_value=_scrape_ok(url1)),
            fn_filtro_pagina=MagicMock(return_value=_filtro(status=StatusPagina.MENCIONADA)),
            fn_extrai_cards=MagicMock(return_value=[card]),
            fn_valida_municipio=fn_valida,
            fn_persistir=MagicMock(return_value=1),
        )
        kwargs = fn_valida.call_args.kwargs
        assert kwargs["pagina_confirmada"] is False


# -----------------------------------------------------------------------------
# Resumo serializável
# -----------------------------------------------------------------------------

class TestResumo:
    def test_resumo_e_dict_com_todas_as_chaves(self):
        s = EstatisticasPipeline(
            frase_busca="x",
            urls_busca=2,
            cards_extraidos=4,
            cards_aprovados_validacao=2,
            persistidos=2,
            creditos_gastos=4,
            creditos_cap=20,
        )
        d = s.resumo()
        for k in (
            "frase_busca", "urls_busca", "urls_aceites_busca", "urls_descartadas_busca",
            "paginas_scrapadas", "paginas_cache_hit", "paginas_filtro_rejeitado",
            "cards_extraidos", "cards_aprovados_validacao", "cards_descartados_validacao",
            "motivos_descarte_validacao", "persistidos", "creditos_gastos",
            "creditos_cap", "abortado", "motivo_aborto",
        ):
            assert k in d


# -----------------------------------------------------------------------------
# B5 — Early exit do loop de scrape
# -----------------------------------------------------------------------------

class TestEarlyExitScrape:
    def test_para_loop_quando_atinge_threshold(self):
        """Com 5 URLs e cap_persistir=2 (threshold=12), gera 12+ aprovados:
        deve parar antes de processar todas as URLs."""
        orc = OrcamentoFirecrawl(cap=200)
        urls = tuple(f"https://www.zapimoveis.com.br/imovel/{i}/" for i in range(5))
        # Cada URL produz 6 cards aprovados (5*6=30 total).
        cards_por_url = {
            u: [_card(url=f"{u}#{i}") for i in range(6)] for u in urls
        }

        fn_search = MagicMock(return_value=_busca_ok(urls))
        fn_scrape = MagicMock(side_effect=lambda u, **kw: _scrape_ok(u))
        fn_filtro = MagicMock(return_value=_filtro())
        idx = {"i": 0}

        def extrai(md, **kw):
            u = urls[idx["i"]]
            idx["i"] += 1
            return cards_por_url[u]

        fn_extrai = MagicMock(side_effect=extrai)
        fn_valida = MagicMock(return_value=_val_ok())
        fn_persistir = MagicMock(return_value=2)

        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=fn_search,
            fn_scrape=fn_scrape,
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=fn_valida,
            fn_persistir=fn_persistir,
            max_persistir_por_ingestao=2,  # threshold = max(12, 2*2) = 12
        )
        s = r.estatisticas
        # Threshold = 12; cada URL gera 6 → 2 URLs (12) atinge, 3a nem deveria
        # ser scrapeada. A checagem é feita no início da iteração — então
        # após scrapar a 2a URL, ao iniciar a 3a o break acontece.
        # NÃO devemos ter scrapeado as 5 URLs.
        assert s.paginas_scrapadas < 5
        # cards_extraidos reflete o que o loop processou (antes do cap).
        assert s.cards_extraidos >= 12

    def test_nao_para_quando_nao_atinge_threshold(self):
        """Cards insuficientes → processa todas as URLs."""
        orc = OrcamentoFirecrawl(cap=200)
        urls = tuple(f"https://www.zapimoveis.com.br/imovel/{i}/" for i in range(3))
        cards_por_url = {
            u: [_card(url=f"{u}#a")] for u in urls  # apenas 1 card por URL
        }

        fn_search = MagicMock(return_value=_busca_ok(urls))
        fn_scrape = MagicMock(side_effect=lambda u, **kw: _scrape_ok(u))
        fn_filtro = MagicMock(return_value=_filtro())
        idx = {"i": 0}

        def extrai(md, **kw):
            u = urls[idx["i"]]
            idx["i"] += 1
            return cards_por_url[u]

        fn_extrai = MagicMock(side_effect=extrai)
        fn_valida = MagicMock(return_value=_val_ok())
        fn_persistir = MagicMock(return_value=3)

        r = executar_pipeline(
            _leilao(),
            orcamento=orc,
            supabase_client=object(),
            fn_montar_frase=lambda **kw: _frase(),
            fn_search=fn_search,
            fn_scrape=fn_scrape,
            fn_filtro_pagina=fn_filtro,
            fn_extrai_cards=fn_extrai,
            fn_valida_municipio=fn_valida,
            fn_persistir=fn_persistir,
            max_persistir_por_ingestao=10,
        )
        # 3 URLs = 3 cards, < threshold 12 → todas as URLs processadas
        assert r.estatisticas.paginas_scrapadas == 3
