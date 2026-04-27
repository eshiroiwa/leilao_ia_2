"""Testes do pré-filtro textual de páginas (PR2)."""

from __future__ import annotations

import pytest

from leilao_ia_v2.comparaveis.pagina_filtro import (
    ResultadoFiltroPagina,
    StatusPagina,
    avaliar_pagina,
)


class TestStatusBasicos:
    def test_cidade_alvo_vazia_rejeitada(self):
        r = avaliar_pagina("# qualquer coisa", cidade_alvo="")
        assert r.status == StatusPagina.REJEITADA
        assert r.motivo == "cidade_alvo_vazia"
        assert not r.deve_extrair

    def test_markdown_vazio_rejeitado(self):
        r = avaliar_pagina("", cidade_alvo="Pindamonhangaba")
        assert r.status == StatusPagina.REJEITADA
        assert r.motivo == "markdown_vazio"

    def test_markdown_apenas_espacos_rejeitado(self):
        r = avaliar_pagina("   \n  \n  ", cidade_alvo="Pindamonhangaba")
        assert r.status == StatusPagina.REJEITADA


class TestCidadeEmH1:
    def test_h1_contem_cidade(self):
        md = "# Apartamentos à venda em Pindamonhangaba\n\nLista de imóveis..."
        r = avaliar_pagina(md, cidade_alvo="Pindamonhangaba")
        assert r.status == StatusPagina.CONFIRMADA
        assert r.confianca_alta
        assert any("Pindamonhangaba" in p for p in r.posicoes_privilegiadas)

    def test_h2_contem_cidade(self):
        md = "## Imóveis à venda em São Paulo SP"
        r = avaliar_pagina(md, cidade_alvo="São Paulo")
        assert r.status == StatusPagina.CONFIRMADA

    def test_acentuacao_indiferente(self):
        md = "# Casas em SAO PAULO"
        r = avaliar_pagina(md, cidade_alvo="São Paulo")
        assert r.status == StatusPagina.CONFIRMADA

    def test_cidade_composta_em_h1(self):
        md = "# Apartamentos em São Bernardo do Campo"
        r = avaliar_pagina(md, cidade_alvo="São Bernardo do Campo")
        assert r.status == StatusPagina.CONFIRMADA


class TestCidadeEmBreadcrumb:
    def test_breadcrumb_aceito(self):
        md = "Home > SP > Pindamonhangaba > Imóveis\n\n# Listagem"
        r = avaliar_pagina(md, cidade_alvo="Pindamonhangaba")
        assert r.status == StatusPagina.CONFIRMADA

    def test_breadcrumb_com_seta_unicode(self):
        md = "Início › São Paulo › Vila Mariana\n\nLorem"
        r = avaliar_pagina(md, cidade_alvo="São Paulo")
        assert r.status == StatusPagina.CONFIRMADA


class TestCidadeEmMeta:
    def test_canonical_url_aceita(self):
        md = "canonical: https://example.com/pindamonhangaba/casas\n\n# Página"
        r = avaliar_pagina(md, cidade_alvo="Pindamonhangaba")
        assert r.status == StatusPagina.CONFIRMADA

    def test_og_locality_aceito(self):
        md = "og:locality: Taubaté\n\n# título"
        r = avaliar_pagina(md, cidade_alvo="Taubaté")
        assert r.status == StatusPagina.CONFIRMADA


class TestCidadeNoCorpoApenas:
    def test_so_no_corpo_devolve_mencionada(self):
        md = (
            "# Casas à venda\n\n"
            "Esta página tem várias casas em Pindamonhangaba e arredores."
        )
        r = avaliar_pagina(md, cidade_alvo="Pindamonhangaba")
        assert r.status == StatusPagina.MENCIONADA
        assert r.deve_extrair  # ainda vale tentar extrair, com validação posterior
        assert not r.confianca_alta


class TestRejeicaoQuandoAusente:
    def test_cidade_inexistente_rejeitada(self):
        md = "# Apartamentos em São Paulo\n\nLista de imóveis em São Paulo SP."
        r = avaliar_pagina(md, cidade_alvo="Pindamonhangaba")
        assert r.status == StatusPagina.REJEITADA
        assert r.motivo == "cidade_alvo_ausente"
        assert not r.deve_extrair


class TestCidadesConcorrentes:
    def test_concorrentes_detectadas(self):
        md = "# Imóveis em São Paulo\n\nE também em São Bernardo."
        r = avaliar_pagina(
            md,
            cidade_alvo="Pindamonhangaba",
            cidades_conhecidas=["São Paulo", "São Bernardo do Campo", "Pindamonhangaba"],
        )
        assert r.status == StatusPagina.REJEITADA
        # Detecta pelo menos uma das duas
        assert any(c in ("saopaulo", "saobernardodocampo") for c in r.cidades_concorrentes)

    def test_sem_lista_de_concorrentes_nao_detecta(self):
        md = "# Imóveis em São Paulo"
        r = avaliar_pagina(md, cidade_alvo="Pindamonhangaba")
        assert r.cidades_concorrentes == ()

    def test_concorrente_e_cidade_alvo_coexistem(self):
        md = "# Apartamentos em Pindamonhangaba e São Paulo"
        r = avaliar_pagina(
            md,
            cidade_alvo="Pindamonhangaba",
            cidades_conhecidas=["São Paulo", "Pindamonhangaba"],
        )
        # Cidade-alvo está presente → confirma. Concorrente registada para info.
        assert r.status == StatusPagina.CONFIRMADA
        assert "saopaulo" in r.cidades_concorrentes


class TestImutabilidade:
    def test_resultado_e_imutavel(self):
        r = avaliar_pagina("# x", cidade_alvo="Pindamonhangaba")
        assert isinstance(r, ResultadoFiltroPagina)
        with pytest.raises(Exception):
            r.status = StatusPagina.CONFIRMADA  # type: ignore[misc]
