"""Testes do router :mod:`comparaveis.integracao` (PR5).

Cobre flag on/off + mapeamento de :class:`EstatisticasPipeline` para o dict
consumido pelo pipeline de ingestão.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from leilao_ia_v2.comparaveis import integracao
from leilao_ia_v2.comparaveis.integracao import (
    FLAG_ENV,
    URL_LISTAGEM_V2,
    executar_comparaveis_pos_ingestao,
    flag_v2_ativa,
)
from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.persistencia import LinhaPersistir
from leilao_ia_v2.comparaveis.pipeline import (
    EstatisticasPipeline,
    LeilaoAlvo,
    ResultadoPipeline,
)
from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM


# -----------------------------------------------------------------------------
# Builders
# -----------------------------------------------------------------------------

def _extn(
    cidade="Pindamonhangaba",
    estado="SP",
    bairro="Centro",
    tipo="apartamento",
    area_util=65.0,
    area_total=None,
    url_leilao="https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnimovel=999",
) -> ExtracaoEditalLLM:
    return ExtracaoEditalLLM(
        url_leilao=url_leilao,
        cidade=cidade,
        estado=estado,
        bairro=bairro,
        tipo_imovel=tipo,
        area_util=area_util,
        area_total=area_total,
    )


def _stats(**overrides) -> EstatisticasPipeline:
    base = dict(
        frase_busca="apartamento 65 m² Centro Pindamonhangaba SP",
        urls_busca=2,
        urls_aceites_busca=2,
        paginas_scrapadas=2,
        paginas_cache_hit=0,
        paginas_filtro_rejeitado=0,
        cards_extraidos=4,
        cards_aprovados_validacao=3,
        cards_descartados_validacao=1,
        motivos_descarte_validacao={"municipio_diferente": 1},
        persistidos=3,
        creditos_gastos=4,
        creditos_cap=15,
    )
    base.update(overrides)
    return EstatisticasPipeline(**base)


def _resultado(stats: EstatisticasPipeline, n_linhas: int = 0) -> ResultadoPipeline:
    leilao = LeilaoAlvo(cidade="Pindamonhangaba", estado_uf="SP", tipo_imovel="apartamento")
    linhas = tuple(_linha_dummy(i) for i in range(n_linhas))
    return ResultadoPipeline(leilao=leilao, linhas_persistidas=linhas, estatisticas=stats)


def _linha_dummy(i: int) -> LinhaPersistir:
    return LinhaPersistir(
        url_anuncio=f"https://www.zapimoveis.com.br/imovel/{i}/",
        portal="zapimoveis.com.br",
        tipo_imovel="apartamento",
        bairro="Centro",
        cidade="Pindamonhangaba",
        estado="SP",
        valor_venda=350_000.0,
        area_construida_m2=65.0,
        transacao="venda",
        latitude=-22.92,
        longitude=-45.46,
        metadados_json={},
    )


# -----------------------------------------------------------------------------
# flag_v2_ativa
# -----------------------------------------------------------------------------

class TestFlag:
    def test_flag_off_por_default(self, monkeypatch):
        monkeypatch.delenv(FLAG_ENV, raising=False)
        assert flag_v2_ativa() is False

    def test_flag_off_string_vazia(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "")
        assert flag_v2_ativa() is False

    def test_flag_off_zero(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "0")
        assert flag_v2_ativa() is False

    def test_flag_off_false(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "false")
        assert flag_v2_ativa() is False

    @pytest.mark.parametrize("v", ["1", "true", "TRUE", "on", "yes", "sim", " 1 ", "Sim"])
    def test_flag_on_valores_aceites(self, monkeypatch, v):
        monkeypatch.setenv(FLAG_ENV, v)
        assert flag_v2_ativa() is True


# -----------------------------------------------------------------------------
# Router: flag off → delega ao antigo
# -----------------------------------------------------------------------------

class TestRouterFlagOff:
    def test_flag_off_delega_v1(self, monkeypatch):
        monkeypatch.delenv(FLAG_ENV, raising=False)
        client = object()
        extn = _extn()
        v1_mock = MagicMock(return_value={"ok": True, "anuncios_salvos": 7})
        with patch(
            "leilao_ia_v2.services.comparaveis_pos_ingestao.executar_comparaveis_apos_ingestao_leilao",
            v1_mock,
        ):
            r = executar_comparaveis_pos_ingestao(
                client,
                leilao_imovel_id="abc",
                extn=extn,
                ignorar_cache_firecrawl=True,
                max_chamadas_api_firecrawl=10,
            )
        assert r == {"ok": True, "anuncios_salvos": 7}
        v1_mock.assert_called_once()
        kwargs = v1_mock.call_args.kwargs
        assert kwargs["leilao_imovel_id"] == "abc"
        assert kwargs["extn"] is extn
        assert kwargs["ignorar_cache_firecrawl"] is True
        assert kwargs["max_chamadas_api_firecrawl"] == 10


# -----------------------------------------------------------------------------
# Router: flag on → executa v2
# -----------------------------------------------------------------------------

class TestRouterFlagOn:
    def test_flag_on_chama_executar_pipeline_com_args_corretos(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "1")
        monkeypatch.setenv("FIRECRAWL_API_KEY", "fake-key")
        client = object()
        extn = _extn()
        stats = _stats()
        resultado = _resultado(stats, n_linhas=3)
        fake = MagicMock(return_value=resultado)
        with patch(
            "leilao_ia_v2.comparaveis.integracao.executar_pipeline",
            fake,
        ), patch(
            "leilao_ia_v2.comparaveis.integracao._resolver_cap",
            return_value=15,
        ):
            r = executar_comparaveis_pos_ingestao(
                client,
                leilao_imovel_id="abc",
                extn=extn,
                max_chamadas_api_firecrawl=15,
            )
        fake.assert_called_once()
        args, kwargs = fake.call_args
        assert isinstance(args[0], LeilaoAlvo)
        assert args[0].cidade == "Pindamonhangaba"
        assert args[0].estado_uf == "SP"
        assert args[0].tipo_imovel == "apartamento"
        assert args[0].bairro == "Centro"
        assert args[0].area_m2 == 65.0
        assert isinstance(kwargs["orcamento"], OrcamentoFirecrawl)
        assert kwargs["orcamento"].cap == 15
        assert kwargs["supabase_client"] is client
        # E o dict de retorno é compatível com o formato do antigo
        assert r["ok"] is True
        assert r["anuncios_salvos"] == 3
        assert r["url_listagem"] == URL_LISTAGEM_V2

    def test_flag_on_sem_cidade_omitido(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "1")
        monkeypatch.setenv("FIRECRAWL_API_KEY", "fake-key")
        fake = MagicMock()
        with patch("leilao_ia_v2.comparaveis.integracao.executar_pipeline", fake):
            r = executar_comparaveis_pos_ingestao(
                object(),
                leilao_imovel_id="abc",
                extn=_extn(cidade=""),
            )
        assert r["omitido"] is True
        assert r["motivo"] == "sem_cidade_ou_estado"
        fake.assert_not_called()

    def test_flag_on_sem_estado_omitido(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "1")
        monkeypatch.setenv("FIRECRAWL_API_KEY", "fake-key")
        fake = MagicMock()
        with patch("leilao_ia_v2.comparaveis.integracao.executar_pipeline", fake):
            r = executar_comparaveis_pos_ingestao(
                object(),
                leilao_imovel_id="abc",
                extn=_extn(estado=""),
            )
        assert r["omitido"] is True
        assert r["motivo"] == "sem_cidade_ou_estado"
        fake.assert_not_called()

    def test_flag_on_sem_api_key_omitido(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "1")
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        fake = MagicMock()
        with patch("leilao_ia_v2.comparaveis.integracao.executar_pipeline", fake):
            r = executar_comparaveis_pos_ingestao(
                object(),
                leilao_imovel_id="abc",
                extn=_extn(),
            )
        assert r["omitido"] is True
        assert r["motivo"] == "FIRECRAWL_API_KEY_ausente"
        fake.assert_not_called()

    def test_flag_on_cap_zero_omitido(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "1")
        monkeypatch.setenv("FIRECRAWL_API_KEY", "fake-key")
        fake = MagicMock()
        with patch("leilao_ia_v2.comparaveis.integracao.executar_pipeline", fake):
            r = executar_comparaveis_pos_ingestao(
                object(),
                leilao_imovel_id="abc",
                extn=_extn(),
                max_chamadas_api_firecrawl=0,
            )
        assert r["omitido"] is True
        assert r["motivo"] == "firecrawl_orcamento_analise_esgotado"
        fake.assert_not_called()

    def test_flag_on_excecao_v2_nao_propaga(self, monkeypatch):
        monkeypatch.setenv(FLAG_ENV, "1")
        monkeypatch.setenv("FIRECRAWL_API_KEY", "fake-key")
        with patch(
            "leilao_ia_v2.comparaveis.integracao.executar_pipeline",
            side_effect=RuntimeError("boom"),
        ):
            r = executar_comparaveis_pos_ingestao(
                object(),
                leilao_imovel_id="abc",
                extn=_extn(),
                max_chamadas_api_firecrawl=15,
            )
        assert r["ok"] is False
        assert "comparaveis_v2_excecao_ver_log" in r["erro"]


# -----------------------------------------------------------------------------
# Mapeamento ResultadoPipeline → dict (para o pipeline de ingestão)
# -----------------------------------------------------------------------------

class TestMapeamento:
    def test_resultado_normal_mapeia_chaves_esperadas(self):
        stats = _stats()
        orc = OrcamentoFirecrawl(cap=15, gasto=4)
        d = integracao._resultado_para_dict(_resultado(stats, n_linhas=3), orc)
        for k in (
            "ok", "anuncios_salvos", "url_listagem", "n_geocodificados",
            "markdown_insuficiente", "firecrawl_chamadas_api",
            "diagnostico_firecrawl_search", "falha_por_filtros_persistencia",
        ):
            assert k in d
        assert d["ok"] is True
        assert d["anuncios_salvos"] == 3
        assert d["n_geocodificados"] == 3
        assert d["markdown_insuficiente"] is False
        assert d["url_listagem"] == URL_LISTAGEM_V2
        assert d["falha_por_filtros_persistencia"] is False

    def test_zero_persistidos_marca_markdown_insuficiente(self):
        stats = _stats(persistidos=0, cards_aprovados_validacao=0)
        d = integracao._resultado_para_dict(_resultado(stats), OrcamentoFirecrawl(cap=15))
        assert d["anuncios_salvos"] == 0
        assert d["markdown_insuficiente"] is True

    def test_cards_extraidos_mas_todos_descartados_marca_falha_por_filtros(self):
        """Cenário Pindamonhangaba → SBC: 4 cards, todos reprovados na validação."""
        stats = _stats(
            persistidos=0,
            cards_extraidos=4,
            cards_aprovados_validacao=0,
            cards_descartados_validacao=4,
            motivos_descarte_validacao={"municipio_diferente": 4},
        )
        d = integracao._resultado_para_dict(_resultado(stats), OrcamentoFirecrawl(cap=15))
        assert d["anuncios_salvos"] == 0
        assert d["falha_por_filtros_persistencia"] is True

    def test_aborto_devolve_omitido_com_motivo(self):
        stats = _stats(
            abortado=True,
            motivo_aborto="frase_vazia",
            urls_busca=0,
            urls_aceites_busca=0,
            paginas_scrapadas=0,
            cards_extraidos=0,
            cards_aprovados_validacao=0,
            cards_descartados_validacao=0,
            persistidos=0,
        )
        d = integracao._resultado_para_dict(_resultado(stats), OrcamentoFirecrawl(cap=15))
        assert d["omitido"] is True
        assert d["motivo"] == "frase_vazia"
        assert d["anuncios_salvos"] == 0
        assert d["url_listagem"] == URL_LISTAGEM_V2

    def test_chamadas_api_descontam_cache_hits(self):
        stats = _stats(paginas_scrapadas=5, paginas_cache_hit=3, urls_busca=10)
        d = integracao._resultado_para_dict(_resultado(stats), OrcamentoFirecrawl(cap=15))
        # 1 search + (5 - 3) scrapes pagos = 3
        assert d["firecrawl_chamadas_api"] == 3

    def test_chamadas_api_zero_quando_nao_houve_busca(self):
        stats = _stats(
            urls_busca=0, urls_aceites_busca=0,
            paginas_scrapadas=0, paginas_cache_hit=0,
            cards_extraidos=0, cards_aprovados_validacao=0,
            cards_descartados_validacao=0, persistidos=0,
        )
        d = integracao._resultado_para_dict(_resultado(stats), OrcamentoFirecrawl(cap=15))
        assert d["firecrawl_chamadas_api"] == 0

    def test_diagnostico_inclui_metricas_chave(self):
        stats = _stats()
        d = integracao._resultado_para_dict(_resultado(stats), OrcamentoFirecrawl(cap=15, gasto=4))
        diag = d["diagnostico_firecrawl_search"]
        assert "v2" in diag
        assert "persistidos=3" in diag
        assert "cards=4" in diag
        assert "creditos=4/15" in diag
        assert "municipio_diferente=1" in diag


# -----------------------------------------------------------------------------
# Helpers internos
# -----------------------------------------------------------------------------

class TestResolverCap:
    def test_arg_explicito_tem_prioridade(self):
        assert integracao._resolver_cap(7) == 7

    def test_arg_negativo_clampado_a_zero(self):
        assert integracao._resolver_cap(-3) == 0

    def test_arg_invalido_devolve_zero(self):
        assert integracao._resolver_cap("xyz") == 0

    def test_arg_none_usa_parametro_default(self):
        with patch("leilao_ia_v2.config.busca_mercado_parametros.get_busca_mercado_parametros") as m:
            m.return_value.max_firecrawl_creditos_analise = 12
            assert integracao._resolver_cap(None) == 12

    def test_arg_none_e_parametro_indisponivel_devolve_15(self):
        with patch(
            "leilao_ia_v2.config.busca_mercado_parametros.get_busca_mercado_parametros",
            side_effect=RuntimeError("config off"),
        ):
            assert integracao._resolver_cap(None) == 15


class TestAreaReferencia:
    def test_prefere_area_util_sobre_area_total(self):
        a = integracao._area_referencia(_extn(area_util=70.0, area_total=120.0))
        assert a == 70.0

    def test_usa_area_total_se_util_zero(self):
        a = integracao._area_referencia(_extn(area_util=0, area_total=120.0))
        assert a == 120.0

    def test_devolve_zero_se_ambas_ausentes(self):
        a = integracao._area_referencia(_extn(area_util=None, area_total=None))
        assert a == 0.0

    def test_devolve_zero_se_ambas_zero(self):
        a = integracao._area_referencia(_extn(area_util=0, area_total=0))
        assert a == 0.0
