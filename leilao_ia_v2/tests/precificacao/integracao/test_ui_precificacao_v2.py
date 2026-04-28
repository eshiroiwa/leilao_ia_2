"""
Testes dos helpers puros de :mod:`leilao_ia_v2.ui.precificacao_v2`.

Não cobrimos a renderização Streamlit (que é apenas casca em cima dos
helpers), apenas o que é determinístico e testável: extração do payload,
formatação BRL/percentagem, paleta de cores, montagem dos blocos HTML,
linhas de meta-info, e o helper de refresh do session_state.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from leilao_ia_v2.precificacao.integracao.persistencia import METADADO_KEY
from leilao_ia_v2.ui.precificacao_v2 import (
    _refrescar_session_state_apos_recalculo,
    cor_confianca,
    cor_liquidez,
    cor_veredito,
    extrair_precificacao_v2,
    formatar_brl,
    formatar_pct,
    montar_html_alerta_liquidez,
    montar_html_resumo,
    montar_linhas_meta,
)


# -----------------------------------------------------------------------------
# extrair_precificacao_v2
# -----------------------------------------------------------------------------


class TestExtrairPrecificacaoV2:
    def test_row_invalida_devolve_none(self):
        assert extrair_precificacao_v2(None) is None  # type: ignore[arg-type]
        assert extrair_precificacao_v2("ola") is None  # type: ignore[arg-type]
        assert extrair_precificacao_v2(42) is None  # type: ignore[arg-type]

    def test_sem_leilao_extra_json(self):
        assert extrair_precificacao_v2({"id": "x"}) is None

    def test_leilao_extra_json_invalido(self):
        assert extrair_precificacao_v2({"leilao_extra_json": "nao-dict"}) is None

    def test_sem_chave_precificacao_v2(self):
        assert extrair_precificacao_v2({"leilao_extra_json": {"outra": 1}}) is None

    def test_chave_precificacao_invalida(self):
        row = {"leilao_extra_json": {METADADO_KEY: "nao-dict"}}
        assert extrair_precificacao_v2(row) is None

    def test_devolve_dict_quando_presente(self):
        payload = {"valor_estimado": 123, "veredito": {"nivel": "FORTE"}}
        row = {"leilao_extra_json": {METADADO_KEY: payload}}
        assert extrair_precificacao_v2(row) == payload


# -----------------------------------------------------------------------------
# formatar_brl / formatar_pct
# -----------------------------------------------------------------------------


class TestFormatarBrl:
    @pytest.mark.parametrize("v", [None, "", "abc", float("nan")])
    def test_invalido_em_traco(self, v):
        assert formatar_brl(v) == "—"

    def test_inteiro(self):
        assert formatar_brl(1234) == "R$ 1.234,00"

    def test_milhares(self):
        assert formatar_brl(1234567.89) == "R$ 1.234.567,89"

    def test_negativo(self):
        assert formatar_brl(-50.5) == "-R$ 50,50"

    def test_sufixo(self):
        assert formatar_brl(3500, sufixo="/m²") == "R$ 3.500,00/m²"

    def test_arredondamento_centavos(self):
        # 0.005 -> 0.01 (banker's? só queremos que não exploda)
        s = formatar_brl(0.005)
        assert s.startswith("R$ 0,") and s.endswith(("00", "01"))


class TestFormatarPct:
    def test_traco_para_invalido(self):
        assert formatar_pct(None) == "—"
        assert formatar_pct("") == "—"
        assert formatar_pct("xx") == "—"
        assert formatar_pct(float("nan")) == "—"

    def test_default_uma_casa(self):
        assert formatar_pct(43.0) == "43.0%"

    def test_casas_personalizadas(self):
        assert formatar_pct(12.345, casas=2) == "12.35%"

    def test_zero(self):
        assert formatar_pct(0) == "0.0%"


# -----------------------------------------------------------------------------
# Paleta de cores
# -----------------------------------------------------------------------------


class TestPaletaCores:
    def test_cor_veredito_conhecido(self):
        txt, bg = cor_veredito("FORTE")
        assert txt and bg

    def test_cor_veredito_case_insensitive(self):
        a = cor_veredito("forte")
        b = cor_veredito("FORTE")
        assert a == b

    def test_cor_veredito_desconhecido_devolve_neutro(self):
        # Tem que devolver alguma cor (não levantar)
        txt, bg = cor_veredito("XYZ")
        assert txt and bg

    def test_cor_confianca(self):
        for nivel in ["ALTA", "MEDIA", "BAIXA", "INSUFICIENTE", "?"]:
            txt, bg = cor_confianca(nivel)
            assert txt and bg

    def test_cor_liquidez(self):
        for sev in ["ok", "media", "alta", "?"]:
            txt, bg = cor_liquidez(sev)
            assert txt and bg

    def test_cor_liquidez_case_insensitive(self):
        assert cor_liquidez("MEDIA") == cor_liquidez("media")


# -----------------------------------------------------------------------------
# montar_html_resumo
# -----------------------------------------------------------------------------


def _payload_basico(**over):
    base = {
        "valor_estimado": 350_000.0,
        "p20_total": 320_000.0,
        "p80_total": 380_000.0,
        "veredito": {"nivel": "FORTE", "descricao": "Lance < P20"},
        "confianca": {"nivel": "ALTA", "motivo": "n=12, CV=8%"},
        "estatistica": {
            "n_uteis": 12,
            "n_descartados_outlier": 1,
            "cv_pct": 8.0,
            "mediana_r_m2": 4500.0,
        },
        "expansao": {
            "raio_final_m": 1500,
            "niveis_expansao_aplicados": 1,
            "tipo_relax_aplicado": False,
        },
        "amostras": [],
    }
    base.update(over)
    return base


class TestMontarHtmlResumo:
    def test_inclui_valor_e_faixa(self):
        html = montar_html_resumo(_payload_basico())
        assert "R$ 350.000,00" in html
        assert "R$ 320.000,00 — R$ 380.000,00" in html

    def test_inclui_veredito_descricao_e_motivo(self):
        html = montar_html_resumo(_payload_basico())
        assert "Lance &lt; P20" in html or "Lance < P20" in html
        assert "n=12, CV=8%" in html

    def test_badges_substituem_underscore(self):
        html = montar_html_resumo(_payload_basico(veredito={"nivel": "SEM_LANCE"}))
        assert "SEM LANCE" in html

    def test_faixa_traco_quando_p_ausente(self):
        html = montar_html_resumo(_payload_basico(p20_total=None))
        assert "Faixa P20–P80: <b>—</b>" in html

    def test_aceita_dicts_vazios_sem_explodir(self):
        html = montar_html_resumo({"valor_estimado": None, "veredito": {}, "confianca": {}})
        assert "—" in html


# -----------------------------------------------------------------------------
# montar_html_alerta_liquidez
# -----------------------------------------------------------------------------


class TestMontarHtmlAlertaLiquidez:
    def test_ok_devolve_none(self):
        html = montar_html_alerta_liquidez({
            "alerta_liquidez": {"severidade": "ok", "mensagem": "Tudo bem", "razao_area": 1.0, "fator_aplicado": 1.0},
        })
        assert html is None

    def test_sem_alerta_devolve_none(self):
        assert montar_html_alerta_liquidez({}) is None

    def test_severidade_media_renderiza(self):
        html = montar_html_alerta_liquidez({
            "alerta_liquidez": {
                "severidade": "media",
                "mensagem": "Imóvel maior que padrão",
                "razao_area": 1.45,
                "fator_aplicado": 0.92,
            },
        })
        assert html is not None
        assert "Imóvel maior que padrão" in html
        assert "razão alvo/amostras = 1.45" in html
        assert "fator aplicado = 0.92" in html

    def test_severidade_alta_renderiza(self):
        html = montar_html_alerta_liquidez({
            "alerta_liquidez": {"severidade": "alta", "mensagem": "Liquidez muito ruim"}
        })
        assert html is not None
        assert "Liquidez muito ruim" in html

    def test_severidade_invalida_devolve_none(self):
        html = montar_html_alerta_liquidez({"alerta_liquidez": {"severidade": "?"}})
        assert html is None


# -----------------------------------------------------------------------------
# montar_linhas_meta
# -----------------------------------------------------------------------------


class TestMontarLinhasMeta:
    def test_tudo_presente(self):
        linhas = montar_linhas_meta(_payload_basico())
        joined = " | ".join(linhas)
        assert "Amostras úteis" in joined and "12" in joined
        assert "1 outlier descartados" in joined or "1 outliers descartados" in joined
        assert "Dispersão" in joined and "8.0%" in joined
        assert "Mediana R$/m²" in joined and "/m²" in joined
        assert "Raio final" in joined and "1500" in joined
        assert "1 expansão" in joined or "1 expansões" in joined

    def test_sem_outliers_omite_sufixo(self):
        p = _payload_basico()
        p["estatistica"]["n_descartados_outlier"] = 0
        linhas = montar_linhas_meta(p)
        joined = " | ".join(linhas)
        assert "outlier" not in joined

    def test_sem_expansao_oculta_linha(self):
        p = _payload_basico()
        p["expansao"] = {}
        linhas = montar_linhas_meta(p)
        joined = " | ".join(linhas)
        assert "Raio final" not in joined

    def test_payload_vazio(self):
        assert montar_linhas_meta({}) == []

    def test_tipo_relax_aparece(self):
        p = _payload_basico()
        p["expansao"]["tipo_relax_aplicado"] = True
        linhas = montar_linhas_meta(p)
        assert any("Tipos próximos" in l for l in linhas)


# -----------------------------------------------------------------------------
# _refrescar_session_state_apos_recalculo
#
# Cobre o bug "recalculei e nada mudou": após persistir, é preciso
# re-buscar a row do banco e atualizar `st.session_state['ultimo_extracao']`,
# senão a UI continua renderizando o `leilao_extra_json` em cache.
# -----------------------------------------------------------------------------


class TestRefrescarSessionState:
    def test_atualiza_session_state_quando_busca_devolve_dict(self):
        novo = {"id": "abc", "leilao_extra_json": {METADADO_KEY: {"v": 1}}}
        client = MagicMock()
        ss: dict = {"ultimo_extracao": {"id": "abc", "leilao_extra_json": {}}}
        with patch("leilao_ia_v2.ui.precificacao_v2.st") as mock_st, \
             patch("leilao_ia_v2.persistence.leilao_imoveis_repo.buscar_por_id", return_value=novo) as mock_busca:
            mock_st.session_state = ss
            _refrescar_session_state_apos_recalculo(client, "abc")
            mock_busca.assert_called_once_with("abc", client)
            assert ss["ultimo_extracao"] == novo

    def test_busca_devolve_none_nao_quebra_e_nao_atualiza(self):
        client = MagicMock()
        ss: dict = {"ultimo_extracao": {"id": "abc", "marker": "old"}}
        with patch("leilao_ia_v2.ui.precificacao_v2.st") as mock_st, \
             patch("leilao_ia_v2.persistence.leilao_imoveis_repo.buscar_por_id", return_value=None):
            mock_st.session_state = ss
            _refrescar_session_state_apos_recalculo(client, "abc")
            # session_state preservado
            assert ss["ultimo_extracao"]["marker"] == "old"

    def test_busca_levanta_e_funcao_engole_excecao(self):
        client = MagicMock()
        ss: dict = {"ultimo_extracao": {"id": "abc", "marker": "old"}}
        with patch("leilao_ia_v2.ui.precificacao_v2.st") as mock_st, \
             patch(
                 "leilao_ia_v2.persistence.leilao_imoveis_repo.buscar_por_id",
                 side_effect=RuntimeError("boom"),
             ):
            mock_st.session_state = ss
            # Não deve levantar
            _refrescar_session_state_apos_recalculo(client, "abc")
            assert ss["ultimo_extracao"]["marker"] == "old"
