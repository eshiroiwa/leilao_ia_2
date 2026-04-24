"""Indicadores desnormalizados a partir de ``operacao_simulacao_json``."""

from leilao_ia_v2.persistence.leilao_imoveis_repo import (
    indicadores_de_operacao_simulacao_json,
    leilao_tem_indicadores_simulacao_gravados,
    leilao_tem_simulacao_utilizador_gravada,
)


def test_indicadores_de_json_alinham_outputs():
    oj = {
        "outputs": {
            "valor_venda_estimado": 157100.0,
            "reforma_brl": 10000.0,
            "lucro_bruto": 52484.83,
            "lucro_liquido": 44612.11,
            "roi_bruto": 0.551374,
            "roi_liquido": 0.468668,
            "lance_maximo_para_roi_desejado": 80408.4,
        }
    }
    ind = indicadores_de_operacao_simulacao_json(oj)
    assert ind["valor_mercado_estimado"] == 157100.0
    assert ind["custo_reforma_estimado"] == 10000.0
    assert ind["lucro_bruto_projetado"] == 52484.83
    assert ind["lucro_liquido_projetado"] == 44612.11
    assert ind["roi_projetado"] == 0.551374
    assert ind["roi_liquido_projetado"] == 0.468668
    assert ind["lance_maximo_recomendado"] == 80408.4


def test_indicadores_vazio_se_sem_outputs():
    assert indicadores_de_operacao_simulacao_json({}) == {}
    assert indicadores_de_operacao_simulacao_json({"outputs": {}}) == {}


def test_leilao_tem_indicadores_gravados():
    assert leilao_tem_indicadores_simulacao_gravados(None) is False
    assert leilao_tem_indicadores_simulacao_gravados({}) is False
    assert leilao_tem_indicadores_simulacao_gravados({"outputs": {}}) is False
    assert leilao_tem_indicadores_simulacao_gravados({"outputs": {"valor_venda_estimado": 1.0}}) is True
    assert leilao_tem_indicadores_simulacao_gravados({"outputs": {"lucro_bruto": 0.0}}) is True


def test_nao_ha_sim_gravada_so_com_inputs_ou_json_minimo():
    # Só "inputs" (defaults) ou bundle sem outputs → ideia inicial continua a ser pós-cache.
    r1 = {
        "operacao_simulacao_json": {
            "versao": 1,
            "inputs": {"lance_brl": 1000.0},
        }
    }
    assert leilao_tem_simulacao_utilizador_gravada(r1) is False
    r2 = {"simulacoes_modalidades_json": {"versao": 1, "vista": {"inputs": {}, "outputs": None}}}
    assert leilao_tem_simulacao_utilizador_gravada(r2) is False


def test_ha_sim_gravada_com_outputs_legado_ou_bundle():
    r1 = {
        "operacao_simulacao_json": {
            "outputs": {"valor_venda_estimado": 100000.0},
        }
    }
    assert leilao_tem_simulacao_utilizador_gravada(r1) is True
    r2 = {
        "simulacoes_modalidades_json": {
            "vista": {"outputs": {"lucro_bruto": 1.0}},
        }
    }
    assert leilao_tem_simulacao_utilizador_gravada(r2) is True

