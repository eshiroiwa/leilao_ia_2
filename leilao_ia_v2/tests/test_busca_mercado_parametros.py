"""Parâmetros ajustáveis de busca (cache + Firecrawl Search)."""

from leilao_ia_v2.config.busca_mercado_parametros import (
    BuscaMercadoParametros,
    get_busca_mercado_parametros,
    mensagem_com_dica_ajuste_busca,
    parametros_de_session_state,
)


def test_parametros_padrao_fora_streamlit():
    p = get_busca_mercado_parametros()
    assert p.area_fator_min == 0.75
    assert p.area_fator_max == 1.30
    assert p.raio_km == 6.0
    assert p.min_amostras_cache == 4
    assert p.max_firecrawl_creditos_analise == 12
    assert p.cache_max_amostras_principal == 8
    assert p.cache_max_amostras_lote == 8


def test_parametros_de_chaves_planas():
    sess = {
        "bm_area_pct_min": 50,
        "bm_area_pct_max": 200,
        "bm_raio_km": 10.0,
        "bm_min_amostras_cache": 5,
        "bm_max_firecrawl_creditos": 20,
        "bm_cache_max_principal": 20,
        "bm_cache_max_lote": 8,
    }
    p = parametros_de_session_state(sess)
    assert p.area_fator_min == 0.5
    assert p.area_fator_max == 2.0
    assert p.raio_km == 10.0
    assert p.min_amostras_cache == 5
    assert p.max_firecrawl_creditos_analise == 20
    assert p.cache_max_amostras_principal == 20
    assert p.cache_max_amostras_lote == 8


def test_area_min_igual_max_corrige():
    sess = {
        "bm_area_pct_min": 90,
        "bm_area_pct_max": 90,
    }
    p = parametros_de_session_state(sess)
    assert p.area_fator_min == 0.9
    assert p.area_fator_max > p.area_fator_min


def test_dict_legado_busca_mercado():
    sess = {"busca_mercado": {"area_pct_min": 70, "area_pct_max": 160, "raio_km": 7.5}}
    p = parametros_de_session_state(sess)
    assert p.area_fator_min == 0.7
    assert p.area_fator_max == 1.6
    assert p.raio_km == 7.5


def test_mensagem_com_dica_nao_duplica():
    base = "Poucos anúncios"
    m = mensagem_com_dica_ajuste_busca(base)
    assert base in m
    assert "Ajustes de busca" in m
    m2 = mensagem_com_dica_ajuste_busca(m)
    assert m2 == m


def test_frozen_dataclass():
    p = BuscaMercadoParametros()
    assert p.min_amostras_cache == 4
