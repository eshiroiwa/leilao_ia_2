from datetime import datetime, timedelta, timezone

from leilao_ia_v2.schemas.relatorio_mercado_contexto import RelatorioMercadoCard
from leilao_ia_v2.services.relatorio_mercado_inteligencia import (
    assinatura_cache_principal,
    avaliar_validade_relatorio,
    calcular_qualidade_relatorio,
    extrair_sinais_objetivos_decisao,
    extrair_sinais_objetivos_por_cards,
    gerar_insights_decisao,
    montar_contexto_minimo_decisao,
    montar_contexto_populacao_bairro,
)


def test_extrair_sinais_objetivos_por_cards_reage_a_keywords():
    cards = [
        RelatorioMercadoCard(
            id="procura_imoveis",
            titulo="Procura",
            topicos=["Alta procura e boa liquidez na faixa analisada."],
            evidencia="Base: 20 amostras.",
        ),
        RelatorioMercadoCard(
            id="volume_anuncios",
            titulo="Volume",
            topicos=["Concorrência elevada e muita oferta no bairro."],
            evidencia="Base: 20 amostras.",
        ),
    ]
    s = extrair_sinais_objetivos_por_cards(cards)
    assert s["liquidez_bairro"] > 50
    assert s["pressao_concorrencia"] > 50


def test_calcular_qualidade_relatorio_compara_bairro_geo():
    cache = {"id": "c1", "n_amostras": 12, "anuncios_ids": "a1,a2,a3"}
    ads = {
        "a1": {"bairro": "Centro", "latitude": -23.1, "longitude": -46.6},
        "a2": {"bairro": "Centro", "latitude": -23.2, "longitude": -46.7},
        "a3": {"bairro": "Outro", "latitude": None, "longitude": None},
    }
    q = calcular_qualidade_relatorio(cache_principal=cache, ads_por_id=ads, bairro_alvo="Centro")
    assert q["n_amostras_cache"] == 12
    assert q["n_anuncios_resolvidos"] == 3
    assert q["pct_mesmo_bairro"] > 60
    assert q["pct_geo_valida"] > 60
    assert q["score_qualidade"] >= 40


def test_avaliar_validade_relatorio_expira_por_ttl():
    cache = {"id": "c1", "n_amostras": 10, "anuncios_ids": "x,y"}
    ass = assinatura_cache_principal(cache)
    old = (datetime.now(timezone.utc) - timedelta(hours=200)).isoformat()
    v = avaliar_validade_relatorio(
        gerado_em_iso=old,
        ttl_horas=168,
        cache_principal_id="c1",
        assinatura_cache=ass,
        cache_principal_atual=cache,
    )
    assert v["expirado"] is True
    assert "TTL" in v["motivo"] or "ttl" in v["motivo"].lower()


def test_gerar_insights_decisao_retorna_bloco_acionavel():
    row = {"cidade": "São José do Rio Preto", "bairro": "Vila Esplanada", "tipo_imovel": "casa"}
    qualidade = {
        "score_qualidade": 74,
        "n_amostras_cache": 16,
        "pct_mesmo_bairro": 70.0,
        "pct_geo_valida": 88.0,
    }
    sinais = {
        "liquidez_bairro": 68,
        "pressao_concorrencia": 62,
        "fit_imovel_bairro": 66,
    }
    out = gerar_insights_decisao(row=row, qualidade=qualidade, sinais=sinais)
    assert out["insights_oportunidade"]
    assert out["insights_risco"]
    assert out["checklist_diligencia"]
    assert "estrategia_sugerida" in out and out["estrategia_sugerida"]
    assert "tese_acao" in out and out["tese_acao"]


def test_gerar_insights_decisao_reage_roi_abaixo_meta():
    row = {
        "cidade": "São José do Rio Preto",
        "bairro": "Vila Esplanada",
        "tipo_imovel": "casa",
        "operacao_simulacao_json": {
            "inputs": {},
            "outputs": {
                "roi_bruto": 0.18,
                "roi_liquido": 0.12,
                "roi_desejado_pct_informado": 30.0,
                "roi_desejado_modo_informado": "bruto",
                "lucro_liquido": -10000.0,
            },
        },
    }
    qualidade = {
        "score_qualidade": 74,
        "n_amostras_cache": 16,
        "pct_mesmo_bairro": 70.0,
        "pct_geo_valida": 88.0,
    }
    sinais = {"liquidez_bairro": 60, "pressao_concorrencia": 62, "fit_imovel_bairro": 66}
    out = gerar_insights_decisao(row=row, qualidade=qualidade, sinais=sinais)
    assert "Descarte recomendado" in out["estrategia_sugerida"] or "Atenção máxima" in out["estrategia_sugerida"]
    assert any("abaixo" in x.lower() and "meta" in x.lower() for x in out["insights_risco"])


def test_gerar_insights_decisao_reage_roi_acima_meta():
    row = {
        "cidade": "São José do Rio Preto",
        "bairro": "Vila Esplanada",
        "tipo_imovel": "casa",
        "operacao_simulacao_json": {
            "inputs": {},
            "outputs": {
                "roi_bruto": 0.55,
                "roi_liquido": 0.45,
                "roi_desejado_pct_informado": 30.0,
                "roi_desejado_modo_informado": "bruto",
                "lucro_liquido": 120000.0,
            },
        },
    }
    qualidade = {
        "score_qualidade": 74,
        "n_amostras_cache": 16,
        "pct_mesmo_bairro": 70.0,
        "pct_geo_valida": 88.0,
    }
    sinais = {"liquidez_bairro": 70, "pressao_concorrencia": 55, "fit_imovel_bairro": 68}
    out = gerar_insights_decisao(row=row, qualidade=qualidade, sinais=sinais)
    assert "Arrematação" in out["estrategia_sugerida"]
    assert any("meta" in x.lower() and "roi" in x.lower() for x in out["insights_oportunidade"])


def test_extrair_sinais_objetivos_decisao_reage_a_insights():
    s = extrair_sinais_objetivos_decisao(
        insights_oportunidade=["Boa liquidez e saída rápida na microrregião."],
        insights_risco=["Concorrência elevada e muita oferta em parte da faixa."],
        estrategia_sugerida="Revenda rápida com entrada disciplinada.",
        tese_acao="Entrar com margem para competir no preço.",
    )
    assert 0 <= s["liquidez_bairro"] <= 100
    assert 0 <= s["pressao_concorrencia"] <= 100
    assert 0 <= s["fit_imovel_bairro"] <= 100


def test_montar_contexto_minimo_decisao_retorna_linhas():
    out = montar_contexto_minimo_decisao(
        row={"cidade": "São José do Rio Preto", "bairro": "Vila Esplanada", "tipo_imovel": "casa"},
        qualidade={"n_amostras_cache": 14, "score_qualidade": 77, "pct_mesmo_bairro": 64.0},
    )
    assert out == []


def test_montar_contexto_populacao_bairro_retorna_cards():
    out = montar_contexto_populacao_bairro(
        row={"cidade": "São José do Rio Preto", "bairro": "Vila Esplanada", "tipo_imovel": "casa"},
        qualidade={"n_amostras_cache": 14, "pct_mesmo_bairro": 64.0, "pct_geo_valida": 82.0},
    )
    assert out["dados_populacao_cidade"]
    assert "SAO JOSE DO RIO PRETO: faixa aproximada de 500 a 600 mil habitantes (estimativa de mercado)." in out["dados_populacao_cidade"][0]
    assert out["informacoes_bairro"] == []

