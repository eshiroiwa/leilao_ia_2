from datetime import datetime, timedelta, timezone

from leilao_ia_v2.schemas.relatorio_mercado_contexto import RelatorioMercadoCard
from leilao_ia_v2.services.relatorio_mercado_inteligencia import (
    assinatura_cache_principal,
    avaliar_validade_relatorio,
    calcular_qualidade_relatorio,
    extrair_sinais_objetivos_por_cards,
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

