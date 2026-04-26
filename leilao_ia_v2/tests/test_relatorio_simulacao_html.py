"""Relatório HTML de simulação."""

from __future__ import annotations

from leilao_ia_v2.schemas.operacao_simulacao import OperacaoSimulacaoDocumento
from leilao_ia_v2.services.relatorio_simulacao_html import montar_html_relatorio_simulacao


def test_relatorio_inclui_dados_adicionais_leilao_extra():
    row = {
        "id": "x",
        "endereco": "Rua Teste",
        "cidade": "Campinas",
        "estado": "SP",
        "leilao_extra_json": {
            "formas_pagamento": ["à vista", "financiamento"],
            "processo_judicial": "0001234-56.2024.8.26.0100",
        },
    }
    doc = OperacaoSimulacaoDocumento()
    html = montar_html_relatorio_simulacao(row=row, caches=[], ads_map={}, doc=doc)
    assert "dc-root" in html and "sp-sim-financeiro" in html
    assert "sp-sim-line" in html
    assert "Campinas" in html
    assert "Dados adicionais" in html
    assert "rel-dois-col" in html
    assert "formas_pagamento" in html
    assert "financiamento" in html
    assert "processo_judicial" in html


def test_relatorio_coluna_adicionais_vazia_mostra_aviso():
    row = {
        "id": "x",
        "endereco": "Rua Y",
        "cidade": "São Paulo",
        "estado": "SP",
        "leilao_extra_json": {},
    }
    html = montar_html_relatorio_simulacao(row=row, caches=[], ads_map={}, doc=OperacaoSimulacaoDocumento())
    assert "Dados adicionais" in html
    assert "Nenhum dado adicional registrado" in html
    assert "rel-dois-col" in html


def test_relatorio_inclui_secao_analise_mercado():
    row = {
        "id": "x",
        "endereco": "Rua Z",
        "cidade": "Curitiba",
        "estado": "PR",
        "relatorio_mercado_contexto_json": {
            "versao": 1,
            "gerado_em_iso": "2026-01-01T12:00:00+00:00",
            "modelo": "test-model",
            "prompt_tokens": 10,
            "completion_tokens": 20,
            "custo_usd_estimado": 0.001,
            "disclaimer": "Aviso teste.",
            "cards": [],
            "insights_oportunidade": ["Microregião com demanda constante."],
            "insights_risco": ["Concorrência pode exigir ajuste de preço."],
            "checklist_diligencia": ["Validar custo real de reforma."],
            "dados_populacao_cidade": ["CURITIBA: faixa aproximada de 1,7 a 2,0 milhões de habitantes (estimativa de mercado)."],
            "informacoes_bairro": ["Bairro com boa aderência para o público-alvo."],
            "contexto_minimo": ["Contexto: Curitiba · Bairro X · tipo casa.", "Base comparável: 12 amostras."],
            "estrategia_sugerida": "Revenda rápida com entrada disciplinada.",
            "tese_acao": "Entrar apenas com margem de segurança para saída competitiva.",
        },
    }
    html = montar_html_relatorio_simulacao(row=row, caches=[], ads_map={}, doc=OperacaoSimulacaoDocumento())
    assert "Análise de mercado e bairro" in html
    assert "Contexto da cidade e população" in html
    assert "Insights de oportunidade" in html
    assert "Alertas de risco" in html
    assert "Checklist de diligência" in html
    assert "População da cidade" not in html
    assert "Informações do bairro" in html
    assert "Tese e ação recomendada" in html
    ordem = [
        "Contexto da cidade e população",
        "Informações do bairro",
        "Alertas de risco",
        "Insights de oportunidade",
        "Tese e ação recomendada",
        "Checklist de diligência",
    ]
    pos = [html.index(x) for x in ordem]
    assert pos == sorted(pos)
    assert "Aviso teste." not in html
    assert "test-model" not in html
    assert "10 / 20" not in html
    assert "2026-01-01T12:00" not in html


def test_relatorio_mapa_marcadores_uniao_anuncios_dos_caches():
    """Mapa: marcadores a partir da união dos anuncios_ids dos caches; sem texto de comparáveis no HTML."""
    row = {
        "id": "L1",
        "endereco": "Rua Mapa",
        "cidade": "Jundiaí",
        "estado": "SP",
        "latitude": -22.0,
        "longitude": -47.0,
    }
    c1 = {
        "nome_cache": "Mercado 10km principal",
        "tipo_imovel": "apartamento",
        "n_amostras": 2,
        "preco_m2_medio": 5000.0,
        "valor_medio_venda": 500000.0,
        "anuncios_ids": "u1,u2",
        "metadados_json": {"uso_simulacao": True},
    }
    c2 = {
        "nome_cache": "Terrenos ref.",
        "tipo_imovel": "terreno",
        "n_amostras": 1,
        "preco_m2_medio": 0.0,
        "valor_medio_venda": 0.0,
        "anuncios_ids": "u3",
        "metadados_json": {"modo_cache": "terrenos"},
    }
    ads_map = {
        "u1": {
            "latitude": -22.01,
            "longitude": -47.01,
            "valor_venda": 400000.0,
            "area_construida_m2": 80.0,
            "url_anuncio": "https://a.com/1",
        },
        "u2": {
            "latitude": -22.02,
            "longitude": -47.02,
            "valor_venda": 450000.0,
            "area_construida_m2": 90.0,
            "url_anuncio": "https://a.com/2",
        },
        "u3": {
            "latitude": -22.03,
            "longitude": -47.03,
            "valor_venda": 100000.0,
            "area_construida_m2": 0.0,
            "url_anuncio": "https://a.com/3",
        },
    }
    html = montar_html_relatorio_simulacao(
        row=row,
        caches=[c1, c2],
        ads_map=ads_map,
        doc=OperacaoSimulacaoDocumento(),
    )
    assert "Mercado 10km principal" not in html
    assert "Anúncios comparativos" not in html
    assert "Mapa" in html
    assert "Clique nos pontos do mapa para ver mais informações" in html
    assert "Jundiaí" in html
    assert "sp-sim-line" in html
    assert "rel-map-json" in html
    # três anúncios com coords distintas → três itens no array markers do JSON embebido
    assert html.count('"lat":-22.01') == 1
    assert html.count('"lat":-22.02') == 1
    assert html.count('"lat":-22.03') == 1


def test_relatorio_sem_analise_mercado_quando_flag_desligada():
    row = {
        "id": "x",
        "endereco": "Rua Z",
        "cidade": "Curitiba",
        "estado": "PR",
        "relatorio_mercado_contexto_json": {
            "versao": 1,
            "cards": [],
            "insights_oportunidade": ["Teste"],
            "insights_risco": ["Teste"],
            "checklist_diligencia": ["Teste"],
            "dados_populacao_cidade": ["CURITIBA: faixa aproximada..."],
            "informacoes_bairro": ["Teste"],
            "estrategia_sugerida": "Teste",
            "tese_acao": "Teste",
        },
    }
    html = montar_html_relatorio_simulacao(
        row=row,
        caches=[],
        ads_map={},
        doc=OperacaoSimulacaoDocumento(),
        incluir_analise_mercado=False,
    )
    assert "Análise de mercado e bairro" not in html
