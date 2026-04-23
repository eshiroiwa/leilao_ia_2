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
    assert "Dados adicionais" in html
    assert "formas_pagamento" in html
    assert "financiamento" in html
    assert "processo_judicial" in html


def test_relatorio_sem_secao_adicionais_quando_extra_vazio():
    row = {
        "id": "x",
        "endereco": "Rua Y",
        "cidade": "São Paulo",
        "estado": "SP",
        "leilao_extra_json": {},
    }
    html = montar_html_relatorio_simulacao(row=row, caches=[], ads_map={}, doc=OperacaoSimulacaoDocumento())
    assert "Dados adicionais" not in html


def test_relatorio_inclui_secao_analise_mercado():
    from leilao_ia_v2.schemas.relatorio_mercado_contexto import CARD_IDS_ORDEM, RelatorioMercadoCard

    cards = [
        RelatorioMercadoCard(id=cid, titulo=tit, topicos=["Ponto A.", "Ponto B."])
        for cid, tit in [
            ("populacao", "Pop"),
            ("perfil_urbano", "Perfil"),
            ("centralidade", "Central"),
            ("classe_renda", "Renda"),
            ("seguranca", "Seg"),
            ("procura_imoveis", "Procura"),
            ("bairros_concorrentes", "Concorrentes"),
            ("condominios_fechados", "Condomínios"),
            ("volume_anuncios", "Volume"),
            ("ajuste_imovel_bairro", "Ajuste"),
        ]
    ]
    assert len(cards) == len(CARD_IDS_ORDEM)
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
            "cards": [c.model_dump() for c in cards],
        },
    }
    html = montar_html_relatorio_simulacao(row=row, caches=[], ads_map={}, doc=OperacaoSimulacaoDocumento())
    assert "Análise de mercado e bairro" in html
    assert "Ponto A." in html
    assert "Aviso teste." not in html
    assert "test-model" not in html
    assert "10 / 20" not in html
    assert "2026-01-01T12:00" not in html


def test_relatorio_inclui_todos_caches_vinculados_e_mapa_todos_anuncios():
    """Vários linhas de cache: secção HTML e marcadores a partir da união dos anuncios_ids."""
    row = {
        "id": "L1",
        "endereco": "Rua Mapa",
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
    assert "Mercado 10km principal" in html
    assert "Terrenos ref." in html
    assert "[referência]" in html
    assert "rel-map-json" in html
    # três anúncios com coords distintas → três itens no array markers do JSON embebido
    assert html.count('"lat":-22.01') == 1
    assert html.count('"lat":-22.02') == 1
    assert html.count('"lat":-22.03') == 1
