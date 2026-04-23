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
