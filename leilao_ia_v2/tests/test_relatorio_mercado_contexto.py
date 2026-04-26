from leilao_ia_v2.schemas.relatorio_mercado_contexto import (
    CARD_IDS_ORDEM,
    normalizar_documento_mercado,
    parse_relatorio_mercado_contexto_json,
)


def test_normalizar_preenche_todos_os_cards():
    raw = {
        "versao": 1,
        "cards": [
            {"id": "populacao", "titulo": "Pop", "topicos": ["a", "b"]},
        ],
    }
    doc = normalizar_documento_mercado(raw)
    assert len(doc.cards) == len(CARD_IDS_ORDEM)
    assert doc.cards[0].id == "populacao"
    assert len(doc.cards[0].topicos) == 2
    assert doc.cards[1].topicos == []


def test_parse_invalido_retorna_vazio():
    doc = parse_relatorio_mercado_contexto_json({"versao": 99, "cards": "nope"})
    assert doc.cards == [] or all(not c.topicos for c in doc.cards)


def test_normalizar_preserva_evidencia_card():
    raw = {
        "versao": 1,
        "cards": [
            {"id": "populacao", "titulo": "Pop", "topicos": ["a"], "evidencia": "Base: 10 amostras."},
        ],
    }
    doc = normalizar_documento_mercado(raw)
    assert doc.cards[0].evidencia == "Base: 10 amostras."


def test_normalizar_limpa_campos_de_insights():
    raw = {
        "versao": 1,
        "cards": [{"id": "populacao", "titulo": "Pop", "topicos": ["a"]}],
        "insights_oportunidade": ["  Oportunidade A  ", "", "Oportunidade A"],
        "insights_risco": ["Risco A"],
        "checklist_diligencia": ["  Checagem 1  ", "  "],
        "dados_populacao_cidade": [" Faixa 500k ", ""],
        "informacoes_bairro": [" Próximo ao eixo de serviços "],
        "contexto_minimo": [" Cidade X ", ""],
        "estrategia_sugerida": "  Revenda rápida  ",
        "tese_acao": "  Tese curta  ",
    }
    doc = normalizar_documento_mercado(raw)
    assert doc.insights_oportunidade == ["Oportunidade A", "Oportunidade A"]
    assert doc.insights_risco == ["Risco A"]
    assert doc.checklist_diligencia == ["Checagem 1"]
    assert doc.dados_populacao_cidade == ["Faixa 500k"]
    assert doc.informacoes_bairro == ["Próximo ao eixo de serviços"]
    assert doc.contexto_minimo == ["Cidade X"]
    assert doc.estrategia_sugerida == "Revenda rápida"
    assert doc.tese_acao == "Tese curta"


def test_normalizar_descarta_cards_invalidos_sem_id():
    raw = {
        "versao": 1,
        "cards": [
            {"title": "Resumo financeiro", "topicos": ["x"]},
            {"id": "populacao", "titulo": "Pop", "topicos": ["ok"]},
        ],
        "insights_oportunidade": ["Oportunidade A"],
    }
    doc = normalizar_documento_mercado(raw)
    # O card sem `id` é descartado e o documento segue válido.
    assert len(doc.cards) == len(CARD_IDS_ORDEM)
    assert doc.cards[0].id == "populacao"
    assert doc.cards[0].topicos == ["ok"]
