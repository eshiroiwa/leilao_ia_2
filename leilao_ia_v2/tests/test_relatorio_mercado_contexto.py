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
