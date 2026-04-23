from __future__ import annotations

from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM, LeilaoExtraJson, schema_extracao_edital_dict
from leilao_ia_v2.services.extracao_edital_llm import _deve_omitir_temperature


def test_schema_raiz_tem_propriedades():
    s = schema_extracao_edital_dict()
    assert s.get("type") == "object"
    assert "url_leilao" in s.get("properties", {})
    assert "valor_avaliacao" in s.get("properties", {})
    assert "url_foto_imovel" in s.get("properties", {})


def test_extracao_valida_minima():
    ext = ExtracaoEditalLLM.model_validate(
        {
            "url_leilao": "https://x.com",
            "leilao_extra": {},
        }
    )
    assert ext.url_leilao.startswith("https://")


def test_url_foto_somente_http():
    ext = ExtracaoEditalLLM.model_validate(
        {
            "url_leilao": "https://x.com",
            "url_foto_imovel": "ftp://nope/img.jpg",
            "leilao_extra": {},
        }
    )
    assert ext.url_foto_imovel is None


def test_leilao_extra_processo():
    ex = LeilaoExtraJson(
        formas_pagamento=["à vista"],
        processo_judicial={"numero": "0000123-12.2024.8.26.0100", "vara": "1ª", "comarca": "SP"},
    )
    d = ex.model_dump(mode="json", exclude_none=True)
    assert d["processo_judicial"]["comarca"] == "SP"


def test_leilao_extra_modalidade_venda_normalizada():
    ex = LeilaoExtraJson(modalidade_venda="Venda direta")
    assert ex.modalidade_venda == "venda_direta"
    ex2 = LeilaoExtraJson(modalidade_venda="Leilão")
    assert ex2.modalidade_venda == "leilao"


def test_modalidade_venda_inferida_sem_pracas():
    from leilao_ia_v2.pipeline import ingestao_edital as ing

    ext = ExtracaoEditalLLM(
        url_leilao="https://x.com",
        data_leilao_1_praca=None,
        data_leilao_2_praca=None,
        data_leilao="2026-05-01",
        leilao_extra=LeilaoExtraJson(),
    )
    n = ing._extracao_normalizada(ext)
    assert n.leilao_extra.modalidade_venda == "venda_direta"


def test_modalidade_venda_inferida_com_1_praca():
    from leilao_ia_v2.pipeline import ingestao_edital as ing

    ext = ExtracaoEditalLLM(
        url_leilao="https://x.com",
        data_leilao_1_praca="2026-04-01",
        leilao_extra=LeilaoExtraJson(),
    )
    n = ing._extracao_normalizada(ext)
    assert n.leilao_extra.modalidade_venda == "leilao"


def test_modelos_que_omitem_temperature():
    assert _deve_omitir_temperature("gpt-5")
    assert _deve_omitir_temperature("gpt-5-mini")
    assert _deve_omitir_temperature("o1-preview")
    assert not _deve_omitir_temperature("gpt-4o-mini")
    assert not _deve_omitir_temperature("gpt-4o")
