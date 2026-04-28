from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from leilao_ia_v2.exceptions import (
    EscolhaSobreDuplicataNecessaria,
    IngestaoSemConteudoEditalError,
    UrlInvalidaIngestaoError,
)
from leilao_ia_v2.pipeline.ingestao_edital import executar_ingestao_edital, montar_payload_gravacao
from leilao_ia_v2.services.cache_media_leilao import ResultadoCriacaoCacheLeilao

from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM, LeilaoExtraJson

_MARKDOWN_PRE_VALIDO = (
    "Edital de leilão judicial. Imóvel apartamento na primeira praça. "
    "Lance mínimo conforme edital. Matrícula 99.999 do CRI. " * 25
)


def test_montar_payload_contem_campos_principais():
    ext = ExtracaoEditalLLM(
        url_leilao="https://z.com/l",
        endereco="Rua A, 1",
        cidade="Gravataí",
        estado="RS",
        bairro="Centro",
        tipo_imovel="apartamento",
        valor_avaliacao=450_000.50,
        url_foto_imovel="https://cdn.exemplo.com/foto.jpg",
        leilao_extra=LeilaoExtraJson(formas_pagamento=["financiamento"]),
    )
    p = montar_payload_gravacao(
        ext,
        url="https://z.com/l",
        markdown_bruto="# edital",
        edital_metadados={"fonte": "firecrawl"},
        metricas_llm={"modelo": "gpt-4o-mini", "prompt_tokens": 1, "completion_tokens": 2, "custo_usd_estimado": 0.0},
        log_text="ok",
        latitude=-29.95,
        longitude=-51.18,
    )
    assert p["url_leilao"] == "https://z.com/l"
    assert p["latitude"] == -29.95
    assert p["longitude"] == -51.18
    assert p["status"] == "pendente"
    assert p["cache_media_bairro_ids"] == []
    assert "leilao_extra_json" in p
    assert p["valor_avaliacao"] == 450_000.50
    assert p["url_foto_imovel"] == "https://cdn.exemplo.com/foto.jpg"


def test_duplicata_sem_decisao():
    cli = MagicMock()
    with patch("leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.buscar_por_url_leilao", return_value={"id": "u1"}):
        with pytest.raises(EscolhaSobreDuplicataNecessaria):
            executar_ingestao_edital("https://dup.com", cli, sobrescrever_duplicata=None)


def test_duplicata_usuario_recusa():
    cli = MagicMock()
    with patch("leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.buscar_por_url_leilao", return_value={"id": "u1"}):
        r = executar_ingestao_edital("https://dup.com", cli, sobrescrever_duplicata=False)
    assert r.modo == "ignorado_duplicata"


def test_scrape_falha_nao_grava():
    cli = MagicMock()
    with patch("leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.buscar_por_url_leilao", return_value=None):
        with patch(
            "leilao_ia_v2.pipeline.ingestao_edital.firecrawl_edital.scrape_url_markdown",
            side_effect=ValueError("vazio"),
        ):
            with pytest.raises(UrlInvalidaIngestaoError):
                executar_ingestao_edital("https://bad.com", cli, sobrescrever_duplicata=None)


def test_insert_sem_duplicata():
    cli = MagicMock()
    ext = ExtracaoEditalLLM(
        url_leilao="https://novo.com",
        cidade="Porto Alegre",
        estado="RS",
        bairro="Centro",
        tipo_imovel="apartamento",
        leilao_extra=LeilaoExtraJson(),
    )
    with patch("leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.buscar_por_url_leilao", return_value=None):
        with patch(
            "leilao_ia_v2.pipeline.ingestao_edital.firecrawl_edital.scrape_url_markdown",
            return_value=(_MARKDOWN_PRE_VALIDO, {"fonte": "firecrawl"}),
        ):
            with patch(
                "leilao_ia_v2.pipeline.ingestao_edital.extracao_edital_llm.extrair_edital_de_markdown",
                return_value=(ext, {"modelo": "x", "prompt_tokens": 1, "completion_tokens": 1, "custo_usd_estimado": 0.0}),
            ):
                with patch(
                    "leilao_ia_v2.pipeline.ingestao_edital._buscar_coordenadas_extracao",
                    return_value=(-30.0, -51.0),
                ):
                    with patch(
                        "leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.inserir_leilao_imovel",
                        return_value={"id": "novo-id"},
                    ) as ins:
                        with patch(
                            "leilao_ia_v2.pipeline.ingestao_edital.comparaveis_integracao.executar_comparaveis_pos_ingestao",
                            return_value={"ok": True, "anuncios_salvos": 2, "firecrawl_chamadas_api": 1},
                        ) as cmp:
                            with patch(
                                "leilao_ia_v2.pipeline.ingestao_edital.resolver_cache_media_pos_ingestao",
                                return_value=ResultadoCriacaoCacheLeilao(
                                    False,
                                    "teste: cache não integrado ao mock",
                                ),
                            ) as res_cache:
                                with patch(
                                    "leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.atualizar_leilao_imovel",
                                ) as upd:
                                    r = executar_ingestao_edital("https://novo.com", cli, sobrescrever_duplicata=None)
    assert r.modo == "inserido"
    assert r.id == "novo-id"
    assert r.pos_comparaveis.get("anuncios_salvos") == 2
    assert r.pos_cache.get("ok") is False
    ins.assert_called_once()
    cmp.assert_called_once()
    assert cmp.call_args.kwargs.get("max_chamadas_api_firecrawl") == 19
    res_cache.assert_called_once()
    assert res_cache.call_args.kwargs.get("max_chamadas_api_firecrawl") == 18
    upd.assert_called_once()


def test_markdown_fraco_nao_chama_llm_nem_insert():
    cli = MagicMock()
    marketing_longo = ("Hotel Pestana. Reserve seu quarto com conforto. " * 60)
    assert len(marketing_longo) >= 450
    with patch("leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.buscar_por_url_leilao", return_value=None):
        with patch(
            "leilao_ia_v2.pipeline.ingestao_edital.firecrawl_edital.scrape_url_markdown",
            return_value=(marketing_longo, {"fonte": "firecrawl"}),
        ):
            with patch(
                "leilao_ia_v2.pipeline.ingestao_edital.extracao_edital_llm.extrair_edital_de_markdown",
            ) as ext_llm:
                with patch(
                    "leilao_ia_v2.pipeline.ingestao_edital.leilao_imoveis_repo.inserir_leilao_imovel",
                ) as ins:
                    with pytest.raises(IngestaoSemConteudoEditalError):
                        executar_ingestao_edital(
                            "https://pestana.example/reserva", cli, sobrescrever_duplicata=None
                        )
    ext_llm.assert_not_called()
    ins.assert_not_called()
