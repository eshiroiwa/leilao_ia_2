"""Pós-ingestão: comparáveis só via Firecrawl Search."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM, LeilaoExtraJson
from leilao_ia_v2.services import comparaveis_pos_ingestao as cpi


@patch("leilao_ia_v2.fc_search.pipeline.complementar_anuncios_firecrawl_search")
def test_executar_comparaveis_chama_pipeline_fc(mock_fc, monkeypatch):
    monkeypatch.setenv("FIRECRAWL_API_KEY", "k")
    mock_fc.return_value = (3, "search: ok", 6)
    cli = MagicMock()
    ext = ExtracaoEditalLLM(
        url_leilao="https://x.com",
        cidade="Campinas",
        estado="SP",
        bairro="Centro",
        tipo_imovel="apartamento",
        leilao_extra=LeilaoExtraJson(),
    )
    r = cpi.executar_comparaveis_apos_ingestao_leilao(
        cli, leilao_imovel_id="L1", extn=ext, ignorar_cache_firecrawl=False
    )
    assert r.get("ok") is True
    assert r.get("anuncios_salvos") == 3
    mock_fc.assert_called_once()


def test_sem_firecrawl_key(monkeypatch):
    monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
    cli = MagicMock()
    ext = ExtracaoEditalLLM(
        url_leilao="https://x.com",
        cidade="Campinas",
        estado="SP",
        bairro="Centro",
        leilao_extra=LeilaoExtraJson(),
    )
    r = cpi.executar_comparaveis_apos_ingestao_leilao(cli, leilao_imovel_id="L1", extn=ext)
    assert r.get("omitido") is True
