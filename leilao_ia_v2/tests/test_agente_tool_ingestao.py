from __future__ import annotations

import json
from unittest.mock import patch

from leilao_ia_v2.agents.agente_ingestao_edital import tool_ingestir_leilao_por_url


def test_tool_retorna_json_sem_conteudo_edital():
    with patch("leilao_ia_v2.agents.agente_ingestao_edital.get_supabase_client"):
        with patch("leilao_ia_v2.agents.agente_ingestao_edital.executar_ingestao_edital") as ex:
            from leilao_ia_v2.exceptions import IngestaoSemConteudoEditalError

            ex.side_effect = IngestaoSemConteudoEditalError("página sem edital", diagnostico=None)
            out = tool_ingestir_leilao_por_url.entrypoint("https://hotel.example", "")  # type: ignore[union-attr]
            data = json.loads(out)
            assert data.get("sem_conteudo_edital") is True
            assert data.get("ok") is False
            assert "orientacao_usuario" in data


def test_tool_retorna_json_duplicata():
    with patch("leilao_ia_v2.agents.agente_ingestao_edital.get_supabase_client"):
        with patch("leilao_ia_v2.agents.agente_ingestao_edital.executar_ingestao_edital") as ex:
            from leilao_ia_v2.exceptions import EscolhaSobreDuplicataNecessaria

            ex.side_effect = EscolhaSobreDuplicataNecessaria({"id": "1", "url_leilao": "https://x"})
            out = tool_ingestir_leilao_por_url.entrypoint("https://x", "")  # type: ignore[union-attr]
            data = json.loads(out)
            assert data.get("duplicata") is True
            assert data.get("ok") is False
            assert data.get("id_existente") == "1"
            assert data.get("url_leilao") == "https://x"
