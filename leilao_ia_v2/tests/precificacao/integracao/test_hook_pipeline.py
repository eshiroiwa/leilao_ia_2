"""
Testa o hook ``_tentar_gravar_precificacao_v2`` em
``services/cache_media_leilao.py``.

Foco: o hook deve **nunca** levantar exceção (try/except interno) e
deve invocar :func:`precificar_leilao` com ``client`` e ``leilao_id``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from leilao_ia_v2.services.cache_media_leilao import (
    _tentar_gravar_precificacao_v2,
)


class TestHook:
    def test_chama_precificar_leilao_com_args_corretos(self):
        client = MagicMock()
        with patch(
            "leilao_ia_v2.precificacao.integracao.precificar_leilao"
        ) as mock_pre:
            mock_pre.return_value = MagicMock(ok=True, motivo="persistido: veredito=NEUTRA")
            _tentar_gravar_precificacao_v2(client, "uuid-leilao-123")
        mock_pre.assert_called_once_with(client, "uuid-leilao-123")

    def test_falha_no_servico_nao_propaga(self):
        client = MagicMock()
        with patch(
            "leilao_ia_v2.precificacao.integracao.precificar_leilao",
            side_effect=RuntimeError("DB down"),
        ):
            # Não deve levantar — log.exception captura.
            _tentar_gravar_precificacao_v2(client, "uuid")

    def test_resultado_negativo_apenas_loga_e_retorna(self):
        client = MagicMock()
        with patch(
            "leilao_ia_v2.precificacao.integracao.precificar_leilao"
        ) as mock_pre:
            mock_pre.return_value = MagicMock(ok=False, motivo="leilão não encontrado")
            # Não levanta.
            _tentar_gravar_precificacao_v2(client, "uuid")

    def test_lid_vazio_nao_quebra(self):
        client = MagicMock()
        with patch(
            "leilao_ia_v2.precificacao.integracao.precificar_leilao"
        ) as mock_pre:
            mock_pre.return_value = MagicMock(ok=False, motivo="id vazio")
            _tentar_gravar_precificacao_v2(client, "")
        mock_pre.assert_called_once_with(client, "")
