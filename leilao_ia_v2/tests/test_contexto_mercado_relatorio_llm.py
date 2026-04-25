import os
from unittest.mock import patch

from leilao_ia_v2.services.contexto_mercado_relatorio_llm import _resolver_modelo_relatorio_mercado


def test_resolver_modelo_relatorio_prioriza_parametro_explicito():
    with patch.dict(
        os.environ,
        {
            "OPENAI_MODEL_RELATORIO_MERCADO": "gpt-5",
            "OPENAI_CHAT_MODEL": "gpt-5-mini",
        },
        clear=False,
    ):
        assert _resolver_modelo_relatorio_mercado("gpt-4.1-mini") == "gpt-4.1-mini"


def test_resolver_modelo_relatorio_usa_override_dedicado():
    with patch.dict(
        os.environ,
        {
            "OPENAI_MODEL_RELATORIO_MERCADO": "gpt-5",
            "OPENAI_CHAT_MODEL": "gpt-5-mini",
        },
        clear=False,
    ):
        assert _resolver_modelo_relatorio_mercado(None) == "gpt-5"


def test_resolver_modelo_relatorio_fallback_openai_chat_model():
    with patch.dict(
        os.environ,
        {
            "OPENAI_MODEL_RELATORIO_MERCADO": "",
            "OPENAI_CHAT_MODEL": "gpt-5-mini",
        },
        clear=False,
    ):
        assert _resolver_modelo_relatorio_mercado(None) == "gpt-5-mini"


def test_resolver_modelo_relatorio_fallback_local_quando_sem_env():
    with patch.dict(
        os.environ,
        {
            "OPENAI_MODEL_RELATORIO_MERCADO": "",
            "OPENAI_CHAT_MODEL": "",
        },
        clear=False,
    ):
        assert _resolver_modelo_relatorio_mercado(None) == "gpt-4o-mini"
