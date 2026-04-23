"""
Agente Agno: cria ``cache_media_bairro`` a partir do leilão e anúncios (sem LLM no núcleo).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.tools import tool

from leilao_ia_v2.services.cache_media_leilao import criar_caches_media_para_leilao
from leilao_ia_v2.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


@tool(show_result=True)
def tool_criar_cache_media_para_leilao(
    leilao_imovel_id: str,
    ignorar_cache_firecrawl: bool = False,
) -> str:
    """
    Seleciona comparáveis em ``anuncios_mercado`` (5 km, tipo, faixa de área), grava ``cache_media_bairro``
    e vincula ao leilão em ``cache_media_bairro_ids``.

    Se faltarem amostras: geocodifica anúncios sem coordenadas; em último caso **uma** listagem Viva Real.
    """
    cli = get_supabase_client()
    try:
        r = criar_caches_media_para_leilao(
            cli,
            str(leilao_imovel_id).strip(),
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
        )
        return json.dumps(
            {
                "ok": r.ok,
                "mensagem": r.mensagem,
                "caches_criados": r.caches_criados,
                "usou_firecrawl_extra": r.usou_firecrawl_extra,
            },
            ensure_ascii=False,
            default=str,
        )
    except Exception as e:
        logger.exception("tool_criar_cache_media_para_leilao")
        return json.dumps({"ok": False, "erro": str(e)}, ensure_ascii=False)


def criar_agente_cache_media(
    *,
    model_id: Optional[str] = None,
    markdown: bool = True,
) -> Agent:
    mid = model_id or os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
    return Agent(
        model=OpenAIChat(id=mid),
        tools=[tool_criar_cache_media_para_leilao],
        instructions=(
            "Você cria cache de mercado (comparáveis) para um leilão já ingerido. "
            "Chame tool_criar_cache_media_para_leilao com o UUID do imóvel em `leilao_imoveis`. "
            "Resposta ao usuário: no máximo 3 frases; **não** cole o JSON bruto. "
            "Explique se o cache foi criado, quantas amostras, e se foi necessário complementar via Viva Real. "
            "Se ok=false, explique a mensagem de insuficiência de anúncios. "
            "Use português do Brasil."
        ),
        markdown=markdown,
    )
