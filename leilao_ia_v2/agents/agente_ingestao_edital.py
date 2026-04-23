"""
Agente Agno: orquestra ingestão por URL (tools executam o pipeline Python).
Para fluxo com pergunta de duplicata na UI, use diretamente `pipeline.ingestao_edital`.

O chat do app deve evoluir para um assistente com todas as tools/agentes; ver `AGENTS.md` na raiz.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.tools import tool

from leilao_ia_v2.exceptions import (
    EscolhaSobreDuplicataNecessaria,
    IngestaoSemConteudoEditalError,
    UrlInvalidaIngestaoError,
)
from leilao_ia_v2.services.conteudo_edital_heuristica import MENSAGEM_ACOES_USUARIO
from leilao_ia_v2.pipeline.ingestao_edital import executar_ingestao_edital
from leilao_ia_v2.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


def _parse_sobrescrever(raw: Optional[str]) -> Optional[bool]:
    if raw is None or str(raw).strip() == "":
        return None
    s = str(raw).strip().lower()
    if s in ("true", "1", "sim", "s", "yes", "sobrescrever"):
        return True
    if s in ("false", "0", "nao", "não", "n", "manter"):
        return False
    return None


@tool(show_result=True)
def tool_ingestir_leilao_por_url(
    url: str,
    sobrescrever_se_duplicado: str = "",
    ignorar_cache_firecrawl: bool = False,
) -> str:
    """
    Ingera um leilão: Firecrawl (1 crédito se não houver cache em disco) + extração LLM + Supabase.

    `sobrescrever_se_duplicado`: vazio = se já existir a URL, retorna JSON pedindo decisão;
      "true" / "sim" = atualiza; "false" / "nao" = não altera.
    """
    cli = get_supabase_client()
    pol = _parse_sobrescrever(sobrescrever_se_duplicado or None)
    try:
        r = executar_ingestao_edital(
            url.strip(),
            cli,
            sobrescrever_duplicata=pol,
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
        )
        m = r.metricas_llm or {}
        pc = r.pos_cache or {}
        # JSON compacto (sem log) — o modelo não deve colar isso na resposta ao usuário.
        out: dict[str, Any] = {
            "ok": True,
            "modo": r.modo,
            "id": r.id,
            "url_leilao": r.url_leilao,
            "metricas_llm": {
                "modelo": m.get("modelo"),
                "prompt_tokens": m.get("prompt_tokens"),
                "completion_tokens": m.get("completion_tokens"),
            },
        }
        if pc:
            out["pos_cache"] = {
                "ok": pc.get("ok"),
                "reutilizou_existente": pc.get("reutilizou_existente"),
                "usou_firecrawl_extra": pc.get("usou_firecrawl_extra"),
                "mensagem": pc.get("mensagem"),
            }
        out["firecrawl_chamadas_api_total"] = int(getattr(r, "firecrawl_chamadas_api_total", 0) or 0)
        return json.dumps(out, ensure_ascii=False, default=str)
    except EscolhaSobreDuplicataNecessaria as e:
        reg = e.registro_existente or {}
        return json.dumps(
            {
                "ok": False,
                "duplicata": True,
                "mensagem": "URL já cadastrada. Pergunte ao usuário se deseja sobrescrever e chame a tool de novo.",
                "id_existente": reg.get("id"),
                "url_leilao": reg.get("url_leilao"),
            },
            ensure_ascii=False,
            default=str,
        )
    except IngestaoSemConteudoEditalError as e:
        return json.dumps(
            {
                "ok": False,
                "sem_conteudo_edital": True,
                "mensagem": e.motivo,
                "orientacao_usuario": MENSAGEM_ACOES_USUARIO,
                "diagnostico": str(e.diagnostico) if getattr(e, "diagnostico", None) else "",
            },
            ensure_ascii=False,
        )
    except UrlInvalidaIngestaoError as e:
        return json.dumps(
            {"ok": False, "url_invalida": True, "mensagem": str(e)},
            ensure_ascii=False,
        )
    except Exception as e:
        logger.exception("tool_ingestir_leilao_por_url")
        return json.dumps({"ok": False, "erro": str(e)}, ensure_ascii=False)


def criar_agente_ingestao_edital(
    *,
    model_id: Optional[str] = None,
    markdown: bool = True,
) -> Agent:
    mid = model_id or os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
    return Agent(
        model=OpenAIChat(id=mid),
        tools=[tool_ingestir_leilao_por_url],
        instructions=(
            "Você opera a ingestão de editais de leilão (imóveis). "
            "Para processar uma URL, chame tool_ingestir_leilao_por_url. "
            "Após gravar o imóvel, o pipeline resolve automaticamente o cache de mercado (reutiliza cache "
            "existente no mesmo geo quando as amostras ainda servem; só então complementa anúncios via Firecrawl Search se preciso). "
            "IMPORTANTE para a resposta ao usuário: seja **breve** (no máximo 3 frases curtas). "
            "**Não** cole na conversa o JSON retornado pela ferramenta, nem tokens, nem log técnico. "
            "Diga em linguagem natural o resultado (gravado / atualizado / ignorado duplicata / erro) e, se houver, "
            "o id do registro; mencione que detalhes, cards e mapa aparecem no painel à esquerda da tela. "
            "A extração lê o markdown do edital e grava campos como valor de lance e, quando o edital trouxer, "
            "valor de avaliação (perícia), que é distinto do lance. "
            "Se duplicata=true, pergunte se deseja sobrescrever e só então chame a tool de novo com "
            "sobrescrever_se_duplicado='true' ou 'false'. "
            "Se sem_conteudo_edital=true, explique em 2 frases e use orientacao_usuario. "
            "Se url_invalida=true, uma frase basta. "
            "Use português do Brasil."
        ),
        markdown=markdown,
    )
