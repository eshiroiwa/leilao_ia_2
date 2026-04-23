"""Chamadas Firecrawl: pesquisa web (``search``)."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

try:
    from firecrawl import Firecrawl
except ImportError:
    Firecrawl = None  # type: ignore[misc, assignment]


def _dump(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    return {}


def executar_busca_web(query: str, *, limit: int | None = None) -> tuple[list[dict[str, Any]], int]:
    """
    Executa ``Firecrawl.search``.

    Devolve ``(itens_web_como_dict, chamadas_api)`` onde ``chamadas_api`` é 1 se consumiu
    crédito de pesquisa na API Firecrawl (estimativa; o painel oficial de saldo é a fonte exata).
    """
    key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    if not key:
        raise RuntimeError("FIRECRAWL_API_KEY ausente.")
    if Firecrawl is None:
        raise RuntimeError("Pacote firecrawl-py não instalado.")

    lim = limit if limit is not None else int(os.getenv("FC_SEARCH_LIMIT", "12") or "12")
    lim = max(3, min(20, lim))

    fc = Firecrawl(api_key=key)
    logger.info("Firecrawl Search: query=%r limit=%s", query[:200], lim)
    raw = fc.search(query, limit=lim)
    d = _dump(raw)
    web = d.get("web") or []
    if not isinstance(web, list):
        web = []
    if not web:
        logger.warning(
            "Firecrawl Search: campo web vazio (top_keys=%s); verifique query, API key e plano.",
            sorted(d.keys()),
        )
    # Custo de search: documentação indica créditos por lote; contamos 1 chamada HTTP de search.
    return web, 1
