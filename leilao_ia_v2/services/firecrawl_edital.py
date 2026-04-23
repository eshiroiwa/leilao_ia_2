"""Scrape de página de leilão via Firecrawl (1 crédito por chamada de API)."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from leilao_ia_v2.services import disk_cache

logger = logging.getLogger(__name__)

try:
    from firecrawl import Firecrawl
except ImportError:
    Firecrawl = None  # type: ignore[misc, assignment]


def _resultado_para_dict(result: Any) -> dict[str, Any]:
    if hasattr(result, "model_dump"):
        return result.model_dump()
    if isinstance(result, dict):
        return result
    return {}


def scrape_url_markdown(
    url: str,
    *,
    ignorar_cache: bool = False,
) -> tuple[str, dict[str, Any]]:
    """
    Retorna (markdown, metadados_firecrawl).
    Usa cache em disco quando `ignorar_cache` é False (não consome crédito Firecrawl).
    """
    url = url.strip()
    if not ignorar_cache:
        cached = disk_cache.ler_markdown_cache(url)
        if cached is not None and cached.strip():
            return cached, {"fonte": "disk_cache", "url": url, "consumiu_credito_api": False}

    key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    if not key:
        raise RuntimeError("Defina FIRECRAWL_API_KEY no ambiente para usar o Firecrawl.")
    if Firecrawl is None:
        raise RuntimeError("Instale o pacote firecrawl-py (firecrawl-py).")

    fc = Firecrawl(api_key=key)
    logger.info("Firecrawl: scrape único (1 crédito) url=%s", url[:120])
    result = fc.scrape(url, formats=["markdown"])
    d = _resultado_para_dict(result)
    markdown = str(d.get("markdown") or "").strip()
    meta = d.get("metadata") if isinstance(d.get("metadata"), dict) else {}
    metadados: dict[str, Any] = {
        "fonte": "firecrawl",
        "url": url,
        "metadata": meta,
        "consumiu_credito_api": True,
    }
    if not markdown:
        raise ValueError("Firecrawl retornou markdown vazio — URL pode ser inválida ou página sem conteúdo.")

    disk_cache.gravar_markdown_cache(url, markdown)
    return markdown, metadados
