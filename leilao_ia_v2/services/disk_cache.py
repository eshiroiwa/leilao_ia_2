"""Cache em disco para markdown do Firecrawl (por URL)."""

from __future__ import annotations

import hashlib
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def diretorio_cache_padrao() -> Path:
    raw = os.getenv("LEILAO_IA_V2_CACHE_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.home() / ".cache" / "leilao_ia_v2" / "firecrawl_markdown"


def caminho_cache_markdown(url: str) -> Path:
    h = hashlib.sha256(url.strip().encode("utf-8")).hexdigest()
    d = diretorio_cache_padrao()
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{h}.md"


def idade_maxima_segundos() -> float:
    try:
        dias = float(os.getenv("LEILAO_IA_V2_FIRECRAWL_CACHE_MAX_AGE_DAYS", "7"))
    except ValueError:
        dias = 7.0
    return max(0.0, dias) * 86400.0


def ler_markdown_cache(url: str) -> str | None:
    path = caminho_cache_markdown(url)
    if not path.is_file():
        logger.info("Cache markdown: miss %s", path.name)
        return None
    max_age = idade_maxima_segundos()
    if max_age > 0:
        idade = time.time() - path.stat().st_mtime
        if idade > max_age:
            logger.info("Cache markdown: expirado (%s)", path.name)
            return None
    texto = path.read_text(encoding="utf-8", errors="replace")
    logger.info("Cache markdown: hit %s (%s bytes)", path.name, len(texto))
    return texto


def gravar_markdown_cache(url: str, markdown: str) -> Path:
    path = caminho_cache_markdown(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
    logger.info("Cache markdown: gravado %s (%s bytes)", path, len(markdown))
    return path
