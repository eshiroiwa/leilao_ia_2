"""Extrai URL de imagem do imóvel a partir do markdown do edital (Firecrawl)."""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

# Markdown de imagem: ![alt](url) — captura alt para priorizar "Imagem 1" (ex.: Portal Zuk).
_RE_IMG_MARKDOWN = re.compile(r"!\[([^\]]*)\]\(\s*([^)]+?)\s*\)", re.MULTILINE)

# HTML residual em alguns scrapes
_RE_IMG_TAG = re.compile(r"<img\s+([^>]+)>", re.IGNORECASE)

# "Imagem 1" / "Image 1" com limite de palavra (evita "Imagem 15", "Imagem 10").
_RE_ALT_FOTO_PRINCIPAL = re.compile(
    r"\b(?:imagem|figura|image|photo|foto)\s*1\b",
    re.IGNORECASE,
)

_SUBSTR_REJEITAR = (
    "favicon",
    "/logo",
    "logo.svg",
    "logo.png",
    "/icons/",
    "sprite",
    "pixel.gif",
    "1x1",
    "blank.gif",
    "tracking",
    "analytics",
    "facebook.com",
    "twitter.com",
    "linkedin.com",
)


def _primeiro_token_url(inner: str) -> str | None:
    inner = (inner or "").strip()
    if not inner:
        return None
    for part in re.split(r"\s+", inner):
        if part.startswith("http") or part.startswith("//") or part.startswith("/"):
            return part
    return inner


def _parse_img_src_alt(blob: str) -> tuple[str | None, str]:
    src_m = re.search(r"""src\s*=\s*["']([^"']+)["']""", blob, re.IGNORECASE)
    alt_m = re.search(r"""alt\s*=\s*["']([^"']*)["']""", blob, re.IGNORECASE)
    src = src_m.group(1).strip() if src_m else None
    alt = alt_m.group(1) if alt_m else ""
    return src, alt


def _url_absoluta(candidato: str, base_url: str) -> str | None:
    u = (candidato or "").strip().strip('"').strip("'")
    if not u or u.startswith("data:") or u.startswith("javascript:"):
        return None
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith(("http://", "https://")):
        return u
    if not base_url or u.startswith("#"):
        return None
    try:
        return urljoin(base_url, u)
    except Exception:
        return None


def _parece_ruido(url: str) -> bool:
    ul = url.lower()
    return any(s in ul for s in _SUBSTR_REJEITAR)


def _parece_imagem(url: str) -> bool:
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")):
        return True
    if "/image" in path or "/img" in path or "/foto" in path or "/photo" in path or "/media" in path:
        return True
    return not _parece_ruido(url)


def _alt_e_foto_principal(alt: str) -> bool:
    """True se o alt indica explicitamente a 1.ª foto da galeria (não 10, 15, etc.)."""
    if not (alt or "").strip():
        return False
    return bool(_RE_ALT_FOTO_PRINCIPAL.search(alt))


def _candidatos_validos(markdown: str, base_url: str) -> list[tuple[int, str, str]]:
    """
    Lista (ordem_documento, alt, url_absoluta) só entradas que passam ruído + parece imagem.
    Ordem: primeiro todas as ![alt](url), depois <img>.
    """
    texto = markdown or ""
    base = (base_url or "").strip()
    out: list[tuple[int, str, str]] = []
    pos = 0

    for m in _RE_IMG_MARKDOWN.finditer(texto):
        alt = m.group(1) or ""
        raw = _primeiro_token_url(m.group(2))
        if not raw:
            continue
        absu = _url_absoluta(raw, base)
        if not absu or _parece_ruido(absu) or not _parece_imagem(absu):
            continue
        out.append((pos, alt, absu))
        pos += 1

    for m in _RE_IMG_TAG.finditer(texto):
        src, alt = _parse_img_src_alt(m.group(1))
        if not src:
            continue
        raw = src.strip()
        absu = _url_absoluta(raw, base)
        if not absu or _parece_ruido(absu) or not _parece_imagem(absu):
            continue
        out.append((pos, alt, absu))
        pos += 1

    return out


def extrair_url_foto_imovel_markdown(markdown: str, base_url: str = "") -> str | None:
    """
    URL da foto principal no markdown.

    Prioridade: primeira imagem cujo texto alternativo indica a foto **1** da galeria
    (ex.: ``![Imagem 1 do Leilão …](url)`` — limites de palavra evitam confundir com
    "Imagem 15"). Se não houver, usa a primeira imagem plausível na ordem do documento.
    """
    cands = _candidatos_validos(markdown, base_url)
    if not cands:
        return None

    for _, alt, absu in cands:
        if _alt_e_foto_principal(alt):
            logger.debug("markdown_foto_imovel: escolhida foto principal (alt) url=%s", absu[:120])
            return absu

    escolhida = cands[0][2]
    logger.debug("markdown_foto_imovel: escolhida primeira imagem url=%s", escolhida[:120])
    return escolhida
