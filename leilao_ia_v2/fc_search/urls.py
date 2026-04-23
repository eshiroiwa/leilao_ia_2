"""Seleção de URLs de portais a partir dos resultados da pesquisa Firecrawl."""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlparse

# Portais-alvo (substring no host). Ordem = prioridade aproximada.
_PORTAIS_ACEITES: tuple[str, ...] = (
    "vivareal.com.br",
    "zapimoveis.com.br",
    "imovelweb.com.br",
    "olx.com.br",
    "chavesnamao.com.br",
    "quintoandar.com.br",
    "loft.com.br",
)

# Ordem ao escolher URLs para scrape: Viva Real por último para não monopolizar
# quando a busca devolve muitas páginas VR antes dos outros portais.
_PORTAIS_ORDEM_SCRAPE: tuple[str, ...] = (
    "zapimoveis.com.br",
    "imovelweb.com.br",
    "olx.com.br",
    "chavesnamao.com.br",
    "quintoandar.com.br",
    "loft.com.br",
    "vivareal.com.br",
)

_EXCLUIR_HOST: tuple[str, ...] = (
    "google.",
    "gstatic.",
    "youtube.",
    "facebook.",
    "instagram.",
    "whatsapp.",
    "wikipedia.org",
    "gov.br",
)


def _host_ok(host: str) -> bool:
    h = (host or "").lower()
    if not h:
        return False
    if any(x in h for x in _EXCLUIR_HOST):
        return False
    return any(p in h for p in _PORTAIS_ACEITES)


def _candidate_url_de_item_resultado_busca(it: dict[str, Any]) -> str:
    """
    URL da página a visitar a partir de um item do array ``web`` do Search.

    Resultados “simples” trazem ``url`` no topo; quando a API devolve pré-visualização
    tipo ``Document`` (markdown/metadata), o URL costuma estar em ``metadata``.
    """
    if not isinstance(it, dict):
        return ""
    u = normalizar_url_resultado(str(it.get("url") or ""))
    if u:
        return u
    meta = it.get("metadata")
    if isinstance(meta, dict):
        for key in ("url", "source_url", "sourceUrl", "og_url"):
            u = normalizar_url_resultado(str(meta.get(key) or ""))
            if u:
                return u
    return ""


def normalizar_url_resultado(url: str) -> str:
    u = (url or "").strip()
    if not u.startswith("http"):
        return ""
    try:
        p = urlparse(u)
        if p.scheme not in ("http", "https") or not p.netloc:
            return ""
        return u.split("#", 1)[0].strip()
    except Exception:
        return ""


def extrair_urls_da_busca(web_items: list[dict[str, Any]]) -> list[str]:
    """URLs principais devolvidas pelo endpoint ``search`` (campo ``web``)."""
    out: list[str] = []
    for it in web_items or []:
        if not isinstance(it, dict):
            continue
        u = _candidate_url_de_item_resultado_busca(it)
        if u and _host_ok(urlparse(u).netloc):
            out.append(u)
    return out


_RE_HTTP = re.compile(r"https?://[^\s\)\]\"'<>]+", re.I)


def extrair_urls_do_markdown(markdown: str) -> list[str]:
    """URLs http(s) encontradas no texto (descrições agregadas ou markdown de preview)."""
    if not (markdown or "").strip():
        return []
    found = _RE_HTTP.findall(markdown)
    out: list[str] = []
    for u in found:
        u = u.rstrip(").,;]")
        nu = normalizar_url_resultado(u)
        if nu and _host_ok(urlparse(nu).netloc):
            out.append(nu)
    return out


def _host_base_de_url(u: str) -> str:
    try:
        return urlparse(u).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def selecionar_urls_para_scrape(urls: list[str], *, max_urls: int | None = None) -> list[str]:
    """
    Até ``max_urls`` hosts distintos.

    Primeiro tenta uma URL por portal na ordem ``_PORTAIS_ORDEM_SCRAPE`` (diversidade),
    depois completa com hosts ainda não usados na ordem original de ``urls``.
    """
    lim = max_urls if max_urls is not None else int(os.getenv("FC_SEARCH_MAX_SCRAPE_URLS", "5") or "5")
    lim = max(1, min(12, lim))
    vistos_host: set[str] = set()
    escolhidas: list[str] = []

    for portal in _PORTAIS_ORDEM_SCRAPE:
        if len(escolhidas) >= lim:
            break
        for u in urls:
            base = _host_base_de_url(u)
            if not base or base in vistos_host:
                continue
            if portal not in base:
                continue
            vistos_host.add(base)
            escolhidas.append(u)
            break

    for u in urls:
        if len(escolhidas) >= lim:
            break
        base = _host_base_de_url(u)
        if not base or base in vistos_host:
            continue
        vistos_host.add(base)
        escolhidas.append(u)
    return escolhidas
