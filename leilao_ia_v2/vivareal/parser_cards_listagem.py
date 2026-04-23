"""Extrai cards de anúncios do markdown de listagem do Viva Real."""

from __future__ import annotations

import logging
import re
from typing import Any

from leilao_ia_v2.services.geocoding import extrair_logradouro_de_url, sanear_logradouro_markdown_card
from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

logger = logging.getLogger(__name__)

_RE_URL_TERRENO = re.compile(r"/(?:lote-terreno|terreno|lote)/", re.IGNORECASE)
_RE_PRECO_BLOCO = re.compile(r"R\$\s*([\d.]+(?:,\d+)?)", re.IGNORECASE)


def _parse_preco_vr(raw: str) -> float | None:
    s = (raw or "").strip()
    if not s:
        return None
    parts = s.split(".")
    if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
        s = s.replace(".", "")
    s = s.replace(",", ".")
    try:
        v = float(s)
        return v if 30_000 <= v <= 120_000_000 else None
    except ValueError:
        return None


def _melhor_preco_no_bloco(block: str, area_start: int) -> tuple[re.Match[str] | None, float | None]:
    """Prefere R$ após a linha de área (evita preço de card vizinho ou taxa no topo)."""
    matches = list(_RE_PRECO_BLOCO.finditer(block))
    valid: list[tuple[re.Match[str], float]] = []
    for m in matches:
        v = _parse_preco_vr(m.group(1))
        if v is None:
            continue
        valid.append((m, v))
    if not valid:
        return None, None
    after = [(m, v) for m, v in valid if m.start() >= area_start]
    if after:
        m, v = after[-1]
        return m, v
    m, v = valid[-1]
    return m, v


def _detectar_tipo_por_card(url: str, titulo: str, block: str) -> str:
    if _RE_URL_TERRENO.search(url):
        return "terreno"
    bl = (titulo + " " + block).lower()
    if any(x in bl for x in ("terreno", "lote ", "loteamento")):
        if not any(x in bl for x in ("casa", "sobrado", "apartamento")):
            return "terreno"
    return ""


def extrair_cards_anuncios_vivareal_markdown(
    markdown: str,
    *,
    cidade_ref: str,
    estado_ref: str,
    bairro_ref: str,
) -> list[dict[str, Any]]:
    """
    Extrai anúncios do markdown de uma listagem (padrão ``Contatar](`` dos cards).

    Campos típicos: ``url_anuncio``, ``area_m2``, ``valor_venda``, ``logradouro``,
    ``titulo``, ``bairro``, ``cidade``, ``estado``, ``_tipo_detectado``.
    """
    anuncios: list[dict[str, Any]] = []
    cards = markdown.split("Contatar](")
    for i, block in enumerate(cards[:-1]):
        try:
            url_part = cards[i + 1].split(")")[0].split('"')[0].strip()
            if not url_part.startswith("http"):
                url_part = "https://www.vivareal.com.br" + url_part

            area_m = re.search(r"Tamanho do im[óo]vel\s*(\d{1,6})\s*m", block, re.IGNORECASE)
            quartos_m = re.search(r"Quantidade de quartos\s*(\d{1,2})", block, re.IGNORECASE)
            vagas_m = re.search(r"Quantidade de vagas[^\d]*(\d{1,2})", block, re.IGNORECASE)

            if not area_m:
                continue
            area = float(area_m.group(1))
            _pm, preco = _melhor_preco_no_bloco(block, area_m.start())
            if preco is None:
                continue

            if preco < 30_000 or preco > 120_000_000 or area < 12 or area > 50_000:
                continue

            rua_m = re.search(
                r"\n\s*(Rua|Avenida|Av\.|R\.|Alameda|Al\.|Travessa|Tv\.|Estrada|Rod\.|Rodovia|Largo|Praça|Pc\.|Servidão|Beco)[^\n]{3,80}",
                block,
                re.IGNORECASE,
            )
            logradouro = rua_m.group(0).strip() if rua_m else ""
            if not logradouro:
                addr_m = re.search(
                    r"\n\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,5}),\s*\d",
                    block,
                )
                if addr_m:
                    logradouro = addr_m.group(1).strip()
            if not logradouro:
                logradouro = extrair_logradouro_de_url(url_part)
            logradouro = sanear_logradouro_markdown_card(logradouro)

            titulo_m = re.search(r"\*\*([^\*]+)\*\*", block)
            titulo = titulo_m.group(1).strip()[:500] if titulo_m else ""

            tipo_card = _detectar_tipo_por_card(url_part, titulo, block)
            uf2 = estado_livre_para_sigla_uf(estado_ref)

            anuncios.append(
                {
                    "url_anuncio": url_part.split("?")[0],
                    "portal": "vivareal.com.br",
                    "area_m2": area,
                    "valor_venda": preco,
                    "quartos": int(quartos_m.group(1)) if quartos_m else None,
                    "vagas": int(vagas_m.group(1)) if vagas_m else None,
                    "logradouro": logradouro,
                    "titulo": titulo,
                    "bairro": bairro_ref,
                    "cidade": cidade_ref,
                    "estado": uf2,
                    "_tipo_detectado": tipo_card,
                }
            )
        except (ValueError, IndexError, AttributeError):
            continue

    logger.info("Parser Viva Real: %s anúncios extraídos da listagem", len(anuncios))
    return anuncios
