"""Extrai anúncios (cards) de markdown de portais — Viva Real + heurística genérica."""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from leilao_ia_v2.services.geocoding import (
    extrair_logradouro_de_url,
    extrair_logradouro_do_titulo_imovel,
    melhor_logradouro_janela_proximo_url,
    sanear_logradouro_markdown_card,
)
from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

logger = logging.getLogger(__name__)

_RE_MD_LINK = re.compile(r"\[([^\]]{0,400})\]\((https?://[^)\s]{8,800})\)")
_RE_ANGLE_LINK = re.compile(r"<(https?://[^>\s]{10,800})>")
_RE_HTTP = re.compile(r"https?://[^\s\)\]\"'<>]+", re.I)
_RE_JSON_URL = re.compile(
    r'"(?:url|link|@id|canonicalUrl|shareUrl)"\s*:\s*"(https?://(?:www\.)?(?:zapimoveis|quintoandar|imovelweb|chavesnamao|olx|loft|vivareal|kenlo)\.com\.br[^"]{8,800})"',
    re.I,
)
_RE_PRECO = re.compile(r"R\$\s*([\d]{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?)", re.I)
# m² com ou sem espaço; área decimal BR (95,5); prefixos típicos Zap/Chaves/ImovelWeb
_RE_AREA_FLEX = re.compile(
    r"(?:\b(?:área|metragem)\s*(?:útil|total|privativa|constru[ií]da|bruta)?\s*:?\s*)?"
    r"([\d]{1,2}(?:\.\d{3})+|\d{2,4}(?:[.,]\d{1,2})?)[\s\u00a0]*m(?:²|2)\b",
    re.I,
)
# Linhas que costumam ser taxa/condomínio (evitar confundir com valor de venda)
_RE_CTX_TAXA = re.compile(
    r"(similares|condom[ií]nio|taxa|iptu|administrativ|financi|parcela|entrada|"
    r"\/m[eê]s|por\s*m[eê]s|valor\s*suger|refer[eê]ncia|estimativa)",
    re.I,
)


def _parse_preco_br(raw: str) -> float | None:
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


def _parse_area_m2_br(raw: str) -> float | None:
    """Metragem em m² (inteiro, decimal BR 95,5 ou milhar 1.536 como em listagens Chaves)."""
    s0 = (raw or "").strip().replace("\u00a0", " ")
    if not s0:
        return None
    s = s0.replace(" ", "")
    parts = s.split(".")
    if len(parts) > 1 and parts[0].isdigit() and all(len(p) == 3 and p.isdigit() for p in parts[1:]):
        try:
            v = float("".join(parts))
        except ValueError:
            return None
        return v if 12 <= v <= 50_000 else None
    if re.fullmatch(r"\d{2,4}", s):
        v = float(s)
    elif re.fullmatch(r"\d{2,4}[.,]\d{1,2}", s):
        v = float(s.replace(",", "."))
    else:
        return None
    return v if 12 <= v <= 50_000 else None


def _areas_na_janela(janela: str) -> list[tuple[int, float]]:
    out: list[tuple[int, float]] = []
    for m in _RE_AREA_FLEX.finditer(janela):
        a = _parse_area_m2_br(m.group(1))
        if a is not None:
            out.append((m.start(), a))
    return out


def _preco_proximo_de_taxa(janela: str, p_start: int, p_end: int) -> bool:
    """Trecho antes do R$ menciona condomínio/taxa/mês — preço provavelmente não é venda."""
    ctx = janela[max(0, p_start - 80) : p_end + 40]
    return bool(_RE_CTX_TAXA.search(ctx))


def _melhor_preco_area_na_janela(
    janela: str,
    url_index_in_janela: int,
    *,
    prefer_depois_url: bool = False,
) -> tuple[float | None, float | None]:
    """
    Escolhe par (preço, área) cuja posição textual está mais próxima do índice da URL na janela.
    Evita o primeiro R$ da janela quando o valor correto está perto do link do anúncio.
    ``prefer_depois_url`` (Loft): só considera tokens **depois** da URL quando existirem — o scrape
    costuma trazer similares ou taxas antes do link.
    """
    precos: list[tuple[int, int, float]] = []
    for m in _RE_PRECO.finditer(janela):
        v = _parse_preco_br(m.group(1))
        if v is None:
            continue
        if _preco_proximo_de_taxa(janela, m.start(), m.end()):
            continue
        precos.append((m.start(), m.end(), v))

    areas = _areas_na_janela(janela)

    if not precos or not areas:
        return None, None

    cut = max(0, url_index_in_janela - 25)
    precos_eff: list[tuple[int, int, float]] = precos
    areas_eff: list[tuple[int, float]] = areas
    if prefer_depois_url:
        pd = [(p0, p1, v) for p0, p1, v in precos if p0 >= cut]
        ad = [(a0, a) for a0, a in areas if a0 >= cut]
        if pd and ad:
            precos_eff = pd
            areas_eff = ad

    best_p: float | None = None
    best_a: float | None = None
    best_score = 1e18
    max_sep = 380
    for p0, _p1, preco in precos_eff:
        for a0, area in areas_eff:
            sep = abs(p0 - a0)
            if sep > max_sep:
                continue
            mid = (p0 + a0) // 2
            score = sep + abs(mid - url_index_in_janela)
            if score < best_score:
                best_score = score
                best_p, best_a = preco, area

    if best_p is not None and best_a is not None:
        return best_p, best_a

    # fallback: primeiro preço válido (não taxa) + primeira área próxima
    for p0, _p1, preco in precos_eff:
        for a0, area in areas_eff:
            if abs(p0 - a0) <= max_sep:
                return preco, area
    return precos_eff[0][2], areas_eff[0][1]


def _portal_de_url(url: str) -> str:
    try:
        h = urlparse(url).netloc.lower().replace("www.", "")
        return h[:200] if h else "desconhecido"
    except Exception:
        return "desconhecido"


def _normalizar_url_candidata(raw: str) -> str:
    u = (raw or "").strip().rstrip(").,;]'\"")
    if not u.startswith("http"):
        return ""
    u = u.split("#", 1)[0].strip()
    return u


def _excluir_hub_listagem_portal(url: str) -> bool:
    """
    Páginas de resultado (rua/bairro/cidade) que não são ficha de imóvel — geram ruído sem cards parseáveis.
    """
    u = (url or "").lower()
    if not u:
        return True
    # Página de listagem SEO: /comprar/imovel/{slug-cidade-uf-brasil}/{casa|apartamento|…}
    if "quintoandar.com.br" in u and re.search(
        r"/comprar/imovel/[^?]+/(casa|apartamento|kitnet|sobrado|studio|cobertura)(?:\?|$)",
        u,
        re.I,
    ):
        return True
    if "zapimoveis.com.br" in u and "/imovel/" not in u and ("/imoveis/" in u or "/imóveis/" in u or "imoveis/" in u):
        return True
    if "imovelweb.com.br" in u and "/imovel/" not in u and "/oferta/" not in u:
        if "-drc-" in u or "/casas-venda-" in u or "/apartamentos-venda-" in u or "/casas-aluguel-" in u:
            return True
    if "chavesnamao.com.br" in u and "/imovel/" not in u and ("/bairros/" in u or "/ruas/" in u or "/regiao/" in u):
        return True
    if "olx.com.br" in u and "/d-" not in u and "/item/" not in u and "/vi-" not in u:
        if "/imoveis/" in u or "/imóveis/" in u or "/listagem" in u:
            return True
    return False


def _url_eh_aluguel_obvio(u: str) -> bool:
    """Fichas de locação (comparáveis de venda não devem misturar)."""
    ul = (u or "").lower()
    if re.search(
        r"(aluguel|/alugar|loca[cç][aã]o|/rent/|-para-alugar|"
        r"imoveis/aluguel|imóveis/aluguel|/rent-)",
        ul,
    ):
        return True
    return False


def _parece_url_de_anuncio(url: str) -> bool:
    if _excluir_hub_listagem_portal(url):
        return False
    u = (url or "").lower()
    if _url_eh_aluguel_obvio(u):
        return False
    if "vivareal.com.br" in u and ("/imovel/" in u or "/imovel-" in u or "listing" in u):
        return True
    if "zapimoveis.com.br" in u and "/imovel/" in u:
        return True
    if "imovelweb.com.br" in u and ("/imovel/" in u or "/oferta/" in u):
        return True
    if "olx.com.br" in u and ("/d-" in u or "/item/" in u or "/vi-" in u):
        return True
    if "chavesnamao.com.br" in u and "/imovel/" in u:
        return True
    if "quintoandar.com.br" in u and "/imovel/" in u:
        return True
    if "loft.com.br" in u and "/imovel/" in u:
        return True
    if "kenlo.com.br" in u and ("/imovel/" in u or "/imoveis/" in u):
        return True
    return False


def _inicio_card_chaves_megalink(md: str, pos: int) -> int:
    """
    Listagens Chaves: cada card costuma começar com ``[![`` (imagens + texto + URL da ficha).
    Devolve o índice do último ``[![`` antes de ``pos``, ou -1.
    """
    lo = max(0, pos - 8000)
    start = -1
    i = lo
    while True:
        k = md.find("[![", i, pos)
        if k < 0:
            break
        start = k
        i = k + 3
    return start


def _limites_janela_card(md: str, pos: int, url: str) -> tuple[int, int]:
    """Recorta o markdown para o bloco do card (evita vazar card vizinho na listagem Chaves)."""
    ul = (url or "").lower()
    if "chavesnamao.com.br/imovel/" in ul:
        cs = _inicio_card_chaves_megalink(md, pos)
        if cs >= 0:
            i0 = cs
        else:
            i0 = max(0, pos - 750)
        # Mega-link: preço/m² vêm na mesma linha ou linhas seguintes (``pos+140`` cortava ``1.890.000``).
        i1 = min(len(md), max(pos + 220, i0 + 950))
        return i0, i1
    return max(0, pos - 1200), min(len(md), pos + 700)


def _slug_venda_sem_rua_na_url(url: str) -> bool:
    """
    Slug típico Zap/Loft/Chaves: ``venda-casa-bairro-cidade-id`` (sem ``rua-`` / ``avenida-``).
    Não deve virar título nem logradouro — não é endereço.
    """
    try:
        path = urlparse(url).path or ""
    except Exception:
        return False
    ul = (url or "").lower()
    m = re.search(r"/imovel/([^?#]+)", path, flags=re.I)
    if not m:
        return False
    seg = m.group(1).lower()
    if re.search(
        r"(^|-)(rua|avenida|av\.|alameda|al\.|travessa|tv\.|estrada|rodovia|rod\.|"
        r"pra[cç]a|pc\.|largo|beco|servidao)(-|$)",
        seg,
    ):
        return False
    if "chavesnamao.com.br" in ul:
        # Slug de ficha: ``terreno-a-venda-sc-…`` ou ``venda-sobrado-bairro-id`` (não é logradouro)
        if "-a-venda-" in seg or "-para-alugar-" in seg:
            return True
        if re.match(
            r"^(?:venda|terreno|casa|apartamento|predio|sobrado|studio|kit|"
            r"cobertura|loft|imoveis?|imovel)-(?:a-venda|para-alugar)",
            seg,
        ):
            return True
        return bool(
            re.match(
                r"^venda-(?:apartamento|casa|sobrado|studio|kitnet|kit|cobertura|loft|"
                r"terreno|lote|imoveis?|imovel)\b",
                seg,
            )
        )
    if "zapimoveis.com.br" in ul or "loft.com.br" in ul:
        return bool(
            re.match(
                r"^venda-(?:apartamento|casa|sobrado|studio|kitnet|kit|cobertura|loft|"
                r"terreno|lote|imoveis?|imovel)",
                seg,
            )
        )
    return False


def _titulo_heuristico_proximo(md: str, pos: int, url: str) -> str:
    """Título aproximado: negrito ou linha antes da URL."""
    i0 = max(0, pos - 220)
    bloco = md[i0:pos]
    m = re.search(r"\*\*([^*]{4,200})\*\*\s*$", bloco)
    if m:
        return m.group(1).strip()[:500]
    linhas = bloco.strip().split("\n")
    if linhas:
        cand = linhas[-1].strip()
        cand = re.sub(r"^#+\s*", "", cand).strip()
        if 8 < len(cand) < 200 and not cand.lower().startswith("http"):
            return cand[:500]
    try:
        path = urlparse(url).path.strip("/").split("/")[-1]
        if path and len(path) > 6 and not _slug_venda_sem_rua_na_url(url):
            return path.replace("-", " ")[:500]
    except Exception:
        pass
    return ""


def _titulo_alt_imagem_proximo(md: str, pos: int) -> str:
    """Tenta usar ALT da imagem próxima ao URL quando link textual é ruim (ex.: 'Mensagem')."""
    i0 = max(0, pos - 280)
    i1 = min(len(md), pos + 260)
    jan = md[i0:i1]
    m = re.search(r"!\[([^\]]{8,220})\]", jan)
    if not m:
        return ""
    alt = " ".join(str(m.group(1) or "").split()).strip()
    alt = re.sub(r"^\s*(mensagem|contatar|ver telefone)\s*$", "", alt, flags=re.I).strip()
    return alt[:500]


def _titulo_sem_valor_monetario_prefixo(titulo: str) -> str:
    """Evita usar título que começa com preço como se fosse logradouro."""
    t = (titulo or "").strip()
    if not t:
        return ""
    t2 = re.sub(
        r"^R\$\s*[\d]{1,3}(?:\.\d{3})*(?:,\d{2})?\s*[\-–·|,:]*\s*",
        "",
        t,
        flags=re.I,
    ).strip()
    return t2 if len(t2) >= 6 else t


def _iter_candidatos_url_markdown(md: str) -> list[tuple[str, int, str]]:
    """
    (url, posição, titulo) — prioriza links ``[texto](url)``, depois ``<url>``, JSON e URLs http brutas.
    """
    vistos: set[str] = set()
    candidatos: list[tuple[str, int, str]] = []

    def _add(url: str, pos: int, titulo: str) -> None:
        u = _normalizar_url_candidata(url)
        if not u or u in vistos:
            return
        if not _parece_url_de_anuncio(u):
            return
        vistos.add(u)
        candidatos.append((u, pos, titulo))

    for m in _RE_MD_LINK.finditer(md):
        _add(m.group(2), m.start(), (m.group(1) or "").strip())

    for m in _RE_ANGLE_LINK.finditer(md):
        _add(m.group(1), m.start(), "")

    for m in _RE_JSON_URL.finditer(md):
        _add(m.group(1), m.start(), "")

    for m in _RE_HTTP.finditer(md):
        _add(m.group(0), m.start(), "")

    candidatos.sort(key=lambda x: x[1])
    return candidatos


def _card_de_url_janela(
    url: str,
    pos: int,
    titulo_ini: str,
    md: str,
    *,
    cidade_ref: str,
    estado_ref: str,
    bairro_ref: str,
    uf2: str,
) -> dict[str, Any] | None:
    """Extrai um card se, na vizinhança da URL, existirem R$ e m² válidos."""
    i0, i1 = _limites_janela_card(md, pos, url)
    janela = md[i0:i1]
    titulo = (titulo_ini or "").strip()
    if re.fullmatch(r"(?i)\s*(mensagem|contatar|ver telefone|mensagem\]\(?\s*)\s*", titulo or ""):
        titulo = ""
    if not titulo:
        titulo = _titulo_alt_imagem_proximo(md, pos) or _titulo_heuristico_proximo(md, pos, url)

    url_in_janela = pos - i0
    portal = _portal_de_url(url)
    prefer_loft = portal == "loft.com.br"
    if 0 <= url_in_janela < len(janela):
        preco, area = _melhor_preco_area_na_janela(
            janela, url_in_janela, prefer_depois_url=prefer_loft
        )
    else:
        preco, area = None, None

    if preco is None or area is None:
        pm = _RE_PRECO.search(janela) or _RE_PRECO.search(titulo)
        areas_fb = _areas_na_janela(janela)
        if not areas_fb and titulo:
            areas_fb = _areas_na_janela(titulo)
        if not pm or not areas_fb:
            return None
        preco = _parse_preco_br(pm.group(1))
        if preco is None:
            return None
        area = areas_fb[0][1]

    if area < 12 or area > 50_000:
        return None

    u_anchor = url_in_janela if 0 <= url_in_janela < len(janela) else len(janela) // 2
    logr = melhor_logradouro_janela_proximo_url(janela, u_anchor)
    if not logr:
        logr = extrair_logradouro_do_titulo_imovel(titulo)
    if not logr:
        logr = extrair_logradouro_de_url(url)
    if not logr and not _slug_venda_sem_rua_na_url(url):
        titulo_logr = _titulo_sem_valor_monetario_prefixo(titulo)
        if titulo_logr and not re.fullmatch(r"\d{6,24}", titulo_logr.strip().replace(" ", "")):
            logr = titulo_logr[:120]
    logr = sanear_logradouro_markdown_card(logr)
    return {
        "url_anuncio": url,
        "portal": portal,
        "area_m2": area,
        "valor_venda": preco,
        "logradouro": (logr or "")[:240],
        "titulo": titulo[:500],
        "bairro": bairro_ref,
        "cidade": cidade_ref,
        "estado": uf2,
        "_tipo_detectado": "",
    }


def extrair_anuncios_markdown_generico(
    markdown: str,
    *,
    cidade_ref: str,
    estado_ref: str,
    bairro_ref: str,
    url_pagina: str = "",
) -> list[dict[str, Any]]:
    """
    Heurística: URLs de anúncios (link Markdown, ``<url>``, JSON ou texto plano) + janela com R$ e m².

    Cobertura típica de markdown Firecrawl: **Zap**, **Chaves na Mão**, **ImovelWeb** (``/imovel/`` e
    ``/oferta/``), **QuintoAndar**, **OLX** (``/d-``, ``/item/``, ``/vi-``), **Loft**, **Viva Real**
    (listagens com ``Contatar]`` usam parser dedicado em ``extrair_anuncios_do_markdown_pagina``).

    Metragem: aceita ``área útil/total``, decimal BR (``95,5 m²``), ``120m²`` sem espaço, e par
    preço+área mais próximo do URL (evita ``R$`` de similares/taxas).
    """
    md = markdown or ""
    uf2 = estado_livre_para_sigla_uf(estado_ref) or str(estado_ref or "").strip()[:2].upper()
    out: list[dict[str, Any]] = []

    for url, pos, tit in _iter_candidatos_url_markdown(md):
        card = _card_de_url_janela(url, pos, tit, md, cidade_ref=cidade_ref, estado_ref=estado_ref, bairro_ref=bairro_ref, uf2=uf2)
        if card:
            out.append(card)

    logger.info(
        "Parser genérico: %s anúncios (urls md/texto/json) url_pagina=%s",
        len(out),
        url_pagina[:80],
    )
    return out


def extrair_anuncios_do_markdown_pagina(
    markdown: str,
    *,
    url_pagina: str,
    cidade_ref: str,
    estado_ref: str,
    bairro_ref: str,
) -> list[dict[str, Any]]:
    """
    Escolhe o parser adequado conforme o conteúdo (Viva Real listagem com cards ``Contatar]``).
    """
    if "vivareal.com.br" in url_pagina.lower() and "Contatar](" in (markdown or ""):
        from leilao_ia_v2.vivareal.parser_cards_listagem import extrair_cards_anuncios_vivareal_markdown

        return extrair_cards_anuncios_vivareal_markdown(
            markdown,
            cidade_ref=cidade_ref,
            estado_ref=estado_ref,
            bairro_ref=bairro_ref,
        )
    return extrair_anuncios_markdown_generico(
        markdown,
        cidade_ref=cidade_ref,
        estado_ref=estado_ref,
        bairro_ref=bairro_ref,
        url_pagina=url_pagina,
    )


def dedupe_por_url(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    vistos: set[str] = set()
    out: list[dict[str, Any]] = []
    for c in cards:
        u = str(c.get("url_anuncio") or "").strip()
        if not u or u in vistos:
            continue
        vistos.add(u)
        out.append(c)
    return out
