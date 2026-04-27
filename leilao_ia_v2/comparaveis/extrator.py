"""
Extrator de cards de anúncios a partir de markdown — **sem inventar cidade**.

Diferenças críticas em relação ao parser antigo
(`leilao_ia_v2/fc_search/parser.py::_card_de_url_janela`):

- O parser antigo recebia ``cidade_ref`` / ``estado_ref`` / ``bairro_ref`` (do
  leilão original) e atribuía esses valores a **todos** os cards extraídos,
  independente do conteúdo da página. Foi a causa-raiz #1 do bug
  Pindamonhangaba → São Bernardo.
- Este novo extrator devolve apenas o que está **provado** no markdown:
  ``url, portal, valor_venda, area_m2, titulo, logradouro_inferido,
  bairro_inferido``. **Cidade, UF e bairro definitivos são preenchidos pela
  validação por geocode** (módulo :mod:`comparaveis.validacao_cidade`).

Mantém a heurística "preço e área mais próximos da URL na janela" porque
funciona bem para os portais (Zap, Quinto Andar, OLX, Loft, Imovelweb,
Chaves na Mão), mas **a janela é mais conservadora** e os filtros de URL
foram simplificados para o módulo ser facilmente testável.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Regex
# -----------------------------------------------------------------------------

_RE_MD_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]+\)")
_RE_MD_LINK = re.compile(r"\[([^\]]{0,400})\]\((https?://[^)\s]{8,800})\)")
_RE_ANGLE_LINK = re.compile(r"<(https?://[^>\s]{10,800})>")
_RE_HTTP_NU = re.compile(r"https?://[^\s\)\]\"'<>]+", re.I)
_RE_TITULO_GENERICO = re.compile(
    r"^\s*(mensagem|contatar|ver telefone|ver detalhes|ver im[óo]vel|saiba mais|"
    r"detalhes|mais informa[çc][õo]es|veja mais|veja|clique)\s*$",
    re.IGNORECASE,
)

_RE_PRECO = re.compile(
    r"R\$\s*([\d]{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?)", re.I
)
_RE_AREA_FLEX = re.compile(
    r"(?:\b(?:área|metragem)\s*(?:útil|total|privativa|constru[ií]da|bruta)?\s*:?\s*)?"
    r"([\d]{1,2}(?:\.\d{3})+|\d{2,4}(?:[.,]\d{1,2})?)[\s\u00a0]*m(?:²|2)\b",
    re.I,
)
_RE_CTX_TAXA = re.compile(
    r"(similares|condom[ií]nio|taxa|iptu|administrativ|financi|parcela|entrada|"
    r"\/m[eê]s|por\s*m[eê]s|valor\s*suger|refer[eê]ncia|estimativa)",
    re.I,
)
_RE_BAIRRO_TITULO = re.compile(
    r"(?:bairro|no\s+bairro)\s+([A-Za-zÀ-ÿ][\wÀ-ÿ\s\-']{2,60})", re.I
)


# Faixa de plausibilidade — fora disto é geralmente lixo (placeholder, OLX a brincar).
_VENDA_MIN, _VENDA_MAX = 30_000.0, 120_000_000.0
_AREA_MIN, _AREA_MAX = 12.0, 50_000.0


# Portais aceites — qualquer outro domínio é descartado para evitar lixo
# (blogs, fóruns, listagens estatais, etc.).
_PORTAIS_ACEITES = (
    "vivareal.com.br",
    "zapimoveis.com.br",
    "imovelweb.com.br",
    "olx.com.br",
    "chavesnamao.com.br",
    "quintoandar.com.br",
    "loft.com.br",
    "kenlo.com.br",
    "redeleilao.com.br",
    "mercadolivre.com.br",
)


# -----------------------------------------------------------------------------
# Dataclass de saída
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CardExtraido:
    """Card cru extraído do markdown — pendente de validação por geocode.

    Atenção: ``cidade``, ``estado_uf`` e ``bairro_confirmado`` são preenchidos
    posteriormente pelo pipeline (em :mod:`comparaveis.persistencia`) com base
    no resultado de :func:`comparaveis.validacao_cidade.validar_municipio_card`.
    """

    url_anuncio: str
    portal: str
    valor_venda: float
    area_m2: float
    titulo: str = ""
    logradouro_inferido: str = ""
    bairro_inferido: str = ""

    @property
    def preco_m2(self) -> float:
        return round(self.valor_venda / self.area_m2, 2) if self.area_m2 > 0 else 0.0


# -----------------------------------------------------------------------------
# Parsing primitivo
# -----------------------------------------------------------------------------

def _parse_preco_br(raw: str) -> Optional[float]:
    """Aceita formato BR canónico (`1.250.000,00`), com milhar (`350.000`) ou plano (`350`)."""
    s = (raw or "").strip()
    if not s:
        return None
    if "," in s:
        inteiro, _, decimal = s.partition(",")
        s = inteiro.replace(".", "") + "." + decimal
    else:
        partes = s.split(".")
        if len(partes) > 1 and all(len(p) == 3 for p in partes[1:]):
            s = s.replace(".", "")
    try:
        v = float(s)
    except ValueError:
        return None
    return v if _VENDA_MIN <= v <= _VENDA_MAX else None


def _parse_area_m2_br(raw: str) -> Optional[float]:
    s0 = (raw or "").strip().replace("\u00a0", " ")
    if not s0:
        return None
    s = s0.replace(" ", "")
    parts = s.split(".")
    if (
        len(parts) > 1
        and parts[0].isdigit()
        and all(len(p) == 3 and p.isdigit() for p in parts[1:])
    ):
        try:
            v = float("".join(parts))
        except ValueError:
            return None
        return v if _AREA_MIN <= v <= _AREA_MAX else None
    if re.fullmatch(r"\d{2,4}", s):
        v = float(s)
    elif re.fullmatch(r"\d{2,4}[.,]\d{1,2}", s):
        v = float(s.replace(",", "."))
    else:
        return None
    return v if _AREA_MIN <= v <= _AREA_MAX else None


# -----------------------------------------------------------------------------
# Filtros de URL (mais simples que o velho)
# -----------------------------------------------------------------------------

def _portal_de_url(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        return host[:120] if host else ""
    except Exception:
        return ""


def _url_eh_aluguel(url: str) -> bool:
    u = (url or "").lower()
    return bool(
        re.search(
            r"(aluguel|/alugar|loca[cç][aã]o|/rent/|-para-alugar|imoveis/aluguel|imóveis/aluguel)",
            u,
        )
    )


_RE_QA_LISTAGEM = re.compile(
    r"/comprar/imovel/[^/?]+/(casa|apartamento|kitnet|sobrado|studio|cobertura)(?:[/?]|$)",
    re.IGNORECASE,
)


def _url_eh_listagem(url: str) -> bool:
    """Páginas de listagem (resultados de busca, hubs por bairro, etc.)."""
    u = (url or "").lower()
    # Quinto Andar: /comprar/imovel/cidade-uf-brasil/tipo é hub SEO de listagem,
    # mesmo contendo "/imovel/" no path.
    if "quintoandar.com.br" in u:
        if _RE_QA_LISTAGEM.search(u):
            return True
        if "/imovel/" not in u:
            return True
        return False
    if "zapimoveis.com.br" in u and "/imovel/" not in u:
        return True
    if "loft.com.br" in u and "/imovel/" not in u:
        return True
    if "vivareal.com.br" in u and "/imovel" not in u:
        return True
    if "olx.com.br" in u and not re.search(r"(/d-|/item/|/vi-)", u):
        return True
    if "imovelweb.com.br" in u and "/imovel/" not in u and "/oferta/" not in u:
        return True
    if "chavesnamao.com.br" in u and "/imovel/" not in u:
        return True
    return False


_RE_EXT_IMAGEM = re.compile(r"\.(?:jpe?g|png|gif|webp|svg|bmp|tiff?|heic|avif)(?:[?#]|$)", re.IGNORECASE)


def _url_eh_recurso_estatico(url: str) -> bool:
    """URLs que apontam para imagens, vídeos ou outros recursos — não são anúncios."""
    return bool(_RE_EXT_IMAGEM.search(url or ""))


def _portal_aceito(url: str) -> bool:
    portal = _portal_de_url(url)
    if not portal:
        return False
    return any(portal == p or portal.endswith("." + p) for p in _PORTAIS_ACEITES)


def url_eh_anuncio_aproveitavel(url: str) -> bool:
    """API pública para validar URLs antes de gastar scrape."""
    if not url:
        return False
    if _url_eh_recurso_estatico(url):
        return False
    if not _portal_aceito(url):
        return False
    if _url_eh_aluguel(url):
        return False
    if _url_eh_listagem(url):
        return False
    return True


# -----------------------------------------------------------------------------
# Janela e par preço/área
# -----------------------------------------------------------------------------

def _preco_eh_taxa(janela: str, p_start: int, p_end: int) -> bool:
    """Avalia 35 chars antes e 20 depois — janelas mais largas pegam "Condomínio"
    do card anterior na listagem e marcam erradamente o preço de venda como taxa."""
    ctx = janela[max(0, p_start - 35) : p_end + 20]
    return bool(_RE_CTX_TAXA.search(ctx))


def _melhor_par_preco_area(janela: str, ancora: int) -> tuple[Optional[float], Optional[float]]:
    """Devolve (preço, área) cuja média de posição mais se aproxima da âncora.

    A âncora é normalmente a posição da URL do anúncio na janela. Filtramos
    preços em contexto de taxa (condomínio, /mês, etc.) e exigimos que o par
    esteja dentro de ``max_sep`` caracteres entre si.
    """
    precos: list[tuple[int, float]] = []
    for m in _RE_PRECO.finditer(janela):
        if _preco_eh_taxa(janela, m.start(), m.end()):
            continue
        v = _parse_preco_br(m.group(1))
        if v is not None:
            precos.append((m.start(), v))

    areas: list[tuple[int, float]] = []
    for m in _RE_AREA_FLEX.finditer(janela):
        v = _parse_area_m2_br(m.group(1))
        if v is not None:
            areas.append((m.start(), v))

    if not precos or not areas:
        return None, None

    melhor: tuple[Optional[float], Optional[float]] = (None, None)
    melhor_score = float("inf")
    max_sep = 380
    for p_pos, preco in precos:
        for a_pos, area in areas:
            sep = abs(p_pos - a_pos)
            if sep > max_sep:
                continue
            mid = (p_pos + a_pos) // 2
            score = sep + abs(mid - ancora)
            if score < melhor_score:
                melhor_score = score
                melhor = (preco, area)

    if melhor[0] is not None:
        return melhor

    # Fallback: par mais próximo entre si, sem cuidar da âncora.
    return precos[0][1], areas[0][1]


# -----------------------------------------------------------------------------
# Inferência de bairro / logradouro a partir do próprio anúncio
# -----------------------------------------------------------------------------

def _inferir_bairro_do_titulo(titulo: str) -> str:
    if not titulo:
        return ""
    m = _RE_BAIRRO_TITULO.search(titulo)
    if m:
        return m.group(1).strip()[:80]
    return ""


def _inferir_logradouro_do_titulo(titulo: str) -> str:
    """Captura "Rua/Av/Alameda Foo" no título, sem o número."""
    if not titulo:
        return ""
    m = re.search(
        r"\b(?:Rua|R\.|Avenida|Av\.|Alameda|Al\.|Travessa|Tv\.|Estrada|Rodovia|Rod\.|Praça|Pç\.|Largo|Beco)"
        r"\s+([A-Za-zÀ-ÿ][\wÀ-ÿ\s\-']{2,80})",
        titulo,
        re.IGNORECASE,
    )
    if not m:
        return ""
    return m.group(0).strip()[:160]


def _titulo_proximo(md: str, pos: int) -> str:
    """Tenta título via negrito/heading antes da URL ou ALT de imagem próxima."""
    bloco = md[max(0, pos - 240) : pos]
    m = re.search(r"\*\*([^*]{4,200})\*\*\s*$", bloco)
    if m:
        return m.group(1).strip()[:300]
    janela_alt = md[max(0, pos - 280) : min(len(md), pos + 260)]
    m = re.search(r"!\[([^\]]{8,220})\]", janela_alt)
    if m:
        return m.group(1).strip()[:300]
    linhas = [l.strip() for l in bloco.split("\n") if l.strip()]
    if linhas:
        cand = re.sub(r"^#+\s*", "", linhas[-1]).strip()
        if 8 < len(cand) < 200 and not cand.lower().startswith("http"):
            return cand[:300]
    return ""


# -----------------------------------------------------------------------------
# Scanner de URLs no markdown
# -----------------------------------------------------------------------------

def _normalizar_url(raw: str) -> str:
    u = (raw or "").strip().rstrip(").,;]'\"")
    if not u.startswith("http"):
        return ""
    return u.split("#", 1)[0].strip()


def _mascarar_imagens(md: str) -> str:
    """Substitui ``![alt](url)`` por espaços do mesmo tamanho.

    Preserva offsets/posições no markdown para que a janela de extração
    continue alinhada, mas garante que as URLs de imagens não sejam capturadas
    por ``_RE_HTTP_NU`` ou ``_RE_MD_LINK``.
    """
    def _sub(m: "re.Match[str]") -> str:
        return " " * (m.end() - m.start())
    return _RE_MD_IMAGE.sub(_sub, md)


def _iter_urls_no_markdown(md: str) -> list[tuple[str, int, str]]:
    """Devolve (url, posição, título_link) para URLs únicas que parecem anúncios.

    Ordenadas por posição no documento; deduplicadas. URLs dentro de
    referências de imagem (``![...](...)``) são mascaradas antes do scan.
    """
    md_sem_img = _mascarar_imagens(md)
    vistos: set[str] = set()
    out: list[tuple[str, int, str]] = []

    def _add(url: str, pos: int, titulo: str) -> None:
        u = _normalizar_url(url)
        if not u or u in vistos:
            return
        if not url_eh_anuncio_aproveitavel(u):
            return
        vistos.add(u)
        out.append((u, pos, (titulo or "").strip()))

    for m in _RE_MD_LINK.finditer(md_sem_img):
        _add(m.group(2), m.start(), m.group(1) or "")
    for m in _RE_ANGLE_LINK.finditer(md_sem_img):
        _add(m.group(1), m.start(), "")
    for m in _RE_HTTP_NU.finditer(md_sem_img):
        _add(m.group(0), m.start(), "")

    out.sort(key=lambda x: x[1])
    return out


# -----------------------------------------------------------------------------
# API pública: extrair_cards
# -----------------------------------------------------------------------------

def extrair_cards(markdown: str) -> list[CardExtraido]:
    """Extrai todos os cards (URL + R$ + m²) de uma página de markdown.

    NÃO devolve cidade, UF nem bairro definitivos — apenas o que se pode inferir
    do **próprio markdown do anúncio**. O pipeline é responsável por validar a
    cidade real via geocode (:func:`comparaveis.validacao_cidade.validar_municipio_card`)
    antes de qualquer persistência.

    Args:
        markdown: texto bruto do Firecrawl scrape.

    Returns:
        Lista (eventualmente vazia) de :class:`CardExtraido`. Sem duplicatas
        por ``url_anuncio``.
    """
    md = markdown or ""
    if not md.strip():
        return []

    candidatos = _iter_urls_no_markdown(md)
    cards: list[CardExtraido] = []
    for url, pos, titulo_link in candidatos:
        i0 = max(0, pos - 1200)
        i1 = min(len(md), pos + 700)
        janela = md[i0:i1]
        ancora = pos - i0

        preco, area = _melhor_par_preco_area(janela, ancora)
        if preco is None or area is None:
            continue

        titulo = titulo_link or _titulo_proximo(md, pos)
        if titulo and _RE_TITULO_GENERICO.match(titulo):
            titulo = _titulo_proximo(md, pos) or titulo

        cards.append(
            CardExtraido(
                url_anuncio=url,
                portal=_portal_de_url(url),
                valor_venda=preco,
                area_m2=area,
                titulo=titulo[:300],
                logradouro_inferido=_inferir_logradouro_do_titulo(titulo),
                bairro_inferido=_inferir_bairro_do_titulo(titulo),
            )
        )

    logger.info("Extrator: %s cards extraídos (%s URLs candidatas)", len(cards), len(candidatos))
    return cards
