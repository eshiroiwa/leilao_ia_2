"""
Extrator de cards de anúncios a partir de markdown — **sem inventar cidade**.

Princípio crítico: o extrator devolve apenas o que está **provado** no markdown:
``url, portal, valor_venda, area_m2, titulo, logradouro_inferido,
bairro_inferido``. **Cidade, UF e bairro definitivos são preenchidos pela
validação por geocode** (módulo :mod:`comparaveis.validacao_cidade`).

Esta separação é a defesa-em-profundidade contra o bug histórico em que cards
recebiam a cidade do leilão por *fallback* (Pindamonhangaba → São Bernardo).

A heurística "preço e área mais próximos da URL na janela" é conservadora e
funciona bem para os portais cobertos (Zap, Quinto Andar, OLX, Loft, Imovelweb,
Chaves na Mão).
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
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

    ``cidade_no_markdown`` é uma evidência **textual local**: contém o nome da
    cidade-alvo se ele apareceu na janela do card no markdown bruto. Serve como
    sinal forte para a hierarquia de validação (validacao_cidade), evitando o
    descarte de cards quando a página foi confirmada como sendo da cidade-alvo
    mas a rua/bairro genérico geocodifica para outra cidade.
    """

    url_anuncio: str
    portal: str
    valor_venda: float
    area_m2: float
    titulo: str = ""
    logradouro_inferido: str = ""
    bairro_inferido: str = ""
    cidade_no_markdown: str = ""
    # Auditoria do refino top-N (preenchido por refino_individual quando aplicável):
    # - ``refinado_top_n`` é True quando o card foi alvo de scrape individual,
    #   independentemente de o resultado ter sido aproveitado.
    # - ``refino_status`` indica o desfecho: ``""`` (não refinado), ``"ok"``,
    #   ``"revertido"``, ``"scrape_falhou"``, ``"extracao_vazia"`` ou
    #   ``"geocode_falhou"``. Cards descartados por cidade diferente não chegam
    #   à persistência, logo não precisam de marcador.
    refinado_top_n: bool = False
    refino_status: str = ""

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
    """Heurística informativa: a URL aparenta ser uma página de **listagem**?

    .. note::

       Esta função **não** é usada para descartar URLs em
       :func:`url_eh_anuncio_aproveitavel`. Ela existe apenas como sinal para
       métricas/logs e para o pipeline ajustar heurísticas quando necessário.

    Por que aceitamos listagens? Em cidades menores (Pindamonhangaba, Cruzeiro,
    Atibaia, …) os portais raramente indexam anúncios individuais; o que aparece
    nos motores de busca são páginas-hub do tipo *"Apartamentos à venda em
    Santana, Pindamonhangaba — SP"*. O extrator é capaz de iterar **vários**
    cards por página e a validação por geocode garante que cards de outras
    cidades sejam descartados depois.
    """
    u = (url or "").lower()
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
    """API pública para validar URLs antes de gastar scrape.

    Aceita tanto **anúncios individuais** quanto **páginas de listagem** dos
    portais reconhecidos. Listagens são especialmente importantes em cidades
    menores (onde portais não indexam anúncios isolados); o extrator extrai
    múltiplos cards por página e a validação por geocode descarta cards que
    não são da cidade-alvo.

    Rejeita: domínios fora da lista, recursos estáticos (jpg, png, …) e URLs
    de **aluguel**.
    """
    if not url:
        return False
    if _url_eh_recurso_estatico(url):
        return False
    if not _portal_aceito(url):
        return False
    if _url_eh_aluguel(url):
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


# -----------------------------------------------------------------------------
# Extracção de endereço completo a partir de página de anúncio individual
# -----------------------------------------------------------------------------

# Tipos de logradouro reconhecidos. Inclui formas abreviadas e plurais
# para apanhar variações dos portais.
_RE_LOGRADOURO_TIPOS = (
    r"Rua|R\.|Avenida|Av\.|Alameda|Al\.|Travessa|Tv\.|Estrada|"
    r"Rodovia|Rod\.|Praça|Pç\.|Pca\.|Largo|Beco|Servidão|Via"
)

# Padrão para "Rua X, 123" ou "Av. Y, 45 — Apto 67". Captura logradouro
# com tipo + nome próprio (até quebra de linha ou pontuação) opcionalmente
# seguido de número. O lookahead final exige separador "forte" (vírgula,
# en-dash, em-dash, ponto, parêntese, fim de linha) — *não* aceita simples
# espaço, para não cortar "Rua das Flores" em "Rua das".
_RE_RUA_COM_NUMERO = re.compile(
    rf"\b({_RE_LOGRADOURO_TIPOS})\s+"
    r"([A-Za-zÀ-ÿ][\wÀ-ÿ\s\-'.]{2,100}?)"
    r"(?:\s*[,–—\-]\s*(?:n[º°ºo]?\s*)?(\d{1,5}[A-Za-z]?))?"
    r"(?=\s*(?:[,–—\-\.\(\)]|\n|$))",
    re.IGNORECASE,
)

# Labels comuns em páginas de anúncio individual: "Endereço:", "Localização:",
# "Bairro:" — capturam o que vem na MESMA linha (até quebra ou outro label).
_RE_LABEL_ENDERECO = re.compile(
    r"(?:^|\n)\s*(?:\*\*)?\s*(?:Endere[çc]o|Localiza[çc][ãa]o|Address)\s*(?:\*\*)?\s*"
    r"[:：]\s*(?:\*\*)?\s*([^\n*]{8,300})",
    re.IGNORECASE,
)
_RE_LABEL_BAIRRO = re.compile(
    r"(?:^|\n)\s*(?:\*\*)?\s*Bairro\s*(?:\*\*)?\s*[:：]\s*(?:\*\*)?\s*"
    r"([A-Za-zÀ-ÿ][\wÀ-ÿ\s\-'.]{1,80})",
    re.IGNORECASE,
)

# Padrão "no bairro Foo" / "do bairro Foo" / "Bairro Foo" no corpo do texto.
_RE_BAIRRO_INLINE = re.compile(
    r"\b(?:no|do|da|de|em|bairro)\s+(?:bairro\s+)?"
    r"([A-Z][A-Za-zÀ-ÿ][\wÀ-ÿ\s\-']{2,60})",
    re.UNICODE,
)


def extrair_endereco_anuncio_individual(markdown: str) -> tuple[str, str]:
    """Extrai (logradouro, bairro) de uma página de anúncio individual.

    Aplica heurísticas mais agressivas que :func:`_inferir_logradouro_do_titulo`
    porque uma página de anúncio individual costuma ter:

    - Labels explícitos: ``Endereço: Rua X, 123 - Bairro``.
    - Padrão "Rua/Av Foo, 123" no corpo (mesmo sem label).
    - Bairro mencionado em "no bairro Foo" ou label próprio.

    Devolve strings vazias quando não encontra. O caller (refino) pode
    decidir manter as coords antigas se o resultado for vazio.

    Args:
        markdown: texto bruto do scrape do anúncio individual.

    Returns:
        Tupla ``(logradouro, bairro)`` — ambas strings podem estar vazias.
        ``logradouro`` inclui tipo + nome (e número quando disponível),
        truncado a 200 chars. ``bairro`` é truncado a 80 chars.
    """
    md = markdown or ""
    if not md.strip():
        return ("", "")

    logradouro = ""
    bairro = ""

    m_end = _RE_LABEL_ENDERECO.search(md)
    if m_end:
        endereco_linha = m_end.group(1).strip()
        m_rua = _RE_RUA_COM_NUMERO.search(endereco_linha)
        if m_rua:
            logradouro = _formatar_logradouro(m_rua)
        elif endereco_linha and len(endereco_linha) >= 5:
            logradouro = endereco_linha[:200]

    if not logradouro:
        for m in _RE_RUA_COM_NUMERO.finditer(md):
            logradouro = _formatar_logradouro(m)
            if m.group(3):
                break

    m_bai_label = _RE_LABEL_BAIRRO.search(md)
    if m_bai_label:
        bairro = m_bai_label.group(1).strip()[:80]
    else:
        m_bai_inline = _RE_BAIRRO_INLINE.search(md)
        if m_bai_inline:
            cand = m_bai_inline.group(1).strip()
            if not _RE_TITULO_GENERICO.match(cand) and len(cand) >= 3:
                bairro = cand[:80]

    return (logradouro.strip()[:200], bairro.strip()[:80])


def _formatar_logradouro(m: "re.Match[str]") -> str:
    """Junta tipo + nome (+ número, se houver) num formato canónico."""
    tipo = (m.group(1) or "").strip()
    nome = (m.group(2) or "").strip().rstrip(",.- ")
    numero = (m.group(3) or "").strip() if m.lastindex and m.lastindex >= 3 else ""
    base = f"{tipo} {nome}".strip()
    if numero:
        base = f"{base}, {numero}"
    return base[:200]


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
# Detecção textual de cidade-alvo na janela do card
# -----------------------------------------------------------------------------

def _normalizar_para_match(s: str) -> str:
    """Lowercase, strip de acentos, colapsa não-alfanuméricos em um único espaço.

    >>> _normalizar_para_match("Pindamonhangaba/SP — Centro")
    'pindamonhangaba sp centro'
    """
    base = unicodedata.normalize("NFD", (s or "").lower())
    base = "".join(c for c in base if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", base).strip()


def _detectar_cidade_no_texto(texto: str, cidade_alvo: str) -> bool:
    """Verdadeiro se o nome da cidade-alvo aparece no texto (com boundaries).

    Usa normalização (sem acentos, lowercase, pontuação → espaço) e busca por
    sequência exata, evitando falsos positivos do tipo *"saopaulo"* matchando
    *"saopaulonorte"*. Rejeita cidades com nome muito curto (<4 chars) para
    evitar matches espúrios.
    """
    if not cidade_alvo or not texto:
        return False
    alvo = _normalizar_para_match(cidade_alvo)
    if len(alvo) < 4:
        return False
    txt = _normalizar_para_match(texto)
    return f" {alvo} " in f" {txt} "


# -----------------------------------------------------------------------------
# API pública: extrair_cards
# -----------------------------------------------------------------------------

def extrair_cards(
    markdown: str,
    *,
    cidade_alvo: str = "",
) -> list[CardExtraido]:
    """Extrai todos os cards (URL + R$ + m²) de uma página de markdown.

    NÃO devolve cidade, UF nem bairro definitivos — apenas o que se pode inferir
    do **próprio markdown do anúncio**. O pipeline é responsável por validar a
    cidade real via geocode (:func:`comparaveis.validacao_cidade.validar_municipio_card`)
    antes de qualquer persistência.

    Args:
        markdown: texto bruto do Firecrawl scrape.
        cidade_alvo: nome da cidade do leilão. Se passado, cada card terá
            ``cidade_no_markdown`` preenchido com este valor sempre que o nome
            aparecer na janela do card (sinal forte para a validação posterior).

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

        cidade_local = (
            cidade_alvo
            if cidade_alvo and _detectar_cidade_no_texto(janela, cidade_alvo)
            else ""
        )

        cards.append(
            CardExtraido(
                url_anuncio=url,
                portal=_portal_de_url(url),
                valor_venda=preco,
                area_m2=area,
                titulo=titulo[:300],
                logradouro_inferido=_inferir_logradouro_do_titulo(titulo),
                bairro_inferido=_inferir_bairro_do_titulo(titulo),
                cidade_no_markdown=cidade_local,
            )
        )

    logger.info(
        "Extrator: %s cards extraídos (%s URLs candidatas, cidade_alvo=%r)",
        len(cards),
        len(candidatos),
        cidade_alvo,
    )
    return cards
