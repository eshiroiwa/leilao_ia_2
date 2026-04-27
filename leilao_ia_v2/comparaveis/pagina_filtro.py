"""
Pré-filtro textual de páginas: a página menciona a cidade-alvo?

Antes de gastar 1 crédito de scrape em cada URL trazida pelo Firecrawl Search,
e mais ainda antes de geocodificar cada card extraído, esta heurística
puramente textual descarta páginas obviamente irrelevantes.

A análise é feita em **duas camadas**:

1. **Posições privilegiadas**: H1/H2 de markdown, primeira linha não vazia
   (proxy para `<title>`), breadcrumbs (linhas com ` > ` / ` › ` / `/`) e
   meta tags simples (`canonical:` ou `og:url`). Se a cidade-alvo aparece em
   pelo menos uma destas posições → ``CONFIRMADA``.
2. **Corpo do texto**: se a cidade aparece só em parágrafos comuns →
   ``MENCIONADA`` (sinal mais fraco; o caller decide se vale extrair).
3. **Ausente**: a cidade não aparece em lugar nenhum → ``REJEITADA`` e a
   página é descartada antes de qualquer scrape/extracção.

Adicionalmente, identificamos a presença de **outras cidades concorrentes**
no H1/título — se uma cidade diferente domina a posição privilegiada e a
cidade-alvo nem aparece lá, o sinal é ainda mais forte para rejeitar.

Toda a comparação é feita por slug normalizado (lowercase + sem acentos +
alfanumérico), reusando :func:`leilao_ia_v2.comparaveis.validacao_cidade._slug`.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


def _slug(s: str) -> str:
    """Slug consistente com `validacao_cidade._slug` (mantido aqui para evitar
    dependência circular)."""
    if not s:
        return ""
    base = "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", "", base)


# Limites de tamanho para procurar a cidade — evita varrer documentos enormes.
_MAX_LEN_PRIVILEGIADO = 4000
_MAX_LEN_CORPO = 60000


# Linhas tratadas como "posição privilegiada" do markdown.
_RE_HEADING = re.compile(r"^\s{0,3}#{1,3}\s+(.+?)\s*$", re.MULTILINE)
_RE_BREADCRUMB = re.compile(
    r"^[^\n]*\s(?:>|›|/|»)\s[^\n]*$", re.MULTILINE
)
_RE_META_CANONICAL = re.compile(
    r"(?:canonical|og:url|og:title|og:locality)\s*[:=]\s*([^\n]{2,400})",
    re.IGNORECASE,
)


class StatusPagina(str, Enum):
    """Veredito da heurística sobre uma página."""

    CONFIRMADA = "confirmada"  # cidade alvo em H1/título/breadcrumb
    MENCIONADA = "mencionada"  # cidade alvo só no corpo
    REJEITADA = "rejeitada"    # cidade alvo ausente


@dataclass(frozen=True)
class ResultadoFiltroPagina:
    """Resultado imutável da análise de uma página."""

    status: StatusPagina
    cidade_alvo_slug: str
    posicoes_privilegiadas: tuple[str, ...] = field(default_factory=tuple)
    cidades_concorrentes: tuple[str, ...] = field(default_factory=tuple)
    motivo: str = ""

    @property
    def deve_extrair(self) -> bool:
        """Vale a pena extrair cards (status ≠ REJEITADA)."""
        return self.status != StatusPagina.REJEITADA

    @property
    def confianca_alta(self) -> bool:
        """Cidade-alvo aparece em posição privilegiada."""
        return self.status == StatusPagina.CONFIRMADA


# -----------------------------------------------------------------------------
# Extração de "posições privilegiadas" do markdown
# -----------------------------------------------------------------------------

def _coletar_titulos(md: str) -> list[str]:
    """Devolve textos de H1/H2/H3, na ordem em que aparecem no markdown."""
    return [m.group(1).strip() for m in _RE_HEADING.finditer(md)]


def _coletar_breadcrumbs(md: str) -> list[str]:
    """Linhas que parecem trilhas de navegação ('Home > SP > Pindamonhangaba')."""
    out: list[str] = []
    for m in _RE_BREADCRUMB.finditer(md):
        linha = m.group(0).strip()
        if 6 <= len(linha) <= 400:
            out.append(linha)
    return out


def _coletar_meta(md: str) -> list[str]:
    return [m.group(1).strip() for m in _RE_META_CANONICAL.finditer(md)]


def _primeira_linha_significativa(md: str) -> str:
    """Heurística simples para 'title' implícito: 1ª linha não-vazia/não-link."""
    for raw in (md or "").splitlines():
        s = raw.strip()
        if not s:
            continue
        # ignora linhas só de imagem ou link puro
        if re.fullmatch(r"!?\[[^\]]*\]\([^)]+\)", s):
            continue
        if s.startswith("http://") or s.startswith("https://"):
            continue
        return s[:300]
    return ""


# -----------------------------------------------------------------------------
# Detecção de "outras cidades" — heurística por slugs conhecidos no texto
# -----------------------------------------------------------------------------

def _slugs_de_texto(texto: str) -> list[str]:
    """Quebra um texto em palavras e devolve sequências de 1..3 palavras
    como slugs. Permite detectar 'São Bernardo do Campo' como slug composto."""
    if not texto:
        return []
    palavras = re.findall(r"[A-Za-zÀ-ÿ0-9]+", texto)
    out: list[str] = []
    for n in (3, 2, 1):
        for i in range(len(palavras) - n + 1):
            out.append(_slug(" ".join(palavras[i : i + n])))
    return [s for s in out if len(s) >= 4]


def _detectar_cidades_concorrentes(
    privilegiados: list[str],
    cidades_conhecidas_slug: set[str],
    cidade_alvo_slug: str,
) -> list[str]:
    """Procura, no texto privilegiado, slugs que pertencem ao conjunto
    `cidades_conhecidas_slug` E são diferentes da cidade-alvo.

    Útil para o caller marcar "página é claramente sobre outra cidade".
    """
    encontrados: list[str] = []
    for texto in privilegiados:
        for slug in _slugs_de_texto(texto):
            if slug == cidade_alvo_slug:
                continue
            if slug in cidades_conhecidas_slug:
                encontrados.append(slug)
    # ordem estável de aparecimento
    seen: set[str] = set()
    out: list[str] = []
    for s in encontrados:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


# -----------------------------------------------------------------------------
# API pública: avaliar_pagina
# -----------------------------------------------------------------------------

def avaliar_pagina(
    markdown: str,
    *,
    cidade_alvo: str,
    cidades_conhecidas: Optional[list[str]] = None,
) -> ResultadoFiltroPagina:
    """Avalia se vale a pena extrair cards desta página.

    Args:
        markdown: texto bruto retornado pelo Firecrawl scrape.
        cidade_alvo: cidade do leilão (será comparada por slug).
        cidades_conhecidas: lista opcional de cidades vizinhas ou da mesma UF
            (ex.: `["Taubaté", "São Paulo", "Caçapava"]`); usada para detectar
            quando a página é manifestamente sobre **outra** cidade.

    Returns:
        :class:`ResultadoFiltroPagina` com o veredito.
    """
    md = (markdown or "")[:_MAX_LEN_CORPO]
    cidade_slug = _slug(cidade_alvo)

    if not cidade_slug:
        return ResultadoFiltroPagina(
            status=StatusPagina.REJEITADA,
            cidade_alvo_slug="",
            motivo="cidade_alvo_vazia",
        )
    if not md.strip():
        return ResultadoFiltroPagina(
            status=StatusPagina.REJEITADA,
            cidade_alvo_slug=cidade_slug,
            motivo="markdown_vazio",
        )

    privilegiados: list[str] = []
    privilegiados.extend(_coletar_titulos(md[:_MAX_LEN_PRIVILEGIADO]))
    privilegiados.extend(_coletar_breadcrumbs(md[:_MAX_LEN_PRIVILEGIADO]))
    privilegiados.extend(_coletar_meta(md[:_MAX_LEN_PRIVILEGIADO]))
    titulo = _primeira_linha_significativa(md[:_MAX_LEN_PRIVILEGIADO])
    if titulo:
        privilegiados.append(titulo)

    posicoes_match: list[str] = []
    for texto in privilegiados:
        if cidade_slug in _slug(texto):
            posicoes_match.append(texto[:200])

    conhecidas_slug: set[str] = set()
    if cidades_conhecidas:
        conhecidas_slug = {
            _slug(c) for c in cidades_conhecidas if _slug(c) and _slug(c) != cidade_slug
        }
    concorrentes = (
        _detectar_cidades_concorrentes(privilegiados, conhecidas_slug, cidade_slug)
        if conhecidas_slug
        else []
    )

    if posicoes_match:
        return ResultadoFiltroPagina(
            status=StatusPagina.CONFIRMADA,
            cidade_alvo_slug=cidade_slug,
            posicoes_privilegiadas=tuple(posicoes_match),
            cidades_concorrentes=tuple(concorrentes),
            motivo="cidade_alvo_em_posicao_privilegiada",
        )

    if cidade_slug in _slug(md):
        return ResultadoFiltroPagina(
            status=StatusPagina.MENCIONADA,
            cidade_alvo_slug=cidade_slug,
            cidades_concorrentes=tuple(concorrentes),
            motivo="cidade_alvo_so_no_corpo",
        )

    return ResultadoFiltroPagina(
        status=StatusPagina.REJEITADA,
        cidade_alvo_slug=cidade_slug,
        cidades_concorrentes=tuple(concorrentes),
        motivo="cidade_alvo_ausente",
    )
