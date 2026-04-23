"""
Heurísticas para decidir se o markdown da página é material de leilão de imóvel.

URLs como sites de hotel (ex.: Pestana) podem retornar HTML/marketing sem edital:
evitamos gastar LLM e não gravamos nada no banco.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from leilao_ia_v2.exceptions import IngestaoSemConteudoEditalError

# Texto muito curto raramente contém edital completo.
_MIN_CARACTERES = int(os.getenv("LEILAO_IA_V2_MIN_MARKDOWN_CHARS", "450"))

# Pelo menos N “indícios” distintos (regex) devem aparecer.
_MIN_INDICIOS = int(os.getenv("LEILAO_IA_V2_MIN_MARKDOWN_INDICIOS", "2"))

_PADROES_INDICIO: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"leil[aã]o", re.IGNORECASE), "leilão"),
    (re.compile(r"\bedital\b", re.IGNORECASE), "edital"),
    (re.compile(r"arremat", re.IGNORECASE), "arrematação"),
    (re.compile(r"\blance\b", re.IGNORECASE), "lance"),
    (re.compile(r"pra[çc]a", re.IGNORECASE), "praça"),
    (re.compile(r"matr[íi]cula", re.IGNORECASE), "matrícula"),
    (re.compile(r"imóvel|imovel", re.IGNORECASE), "imóvel"),
    (re.compile(r"judicial|extrajudicial|fiduciante|comitente", re.IGNORECASE), "judicial/fiduciário"),
    (re.compile(r"valor\s*m[íi]nimo|lance\s*m[íi]nimo", re.IGNORECASE), "valores de lance"),
]


@dataclass(frozen=True)
class DiagnosticoMarkdownEdital:
    caracteres: int
    indicios_encontrados: frozenset[str]


def diagnosticar_markdown_edital(markdown: str) -> DiagnosticoMarkdownEdital:
    s = (markdown or "").strip()
    tags: set[str] = set()
    for rx, tag in _PADROES_INDICIO:
        if rx.search(s):
            tags.add(tag)
    return DiagnosticoMarkdownEdital(caracteres=len(s), indicios_encontrados=frozenset(tags))


def validar_markdown_antes_da_extracao(markdown: str) -> DiagnosticoMarkdownEdital:
    """
    Levanta `IngestaoSemConteudoEditalError` se o conteúdo não for suficiente para seguir o pipeline.
    Caso contrário retorna o diagnóstico (útil para logs/UI).
    """
    diag = diagnosticar_markdown_edital(markdown)
    if diag.caracteres < _MIN_CARACTERES:
        raise IngestaoSemConteudoEditalError(
            motivo=(
                f"O texto obtido da URL é muito curto ({diag.caracteres} caracteres; "
                f"mínimo {_MIN_CARACTERES}). A página pode estar vazia, bloqueada ou não é a do leilão."
            ),
            diagnostico=diag,
        )
    if len(diag.indicios_encontrados) < _MIN_INDICIOS:
        raise IngestaoSemConteudoEditalError(
            motivo=(
                "A página não parece conter um edital de leilão de imóvel "
                f"(poucos indícios de leilão/edital/lance; encontrados: {sorted(diag.indicios_encontrados) or 'nenhum'}). "
                "Ex.: páginas só de marketing ou de outro serviço."
            ),
            diagnostico=diag,
        )
    return diag


MENSAGEM_ACOES_USUARIO = (
    "Nada foi gravado no banco de dados. Você pode **abortar** este fluxo e tentar outra URL, "
    "ou **cadastrar manualmente** os dados do imóvel (use a ingestão clássica por planilha ou o painel do sistema, "
    "conforme estiver disponível)."
)
