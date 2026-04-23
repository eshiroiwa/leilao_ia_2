"""
Remove do markdown do edital trechos típicos de cross-sell, newsletter e rodapé
antes de persistir em `edital_markdown` (opção 2 — pós-processamento heurístico).

O texto completo continua disponível na ingestão para a extração LLM; a limpeza
é aplicada só na gravação no banco (menos ruído para o próximo agente).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# Início do trecho a descartar até o fim do arquivo (primeiro match vence).
_ANCORAS_CORTE_ATE_FIM: list[tuple[str, re.Pattern[str]]] = [
    (
        "veja_tambem",
        re.compile(r"\n#{1,4}\s*Veja também\b", re.IGNORECASE | re.MULTILINE),
    ),
    (
        "voce_tambem_pode",
        re.compile(
            r"\n#{1,4}\s*Você também pode\b|\n#{1,4}\s*Voce tambem pode\b",
            re.IGNORECASE | re.MULTILINE,
        ),
    ),
    (
        "newsletter_cadastro",
        re.compile(
            r"\n#{1,4}\s*(?:Cadastre-se|Inscreva-se)(?:\s+na)?(?:\s+nossa)?\s+Newsletter\b",
            re.IGNORECASE | re.MULTILINE,
        ),
    ),
    (
        "newsletter_heading",
        re.compile(r"\n#{1,4}\s*Newsletter\b(?!\])", re.IGNORECASE | re.MULTILINE),
    ),
    (
        "newsletter_cta_zuk",
        re.compile(
            r"\n#{1,4}\s*Vamos encontrar o imóvel ideal\b",
            re.IGNORECASE | re.MULTILINE,
        ),
    ),
    (
        "google_forms_newsletter",
        re.compile(r"\n\[Newsletter\]\(https://docs\.google\.com/forms/", re.IGNORECASE),
    ),
    (
        "login_modal",
        re.compile(r"\n#{1,4}\s*Log\s+in\b", re.IGNORECASE | re.MULTILINE),
    ),
    (
        "complete_cadastro",
        re.compile(r"\n#{1,4}\s*Complete seu cadastro\b", re.IGNORECASE | re.MULTILINE),
    ),
    (
        "proximidades_blog",
        re.compile(r"\n#{2,4}\s*Proximidades\b", re.IGNORECASE | re.MULTILINE),
    ),
    (
        "conheca_melhores_cidades",
        re.compile(r"\n#{2,4}\s*Conheça as \d+ melhores cidades\b", re.IGNORECASE | re.MULTILINE),
    ),
    (
        "whatsapp_flutuante",
        re.compile(r"\n###\s*Whatsapp\s*$", re.IGNORECASE | re.MULTILINE),
    ),
]


@dataclass
class ResultadoLimpezaMarkdown:
    texto: str
    cortes_aplicados: list[str] = field(default_factory=list)
    caracteres_antes: int = 0
    caracteres_depois: int = 0

    @property
    def removidos_caracteres(self) -> int:
        return max(0, self.caracteres_antes - self.caracteres_depois)


def limpar_edital_markdown_ruido(texto: str) -> ResultadoLimpezaMarkdown:
    """
    Corta do primeiro ancoramento reconhecido até o fim do texto.
    Aplica trim e colapsa quebras de linha excessivas no resultado.
    """
    if not texto or not texto.strip():
        return ResultadoLimpezaMarkdown(texto=texto or "", caracteres_antes=len(texto or ""))

    original = texto
    cortes: list[str] = []
    cortes_pos: list[tuple[int, str]] = []

    for nome, rx in _ANCORAS_CORTE_ATE_FIM:
        m = rx.search(texto)
        if m:
            cortes_pos.append((m.start(), nome))

    if cortes_pos:
        idx, nome = min(cortes_pos, key=lambda x: x[0])
        texto = texto[:idx].rstrip()
        cortes.append(nome)
        logger.info(
            "edital_markdown_limpeza: cortado '%s' a partir do índice %s (%s chars removidos)",
            nome,
            idx,
            len(original) - len(texto),
        )

    # Colapsa 4+ quebras consecutivas em no máximo 2
    texto = re.sub(r"\n{4,}", "\n\n\n", texto)
    texto = texto.strip()

    return ResultadoLimpezaMarkdown(
        texto=texto,
        cortes_aplicados=cortes,
        caracteres_antes=len(original),
        caracteres_depois=len(texto),
    )
