"""
Redação mínima de PII em textos persistidos (CPF, e-mail, telefone BR comuns).
Não remove números de processo judicial (padrões típicos são distintos).
"""

from __future__ import annotations

import re


_RE_CPF = re.compile(
    r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b|\b\d{11}\b",
    re.IGNORECASE,
)
_RE_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_RE_TEL = re.compile(
    r"\(?\b0?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}\b|\b\+?55\s*\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}\b"
)


def redigir_pii_texto(texto: str) -> str:
    if not texto:
        return texto
    s = _RE_CPF.sub("[CPF REMOVIDO]", texto)
    s = _RE_EMAIL.sub("[E-MAIL REMOVIDO]", s)
    s = _RE_TEL.sub("[TELEFONE REMOVIDO]", s)
    return s


def redigir_pii_extracao_extra(obs_md: str | None, regras_md: str | None) -> tuple[str | None, str | None]:
    o = redigir_pii_texto(obs_md) if obs_md else None
    r = redigir_pii_texto(regras_md) if regras_md else None
    return o, r
