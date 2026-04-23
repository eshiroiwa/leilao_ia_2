"""Regras de slug do Viva Real: sem acentos, minúsculas, hífens no lugar de espaços."""

from __future__ import annotations

import re
import unicodedata


def slug_vivareal(texto: str) -> str:
    """
    Converte texto livre (cidade, bairro, etc.) para segmento de path estilo Viva Real.
    """
    s = str(texto or "").strip().lower()
    if not s:
        return ""
    s = "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )
    s = re.sub(r"[^a-z0-9]+", "-", s, flags=re.IGNORECASE)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s
