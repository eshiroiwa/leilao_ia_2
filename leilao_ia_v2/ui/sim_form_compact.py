"""
Constantes e helper para o formulário de simulação compacto (larguras em px).

Streamlit ≥1.33 aceita ``width`` em píxeis em ``st.number_input`` / ``st.selectbox``,
evitando que campos de 2 dígitos ocupem toda a coluna.
"""
from __future__ import annotations

from typing import Any

import streamlit as st

# Larguras padrão (px)
W_NUM = 90
W_PCT = 94
W_MESES = 88
W_BRL = 132
W_BRL_MED = 152
W_SELECT = 300


def number_compact(
    label: str,
    **kwargs: Any,
) -> Any:
    """Largura padrão reduzida; passe ``w=`` para ajustar (veja constantes W_*)."""
    w: int = int(kwargs.pop("w", W_NUM))
    return st.number_input(label, width=w, **kwargs)
