"""
Constantes e helper para inputs compactos (larguras em px) em toda a app Streamlit.

Streamlit ≥1.33 aceita ``width`` em píxeis em ``st.number_input`` / ``st.selectbox``,
evitando que campos curtos ocupem toda a coluna.
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
W_LIM = 100  # máx. linhas, contagens pequenas
W_KM = 96
W_LATLON = 120
W_INT = 80  # inteiros (quartos, passos)


def number_compact(
    label: str,
    **kwargs: Any,
) -> Any:
    """Largura padrão reduzida; passe ``w=`` para ajustar (veja constantes W_*)."""
    w: int = int(kwargs.pop("w", W_NUM))
    return st.number_input(label, width=w, **kwargs)
