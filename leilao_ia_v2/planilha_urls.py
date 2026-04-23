"""Leitura de planilhas contendo apenas URLs de leilão (.xlsx / .csv)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_COLUNAS_URL = (
    "url_leilao",
    "url",
    "link",
    "link_leilao",
    "href",
)


def ler_urls_de_planilha(caminho: str | Path) -> list[str]:
    path = Path(caminho).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    suf = path.suffix.lower()
    if suf in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    elif suf == ".csv":
        df = pd.read_csv(path, encoding="utf-8-sig")
    else:
        raise ValueError("Use .csv, .xlsx ou .xls")

    if df.empty:
        logger.warning("Planilha vazia: %s", path)
        return []

    col_map = {str(c).strip().lower(): c for c in df.columns}
    url_col = None
    for cand in _COLUNAS_URL:
        if cand in col_map:
            url_col = col_map[cand]
            break
    if url_col is None:
        url_col = df.columns[0]
        logger.info("Usando primeira coluna como URL: %s", url_col)

    out: list[str] = []
    for raw in df[url_col].dropna().astype(str):
        u = raw.strip()
        if not u or u.lower() in ("nan", "none"):
            continue
        if not u.lower().startswith(("http://", "https://")):
            u = "https://" + u
        out.append(u)
    logger.info("Planilha %s: %s URLs lidas", path.name, len(out))
    return out
