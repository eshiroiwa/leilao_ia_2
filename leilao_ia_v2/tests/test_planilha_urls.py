from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from leilao_ia_v2.planilha_urls import ler_urls_de_planilha


def test_ler_csv(tmp_path: Path):
    p = tmp_path / "u.csv"
    df = pd.DataFrame({"url": [" https://a.com ", "", "https://b.com"]})
    df.to_csv(p, index=False, encoding="utf-8-sig")
    urls = ler_urls_de_planilha(p)
    assert urls == ["https://a.com", "https://b.com"]


def test_ler_xlsx(tmp_path: Path):
    pytest.importorskip("openpyxl")
    p = tmp_path / "u.xlsx"
    df = pd.DataFrame({"link": ["a.com"]})
    df.to_excel(p, index=False)
    urls = ler_urls_de_planilha(p)
    assert urls == ["https://a.com"]
