from __future__ import annotations

import os
import time

from leilao_ia_v2.services import disk_cache


def test_caminho_cache_usa_env(tmp_path, monkeypatch):
    monkeypatch.setenv("LEILAO_IA_V2_CACHE_DIR", str(tmp_path))
    p = disk_cache.caminho_cache_markdown("https://exemplo.com/lote/1")
    assert p.parent == tmp_path
    assert p.suffix == ".md"


def test_gravar_ler_cache_markdown(tmp_path, monkeypatch):
    monkeypatch.setenv("LEILAO_IA_V2_CACHE_DIR", str(tmp_path))
    url = "https://exemplo.com/a"
    disk_cache.gravar_markdown_cache(url, "# Olá")
    assert disk_cache.ler_markdown_cache(url) == "# Olá"


def test_cache_expira(monkeypatch, tmp_path):
    monkeypatch.setenv("LEILAO_IA_V2_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("LEILAO_IA_V2_FIRECRAWL_CACHE_MAX_AGE_DAYS", "0.00001")
    url = "https://exemplo.com/b"
    p = disk_cache.caminho_cache_markdown(url)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("x", encoding="utf-8")
    velho = time.time() - 120.0
    os.utime(p, (velho, velho))
    assert disk_cache.ler_markdown_cache(url) is None
