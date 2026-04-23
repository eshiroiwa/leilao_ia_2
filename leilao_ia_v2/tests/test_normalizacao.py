from __future__ import annotations

from leilao_ia_v2 import normalizacao


def test_normalizar_tipo_apartamento():
    assert normalizacao.normalizar_tipo_imovel("Apto 3 dorms") == "apartamento"


def test_normalizar_tipo_galpao_e_armazem():
    assert normalizacao.normalizar_tipo_imovel("Galpão Logístico") == "galpao"
    assert normalizacao.normalizar_tipo_imovel("armazém industrial") == "armazem"


def test_normalizar_tipo_varios():
    assert normalizacao.normalizar_tipo_imovel("Kitnet") == "kitnet"
    assert normalizacao.normalizar_tipo_imovel("Ponto comercial") == "ponto_comercial"
    assert normalizacao.normalizar_tipo_imovel("casamento civil") == "desconhecido"
    assert normalizacao.normalizar_tipo_imovel("Casa térrea") == "casa"


def test_normalizar_data():
    assert normalizacao.normalizar_data_para_iso("15/03/2025") == "2025-03-15"


def test_normalizar_url():
    assert normalizacao.normalizar_url_leilao("exemplo.com/x").startswith("https://")
