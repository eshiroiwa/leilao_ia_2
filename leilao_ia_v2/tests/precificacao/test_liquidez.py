"""
Testes do alerta de liquidez por desvio de área.
"""

from __future__ import annotations

import pytest

from leilao_ia_v2.precificacao.liquidez import (
    SEVERIDADE_ALTA,
    SEVERIDADE_MEDIA,
    SEVERIDADE_OK,
    avaliar_liquidez,
)


class TestAvaliarLiquidez:
    def test_dentro_do_padrao_devolve_ok(self):
        a = avaliar_liquidez(area_alvo=80, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_OK
        assert a.fator_aplicado == 1.0
        assert a.rebaixa_niveis == 0

    def test_alvo_pouco_menor_ainda_ok(self):
        # razão = 0.80 → ok (faixa [0.75, 1.35))
        a = avaliar_liquidez(area_alvo=64, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_OK

    def test_alvo_pouco_maior_ainda_ok(self):
        # razão = 1.30 → ok
        a = avaliar_liquidez(area_alvo=104, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_OK

    def test_alvo_menor_severidade_media(self):
        # razão = 0.60 → media (faixa [0.55, 0.75))
        a = avaliar_liquidez(area_alvo=48, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_MEDIA
        assert a.fator_aplicado == pytest.approx(0.92)
        assert a.rebaixa_niveis == 1
        assert "menor" in a.mensagem

    def test_alvo_maior_severidade_media(self):
        # razão = 1.50 → media (faixa [1.35, 1.80))
        a = avaliar_liquidez(area_alvo=120, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_MEDIA
        assert a.fator_aplicado == pytest.approx(0.92)
        assert a.rebaixa_niveis == 1
        assert "maior" in a.mensagem

    def test_alvo_muito_menor_severidade_alta(self):
        # razão = 0.40 → alta
        a = avaliar_liquidez(area_alvo=32, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_ALTA
        assert a.fator_aplicado == pytest.approx(0.85)
        assert a.rebaixa_niveis == 2

    def test_alvo_muito_maior_severidade_alta(self):
        # razão = 2.0 → alta
        a = avaliar_liquidez(area_alvo=160, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_ALTA
        assert a.fator_aplicado == pytest.approx(0.85)
        assert a.rebaixa_niveis == 2

    def test_dados_invalidos_devolve_ok_neutro(self):
        a = avaliar_liquidez(area_alvo=0, mediana_area_amostras=80)
        assert a.severidade == SEVERIDADE_OK
        assert a.fator_aplicado == 1.0
        assert a.rebaixa_niveis == 0
