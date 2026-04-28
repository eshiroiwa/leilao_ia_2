"""
Testes da homogeneização (fator oferta + fator área Heineck).

Cobre:

- Fator de oferta default 0.90.
- Fator Heineck: direção correta (alvo maior que amostra → fator < 1.0,
  porque amostras pequenas têm R$/m² maior).
- ``homogeneizar`` aplica os dois e zera quando a amostra é inválida.
"""

from __future__ import annotations

import math

import pytest

from leilao_ia_v2.precificacao.dominio import Amostra
from leilao_ia_v2.precificacao.homogeneizacao import (
    EXPOENTE_HEINECK,
    FATOR_OFERTA_DEFAULT,
    fator_area_heineck,
    fator_oferta,
    homogeneizar,
)


def _amostra(*, valor=400_000.0, area=80.0, tipo="apartamento"):
    return Amostra(
        url="https://portal.com/anuncio/x",
        valor_anuncio=valor,
        area_m2=area,
        tipo_imovel=tipo,
        distancia_km=0.5,
        precisao_geo="rua",
        raio_origem_m=500,
    )


class TestFatorOferta:
    def test_default_eh_090(self):
        assert fator_oferta() == FATOR_OFERTA_DEFAULT == 0.90

    def test_aceita_override(self):
        assert fator_oferta(0.85) == pytest.approx(0.85)


class TestFatorAreaHeineck:
    def test_areas_iguais_devolve_um(self):
        assert fator_area_heineck(area_amostra=80, area_alvo=80) == pytest.approx(1.0)

    def test_amostra_menor_que_alvo_devolve_menor_que_um(self):
        # Amostra pequena (50) tem R$/m² alto. Alvo maior (100): puxa para baixo.
        f = fator_area_heineck(area_amostra=50, area_alvo=100)
        assert 0 < f < 1.0
        assert f == pytest.approx(0.5 ** EXPOENTE_HEINECK, rel=1e-6)

    def test_amostra_maior_que_alvo_devolve_maior_que_um(self):
        # Amostra grande (200) tem R$/m² baixo. Alvo menor (100): puxa para cima.
        f = fator_area_heineck(area_amostra=200, area_alvo=100)
        assert f > 1.0
        assert f == pytest.approx(2.0 ** EXPOENTE_HEINECK, rel=1e-6)

    def test_areas_invalidas_devolvem_um_neutro(self):
        assert fator_area_heineck(area_amostra=0, area_alvo=80) == 1.0
        assert fator_area_heineck(area_amostra=80, area_alvo=-1) == 1.0

    def test_expoente_customizado(self):
        f1 = fator_area_heineck(area_amostra=50, area_alvo=100, expoente=0.10)
        f2 = fator_area_heineck(area_amostra=50, area_alvo=100, expoente=0.25)
        # expoente maior amplifica a correção — fica mais distante de 1.
        assert abs(1 - f2) > abs(1 - f1)


class TestHomogeneizar:
    def test_aplica_oferta_e_area_juntos(self):
        a = _amostra(valor=400_000, area=80)  # 5000 R$/m²
        h = homogeneizar(a, area_alvo=80)
        # Áreas iguais: só fator de oferta entra (0.90).
        assert h.fator_area == pytest.approx(1.0)
        assert h.fator_oferta == pytest.approx(0.90)
        assert h.preco_m2_bruto == pytest.approx(5000.0, abs=0.01)
        assert h.preco_m2_ajustado == pytest.approx(5000.0 * 0.90, abs=0.5)

    def test_amostra_menor_alvo_maior_preco_ajustado_menor_que_o_bruto_x_oferta(self):
        # 50m² com R$/m² alto sendo aplicada a alvo de 100m² — extra-redução
        # via Heineck além do fator de oferta.
        a = _amostra(valor=300_000, area=50)  # 6000 R$/m²
        h = homogeneizar(a, area_alvo=100)
        assert h.fator_area < 1.0
        assert h.preco_m2_ajustado < 6000.0 * 0.90

    def test_amostra_invalida_devolve_zeros(self):
        a = _amostra(valor=0, area=80)
        h = homogeneizar(a, area_alvo=80)
        assert h.preco_m2_ajustado == 0.0
        assert h.fator_oferta == 1.0
        assert h.fator_area == 1.0

    def test_alvo_invalido_devolve_zeros(self):
        a = _amostra(valor=400_000, area=80)
        h = homogeneizar(a, area_alvo=0)
        assert h.preco_m2_ajustado == 0.0

    def test_arredondamento_em_quatro_casas(self):
        h = homogeneizar(_amostra(), area_alvo=70)
        assert isinstance(h.fator_area, float)
        # 4 decimais no fator_area, 2 no preço.
        assert math.isclose(h.fator_area, round(h.fator_area, 4))
