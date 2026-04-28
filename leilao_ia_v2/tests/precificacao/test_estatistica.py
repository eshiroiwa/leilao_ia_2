"""
Testes da estatística robusta (mediana, percentis, IQR, MAD/CV, descarte
boxplot).
"""

from __future__ import annotations

import pytest

from leilao_ia_v2.precificacao.estatistica import (
    cv_robusto_pct,
    descartar_outliers_boxplot,
    iqr,
    mad,
    mediana,
    percentil,
)


class TestMediana:
    def test_lista_impar(self):
        assert mediana([3, 1, 2]) == 2.0

    def test_lista_par_interpola(self):
        assert mediana([1, 2, 3, 4]) == 2.5

    def test_vazia(self):
        assert mediana([]) == 0.0

    def test_um_elemento(self):
        assert mediana([7.5]) == 7.5


class TestPercentil:
    def test_p50_eh_mediana(self):
        valores = [1, 2, 3, 4, 5]
        assert percentil(valores, 50) == pytest.approx(mediana(valores))

    def test_p0_eh_minimo(self):
        assert percentil([10, 20, 30], 0) == 10

    def test_p100_eh_maximo(self):
        assert percentil([10, 20, 30], 100) == 30

    def test_p20_p80_em_lista_uniforme(self):
        # 11 pontos de 0 a 10: P20 = 2.0, P80 = 8.0 (interpolação linear).
        valores = list(range(11))
        assert percentil(valores, 20) == pytest.approx(2.0)
        assert percentil(valores, 80) == pytest.approx(8.0)

    def test_clampa_fora_do_intervalo(self):
        assert percentil([1, 2, 3], -10) == 1
        assert percentil([1, 2, 3], 200) == 3

    def test_vazia(self):
        assert percentil([], 50) == 0.0


class TestIqr:
    def test_distribuicao_conhecida(self):
        valores = list(range(11))  # 0..10
        # Q1=2.5, Q3=7.5, IQR=5.0
        assert iqr(valores) == pytest.approx(5.0)

    def test_vazia(self):
        assert iqr([]) == 0.0


class TestMadEcv:
    def test_mad_resistente_a_outlier(self):
        # MAD não infla com 1 ponto extremo.
        sem_outlier = [10, 12, 11, 13, 9]
        com_outlier = [10, 12, 11, 13, 9, 1000]
        assert mad(com_outlier) <= mad(sem_outlier) * 2.5
        # já desvio-padrão clássico explodiria — não testamos aqui mas é
        # justamente a propriedade desejada do MAD.

    def test_cv_zero_quando_todos_iguais(self):
        assert cv_robusto_pct([5, 5, 5, 5]) == 0.0

    def test_cv_em_porcentagem(self):
        # mediana=10, desvios=[2,2,2,2] → MAD=2 → CV = 20%
        assert cv_robusto_pct([8, 12, 8, 12]) == pytest.approx(20.0, abs=1e-6)

    def test_cv_zero_quando_mediana_zero(self):
        assert cv_robusto_pct([0, 0, 0]) == 0.0


class TestDescarteOutliersBoxplot:
    def test_nao_filtra_quando_n_pequeno(self):
        valores = [1, 100]
        r = descartar_outliers_boxplot(valores)
        assert r.n_descartados == 0
        assert r.valores_dentro == (1.0, 100.0)

    def test_remove_outlier_obvio(self):
        valores = [10, 12, 11, 13, 9, 10, 11, 12, 1000]
        r = descartar_outliers_boxplot(valores)
        assert r.n_descartados >= 1
        assert 1000 not in r.valores_dentro

    def test_nao_remove_quando_sem_outliers(self):
        valores = [10, 11, 12, 10, 11, 12, 10, 11]
        r = descartar_outliers_boxplot(valores)
        assert r.n_descartados == 0

    def test_k_maior_eh_mais_permissivo(self):
        valores = [10, 11, 12, 11, 10, 50]
        r_clas = descartar_outliers_boxplot(valores, k=1.5)
        r_lib = descartar_outliers_boxplot(valores, k=3.0)
        assert r_lib.n_descartados <= r_clas.n_descartados
