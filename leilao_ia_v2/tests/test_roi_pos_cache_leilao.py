"""Testes do agente ROI pós-cache (cálculo determinístico)."""

from leilao_ia_v2.services.roi_pos_cache_leilao import (
    _custo_fixos,
    _custo_reforma_pos_cache,
    calcular_roi_e_lance_max,
    metricas_lucro_roi_pos_cache,
)


def test_custo_reforma_faixas():
    assert _custo_reforma_pos_cache(0.0) == 0.0
    assert _custo_reforma_pos_cache(30.0) == 10_000.0
    assert _custo_reforma_pos_cache(50.0) == 10_000.0
    assert _custo_reforma_pos_cache(60.0) == 15_000.0
    assert _custo_reforma_pos_cache(70.0) == 15_000.0
    assert _custo_reforma_pos_cache(80.0) == 80.0 * 500.0


def test_custo_fixos_80m2_sem_desoc():
    assert _custo_fixos(80.0) == 80.0 * 500.0


def test_custo_fixos_120m2_inclui_desoc():
    assert _custo_fixos(120.0) == 120.0 * 500.0 + 10_000.0


def test_roi_e_lance_max_com_leiloeiro():
    # V=1_000_000, L=400_000, 80m², com 5% leiloeiro; r = 2% registro; saída com 6% corretagem s/ venda
    roi, lmx = calcular_roi_e_lance_max(1_000_000.0, 400_000.0, 80.0, aplica_5=True)
    assert roi is not None and lmx is not None
    r = 0.05 + 0.03 + 0.02
    cfix = 80.0 * 500.0
    invest = 400_000.0 * (1 + r) + cfix
    vliq = 1_000_000.0 * (1 - r) - 0.06 * 1_000_000.0
    assert abs(roi - (vliq - invest) / invest) < 1e-4
    # Verifica equação 50% no lance devolvido (aproximado)
    r2, _ = calcular_roi_e_lance_max(1_000_000.0, lmx, 80.0, aplica_5=True)
    assert r2 is not None
    assert abs(r2 - 0.5) < 0.02


def test_sem_comissao_leiloeiro_maior_lance_max():
    r1, l1 = calcular_roi_e_lance_max(1_000_000.0, 500_000.0, 50.0, aplica_5=True)
    r2, l2 = calcular_roi_e_lance_max(1_000_000.0, 500_000.0, 50.0, aplica_5=False)
    assert l1 is not None and l2 is not None
    assert l2 > l1


def test_lucro_liquido_15_ir_sobre_bruto():
    m = metricas_lucro_roi_pos_cache(100_000.0, 40_000.0, 50.0, aplica_5_leiloeiro=True)
    assert m.get("lucro_bruto_projetado") is not None
    assert m.get("lucro_liquido_projetado") is not None
    lb = float(m["lucro_bruto_projetado"])
    ll = float(m["lucro_liquido_projetado"])
    ir = 0.15 * max(0.0, lb)
    assert abs(ll - (lb - ir)) < 0.02
