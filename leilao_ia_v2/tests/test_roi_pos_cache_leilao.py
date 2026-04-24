"""Testes do agente ROI pós-cache (cálculo determinístico)."""

from leilao_ia_v2.services.roi_pos_cache_leilao import (
    aplica_comissao_leiloeiro,
    _custo_fixos,
    _custo_reforma_pos_cache,
    calcular_roi_e_lance_max,
    imovel_sem_reforma_pos_cache,
    metricas_lucro_roi_pos_cache,
    metricas_pos_cache_de_leilao_row,
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


def test_custo_reforma_zero_quando_sem_reforma():
    assert _custo_reforma_pos_cache(80.0, sem_reforma=True) == 0.0


def test_custo_fixos_terreno_reforma_zero_desoc_somente_acima_100m2():
    assert _custo_fixos(80.0, sem_reforma=True) == 0.0
    assert _custo_fixos(120.0, sem_reforma=True) == 10_000.0


def test_imovel_sem_reforma_terreno_e_lote():
    assert imovel_sem_reforma_pos_cache({"tipo_imovel": "terreno"}) is True
    assert imovel_sem_reforma_pos_cache({"tipo_imovel": "lote"}) is True
    assert imovel_sem_reforma_pos_cache({"tipo_imovel": "apartamento"}) is False


def test_metricas_pos_cache_mais_lucro_quando_reforma_zero_terreno():
    m_casa = metricas_lucro_roi_pos_cache(
        500_000.0, 200_000.0, 50.0, aplica_5_leiloeiro=True, sem_reforma=False
    )
    m_terr = metricas_lucro_roi_pos_cache(
        500_000.0, 200_000.0, 50.0, aplica_5_leiloeiro=True, sem_reforma=True
    )
    assert m_casa.get("lucro_bruto_projetado") is not None
    assert m_terr.get("lucro_bruto_projetado") is not None
    assert m_terr["lucro_bruto_projetado"] > m_casa["lucro_bruto_projetado"]


def test_metricas_pos_cache_de_leilao_row_respeita_terreno():
    base = {
        "area_util": 80.0,
        "valor_mercado_estimado": 400_000.0,
        "valor_lance_1_praca": 150_000.0,
        "url_leilao": "https://exemplo.com/l",
    }
    t = {**base, "tipo_imovel": "terreno"}
    a = {**base, "tipo_imovel": "apartamento"}
    mt = metricas_pos_cache_de_leilao_row(t)
    ma = metricas_pos_cache_de_leilao_row(a)
    assert mt is not None and ma is not None
    assert mt["lucro_bruto_projetado"] > ma["lucro_bruto_projetado"]


def test_roi_e_lance_max_com_leiloeiro():
    # V=1_000_000, L=400_000, 80m², com 5% leiloeiro; r = 2% registro; saída com 6% corretagem s/ venda
    roi, lmx = calcular_roi_e_lance_max(1_000_000.0, 400_000.0, 80.0, aplica_5=True)
    assert roi is not None and lmx is not None
    r = 0.05 + 0.03 + 0.02
    cfix = 80.0 * 500.0
    invest = 400_000.0 * (1 + r) + cfix
    # 5+3+2% só no lance; na venda desconta-se apenas corretagem 6%
    vliq = 1_000_000.0 * (1 - 0.06)
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


def test_comissao_leiloeiro_caixa_licitacao_aberta_cobra_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?x=1",
        "leilao_extra_json": {
            "observacoes_markdown": "Data da **Licitação Aberta** - 04/05/2026 - 10h00",
            "modalidade_venda": "leilao",
        },
    }
    assert aplica_comissao_leiloeiro(row) is True


def test_comissao_leiloeiro_caixa_venda_online_sem_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/x",
        "leilao_extra_json": {"regras_leilao_markdown": "Modalidade: venda online (sem comitente presencial)."},
    }
    assert aplica_comissao_leiloeiro(row) is False


def test_comissao_leiloeiro_caixa_venda_direta_online_sem_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/x",
        "edital_markdown": "Tipo de venda: venda direta online - lances pelo portal.",
    }
    assert aplica_comissao_leiloeiro(row) is False


def test_comissao_leiloeiro_caixa_compra_direta_sem_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/x",
        "edital_markdown": "Modalidade de venda: compra direta pelo site da Caixa.",
    }
    assert aplica_comissao_leiloeiro(row) is False


def test_comissao_leiloeiro_caixa_campo_leiloeiro_cobra_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/x",
        "edital_markdown": "Leiloeiro(a): PÉRICLES LUCIANO SANTOS DE JESUS",
    }
    assert aplica_comissao_leiloeiro(row) is True


def test_comissao_leiloeiro_caixa_leilao_unico_cobra_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/x",
        "observacoes_markdown": "Realizar-se-á em sessão pública, em **leilão único** na data abaixo.",
    }
    assert aplica_comissao_leiloeiro(row) is True


def test_comissao_leiloeiro_fora_da_caixa_sempre_5_ainda_que_venda_online_no_texto():
    row = {
        "url_leilao": "https://leiloeiro-xyz.com/imovel/1",
        "leilao_extra_json": {
            "regras_leilao_markdown": "Venda online — lances pelo site.",
        },
    }
    assert aplica_comissao_leiloeiro(row) is True


def test_comissao_leiloeiro_caixa_default_sem_modalidade_cobra_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/x",
        "leilao_extra_json": {},
    }
    assert aplica_comissao_leiloeiro(row) is True


def test_comissao_leiloeiro_leilao_sfi_cobra_5():
    row = {
        "url_leilao": "https://venda-imoveis.caixa.gov.br/x",
        "edital_markdown": "Procedimento: Leilão SFI - edital 12/2025",
    }
    assert aplica_comissao_leiloeiro(row) is True
