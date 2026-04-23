from leilao_ia_v2.ui.tabela_comparacao_decisao import html_bloco_vista, margem_bruta_pct


def test_margem_bruta_exemplo_planilha():
    """81.470 / 191.130 ≈ 42,63 % (ex. planilha anexa)."""
    m = margem_bruta_pct(81_470.0, 191_130.0)
    assert m is not None
    assert abs(m - 42.63) < 0.15


def test_margem_subtotal_zero():
    assert margem_bruta_pct(100.0, 0.0) is None
    assert margem_bruta_pct(0.0, 0.0) is None


def test_bloco_vista_html_tem_tabela():
    from leilao_ia_v2.schemas.operacao_simulacao import ModoPagamentoSimulacao, ModoValorVenda, SimulacaoOperacaoInputs
    from leilao_ia_v2.services.simulacao_operacao import calcular_simulacao

    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        tipo_pessoa="PF",
        modo_pagamento=ModoPagamentoSimulacao.VISTA,
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=200_000.0,
        comissao_leiloeiro_pct_sobre_arrematacao=0.0,
        itbi_pct_sobre_arrematacao=0.0,
        registro_pct_sobre_arrematacao=0.0,
        reforma_modo="manual",
        reforma_brl=0.0,
        comissao_imobiliaria_pct_sobre_venda=0.0,
        ir_aliquota_pf_pct=0.0,
    )
    a = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={})
    b = calcular_simulacao(
        row_leilao=row,
        inp=inp.model_copy(update={"lance_brl": 300_000.0}),
        caches_ordenados=[cache],
        ads_por_id={},
    )
    h = html_bloco_vista(a, b)
    assert "À vista" in h
    assert "MIN" in h and "MAX" in h
    assert "cmp-tabela" in h
