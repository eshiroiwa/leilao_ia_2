from leilao_ia_v2.schemas.operacao_simulacao import (
    ModoPagamentoSimulacao,
    ModoValorVenda,
    SimulacaoOperacaoInputs,
)
from leilao_ia_v2.services.simulacao_operacao import REFORMA_RS_M2, calcular_simulacao, resolver_valor_venda_estimado
from leilao_ia_v2.app_assistente_ingestao import _normalizar_selecao_modo_venda


def test_calcular_lucro_roi_com_pct_leiloeiro_itbi():
    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "valor_medio_venda": 0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        tipo_pessoa="PF",
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=300_000.0,
        comissao_leiloeiro_pct_sobre_arrematacao=5.0,
        itbi_pct_sobre_arrematacao=3.5,
        registro_pct_sobre_arrematacao=0.0,
        reforma_modo="manual",
        reforma_brl=0.0,
        desocupacao_brl=0.0,
        comissao_imobiliaria_pct_sobre_venda=6.0,
        ir_aliquota_pf_pct=15.0,
    )
    doc = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={})
    assert doc.outputs is not None
    o = doc.outputs
    assert o.valor_venda_estimado == 500_000.0
    clei_esperado = 300_000 * 0.05
    itbi_esperado = 300_000 * 0.035
    assert abs(o.comissao_leiloeiro_brl - clei_esperado) < 1
    assert abs(o.itbi_brl - itbi_esperado) < 1
    sub = 300_000 + clei_esperado + itbi_esperado
    assert abs(o.subtotal_custos_operacao - sub) < 1
    comimob = 500_000 * 0.06
    assert abs(o.comissao_imobiliaria_brl - comimob) < 1
    lucro = 500_000 - sub - comimob
    assert abs(o.lucro_bruto - lucro) < 2
    assert o.roi_bruto is not None and abs(o.roi_bruto - lucro / sub) < 1e-4


def test_modalidade_prazo_judicial_price():
    """Entrada 30%, 10 parcelas, venda T=3 meses: saldo + cash vs venda."""
    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        modo_pagamento=ModoPagamentoSimulacao.PRAZO,
        tempo_estimado_venda_meses=3.0,
        prazo_entrada_pct=30.0,
        prazo_num_parcelas=10,
        prazo_juros_mensal_pct=0.0,
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=100_000.0,
        comissao_leiloeiro_pct_sobre_arrematacao=0.0,
        itbi_pct_sobre_arrematacao=0.0,
        registro_pct_sobre_arrematacao=0.0,
        reforma_modo="manual",
        reforma_brl=0.0,
        comissao_imobiliaria_pct_sobre_venda=0.0,
        ir_aliquota_pf_pct=0.0,
    )
    o = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={}).outputs
    assert o is not None
    assert o.modo_pagamento_resolvido == "prazo"
    E = 30_000.0
    P0 = 70_000.0
    pmt = P0 / 10.0
    assert abs(o.pmt_mensal_resolvido - pmt) < 0.1
    assert o.investimento_cash_ate_momento_venda > E + 2.5 * pmt - 1.0
    assert o.saldo_divida_quitacao_na_venda > 0.0


def test_desconto_avista_leiloeiro_sobre_lance_nominal():
    """10% de desconto no caixa do lance; comissão 5% e ITBI 3% permanecem sobre 300k."""
    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "valor_medio_venda": 0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        tipo_pessoa="PF",
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=300_000.0,
        desconto_pagamento_avista=True,
        desconto_pagamento_avista_pct=10.0,
        comissao_leiloeiro_pct_sobre_arrematacao=5.0,
        itbi_pct_sobre_arrematacao=3.0,
        registro_pct_sobre_arrematacao=0.0,
        reforma_modo="manual",
        reforma_brl=0.0,
        desocupacao_brl=0.0,
        comissao_imobiliaria_pct_sobre_venda=6.0,
        ir_aliquota_pf_pct=15.0,
    )
    o = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={}).outputs
    assert o is not None
    assert o.lance_brl == 300_000.0
    assert o.lance_pago_apos_desconto_brl == 270_000.0
    assert abs(o.desconto_pagamento_avista_valor_brl - 30_000.0) < 1
    clei = 300_000 * 0.05
    itbi = 300_000 * 0.03
    assert abs(o.comissao_leiloeiro_brl - clei) < 1
    assert abs(o.itbi_brl - itbi) < 1
    sub = 270_000.0 + clei + itbi
    assert abs(o.subtotal_custos_operacao - sub) < 1


def test_ir_pj_pct_sobre_venda_liquida():
    row = {"id": "x", "area_util": 50.0}
    cache = {"id": "c1", "preco_m2_medio": 10_000.0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        tipo_pessoa="PJ",
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=200_000.0,
        comissao_imobiliaria_brl=25_000.0,
        ir_aliquota_pj_pct=6.7,
    )
    doc = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={})
    o = doc.outputs
    assert o is not None
    venda = 500_000.0
    assert o.valor_venda_estimado == venda
    base_ir = venda - 25_000.0
    assert abs(o.base_ir - base_ir) < 1
    assert abs(o.ir_calculado_brl - 0.067 * base_ir) < 1


def test_comissao_imobiliaria_valor_fixo_prevalece_sobre_pct():
    row = {"id": "x", "area_util": 10.0}
    cache = {"id": "c1", "preco_m2_medio": 100_000.0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=50_000.0,
        comissao_imobiliaria_brl=5_000.0,
        comissao_imobiliaria_pct_sobre_venda=50.0,
    )
    o = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={}).outputs
    assert o is not None
    assert o.comissao_imobiliaria_brl == 5_000.0


def test_normalizar_selecao_modo_venda_aceita_label_dinamico_manual():
    order = [
        ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value,
        ModoValorVenda.MANUAL.value,
    ]
    label_map = {
        ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value: "Cache · valor médio — R$ 500.000,00",
        ModoValorVenda.MANUAL.value: "Manual — R$ 350.000,00",
    }
    # Simula rótulo "antigo" retornado pelo widget após mudança do valor manual.
    raw = "Manual — R$ 320.000,00"
    out = _normalizar_selecao_modo_venda(raw, order, label_map)
    assert out == ModoValorVenda.MANUAL.value


def test_reforma_por_m2():
    row = {"id": "x", "area_util": 80.0}
    cache = {"id": "c1", "preco_m2_medio": 1000.0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=10_000.0,
        reforma_modo="media",
    )
    o = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={}).outputs
    assert o is not None
    assert o.reforma_modo_resolvido == "media"
    assert abs(o.reforma_brl - 80 * REFORMA_RS_M2["media"]) < 0.1


def test_simulacao_ignora_cache_terreno_referencia():
    row = {"id": "x", "area_util": 100.0}
    cache_terreno = {
        "id": "t1",
        "preco_m2_medio": 999.0,
        "valor_medio_venda": 0,
        "anuncios_ids": "",
        "metadados_json": {"modo_cache": "terrenos", "uso_simulacao": False, "apenas_referencia": True},
    }
    cache_casa = {
        "id": "c1",
        "preco_m2_medio": 5000.0,
        "valor_medio_venda": 0,
        "anuncios_ids": "",
        "metadados_json": {"modo_cache": "principal", "uso_simulacao": True, "apenas_referencia": False},
    }
    inp = SimulacaoOperacaoInputs(
        tipo_pessoa="PF",
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id=None,
        lance_brl=100_000.0,
        comissao_leiloeiro_pct_sobre_arrematacao=5.0,
        itbi_pct_sobre_arrematacao=3.5,
        registro_pct_sobre_arrematacao=0.0,
        reforma_modo="manual",
        reforma_brl=0.0,
        desocupacao_brl=0.0,
        comissao_imobiliaria_pct_sobre_venda=6.0,
        ir_aliquota_pf_pct=15.0,
    )
    doc = calcular_simulacao(
        row_leilao=row,
        inp=inp,
        caches_ordenados=[cache_terreno, cache_casa],
        ads_por_id={},
    )
    assert doc.outputs is not None
    assert doc.outputs.cache_media_bairro_id_usado == "c1"
    assert doc.outputs.valor_venda_estimado == 500_000.0


def test_resolver_valor_pref_ineligible_cache():
    row = {"id": "x", "area_util": 50.0}
    cache_ref = {
        "id": "r1",
        "preco_m2_medio": 8000.0,
        "anuncios_ids": "",
        "metadados_json": {"apenas_referencia": True, "uso_simulacao": False},
    }
    inp = SimulacaoOperacaoInputs(
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="r1",
        lance_brl=1.0,
    )
    venda, out = resolver_valor_venda_estimado(
        row_leilao=row,
        inp=inp,
        caches_ordenados=[cache_ref],
        ads_por_id={},
    )
    assert venda == 0.0
    assert out.cache_media_bairro_id_usado is None
    assert any("referência" in n for n in out.notas)


def test_lance_maximo_roi_bruto():
    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=100_000.0,
        reforma_modo="media",
        comissao_leiloeiro_pct_sobre_arrematacao=5.0,
        itbi_pct_sobre_arrematacao=3.5,
        registro_pct_sobre_arrematacao=0.0,
        ir_aliquota_pf_pct=0.0,
        roi_desejado_pct=20.0,
        roi_desejado_modo="bruto",
    )
    o = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={}).outputs
    assert o is not None
    assert o.lance_maximo_para_roi_desejado is not None
    assert o.lance_maximo_para_roi_desejado > 0


def test_resolver_cache_menor_valor_venda():
    row = {"id": "x", "area_util": 100.0}
    cache = {
        "id": "c1",
        "menor_valor_venda": 420_000.0,
        "valor_medio_venda": 500_000.0,
        "anuncios_ids": "",
    }
    inp = SimulacaoOperacaoInputs(
        modo_valor_venda=ModoValorVenda.CACHE_MENOR_VALOR_VENDA,
        cache_media_bairro_id="c1",
    )
    venda, out = resolver_valor_venda_estimado(
        row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={}
    )
    assert venda == 420_000.0
    assert out.cache_media_bairro_id_usado == "c1"


def test_resolver_anuncios_menor_valor():
    row = {"id": "x", "area_util": 80.0}
    cache = {"id": "c1", "preco_m2_medio": 0, "anuncios_ids": "a1,a2,a3"}
    ads = {
        "a1": {"valor_venda": 900_000.0},
        "a2": {"valor_venda": 750_000.0},
        "a3": {"valor_venda": 800_000.0},
    }
    inp = SimulacaoOperacaoInputs(
        modo_valor_venda=ModoValorVenda.ANUNCIOS_MENOR_VALOR,
        cache_media_bairro_id="c1",
    )
    venda, out = resolver_valor_venda_estimado(
        row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id=ads
    )
    assert venda == 750_000.0
    assert out.cache_media_bairro_id_usado == "c1"


def test_lucro_bruto_igual_venda_menos_subtotal_s_juros_maior_que_vista_mesma_base():
    """
    Subtotal inclui quitação (caixa+saldo) para bater com o lucro; com juros > 0, lucro
    financiado < à vista, mesmas premissas (sem desconto no lance pago).
    """
    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "anuncios_ids": ""}
    base = dict(
        tipo_pessoa="PF",
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=300_000.0,
        comissao_leiloeiro_pct_sobre_arrematacao=0.0,
        itbi_pct_sobre_arrematacao=0.0,
        registro_pct_sobre_arrematacao=0.0,
        reforma_modo="manual",
        reforma_brl=0.0,
        comissao_imobiliaria_pct_sobre_venda=0.0,
        ir_aliquota_pf_pct=0.0,
        tempo_estimado_venda_meses=6.0,
        fin_entrada_pct=20.0,
        fin_prazo_meses=360,
        fin_taxa_juros_anual_pct=12.0,
        fin_sistema="SAC",
    )
    o_v = calcular_simulacao(
        row_leilao=row,
        inp=SimulacaoOperacaoInputs(modo_pagamento=ModoPagamentoSimulacao.VISTA, **base),
        caches_ordenados=[cache],
        ads_por_id={},
    ).outputs
    o_f = calcular_simulacao(
        row_leilao=row,
        inp=SimulacaoOperacaoInputs(modo_pagamento=ModoPagamentoSimulacao.FINANCIADO, **base),
        caches_ordenados=[cache],
        ads_por_id={},
    ).outputs
    assert o_v is not None and o_f is not None
    for o in (o_v, o_f):
        v = float(o.valor_venda_estimado)
        c = float(o.comissao_imobiliaria_brl)
        s = float(o.subtotal_custos_operacao)
        assert abs(o.lucro_bruto - (v - c - s)) < 0.1
    assert o_f.lucro_bruto < o_v.lucro_bruto
    assert o_f.saldo_divida_quitacao_na_venda > 0.0
    assert o_f.subtotal_custos_operacao > o_v.subtotal_custos_operacao


def test_financiado_sac_primeira_prestacao_inclui_juros_sobre_saldo():
    """P0=160k, 14% a.a., 360m: a 1.ª prestação não é só P/n (~444); inclui juros ~1,7k+."""
    from leilao_ia_v2.services.simulacao_pagamento_prazo_fin import (
        primeira_prestacao_sac,
        taxa_mensal_de_anual,
    )

    p0 = 160_000.0
    n = 360
    im = taxa_mensal_de_anual(14.0)
    p1 = primeira_prestacao_sac(p0, im, n)
    so_amort = p0 / n
    assert p1 > so_amort * 2.5
    assert p1 > 2_000.0
    assert abs(p1 - (so_amort + p0 * im)) < 0.02

    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "anuncios_ids": ""}
    inp = SimulacaoOperacaoInputs(
        modo_pagamento=ModoPagamentoSimulacao.FINANCIADO,
        modo_valor_venda=ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        cache_media_bairro_id="c1",
        lance_brl=200_000.0,
        fin_entrada_pct=20.0,
        fin_prazo_meses=360,
        fin_taxa_juros_anual_pct=14.0,
        fin_sistema="SAC",
        comissao_leiloeiro_pct_sobre_arrematacao=0.0,
        itbi_pct_sobre_arrematacao=0.0,
        registro_pct_sobre_arrematacao=0.0,
        reforma_modo="manual",
        reforma_brl=0.0,
        comissao_imobiliaria_pct_sobre_venda=0.0,
        ir_aliquota_pf_pct=0.0,
        tempo_estimado_venda_meses=6.0,
    )
    o = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=[cache], ads_por_id={}).outputs
    assert o is not None
    assert abs(o.pmt_mensal_resolvido - p1) < 1.0


def test_normalizar_selecao_modo_venda_reverte_rotulo_format_func():
    """Evita ValueError quando o estado do selectbox guarda o rótulo (ex.: Manual — R$ …)."""
    from leilao_ia_v2.app_assistente_ingestao import _normalizar_selecao_modo_venda

    order = [
        ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value,
        ModoValorVenda.MANUAL.value,
    ]
    labels = {
        ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value: "Cache · valor médio — R$ 800.000,00",
        ModoValorVenda.MANUAL.value: "Manual — R$ 1.400.000,00",
    }
    assert _normalizar_selecao_modo_venda("manual", order, labels) == ModoValorVenda.MANUAL.value
    assert (
        _normalizar_selecao_modo_venda("Manual — R$ 1.400.000,00", order, labels)
        == ModoValorVenda.MANUAL.value
    )
    assert _normalizar_selecao_modo_venda(None, order, labels) in order
