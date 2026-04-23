from leilao_ia_v2.schemas.operacao_simulacao import (
    ModoPagamentoSimulacao,
    ModoValorVenda,
    SimulacaoOperacaoInputs,
    SimulacaoOperacaoOutputs,
)
from leilao_ia_v2.services.simulacao_operacao import calcular_simulacao
from leilao_ia_v2.ui.dashboard_comparacao_modais import build_dashboard_comparacao_html, _encargos_operacionais


def test_encargos_operacionais_soma_encargos():
    o = SimulacaoOperacaoOutputs(
        comissao_leiloeiro_brl=1.0,
        itbi_brl=2.0,
        registro_brl=3.0,
        reforma_brl=4.0,
        condominio_atrasado_brl=0.0,
        iptu_atrasado_brl=0.0,
        desocupacao_brl=0.0,
        outros_custos_brl=0.0,
    )
    assert abs(_encargos_operacionais(o) - 10.0) < 0.01


def test_build_dashboard_comparacao_html():
    row = {"id": "x", "area_util": 100.0}
    cache = {"id": "c1", "preco_m2_medio": 5000.0, "anuncios_ids": ""}
    base = dict(
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
    kwargs = {**base, "lance_brl": 200_000.0}
    dv = calcular_simulacao(
        row_leilao=row,
        inp=SimulacaoOperacaoInputs(modo_pagamento=ModoPagamentoSimulacao.VISTA, **kwargs),  # type: ignore[arg-type]
        caches_ordenados=[cache],
        ads_por_id={},
    )
    dp = calcular_simulacao(
        row_leilao=row,
        inp=SimulacaoOperacaoInputs(modo_pagamento=ModoPagamentoSimulacao.PRAZO, **kwargs),  # type: ignore[arg-type]
        caches_ordenados=[cache],
        ads_por_id={},
    )
    df = calcular_simulacao(
        row_leilao=row,
        inp=SimulacaoOperacaoInputs(modo_pagamento=ModoPagamentoSimulacao.FINANCIADO, **kwargs),  # type: ignore[arg-type]
        caches_ordenados=[cache],
        ads_por_id={},
    )
    html = build_dashboard_comparacao_html(
        lance=200_000.0, doc_vista=dv, doc_prazo=dp, doc_fin=df
    )
    assert "dc-grid" in html
    assert "Comparar modalidades" in html
