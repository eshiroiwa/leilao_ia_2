"""
Tabela comparativa estilo planilha de decisão: cada modalidade com colunas MIN e MAX
(dois lances) e linhas essenciais para análise de arrematação.
"""

from __future__ import annotations

import html
from typing import Literal

from leilao_ia_v2.schemas.operacao_simulacao import (
    ModoPagamentoSimulacao,
    OperacaoSimulacaoDocumento,
    SimulacaoOperacaoInputs,
    SimulacaoOperacaoOutputs,
)

ModoBloco = Literal["vista", "prazo", "fin"]


def _brl(x: float | None) -> str:
    if x is None:
        return "—"
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "—"
    neg = "-" if v < 0 else ""
    a = abs(v)
    inteiro = int(a)
    cent = int(round((a - inteiro) * 100 + 1e-9)) % 100
    if cent >= 100:
        cent = 0
    corpo = f"{inteiro:,}".replace(",", ".")
    return f"{neg}R$ {corpo},{cent:02d}"


def _pct_fraq(frac: float | None) -> str:
    if frac is None:
        return "—"
    v = float(frac) * 100.0
    s = f"{v:,.2f}".replace(",", "§").replace(".", ",").replace("§", ".")
    return f"{s} %"


def margem_bruta_pct(lucro: float, subtotal: float) -> float | None:
    """% lucro bruto s/ subtotal de custo (econômico), p.ex. 42,6."""
    if subtotal is None or float(subtotal) == 0:
        return None
    return 100.0 * float(lucro) / float(subtotal)


def _entrada_prazo(inp: SimulacaoOperacaoInputs) -> float:
    L = max(0.0, float(inp.lance_brl or 0))
    p = max(0.0, min(95.0, float(inp.prazo_entrada_pct or 0.0)))
    return round(L * (p / 100.0), 2)


def _p0_prazo(inp: SimulacaoOperacaoInputs) -> float:
    L = max(0.0, float(inp.lance_brl or 0))
    return max(0.0, round(L - _entrada_prazo(inp), 2))


def _entrada_fin(inp: SimulacaoOperacaoInputs) -> float:
    L = max(0.0, float(inp.lance_brl or 0))
    p = max(5.0, min(50.0, float(getattr(inp, "fin_entrada_pct", 20.0) or 20.0)))
    return round(L * (p / 100.0), 2)


def _p0_fin(inp: SimulacaoOperacaoInputs) -> float:
    L = max(0.0, float(inp.lance_brl or 0))
    return max(0.0, round(L - _entrada_fin(inp), 2))


def _demais_encargos(o: SimulacaoOperacaoOutputs) -> float:
    return float(
        (o.condominio_atrasado_brl or 0)
        + (o.iptu_atrasado_brl or 0)
        + (o.desocupacao_brl or 0)
        + (o.outros_custos_brl or 0)
    )


def _tr(
    label: str,
    vmin: str,
    vmax: str,
    *,
    row_class: str = "",
) -> str:
    rc = f' class="{row_class}"' if row_class else ""
    return (
        f"<tr{rc}><td>{html.escape(label)}</td>"
        f'<td class="cmp-min">{html.escape(vmin)}</td>'
        f'<td class="cmp-max">{html.escape(vmax)}</td></tr>'
    )


def _linha_lucro_bruto(omin: SimulacaoOperacaoOutputs, omax: SimulacaoOperacaoOutputs) -> str:
    def cell(o: SimulacaoOperacaoOutputs) -> str:
        m = margem_bruta_pct(float(o.lucro_bruto or 0), float(o.subtotal_custos_operacao or 0))
        b = _brl(o.lucro_bruto)
        if m is None:
            return b
        s = f"{m:.2f}".replace(".", ",")
        return f"{b} ({s} %)"

    return _tr("Lucro bruto (R$) — % s/ subtotal de custo", cell(omin), cell(omax), row_class="cmp-row-destaque")


def html_bloco_vista(
    doc_min: OperacaoSimulacaoDocumento,
    doc_max: OperacaoSimulacaoDocumento,
) -> str:
    om, ox = doc_min.outputs, doc_max.outputs
    if not om or not ox:
        return ""

    d_av = om.desconto_pagamento_avista_ativo or ox.desconto_pagamento_avista_ativo
    parts: list[str] = [
        _tr("Lance nominal (R$)", _brl(om.lance_brl), _brl(ox.lance_brl)),
    ]
    if d_av:
        parts.append(
            _tr(
                "Lance pago pós-desconto (R$)",
                _brl(om.lance_pago_apos_desconto_brl),
                _brl(ox.lance_pago_apos_desconto_brl),
            )
        )
    parts.extend(
        [
            _tr("Comissão leiloeiro (R$)", _brl(om.comissao_leiloeiro_brl), _brl(ox.comissao_leiloeiro_brl)),
            _tr("ITBI + registro (R$)", _brl(om.itbi_brl + om.registro_brl), _brl(ox.itbi_brl + ox.registro_brl)),
            _tr("Reforma (R$)", _brl(om.reforma_brl), _brl(ox.reforma_brl)),
            _tr("Condom. + IPTU + desocup. + outros (R$)", _brl(_demais_encargos(om)), _brl(_demais_encargos(ox))),
            _tr("Subtotal custos (R$)", _brl(om.subtotal_custos_operacao), _brl(ox.subtotal_custos_operacao), row_class="cmp-row-sub"),
            _tr("Venda estimada (R$)", _brl(om.valor_venda_estimado), _brl(ox.valor_venda_estimado)),
            _tr("Corretagem (R$)", _brl(om.comissao_imobiliaria_brl), _brl(ox.comissao_imobiliaria_brl)),
        ]
    )
    parts.append(_linha_lucro_bruto(om, ox))
    parts.extend(
        [
            _tr("Lucro líquido (R$)", _brl(om.lucro_liquido), _brl(ox.lucro_liquido), row_class="cmp-row-destaque"),
            _tr("ROI bruto (%)", _pct_fraq(om.roi_bruto), _pct_fraq(ox.roi_bruto), row_class="cmp-row-roi"),
            _tr("ROI líquido (%)", _pct_fraq(om.roi_liquido), _pct_fraq(ox.roi_liquido), row_class="cmp-row-roi"),
        ]
    )
    body = "".join(parts)

    return (
        f'<div class="cmp-bloco">'
        f'<p class="cmp-bloco-tit">À vista</p>'
        f'<table class="cmp-tabela" aria-label="Comparação à vista MIN e MAX">'
        f'<thead><tr><th>Indicador</th><th>MIN</th><th>MAX</th></tr></thead>'
        f"<tbody>{body}</tbody></table></div>"
    )


def html_bloco_financiado(
    doc_min: OperacaoSimulacaoDocumento,
    doc_max: OperacaoSimulacaoDocumento,
) -> str:
    om, ox = doc_min.outputs, doc_max.outputs
    im, ix = doc_min.inputs, doc_max.inputs
    if not om or not ox:
        return ""
    parts: list[str] = [
        _tr("Lance nominal (R$)", _brl(om.lance_brl), _brl(ox.lance_brl)),
        _tr(
            f"Entrada ({im.fin_entrada_pct:.2f} % s/ lance) (R$)",
            _brl(_entrada_fin(im)),
            _brl(_entrada_fin(ix)),
        ),
        _tr("Valor a financiar (R$)", _brl(_p0_fin(im)), _brl(_p0_fin(ix))),
        _tr("Comissão leiloeiro (R$)", _brl(om.comissao_leiloeiro_brl), _brl(ox.comissao_leiloeiro_brl)),
        _tr("ITBI + registro (R$)", _brl(om.itbi_brl + om.registro_brl), _brl(ox.itbi_brl + ox.registro_brl)),
        _tr("Reforma (R$)", _brl(om.reforma_brl), _brl(ox.reforma_brl)),
        _tr("Condom. + IPTU + desocup. + outros (R$)", _brl(_demais_encargos(om)), _brl(_demais_encargos(ox))),
        _tr("1.ª prest. (SAC) / PMT (Price) (R$)", _brl(om.pmt_mensal_resolvido), _brl(ox.pmt_mensal_resolvido)),
        _tr("Prazo do financiamento (meses)", str(im.fin_prazo_meses), str(ix.fin_prazo_meses)),
        _tr("Tempo até a venda T (meses)", _fmt_t(om), _fmt_t(ox)),
        _tr("Caixa pago até T (R$)", _brl(om.investimento_cash_ate_momento_venda), _brl(ox.investimento_cash_ate_momento_venda)),
        _tr("Juros no período (até T) (R$)", _brl(om.total_juros_ate_momento_venda), _brl(ox.total_juros_ate_momento_venda)),
        _tr("Saldo a quitar na venda (R$)", _brl(om.saldo_divida_quitacao_na_venda), _brl(ox.saldo_divida_quitacao_na_venda)),
        _tr(
            "Subtotal custos (caixa+quitação) (R$)",
            _brl(om.subtotal_custos_operacao),
            _brl(ox.subtotal_custos_operacao),
            row_class="cmp-row-sub",
        ),
        _tr("Venda estimada (R$)", _brl(om.valor_venda_estimado), _brl(ox.valor_venda_estimado)),
        _tr("Corretagem (R$)", _brl(om.comissao_imobiliaria_brl), _brl(ox.comissao_imobiliaria_brl)),
    ]
    parts.append(_linha_lucro_bruto(om, ox))
    parts.extend(
        [
            _tr("Lucro líquido (R$)", _brl(om.lucro_liquido), _brl(ox.lucro_liquido), row_class="cmp-row-destaque"),
            _tr("ROI bruto (s/ caixa) (%)", _pct_fraq(om.roi_bruto), _pct_fraq(ox.roi_bruto), row_class="cmp-row-roi"),
            _tr("ROI líquido (s/ caixa) (%)", _pct_fraq(om.roi_liquido), _pct_fraq(ox.roi_liquido), row_class="cmp-row-roi"),
        ]
    )
    return (
        f'<div class="cmp-bloco">'
        f'<p class="cmp-bloco-tit">Financiado (bancário) — {html.escape(str(im.fin_sistema))} @ {im.fin_taxa_juros_anual_pct:.2f} % a.a.</p>'
        f'<table class="cmp-tabela" aria-label="Comparação financiado MIN e MAX">'
        f'<thead><tr><th>Indicador</th><th>MIN</th><th>MAX</th></tr></thead>'
        f'<tbody>{"".join(parts)}</tbody></table></div>'
    )


def _fmt_t(o: SimulacaoOperacaoOutputs) -> str:
    t = o.tempo_estimado_venda_meses_resolvido
    if t is None:
        return "—"
    return _fmt_milhar_decimal(t, 1)


def _fmt_milhar_decimal(n: float, dec: int) -> str:
    s = f"{float(n):,.{dec}f}"
    return s.replace(",", "§").replace(".", ",").replace("§", ".")


def html_bloco_prazo(
    doc_min: OperacaoSimulacaoDocumento,
    doc_max: OperacaoSimulacaoDocumento,
) -> str:
    om, ox = doc_min.outputs, doc_max.outputs
    im, ix = doc_min.inputs, doc_max.inputs
    if not om or not ox:
        return ""
    parts: list[str] = [
        _tr("Lance nominal (R$)", _brl(om.lance_brl), _brl(ox.lance_brl)),
        _tr(
            f"Entrada ({im.prazo_entrada_pct:.2f} % s/ lance) (R$)",
            _brl(_entrada_prazo(im)),
            _brl(_entrada_prazo(ix)),
        ),
        _tr("Saldo a parcelar (R$)", _brl(_p0_prazo(im)), _brl(_p0_prazo(ix))),
        _tr("Nº de parcelas (edital / modelo)", str(im.prazo_num_parcelas), str(ix.prazo_num_parcelas)),
        _tr("Juros (modelo) % a.m. s/ saldo", f"{im.prazo_juros_mensal_pct:.2f} %", f"{ix.prazo_juros_mensal_pct:.2f} %"),
        _tr("Comissão leiloeiro (R$)", _brl(om.comissao_leiloeiro_brl), _brl(ox.comissao_leiloeiro_brl)),
        _tr("ITBI + registro (R$)", _brl(om.itbi_brl + om.registro_brl), _brl(ox.itbi_brl + ox.registro_brl)),
        _tr("Reforma (R$)", _brl(om.reforma_brl), _brl(ox.reforma_brl)),
        _tr("Condom. + IPTU + desocup. + outros (R$)", _brl(_demais_encargos(om)), _brl(_demais_encargos(ox))),
        _tr("1.ª prest. (SAC) / PMT (Price) (R$)", _brl(om.pmt_mensal_resolvido), _brl(ox.pmt_mensal_resolvido)),
        _tr("Tempo até a venda T (meses)", _fmt_t(om), _fmt_t(ox)),
        _tr("Caixa pago até T (R$)", _brl(om.investimento_cash_ate_momento_venda), _brl(ox.investimento_cash_ate_momento_venda)),
        _tr("Juros no período (até T) (R$)", _brl(om.total_juros_ate_momento_venda), _brl(ox.total_juros_ate_momento_venda)),
        _tr("Saldo a quitar na venda (R$)", _brl(om.saldo_divida_quitacao_na_venda), _brl(ox.saldo_divida_quitacao_na_venda)),
        _tr(
            "Subtotal custos (caixa+quitação) (R$)",
            _brl(om.subtotal_custos_operacao),
            _brl(ox.subtotal_custos_operacao),
            row_class="cmp-row-sub",
        ),
        _tr("Venda estimada (R$)", _brl(om.valor_venda_estimado), _brl(ox.valor_venda_estimado)),
        _tr("Corretagem (R$)", _brl(om.comissao_imobiliaria_brl), _brl(ox.comissao_imobiliaria_brl)),
    ]
    parts.append(_linha_lucro_bruto(om, ox))
    parts.extend(
        [
            _tr("Lucro líquido (R$)", _brl(om.lucro_liquido), _brl(ox.lucro_liquido), row_class="cmp-row-destaque"),
            _tr("ROI bruto (s/ caixa) (%)", _pct_fraq(om.roi_bruto), _pct_fraq(ox.roi_bruto), row_class="cmp-row-roi"),
            _tr("ROI líquido (s/ caixa) (%)", _pct_fraq(om.roi_liquido), _pct_fraq(ox.roi_liquido), row_class="cmp-row-roi"),
        ]
    )
    return (
        f'<div class="cmp-bloco">'
        f'<p class="cmp-bloco-tit">Parcelado (judicial)</p>'
        f'<table class="cmp-tabela" aria-label="Comparação parcelado MIN e MAX">'
        f'<thead><tr><th>Indicador</th><th>MIN</th><th>MAX</th></tr></thead>'
        f'<tbody>{"".join(parts)}</tbody></table></div>'
    )


def html_cotas_rodape(
    doc_ref: OperacaoSimulacaoDocumento,
    *,
    n_cotas: int,
) -> str:
    """Uma linha de cotas a partir do cenário «min» (subtotal = custo; lucro = líquido)."""
    o = doc_ref.outputs
    if not o or n_cotas < 1:
        return ""
    inv = float(o.subtotal_custos_operacao or 0)
    lq = float(o.lucro_liquido or 0)
    ic = inv / n_cotas
    lc = lq / n_cotas
    return (
        f'<p class="cmp-cotas">Cotas (n={n_cotas}) — subtotal (MIN): { _brl(inv) } — '
        f"por cota (referência) ≈ { _brl(ic) } · "
        f"lucro líquido por cota (MIN) ≈ { _brl(lc) }</p>"
    )


def bloco_por_modo(
    modo: ModoBloco,
    doc_min: OperacaoSimulacaoDocumento,
    doc_max: OperacaoSimulacaoDocumento,
) -> str:
    if modo == "vista":
        return html_bloco_vista(doc_min, doc_max)
    if modo == "fin":
        return html_bloco_financiado(doc_min, doc_max)
    return html_bloco_prazo(doc_min, doc_max)


def html_estilo_cmp() -> str:
    """Paleta escura + texto claro (legível com tema claro/escuro do Streamlit)."""
    return """
<style>
.cmp-dec-wrap { max-width: 100%; color: #f1f5f9 !important; }
.cmp-dec-wrap p, .cmp-dec-wrap th, .cmp-dec-wrap td, .cmp-dec-wrap span { color: #f1f5f9 !important; }
.cmp-dec-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(18rem, 1fr));
  gap: 0.75rem;
  align-items: start;
}
.cmp-dec-head { color: #f8fafc !important; font-size: 1.05rem; font-weight: 600; margin-bottom: 0.5rem; }
.cmp-bloco-tit { font-weight: 600; color: #f8fafc !important; margin: 0.75rem 0 0.35rem; font-size: 0.95rem; }
.cmp-bloco { break-inside: avoid; }
.cmp-tabela { width: 100%; border-collapse: collapse; font-size: 0.9rem; margin-bottom: 0.5rem; color: #f1f5f9; }
.cmp-tabela th, .cmp-tabela td { border: 1px solid #475569; padding: 0.35rem 0.5rem; text-align: right; color: #f1f5f9 !important; }
.cmp-tabela th:first-child, .cmp-tabela td:first-child { text-align: left; }
.cmp-tabela thead th { background: #0c4a6e; color: #f8fafc !important; border-color: #334155; }
.cmp-tabela tbody tr { color: #f1f5f9; background: #1e293b; }
.cmp-tabela tbody tr:nth-child(even) { background: #172033; }
.cmp-tabela .cmp-min { background: #14532d; color: #f0fdf4 !important; }
.cmp-tabela .cmp-max { background: #7c2d12; color: #fff7ed !important; }
.cmp-tabela tr.cmp-row-sub td { background: #0c4a6e; font-weight: 600; color: #e0f2fe !important; }
.cmp-tabela tr.cmp-row-destaque td { background: #854d0e; font-weight: 600; color: #fffbeb !important; }
.cmp-tabela tr.cmp-row-roi td { background: #1e3a3a; color: #ccfbf1 !important; }
.cmp-cotas { font-size: 0.88rem; color: #e2e8f0 !important; margin-top: 0.75rem; }
</style>
"""


def tabela_comparacao_decisao_completa_html(
    *,
    doc_vista: tuple[OperacaoSimulacaoDocumento, OperacaoSimulacaoDocumento],
    doc_prazo: tuple[OperacaoSimulacaoDocumento, OperacaoSimulacaoDocumento],
    doc_fin: tuple[OperacaoSimulacaoDocumento, OperacaoSimulacaoDocumento],
    n_cotas: int,
) -> str:
    """HTML completo: 3 blocos (à vista, financiado, parcelado) com MIN/MAX. Rodapé cotas p/ MIN vista."""
    vm, vx = doc_vista
    pm, px = doc_prazo
    fm, fx = doc_fin
    out = [
        html_estilo_cmp(),
        '<div class="cmp-dec-wrap" style="color: #f1f5f9 !important">',
        '<p class="cmp-dec-head" style="color: #f8fafc !important">Comparativo de decisão (dois lances: MIN e MAX)</p>',
    ]
    out.append('<div class="cmp-dec-grid">')
    out.append(bloco_por_modo("vista", vm, vx))
    out.append(bloco_por_modo("fin", fm, fx))
    out.append(bloco_por_modo("prazo", pm, px))
    out.append("</div>")
    out.append(html_cotas_rodape(vm, n_cotas=n_cotas))
    out.append("</div>")
    return "\n".join(out)
