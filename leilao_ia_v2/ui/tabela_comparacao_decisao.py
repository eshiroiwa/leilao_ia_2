"""
Tabela comparativa estilo planilha de decisão: cada modalidade com colunas MIN e MAX
(dois lances) e linhas essenciais para análise de arrematação.
"""

from __future__ import annotations

import html

from leilao_ia_v2.schemas.operacao_simulacao import (
    OperacaoSimulacaoDocumento,
    SimulacaoOperacaoOutputs,
)


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
