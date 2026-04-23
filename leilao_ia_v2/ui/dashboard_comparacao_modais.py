"""
Dashboard visual para comparar modalidades (à vista, parcelado, financiado) com o mesmo lance.
HTML + CSS autónomo (legível com tema claro/escuro do Streamlit).
"""

from __future__ import annotations

import html
from dataclasses import dataclass

from leilao_ia_v2.schemas.operacao_simulacao import (
    OperacaoSimulacaoDocumento,
    SimulacaoOperacaoOutputs,
)


def _brl(n: float | None) -> str:
    if n is None:
        return "—"
    try:
        v = float(n)
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


def _pct(frac: float | None) -> str:
    if frac is None:
        return "—"
    s = f"{float(frac) * 100:.2f}".replace(".", ",")
    return f"{s} %"


def _encargos_operacionais(o: SimulacaoOperacaoOutputs) -> float:
    return float(
        (o.comissao_leiloeiro_brl or 0)
        + (o.itbi_brl or 0)
        + (o.registro_brl or 0)
        + (o.reforma_brl or 0)
        + (o.condominio_atrasado_brl or 0)
        + (o.iptu_atrasado_brl or 0)
        + (o.desocupacao_brl or 0)
        + (o.outros_custos_brl or 0)
    )


@dataclass
class _CardMetrics:
    titulo: str
    css_var: str
    o: SimulacaoOperacaoOutputs


def _row(label: str, value: str, *, muted: bool = False, sub: str | None = None) -> str:
    cl = "dc-row" + (" dc-muted" if muted else "")
    sub_html = (
        f'<div class="dc-sub" style="margin:0.1rem 0 0; font-size:0.7rem;">{html.escape(sub)}</div>'
        if sub
        else ""
    )
    return (
        f'<div class="{cl}"><span class="dc-lbl">{html.escape(label)}</span>'
        f'<span class="dc-val">{html.escape(value)}</span></div>{sub_html}'
    )


def _row_detalhe_painel(
    label: str,
    value: str,
    *,
    muted: bool = False,
    sub: str | None = None,
) -> str:
    """
    Linha do detalhamento do painel Simulação: rótulo + conector + valor
    (evita o vão enorme de ``space-between`` em ecrãs largos).
    """
    cl = "sp-sim-line" + (" sp-sim-line--muted" if muted else "")
    sub_html = (
        f'<div class="sp-sim-line-sub">{html.escape(sub)}</div>'
        if sub
        else ""
    )
    return (
        f'<div class="{cl}">'
        f'<span class="sp-sim-line-lbl">{html.escape(label)}</span>'
        f'<span class="sp-sim-line-dots" aria-hidden="true"></span>'
        f'<span class="sp-sim-line-val">{html.escape(value)}</span>'
        f"</div>{sub_html}"
    )


def _sec_grupo_painel(titulo: str, linhas: list[str]) -> str:
    if not linhas:
        return ""
    inner = "".join(linhas)
    arial = html.escape(titulo, quote=True)
    return (
        f'<div class="sp-sim-grupo" role="group" aria-label="{arial}">'
        f'<div class="sp-sim-sec"><span class="sp-sim-sec-tit">{html.escape(titulo)}</span></div>'
        f'<div class="sp-sim-grupo-lines">{inner}</div></div>'
    )


def _row_subtotal_painel(label: str, value: str, *, sub: str | None = None) -> str:
    sub_html = (
        f'<div class="sp-sim-line-sub sp-sim-line-sub--sum">{html.escape(sub)}</div>'
        if sub
        else ""
    )
    return (
        f'<div class="sp-sim-line sp-sim-line--subtot">'
        f'<span class="sp-sim-line-lbl">{html.escape(label)}</span>'
        f'<span class="sp-sim-line-dots" aria-hidden="true"></span>'
        f'<span class="sp-sim-line-val sp-sim-line-val--sum">{html.escape(value)}</span>'
        f"</div>{sub_html}"
    )


def _painel_totais_decomp_por_cenario(o: SimulacaoOperacaoOutputs) -> dict[str, float | int | str | None]:
    """
    Totais coerentes: usa campos novos do cálculo ou recompõe a partir dos valores
    exibidos (útil para simulações gravadas antes da versão com subtotais).
    """
    m = str(o.modo_pagamento_resolvido or "vista").strip().lower()
    lance = max(0.0, float(o.lance_brl or 0.0))
    lp = max(0.0, float(o.lance_pago_apos_desconto_brl or 0) or lance)
    clei = max(0.0, float(o.comissao_leiloeiro_brl or 0.0))
    itbi = max(0.0, float(o.itbi_brl or 0.0))
    reg = max(0.0, float(o.registro_brl or 0.0))
    s_arrem = max(0.0, float(getattr(o, "subtotal_grupo_arrematacao_brl", 0) or 0.0))
    if s_arrem < 0.01 and lance > 0:
        s_arrem = round(lp + clei + itbi + reg, 2)
    s_imo = max(0.0, float(getattr(o, "subtotal_grupo_imovel_obra_brl", 0) or 0.0))
    if s_imo < 0.01 and lance >= 0:
        s_imo = round(
            float(o.condominio_atrasado_brl or 0)
            + float(o.iptu_atrasado_brl or 0)
            + float(o.reforma_brl or 0)
            + float(o.desocupacao_brl or 0)
            + float(o.outros_custos_brl or 0),
            2,
        )
    t_prest = max(0.0, float(getattr(o, "total_parcelas_acumuladas_ate_t_brl", 0) or 0.0))
    n_prest = int(getattr(o, "num_prestacoes_contrato_resolvido", 0) or 0)
    d_ini = max(0.0, float(getattr(o, "desembolso_inicial_lance_ou_entrada_brl", 0) or 0.0))
    if d_ini < 0.01 and lance > 0:
        if m == "vista":
            d_ini = round(lp, 2)
        else:
            d_ini = max(0.0, round(s_arrem - clei - itbi - reg, 2))
    if t_prest < 0.01 and m in ("prazo", "financiado"):
        caixa = max(0.0, float(o.investimento_cash_ate_momento_venda or 0))
        guess = max(0.0, round(caixa - s_arrem - s_imo, 2))
        if guess > 0.5:
            t_prest = guess
    e_pct: float | None = None
    if lance > 0.1 and m in ("prazo", "financiado") and d_ini > 0:
        e_pct = round(100.0 * d_ini / lance, 2)
    return {
        "modo": m,
        "s_arrem": s_arrem,
        "s_imo": s_imo,
        "t_prest": t_prest,
        "d_ini": d_ini,
        "n_prest": n_prest,
        "e_pct": e_pct,
    }


def _build_card(cm: _CardMetrics) -> str:
    o = cm.o
    inv_tot = o.subtotal_custos_operacao
    enc = _encargos_operacionais(o)
    juro = float(o.total_juros_ate_momento_venda or 0)
    caixa = float(o.investimento_cash_ate_momento_venda or 0)
    quita = float(o.saldo_divida_quitacao_na_venda or 0)
    lance_n = o.lance_brl
    lb = o.lucro_bruto
    lq = o.lucro_liquido
    roib = o.roi_bruto
    roil = o.roi_liquido
    venda = o.valor_venda_estimado
    cimob = o.comissao_imobiliaria_brl

    lucro_cls = "dc-lucro dc-ok" if (lq or 0) >= 0 else "dc-lucro dc-neg"
    pico = max(abs(lq or 0), 1.0) * 0.5
    bar_w = 50.0 if lq is None else max(6.0, min(100.0, 50 + (float(lq) / pico) * 20))

    return f"""
<article class="dc-card" style="--dc-accent: var({cm.css_var});">
  <div class="dc-card-head">
    <span class="dc-badge">{html.escape(cm.titulo)}</span>
  </div>
  <div class="dc-hero {lucro_cls}">
    <span class="dc-hero-lbl">Lucro líquido</span>
    <span class="dc-hero-val">{_brl(lq)}</span>
    <div class="dc-bar" role="progressbar" aria-label="Direcção do lucro" style="width: {bar_w:.0f}%;"></div>
  </div>
  <div class="dc-stack">
    {_row("Lance (referência deste painel)", _brl(lance_n))}
    {_row("Investimento total (até a saída)", _brl(inv_tot), muted=False)}
    {_row("Custos gerais (leil., ITBI, reg., obra, cond., etc.)", _brl(enc))}
    {_row("Juros (período até a venda)", _brl(juro) if juro > 0.005 else "— (à vista)")}
    {_row("Desembolso de caixa (até T)", _brl(caixa))}
    {_row("Quitação na venda (dívida)", _brl(quita) if quita > 0.5 else "—")}
    {_row("Venda estimada", _brl(venda), muted=True)}
    {_row("Corretagem (saída)", _brl(cimob), muted=True)}
  </div>
  <div class="dc-mid">
    <div>
      <span class="dc-mid-lbl">Lucro bruto</span>
      <span class="dc-mid-v">{_brl(lb)}</span>
    </div>
    <div>
      <span class="dc-mid-lbl">Lucro líquido</span>
      <span class="dc-mid-v">{_brl(lq)}</span>
    </div>
    <div>
      <span class="dc-mid-lbl">ROI líq.</span>
      <span class="dc-mid-v dc-tag">{_pct(roil)}</span>
    </div>
    <div>
      <span class="dc-mid-lbl">ROI bruto</span>
      <span class="dc-mid-v dc-tag">{_pct(roib)}</span>
    </div>
  </div>
</article>"""


def _doc_or_none(d: OperacaoSimulacaoDocumento | None) -> SimulacaoOperacaoOutputs | None:
    if d is None or d.outputs is None:
        return None
    return d.outputs


_ROI_BRUTO_PAINEL_ALVO = 0.4


def _lucro_hero_class(lucro: float | None) -> str:
    return "dc-ok" if (lucro or 0) >= 0 else "dc-neg"


def _lucro_bruto_cls(o: SimulacaoOperacaoOutputs) -> str:
    rb = o.roi_bruto
    if rb is not None and float(rb) < _ROI_BRUTO_PAINEL_ALVO:
        return "dc-neg"
    return _lucro_hero_class(o.lucro_bruto)


def _roi_painel_bruto(o: SimulacaoOperacaoOutputs) -> str:
    rb = f"{(o.roi_bruto or 0) * 100:.2f} %".replace(".", ",")
    rba = o.roi_bruto_anualizado
    anual = f" · anual {(rba * 100):.2f} %".replace(".", ",") if rba is not None else ""
    return f"ROI bruto {rb}{anual}"


def _roi_painel_liquido(o: SimulacaoOperacaoOutputs) -> str:
    rl = f"{(o.roi_liquido or 0) * 100:.2f} %".replace(".", ",")
    rla = o.roi_liquido_anualizado
    anual = f" · anual {(rla * 100):.2f} %".replace(".", ",") if rla is not None else ""
    return f"ROI líquido {rl}{anual}"


def build_painel_simulacao_resumo_html(
    o: SimulacaoOperacaoOutputs,
    *,
    embutir_css: bool = True,
    incluir_cabecalho_rodape: bool = True,
) -> str:
    """
    Painel financeiro da aba Simulação: dois destaques (lance, venda), um bloco detalhado,
    dois destaques (lucro bruto / líquido com ROI) e o lance máximo recomendado.
    Reutiliza o visual ``dc-*`` do dashboard Comparar.

    ``embutir_css``: em relatórios HTML, passar False e injetar ``PAINEL_SIMULACAO_RESUMO_DASH_STYLES`` uma vez.
    ``incluir_cabecalho_rodape``: False omite título/rodapé do cartão (ex.: colunas de comparação no relatório).
    """
    lance_n = o.lance_brl
    venda = o.valor_venda_estimado
    juro = float(o.total_juros_ate_momento_venda or 0)
    caixa = float(o.investimento_cash_ate_momento_venda or 0)
    quita = float(o.saldo_divida_quitacao_na_venda or 0)
    cimob = o.comissao_imobiliaria_brl

    lance_sub = "nominal (arrematação)"
    if o.desconto_pagamento_avista_ativo and (o.desconto_pagamento_avista_valor_brl or 0) > 0.01:
        lance_sub = "nominal — leiloeiro/ITBI/reg. (%) s/ cheio"
    venda_sub = "estimada"
    mpr = o.modo_pagamento_resolvido
    if mpr is not None and str(mpr).strip():
        venda_sub = f"estimada · {str(mpr).replace('_', ' ')}"

    sub_lbl = "sem corretagem"
    if (o.saldo_divida_quitacao_na_venda or 0) > 0.5:
        sub_lbl = "incl. quitação (caixa+saldo) · sem corretagem"

    clei_sub = ""
    if (o.comissao_leiloeiro_pct_efetivo or 0) > 0:
        clei_sub = f"{o.comissao_leiloeiro_pct_efetivo:.2f} % s/ lance".replace(".", ",")
    elif o.comissao_leiloeiro_brl and o.comissao_leiloeiro_brl > 0:
        clei_sub = "fixo"
    itbi_sub = ""
    if (o.itbi_pct_efetivo or 0) > 0:
        itbi_sub = f"{o.itbi_pct_efetivo:.2f} % s/ lance".replace(".", ",")
    elif o.itbi_brl and o.itbi_brl > 0:
        itbi_sub = "fixo (legado)"
    reg_sub = ""
    if (o.registro_pct_efetivo or 0) > 0:
        reg_sub = f"{o.registro_pct_efetivo:.2f} % s/ lance".replace(".", ",")
    elif o.registro_brl and o.registro_brl > 0:
        reg_sub = "fixo"
    ir_sub = ""
    if o.ir_usou_manual:
        ir_sub = "valor fixo informado"
    elif (o.base_ir or 0) > 0:
        ir_sub = f"Base p/ IR {_brl(o.base_ir)}"
    ctot = o.custo_total_com_corretagem
    dco = _painel_totais_decomp_por_cenario(o)
    modo_s = str(dco.get("modo") or "vista")
    s_arrem = float(dco.get("s_arrem") or 0.0)
    s_imo = float(dco.get("s_imo") or 0.0)
    t_prest = float(dco.get("t_prest") or 0.0)
    d_ini = float(dco.get("d_ini") or 0.0)
    n_prest = int(dco.get("n_prest") or 0)
    e_pct = dco.get("e_pct")

    g_ctx: list[str] = []
    if (o.tempo_estimado_venda_meses_resolvido or 0) > 0 and str(o.modo_pagamento_resolvido or "").strip():
        mp_r = str(o.modo_pagamento_resolvido or "").replace("_", " ")
        g_ctx.append(
            _row_detalhe_painel("Tempo até a venda (T)", f"{o.tempo_estimado_venda_meses_resolvido:.1f} meses · {mp_r}")
        )

    g_arrem: list[str] = []
    if modo_s == "vista":
        g_arrem.append(
            _row_detalhe_painel(
                "Lance pago (caixa, à vista)",
                _brl(d_ini),
                sub="Após desconto à vista, se houver" if o.desconto_pagamento_avista_ativo else "Lance nominal, sem desconto",
            )
        )
    else:
        ent_sub = f"{e_pct:.2f} % s/ lance nominal".replace(".", ",") if e_pct is not None else "sobre o lance nominal"
        g_arrem.append(
            _row_detalhe_painel("Entrada (desembolso inicial)", _brl(d_ini), sub=ent_sub)
        )
    g_arrem.extend(
        [
            _row_detalhe_painel("Comissão leiloeiro", _brl(o.comissao_leiloeiro_brl), sub=clei_sub or None, muted=False),
            _row_detalhe_painel("ITBI", _brl(o.itbi_brl), sub=itbi_sub or None, muted=False),
            _row_detalhe_painel("Registro", _brl(o.registro_brl), sub=reg_sub or None, muted=False),
        ]
    )
    s_parts = d_ini + float(o.comissao_leiloeiro_brl or 0) + float(o.itbi_brl or 0) + float(o.registro_brl or 0)
    chk1 = f"Soma das linhas: {_brl(s_parts)}" if abs(s_parts - s_arrem) < 0.5 else f"Atenção: soma R$ {_brl(s_parts)} · subtotal R$ {_brl(s_arrem)} (arredond.)"
    g_arrem.append(_row_subtotal_painel("Subtotal — arrematação e tributos", _brl(s_arrem), sub=chk1))

    g_av: list[str] = []
    if o.desconto_pagamento_avista_ativo and (o.desconto_pagamento_avista_valor_brl or 0) > 0.01:
        g_av.append(
            _row_detalhe_painel(
                "Desconto (à vista)",
                _brl(o.desconto_pagamento_avista_valor_brl),
                muted=True,
            )
        )

    g_imo: list[str] = [
        _row_detalhe_painel("Condomínio atrasado", _brl(o.condominio_atrasado_brl)),
        _row_detalhe_painel("IPTU atrasado", _brl(o.iptu_atrasado_brl)),
        _row_detalhe_painel(
            "Reforma",
            _brl(o.reforma_brl),
            sub=(str(o.reforma_modo_resolvido) if o.reforma_modo_resolvido else None),
            muted=False,
        ),
        _row_detalhe_painel("Desocupação", _brl(o.desocupacao_brl)),
        _row_detalhe_painel("Outros custos", _brl(o.outros_custos_brl)),
    ]
    s_imo_chk = (
        float(o.condominio_atrasado_brl or 0)
        + float(o.iptu_atrasado_brl or 0)
        + float(o.reforma_brl or 0)
        + float(o.desocupacao_brl or 0)
        + float(o.outros_custos_brl or 0)
    )
    g_imo.append(
        _row_subtotal_painel(
            "Subtotal — imóvel e débitos",
            _brl(s_imo),
            sub="Soma das linhas: " + _brl(s_imo_chk)
            if abs(s_imo_chk - s_imo) < 0.5
            else f"Soma R$ {_brl(s_imo_chk)} · subtotal R$ {_brl(s_imo)}",
        )
    )

    g_prest: list[str] = []
    if modo_s in ("prazo", "financiado") and t_prest > 0.005:
        g_prest.append(
            _row_detalhe_painel("Total em prestações (até T)", _brl(t_prest), sub="Principal + juros, conforme tabela (Price ou SAC)", muted=False)
        )
        g_prest.append(
            _row_detalhe_painel(
                "Juros no período (até T)",
                _brl(juro) if juro > 0.005 else "—",
                sub="Incluídos no total acima (amortização)",
                muted=True,
            )
        )
        pmt_v = float(o.pmt_mensal_resolvido or 0.0)
        pmt_sub = (
            f"Contrato: {n_prest} prestações · SAC = 1.ª da série; Price = PMT fixa"
            if modo_s == "financiado" and n_prest > 0
            else (f"Judicial: {n_prest} parcelas (tabela Price)" if n_prest > 0 else None)
        )
        g_prest.append(
            _row_detalhe_painel(
                "Parcela mensal estimada",
                _brl(pmt_v) if pmt_v > 0.005 else "—",
                sub=pmt_sub,
            )
        )
        g_prest.append(_row_subtotal_painel("Subtotal — financiamento (até T)", _brl(t_prest), sub="Igual ao total em prestações (caixa)"))

    g_conf: list[str] = [
        _row_subtotal_painel(
            "Soma (arrematação + imóvel + prestações até T)",
            _brl(s_arrem + s_imo + t_prest),
            sub="Deve alinhar ao desembolso de caixa abaixo",
        ),
        _row_detalhe_painel("Desembolso de caixa (até T, cenário)", _brl(caixa), sub=sub_lbl, muted=True),
    ]
    if abs(s_arrem + s_imo + t_prest - caixa) > 0.5:
        g_conf.append(
            _row_detalhe_painel(
                "Diferença (soma vs cenário)",
                _brl(s_arrem + s_imo + t_prest - caixa),
                sub="Tolerância de arredondamento ou simulação antiga — recalcule (gravar).",
                muted=True,
            )
        )
    g_conf.append(
        _row_detalhe_painel("Quitação na venda (dívida)", _brl(quita) if quita > 0.5 else "—", sub="A liquidar com repasse/instituição", muted=True)
    )
    g_conf.append(
        _row_detalhe_painel("Corretagem (na venda)", _brl(cimob), sub="Não compõe o caixa da arrematação", muted=True)
    )
    g_conf.append(
        _row_subtotal_painel("Custo total (operação + corretagem)", _brl(ctot), sub="Subtotal + corretagem (base IR PJ, etc.)")
    )
    g_fiscal = [
        _row_detalhe_painel("IR", _brl(o.ir_calculado_brl), sub=ir_sub or None, muted=True),
    ]

    blocos: list[str] = [
        _sec_grupo_painel("Horizonte e modalidade", g_ctx) if g_ctx else "",
        _sec_grupo_painel("Arrematação: desembolso inicial e tributos", g_arrem),
        _sec_grupo_painel("Condição à vista (edital)", g_av) if g_av else "",
        _sec_grupo_painel("Imóvel, obra e débitos", g_imo),
        _sec_grupo_painel("Parcelamento (até a venda)", g_prest) if g_prest else "",
        _sec_grupo_painel("Conferência de caixa e venda", g_conf),
        _sec_grupo_painel("Fiscal (IR)", g_fiscal),
    ]
    # Uma linha: recuos/quebras no meio de st.markdown (unsafe_allow_html) viram bloco de código.
    _det_inner = "".join(b for b in blocos if b)
    stack_html = f'<div class="sp-sim-detail-list">{_det_inner}</div>'

    lb_cls = _lucro_bruto_cls(o)
    ll_cls = _lucro_hero_class(o.lucro_liquido)
    pico = max(abs(o.lucro_liquido or 0), 1.0) * 0.5
    lq = o.lucro_liquido
    bar_w = 50.0 if lq is None else max(6.0, min(100.0, 50 + (float(lq) / pico) * 20))
    pico_b = max(abs(o.lucro_bruto or 0), 1.0) * 0.5
    lb_ = o.lucro_bruto
    bar_b = 50.0 if lb_ is None else max(6.0, min(100.0, 50 + (float(lb_) / pico_b) * 20))

    lmx = o.lance_maximo_para_roi_desejado
    lmx_ok = lmx is not None and float(lmx) > 0
    lmax_sub_parts: list[str] = []
    rinf = o.roi_desejado_pct_informado
    if rinf is not None and float(rinf) > 0:
        modo_inf = str(o.roi_desejado_modo_informado or "bruto").strip().lower()
        base_lbl = "ROI bruto" if modo_inf in ("bruto", "") else "ROI líquido"
        lmax_sub_parts.append(f"Meta {float(rinf):.2f} % · {base_lbl}".replace(".", ","))
    if o.lance_maximo_roi_notas:
        lmax_sub_parts.append(str(o.lance_maximo_roi_notas[0])[:200])
    lmax_sub = " · ".join(lmax_sub_parts) if lmax_sub_parts else (
        "Informe ROI desejado > 0 para estimar o teto de lance." if not lmx_ok else "Alvo de sensibilidade"
    )
    lmax_sub_esc = html.escape(lmax_sub)
    lmx_hero_cl = "dc-ok" if lmx_ok else ""
    lmx_val_col = "#6ee7b7" if lmx_ok else "hsl(215, 18%, 70%)"
    lmx_brl_txt = _brl(lmx) if lmx_ok else "—"

    css_bloco = (
        (CSS_DASH + f"\n<style>\n{PAINEL_SIMULACAO_SCOPED_ONLY}\n</style>\n")
        if embutir_css
        else ""
    )
    bloco_cab = (
        """  <header class="dc-top">
    <div class="dc-top-inner">
      <h2 class="dc-h2" style="font-size:1.15rem;">Painel financeiro</h2>
      <p class="dc-hint" style="margin-top:0.45rem">Resumo da modalidade e parâmetros atuais. Mesmo estilo do painel <strong>Comparar</strong>.</p>
    </div>
  </header>
"""
        if incluir_cabecalho_rodape
        else ""
    )
    bloco_rod = (
        '  <p class="dc-foot">T e modalidade afetam juros, caixa e ROI. Use <strong>Gravar</strong> para persistir no banco.</p>\n'
        if incluir_cabecalho_rodape
        else ""
    )
    return f"""
{css_bloco}<div class="dc-root sp-sim-financeiro" lang="pt-BR">
{bloco_cab}  <div class="sp-sim-hero-grid">
    <article class="dc-card" style="--dc-accent: var(--dc-a);">
      <div class="dc-card-head"><span class="dc-badge">Lance (arrematação)</span></div>
      <div class="sp-sim-hero-g">
        <span class="sp-sim-hero-tit">Valor do lance</span>
        <div class="sp-sim-hero-amt" style="color:#6ee7b7;">{_brl(lance_n)}</div>
        <div class="sp-sim-hero-sub">{html.escape(lance_sub)}</div>
      </div>
    </article>
    <article class="dc-card" style="--dc-accent: var(--dc-b);">
      <div class="dc-card-head"><span class="dc-badge">Venda estimada</span></div>
      <div class="sp-sim-hero-g">
        <span class="sp-sim-hero-tit">Valor de venda</span>
        <div class="sp-sim-hero-amt" style="color:#a5b4fc;">{_brl(venda)}</div>
        <div class="sp-sim-hero-sub">{html.escape(venda_sub)}</div>
      </div>
    </article>
  </div>
  <div class="dc-grid" style="grid-template-columns: 1fr; padding: 0 1.25rem 1rem 1.25rem;">
    <article class="dc-card sp-sim-main" style="--dc-accent: 215 20% 45%;">
      <div class="dc-card-head"><span class="dc-badge">Detalhamento</span>
      <p class="dc-sub" style="margin-top:0.35rem">Composição de custos, caixa, tributos e IR — agrupado para leitura rápida.</p></div>{stack_html}
    </article>
  </div>
  <div class="sp-sim-hero-grid">
    <article class="dc-card" style="--dc-accent: var(--dc-c);">
      <div class="dc-card-head"><span class="dc-badge">Lucro bruto</span></div>
      <div class="dc-hero {lb_cls}">
        <span class="dc-hero-lbl">Lucro bruto</span>
        <span class="dc-hero-val">{_brl(o.lucro_bruto)}</span>
        <p class="dc-sub" style="margin:0.4rem 0 0; font-size:0.8rem;">{_roi_painel_bruto(o)}</p>
        <div class="dc-bar" role="progressbar" style="width: {bar_b:.0f}%;" aria-label="Direcção do lucro bruto"></div>
      </div>
    </article>
    <article class="dc-card" style="--dc-accent: 160 50% 42%;">
      <div class="dc-card-head"><span class="dc-badge">Lucro líquido</span></div>
      <div class="dc-hero {ll_cls}">
        <span class="dc-hero-lbl">Lucro líquido (após IR)</span>
        <span class="dc-hero-val">{_brl(lq)}</span>
        <p class="dc-sub" style="margin:0.4rem 0 0; font-size:0.8rem;">{_roi_painel_liquido(o)}</p>
        <div class="dc-bar" role="progressbar" style="width: {bar_w:.0f}%;" aria-label="Direcção do lucro líquido"></div>
      </div>
    </article>
  </div>
  <div class="dc-grid sp-sim-lmx" style="grid-template-columns: 1fr; padding: 0 1.25rem 1.25rem 1.25rem;">
    <article class="dc-card" style="--dc-accent: 280 50% 50%; min-height:6rem">
      <div class="dc-card-head"><span class="dc-badge">Sensibilidade (ROI desejado)</span></div>
      <div class="dc-hero {lmx_hero_cl}" style="margin-top:0.5rem; margin-bottom:0.5rem">
        <span class="dc-hero-lbl">Lance máximo recomendado</span>
        <span class="dc-hero-val" style="font-size:1.45rem; color: {lmx_val_col};">{lmx_brl_txt}</span>
        <p class="dc-sub" style="margin:0.5rem 0 0.2rem; font-size:0.8rem; line-height:1.4;">{lmax_sub_esc}</p>
      </div>
    </article>
  </div>
{bloco_rod}</div>
"""


def build_dashboard_comparacao_html(
    *,
    lance: float,
    doc_vista: OperacaoSimulacaoDocumento | None,
    doc_prazo: OperacaoSimulacaoDocumento | None,
    doc_fin: OperacaoSimulacaoDocumento | None,
) -> str:
    """Gera o HTML do dashboard. Texto escapado. CSS scope em `.dc-root`."""
    esc_lance = _brl(lance)
    cards_html: list[str] = []
    spec = [
        ("À vista", "--dc-a", doc_vista),
        ("Financiado", "--dc-b", doc_fin),
        ("Parcelado (judicial)", "--dc-c", doc_prazo),
    ]
    for tit, vvar, doc in spec:
        o = _doc_or_none(doc)
        if o is None:
            cards_html.append(
                f"""
<article class="dc-card dc-card-err" style="--dc-accent: var({vvar});">
  <div class="dc-card-head"><span class="dc-badge">{html.escape(tit)}</span>
  <p class="dc-sub">Parâmetros incompletos ou erro de cálculo — ajuste na aba Simulação.</p></div>
</article>"""
            )
            continue
        cards_html.append(_build_card(_CardMetrics(tit, vvar, o)))

    body = "\n".join(cards_html)
    return f"""
{CSS_DASH}
<div class="dc-root" lang="pt-BR">
  <header class="dc-top">
    <div class="dc-top-inner">
      <h2 class="dc-h2">Comparar modalidades</h2>
      <p class="dc-lead">Um <strong>único lance</strong> aplica-se às três: <span class="dc-lance-pill">{esc_lance}</span></p>
      <p class="dc-hint">O investimento <em>total</em> soma o que você desembolsa e o que ainda precisa quitar (caixa + quitação, quando houver dívida).</p>
    </div>
  </header>
  <div class="dc-grid">{body}</div>
  <p class="dc-foot">Valores reativos aos parâmetros da Simulação (T, % entrada, taxa, etc.). <strong>Gravar</strong> no banco persiste a simulação.</p>
</div>
"""


# Paleta: profundo, contrastes suaves, acentos distintos por coluna.
# Tipografia (DM Sans) e tokens :root vêm de ``app_theme.STREAMLIT_PAGE_CSS`` na app;
# aqui mantemos só o bloco com escopo .dc- para o HTML embutido.
CSS_DASH = """
<style>
.dc-root {
  --dc-a: 161 94% 30%;
  --dc-b: 250 60% 58%;
  --dc-c: 32 90% 55%;
  --dc-bg0: 222 47% 6%;
  --dc-bg1: 217 33% 11%;
  --dc-text: 210 40% 98%;
  --dc-muted: 215 20% 70%;
  font-family: "DM Sans", system-ui, sans-serif;
  color: hsl(var(--dc-text));
  background: linear-gradient(165deg, hsl(222 45% 8%) 0%, hsl(230 40% 6%) 45%, hsl(222 50% 5%) 100%);
  border-radius: 20px;
  padding: 0;
  margin: 0.25rem 0 1rem 0;
  overflow: hidden;
  box-shadow: 0 4px 40px -12px rgba(0,0,0,.45), 0 0 0 1px rgba(255,255,255,.04);
}
.dc-top {
  background: radial-gradient(ellipse 120% 100% at 0% 0%, hsl(190 50% 18% / .35) 0%, transparent 55%),
    radial-gradient(ellipse 100% 80% at 100% 0%, hsl(270 40% 22% / .3) 0%, transparent 50%);
  padding: 1.5rem 1.5rem 1.1rem 1.5rem;
  border-bottom: 1px solid rgba(255,255,255,.06);
}
.dc-h2 { margin: 0; font-size: 1.35rem; font-weight: 700; letter-spacing: -0.02em; }
.dc-lead { margin: 0.5rem 0 0 0; font-size: 0.95rem; color: hsl(var(--dc-muted)); }
.dc-lead strong { color: hsl(var(--dc-text)); }
.dc-lance-pill {
  display: inline-block;
  padding: 0.2rem 0.65rem;
  border-radius: 999px;
  background: hsl(160 30% 20% / .5);
  color: #ecfdf5;
  font-weight: 600;
  border: 1px solid rgba(255,255,255,.1);
  margin-left: 0.15rem;
}
.dc-hint { margin: 0.5rem 0 0; font-size: 0.82rem; color: hsl(215 16% 58%); line-height: 1.4; }
.dc-hint em { color: hsl(210 30% 78%); font-style: normal; font-weight: 500; }
.dc-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(16.5rem, 1fr));
  gap: 1rem;
  padding: 1.25rem 1.5rem 1.5rem 1.5rem;
  align-items: stretch;
}
.dc-card {
  --x: 160 88% 34%;
  background: linear-gradient(155deg, hsl(220 30% 12% / .95) 0%, hsl(230 32% 8% / .98) 100%);
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,.08);
  padding: 0;
  position: relative;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  min-height: 100%;
  box-shadow: 0 8px 32px -12px rgba(0,0,0,.4);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.dc-card::before {
  content: "";
  position: absolute;
  top: 0; left: 0; right: 0; height: 3px;
  background: linear-gradient(90deg, hsl(var(--dc-accent, var(--x))) 0%, hsl(190 60% 45%) 100%);
  opacity: 0.9;
}
.dc-card:hover { transform: translateY(-2px); box-shadow: 0 16px 40px -16px rgba(0,0,0,.5); }
.dc-card-err { opacity: 0.85; min-height: 8rem; }
.dc-card-err::before { background: hsl(0 50% 40%); }
.dc-card-head { padding: 1rem 1.1rem 0.3rem; }
.dc-badge {
  display: inline-block;
  font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em;
  color: hsl(var(--dc-muted));
}
.dc-sub { margin: 0.35rem 0 0; font-size: 0.78rem; color: hsl(215 18% 60%); line-height: 1.35; }
.dc-hero {
  margin: 0.75rem 1.1rem 0;
  padding: 1rem 1rem 0.85rem;
  border-radius: 12px;
  background: linear-gradient(135deg, hsl(0 0% 100% / .06) 0%, hsl(0 0% 100% / .02) 100%);
  border: 1px solid rgba(255,255,255,.07);
  position: relative;
}
.dc-hero.dc-ok { border-color: rgba(52, 211, 153, .25); }
.dc-hero.dc-neg { border-color: rgba(248, 113, 113, .3); }
.dc-hero-lbl { display: block; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .07em; color: hsl(var(--dc-muted)); }
.dc-hero-val { display: block; font-size: 1.55rem; font-weight: 700; letter-spacing: -0.03em; margin-top: 0.2rem; }
.dc-ok .dc-hero-val { color: #6ee7b7; }
.dc-neg .dc-hero-val { color: #fca5a5; }
.dc-bar {
  height: 3px; margin-top: 0.65rem; border-radius: 2px;
  background: linear-gradient(90deg, hsl(160 60% 45%) 0%, hsl(180 50% 40%) 100%);
  max-width: 100%;
  opacity: 0.85;
}
.dc-neg .dc-bar { background: linear-gradient(90deg, #f87171 0%, #c2410c 100%); }
.dc-stack { padding: 0.85rem 1.1rem; flex: 1; display: flex; flex-direction: column; gap: 0.4rem; }
.dc-row { display: flex; justify-content: space-between; align-items: baseline; gap: 0.5rem; font-size: 0.82rem; }
.dc-lbl { color: hsl(215 16% 62%); }
.dc-val { color: #f1f5f9; font-weight: 500; text-align: right; }
.dc-muted .dc-lbl, .dc-muted .dc-val { color: hsl(215 12% 55%); }
.dc-mid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.4rem;
  margin: 0.4rem 1.1rem 1rem 1.1rem;
  padding: 0.75rem 0.5rem;
  background: rgba(0,0,0,.2);
  border-radius: 10px;
  border: 1px solid rgba(255,255,255,.05);
}
.dc-mid > div { text-align: center; }
.dc-mid-lbl { display: block; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.06em; color: hsl(215 16% 55%); }
.dc-mid-v { display: block; font-size: 0.9rem; font-weight: 600; color: #e2e8f0; margin-top: 0.2rem; }
.dc-mid .dc-tag { color: #a5b4fc; font-size: 0.85rem; }
.dc-foot {
  font-size: 0.75rem; color: hsl(215 14% 55%);
  margin: 0; padding: 0 1.5rem 1.2rem 1.5rem;
  line-height: 1.45;
}
</style>
"""

# Regras adicionais do painel Simulação (`.dc-root.sp-sim-financeiro`), injetadas após `CSS_DASH`.
PAINEL_SIMULACAO_SCOPED_ONLY = """
.dc-root.sp-sim-financeiro .sp-sim-hero-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1rem;
  padding: 0 1.25rem 1rem 1.25rem;
}
.dc-root.sp-sim-financeiro .sp-sim-hero-grid .dc-card { min-height: 0; }
.dc-root.sp-sim-financeiro .sp-sim-hero-tit { font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: hsl(var(--dc-muted)); display: block; margin-bottom: 0.35rem; }
.dc-root.sp-sim-financeiro .sp-sim-hero-amt { font-size: 1.65rem; font-weight: 800; letter-spacing: -0.03em; line-height: 1.2; color: #f1f5f9; }
.dc-root.sp-sim-financeiro .sp-sim-hero-sub { font-size: 0.8rem; color: hsl(215 18% 62%); margin-top: 0.45rem; line-height: 1.35; }
.dc-root.sp-sim-financeiro .sp-sim-hero-g {
  border-radius: 14px;
  padding: 1.1rem 1.15rem 1rem;
  background: linear-gradient(135deg, hsl(0 0% 100% / .07) 0%, hsl(0 0% 100% / .02) 100%);
  border: 1px solid rgba(255,255,255,.08);
}
.dc-root.sp-sim-financeiro .sp-sim-main .dc-stack { max-height: none; }
.dc-root.sp-sim-financeiro .sp-sim-mid-h { margin: 0 0 0.5rem; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: hsl(215 20% 58%); font-weight: 600; padding: 0 0.1rem; }
.dc-root.sp-sim-financeiro .sp-sim-lmx .dc-hero { margin: 0.5rem 1.1rem 1rem; }
.dc-root.sp-sim-financeiro .sp-sim-detail-list {
  display: flex; flex-direction: column; gap: 0.65rem; padding: 0.15rem 0 0.1rem 0;
}
.dc-root.sp-sim-financeiro .sp-sim-grupo {
  border-radius: 12px; overflow: hidden;
  border: 1px solid rgba(99, 102, 241, 0.14);
  background: linear-gradient(165deg, rgba(15, 23, 42, 0.55) 0%, rgba(12, 18, 32, 0.42) 100%);
  box-shadow: 0 2px 14px rgba(0,0,0,.2);
}
.dc-root.sp-sim-financeiro .sp-sim-sec {
  display: flex; align-items: center; gap: 0.4rem;
  padding: 0.48rem 0.7rem 0.38rem;
  background: linear-gradient(90deg, hsl(200 50% 22% / .38) 0%, hsl(255 30% 18% / .12) 100%);
  border-bottom: 1px solid rgba(99, 102, 241, 0.12);
}
.dc-root.sp-sim-financeiro .sp-sim-sec::before {
  content: "";
  width: 3px; height: 0.85rem; border-radius: 2px; flex-shrink: 0;
  background: linear-gradient(180deg, #6ee7b7, #2dd4bf 55%, #6366f1);
  box-shadow: 0 0 10px rgba(110, 231, 183, 0.25);
}
.dc-root.sp-sim-financeiro .sp-sim-sec-tit {
  font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.11em;
  color: #c7d2fe;
}
.dc-root.sp-sim-financeiro .sp-sim-grupo-lines { padding: 0.1rem 0; }
.dc-root.sp-sim-financeiro .sp-sim-line {
  display: flex; flex-wrap: nowrap; align-items: baseline; gap: 0.35rem;
  font-size: 0.82rem; min-height: 1.5rem; padding: 0.4rem 0.7rem 0.42rem; margin: 0;
  border-bottom: 1px solid rgba(255, 255, 255, 0.045);
  transition: background 0.12s ease;
}
.dc-root.sp-sim-financeiro .sp-sim-grupo-lines .sp-sim-line:last-child { border-bottom: none; padding-bottom: 0.5rem; }
.dc-root.sp-sim-financeiro .sp-sim-grupo-lines .sp-sim-line:nth-child(odd) {
  background: rgba(99, 102, 241, 0.04);
}
.dc-root.sp-sim-financeiro .sp-sim-grupo-lines .sp-sim-line:nth-child(even) {
  background: rgba(16, 185, 129, 0.05);
}
.dc-root.sp-sim-financeiro .sp-sim-line:hover {
  background: rgba(110, 231, 183, 0.07) !important;
}
.dc-root.sp-sim-financeiro .sp-sim-line--muted .sp-sim-line-lbl { color: hsl(215 16% 58%) !important; }
.dc-root.sp-sim-financeiro .sp-sim-line--muted .sp-sim-line-val { color: hsl(215 20% 72%) !important; font-weight: 500; }
.dc-root.sp-sim-financeiro .sp-sim-line-lbl {
  flex: 0 1 auto; max-width: 52%; text-align: left; color: hsl(214 20% 78%);
  line-height: 1.3; min-width: 0;
}
.dc-root.sp-sim-financeiro .sp-sim-line-dots {
  flex: 1 1 0.5rem; min-width: 0.4rem; align-self: center; height: 0;
  border-bottom: 1px dotted rgba(148, 163, 184, 0.38);
  margin: 0 0.15rem; opacity: 0.95;
}
.dc-root.sp-sim-financeiro .sp-sim-line-val {
  flex: 0 0 auto; text-align: right; font-weight: 600; color: #f1f5f9; font-variant-numeric: tabular-nums;
  text-shadow: 0 0 20px rgba(110, 231, 183, 0.12);
}
.dc-root.sp-sim-financeiro .sp-sim-line-sub {
  margin: -0.12rem 0.7rem 0.4rem; padding: 0.15rem 0 0 0.35rem; border-left: 2px solid rgba(100, 116, 139, 0.35);
  font-size: 0.7rem; color: hsl(215 16% 62%); line-height: 1.4;
}
.dc-root.sp-sim-financeiro .sp-sim-line--subtot {
  margin-top: 0.18rem; padding-top: 0.52rem; border-top: 1px solid rgba(99, 102, 241, 0.28);
  background: rgba(99, 102, 241, 0.07) !important;
}
.dc-root.sp-sim-financeiro .sp-sim-line--subtot .sp-sim-line-lbl {
  font-weight: 650; color: #e0e7ff;
}
.dc-root.sp-sim-financeiro .sp-sim-line-val--sum {
  color: #a5f3d0 !important; font-size: 0.92rem;
}
.dc-root.sp-sim-financeiro .sp-sim-line-sub--sum {
  border-left-color: rgba(52, 211, 153, 0.45); color: hsl(152 35% 72%); font-style: italic;
}
@media (max-width: 640px) {
  .dc-root.sp-sim-financeiro .sp-sim-hero-grid { grid-template-columns: 1fr; }
  .dc-root.sp-sim-financeiro .sp-sim-line-lbl { max-width: 100%; margin-bottom: 0.1rem; }
  .dc-root.sp-sim-financeiro .sp-sim-line { flex-wrap: wrap; }
  .dc-root.sp-sim-financeiro .sp-sim-line-dots { display: none; }
  .dc-root.sp-sim-financeiro .sp-sim-line-val { width: 100%; text-align: right; border-top: 1px dotted rgba(148, 163, 184, 0.25); margin-top: 0.1rem; padding-top: 0.15rem; }
}
"""

PAINEL_SIMULACAO_RESUMO_DASH_STYLES = (
    f"{CSS_DASH}\n<style>\n{PAINEL_SIMULACAO_SCOPED_ONLY}\n</style>\n"
)
