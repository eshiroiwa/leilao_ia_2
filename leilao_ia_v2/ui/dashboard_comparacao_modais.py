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


def _row(label: str, value: str, *, muted: bool = False) -> str:
    cl = "dc-row" + (" dc-muted" if muted else "")
    return (
        f'<div class="{cl}"><span class="dc-lbl">{html.escape(label)}</span>'
        f'<span class="dc-val">{html.escape(value)}</span></div>'
    )


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
