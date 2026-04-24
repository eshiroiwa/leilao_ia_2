"""
Painel inicial (dashboard): agrega leilões do Supabase para decisão rápida.
Sem tabelas novas: usa colunas existentes (datas, JSONs de simulação/relatório).

Prioridade estratégica:
- destacar leilões com maior proximidade de praça e alto potencial.
- "alto potencial" = ROI >= 50% ou lucro líquido >= R$ 500 mil.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.schemas.operacao_simulacao import (
    parse_operacao_simulacao_json,
    parse_simulacoes_modalidades_json,
)

_TZ_SP = ZoneInfo("America/Sao_Paulo")

# Oportunidades no painel inicial: com simulação gravada, só entra com ROI bruto > 40 %; sem simulação entra sempre.
_ROI_BRUTO_MIN_OPORTUNIDADES = 0.4
_ROI_PRIORIDADE = 0.5
_LUCRO_PRIORIDADE = 500_000.0


@dataclass
class _RowOut:
    id: str
    cidade: str
    estado: str
    bairro: str
    tipo_imovel: str
    url: str
    endereco: str
    prox_data: date | None
    lucro_liq: float | None
    roi_bruto: float | None
    tem_simulacao: bool
    tem_mercado_llm: bool
    tem_cache: bool
    praca_label: str
    score_prioridade: float
    url_foto_imovel: str | None = None


@dataclass
class DashboardDados:
    agora: date
    total: int
    prox_7d: int
    priorizados: int
    priorizados_prox_7d: int
    sem_sim: int
    sem_mercado: int
    sem_cache: int
    ticket_medio_lucro_priorizados: float | None
    roi_medio_priorizados: float | None
    priorizados_lista: list[_RowOut] = field(default_factory=list)
    top_lucro: list[_RowOut] = field(default_factory=list)
    proximos: list[_RowOut] = field(default_factory=list)
    pendentes: list[_RowOut] = field(default_factory=list)
    lembretes: list[str] = field(default_factory=list)
    # dia iso -> lista (cidade, id)
    calendario: dict[str, list[tuple[str, str]]] = field(default_factory=dict)

    def _kpi_html(self) -> str:
        roi_med = f"{(self.roi_medio_priorizados or 0.0) * 100:.1f} %" if self.roi_medio_priorizados is not None else "—"
        lucro_med = (
            f"R$ {self.ticket_medio_lucro_priorizados:,.0f}".replace(",", ".")
            if self.ticket_medio_lucro_priorizados is not None
            else "—"
        )
        return f"""
<div class="db-kpis">
  <div class="db-kpi"><span class="db-kpi-v">{self.total}</span><span class="db-kpi-l">leilões carregados</span></div>
  <div class="db-kpi db-kpi-accent"><span class="db-kpi-v">{self.priorizados}</span><span class="db-kpi-l">alto potencial (ROI >= 50% ou lucro >= 500k)</span></div>
  <div class="db-kpi db-kpi-accent"><span class="db-kpi-v">{self.priorizados_prox_7d}</span><span class="db-kpi-l">alto potencial em 7 dias</span></div>
  <div class="db-kpi"><span class="db-kpi-v">{self.prox_7d}</span><span class="db-kpi-l">leilões em 7 dias</span></div>
  <div class="db-kpi"><span class="db-kpi-v">{roi_med}</span><span class="db-kpi-l">ROI médio do radar prioritário</span></div>
  <div class="db-kpi"><span class="db-kpi-v">{lucro_med}</span><span class="db-kpi-l">lucro médio do radar prioritário</span></div>
  <div class="db-kpi db-kpi-warn"><span class="db-kpi-v">{self.sem_sim}</span><span class="db-kpi-l">sem simulação gravada</span></div>
  <div class="db-kpi db-kpi-warn"><span class="db-kpi-v">{self.sem_mercado}</span><span class="db-kpi-l">sem análise de mercado</span></div>
  <div class="db-kpi db-kpi-warn"><span class="db-kpi-v">{self.sem_cache}</span><span class="db-kpi-l">sem cache de bairro</span></div>
</div>"""

    def _lembretes_html(self) -> str:
        if self.lembretes:
            return (
                "<ul class=\"db-rem\">" + "".join(f"<li>{html.escape(t)}</li>" for t in self.lembretes) + "</ul>"
            )
        return "<p class=\"db-muted\">Nada urgente — bom momento para revisar oportunidades abaixo.</p>"

    def to_html_kpis_sozinho(self) -> str:
        """KPIs no mesmo padrão visual do dashboard «Comparar» (classe ``dc-``) — requer ``CSS_DASH`` injetado na app."""
        return f"""{CSS_DASHBOARD_INICIO}
<div class="dc-root" lang="pt-BR" style="margin:0.2rem 0 0.85rem; border-radius:20px; overflow:hidden; box-shadow:0 4px 40px -12px rgba(0,0,0,.45), 0 0 0 1px rgba(255,255,255,.04);">
<header class="dc-top" style="padding:1.05rem 1.3rem 1.05rem 1.3rem;">
  <div class="dc-top-inner">
    <h2 class="dc-h2" style="font-size:1.18rem; margin:0 0 0.55rem 0">Indicadores</h2>
{self._kpi_html()}
  </div>
</header>
</div>"""

    def to_html_lembretes_secao(self) -> str:
        """Lembretes no mesmo padrão de card ``dc-card`` que «Comparar modalidades»."""
        return f"""
<article class="dc-card" style="--dc-accent: 250 60% 58%;">
  <div class="dc-card-head">
    <span class="dc-badge">Lembretes e alertas</span>
  </div>
  <div style="padding:0.25rem 1.1rem 1.05rem;">
{self._lembretes_html()}
  </div>
</article>"""


# Injetar no painel Streamlit: cards de seção (border) e botões estilo “card” clicáveis
CSS_STREAMLIT_PAINEL_INICIO = """
<style>
/* st.container(border) ≈ .dc-card (mesmo fio de «Comparar modalidades») */
section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] {
  background: linear-gradient(155deg, hsl(220 30% 12% / 0.95) 0%, hsl(230 32% 8% / 0.98) 100%) !important;
  border-radius: 16px !important;
  border: 1px solid rgba(255, 255, 255, 0.08) !important;
  position: relative !important;
  overflow: hidden !important;
  box-shadow: 0 8px 32px -12px rgba(0, 0, 0, 0.4) !important;
  transition: transform 0.2s ease, box-shadow 0.2s ease !important;
  margin-bottom: 0.1rem;
}
section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"]::before {
  content: "" !important;
  position: absolute !important;
  top: 0;
  left: 0;
  right: 0;
  height: 3px;
  background: linear-gradient(90deg, hsl(160 55% 48%) 0%, hsl(190 60% 45%) 100%) !important;
  opacity: 0.9;
  z-index: 0;
  pointer-events: none;
}
section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"]:hover {
  transform: translateY(-2px) !important;
  box-shadow: 0 16px 40px -16px rgba(0, 0, 0, 0.5) !important;
}
/* Botão de imóvel (secondary) alinhado ao “hero” interno dos cards dc- */
section[data-testid="stMain"] [data-testid="stBaseButton-secondary"] {
  width: 100%;
  min-height: 3.1rem;
  height: auto !important;
  white-space: pre-line !important;
  line-height: 1.4 !important;
  font-size: 0.86rem !important;
  font-weight: 500 !important;
  text-align: left !important;
  justify-content: flex-start !important;
  align-items: flex-start !important;
  padding: 0.7rem 0.9rem !important;
  border-radius: 12px !important;
  color: #e2e8f0 !important;
  background: linear-gradient(135deg, hsl(0 0% 100% / 0.06) 0%, hsl(0 0% 100% / 0.02) 100%) !important;
  border: 1px solid rgba(255, 255, 255, 0.07) !important;
  margin-bottom: 0.4rem;
  box-shadow: none !important;
}
section[data-testid="stMain"] [data-testid="stBaseButton-secondary"]:hover {
  border-color: rgba(52, 211, 153, 0.3) !important;
  background: linear-gradient(135deg, hsl(0 0% 100% / 0.1) 0%, hsl(0 0% 100% / 0.04) 100%) !important;
}
/* Fallback: algumas versões do Streamlit usam o atributo kind no <button> */
section[data-testid="stMain"] .stButton > button[kind="secondary"] {
  width: 100%;
  min-height: 3.1rem;
  height: auto !important;
  white-space: pre-line !important;
  line-height: 1.4 !important;
  text-align: left !important;
  justify-content: flex-start !important;
  align-items: flex-start !important;
  border-radius: 12px !important;
}
/*
 * Calendário: “bolinhas” (terciário = dia com leilão; primário = dia selecionado no filtro).
 * No painel Início, estes tipos vêm só do widget de calendário (cards de imóvel usam secondary).
 */
section[data-testid="stMain"] [data-testid="stBaseButton-tertiary"] {
  width: 2.4rem !important;
  min-width: 2.4rem !important;
  max-width: 2.4rem !important;
  min-height: 2.4rem !important;
  max-height: 2.4rem !important;
  height: 2.4rem !important;
  border-radius: 50% !important;
  margin: 0.2rem auto 0.45rem auto !important;
  padding: 0 !important;
  font-size: 0.68rem !important;
  font-weight: 650 !important;
  line-height: 1 !important;
  white-space: nowrap !important;
  color: #d1fae5 !important;
  text-align: center !important;
  display: block !important;
  align-items: center !important;
  justify-content: center !important;
  background: radial-gradient(ellipse at 30% 25%, rgba(16, 185, 129, 0.38) 0%, rgba(5, 150, 105, 0.22) 100%) !important;
  border: 1.5px solid rgba(45, 212, 191, 0.42) !important;
  box-shadow: 0 1px 0 rgba(255, 255, 255, 0.1) inset, 0 2px 10px rgba(0, 0, 0, 0.2) !important;
  transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s;
}
section[data-testid="stMain"] [data-testid="stBaseButton-tertiary"] p,
section[data-testid="stMain"] [data-testid="stBaseButton-primary"] p {
  margin: 0 !important;
  line-height: 1.1 !important;
  text-align: center !important;
}
section[data-testid="stMain"] [data-testid="stBaseButton-tertiary"]:hover {
  background: radial-gradient(ellipse at 30% 25%, rgba(16, 185, 129, 0.52) 0%, rgba(5, 150, 105, 0.35) 100%) !important;
  border-color: rgba(45, 212, 191, 0.65) !important;
  transform: scale(1.08);
  box-shadow: 0 0 0 1px rgba(52, 211, 153, 0.25);
}
/* Dia com filtro ativo: tom mais claro = “lido” com leilão */
section[data-testid="stMain"] [data-testid="stBaseButton-primary"] {
  width: 2.5rem !important;
  min-width: 2.5rem !important;
  max-width: 2.5rem !important;
  min-height: 2.5rem !important;
  max-height: 2.5rem !important;
  height: 2.5rem !important;
  border-radius: 50% !important;
  margin: 0.2rem auto 0.45rem auto !important;
  padding: 0 !important;
  font-size: 0.7rem !important;
  font-weight: 700 !important;
  line-height: 1 !important;
  color: #042f2e !important;
  text-align: center !important;
  display: block !important;
  white-space: nowrap !important;
  background: linear-gradient(150deg, #5eead4 0%, #14b8a6 45%, #0d9488 100%) !important;
  border: 1.5px solid rgba(255, 255, 255, 0.35) !important;
  box-shadow: 0 0 0 2px rgba(20, 184, 166, 0.4), 0 3px 14px rgba(0, 0, 0, 0.28) !important;
}
section[data-testid="stMain"] [data-testid="stBaseButton-primary"]:hover {
  filter: brightness(1.05);
  transform: scale(1.05);
}
/* Oportunidades: miniatura do imóvel (url_foto_imovel) ao lado do botão */
section[data-testid="stMain"] .dash-op-thumb-ph {
  width: 72px;
  height: 72px;
  min-width: 72px;
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.05);
  border: 1px dashed rgba(255, 255, 255, 0.12);
  box-sizing: border-box;
}
section[data-testid="stMain"] .dash-op-thumb-wrap { line-height: 0; margin: 0.1rem 0 0.35rem 0; }
section[data-testid="stMain"] .dash-op-card-body {
  border-radius: 12px;
  padding: 0.1rem 0.2rem 0.35rem;
}
section[data-testid="stMain"] .dash-op-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.45rem;
  margin-bottom: 0.4rem;
}
section[data-testid="stMain"] .dash-op-pill {
  border-radius: 999px;
  padding: 0.2rem 0.55rem;
  font-size: 0.64rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  border: 1px solid rgba(255,255,255,0.15);
}
section[data-testid="stMain"] .dash-op-pill.ok {
  color: #0d2f24;
  background: linear-gradient(130deg, #6ee7b7 0%, #34d399 100%);
}
section[data-testid="stMain"] .dash-op-pill.warn {
  color: #422006;
  background: linear-gradient(130deg, #fde68a 0%, #fbbf24 100%);
}
section[data-testid="stMain"] .dash-op-pill.muted {
  color: #cbd5e1;
  background: rgba(148, 163, 184, 0.16);
}
section[data-testid="stMain"] .dash-op-date {
  font-size: 0.71rem;
  color: #93c5fd;
  font-weight: 600;
}
section[data-testid="stMain"] .dash-op-title {
  color: #f1f5f9;
  font-size: 0.87rem;
  font-weight: 650;
  margin-bottom: 0.22rem;
}
section[data-testid="stMain"] .dash-op-end {
  color: #94a3b8;
  font-size: 0.74rem;
  line-height: 1.3;
  margin-bottom: 0.38rem;
}
</style>
"""


def _parse_date_any(v: Any) -> date | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return date.fromisoformat(m.group(1))
        except ValueError:
            return None
    return None


def _proxima_data_e_praca(row: dict[str, Any], *, hoje: date) -> tuple[date | None, str]:
    d1, d2 = _parse_date_any(row.get("data_leilao_1_praca")), _parse_date_any(row.get("data_leilao_2_praca"))
    d0 = _parse_date_any(row.get("data_leilao"))
    candidatos: list[tuple[date, str]] = []
    if d1:
        candidatos.append((d1, "1ª praça"))
    if d2:
        candidatos.append((d2, "2ª praça"))
    if d0 and d0 not in (d1, d2):
        candidatos.append((d0, "Data edital" if d1 or d2 else "Venda / data"))
    if not candidatos:
        return None, ""
    fut = [(d, p) for d, p in candidatos if d >= hoje]
    if fut:
        d, p = min(fut, key=lambda x: x[0])
        return d, p
    d, p = max(candidatos, key=lambda x: x[0])
    return d, f"{p} (passada)"


def _lucro_liquido_de_row(row: dict[str, Any]) -> float | None:
    try:
        doc = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
        if doc.outputs and doc.outputs.lucro_liquido is not None:
            return float(doc.outputs.lucro_liquido)
    except Exception:
        pass
    try:
        b = parse_simulacoes_modalidades_json(
            row.get("simulacoes_modalidades_json"), legado_operacao=row.get("operacao_simulacao_json")
        )
        v = b.vista
        if v.outputs and v.outputs.lucro_liquido is not None:
            return float(v.outputs.lucro_liquido)
    except Exception:
        pass
    if not _tem_simulacao(row):
        try:
            ll = row.get("lucro_liquido_projetado")
            if ll is not None and float(ll) == float(ll):
                return float(ll)
        except (TypeError, ValueError):
            pass
    return None


def _tem_simulacao(row: dict[str, Any]) -> bool:
    """Há simulação **gravada** (outputs reais), não só JSON com defaults/inputs vazios."""
    return leilao_imoveis_repo.leilao_tem_simulacao_utilizador_gravada(row)


def _tem_mercado_llm(row: dict[str, Any]) -> bool:
    r = row.get("relatorio_mercado_contexto_json")
    if r is None:
        return False
    if isinstance(r, dict) and r:
        return True
    if isinstance(r, str) and r.strip() and r.strip() not in ("null", "{}", "[]"):
        return True
    return False


def _roi_bruto_de_row(row: dict[str, Any]) -> float | None:
    try:
        doc = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
        if doc.outputs and doc.outputs.roi_bruto is not None:
            return float(doc.outputs.roi_bruto)
    except Exception:
        pass
    try:
        b = parse_simulacoes_modalidades_json(
            row.get("simulacoes_modalidades_json"), legado_operacao=row.get("operacao_simulacao_json")
        )
        v = b.vista
        if v.outputs and v.outputs.roi_bruto is not None:
            return float(v.outputs.roi_bruto)
    except Exception:
        pass
    if not _tem_simulacao(row):
        try:
            rv = row.get("roi_projetado")
            if rv is not None and float(rv) == float(rv):
                return float(rv)
        except (TypeError, ValueError):
            pass
    return None


def _url_foto_imovel_row(r: dict[str, Any]) -> str | None:
    u = str(r.get("url_foto_imovel") or "").strip()
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return None


def _elegivel_oportunidades_roi(o: _RowOut) -> bool:
    """Sem simulação: aparece nas listagens de oportunidade. Com simulação: só se ROI bruto > limiar."""
    if not o.tem_simulacao:
        return True
    rb = o.roi_bruto
    return rb is not None and rb > _ROI_BRUTO_MIN_OPORTUNIDADES


def _e_prioritario(o: _RowOut) -> bool:
    rb = o.roi_bruto
    ll = o.lucro_liq
    return (rb is not None and rb >= _ROI_PRIORIDADE) or (ll is not None and ll >= _LUCRO_PRIORIDADE)


def _score_prioridade(o: _RowOut, *, hoje: date) -> float:
    score = 0.0
    rb = o.roi_bruto
    ll = o.lucro_liq
    if rb is not None:
        score += min(max(float(rb), 0.0), 1.5) * 120.0
    if ll is not None:
        score += min(max(float(ll), 0.0) / 100_000.0, 25.0) * 7.0
    if o.prox_data:
        dias = (o.prox_data - hoje).days
        if dias >= 0:
            score += max(0.0, (60.0 - float(dias)) * 2.4)
        else:
            score -= min(25.0, abs(float(dias)) * 0.6)
    if _e_prioritario(o):
        score += 120.0
    if not o.tem_simulacao:
        score -= 10.0
    if not o.tem_mercado_llm:
        score -= 8.0
    return round(score, 2)


def _row_to_out(r: dict[str, Any], *, hoje: date) -> _RowOut:
    iid = str(r.get("id") or "")
    d, pl = _proxima_data_e_praca(r, hoje=hoje)
    cids = r.get("cache_media_bairro_ids") or []
    ncache = len(cids) if isinstance(cids, (list, tuple)) else 0
    return _RowOut(
        id=iid,
        cidade=str(r.get("cidade") or "—")[:32],
        estado=str(r.get("estado") or "")[:3],
        bairro=str(r.get("bairro") or "")[:28],
        tipo_imovel=str(r.get("tipo_imovel") or "")[:40],
        url=str(r.get("url_leilao") or "")[:120],
        endereco=str(r.get("endereco") or "")[:60],
        prox_data=d,
        lucro_liq=_lucro_liquido_de_row(r),
        roi_bruto=_roi_bruto_de_row(r),
        tem_simulacao=_tem_simulacao(r),
        tem_mercado_llm=_tem_mercado_llm(r),
        tem_cache=ncache > 0,
        praca_label=pl,
        score_prioridade=0.0,
        url_foto_imovel=_url_foto_imovel_row(r),
    )


def processar_rows_dashboard(rows: list[dict[str, Any]]) -> DashboardDados:
    hoje = datetime.now(_TZ_SP).date()
    outs = [_row_to_out(r, hoje=hoje) for r in rows if r.get("id")]
    for i, o in enumerate(outs):
        outs[i].score_prioridade = _score_prioridade(o, hoje=hoje)
    op = [x for x in outs if _elegivel_oportunidades_roi(x)]
    sem_sim = [x for x in outs if not x.tem_simulacao]
    sem_merc = [x for x in outs if not x.tem_mercado_llm]
    com_data = [x for x in op if x.prox_data is not None]

    fim = hoje + timedelta(days=7)
    prox_7 = [x for x in com_data if x.prox_data is not None and hoje <= x.prox_data <= fim]

    priorizados = [x for x in op if _e_prioritario(x)]
    priorizados_ordenados = sorted(
        [x for x in priorizados if x.prox_data and x.prox_data >= hoje],
        key=lambda z: (z.prox_data or hoje, -float(z.score_prioridade), -float(z.roi_bruto or 0.0)),
    )[:10]
    priorizados_sem_data = sorted(
        [x for x in priorizados if not x.prox_data],
        key=lambda z: (-float(z.score_prioridade), -float(z.roi_bruto or 0.0)),
    )[:4]
    priorizados_lista = (priorizados_ordenados + priorizados_sem_data)[:10]

    top_l = sorted(
        [x for x in op if x.lucro_liq is not None],
        key=lambda z: (z.lucro_liq or 0.0, z.score_prioridade),
        reverse=True,
    )[:6]
    proximos = sorted(
        [x for x in com_data if x.prox_data and x.prox_data >= hoje and "(passada)" not in (x.praca_label or "")],
        key=lambda z: (z.prox_data or hoje, -z.score_prioridade, z.cidade),
    )[:8]
    pendentes = sorted(
        [x for x in outs if (not x.tem_simulacao or not x.tem_mercado_llm) and x.prox_data and x.prox_data >= hoje],
        key=lambda z: (z.prox_data or hoje, -z.score_prioridade),
    )[:8]
    if not pendentes:
        pendentes = [x for x in sem_sim if x.prox_data and x.prox_data >= hoje][:8]
    if not pendentes:
        pendentes = sem_sim[:8]

    lembretes: list[str] = []
    if prox_7:
        lembretes.append(f"**{len(prox_7)}** leilão(ões) com data nos próximos 7 dias — revisar lance e simulação.")
    if priorizados:
        lembretes.append(
            f"**{len(priorizados)}** oportunidade(s) com **ROI >= 50%** ou **lucro >= R$ 500 mil**."
        )
    if sem_sim and len(sem_sim) >= 1:
        lembretes.append(f"**{len(sem_sim)}** registo(s) ainda **sem simulação gravada** (risco de decisão no escuro).")
    if sem_merc:
        lembretes.append(f"**{len(sem_merc)}** ainda **sem análise de mercado (LLM)** — considere gerar na aba Simulação.")
    cache_sem = [x for x in outs if not x.tem_cache]
    if len(cache_sem) > len(outs) * 0.3 and outs:
        lembretes.append("Muitos leilões **sem cache de bairro** vinculado — mercado e preço m² podem estar frágeis.")

    cal: dict[str, list[tuple[str, str]]] = {}
    for x in com_data:
        if not x.prox_data:
            continue
        if x.prox_data.year != hoje.year or x.prox_data.month != hoje.month:
            # só destaca mês visível; meses fora ainda entram no próximos
            if x.prox_data >= hoje and (x.prox_data - hoje).days <= 45:
                pass
        key = x.prox_data.isoformat()
        cal.setdefault(key, []).append((x.cidade, x.id))

    return DashboardDados(
        agora=hoje,
        total=len(outs),
        prox_7d=len(prox_7),
        priorizados=len(priorizados),
        priorizados_prox_7d=len([x for x in priorizados if x.prox_data and hoje <= x.prox_data <= fim]),
        sem_sim=len(sem_sim),
        sem_mercado=len(sem_merc),
        sem_cache=len(cache_sem),
        ticket_medio_lucro_priorizados=(
            sum(float(x.lucro_liq or 0.0) for x in priorizados if x.lucro_liq is not None)
            / max(1, len([x for x in priorizados if x.lucro_liq is not None]))
            if any(x.lucro_liq is not None for x in priorizados)
            else None
        ),
        roi_medio_priorizados=(
            sum(float(x.roi_bruto or 0.0) for x in priorizados if x.roi_bruto is not None)
            / max(1, len([x for x in priorizados if x.roi_bruto is not None]))
            if any(x.roi_bruto is not None for x in priorizados)
            else None
        ),
        priorizados_lista=priorizados_lista,
        top_lucro=top_l,
        proximos=proximos,
        pendentes=pendentes,
        lembretes=lembretes,
        calendario=cal,
    )


def agregar_listas_por_dia(
    rows: list[dict[str, Any]],
    dia: date,
) -> tuple[list[_RowOut], list[_RowOut], list[_RowOut]]:
    """
    Recalcula as três colunas (próximos, top lucro, pendências) somente com leilões
    cuja **próxima data de praça** (mesma regra de ``_row_to_out``) é ``dia``.
    """
    hoje = datetime.now(_TZ_SP).date()
    outs_dia: list[_RowOut] = []
    for r in rows:
        if not r.get("id"):
            continue
        o = _row_to_out(r, hoje=hoje)
        o.score_prioridade = _score_prioridade(o, hoje=hoje)
        if o.prox_data == dia:
            outs_dia.append(o)
    if not outs_dia:
        return [], [], []
    op_dia = [x for x in outs_dia if _elegivel_oportunidades_roi(x)]
    com_data = [x for x in op_dia if x.prox_data is not None]
    top_l = sorted(
        [x for x in op_dia if x.lucro_liq is not None],
        key=lambda z: (z.lucro_liq or 0.0, z.score_prioridade),
        reverse=True,
    )[:8]
    proximos = sorted(
        com_data, key=lambda z: (-z.score_prioridade, z.cidade, z.bairro),
    )[:8]
    pendentes = sorted(
        [x for x in outs_dia if (not x.tem_simulacao or not x.tem_mercado_llm)],
        key=lambda z: (-z.score_prioridade, z.cidade),
    )[:8]
    if not pendentes:
        pendentes = [x for x in outs_dia if not x.tem_simulacao][:8]
    if not pendentes:
        pendentes = outs_dia[:8]
    return proximos, top_l, pendentes


def _normalizar_filtros_dashboard(filtro: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(filtro, str):
        base = [f.strip().lower() for f in filtro.split(",") if f.strip()]
    else:
        base = [str(f).strip().lower() for f in filtro if str(f).strip()]
    validos = {"priorizados", "prox7", "sem_sim", "sem_mercado", "sem_cache"}
    out: list[str] = []
    for f in base:
        if f in validos and f not in out:
            out.append(f)
    return out


def filtrar_rows_dashboard(
    rows: list[dict[str, Any]],
    filtro: str | list[str] | tuple[str, ...],
) -> list[dict[str, Any]]:
    """
    Filtro rápido para o painel principal.
    Valores válidos de filtro: priorizados | prox7 | sem_sim | sem_mercado | sem_cache.
    Aceita string única, CSV (``"priorizados,prox7"``) ou lista/tupla.
    Quando há múltiplos filtros, aplica regra AND.
    """
    filtros = _normalizar_filtros_dashboard(filtro)
    if not filtros:
        return rows
    hoje = datetime.now(_TZ_SP).date()
    fim = hoje + timedelta(days=7)
    out: list[dict[str, Any]] = []
    for r in rows:
        if not r.get("id"):
            continue
        o = _row_to_out(r, hoje=hoje)
        ok = True
        for f in filtros:
            if f == "priorizados" and not _e_prioritario(o):
                ok = False
                break
            if f == "prox7" and not (o.prox_data and hoje <= o.prox_data <= fim):
                ok = False
                break
            if f == "sem_sim" and o.tem_simulacao:
                ok = False
                break
            if f == "sem_mercado" and o.tem_mercado_llm:
                ok = False
                break
            if f == "sem_cache" and o.tem_cache:
                ok = False
                break
        if ok:
            out.append(r)
    return out


def _fmt_brl_rel(v: float | None) -> str:
    if v is None:
        return "—"
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct_rel(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{float(v) * 100:.1f}%"


def gerar_relatorio_html_prioridade_maxima(o: _RowOut) -> str:
    data_txt = o.prox_data.strftime("%d/%m/%Y") if o.prox_data else "Sem data"
    status = "ROI >= 50%" if (o.roi_bruto is not None and o.roi_bruto >= _ROI_PRIORIDADE) else (
        "Lucro >= 500k" if (o.lucro_liq is not None and o.lucro_liq >= _LUCRO_PRIORIDADE) else "Em análise"
    )
    local = f"{o.estado} · {o.cidade} · {o.bairro} · {o.tipo_imovel}"
    link = html.escape(o.url or "", quote=True)
    link_html = (
        f'<a href="{link}" target="_blank" rel="noopener noreferrer">{html.escape(o.url)}</a>'
        if link
        else "URL não informada"
    )
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Relatório - Prioridade Máxima</title>
  <style>
    body {{ font-family: Inter, Segoe UI, Arial, sans-serif; background:#0b1220; color:#e2e8f0; margin:0; padding:24px; }}
    .wrap {{ max-width: 880px; margin:0 auto; }}
    .card {{ background: linear-gradient(165deg,#0f172a 0%, #111827 100%); border:1px solid #243244; border-radius:14px; padding:16px; }}
    .h {{ font-size:1.2rem; font-weight:700; margin:0 0 12px 0; }}
    .meta {{ color:#93c5fd; font-size:0.9rem; margin-bottom:8px; }}
    .status {{ display:inline-block; border-radius:999px; padding:4px 10px; font-weight:700; background:#34d399; color:#052e2b; margin-bottom:10px; }}
    .grid {{ display:grid; grid-template-columns:repeat(3,minmax(140px,1fr)); gap:10px; margin-top:10px; }}
    .k {{ border:1px solid #1f2b3d; border-radius:10px; padding:10px; background:#0b1322; }}
    .kl {{ font-size:0.75rem; color:#94a3b8; text-transform:uppercase; letter-spacing:.04em; }}
    .kv {{ font-size:1rem; font-weight:700; margin-top:6px; }}
    a {{ color:#5eead4; word-break: break-all; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1 class="h">Relatório de Prioridade Máxima</h1>
      <div class="status">{html.escape(status)}</div>
      <div class="meta">{html.escape(o.praca_label or "Praça")} · {html.escape(data_txt)}</div>
      <div><strong>Imóvel:</strong> {html.escape(local)}</div>
      <div><strong>Endereço:</strong> {html.escape(o.endereco or "Endereço não informado")}</div>
      <div class="grid">
        <div class="k"><div class="kl">ROI bruto</div><div class="kv">{_fmt_pct_rel(o.roi_bruto)}</div></div>
        <div class="k"><div class="kl">Lucro líquido</div><div class="kv">{_fmt_brl_rel(o.lucro_liq)}</div></div>
        <div class="k"><div class="kl">Score prioridade</div><div class="kv">{o.score_prioridade:.2f}</div></div>
      </div>
      <div style="margin-top:14px"><strong>Link do leilão:</strong><br/>{link_html}</div>
    </div>
  </div>
</body>
</html>
"""


def gerar_relatorio_html_prioridade_maxima_lote(rows: list[_RowOut]) -> str:
    itens: list[str] = []
    for o in rows:
        data_txt = o.prox_data.strftime("%d/%m/%Y") if o.prox_data else "Sem data"
        status = "ROI >= 50%" if (o.roi_bruto is not None and o.roi_bruto >= _ROI_PRIORIDADE) else (
            "Lucro >= 500k" if (o.lucro_liq is not None and o.lucro_liq >= _LUCRO_PRIORIDADE) else "Em análise"
        )
        local = f"{o.estado} · {o.cidade} · {o.bairro} · {o.tipo_imovel}"
        link = html.escape(o.url or "", quote=True)
        link_html = (
            f'<a href="{link}" target="_blank" rel="noopener noreferrer">{html.escape(o.url)}</a>'
            if link
            else "URL não informada"
        )
        foto = html.escape(o.url_foto_imovel or "", quote=True)
        foto_html = (
            f'<img src="{foto}" alt="Foto do imóvel" loading="lazy" referrerpolicy="no-referrer" class="foto" />'
            if foto
            else '<div class="foto-ph">Sem foto</div>'
        )
        itens.append(
            '<article class="item">'
            f'<div class="meta">{html.escape(o.praca_label or "Praça")} · {html.escape(data_txt)} · Score {o.score_prioridade:.2f}</div>'
            f'<div class="status">{html.escape(status)}</div>'
            f'<div class="foto-wrap">{foto_html}</div>'
            f'<div><strong>Imóvel:</strong> {html.escape(local)}</div>'
            f'<div><strong>Endereço:</strong> {html.escape(o.endereco or "Endereço não informado")}</div>'
            '<div class="grid">'
            f'<div class="k"><div class="kl">ROI bruto</div><div class="kv">{_fmt_pct_rel(o.roi_bruto)}</div></div>'
            f'<div class="k"><div class="kl">Lucro líquido</div><div class="kv">{_fmt_brl_rel(o.lucro_liq)}</div></div>'
            f'<div class="k"><div class="kl">Status</div><div class="kv">{html.escape(status)}</div></div>'
            "</div>"
            f'<div style="margin-top:10px"><strong>Link do leilão:</strong><br/>{link_html}</div>'
            "</article>"
        )
    corpo = "".join(itens) if itens else '<p class="vazio">Nenhum imóvel prioritário no filtro atual.</p>'
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Relatório Consolidado - Prioridade Máxima</title>
  <style>
    body {{ font-family: Inter, Segoe UI, Arial, sans-serif; background:#0b1220; color:#e2e8f0; margin:0; padding:24px; }}
    .wrap {{ max-width: 980px; margin:0 auto; }}
    .top {{ margin-bottom:14px; }}
    .h {{ font-size:1.25rem; font-weight:700; margin:0; }}
    .sub {{ color:#94a3b8; margin-top:6px; }}
    .item {{ background: linear-gradient(165deg,#0f172a 0%, #111827 100%); border:1px solid #243244; border-radius:14px; padding:14px; margin:0 0 12px 0; }}
    .meta {{ color:#93c5fd; font-size:0.88rem; margin-bottom:8px; }}
    .status {{ display:inline-block; border-radius:999px; padding:4px 10px; font-weight:700; background:#34d399; color:#052e2b; margin-bottom:10px; }}
    .foto-wrap {{ margin: 0 0 10px 0; }}
    .foto {{ width: 100%; max-width: 360px; height: 200px; object-fit: cover; border-radius: 10px; border:1px solid #243244; display:block; }}
    .foto-ph {{ width: 100%; max-width: 360px; height: 200px; border-radius:10px; border:1px dashed #334155; display:flex; align-items:center; justify-content:center; color:#94a3b8; background:#0b1322; }}
    .grid {{ display:grid; grid-template-columns:repeat(3,minmax(140px,1fr)); gap:10px; margin-top:10px; }}
    .k {{ border:1px solid #1f2b3d; border-radius:10px; padding:10px; background:#0b1322; }}
    .kl {{ font-size:0.75rem; color:#94a3b8; text-transform:uppercase; letter-spacing:.04em; }}
    .kv {{ font-size:1rem; font-weight:700; margin-top:6px; }}
    .vazio {{ color:#94a3b8; }}
    a {{ color:#5eead4; word-break: break-all; }}
  </style>
</head>
<body>
  <div class="wrap">
    <header class="top">
      <h1 class="h">Relatório Consolidado - Prioridade Máxima</h1>
      <div class="sub">Total de imóveis no relatório: {len(rows)}</div>
    </header>
    {corpo}
  </div>
</body>
</html>
"""


CSS_DASHBOARD_INICIO = """
<style>
.db-root { font-family: "DM Sans", system-ui, sans-serif; color: #e2e8f0; margin: 0.25rem 0 1rem 0; }
.db-inline { margin-bottom: 0.35rem; }
.db-kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(9.5rem, 1fr)); gap: 0.65rem; margin-bottom: 1rem; }
.db-kpi { background: linear-gradient(155deg, hsl(220 30% 14% / .95) 0%, hsl(230 32% 9% / .98) 100%);
  border: 1px solid rgba(255,255,255,.08); border-radius: 14px; padding: 0.75rem 0.9rem; text-align: center; }
.db-kpi-accent { border-color: rgba(52, 211, 153, 0.35); }
.db-kpi-warn { border-color: rgba(251, 191, 36, 0.4); }
.db-kpi-v { display: block; font-size: 1.45rem; font-weight: 700; color: #6ee7b7; line-height: 1.1; }
.db-kpi-warn .db-kpi-v { color: #fde68a; }
.db-kpi-l { display: block; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.06em; color: #94a3b8; margin-top: 0.35rem; }
.db-grid-2 { display: grid; grid-template-columns: 1.15fr 0.85fr; gap: 0.9rem; align-items: start; }
@media (max-width: 960px) { .db-grid-2 { grid-template-columns: 1fr; } }
.db-panel { background: linear-gradient(165deg, hsl(222 45% 8% / .55) 0%, hsl(230 40% 6% / .75) 100%);
  border: 1px solid rgba(255,255,255,.06); border-radius: 16px; padding: 0.9rem 1rem 1rem; }
.db-h3 { margin: 0 0 0.45rem; font-size: 1.02rem; font-weight: 650; color: #f1f5f9; }
.db-muted { color: #94a3b8; font-size: 0.84rem; line-height: 1.4; }
.db-muted.sm { font-size: 0.78rem; margin-bottom: 0.5rem; }
.db-rem { margin: 0.35rem 0 0 1.1rem; padding: 0; color: #cbd5e1; font-size: 0.86rem; line-height: 1.5; }
/* Lembretes dentro de card dc- (paleta alinhada a Comparar) */
.dc-card .db-rem { color: hsl(215 16% 72%); }
.dc-card .db-muted { color: hsl(215 18% 58%); }
</style>
"""
