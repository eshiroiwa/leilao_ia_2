"""
Painel inicial (dashboard): agrega leilões do Supabase para decisão rápida.
Sem tabelas novas: usa colunas existentes (datas, JSONs de simulação/relatório).

Prioridade estratégica:
- destacar leilões com maior proximidade de praça e alto potencial.
- "alto potencial" = ROI >= 50% ou lucro líquido >= R$ 500 mil.
"""

from __future__ import annotations

import html
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.schemas.operacao_simulacao import (
    parse_operacao_simulacao_json,
    parse_simulacoes_modalidades_json,
)
from leilao_ia_v2.schemas.relatorio_mercado_contexto import parse_relatorio_mercado_contexto_json
from leilao_ia_v2.services.relatorio_mercado_inteligencia import extrair_sinais_objetivos_por_cards

_TZ_SP = ZoneInfo("America/Sao_Paulo")

# Oportunidades no painel inicial: com simulação gravada, só entra com ROI bruto > 40 %; sem simulação entra sempre.
_ROI_BRUTO_MIN_OPORTUNIDADES = 0.4
_ROI_PRIORIDADE = float(os.getenv("DASHBOARD_ROI_PRIORIDADE", "0.5") or "0.5")
_LUCRO_PRIORIDADE = float(os.getenv("DASHBOARD_LUCRO_PRIORIDADE", "500000") or "500000")


def _float_env(nome: str, default: float, *, min_v: float | None = None, max_v: float | None = None) -> float:
    raw = str(os.getenv(nome, str(default)) or str(default)).strip()
    try:
        v = float(raw)
    except Exception:
        v = float(default)
    if min_v is not None:
        v = max(float(min_v), v)
    if max_v is not None:
        v = min(float(max_v), v)
    return float(v)


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
    roi_liquido: float | None
    roi_origem: str
    tem_simulacao: bool
    tem_mercado_llm: bool
    tem_cache: bool
    confianca_operacional: int
    capital_imobilizado: float | None
    retorno_por_capital: float | None
    lucro_conservador: float | None
    lucro_agressivo: float | None
    roi_conservador: float | None
    roi_agressivo: float | None
    tempo_venda_conservador_meses: float | None
    haircut_venda_conservador_pct: float | None
    liquidez_bairro_score: int
    pressao_concorrencia_score: int
    fit_imovel_bairro_score: int
    qualidade_relatorio_score: int
    relatorio_expirado: bool
    hibrido_ativo: bool
    capital_comprometido_pct: float | None
    caixa_util_brl: float | None
    semaforo_decisao: str
    semaforo_justificativa: str
    praca_label: str
    score_prioridade: float
    score_explicacao: str = ""
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
    eficiencia_capital: list[_RowOut] = field(default_factory=list)
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


def _roi_liquido_de_row(row: dict[str, Any]) -> float | None:
    try:
        doc = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
        if doc.outputs and doc.outputs.roi_liquido is not None:
            return float(doc.outputs.roi_liquido)
    except Exception:
        pass
    try:
        b = parse_simulacoes_modalidades_json(
            row.get("simulacoes_modalidades_json"), legado_operacao=row.get("operacao_simulacao_json")
        )
        v = b.vista
        if v.outputs and v.outputs.roi_liquido is not None:
            return float(v.outputs.roi_liquido)
    except Exception:
        pass
    if not _tem_simulacao(row):
        try:
            rv = row.get("roi_liquido_projetado")
            if rv is not None and float(rv) == float(rv):
                return float(rv)
        except (TypeError, ValueError):
            pass
    return None


def _capital_imobilizado_de_row(row: dict[str, Any]) -> float | None:
    try:
        doc = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
        if doc.outputs and doc.outputs.investimento_cash_ate_momento_venda is not None:
            v = float(doc.outputs.investimento_cash_ate_momento_venda)
            if v > 0:
                return v
    except Exception:
        pass
    try:
        b = parse_simulacoes_modalidades_json(
            row.get("simulacoes_modalidades_json"), legado_operacao=row.get("operacao_simulacao_json")
        )
        v = b.vista
        if v.outputs and v.outputs.investimento_cash_ate_momento_venda is not None:
            x = float(v.outputs.investimento_cash_ate_momento_venda)
            if x > 0:
                return x
    except Exception:
        pass
    if not _tem_simulacao(row):
        try:
            ll = row.get("lucro_liquido_projetado")
            rb = row.get("roi_projetado")
            llf = float(ll) if ll is not None else 0.0
            rbf = float(rb) if rb is not None else 0.0
            if llf > 0 and rbf > 0:
                return llf / rbf
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return None


def _tempo_estimado_venda_meses_row(row: dict[str, Any]) -> float:
    try:
        doc = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
        t = float(doc.inputs.tempo_estimado_venda_meses or 12.0)
        return max(1.0, min(120.0, t))
    except Exception:
        return 12.0


def _doc_relatorio_mercado_row(row: dict[str, Any]):
    raw = row.get("relatorio_mercado_contexto_json")
    if isinstance(raw, str) and raw.strip():
        try:
            import json

            raw = json.loads(raw)
        except Exception:
            raw = {}
    return parse_relatorio_mercado_contexto_json(raw if isinstance(raw, dict) else {})


def _ttl_relatorio_horas() -> int:
    raw = str(os.getenv("RELATORIO_MERCADO_TTL_HORAS", "168") or "168").strip()
    try:
        return max(24, min(int(raw), 24 * 90))
    except Exception:
        return 168


def _status_validade_relatorio_row(row: dict[str, Any]) -> tuple[bool, str]:
    raw = row.get("relatorio_mercado_contexto_json")
    if not raw:
        return False, ""
    doc = _doc_relatorio_mercado_row(row)
    if not str(doc.gerado_em_iso or "").strip():
        return False, ""
    ttl_h = _ttl_relatorio_horas()
    try:
        dt = datetime.fromisoformat(str(doc.gerado_em_iso or "").replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        horas = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0
    except Exception:
        horas = float(ttl_h + 1)
    expirado = bool(getattr(doc.validade, "expirado", False)) or horas > float(ttl_h)
    motivo = str(getattr(doc.validade, "motivo", "") or "").strip()
    if not motivo and expirado:
        motivo = f"Relatório de mercado desatualizado ({horas:.0f}h > TTL {ttl_h}h)."
    return expirado, motivo


def _sinais_mercado_row(row: dict[str, Any]) -> tuple[int, int, int, int]:
    doc = _doc_relatorio_mercado_row(row)
    cards = list(doc.cards or [])
    tem_conteudo = any((c.topicos or []) or str(getattr(c, "evidencia", "") or "").strip() for c in cards)
    fallback = extrair_sinais_objetivos_por_cards(cards) if cards else {
        "liquidez_bairro": 50,
        "pressao_concorrencia": 50,
        "fit_imovel_bairro": 50,
    }
    liq = int(getattr(doc.sinais_decisao, "liquidez_bairro", 0) or 0) or int(fallback["liquidez_bairro"])
    prs = int(getattr(doc.sinais_decisao, "pressao_concorrencia", 0) or 0) or int(fallback["pressao_concorrencia"])
    fit = int(getattr(doc.sinais_decisao, "fit_imovel_bairro", 0) or 0) or int(fallback["fit_imovel_bairro"])
    qual = int(getattr(doc.qualidade, "score_qualidade", 0) or 0)
    if qual <= 0:
        qual = 55 if tem_conteudo else 0
    return (
        max(0, min(100, liq)),
        max(0, min(100, prs)),
        max(0, min(100, fit)),
        max(0, min(100, qual)),
    )


def _ajuste_conservador_por_mercado(
    *,
    roi_cons: float | None,
    lucro_cons: float | None,
    tempo_base_meses: float,
    liquidez: int,
    pressao: int,
    fit: int,
    qualidade: int,
) -> tuple[float | None, float | None, float, float]:
    haircut = 0.0
    haircut += max(0.0, (55.0 - float(liquidez)) * 0.0035)
    haircut += max(0.0, (float(pressao) - 55.0) * 0.0028)
    haircut += max(0.0, (50.0 - float(fit)) * 0.0025)
    if qualidade < 50:
        haircut += 0.03
    haircut = max(0.0, min(0.22, haircut))
    meses_extra = max(0.0, (55.0 - float(liquidez)) / 8.0) + max(0.0, (float(pressao) - 55.0) / 10.0)
    if qualidade < 50:
        meses_extra += 1.0
    tempo_cons = max(1.0, float(tempo_base_meses) + round(meses_extra))
    fator_tempo = float(tempo_base_meses) / tempo_cons if tempo_cons > 0 else 1.0
    fator = max(0.45, (1.0 - haircut) * fator_tempo)
    roi_out = (float(roi_cons) * fator) if roi_cons is not None else None
    lucro_out = (float(lucro_cons) * fator) if lucro_cons is not None else None
    return roi_out, lucro_out, tempo_cons, haircut


def _semaforo_decisao(o: "_RowOut") -> tuple[str, str]:
    rb = o.roi_conservador if o.roi_conservador is not None else o.roi_bruto
    ef = o.retorno_por_capital
    roi_buy = _float_env("DASHBOARD_SEMAFORO_ROI_COMPRAR", 0.45, min_v=0.0, max_v=3.0)
    ef_buy = _float_env("DASHBOARD_SEMAFORO_EFICIENCIA_COMPRAR", 0.35, min_v=0.0, max_v=3.0)
    qual_buy = _float_env("DASHBOARD_SEMAFORO_QUALIDADE_COMPRAR", 65.0, min_v=0.0, max_v=100.0)
    liq_buy = _float_env("DASHBOARD_SEMAFORO_LIQUIDEZ_COMPRAR_MIN", 55.0, min_v=0.0, max_v=100.0)
    conc_buy_max = _float_env("DASHBOARD_SEMAFORO_CONCORRENCIA_COMPRAR_MAX", 60.0, min_v=0.0, max_v=100.0)
    roi_neg = _float_env("DASHBOARD_SEMAFORO_ROI_NEGOCIAR", 0.25, min_v=0.0, max_v=3.0)
    ef_neg = _float_env("DASHBOARD_SEMAFORO_EFICIENCIA_NEGOCIAR", 0.18, min_v=0.0, max_v=3.0)
    qual_neg = _float_env("DASHBOARD_SEMAFORO_QUALIDADE_NEGOCIAR", 45.0, min_v=0.0, max_v=100.0)
    comp = float(o.capital_comprometido_pct or 0.0) if o.capital_comprometido_pct is not None else 0.0
    if o.hibrido_ativo:
        if comp > 35.0:
            roi_buy += 0.12
            ef_buy += 0.12
            qual_buy += 18
            liq_buy += 8
            conc_buy_max -= 8
            roi_neg += 0.05
            ef_neg += 0.05
            qual_neg += 10
        elif comp > 20.0:
            roi_buy += 0.05
            ef_buy += 0.05
            qual_buy += 10
            liq_buy += 4
            conc_buy_max -= 4
            roi_neg += 0.02
            ef_neg += 0.02
            qual_neg += 4
        if comp > 50.0:
            return "Evitar", f"Comprometimento de caixa muito alto ({comp:.1f}%)."
    if (
        rb is not None
        and rb >= roi_buy
        and ef is not None
        and ef >= ef_buy
        and o.qualidade_relatorio_score >= qual_buy
        and not o.relatorio_expirado
        and o.liquidez_bairro_score >= liq_buy
        and o.pressao_concorrencia_score <= conc_buy_max
    ):
        if o.hibrido_ativo and comp > 0:
            return "Comprar", f"Risco/liquidez equilibrados com comprometimento de caixa em {comp:.1f}%."
        return "Comprar", "Risco/liquidez equilibrados para execução."
    if rb is not None and rb >= roi_neg and ef is not None and ef >= ef_neg and o.qualidade_relatorio_score >= qual_neg:
        if o.hibrido_ativo and comp > 0:
            return "Negociar lance", f"Oportunidade existe, mas requer margem de segurança (caixa {comp:.1f}%)."
        return "Negociar lance", "Oportunidade existe, mas requer margem de segurança maior."
    motivo = "Baixo retorno ajustado ao risco/liquidez."
    if o.relatorio_expirado:
        motivo = "Relatório de mercado desatualizado; regenere antes de decidir."
    return "Evitar", motivo


def _sensibilidade_risco(o: "_RowOut", *, perfil: str = "balanceado") -> tuple[float, float, float]:
    """
    Multiplicadores (conservador, base, agressivo) para ROI/lucro
    a partir da confiança operacional e da origem dos números.
    """
    p = _parametros_risco_por_perfil(perfil)
    conf = max(0, min(100, int(o.confianca_operacional or 0)))
    penal = float(p["base_penal"]) + ((100 - conf) / 100.0) * float(p["faixa_conf"])
    if o.roi_origem == "pós-cache":
        penal += float(p["penal_origem_pos_cache"])
    if not o.tem_cache:
        penal += float(p["penal_sem_cache"])
    if not o.tem_mercado_llm:
        penal += float(p["penal_sem_mercado"])
    penal = max(float(p["penal_min"]), min(float(p["penal_max"]), penal))
    conservador = max(0.5, 1.0 - penal)
    agressivo = min(1.30, 1.0 + penal * float(p["ganho_agressivo"]))
    return conservador, 1.0, agressivo


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


def _pesos_score_por_perfil(perfil: str) -> dict[str, float]:
    p = str(perfil or "").strip().lower()
    if p == "conservador":
        return {"roi": 95.0, "lucro": 6.0, "urgencia": 2.0, "prioritario": 90.0}
    if p == "agressivo":
        return {"roi": 140.0, "lucro": 8.0, "urgencia": 2.8, "prioritario": 140.0}
    return {"roi": 120.0, "lucro": 7.0, "urgencia": 2.4, "prioritario": 120.0}


def _parametros_risco_por_perfil(perfil: str) -> dict[str, float]:
    p = str(perfil or "").strip().lower()
    if p == "conservador":
        return {
            "base_penal": 0.14,
            "faixa_conf": 0.20,
            "penal_origem_pos_cache": 0.07,
            "penal_sem_cache": 0.07,
            "penal_sem_mercado": 0.04,
            "penal_min": 0.12,
            "penal_max": 0.44,
            "ganho_agressivo": 0.35,
        }
    if p == "agressivo":
        return {
            "base_penal": 0.08,
            "faixa_conf": 0.13,
            "penal_origem_pos_cache": 0.03,
            "penal_sem_cache": 0.04,
            "penal_sem_mercado": 0.02,
            "penal_min": 0.06,
            "penal_max": 0.32,
            "ganho_agressivo": 0.60,
        }
    return {
        "base_penal": 0.10,
        "faixa_conf": 0.16,
        "penal_origem_pos_cache": 0.05,
        "penal_sem_cache": 0.05,
        "penal_sem_mercado": 0.03,
        "penal_min": 0.08,
        "penal_max": 0.40,
        "ganho_agressivo": 0.45,
    }


def _score_prioridade(o: _RowOut, *, hoje: date, perfil: str = "balanceado") -> tuple[float, str]:
    score = 0.0
    partes: list[str] = []
    w = _pesos_score_por_perfil(perfil)
    rb = o.roi_conservador if o.roi_conservador is not None else o.roi_bruto
    ll = o.lucro_conservador if o.lucro_conservador is not None else o.lucro_liq
    if rb is not None:
        s_roi = min(max(float(rb), 0.0), 1.5) * float(w["roi"])
        score += s_roi
        partes.append(f"ROI +{s_roi:.1f}")
    if ll is not None:
        s_lucro = min(max(float(ll), 0.0) / 100_000.0, 25.0) * float(w["lucro"])
        score += s_lucro
        partes.append(f"Lucro +{s_lucro:.1f}")
    if o.prox_data:
        dias = (o.prox_data - hoje).days
        if dias >= 0:
            s_tempo = max(0.0, (60.0 - float(dias)) * float(w["urgencia"]))
            score += s_tempo
            if s_tempo > 0:
                partes.append(f"Urgência +{s_tempo:.1f}")
        else:
            s_passado = min(25.0, abs(float(dias)) * 0.6)
            score -= s_passado
            partes.append(f"Praça passada -{s_passado:.1f}")
    if _e_prioritario(o):
        s_pri = float(w["prioritario"])
        score += s_pri
        partes.append(f"Prioritário +{s_pri:.0f}")
    if not o.tem_simulacao:
        score -= 10.0
        partes.append("Sem simulação -10")
    if not o.tem_mercado_llm:
        score -= 8.0
        partes.append("Sem mercado LLM -8")
    if o.retorno_por_capital is not None:
        w_ef = _float_env("DASHBOARD_SCORE_PESO_EFICIENCIA_CAPITAL", 24.0, min_v=0.0, max_v=80.0)
        s_ef = min(max(float(o.retorno_por_capital), 0.0), 2.0) * w_ef
        score += s_ef
        partes.append(f"Eficiência capital +{s_ef:.1f}")
    w_liq = _float_env("DASHBOARD_SCORE_PESO_LIQUIDEZ", 20.0, min_v=0.0, max_v=80.0)
    w_conc = _float_env("DASHBOARD_SCORE_PESO_CONCORRENCIA", 18.0, min_v=0.0, max_v=80.0)
    w_fit = _float_env("DASHBOARD_SCORE_PESO_FIT", 14.0, min_v=0.0, max_v=60.0)
    w_qual = _float_env("DASHBOARD_SCORE_PESO_QUALIDADE_RELATORIO", 12.0, min_v=0.0, max_v=60.0)
    p_exp = _float_env("DASHBOARD_SCORE_PENALIDADE_RELATORIO_EXPIRADO", 8.0, min_v=0.0, max_v=40.0)
    s_liq = ((float(o.liquidez_bairro_score) - 50.0) / 50.0) * w_liq
    score += s_liq
    if abs(s_liq) >= 0.2:
        partes.append(f"Liquidez bairro {s_liq:+.1f}")
    s_conc = -((float(o.pressao_concorrencia_score) - 50.0) / 50.0) * w_conc
    score += s_conc
    if abs(s_conc) >= 0.2:
        partes.append(f"Pressão concorrência {s_conc:+.1f}")
    s_fit = ((float(o.fit_imovel_bairro_score) - 50.0) / 50.0) * w_fit
    score += s_fit
    if abs(s_fit) >= 0.2:
        partes.append(f"Fit imóvel-bairro {s_fit:+.1f}")
    s_qual = ((float(o.qualidade_relatorio_score) - 50.0) / 50.0) * w_qual
    score += s_qual
    if abs(s_qual) >= 0.2:
        partes.append(f"Qualidade relatório {s_qual:+.1f}")
    if o.relatorio_expirado:
        score -= p_exp
        partes.append(f"Relatório expirado -{p_exp:.0f}")
    if o.hibrido_ativo and o.capital_comprometido_pct is not None:
        pct_ref = _float_env("DASHBOARD_HIBRIDO_FAIXA_CAIXA_CONFORTO_PCT", 20.0, min_v=5.0, max_v=60.0)
        k_pen = _float_env("DASHBOARD_SCORE_PENALIDADE_CAIXA_POR_PONTO", 0.70, min_v=0.0, max_v=3.0)
        excesso = max(0.0, float(o.capital_comprometido_pct) - pct_ref)
        p_cx = min(30.0, excesso * k_pen)
        if p_cx > 0:
            score -= p_cx
            partes.append(f"Caixa -{p_cx:.1f}")
    explicacao = " · ".join(partes[:6])
    return round(score, 2), explicacao


def _row_to_out(
    r: dict[str, Any],
    *,
    hoje: date,
    perfil_risco: str = "balanceado",
    hibrido_ativo: bool = False,
    caixa_disponivel_brl: float | None = None,
    caixa_reserva_brl: float = 0.0,
) -> _RowOut:
    iid = str(r.get("id") or "")
    d, pl = _proxima_data_e_praca(r, hoje=hoje)
    cids = r.get("cache_media_bairro_ids") or []
    ncache = len(cids) if isinstance(cids, (list, tuple)) else 0
    tem_sim = _tem_simulacao(r)
    tem_merc = _tem_mercado_llm(r)
    confianca = 40
    if ncache > 0:
        confianca += 25
    if tem_merc:
        confianca += 15
    if tem_sim:
        confianca += 20
    confianca = max(0, min(100, int(confianca)))
    ll = _lucro_liquido_de_row(r)
    rb = _roi_bruto_de_row(r)
    rl = _roi_liquido_de_row(r)
    cap = _capital_imobilizado_de_row(r)
    ret_cap = (ll / cap) if (ll is not None and cap is not None and cap > 0) else None
    caixa_util = None
    comprometido_pct = None
    if hibrido_ativo:
        try:
            caixa_total = float(caixa_disponivel_brl or 0.0)
            reserva = float(caixa_reserva_brl or 0.0)
            caixa_util_calc = max(0.0, caixa_total - reserva)
            caixa_util = caixa_util_calc
            if caixa_util_calc > 0 and cap is not None and cap > 0:
                comprometido_pct = (float(cap) / float(caixa_util_calc)) * 100.0
        except Exception:
            caixa_util = 0.0
            comprometido_pct = None
    liq, prs, fit, qual = _sinais_mercado_row(r)
    rel_exp, _rel_motivo = _status_validade_relatorio_row(r)
    tempo_base = _tempo_estimado_venda_meses_row(r)
    out = _RowOut(
        id=iid,
        cidade=str(r.get("cidade") or "—")[:32],
        estado=str(r.get("estado") or "")[:3],
        bairro=str(r.get("bairro") or "")[:28],
        tipo_imovel=str(r.get("tipo_imovel") or "")[:40],
        url=str(r.get("url_leilao") or "")[:120],
        endereco=str(r.get("endereco") or "")[:60],
        prox_data=d,
        lucro_liq=ll,
        roi_bruto=rb,
        roi_liquido=rl,
        roi_origem=("simulação gravada" if tem_sim else "pós-cache"),
        tem_simulacao=tem_sim,
        tem_mercado_llm=tem_merc,
        tem_cache=ncache > 0,
        confianca_operacional=confianca,
        capital_imobilizado=cap,
        retorno_por_capital=ret_cap,
        lucro_conservador=None,
        lucro_agressivo=None,
        roi_conservador=None,
        roi_agressivo=None,
        tempo_venda_conservador_meses=None,
        haircut_venda_conservador_pct=None,
        liquidez_bairro_score=liq,
        pressao_concorrencia_score=prs,
        fit_imovel_bairro_score=fit,
        qualidade_relatorio_score=qual,
        relatorio_expirado=rel_exp,
        hibrido_ativo=bool(hibrido_ativo),
        capital_comprometido_pct=round(float(comprometido_pct), 2) if comprometido_pct is not None else None,
        caixa_util_brl=round(float(caixa_util), 2) if caixa_util is not None else None,
        semaforo_decisao="",
        semaforo_justificativa="",
        praca_label=pl,
        score_prioridade=0.0,
        url_foto_imovel=_url_foto_imovel_row(r),
    )
    m_cons, _, m_agr = _sensibilidade_risco(out, perfil=perfil_risco)
    lucro_cons_base = (ll * m_cons) if ll is not None else None
    roi_cons_base = (rb * m_cons) if rb is not None else None
    roi_cons_aj, lucro_cons_aj, tempo_cons, haircut = _ajuste_conservador_por_mercado(
        roi_cons=roi_cons_base,
        lucro_cons=lucro_cons_base,
        tempo_base_meses=tempo_base,
        liquidez=liq,
        pressao=prs,
        fit=fit,
        qualidade=qual,
    )
    out.lucro_conservador = lucro_cons_aj
    out.lucro_agressivo = (ll * m_agr) if ll is not None else None
    out.roi_conservador = roi_cons_aj
    out.roi_agressivo = (rb * m_agr) if rb is not None else None
    out.tempo_venda_conservador_meses = tempo_cons
    out.haircut_venda_conservador_pct = haircut
    sema, sema_j = _semaforo_decisao(out)
    out.semaforo_decisao = sema
    out.semaforo_justificativa = sema_j
    return out


def processar_rows_dashboard(
    rows: list[dict[str, Any]],
    *,
    perfil_score: str = "balanceado",
    perfil_risco: str = "balanceado",
    hibrido_ativo: bool = False,
    caixa_disponivel_brl: float | None = None,
    caixa_reserva_brl: float = 0.0,
) -> DashboardDados:
    hoje = datetime.now(_TZ_SP).date()
    outs = [
        _row_to_out(
            r,
            hoje=hoje,
            perfil_risco=perfil_risco,
            hibrido_ativo=hibrido_ativo,
            caixa_disponivel_brl=caixa_disponivel_brl,
            caixa_reserva_brl=caixa_reserva_brl,
        )
        for r in rows
        if r.get("id")
    ]
    for i, o in enumerate(outs):
        s, e = _score_prioridade(o, hoje=hoje, perfil=perfil_score)
        outs[i].score_prioridade = s
        outs[i].score_explicacao = e
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
    ef_cap = sorted(
        [x for x in op if x.retorno_por_capital is not None],
        key=lambda z: (z.retorno_por_capital or 0.0, z.score_prioridade),
        reverse=True,
    )[:8]
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
        eficiencia_capital=ef_cap,
        proximos=proximos,
        pendentes=pendentes,
        lembretes=lembretes,
        calendario=cal,
    )


def agregar_listas_por_dia(
    rows: list[dict[str, Any]],
    dia: date,
    *,
    perfil_score: str = "balanceado",
    perfil_risco: str = "balanceado",
    hibrido_ativo: bool = False,
    caixa_disponivel_brl: float | None = None,
    caixa_reserva_brl: float = 0.0,
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
        o = _row_to_out(
            r,
            hoje=hoje,
            perfil_risco=perfil_risco,
            hibrido_ativo=hibrido_ativo,
            caixa_disponivel_brl=caixa_disponivel_brl,
            caixa_reserva_brl=caixa_reserva_brl,
        )
        s, e = _score_prioridade(o, hoje=hoje, perfil=perfil_score)
        o.score_prioridade = s
        o.score_explicacao = e
        if o.prox_data == dia:
            outs_dia.append(o)
    if not outs_dia:
        return [], [], []
    op_dia = [x for x in outs_dia if _elegivel_oportunidades_roi(x)]
    com_data = [x for x in op_dia if x.prox_data is not None]
    top_l = sorted(
        [x for x in op_dia if x.retorno_por_capital is not None],
        key=lambda z: (z.retorno_por_capital or 0.0, z.score_prioridade),
        reverse=True,
    )[:8]
    if not top_l:
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
