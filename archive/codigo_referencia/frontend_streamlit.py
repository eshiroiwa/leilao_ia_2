from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import math
import statistics
import tempfile
import unicodedata
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd
import streamlit as st

import requests

logger = logging.getLogger(__name__)

from anuncios_mercado import (
    TABLE_ANUNCIOS_MERCADO,
    AnuncioMercadoPersist,
    chave_estado,
    coletar_e_persistir_via_ddgs,
    descobrir_bairros_vivareal,
    firecrawl_account_credits,
    firecrawl_status,
    portal_de_url,
    slug_bairro_para_nome,
)
from financial_agent import (
    RoiCalculoEntrada,
    calcular_lance_maximo_para_roi,
    calcular_roi_liquido,
)
from ingestion_agent import (
    SUPABASE_TABLE,
    atualizar_leilao_imovel_campos,
    get_supabase_client,
    ingerir_url_leilao,
)
from leilao_constants import (
    STATUS_PENDENTE,
    area_efetiva_de_registro,
    normalizar_tipo_imovel,
    segmento_mercado_de_registro,
)
from pricing_pipeline import (
    LeilaoPricingPipelineConfig,
    executar_pipeline_precificacao_leiloes,
    ler_entradas_leilao_de_planilha,
)
from token_efficiency import (
    CACHE_TABLE,
    nome_cache_automatico,
    normalizar_chave_bairro,
    normalizar_chave_segmento,
)

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Leilão IA",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
/*
 * Header overlap fix without breaking scroll:
 * Do NOT set header position:relative — it pulls the fixed header into document flow,
 * makes .stApp taller than the viewport, and the bottom of the page gets clipped.
 * Keep the default fixed header and only offset the scrollable main content.
 */
header[data-testid="stHeader"],
.stAppHeader {
    z-index: 999991;
}
.block-container,
[data-testid="stAppViewBlockContainer"],
.stMainBlockContainer {
    padding-top: max(3.25rem, calc(0.75rem + env(safe-area-inset-top))) !important;
    padding-bottom: max(2.5rem, calc(1rem + env(safe-area-inset-bottom))) !important;
}
/* Toolbar stays above content but does not change layout flow */
[data-testid="stToolbar"] {
    z-index: 999992;
}
[data-testid="stDecoration"] {
    display: none;
}

/* Metrics cards */
[data-testid="stMetricValue"] {
    font-size: 1.15rem !important;
    font-weight: 600;
}
[data-testid="stMetricLabel"] { font-size: 0.82rem !important; }

/* Sidebar polish */
section[data-testid="stSidebar"] > div { padding-top: 1.2rem; }
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stNumberInput label {
    font-size: 0.85rem;
}

/* Table links */
.stDataFrame a { color: #4da6ff !important; text-decoration: none; }
.stDataFrame a:hover { text-decoration: underline; }
/* Composição: scroll horizontal sem “perder” controles de seleção (dataframe nativo) */
[data-testid="stDataFrame"] { max-width: 100%; overflow-x: auto; }

/* Divider */
hr { margin: 0.6rem 0 !important; }

/* ── Leilões: KPI cards ── */
.kpi-row { display: flex; gap: 0.7rem; margin-bottom: 1rem; flex-wrap: wrap; }
.kpi-card {
    flex: 1 1 0;
    min-width: 120px;
    background: linear-gradient(135deg, rgba(30,58,95,.45) 0%, rgba(20,40,70,.35) 100%);
    border: 1px solid rgba(100,160,255,.15);
    border-radius: 12px;
    padding: 0.85rem 1rem;
    text-align: center;
    backdrop-filter: blur(4px);
}
.kpi-card .kpi-value { font-size: 1.5rem; font-weight: 700; color: #e8edf3; line-height: 1.3; }
.kpi-card .kpi-label { font-size: 0.75rem; color: #8fa4bf; text-transform: uppercase; letter-spacing: 0.04em; margin-top: 2px; }
.kpi-card.positive .kpi-value { color: #2ecc71; }
.kpi-card.negative .kpi-value { color: #e74c3c; }
.kpi-card.accent .kpi-value { color: #f1c40f; }

/* Simulador: KPIs mais compactos (números em uma linha; scroll se faltar largura) */
.kpi-row.sim-kpi {
    gap: 0.45rem;
    flex-wrap: nowrap;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    padding-bottom: 2px;
}
.kpi-row.sim-kpi .kpi-card {
    min-width: 5.5rem;
    flex: 0 1 auto;
    max-width: 100%;
    padding: 0.45rem 0.35rem;
}
.kpi-row.sim-kpi .kpi-card .kpi-value {
    font-size: clamp(0.62rem, 1.85vw, 0.88rem);
    font-weight: 700;
    line-height: 1.2;
    white-space: nowrap;
}
.kpi-row.sim-kpi .kpi-card .kpi-label {
    font-size: 0.55rem;
    letter-spacing: 0.02em;
    line-height: 1.15;
}

/* ── Leilões: Detail header ── */
.lei-header {
    background: linear-gradient(135deg, rgba(40,70,120,.35) 0%, rgba(25,45,80,.25) 100%);
    border: 1px solid rgba(100,160,255,.12);
    border-radius: 14px;
    padding: 1rem 1.2rem;
    margin-bottom: 0.8rem;
}
.lei-header .lei-location { font-size: 1.15rem; font-weight: 600; color: #e0e6ed; }
.lei-header .lei-sub { font-size: 0.82rem; color: #8fa4bf; margin-top: 2px; }

/* ── Leilões: Badge ── */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.03em;
}
.badge-green { background: rgba(46,204,113,.18); color: #2ecc71; border: 1px solid rgba(46,204,113,.3); }
.badge-red { background: rgba(231,76,60,.18); color: #e74c3c; border: 1px solid rgba(231,76,60,.3); }
.badge-yellow { background: rgba(241,196,15,.18); color: #f1c40f; border: 1px solid rgba(241,196,15,.3); }
.badge-blue { background: rgba(52,152,219,.18); color: #3498db; border: 1px solid rgba(52,152,219,.3); }
.badge-gray { background: rgba(149,165,166,.18); color: #95a5a6; border: 1px solid rgba(149,165,166,.3); }

/* ── Leilões: Info grid ── */
.info-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; margin: 0.6rem 0; }
.info-item {
    background: rgba(30,50,80,.25);
    border: 1px solid rgba(100,160,255,.08);
    border-radius: 8px;
    padding: 0.55rem 0.7rem;
}
.info-item .info-label { font-size: 0.68rem; color: #7a8ea0; text-transform: uppercase; letter-spacing: 0.04em; }
.info-item .info-value { font-size: 1rem; font-weight: 600; color: #d5dde5; margin-top: 1px; }

/* ── Leilões: section divider ── */
.section-title {
    font-size: 0.78rem;
    font-weight: 600;
    color: #7a99b8;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid rgba(100,160,255,.12);
    padding-bottom: 4px;
    margin: 0.9rem 0 0.5rem 0;
}

/* ── Leilões: empty state ── */
.empty-state {
    text-align: center;
    padding: 3rem 1rem;
    color: #6a7f96;
}
.empty-state .empty-icon { font-size: 2.5rem; margin-bottom: 0.5rem; }
.empty-state .empty-text { font-size: 0.95rem; }
</style>
""", unsafe_allow_html=True)

# ─── Globals ──────────────────────────────────────────────────────────────────
_TASKS_LOCK = threading.Lock()
_TASKS: dict[str, dict[str, Any]] = {}
_ANUNCIOS_SOFT_DELETE_CACHE: bool | None = None

# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _refresh() -> None:
    st.session_state["_last_data_refresh_ts"] = time.time()
    st.rerun()


# Persistência de seleções na aba Cache (sobrevive ao st.rerun após incluir/excluir)
_CA_SS_SEL_CACHE_ROWS = "ca_persist_selected_cache_row_ids"
_CA_SS_SEL_POOL_ROWS = "ca_persist_selected_pool_row_ids"
# Última lista de IDs do pool na tabela (deteta desmarcar → limpar seen do Folium e não re-injetar do mapa).
_CA_SS_POOL_SEL_TRACK = "_ca_pool_table_sel_ids_track"
# Retângulo visível devolvido pelo st_folium (sidebar, marcar no mapa); não se usa para fit_bounds no Python.
_CA_SS_POOL_VIEWPORT_BOUNDS = "_ca_pool_map_viewport_bounds"
# IDs acumulados por clique no mapa (popup) — pool vs composição.
_CA_SS_POOL_MAP_IDS = "ca_pool_ids_from_map"
_CA_SS_COMP_MAP_IDS = "ca_comp_ids_from_map"
# Altura única dos mapas (e tabelas alinhadas) na aba Cache — pool e composição.
_CA_CACHE_MAP_HEIGHT = 400
# Seleção na aba Anúncios (mapa + checkboxes); união com cliques sucessivos no mapa.
_AN_SS_SEL_IDS = "an_persist_selected_row_ids"
# Bounds do Folium na aba Anúncios (sidebar: marcar no filtro todos do mapa).
_AN_SS_MAP_VIEWPORT_BOUNDS = "_an_pool_map_viewport_bounds"


def _ca_prune_persist_to_valid_ids(persist_key: str, valid_ids: set[str]) -> None:
    cur = [str(x) for x in (st.session_state.get(persist_key) or []) if str(x) in valid_ids]
    st.session_state[persist_key] = cur


def _ca_widget_sel_state(rows: list[int]) -> dict[str, Any]:
    """Estado de seleção compatível com ``st.dataframe`` (``on_select`` / session_state da chave)."""
    return {"selection": {"rows": [int(i) for i in rows], "columns": [], "cells": []}}


def _ca_set_pool_persist_selection_ids(ids: list[str]) -> None:
    """Atualiza IDs persistidos do pool; se a seleção encolheu, limpa ``seen`` do Folium (evita re-merge stale)."""
    new_s = {str(x) for x in (ids or [])}
    prev_s = {str(x) for x in (st.session_state.get(_CA_SS_POOL_SEL_TRACK) or [])}
    if prev_s and new_s < prev_s:
        for _sk in ("_folium_popup_seen_ca_pool_map", "_folium_popup_seen_ca_pool_map_pc"):
            st.session_state.pop(_sk, None)
    st.session_state[_CA_SS_POOL_SEL_TRACK] = list(ids)
    st.session_state[_CA_SS_SEL_POOL_ROWS] = list(ids)


def _ca_sync_pool_sel_track_from_persist() -> None:
    """Alinha o track ao persist (ex.: após ``_ca_prune``), sem interpretar como desmarcar na tabela."""
    st.session_state[_CA_SS_POOL_SEL_TRACK] = list(st.session_state.get(_CA_SS_SEL_POOL_ROWS) or [])


def _ca_pool_touch_viewport_cache(df: pd.DataFrame) -> None:
    """Limpa bounds do mapa do pool quando a lista filtrada (ids) muda, para não usar retângulo antigo."""
    if df.empty or "id" not in df.columns:
        st.session_state.pop(_CA_SS_POOL_VIEWPORT_BOUNDS, None)
        st.session_state.pop("_ca_pool_vis_fp", None)
        return
    fp = hashlib.md5(",".join(sorted(df["id"].astype(str))).encode()).hexdigest()[:20]
    if st.session_state.get("_ca_pool_vis_fp") != fp:
        st.session_state["_ca_pool_vis_fp"] = fp
        st.session_state.pop(_CA_SS_POOL_VIEWPORT_BOUNDS, None)


def _an_map_touch_viewport_cache(df: pd.DataFrame) -> None:
    """Limpa bounds do mapa da aba Anúncios quando a lista filtrada (ids) muda."""
    if df.empty or "id" not in df.columns:
        st.session_state.pop(_AN_SS_MAP_VIEWPORT_BOUNDS, None)
        st.session_state.pop("_an_map_vis_fp", None)
        return
    fp = hashlib.md5(",".join(sorted(df["id"].astype(str))).encode()).hexdigest()[:20]
    if st.session_state.get("_an_map_vis_fp") != fp:
        st.session_state["_an_map_vis_fp"] = fp
        st.session_state.pop(_AN_SS_MAP_VIEWPORT_BOUNDS, None)


def _ca_df_select_widget_key(prefix: str, df: pd.DataFrame, id_col: str = "id") -> str:
    """Chave única por conjunto de IDs visíveis, para remontar seleção após mudar filtros/dados."""
    if df.empty or id_col not in df.columns:
        return f"{prefix}_empty"
    h = hashlib.md5(",".join(sorted(df[id_col].astype(str))).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{h}"


def _ca_maybe_seed_df_selection(key: str, df: pd.DataFrame, id_col: str, persist_key: str) -> None:
    """Primeira montagem do widget: aplica IDs persistidos como linhas selecionadas."""
    if key in st.session_state or df.empty or id_col not in df.columns:
        return
    want = {str(x) for x in (st.session_state.get(persist_key) or [])}
    if not want:
        return
    rows = [i for i, rid in enumerate(df[id_col].astype(str)) if rid in want]
    if rows:
        st.session_state[key] = _ca_widget_sel_state(rows)


def _ca_text_query_mask(df: pd.DataFrame, query: str, cols: tuple[str, ...]) -> pd.Series:
    """Máscara por texto: contém em qualquer uma das colunas (case-insensitive)."""
    qn = (query or "").strip().lower()
    if not qn:
        return pd.Series(True, index=df.index)
    present = tuple(c for c in cols if c in df.columns)
    if not present:
        return pd.Series(True, index=df.index)
    acc = pd.Series(False, index=df.index)
    for c in present:
        acc |= df[c].astype(str).str.lower().str.contains(qn, na=False, regex=False)
    return acc


def _ca_num_range_mask(df: pd.DataFrame, col: str, lo: float, hi: float) -> pd.Series:
    """Filtro numérico inclusivo; ``lo`` ou ``hi`` <= 0 significa sem limite nesse extremo."""
    if col not in df.columns:
        return pd.Series(True, index=df.index)
    s = pd.to_numeric(df[col], errors="coerce")
    m = pd.Series(True, index=df.index)
    if lo > 0:
        m &= s >= float(lo)
    if hi > 0:
        m &= s <= float(hi)
    return m


def _ca_col_contains_ci(df: pd.DataFrame, col: str, q: str) -> pd.Series:
    """Contém case-insensitive numa coluna; texto vazio = sem filtro."""
    s = (q or "").strip()
    if not s or col not in df.columns:
        return pd.Series(True, index=df.index)
    sl = s.lower()
    return df[col].astype(str).str.lower().str.contains(sl, na=False, regex=False)


def _ca_mask_pool_like(
    df: pd.DataFrame,
    *,
    cidade: str,
    bairro: str,
    estado: str,
    texto_outros: str,
    pm2_lo: float,
    pm2_hi: float,
    v_lo: float,
    v_hi: float,
    a_lo: float,
    a_hi: float,
) -> pd.Series:
    """Filtros locais para tabelas no estilo anúncios (pool / composição).

    Cidade, bairro e estado são **independentes** (AND): preencha só o que precisar.
    ``texto_outros`` busca em tipo, título, URL e descrição.
    """
    m = pd.Series(True, index=df.index)
    m &= _ca_col_contains_ci(df, "cidade", cidade)
    m &= _ca_col_contains_ci(df, "bairro", bairro)
    m &= _ca_col_contains_ci(df, "estado", estado)
    if (texto_outros or "").strip():
        m &= _ca_text_query_mask(
            df,
            texto_outros,
            ("tipo_imovel", "titulo", "url_anuncio", "descricao"),
        )
    m &= _ca_num_range_mask(df, "preco_m2", pm2_lo, pm2_hi)
    m &= _ca_num_range_mask(df, "valor_venda", v_lo, v_hi)
    m &= _ca_num_range_mask(df, "area_construida_m2", a_lo, a_hi)
    return m


def _ca_mask_cache_entradas(
    df: pd.DataFrame,
    *,
    cidade: str,
    bairro: str,
    estado: str,
    texto_outros: str,
    pm2_lo: float,
    pm2_hi: float,
    vmv_lo: float,
    vmv_hi: float,
    n_lo: int,
    n_hi: int,
    faixa: str,
) -> pd.Series:
    """Filtros locais para entradas de ``cache_media_bairro``.

    Cidade, bairro e estado em AND; ``texto_outros`` em tipo, fonte e chave de segmento
    (a faixa de área continua no seletor dedicado).
    """
    m = pd.Series(True, index=df.index)
    m &= _ca_col_contains_ci(df, "cidade", cidade)
    m &= _ca_col_contains_ci(df, "bairro", bairro)
    m &= _ca_col_contains_ci(df, "estado", estado)
    if (texto_outros or "").strip():
        m &= _ca_text_query_mask(df, texto_outros, ("tipo_imovel", "fonte", "chave_segmento"))
    m &= _ca_num_range_mask(df, "preco_m2_medio", pm2_lo, pm2_hi)
    m &= _ca_num_range_mask(df, "valor_medio_venda", vmv_lo, vmv_hi)
    if "n_amostras" in df.columns and (n_lo > 0 or n_hi > 0):
        ns = pd.to_numeric(df["n_amostras"], errors="coerce")
        if n_lo > 0:
            m &= ns >= float(n_lo)
        if n_hi > 0:
            m &= ns <= float(n_hi)
    if faixa and str(faixa).strip() and str(faixa).strip() != "(todas)" and "faixa_area" in df.columns:
        m &= df["faixa_area"].astype(str).str.strip() == str(faixa).strip()
    return m


def _ca_mask_leilao_like(
    df: pd.DataFrame,
    *,
    cidade: str,
    bairro: str,
    estado: str,
    texto_outros: str,
    lance_lo: float,
    lance_hi: float,
    roi_lo: float | None,
    roi_hi: float | None,
    area_lo: float,
    area_hi: float,
) -> pd.Series:
    """Filtros locais para tabela de leilões (``leilao_imoveis`` / simulador)."""
    m = pd.Series(True, index=df.index)
    m &= _ca_col_contains_ci(df, "cidade", cidade)
    m &= _ca_col_contains_ci(df, "bairro", bairro)
    m &= _ca_col_contains_ci(df, "estado", estado)
    if (texto_outros or "").strip():
        m &= _ca_text_query_mask(
            df,
            texto_outros,
            tuple(c for c in ("tipo_imovel", "url_leilao", "status", "endereco", "padrao_imovel") if c in df.columns),
        )
    m &= _ca_num_range_mask(df, "valor_arrematacao", lance_lo, lance_hi)
    if "roi_projetado" in df.columns and (roi_lo is not None or roi_hi is not None):
        s = pd.to_numeric(df["roi_projetado"], errors="coerce")
        if roi_lo is not None:
            m &= s >= float(roi_lo)
        if roi_hi is not None:
            m &= s <= float(roi_hi)
    m &= _ca_num_range_mask(df, "area_util", area_lo, area_hi)
    return m


def _lei_table_filters_expander_ui(
    *,
    key_prefix: str,
    title: str = "Filtros da tabela (leilões)",
    expanded: bool = True,
) -> dict[str, Any]:
    """Widgets para ``_ca_mask_leilao_like`` (leilões / lista do simulador)."""
    with st.expander(title, expanded=expanded):
        fc1, fc2, fc3 = st.columns(3)
        cidade = fc1.text_input("Cidade (contém)", "", key=f"{key_prefix}_cid", placeholder="ex.: Porto Alegre")
        bairro = fc2.text_input("Bairro (contém)", "", key=f"{key_prefix}_bai")
        estado = fc3.text_input("Estado / UF (contém)", "", key=f"{key_prefix}_uf", placeholder="ex.: RS")
        outros = st.text_input(
            "Outros campos (contém)",
            "",
            key=f"{key_prefix}_out",
            placeholder="tipo, URL, status, endereço…",
        )
        r1, r2, r3 = st.columns(3)
        l_lo = r1.number_input("Lance mín. (R$)", 0.0, step=5000.0, key=f"{key_prefix}_l_lo", help="0 = sem limite inferior")
        l_hi = r2.number_input("Lance máx. (R$)", 0.0, step=5000.0, key=f"{key_prefix}_l_hi", help="0 = sem limite superior")
        r1, r2, r3 = st.columns(3)
        roi_lo_s = r1.text_input("ROI % mín.", "", key=f"{key_prefix}_roi_lo_s", placeholder="vazio = sem limite")
        roi_hi_s = r2.text_input("ROI % máx.", "", key=f"{key_prefix}_roi_hi_s", placeholder="vazio = sem limite")
        r1, r2, r3 = st.columns(3)
        a_lo = r1.number_input("Área útil mín. (m²)", 0.0, step=10.0, key=f"{key_prefix}_a_lo", help="0 = sem limite inferior")
        a_hi = r2.number_input("Área útil máx. (m²)", 0.0, step=10.0, key=f"{key_prefix}_a_hi", help="0 = sem limite superior")
    roi_lo = _to_float_or_none(roi_lo_s)
    roi_hi = _to_float_or_none(roi_hi_s)
    return {
        "cidade": str(cidade),
        "bairro": str(bairro),
        "estado": str(estado),
        "texto_outros": str(outros),
        "lance_lo": float(l_lo),
        "lance_hi": float(l_hi),
        "roi_lo": roi_lo,
        "roi_hi": roi_hi,
        "area_lo": float(a_lo),
        "area_hi": float(a_hi),
    }


def _ca_pool_filters_dict_from_state(key_prefix: str) -> dict[str, Any]:
    """Lê o último estado dos widgets ``_ca_pool_filters_expander_ui`` (mesmo ``key_prefix``)."""
    def _g(suf: str, default: Any) -> Any:
        return st.session_state.get(f"{key_prefix}_{suf}", default)

    return {
        "cidade": str(_g("cid", "")),
        "bairro": str(_g("bai", "")),
        "estado": str(_g("uf", "")),
        "texto_outros": str(_g("out", "")),
        "pm2_lo": float(_g("pm2_lo", 0.0)),
        "pm2_hi": float(_g("pm2_hi", 0.0)),
        "v_lo": float(_g("v_lo", 0.0)),
        "v_hi": float(_g("v_hi", 0.0)),
        "a_lo": float(_g("a_lo", 0.0)),
        "a_hi": float(_g("a_hi", 0.0)),
    }


def _lei_filters_dict_from_state(key_prefix: str) -> dict[str, Any]:
    """Lê estado dos widgets ``_lei_table_filters_expander_ui``."""
    def _g(suf: str, default: Any) -> Any:
        return st.session_state.get(f"{key_prefix}_{suf}", default)

    roi_lo = _to_float_or_none(str(_g("roi_lo_s", "")))
    roi_hi = _to_float_or_none(str(_g("roi_hi_s", "")))
    return {
        "cidade": str(_g("cid", "")),
        "bairro": str(_g("bai", "")),
        "estado": str(_g("uf", "")),
        "texto_outros": str(_g("out", "")),
        "lance_lo": float(_g("l_lo", 0.0)),
        "lance_hi": float(_g("l_hi", 0.0)),
        "roi_lo": roi_lo,
        "roi_hi": roi_hi,
        "area_lo": float(_g("a_lo", 0.0)),
        "area_hi": float(_g("a_hi", 0.0)),
    }


def _ca_pool_filters_expander_ui(
    *,
    key_prefix: str,
    title: str = "Filtros do pool",
    expanded: bool = True,
) -> dict[str, Any]:
    """Widgets de filtro para pool/composição; retorna argumentos para ``_ca_mask_pool_like``."""
    with st.expander(title, expanded=expanded):
        fc1, fc2, fc3 = st.columns(3)
        cidade = fc1.text_input("Cidade (contém)", "", key=f"{key_prefix}_cid", placeholder="ex.: Gravataí")
        bairro = fc2.text_input("Bairro (contém)", "", key=f"{key_prefix}_bai", placeholder="ex.: Centro")
        estado = fc3.text_input("Estado / UF (contém)", "", key=f"{key_prefix}_uf", placeholder="ex.: RS")
        outros = st.text_input(
            "Outros campos (contém)",
            "",
            key=f"{key_prefix}_out",
            placeholder="tipo, título, trecho do link, descrição…",
        )
        pr1, pr2, pr3 = st.columns(3)
        pm2_lo = pr1.number_input("R$/m² mín.", 0.0, step=50.0, key=f"{key_prefix}_pm2_lo", help="0 = sem limite inferior")
        pm2_hi = pr2.number_input("R$/m² máx.", 0.0, step=50.0, key=f"{key_prefix}_pm2_hi", help="0 = sem limite superior")
        pr1, pr2, pr3 = st.columns(3)
        v_lo = pr1.number_input("Valor venda mín. (R$)", 0.0, step=10_000.0, key=f"{key_prefix}_v_lo", help="0 = sem limite inferior")
        v_hi = pr2.number_input("Valor venda máx. (R$)", 0.0, step=10_000.0, key=f"{key_prefix}_v_hi", help="0 = sem limite superior")
        pr1, pr2, pr3 = st.columns(3)
        a_lo = pr1.number_input("Área m² mín.", 0.0, step=10.0, key=f"{key_prefix}_a_lo", help="0 = sem limite inferior")
        a_hi = pr2.number_input("Área m² máx.", 0.0, step=10.0, key=f"{key_prefix}_a_hi", help="0 = sem limite superior")
    return {
        "cidade": str(cidade),
        "bairro": str(bairro),
        "estado": str(estado),
        "texto_outros": str(outros),
        "pm2_lo": float(pm2_lo),
        "pm2_hi": float(pm2_hi),
        "v_lo": float(v_lo),
        "v_hi": float(v_hi),
        "a_lo": float(a_lo),
        "a_hi": float(a_hi),
    }


def _task_get(tid: str | None) -> dict[str, Any] | None:
    if not tid:
        return None
    with _TASKS_LOCK:
        t = _TASKS.get(tid)
        return dict(t) if isinstance(t, dict) else None

def _task_update(tid: str, **kw: Any) -> None:
    with _TASKS_LOCK:
        base = _TASKS.get(tid) or {}
        prog = kw.get("progress")
        if isinstance(prog, dict):
            hist = list(base.get("progress_history") or [])
            evt = dict(prog)
            evt["ts"] = time.time()
            hist.append(evt)
            base["progress_history"] = hist[-120:]
        base.update(kw)
        _TASKS[tid] = base

def _task_should_abort(tid: str) -> bool:
    with _TASKS_LOCK:
        return bool((_TASKS.get(tid) or {}).get("abort_requested", False))


# ── Formatação BR ─────────────────────────────────────────────────────────────
def _fmt_seg(v: Any) -> str:
    try:
        s = int(float(v))
    except (TypeError, ValueError):
        return "-"
    mm, ss = divmod(max(0, s), 60)
    hh, mm = divmod(mm, 60)
    return f"{hh:02d}:{mm:02d}:{ss:02d}" if hh else f"{mm:02d}:{ss:02d}"

def _fmt_n(v: Any, d: int = 2) -> str:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "-"
    if d <= 0:
        return f"{int(round(n)):,}".replace(",", ".")
    s = f"{n:,.{d}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_brl(v: Any) -> str:
    return f"R$ {_fmt_n(v, 2)}"

def _fmt_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if col in {"selecionar", "restaurar", "id"}:
            continue
        if pd.api.types.is_bool_dtype(out[col]):
            continue
        if pd.api.types.is_numeric_dtype(out[col]):
            has_dec = bool((out[col].dropna() % 1 != 0).any()) if len(out[col].dropna()) else False
            dec = 2 if has_dec else 0
            out[col] = out[col].map(lambda x, d=dec: "" if pd.isna(x) else _fmt_n(x, d))
    return out

def _to_float(v: Any, d: float = 0.0) -> float:
    try:
        return float(v) if v is not None else float(d)
    except (TypeError, ValueError):
        return float(d)

def _to_float_or_none(raw: str) -> float | None:
    txt = (raw or "").strip().replace(".", "").replace(",", ".")
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None

def _to_int_or_none(raw: str) -> int | None:
    f = _to_float_or_none(raw)
    return int(f) if f is not None else None

def _kpi_card(value: str, label: str, cls: str = "") -> str:
    return f'<div class="kpi-card {cls}"><div class="kpi-value">{value}</div><div class="kpi-label">{label}</div></div>'


def _ca_kpi_row_anuncios_df(df_sel: pd.DataFrame, *, count_label: str = "Anúncios") -> str:
    """KPIs a partir de linhas de anúncios (pool selecionado ou composição do cache). Colunas numéricas brutas."""
    if df_sel is None or df_sel.empty:
        return (
            '<div class="kpi-row">'
            + _kpi_card("0", count_label, "accent")
            + _kpi_card("-", "Mediana R$/m²")
            + _kpi_card("-", "Média R$/m²")
            + _kpi_card("-", "Maior R$/m²")
            + _kpi_card("-", "Menor R$/m²")
            + _kpi_card("-", "Maior venda")
            + _kpi_card("-", "Menor venda")
            + "</div>"
        )
    pm2: list[float] = []
    vendas: list[float] = []
    for _, ar in df_sel.iterrows():
        p = _to_float(ar.get("preco_m2"))
        if p <= 0:
            a_v, v_v = _to_float(ar.get("area_construida_m2")), _to_float(ar.get("valor_venda"))
            p = v_v / a_v if a_v > 0 and v_v > 0 else 0
        if p > 0:
            pm2.append(p)
        vv = _to_float(ar.get("valor_venda"))
        if vv > 0:
            vendas.append(vv)
    n = len(df_sel)
    html = '<div class="kpi-row">'
    html += _kpi_card(_fmt_n(n, 0), count_label, "accent")
    if pm2:
        html += _kpi_card(_fmt_brl(statistics.median(pm2)), "Mediana R$/m²")
        html += _kpi_card(_fmt_brl(statistics.mean(pm2)), "Média R$/m²")
        html += _kpi_card(_fmt_brl(max(pm2)), "Maior R$/m²")
        html += _kpi_card(_fmt_brl(min(pm2)), "Menor R$/m²")
    else:
        html += _kpi_card("-", "Mediana R$/m²")
        html += _kpi_card("-", "Média R$/m²")
        html += _kpi_card("-", "Maior R$/m²")
        html += _kpi_card("-", "Menor R$/m²")
    if vendas:
        html += _kpi_card(_fmt_brl(max(vendas)), "Maior venda")
        html += _kpi_card(_fmt_brl(min(vendas)), "Menor venda")
    else:
        html += _kpi_card("-", "Maior venda")
        html += _kpi_card("-", "Menor venda")
    html += "</div>"
    return html


def _info_item(label: str, value: str) -> str:
    return f'<div class="info-item"><div class="info-label">{label}</div><div class="info-value">{value}</div></div>'


def _badge(text: str, variant: str = "gray") -> str:
    return f'<span class="badge badge-{variant}">{text}</span>'


def _status_badge(status: str) -> str:
    s = (status or "").strip().lower()
    v = "green" if s == "analisado" else "blue" if s in ("novo", "pendente") else "yellow" if s == "coletado" else "gray"
    return _badge(status.title() if status else "-", v)


def _roi_badge(roi: Any) -> str:
    try:
        r = float(roi)
    except (TypeError, ValueError):
        return _badge("-", "gray")
    if r >= 25:
        return _badge(f"{_fmt_n(r, 1)}%", "green")
    if r > 0:
        return _badge(f"{_fmt_n(r, 1)}%", "yellow")
    return _badge(f"{_fmt_n(r, 1)}%", "red")


_SIM_IR_PJ_PCT = 6.7
_SIM_IR_PF_PCT = 15.0


def _sim_roi_leilao_snapshot(
    *,
    lance: float,
    desconto_avista_pct: float,
    venda: float,
    com_imob: float,
    reforma: float,
    registro: float,
    com_lei: float,
    itbi: float,
    itbi_sobre_venda: bool,
    fat_liq: float,
    roi_alvo: float,
    vd_caixa: bool,
    pessoa_juridica: bool,
) -> dict[str, Any]:
    """
    Cenário do simulador: fator de liquidez aplicado sobre a **venda bruta**; em seguida comissão
    imobiliária; ROI bruto (via ``calcular_roi_liquido`` com fator 1 na receita); IR (PJ ou PF);
    lucro e ROI líquidos após IR.
    """
    out: dict[str, Any] = {"ok": False}
    if lance <= 0:
        out["erro"] = "Informe um lance maior que zero."
        return out
    if venda <= 0:
        out["erro"] = "Informe uma venda estimada maior que zero."
        return out
    fl = float(fat_liq)
    if fl <= 0:
        out["erro"] = "Fator de liquidez deve ser maior que zero."
        return out
    v_bruta_aj = round(float(venda) * fl, 2)
    if v_bruta_aj <= 0:
        out["erro"] = "Venda bruta após liquidez deve ser maior que zero."
        return out
    venda_pc = round(v_bruta_aj * (1.0 - float(com_imob) / 100.0), 2)
    if venda_pc <= 0:
        out["erro"] = (
            "A venda após liquidez e comissão da imobiliária precisa ser > 0. "
            "Reduza a comissão ou aumente a venda estimada / fator de liquidez."
        )
        return out
    d_av = max(0.0, min(float(desconto_avista_pct or 0.0), 99.0))
    base_itbi = float(v_bruta_aj) if itbi_sobre_venda else float(lance)
    itbi_monetario = round(base_itbi * float(itbi) / 100.0, 2)
    if itbi_sobre_venda:
        itbi_efetivo_pct = 0.0
        registro_efetivo = round(float(registro) + float(itbi_monetario), 2)
    else:
        itbi_efetivo_pct = float(itbi)
        registro_efetivo = float(registro)
    try:
        ent = RoiCalculoEntrada(
            valor_lance=float(lance),
            valor_venda_estimado=float(venda_pc),
            custo_reforma=float(max(0.0, reforma)),
            comissao_leiloeiro_pct=float(com_lei),
            itbi_pct=itbi_efetivo_pct,
            custos_registro=float(max(0.0, registro_efetivo)),
            fator_liquidez_venda=1.0,
            venda_direta_caixa=bool(vd_caixa),
            desconto_avista_pct=float(d_av),
        )
        res = calcular_roi_liquido(ent)
    except ValueError as e:
        out["erro"] = str(e)
        return out
    inv = float(res.investimento_total)
    lucro_antes_ir = round(float(venda_pc) - inv, 2)
    roi_bruto_pct = round((lucro_antes_ir / inv) * 100.0, 4) if inv > 0 else 0.0
    if pessoa_juridica:
        ir_valor = round(v_bruta_aj * (_SIM_IR_PJ_PCT / 100.0), 2)
        ir_pct = _SIM_IR_PJ_PCT
        ir_base_desc = "venda após liquidez"
    else:
        base_ir_pf = max(0.0, lucro_antes_ir)
        ir_valor = round(base_ir_pf * (_SIM_IR_PF_PCT / 100.0), 2)
        ir_pct = _SIM_IR_PF_PCT
        ir_base_desc = "lucro após comissão imob. (receita − investimento)"
    lucro_liquido = round(lucro_antes_ir - ir_valor, 2)
    roi_liquido_pct = round((lucro_liquido / inv) * 100.0, 4) if inv > 0 else 0.0

    lance_max = 0.0
    try:
        lance_max = float(
            calcular_lance_maximo_para_roi(
                valor_venda_estimado=float(venda_pc),
                roi_objetivo_pct=float(roi_alvo),
                custo_reforma=float(max(0.0, reforma)),
                comissao_leiloeiro_pct=float(com_lei),
                itbi_pct=itbi_efetivo_pct,
                custos_registro=float(max(0.0, registro_efetivo)),
                fator_liquidez_venda=1.0,
                venda_direta_caixa=bool(vd_caixa),
                desconto_avista_pct=float(d_av),
            )
        )
    except ValueError:
        lance_max = 0.0
    out.update(
        ok=True,
        ent=ent,
        res=res,
        lance_max=lance_max,
        venda_bruta=float(venda),
        venda_bruta_ajustada=float(v_bruta_aj),
        venda_pc=float(venda_pc),
        com_imob_pct=float(com_imob),
        comissao_imob_valor=round(float(v_bruta_aj) - float(venda_pc), 2),
        itbi_monetario=float(itbi_monetario),
        itbi_sobre_venda=bool(itbi_sobre_venda),
        registro_informado=float(max(0.0, registro)),
        fat_liq=float(fat_liq),
        pessoa_juridica=bool(pessoa_juridica),
        lucro_antes_ir=float(lucro_antes_ir),
        roi_bruto_pct=float(roi_bruto_pct),
        ir_valor=float(ir_valor),
        ir_pct=float(ir_pct),
        ir_base_desc=str(ir_base_desc),
        lucro_liquido=float(lucro_liquido),
        roi_liquido_pct=float(roi_liquido_pct),
        desconto_avista_pct=float(d_av),
    )
    return out


def _sim_purge_keys_para_imovel(iid: str) -> None:
    """Remove estado de widgets do Simulador para o imóvel, voltando aos defaults da linha do banco."""
    if not iid:
        return
    fixed = (
        f"sl_{iid}",
        f"sv_modo_{iid}",
        f"ref_padrao_{iid}",
        f"srg_pct_{iid}",
        f"scl_{iid}",
        f"sda_{iid}",
        f"sci_{iid}",
        f"si_{iid}",
        f"si_sv_{iid}",
        f"sf_{iid}",
        f"sra_{iid}",
        f"svc_{iid}",
        f"sim_tipo_ir_{iid}",
        f"apply_{iid}",
        f"btn_apply_{iid}",
    )
    for k in list(st.session_state.keys()):
        if k in fixed:
            st.session_state.pop(k, None)
            continue
        if k.startswith(f"sv_{iid}_") or k.startswith(f"sr_{iid}_") or k.startswith(f"srg_{iid}_"):
            st.session_state.pop(k, None)


def _series_to_numeric(s: pd.Series) -> pd.Series:
    if s is None or len(s) == 0:
        return pd.Series(dtype=float)
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    return pd.to_numeric(
        s.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^\d.\-]", "", regex=True),
        errors="coerce",
    )


def _parse_preco_br(raw: Any) -> float:
    """Converts 'R$ 550.000' or 'R$ 1.200,50' or numeric to float."""
    import re as _re
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw or "").strip()
    s = s.replace("R$", "").replace("r$", "").strip()
    if not s or s.lower() in ("não informado", "n/a", "-", ""):
        return 0.0
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif "." in s:
        # "550.000" -> dot is thousands separator (3 digits after dot, no decimals)
        # "550.5"   -> dot is decimal separator (fewer than 3 digits after dot)
        parts = s.split(".")
        if all(_re.fullmatch(r"\d{3}", p) for p in parts[1:]):
            s = s.replace(".", "")
        elif len(parts) > 2:
            s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        nums = _re.sub(r"[^\d.]", "", s)
        return float(nums) if nums else 0.0

def _parse_area_br(raw: Any) -> float:
    """Converts '113 m²', '650 m²', '113,5' ou numérico para float (m²)."""
    if isinstance(raw, (int, float)):
        v = float(raw)
        return v if v > 0 else 0.0
    s0 = str(raw or "").strip()
    low = s0.lower()
    if not s0 or low in ("não informado", "nao informado", "n/a", "-", "—", "s/n"):
        return 0.0

    s = unicodedata.normalize("NFKC", s0)
    s = s.replace("\u00a0", " ").replace("\u202f", " ").strip().lower()
    s = s.replace("m²", "").replace("m2", "").replace("m^2", "")
    s = s.replace("metros quadrados", "").replace("metro quadrado", "").replace("metros", "").replace("mq", "")
    s = s.replace(",", ".").strip()
    try:
        v = float(s)
        return v if v > 0 else 0.0
    except ValueError:
        m = re.search(r"(\d+(?:[.,]\d+)?)", s0)
        if m:
            try:
                v = float(m.group(1).replace(",", "."))
                return v if v > 0 else 0.0
            except ValueError:
                pass
        return 0.0


def _parse_optional_coord(raw: Any) -> float | None:
    try:
        if raw is None:
            return None
        if isinstance(raw, str) and not raw.strip():
            return None
        v = float(raw)
        if pd.isna(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def _enriquecer_anuncio_geolocation(row: dict[str, Any]) -> dict[str, Any]:
    """Garante latitude/longitude: mantém coordenadas válidas ou geocodifica (Nominatim) via geocoding."""
    from geocoding import geocodificar_anuncios_batch

    d = dict(row)
    gl = _geo_lat_lon_ok(d.get("latitude"), d.get("longitude"))
    if gl:
        d["latitude"], d["longitude"] = gl[0], gl[1]
        return d
    geocodificar_anuncios_batch([d])
    return d


def _json_anuncio_to_row(item: dict[str, Any]) -> dict[str, Any]:
    """Converts a user-provided JSON anúncio dict to DB-ready dict."""
    url = str(item.get("link_do_anuncio") or item.get("url_anuncio") or item.get("link") or item.get("url") or "").strip()
    preco = _parse_preco_br(item.get("preco") or item.get("valor_venda") or item.get("valor") or 0)
    _raw_area = (
        item.get("tamanho_imovel")
        or item.get("area_construida_m2")
        or item.get("area_util")
        or item.get("area_total")
        or item.get("area")
        or 0
    )
    area = _parse_area_br(_raw_area)
    tipo_raw = str(item.get("tipo") or item.get("tipo_imovel") or "").strip()
    tipo = normalizar_tipo_imovel(tipo_raw)
    cidade = str(item.get("cidade") or "").strip()
    estado = str(item.get("estado") or item.get("uf") or "").strip().upper()[:2]
    bairro = str(item.get("bairro") or "").strip()
    logradouro = str(item.get("endereco") or item.get("logradouro") or "").strip()
    titulo = str(item.get("titulo") or item.get("title") or item.get("titulo_anuncio") or "").strip()
    quartos = item.get("quartos")
    if quartos is not None:
        try:
            quartos = int(quartos)
        except (TypeError, ValueError):
            quartos = None

    metadados: dict[str, Any] = {}
    for extra_key in ("banheiros", "vagas", "condominio", "iptu", "condominium", "parking"):
        if item.get(extra_key) is not None:
            metadados[extra_key] = item[extra_key]
    metadados["tipo_original"] = tipo_raw
    if titulo:
        metadados["titulo_importacao"] = titulo

    out: dict[str, Any] = {
        "url_anuncio": url,
        "portal": portal_de_url(url) if url.startswith("http") else "manual",
        "tipo_imovel": tipo,
        "logradouro": logradouro,
        "bairro": bairro,
        "cidade": cidade,
        "estado": estado,
        "area_construida_m2": area,
        "valor_venda": preco,
        "quartos": quartos,
        "preco_m2": round(preco / area, 2) if area > 0 and preco > 0 else None,
        "metadados_json": metadados,
        "transacao": "venda",
        "titulo": titulo,
        "_valid": bool(url and preco > 0 and area > 0 and cidade and bairro),
        "_validation_errors": [],
    }
    la = _parse_optional_coord(item.get("latitude") or item.get("lat"))
    lo = _parse_optional_coord(item.get("longitude") or item.get("lng") or item.get("lon"))
    if la is not None and lo is not None:
        out["latitude"] = la
        out["longitude"] = lo
    return out


# ── Queries ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def _query_table(table: str, limit: int = 300) -> pd.DataFrame:
    cli = get_supabase_client()
    rows = cli.table(table).select("*").order("created_at", desc=True).limit(limit).execute().data or []
    return pd.DataFrame(rows)

def _anuncios_soft_delete_ok(force: bool = False) -> bool:
    global _ANUNCIOS_SOFT_DELETE_CACHE
    if _ANUNCIOS_SOFT_DELETE_CACHE is not None and not force:
        return bool(_ANUNCIOS_SOFT_DELETE_CACHE)
    cli = get_supabase_client()
    try:
        cli.table(TABLE_ANUNCIOS_MERCADO).select("id,arquivado_em").limit(1).execute()
        _ANUNCIOS_SOFT_DELETE_CACHE = True
    except Exception:
        _ANUNCIOS_SOFT_DELETE_CACHE = False
    return bool(_ANUNCIOS_SOFT_DELETE_CACHE)

@st.cache_data(ttl=30)
def _query_anuncios(limit: int = 300, *, include_arq: bool = False, only_arq: bool = False) -> pd.DataFrame:
    cli = get_supabase_client()
    q = cli.table(TABLE_ANUNCIOS_MERCADO).select("*")
    soft_ok = _anuncios_soft_delete_ok()
    if soft_ok:
        if only_arq:
            q = q.not_.is_("arquivado_em", "null")
        elif not include_arq:
            q = q.is_("arquivado_em", "null")
    rows = q.order("ultima_coleta_em", desc=True).limit(limit).execute().data or []
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def _query_cache_bairro_all() -> pd.DataFrame:
    """Fetches all cache entries (lightweight table, usually <500 rows)."""
    cli = get_supabase_client()
    try:
        rows = cli.table(CACHE_TABLE).select("*").order("atualizado_em", desc=True).limit(2000).execute().data or []
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame(rows)

def _normalizar_texto(t: str) -> str:
    """Lowercase, strip accents, collapse whitespace for fuzzy comparison."""
    import unicodedata
    s = unicodedata.normalize("NFKD", str(t or "").strip().lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace(".", " ").replace("-", " ").replace("_", " ")
    return " ".join(s.split())

def _similaridade_bairro(a: str, b: str) -> float:
    """Returns 0.0-1.0 similarity score between two neighbourhood names."""
    from difflib import SequenceMatcher
    na, nb = _normalizar_texto(a), _normalizar_texto(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.9
    return SequenceMatcher(None, na, nb).ratio()

def _buscar_cache_para_imovel(cidade: str, bairro: str, estado: str, limiar_bairro: float = 0.5) -> pd.DataFrame:
    """Searches cache with fuzzy matching on city/neighbourhood names.
    Returns matched rows sorted by relevance (exact bairro > similar bairro > same city)."""
    df_all = _query_cache_bairro_all()
    if df_all.empty:
        return df_all

    cidade_n = _normalizar_texto(cidade)
    estado_n = _normalizar_texto(estado)
    bairro_n = _normalizar_texto(bairro)

    scores: list[float] = []
    for _, r in df_all.iterrows():
        c = _normalizar_texto(str(r.get("cidade") or ""))
        e = _normalizar_texto(str(r.get("estado") or ""))
        b = _normalizar_texto(str(r.get("bairro") or ""))

        if estado_n and e and estado_n != e:
            scores.append(-1.0)
            continue

        city_match = _similaridade_bairro(cidade_n, c)
        if city_match < 0.7:
            scores.append(-1.0)
            continue

        if bairro_n and b:
            bairro_score = _similaridade_bairro(bairro_n, b)
        else:
            bairro_score = 0.0

        scores.append(bairro_score)

    df_all = df_all.copy()
    df_all["_relevancia"] = scores
    df_matched = df_all[df_all["_relevancia"] >= 0].copy()
    if df_matched.empty:
        return pd.DataFrame()

    df_matched = df_matched.sort_values("_relevancia", ascending=False)
    return df_matched


def _fetch_cache_row_by_id(cache_id: str) -> pd.DataFrame:
    """Uma linha de ``cache_media_bairro`` por UUID (fora do cache Streamlit para linhas fora do top 2000)."""
    cid = str(cache_id or "").strip()
    if not cid or cid.lower() == "nan":
        return pd.DataFrame()
    cli = get_supabase_client()
    try:
        rows = cli.table(CACHE_TABLE).select("*").eq("id", cid).limit(1).execute().data or []
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _df_cache_para_linha_leilao(row: pd.Series) -> pd.DataFrame:
    """Prioriza o cache vinculado em ``leilao_imoveis.cache_media_bairro_id``; senão busca por cidade/bairro."""
    cidade = _safe_str(row.get("cidade"))
    bairro = _safe_str(row.get("bairro"))
    estado = _safe_str(row.get("estado"))
    raw_lid = row.get("cache_media_bairro_id")
    lid_s = str(raw_lid).strip() if raw_lid is not None and str(raw_lid).strip() and str(raw_lid).lower() != "nan" else ""
    df_fb = _buscar_cache_para_imovel(cidade, bairro, estado)
    if not lid_s:
        return df_fb
    df_link = _fetch_cache_row_by_id(lid_s)
    if df_link.empty:
        return df_fb
    linked = df_link.iloc[0:1].copy()
    linked["_relevancia"] = 1.0
    if df_fb.empty:
        return linked
    rest = df_fb[df_fb["id"].astype(str) != lid_s].copy()
    return pd.concat([linked, rest], ignore_index=True)


# ── Distinct values for cascading filters ─────────────────────────────────────
@st.cache_data(ttl=60)
def _distinct_col(table: str, col: str) -> list[str]:
    cli = get_supabase_client()
    try:
        rows = cli.table(table).select(col).limit(2000).execute().data or []
    except Exception:
        return []
    vals = sorted({str(r.get(col) or "").strip() for r in rows if str(r.get(col) or "").strip()})
    return vals

@st.cache_data(ttl=60)
def _distinct_col_filtered(table: str, col: str, filters: dict[str, str]) -> list[str]:
    cli = get_supabase_client()
    try:
        q = cli.table(table).select(col).limit(2000)
        for k, v in filters.items():
            if v:
                q = q.eq(k, v)
        rows = q.execute().data or []
    except Exception:
        return []
    vals = sorted({str(r.get(col) or "").strip() for r in rows if str(r.get(col) or "").strip()})
    return vals


# ── IBGE API (estados + cidades) ──────────────────────────────────────────────
_IBGE_BASE = "https://servicodados.ibge.gov.br/api/v1/localidades"

@st.cache_data(ttl=3600)
def _ibge_estados() -> list[dict[str, str]]:
    """Returns [{"sigla": "RS", "nome": "Rio Grande do Sul"}, ...]"""
    try:
        r = requests.get(f"{_IBGE_BASE}/estados?orderBy=nome", timeout=8)
        r.raise_for_status()
        return [{"sigla": e["sigla"], "nome": e["nome"]} for e in r.json()]
    except Exception:
        return []

@st.cache_data(ttl=3600)
def _ibge_cidades(uf: str) -> list[str]:
    """Returns sorted city names for a given UF (e.g. 'RS')."""
    if not uf:
        return []
    try:
        r = requests.get(f"{_IBGE_BASE}/estados/{uf}/municipios?orderBy=nome", timeout=8)
        r.raise_for_status()
        return [m["nome"] for m in r.json()]
    except Exception:
        return []

@st.cache_data(ttl=3600)
def _bairros_viacep(uf: str, cidade: str) -> list[str]:
    """Extracts unique bairro names from ViaCEP by querying common street prefixes.
    100% free, no credits. Returns sorted list of neighborhood names."""
    if not uf or not cidade:
        return []
    import unicodedata
    cidade_ascii = unicodedata.normalize("NFD", cidade).encode("ascii", "ignore").decode()
    bairros: set[str] = set()
    for termo in ("rua", "avenida", "travessa", "alameda", "praca", "largo"):
        try:
            r = requests.get(
                f"https://viacep.com.br/ws/{uf}/{cidade_ascii}/{termo}/json/",
                timeout=6,
            )
            if r.status_code != 200:
                continue
            data = r.json()
            if isinstance(data, list):
                for item in data:
                    b = (item.get("bairro") or "").strip()
                    if b:
                        bairros.add(b)
        except Exception:
            continue
    return sorted(bairros)


@st.cache_data(ttl=7200, show_spinner="Buscando bairros no VivaReal...")
def _bairros_vivareal_cached(uf: str, cidade: str) -> list[tuple[str, str]]:
    """Retorna lista de (slug, nome_humanizado) de bairros disponíveis no VivaReal.
    Custo: 1 crédito Firecrawl na primeira chamada; cacheado por 2h."""
    if not uf or not cidade:
        return []
    try:
        slugs = descobrir_bairros_vivareal(uf, cidade)
    except Exception:
        return []
    return [(s, slug_bairro_para_nome(s)) for s in sorted(slugs)]


# ── CRUD anúncios ─────────────────────────────────────────────────────────────
def _delete_anuncios(ids: list[str]) -> int:
    if not ids:
        return 0
    cli = get_supabase_client()
    return len((cli.table(TABLE_ANUNCIOS_MERCADO).delete().in_("id", ids).execute()).data or [])

def _arquivar_anuncios(ids: list[str], motivo: str = "frontend_manual") -> int:
    if not ids:
        return 0
    cli = get_supabase_client()
    payload = {"arquivado_em": datetime.now(timezone.utc).isoformat(), "arquivado_motivo": motivo[:120]}
    return len((cli.table(TABLE_ANUNCIOS_MERCADO).update(payload).in_("id", ids).execute()).data or [])

def _restaurar_anuncios(ids: list[str]) -> int:
    if not ids:
        return 0
    cli = get_supabase_client()
    return len((cli.table(TABLE_ANUNCIOS_MERCADO).update({"arquivado_em": None, "arquivado_motivo": None}).in_("id", ids).execute()).data or [])

def _safe_str(v: Any, default: str = "") -> str:
    """Converte valor (inclusive NaN do pandas) para string limpa."""
    if v is None:
        return default
    s = str(v).strip()
    if s.lower() in ("nan", "none", ""):
        return default
    return s


def _inserir_cache_novo(
    cidade: str, bairro: str, estado: str, tipo_imovel: str,
    preco_m2_medio: float, fonte: str = "manual",
    metadados_json: str | None = None,
    *,
    valor_medio_venda: float | None = None,
    maior_valor_venda: float | None = None,
    menor_valor_venda: float | None = None,
    n_amostras: int | None = None,
    anuncios_ids: str | None = None,
    nome_cache: str | None = None,
    client: Any = None,
) -> dict[str, Any]:
    """INSERT direto no cache_media_bairro com chave única (permite duplicatas de bairro)."""
    from postgrest.exceptions import APIError

    cidade = _safe_str(cidade)
    bairro = _safe_str(bairro)
    estado = _safe_str(estado).upper()
    tipo_imovel = _safe_str(tipo_imovel, "desconhecido")

    uid = uuid.uuid4().hex[:8]
    chave_geo = normalizar_chave_bairro(cidade, bairro, estado)
    seg = {"tipo_imovel": tipo_imovel, "conservacao": "desconhecido",
           "tipo_casa": "-", "faixa_andar": "-", "faixa_area": "-", "logradouro_chave": "-"}
    chave_seg = f"{normalizar_chave_segmento(chave_geo, seg)}|uid={uid}"
    bairro_grava = bairro or estado or "geral"
    nome_c = _safe_str(nome_cache).strip()
    if not nome_c:
        nome_c = nome_cache_automatico(cidade, bairro_grava, estado, tipo_imovel)
    nome_c = nome_c[:240]

    row_full: dict[str, Any] = {
        "chave_bairro": f"{chave_geo}|uid={uid}",
        "chave_segmento": chave_seg,
        "cidade": cidade,
        "bairro": bairro_grava,
        "estado": estado or None,
        "tipo_imovel": tipo_imovel,
        "conservacao": "desconhecido",
        "tipo_casa": "-",
        "faixa_andar": "-",
        "logradouro_chave": "-",
        "preco_m2_medio": round(float(preco_m2_medio), 2),
        "fonte": _safe_str(fonte, "manual"),
        "nome_cache": nome_c,
    }
    if not row_full["estado"]:
        row_full.pop("estado", None)
    if metadados_json is not None:
        row_full["metadados_json"] = metadados_json
    if valor_medio_venda is not None:
        row_full["valor_medio_venda"] = round(float(valor_medio_venda), 2)
    if maior_valor_venda is not None:
        row_full["maior_valor_venda"] = round(float(maior_valor_venda), 2)
    if menor_valor_venda is not None:
        row_full["menor_valor_venda"] = round(float(menor_valor_venda), 2)
    if n_amostras is not None:
        row_full["n_amostras"] = int(n_amostras)
    if anuncios_ids:
        row_full["anuncios_ids"] = str(anuncios_ids)[:4990]

    cli = client or get_supabase_client()

    try:
        result = cli.table(CACHE_TABLE).insert(row_full).execute()
    except APIError:
        row_min: dict[str, Any] = {
            "chave_bairro": f"{chave_geo}|uid={uid}",
            "cidade": cidade,
            "bairro": bairro_grava,
            "preco_m2_medio": round(float(preco_m2_medio), 2),
            "fonte": _safe_str(fonte, "manual"),
            "nome_cache": nome_c,
        }
        if estado:
            row_min["estado"] = estado
        if tipo_imovel and tipo_imovel != "desconhecido":
            row_min["tipo_imovel"] = tipo_imovel
        if metadados_json is not None:
            row_min["metadados_json"] = metadados_json
        for k in ("valor_medio_venda", "maior_valor_venda", "menor_valor_venda", "n_amostras", "anuncios_ids"):
            if k in row_full:
                row_min[k] = row_full[k]
        result = cli.table(CACHE_TABLE).insert(row_min).execute()

    return {"ok": True, "chave_bairro": row_full.get("chave_bairro", ""), "preco_m2_medio": preco_m2_medio, "data": result.data}


def _recalcular_cache(ids: list[str], nome_cache_usuario: str | None = None) -> dict[str, Any]:
    if not ids:
        return {"grupos": 0, "cache_atualizado": 0}
    cli = get_supabase_client()
    rows = cli.table(TABLE_ANUNCIOS_MERCADO).select("*").in_("id", ids).limit(2000).execute().data or []
    grupos: dict[tuple, list] = {}
    for r in rows:
        cidade = _safe_str(r.get("cidade"))
        bairro = _safe_str(r.get("bairro"))
        estado = _safe_str(r.get("estado"))
        tipo = normalizar_tipo_imovel(r.get("tipo_imovel"))
        if not cidade or not bairro:
            continue
        grupos.setdefault((cidade, bairro, estado, tipo), []).append(r)
    base_uc = (nome_cache_usuario or "").strip()
    pend: list[tuple[str, str, str, str, float, list[float], list[float], list[str]]] = []
    det: list[dict] = []
    for (cidade, bairro, estado, tipo), itens in grupos.items():
        pm2: list[float] = []
        valores_venda: list[float] = []
        for it in itens:
            p = _to_float(it.get("preco_m2"))
            v_venda = _to_float(it.get("valor_venda"))
            if p <= 0:
                a = _to_float(it.get("area_construida_m2"))
                p = v_venda / a if a > 0 and v_venda > 0 else 0
            if p > 0:
                pm2.append(p)
            if v_venda > 0:
                valores_venda.append(v_venda)
        if not pm2:
            det.append({"cidade": cidade, "bairro": bairro, "tipo": tipo, "status": "sem_pm2"})
            continue
        med = float(statistics.median(pm2))
        ids_grupo = [str(x.get("id") or "").strip() for x in itens if x.get("id")]
        ids_grupo = list(dict.fromkeys(i for i in ids_grupo if i))
        pend.append((cidade, bairro, estado, tipo, med, pm2, valores_venda, ids_grupo))

    n_ok = len(pend)
    atualiz = 0
    for i, (cidade, bairro, estado, tipo, med, pm2, valores_venda, ids_grupo) in enumerate(pend):
        if base_uc:
            nm = base_uc if n_ok == 1 else f"{base_uc} ({i + 1})"[:240]
        else:
            nm = nome_cache_automatico(cidade, bairro, estado, tipo)
        _inserir_cache_novo(
            cidade=cidade, bairro=bairro, estado=estado, tipo_imovel=tipo,
            preco_m2_medio=round(med, 2), fonte="frontend_recalculo",
            metadados_json=json.dumps({"n_amostras": len(pm2), "origem": "frontend"}, ensure_ascii=False),
            valor_medio_venda=statistics.mean(valores_venda) if valores_venda else None,
            maior_valor_venda=max(valores_venda) if valores_venda else None,
            menor_valor_venda=min(valores_venda) if valores_venda else None,
            n_amostras=len(pm2),
            anuncios_ids=",".join(ids_grupo) if ids_grupo else None,
            nome_cache=nm,
            client=cli,
        )
        atualiz += 1
        det.append({"cidade": cidade, "bairro": bairro, "tipo": tipo, "status": "ok", "amostras": len(pm2), "pm2_mediana": round(med, 2), "nome_cache": nm})
    return {"grupos": len(grupos), "cache_atualizado": atualiz, "detalhes": det}


def _parse_anuncios_ids_field(raw: Any) -> list[str]:
    s = _safe_str(raw, "")
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return list(dict.fromkeys(p for p in parts if p))


def _fetch_anuncios_rows(ids: list[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    cli = get_supabase_client()
    out: list[dict[str, Any]] = []
    chunk = 80
    for i in range(0, len(ids), chunk):
        part = ids[i : i + chunk]
        try:
            data = cli.table(TABLE_ANUNCIOS_MERCADO).select("*").in_("id", part).limit(2000).execute().data or []
            out.extend(data)
        except Exception:
            continue
    return out


def _geo_lat_lon_ok(lat: Any, lon: Any) -> tuple[float, float] | None:
    """Retorna (lat, lon) se coordenadas parecem válidas; caso contrário ``None``."""
    try:
        la = float(lat)
        lo = float(lon)
    except (TypeError, ValueError):
        return None
    if pd.isna(lat) or pd.isna(lon):
        return None
    if abs(la) > 90 or abs(lo) > 180:
        return None
    if abs(la) < 1e-9 and abs(lo) < 1e-9:
        return None
    return la, lo


def _ca_bootstrap_nav_from_map_query() -> None:
    """Se a URL traz marcação vinda do mapa, abre a aba Cache (reload pode resetar o rádio)."""
    try:
        qp = st.query_params
        if "ca_pool_map_sel" in qp or ("ca_comp_map_sel" in qp and "ca_comp_cid" in qp):
            st.session_state["nav_page"] = "🗄️ Cache"
    except Exception:
        pass


def _lei_popup_link(title: str, url: str) -> str:
    u = (url or "").strip()
    t = html.escape((title or "").strip() or "Abrir", quote=False)
    if u.lower().startswith(("http://", "https://")):
        ue = html.escape(u, quote=True)
        return f'<div style="min-width:160px"><strong>{t}</strong><br/><a href="{ue}" target="_blank" rel="noopener noreferrer">Abrir link</a></div>'
    return f"<div><strong>{t}</strong><br/><span>Sem URL</span></div>"


def _lei_popup_cache_map_select_html(title: str, url: str, anuncio_id: str) -> str:
    """Popup na aba Cache: sem sair do iframe; o id segue no texto lido pelo ``st_folium`` ao clicar no pino."""
    aid = str(anuncio_id or "").strip()
    base = _lei_popup_link(title, url)
    if not aid:
        return base
    instr = (
        '<p style="margin:10px 0 0 0;border-top:1px solid rgba(0,0,0,.12);padding-top:8px;'
        'font-size:12px;color:#333;line-height:1.4">'
        "<strong>Selecionar na tabela:</strong> <strong>clique no círculo verde</strong> deste imóvel no mapa "
        "(o id é enviado ao Streamlit no clique no pino, não por link no iframe).</p>"
    )
    # Texto mínimo incluído em innerText do popup (streamlit-folium); evite display:none/clip que some do innerText.
    hidden = (
        f'<span style="font-size:1px;line-height:1px;color:transparent;display:block">'
        f"[[ANMAP:{html.escape(aid)}]]</span>"
    )
    if base.rstrip().endswith("</div>"):
        return base.rstrip()[:-6] + instr + hidden + "</div>"
    return base + instr + hidden


def _folium_out_has_marker_click(out: dict[str, Any] | None) -> bool:
    """True se o retorno do mapa traz ``last_object_clicked`` com lat/lng."""
    if not out or not isinstance(out, dict):
        return False
    loc = out.get("last_object_clicked")
    if loc is None:
        return False
    if isinstance(loc, dict):
        try:
            la = loc.get("lat")
            lo = loc.get("lng")
            if la is None or lo is None:
                return False
            float(la)
            float(lo)
            return True
        except (TypeError, ValueError):
            return False
    return True


def _lei_popup_anuncio_pool_html(
    title: str,
    url: str,
    anuncio_id: str,
    *,
    query_param: str = "an_map_sel",
    extra_query: dict[str, str] | None = None,
    embed_hidden_map_token: bool = True,
) -> str:
    """Popup aba Anúncios: botão que altera query na janela principal + token oculto para clique no pino."""
    aid = str(anuncio_id or "").strip()
    base = _lei_popup_link(title, url)
    if not aid:
        return base
    id_js = json.dumps(aid)
    qp_parts = [f"u.searchParams.set({json.dumps(query_param)},{id_js});"]
    for ek, ev in (extra_query or {}).items():
        qp_parts.append(f"u.searchParams.set({json.dumps(ek)},{json.dumps(str(ev))});")
    qp_parts.append('u.searchParams.set("_ca_map_clk",String(Date.now()));')
    js = (
        "try{var t=window.top||window;var u=new URL(t.location.href);"
        + "".join(qp_parts)
        + "t.location.assign(u.toString());}catch(e){}return false;"
    )
    esc_onclick = html.escape(js, quote=True)
    mark_block = (
        '<p style="margin:10px 0 0 0;border-top:1px solid rgba(0,0,0,.12);padding-top:8px">'
        f'<button type="button" onclick="{esc_onclick}" '
        'style="font:inherit;font-weight:600;color:#1e8449;background:none;border:none;'
        'padding:0;margin:0;cursor:pointer;text-decoration:underline">Marcar na tabela</button>'
        '<span style="display:block;font-size:11px;color:#666;margin-top:4px">'
        "Seleciona este imóvel na lista abaixo.</span></p>"
    )
    hidden = ""
    if embed_hidden_map_token:
        hidden = (
            f'<span style="font-size:1px;line-height:1px;color:transparent;display:block">'
            f"[[ANMAP:{html.escape(aid)}]]</span>"
        )
    if base.rstrip().endswith("</div>"):
        return base.rstrip()[:-6] + mark_block + hidden + "</div>"
    return base + mark_block + hidden


def _merge_ids_from_map_popup(
    out: dict[str, Any] | None,
    *,
    ids_session_key: str,
    seen_session_key: str,
    ids_as_dict_cid: str | None = None,
    require_marker_click: bool = False,
) -> None:
    """Extrai ``[[ANMAP:id]]`` do popup do st_folium e acumula IDs (set global ou dict[cid]→set)."""
    if not out:
        return
    if require_marker_click and not _folium_out_has_marker_click(out):
        return
    pop = out.get("last_object_clicked_popup")
    if pop is None or not str(pop).strip():
        return
    cur = str(pop)
    prev = st.session_state.get(seen_session_key)
    if cur == prev:
        return
    st.session_state[seen_session_key] = cur
    for m in re.finditer(r"\[\[ANMAP:([^\]]+)\]\]", cur):
        aid = (m.group(1) or "").strip()
        if not aid:
            continue
        if ids_as_dict_cid:
            root = st.session_state.setdefault(ids_session_key, {})
            if not isinstance(root, dict):
                root = {}
                st.session_state[ids_session_key] = root
            s = root.setdefault(ids_as_dict_cid, set())
            if not isinstance(s, set):
                s = set(s) if s else set()
                root[ids_as_dict_cid] = s
            s.add(aid)
        else:
            st.session_state.setdefault(ids_session_key, set()).add(aid)


def _an_merge_ids_from_map_popup(out: dict[str, Any] | None) -> None:
    _merge_ids_from_map_popup(
        out,
        ids_session_key="an_ids_from_map",
        seen_session_key="_an_folium_popup_seen",
    )


def _an_merge_map_buffer_into_persist(valid_ids: set[str]) -> bool:
    """Une ``an_ids_from_map`` à persistência. Retorna True se entrou id novo do mapa neste rerun."""
    cur = {str(x) for x in (st.session_state.get(_AN_SS_SEL_IDS) or []) if str(x) in valid_ids}
    raw = st.session_state.get("an_ids_from_map")
    merged = False
    if raw:
        s = raw if isinstance(raw, set) else set(raw)
        add = {str(x) for x in s if str(x) in valid_ids}
        if add:
            merged = True
        cur |= add
        st.session_state["an_ids_from_map"] = set()
    st.session_state[_AN_SS_SEL_IDS] = sorted(cur)
    return merged


def _an_sync_dataframe_selection_from_persist(df: pd.DataFrame, tbl_key: str) -> None:
    """Alinha ``st.dataframe`` (multi-linha) à persistência — evita desencontro com o mapa / ``data_editor``."""
    if df.empty or "id" not in df.columns:
        return
    ps = {str(x) for x in (st.session_state.get(_AN_SS_SEL_IDS) or [])}
    row_ix = [i for i, rid in enumerate(df["id"].astype(str)) if str(rid) in ps]
    st.session_state[tbl_key] = _ca_widget_sel_state(row_ix)


def _an_consume_map_sel_query_param() -> None:
    """Lê ?an_map_sel= do link \"Marcar na tabela\" (janela principal) e acumula o id."""
    try:
        qp = st.query_params
        if "an_map_sel" not in qp:
            return
        raw = qp.get("an_map_sel")
        if isinstance(raw, (list, tuple)):
            raw = raw[0] if raw else ""
        aid = str(raw or "").strip()
        if aid:
            st.session_state.setdefault("an_ids_from_map", set()).add(aid)
        qp.pop("an_map_sel", None)
        if "_ca_map_clk" in qp:
            qp.pop("_ca_map_clk", None)
    except Exception:
        pass


def _ca_consume_map_sel_query_params() -> None:
    """Query params dos mapas da aba Cache (marcar na tabela via link no popup)."""
    try:
        qp = st.query_params
        if "ca_pool_map_sel" in qp:
            raw = qp.get("ca_pool_map_sel")
            if isinstance(raw, (list, tuple)):
                raw = raw[0] if raw else ""
            aid = str(raw or "").strip()
            if aid:
                st.session_state.setdefault(_CA_SS_POOL_MAP_IDS, set()).add(aid)
            qp.pop("ca_pool_map_sel", None)
        if "ca_comp_map_sel" in qp and "ca_comp_cid" in qp:
            raw_a = qp.get("ca_comp_map_sel")
            raw_c = qp.get("ca_comp_cid")
            if isinstance(raw_a, (list, tuple)):
                raw_a = raw_a[0] if raw_a else ""
            if isinstance(raw_c, (list, tuple)):
                raw_c = raw_c[0] if raw_c else ""
            aid = str(raw_a or "").strip()
            cid = str(raw_c or "").strip()
            if aid and cid:
                root = st.session_state.setdefault(_CA_SS_COMP_MAP_IDS, {})
                if not isinstance(root, dict):
                    root = {}
                    st.session_state[_CA_SS_COMP_MAP_IDS] = root
                root.setdefault(cid, set()).add(aid)
            qp.pop("ca_comp_map_sel", None)
            qp.pop("ca_comp_cid", None)
        if "_ca_map_clk" in qp:
            qp.pop("_ca_map_clk", None)
    except Exception:
        pass


def _ca_apply_pool_map_ids_to_selection(df_an_disp: pd.DataFrame, an_tbl_key: str) -> None:
    """Une IDs vindos do mapa do pool à seleção persistida e ao widget da tabela do pool."""
    raw = st.session_state.get(_CA_SS_POOL_MAP_IDS)
    if not raw or df_an_disp.empty or "id" not in df_an_disp.columns:
        return
    s = raw if isinstance(raw, set) else set(raw)
    if not s:
        return
    cur = {str(x) for x in (st.session_state.get(_CA_SS_SEL_POOL_ROWS) or [])}
    cur |= s
    _ca_set_pool_persist_selection_ids(sorted(cur))
    row_ix = [i for i, rid in enumerate(df_an_disp["id"].astype(str)) if str(rid) in cur]
    if row_ix:
        st.session_state[an_tbl_key] = _ca_widget_sel_state(row_ix)
    # Evita re-aplicar os mesmos IDs a cada rerun.
    st.session_state[_CA_SS_POOL_MAP_IDS] = set()


def _ca_apply_comp_map_ids_to_selection(cid: str, df_view: pd.DataFrame, df_key: str) -> None:
    """Aplica cliques do mapa da composição à seleção do ``st.dataframe`` da composição."""
    root = st.session_state.get(_CA_SS_COMP_MAP_IDS)
    if not isinstance(root, dict) or df_view.empty or "id" not in df_view.columns:
        return
    pending = root.get(cid)
    if not pending:
        return
    want = {str(x) for x in (pending if isinstance(pending, set) else set(pending))}
    if not want:
        return
    id_series = df_view["id"].astype(str)
    new_rows = [i for i, rid in enumerate(id_series) if rid in want]
    if not new_rows:
        root[cid] = set()
        st.session_state[_CA_SS_COMP_MAP_IDS] = root
        return
    existing = st.session_state.get(df_key)
    cur_rows: list[int] = []
    if isinstance(existing, dict):
        cur_rows = [int(x) for x in (existing.get("selection") or {}).get("rows") or []]
    merged = sorted(set(cur_rows) | set(new_rows))
    st.session_state[df_key] = _ca_widget_sel_state(merged)
    root[cid] = set()
    st.session_state[_CA_SS_COMP_MAP_IDS] = root


def _lei_map_hover_tooltip_text(*, tipo: str, area_m2: float, valor_venda: float) -> str:
    """Uma linha para tooltip ao passar o mouse: tipo · área · valor de venda."""
    t = (_safe_str(tipo, "") or "").replace("_", " ").strip().title()
    if not t:
        t = "-"
    if area_m2 > 0:
        a = f"{_fmt_n(area_m2, 1)} m²"
    else:
        a = "-"
    v = _fmt_brl(valor_venda) if valor_venda > 0 else "-"
    return f"{t} · {a} · {v}"


def _pool_coord_key(lat: float, lon: float, ndigits: int = 5) -> tuple[float, float]:
    """Arredonda lat/lon para agrupar imóveis no mesmo ponto geocodificado."""
    return (round(float(lat), ndigits), round(float(lon), ndigits))


def _pool_group_pins_by_coord(
    pins: list[dict[str, Any]], *, ndigits: int = 5
) -> dict[tuple[float, float], list[dict[str, Any]]]:
    from collections import defaultdict

    g = defaultdict(list)
    for p in pins:
        k = _pool_coord_key(p["lat"], p["lon"], ndigits=ndigits)
        g[k].append(p)
    return dict(g)


def _pool_spider_offsets_m(n: int, lat0: float, *, base_m: float = 14.0) -> list[tuple[float, float]]:
    """Deslocamentos (dlat, dlon) em graus para espalhar n pinos em espiral ao redor do centro."""
    if n <= 0:
        return []
    cos_lat = max(0.25, math.cos(math.radians(lat0)))
    out: list[tuple[float, float]] = []
    for k in range(n):
        ang = 2 * math.pi * k / max(n, 1) + 0.12 * k
        r_m = base_m * (1.0 + 0.22 * (k // max(1, n // 4)))
        dx_m = r_m * math.cos(ang)
        dy_m = r_m * math.sin(ang)
        dlat = dy_m / 111_320.0
        dlon = dx_m / (111_320.0 * cos_lat)
        out.append((dlat, dlon))
    return out


def _anuncio_row_geocode_pin(rec: dict[str, Any] | pd.Series) -> dict[str, Any] | None:
    """Monta um pino de mapa (anúncio) se houver latitude/longitude válidas."""
    d = rec.to_dict() if isinstance(rec, pd.Series) else dict(rec)
    gl = _geo_lat_lon_ok(d.get("latitude"), d.get("longitude"))
    if not gl:
        return None
    tit = f"Anúncio · {_safe_str(d.get('bairro'))} · {_safe_str(d.get('cidade'))}"
    uan = str(d.get("url_anuncio") or "").strip()
    tip = _lei_map_hover_tooltip_text(
        tipo=_safe_str(d.get("tipo_imovel")),
        area_m2=_to_float(d.get("area_construida_m2")),
        valor_venda=_to_float(d.get("valor_venda")),
    )
    return {
        "lat": gl[0],
        "lon": gl[1],
        "kind": "anuncio",
        "title": tit,
        "url": uan,
        "tooltip": tip,
        "anuncio_id": str(d.get("id") or "").strip(),
    }


def _pool_map_visible_ids_from_df(df: pd.DataFrame, *, max_pins: int = 800) -> list[str]:
    """IDs dos anúncios que aparecem no mapa (com geo; mesma amostra de até ``max_pins`` que o mapa)."""
    pins: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        p = _anuncio_row_geocode_pin(row)
        if p:
            pins.append(p)
    if len(pins) > max_pins:
        step = max(1, len(pins) // max_pins)
        pins = pins[::step][:max_pins]
    out: list[str] = []
    seen: set[str] = set()
    for p in pins:
        aid = str(p.get("anuncio_id") or "").strip()
        if aid and aid not in seen:
            seen.add(aid)
            out.append(aid)
    return out


def _pool_folium_bounds_to_box(bounds: Any) -> tuple[float, float, float, float] | None:
    """Converte ``bounds`` do streamlit-folium em (south_lat, west_lng, north_lat, east_lng)."""
    if not isinstance(bounds, dict):
        return None
    sw = bounds.get("_southWest") or {}
    ne = bounds.get("_northEast") or {}
    try:
        lat1 = float(sw["lat"])
        lng1 = float(sw["lng"])
        lat2 = float(ne["lat"])
        lng2 = float(ne["lng"])
    except (KeyError, TypeError, ValueError):
        return None
    south = min(lat1, lat2)
    north = max(lat1, lat2)
    west = min(lng1, lng2)
    east = max(lng1, lng2)
    return (south, west, north, east)


def _pool_bounds_dict_from_pin_coords(pins: list[dict[str, Any]], *, pad: float = 0.02) -> dict[str, dict[str, float]] | None:
    """BBox estilo Folium (SW/NE) para os pinos; mesmo espírito do ``fit_bounds`` inicial do mapa."""
    if not pins:
        return None
    lats = [float(p["lat"]) for p in pins]
    lons = [float(p["lon"]) for p in pins]
    return {
        "_southWest": {"lat": min(lats) - pad, "lng": min(lons) - pad},
        "_northEast": {"lat": max(lats) + pad, "lng": max(lons) + pad},
    }


def _pool_pins_with_geo_from_df(df: pd.DataFrame) -> list[dict[str, Any]]:
    pins: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        p = _anuncio_row_geocode_pin(row)
        if p:
            pins.append(p)
    return pins


def _pool_map_ids_in_viewport(
    df: pd.DataFrame,
    bounds: Any,
    *,
    max_pins: int = 800,
) -> list[str]:
    """IDs com geo cuja coordenada cai dentro do retângulo visível do mapa (zoom/pan)."""
    box = _pool_folium_bounds_to_box(bounds)
    if box is None:
        return []
    south, west, north, east = box
    pins = _pool_pins_with_geo_from_df(df)
    inside: list[dict[str, Any]] = []
    for p in pins:
        la, lo = float(p["lat"]), float(p["lon"])
        if south <= la <= north and west <= lo <= east:
            inside.append(p)
    if len(inside) > max_pins:
        step = max(1, len(inside) // max_pins)
        inside = inside[::step][:max_pins]
    out: list[str] = []
    seen: set[str] = set()
    for p in inside:
        aid = str(p.get("anuncio_id") or "").strip()
        if aid and aid not in seen:
            seen.add(aid)
            out.append(aid)
    return out


def _pool_render_mapa_folium(
    df: pd.DataFrame,
    *,
    max_pins: int = 800,
    pins_fp_state_key: str = "an_pool_pins_fp",
    folium_key: str = "an_pool_map",
    show_clear_button: bool = True,
    clear_button_key: str = "an_pool_map_clear",
    use_popup_merge: bool = True,
    merge_popup_from_returned_inner_text: bool = True,
    cache_map_select_mode: bool = False,
    map_height: int = 480,
    viewport_bounds_state_key: str | None = None,
    popup_query_param: str = "an_map_sel",
    popup_extra_query: dict[str, str] | None = None,
    merge_ids_session_key: str | None = None,
    merge_popup_seen_key: str | None = None,
    merge_ids_dict_cid: str | None = None,
) -> None:
    """Mapa Folium: anúncios com coordenadas (espiral em mesma geo). Com ``cache_map_select_mode``, a seleção segue o padrão da aba Cache (``last_object_clicked`` + token ``[[ANMAP:id]]`` no popup). Com ``viewport_bounds_state_key``, grava o retângulo do mapa na sessão para a sidebar (sem reaplicar ``fit_bounds`` no servidor)."""
    try:
        import folium
        from folium.plugins import MarkerCluster
        from streamlit_folium import st_folium
    except ImportError:
        st.warning("Instale `folium` e `streamlit-folium` (veja requirements.txt) para ver o mapa.")
        return

    if df.empty:
        st.info("Nenhum anúncio na lista filtrada.")
        return

    eff_merge = bool(merge_popup_from_returned_inner_text or cache_map_select_mode)

    pins: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        p = _anuncio_row_geocode_pin(row)
        if p:
            pins.append(p)

    if not pins:
        st.info("Nenhum anúncio desta lista tem latitude/longitude cadastradas.")
        return

    if len(pins) > max_pins:
        step = max(1, len(pins) // max_pins)
        pins = pins[::step][:max_pins]

    groups = _pool_group_pins_by_coord(pins, ndigits=5)
    has_stack = any(len(g) > 1 for g in groups.values())

    lats_all = [p["lat"] for p in pins]
    lons_all = [p["lon"] for p in pins]
    center_lat = sum(lats_all) / len(lats_all)
    center_lon = sum(lons_all) / len(lons_all)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, control_scale=True, tiles="OpenStreetMap")

    use_cluster = len(pins) > 70 and not has_stack
    layer: Any = m
    if use_cluster:
        layer = MarkerCluster(name="Anúncios").add_to(m)

    bounds_lats: list[float] = []
    bounds_lons: list[float] = []

    def _add_circle(la: float, lo: float, p: dict[str, Any]) -> None:
        tip = str(p.get("tooltip") or "")
        aid = str(p.get("anuncio_id") or "").strip()
        if use_popup_merge:
            if cache_map_select_mode and aid:
                pop_html = _lei_popup_cache_map_select_html(str(p["title"]), str(p["url"]), aid)
            elif aid:
                pop_html = _lei_popup_anuncio_pool_html(
                    str(p["title"]),
                    str(p["url"]),
                    aid,
                    query_param=popup_query_param,
                    extra_query=popup_extra_query,
                    embed_hidden_map_token=bool(merge_popup_from_returned_inner_text),
                )
            else:
                pop_html = _lei_popup_link(str(p["title"]), str(p["url"]))
        else:
            pop_html = _lei_popup_link(str(p["title"]), str(p["url"]))
        folium.CircleMarker(
            location=[la, lo],
            radius=7,
            color="#1e8449",
            weight=2,
            fill=True,
            fill_color="#2ecc71",
            fill_opacity=0.75,
            popup=folium.Popup(pop_html, max_width=320),
            tooltip=folium.Tooltip(tip, sticky=not cache_map_select_mode) if tip else "Anúncio",
        ).add_to(layer)
        bounds_lats.append(la)
        bounds_lons.append(lo)

    for _key, grp in groups.items():
        la0 = float(grp[0]["lat"])
        lo0 = float(grp[0]["lon"])
        n = len(grp)
        if n == 1:
            _add_circle(la0, lo0, grp[0])
        else:
            offs = _pool_spider_offsets_m(n, la0)
            for p, (dla, dlo) in zip(grp, offs):
                _add_circle(la0 + dla, lo0 + dlo, p)

    pins_sig = json.dumps(
        [{"id": str(p.get("anuncio_id")), "lat": round(float(p["lat"]), 6), "lon": round(float(p["lon"]), 6)} for p in pins],
        sort_keys=True,
    )
    data_fp = hashlib.sha256(pins_sig.encode("utf-8")).hexdigest()[:32]
    last_fp = st.session_state.get(pins_fp_state_key)
    if bounds_lats and last_fp != data_fp:
        pad = 0.02
        m.fit_bounds(
            [[min(bounds_lats) - pad, min(bounds_lons) - pad], [max(bounds_lats) + pad, max(bounds_lons) + pad]]
        )
    st.session_state[pins_fp_state_key] = data_fp

    if show_clear_button:
        c1, _c2 = st.columns(2)
        with c1:
            if st.button(
                "Limpar marcações do mapa",
                key=clear_button_key,
                help="Remove só as linhas marcadas via mapa / popup (aba Anúncios).",
            ):
                _mk = merge_ids_session_key or "an_ids_from_map"
                _sk = merge_popup_seen_key or (
                    f"_folium_popup_seen_{folium_key}" if merge_ids_session_key else "_an_folium_popup_seen"
                )
                st.session_state.pop(_mk, None)
                st.session_state.pop(_sk, None)
                st.rerun()
    ro: list[str] = []
    if use_popup_merge and eff_merge:
        ro.append("last_object_clicked_popup")
        if cache_map_select_mode:
            ro.append("last_object_clicked")
    if viewport_bounds_state_key:
        ro.append("bounds")
    out = st_folium(
        m,
        width=None,
        height=map_height,
        use_container_width=True,
        key=folium_key,
        returned_objects=ro if ro else [],
    )
    if viewport_bounds_state_key and isinstance(out, dict):
        b = out.get("bounds")
        if isinstance(b, dict) and b.get("_southWest") and b.get("_northEast"):
            st.session_state[viewport_bounds_state_key] = b
    if use_popup_merge and eff_merge:
        if merge_ids_session_key:
            _seen = merge_popup_seen_key or f"_folium_popup_seen_{folium_key}"
            _merge_ids_from_map_popup(
                out if isinstance(out, dict) else None,
                ids_session_key=merge_ids_session_key,
                seen_session_key=_seen,
                ids_as_dict_cid=merge_ids_dict_cid,
                require_marker_click=bool(cache_map_select_mode),
            )
        else:
            _an_merge_ids_from_map_popup(out if isinstance(out, dict) else None)


def _lei_render_mapa_folium(row: pd.Series) -> None:
    """Mapa Folium: pino do leilão (cor distinta) + pinos dos anúncios do cache principal (melhor match)."""
    try:
        import folium
        from streamlit_folium import st_folium
    except ImportError:
        st.warning("Instale `folium` e `streamlit-folium` (veja requirements.txt) para ver o mapa.")
        return

    cidade = _safe_str(row.get("cidade"))
    bairro = _safe_str(row.get("bairro"))
    estado = _safe_str(row.get("estado"))
    url_lei = str(row.get("url_leilao") or "").strip()
    title_lei = f"Leilão · {cidade} / {bairro}"
    area_lei = _to_float(area_efetiva_de_registro(row))
    v_lei = _to_float(row.get("valor_venda_sugerido"))
    if v_lei <= 0:
        v_lei = _to_float(row.get("valor_mercado_estimado"))
    tip_lei = _lei_map_hover_tooltip_text(
        tipo=_safe_str(row.get("tipo_imovel")),
        area_m2=area_lei,
        valor_venda=v_lei,
    )

    pins: list[dict[str, Any]] = []
    ll = _geo_lat_lon_ok(row.get("latitude"), row.get("longitude"))
    if ll:
        pins.append(
            {
                "lat": ll[0],
                "lon": ll[1],
                "kind": "leilao",
                "title": title_lei,
                "url": url_lei,
                "tooltip": tip_lei,
            }
        )

    an_rows: list[dict[str, Any]] = []
    df_cache = _df_cache_para_linha_leilao(row)
    if not df_cache.empty:
        cr0 = df_cache.iloc[0]
        ids = _parse_anuncios_ids_field(cr0.get("anuncios_ids"))
        an_rows = _fetch_anuncios_rows(ids)[:120]

    for ar in an_rows:
        ap = _anuncio_row_geocode_pin(ar)
        if ap:
            pins.append(ap)

    if not pins:
        st.info("Sem coordenadas para mapa: cadastre geolocalização no leilão e nos anúncios do cache, ou aguarde o pipeline.")
        return

    center_lat = sum(p["lat"] for p in pins) / len(pins)
    center_lon = sum(p["lon"] for p in pins) / len(pins)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=13, control_scale=True, tiles="OpenStreetMap")

    groups = _pool_group_pins_by_coord(pins, ndigits=5)
    bounds_lats: list[float] = []
    bounds_lons: list[float] = []

    def _place_pin(la: float, lo: float, p: dict[str, Any]) -> None:
        tip = str(p.get("tooltip") or "")
        if p["kind"] == "leilao":
            folium.Marker(
                location=[la, lo],
                popup=folium.Popup(_lei_popup_link(str(p["title"]), str(p["url"])), max_width=320),
                tooltip=folium.Tooltip(tip, sticky=True) if tip else "Leilão",
                icon=folium.Icon(color="blue"),
            ).add_to(m)
        else:
            folium.CircleMarker(
                location=[la, lo],
                radius=7,
                color="#1e8449",
                weight=2,
                fill=True,
                fill_color="#2ecc71",
                fill_opacity=0.75,
                popup=folium.Popup(_lei_popup_link(str(p["title"]), str(p["url"])), max_width=320),
                tooltip=folium.Tooltip(tip, sticky=True) if tip else "Comparável",
            ).add_to(m)
        bounds_lats.append(la)
        bounds_lons.append(lo)

    for _key, grp in groups.items():
        la0 = float(grp[0]["lat"])
        lo0 = float(grp[0]["lon"])
        n = len(grp)
        if n == 1:
            _place_pin(la0, lo0, grp[0])
        else:
            for p, (dla, dlo) in zip(grp, _pool_spider_offsets_m(n, la0)):
                _place_pin(la0 + dla, lo0 + dlo, p)

    rid = str(row.get("id") or url_lei or title_lei)
    pins_sig = json.dumps(
        [
            {
                "kind": p["kind"],
                "id": str(p.get("anuncio_id") or ""),
                "lat": round(float(p["lat"]), 6),
                "lon": round(float(p["lon"]), 6),
            }
            for p in pins
        ],
        sort_keys=True,
    )
    data_fp = hashlib.sha256(f"{rid}|{pins_sig}".encode("utf-8")).hexdigest()[:32]
    _lei_fp_slot = "lei_detail_map_pins_fp::" + hashlib.sha256(rid.encode("utf-8", errors="replace")).hexdigest()[:24]
    last_fp = st.session_state.get(_lei_fp_slot)
    if bounds_lats and last_fp != data_fp:
        pad = 0.02
        m.fit_bounds(
            [[min(bounds_lats) - pad, min(bounds_lons) - pad], [max(bounds_lats) + pad, max(bounds_lons) + pad]]
        )
    st.session_state[_lei_fp_slot] = data_fp

    st_folium(
        m,
        width=None,
        height=420,
        use_container_width=True,
        key="lei_detail_map",
        returned_objects=[],
    )


def _order_anuncios_rows_by_ids(rows: list[dict[str, Any]], ids: list[str]) -> list[dict[str, Any]]:
    m = {str(r.get("id") or ""): r for r in rows if r.get("id") is not None}
    return [m[i] for i in ids if i in m]


def _recompute_cache_payload_from_anuncio_rows(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    pm2: list[float] = []
    valores_venda: list[float] = []
    ids_out: list[str] = []
    for it in rows:
        iid = str(it.get("id") or "").strip()
        if not iid:
            continue
        p = _to_float(it.get("preco_m2"))
        v_v = _to_float(it.get("valor_venda"))
        if p <= 0:
            a = _to_float(it.get("area_construida_m2"))
            p = v_v / a if a > 0 and v_v > 0 else 0
        if p <= 0:
            continue
        pm2.append(p)
        if v_v > 0:
            valores_venda.append(v_v)
        ids_out.append(iid)
    if not pm2:
        return None
    return {
        "preco_m2_medio": round(float(statistics.median(pm2)), 2),
        "valor_medio_venda": round(statistics.mean(valores_venda), 2) if valores_venda else None,
        "maior_valor_venda": round(max(valores_venda), 2) if valores_venda else None,
        "menor_valor_venda": round(min(valores_venda), 2) if valores_venda else None,
        "n_amostras": len(pm2),
        "anuncios_ids": ",".join(ids_out),
    }


def _merge_metadados_cache_edit(prev: Any, patch: dict[str, Any]) -> str:
    try:
        if isinstance(prev, str) and prev.strip():
            base = json.loads(prev)
        elif isinstance(prev, dict):
            base = dict(prev)
        else:
            base = {}
        if not isinstance(base, dict):
            base = {}
    except json.JSONDecodeError:
        base = {}
    base.update(patch)
    return json.dumps(base, ensure_ascii=False)


def _apply_cache_member_ids(
    cache_id: str,
    id_list: list[str],
    *,
    metadados_prev: Any = None,
    nome_cache: str | None = None,
) -> tuple[bool, str]:
    """Redefine os anúncios que compõem o cache, recalcula agregados ou apaga se vazio."""
    cli = get_supabase_client()
    clean = list(dict.fromkeys(x.strip() for x in id_list if x and str(x).strip()))
    if not clean:
        try:
            cli.table(CACHE_TABLE).delete().eq("id", cache_id).execute()
        except Exception as e:
            return False, str(e)
        return True, "Cache removido (nenhum anúncio vinculado)."

    rows = _fetch_anuncios_rows(clean)
    payload = _recompute_cache_payload_from_anuncio_rows(rows)
    if not payload:
        return False, "Nenhum anúncio com R$/m² válido na lista resultante."

    payload["fonte"] = "frontend_cache_membros"
    payload["metadados_json"] = _merge_metadados_cache_edit(
        metadados_prev,
        {
            "origem": "edicao_membros_cache",
            "ids_solicitados": len(clean),
            "ids_validos_pm2": len(payload["anuncios_ids"].split(",")),
        },
    )
    nome_c = _safe_str(nome_cache).strip()
    if not nome_c:
        return False, "Informe um nome para o cache antes de aplicar alterações na composição."
    payload["nome_cache"] = nome_c[:240]
    try:
        cli.table(CACHE_TABLE).update(payload).eq("id", cache_id).execute()
    except Exception as e:
        return False, str(e)
    return True, f"Cache atualizado ({payload['n_amostras']} amostra(s), mediana {_fmt_brl(payload['preco_m2_medio'])}/m²)."


def _cache_row_tab_label(row: pd.Series) -> str:
    cid = str(row.get("id") or "")[:8] or "?"
    cidade = (_safe_str(row.get("cidade"), "?")[:16] + "…") if len(_safe_str(row.get("cidade"), "")) > 16 else _safe_str(row.get("cidade"), "?")
    bairro = (_safe_str(row.get("bairro"), "?")[:16] + "…") if len(_safe_str(row.get("bairro"), "")) > 16 else _safe_str(row.get("bairro"), "?")
    nn = _safe_str(row.get("nome_cache"), "")
    if nn:
        nn_disp = (nn[:28] + "…") if len(nn) > 28 else nn
        return f"{nn_disp} · {cidade} / {bairro} · {cid}"
    return f"{cidade} / {bairro} · {cid}"


def _dataframe_selection_rows(ev: Any) -> list[int]:
    """Extrai índices de linhas selecionadas do retorno de ``st.dataframe`` com ``on_select``."""
    if ev is None:
        return []
    try:
        r = ev["selection"]["rows"]
        return [int(x) for x in (r or [])]
    except (TypeError, KeyError, AttributeError, ValueError):
        pass
    try:
        r = ev.selection.rows
        return [int(x) for x in (r or [])]
    except (TypeError, KeyError, AttributeError, ValueError):
        return []


def _render_cache_members_editor(
    cache_row: pd.Series,
    *,
    table_height: int = 380,
) -> tuple[str, list[str], list[dict[str, Any]] | None, list[int]]:
    """Composição: ``st.dataframe`` com seleção por linha (clique). Retorna (cache_id, raw_ids, ordered, índices)."""
    cid = str(cache_row.get("id") or "")
    if not cid:
        st.error("Cache sem id.")
        return "", [], None, []
    raw_ids = _parse_anuncios_ids_field(cache_row.get("anuncios_ids"))
    rows = _fetch_anuncios_rows(raw_ids)
    ordered = _order_anuncios_rows_by_ids(rows, raw_ids)

    if raw_ids and len(ordered) < len(raw_ids):
        st.warning(
            f"{len(raw_ids) - len(ordered)} ID(s) listados no cache não foram encontrados em `{TABLE_ANUNCIOS_MERCADO}`."
        )

    if not raw_ids:
        st.info("Este cache ainda não tem `anuncios_ids` gravados. Use **→** para vincular anúncios do pool.")
        return cid, [], None, []

    if not ordered:
        return cid, raw_ids, None, []

    df_m = pd.DataFrame(ordered)
    comp_fd = _ca_pool_filters_expander_ui(
        key_prefix=f"ca_comp_{cid}",
        title="Filtros da tabela (composição)",
        expanded=True,
    )
    comp_mask = _ca_mask_pool_like(df_m, **comp_fd)
    df_view = df_m[comp_mask].reset_index(drop=True)
    ordered_view = df_view.to_dict("records")
    n = len(df_view)
    if n == 0 and len(df_m) > 0:
        st.warning("Nenhum anúncio na composição corresponde aos filtros da tabela.")
    df_key = f"ca_comp_df_{cid}"
    if n > 0 and not df_view.empty:
        st.markdown("##### Mapa da composição")
        _pool_render_mapa_folium(
            df_view,
            pins_fp_state_key=f"ca_comp_map_pins_fp_{cid}",
            folium_key=f"ca_comp_map_{cid}",
            show_clear_button=False,
            use_popup_merge=True,
            cache_map_select_mode=True,
            map_height=int(table_height),
            popup_query_param="ca_comp_map_sel",
            popup_extra_query={"ca_comp_cid": cid},
            merge_ids_session_key=_CA_SS_COMP_MAP_IDS,
            merge_ids_dict_cid=cid,
        )
    _ca_apply_comp_map_ids_to_selection(cid, df_view, df_key)
    mem_sel_all = st.toggle(
        "Selecionar todos (filtrados)",
        value=False,
        key=f"ca_mem_sel_all_{cid}",
    )
    if mem_sel_all and n > 0:
        st.session_state[df_key] = {"selection": {"rows": list(range(n)), "columns": [], "cells": []}}

    disp_cols = [c for c in [
        "cidade", "bairro", "estado", "tipo_imovel",
        "valor_venda", "preco_m2", "area_construida_m2", "url_anuncio",
    ] if c in df_view.columns]
    df_show = df_view[disp_cols].copy()
    cfg: dict[str, Any] = {}
    if "url_anuncio" in df_show.columns:
        cfg["url_anuncio"] = st.column_config.LinkColumn("Link", display_text="Abrir")
    for _anc, _lab in (
        ("valor_venda", "Valor venda"),
        ("preco_m2", "R$/m²"),
        ("area_construida_m2", "Área m²"),
    ):
        if _anc in df_show.columns:
            cfg[_anc] = st.column_config.NumberColumn(_lab, format="%.2f")

    ev = st.dataframe(
        df_show,
        hide_index=True,
        width="stretch",
        height=int(table_height),
        column_config=cfg if cfg else None,
        key=df_key,
        on_select="rerun",
        selection_mode="multi-row",
    )
    sel_rows = _dataframe_selection_rows(ev)
    return cid, raw_ids, ordered_view, sel_rows


# ── Pipeline background ──────────────────────────────────────────────────────
def _run_pipeline(path: Path, params: dict[str, Any]) -> dict[str, Any]:
    raw_me = params.get("modo_entrada") or "planilha"
    modo_e = cast(
        Literal["planilha", "avulso"],
        raw_me if raw_me in ("planilha", "avulso") else "planilha",
    )
    cfg = LeilaoPricingPipelineConfig(
        caminho_planilha=path,
        usar_avaliacao_llm=params["usar_avaliacao_llm"],
        limite_imoveis_llm_por_execucao=params["limite_llm"],
        min_anuncios_mercado_similares=params["min_anuncios"],
        confianca_minima_comparaveis=params["confianca_minima"],
        bloquear_llm_baixa_confianca=params["bloquear_llm_baixa_confianca"],
        raio_similaridade_inicial_km=params["raio_inicial"],
        raios_similaridade_expansao_km=tuple(params["raios_expansao"]),
        roi_minimo_exportacao_pct=params["roi_minimo_exportacao"],
        fator_prudencia_cache=params["fator_prudencia_cache"],
        tempo_limite_execucao_seg=int(params.get("timeout_total_seg") or 0),
        abort_checker=params.get("abort_checker"),
        progress_callback=params.get("progress_callback"),
        modo_entrada=modo_e,
    )
    return executar_pipeline_precificacao_leiloes(cfg)

def _build_temp_xlsx(form: dict[str, Any]) -> Path:
    row: dict[str, Any] = {"url_leilao": form["url_leilao"]}
    for k in ("cidade", "estado", "bairro", "endereco", "padrao_imovel", "tipo_imovel",
              "conservacao", "tipo_casa", "andar", "data_leilao", "status",
              "area_util", "area_total", "quartos", "vagas", "valor_arrematacao",
              "latitude", "longitude"):
        val = form.get(k)
        if val is not None and str(val).strip() != "":
            row[k] = val
    tmp = tempfile.NamedTemporaryFile(prefix="leilao_", suffix=".xlsx", delete=False)
    pd.DataFrame([row]).to_excel(tmp.name, index=False)
    return Path(tmp.name)

def _extrair_urls(modo: str, path: Path | None, form: dict[str, Any] | None) -> list[str]:
    if modo == "avulso":
        u = str((form or {}).get("url_leilao") or "").strip()
        return [u] if u else []
    if not path:
        return []
    try:
        return [str(e.get("url_leilao") or "").strip() for e in ler_entradas_leilao_de_planilha(path) if str(e.get("url_leilao") or "").strip()]
    except Exception:
        return []

def _forcar_reprocessamento(urls: list[str]) -> dict[str, int]:
    if not urls:
        return {"candidatos": 0, "resetados": 0}
    cli = get_supabase_client()
    rows = cli.table(SUPABASE_TABLE).select("id,url_leilao,status").in_("url_leilao", urls).limit(1000).execute().data or []
    n = 0
    for r in rows:
        iid = r.get("id")
        if not iid:
            continue
        atualizar_leilao_imovel_campos(str(iid), {
            "status": "pendente", "valor_mercado_estimado": None,
            "valor_venda_sugerido": None, "valor_venda_liquido": None,
            "lance_maximo_recomendado": None, "roi_projetado": None,
            "valor_maximo_regiao_estimado": None, "valor_teto_regiao_agressivo": None,
            "potencial_reposicionamento_pct": None, "custo_reforma_estimado": None,
            "alerta_precificacao_baixa_amostragem": None,
        }, client=cli)
        n += 1
    return {"candidatos": len(rows), "resetados": n}

def _coletar_web_avulso(form: dict[str, Any], *, min_salvos: int) -> dict[str, Any]:
    cidade = str(form.get("cidade") or "").strip()
    estado = str(form.get("estado") or "").strip()
    bairro = str(form.get("bairro") or "").strip()
    loc = bairro or estado
    uf = chave_estado(estado)
    if not cidade or not loc:
        return {"ok": False, "mensagem": "Preencha cidade e bairro/estado."}
    if not uf:
        return {"ok": False, "mensagem": "Preencha o estado (UF)."}
    row_ref = {k: form.get(k) for k in ("cidade", "estado", "bairro", "endereco", "tipo_imovel", "conservacao", "tipo_casa", "quartos", "vagas", "area_util", "area_total", "bairro_vivareal_slug")}
    seg = segmento_mercado_de_registro(row_ref)
    tipo = seg["tipo_imovel"] if seg["tipo_imovel"] != "desconhecido" else "imóvel"
    qi = form.get("quartos")
    cli = get_supabase_client()
    cr = coletar_e_persistir_via_ddgs(
        row_referencia=row_ref, cidade=cidade, localizacao=loc,
        quartos=qi if isinstance(qi, int) else None, tipo_imovel_busca=tipo,
        seg=seg, client=cli, min_salvos=min_salvos,
    )
    if cr.vivareal_markdown_insuficiente:
        return {
            "ok": False,
            "vivareal_insuficiente": True,
            "mensagem": "Anúncios insuficientes: a listagem VivaReal não retornou cards válidos.",
            "salvos": 0,
        }
    salvos = cr.salvos
    if salvos == 0:
        cr2 = coletar_e_persistir_via_ddgs(
            row_referencia=row_ref, cidade=cidade, localizacao=loc,
            quartos=None, tipo_imovel_busca="imóvel", seg=seg, client=cli,
            min_salvos=max(1, min_salvos // 2), max_results_inicial=38, max_rodadas=8,
        )
        if cr2.vivareal_markdown_insuficiente:
            return {
                "ok": False,
                "vivareal_insuficiente": True,
                "mensagem": "Anúncios insuficientes: a listagem VivaReal não retornou cards válidos.",
                "salvos": 0,
            }
        salvos = cr2.salvos
    return {"ok": True, "salvos": salvos}

def _exec_background(*, task_id: str, modo: str, path: Path, form: dict | None,
                      params: dict, reprocess: bool, coleta_web: bool, timeout: int) -> None:
    try:
        _task_update(task_id, status="running", started_at=time.time())
        urls = _extrair_urls(modo, path, form)
        reset = _forcar_reprocessamento(urls) if reprocess and urls else None
        pre = None
        if modo == "avulso" and form and coleta_web:
            if _task_should_abort(task_id):
                _task_update(task_id, status="aborted"); return
            pre = _coletar_web_avulso(form, min_salvos=int(params["min_anuncios"]))
            if pre.get("vivareal_insuficiente"):
                started = float((_task_get(task_id) or {}).get("started_at") or time.time())
                fin = time.time()
                try:
                    cli = get_supabase_client()
                    meta = {
                        k: form[k]
                        for k in (
                            "cidade", "estado", "bairro", "endereco", "padrao_imovel", "tipo_imovel",
                            "conservacao", "tipo_casa", "andar", "data_leilao", "status",
                            "area_util", "area_total", "quartos", "vagas", "valor_arrematacao",
                            "latitude", "longitude", "bairro_vivareal_slug",
                        )
                        if k in form and form.get(k) is not None and str(form.get(k)).strip() != ""
                    }
                    url_l = str(form.get("url_leilao") or "").strip()
                    if url_l:
                        r_ing = ingerir_url_leilao(
                            url_l, metadados_planilha=meta or None, client=cli
                        )
                        rid = r_ing.get("id")
                        if rid:
                            atualizar_leilao_imovel_campos(
                                str(rid), {"status": STATUS_PENDENTE}, client=cli
                            )
                except Exception:
                    logger.exception("Persistência pendente após VivaReal insuficiente (avulso)")
                _task_update(
                    task_id,
                    status="done",
                    relatorio={
                        "interrompido": True,
                        "motivo_interrupcao": "vivareal_anuncios_insuficientes",
                        "precoleta": pre,
                        "mensagem": "Listagem VivaReal sem anúncios válidos. Imóvel mantido como pendente.",
                    },
                    precoleta=pre,
                    finished_at=fin,
                    execution_seconds=round(fin - started, 2),
                )
                return
        p = dict(params)
        p["timeout_total_seg"] = timeout
        p["modo_entrada"] = modo
        p["abort_checker"] = lambda: _task_should_abort(task_id)
        p["progress_callback"] = lambda payload: _task_update(task_id, progress=payload)
        rel = _run_pipeline(path, p)
        st_final = "aborted" if rel.get("interrompido") or rel.get("abortado") or _task_should_abort(task_id) else "done"
        _task_update(task_id, status=st_final, relatorio=rel, reset_stats=reset, precoleta=pre,
                     finished_at=time.time(),
                     execution_seconds=round(time.time() - float((_task_get(task_id) or {}).get("started_at") or time.time()), 2))
    except Exception as e:
        _task_update(task_id, status="error", erro=str(e), finished_at=time.time(),
                     execution_seconds=round(time.time() - float((_task_get(task_id) or {}).get("started_at") or time.time()), 2))


# ═══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR — Navigation + contextual controls
# ═══════════════════════════════════════════════════════════════════════════════
_PAGES = ["📊 Resumo", "🏠 Leilões", "📋 Anúncios", "🗄️ Cache", "🧮 Simulador"]

if "active_task_id" not in st.session_state:
    st.session_state["active_task_id"] = None

# Sidebar variables initialised before conditional blocks
run_clicked = False
planilha_path: Path | None = None
manual_form: dict[str, Any] | None = None
coleta_web = False
timeout = 900

# Sidebar-level filter/state holders per page (set inside sidebar, read in main)
sb: dict[str, Any] = {}
ca_merge_nome = ""
an_recalc_cache_nome = ""

_ca_bootstrap_nav_from_map_query()

with st.sidebar:
    st.markdown("### Leilão IA")
    pagina = st.radio("Navegação", _PAGES, label_visibility="collapsed", key="nav_page")
    _ca_consume_map_sel_query_params()
    st.divider()

    # ────────── Resumo / Pipeline ──────────────────────────────────────────
    if pagina == "📊 Resumo":
        st.markdown("##### Pipeline")
        with st.expander("Motor de busca", expanded=False):
            _fc = firecrawl_status()
            if _fc["ativo"]:
                st.success(f"Firecrawl ativo ({_fc['credits_used_session']}/{_fc['credit_limit_session']})")
            elif _fc["disponivel"] and _fc["credits_exhausted"]:
                st.error("Créditos esgotados — DDGS")
            elif _fc["disponivel"]:
                st.warning(f"Limite sessão — DDGS")
            else:
                st.info("Firecrawl indisponível — DDGS")
            if st.button("Ver saldo", key="fc_bal"):
                acct = firecrawl_account_credits()
                if acct and acct.get("remaining") is not None:
                    st.metric("Restantes", _fmt_n(acct['remaining'], 0))

        modo = st.radio("Entrada", ["Planilha", "Imóvel avulso"], horizontal=True)

        with st.expander("Parâmetros", expanded=False):
            usar_llm = st.toggle("Avaliação LLM", value=True)
            bloquear_llm_baixa = st.toggle("Bloquear LLM baixa confiança", value=True)
            limite_llm = st.number_input("Limite LLM", min_value=0, value=5, step=1)
            min_anuncios = st.number_input("Mín. anúncios", min_value=1, value=5, step=1)
            confianca_min = st.slider("Confiança mínima", 0.0, 100.0, 55.0, 1.0)
            raio_ini = st.slider("Raio inicial (km)", 1.0, 10.0, 3.0, 0.5)
            raios_txt = st.text_input("Raios expansão", "5,8,12")
            fator_prud = st.slider("Fator prudência", 0.7, 1.2, 0.92, 0.01)
            roi_min_exp = st.number_input("ROI mín. export (%)", -100.0, 500.0, 25.0, 1.0)
            timeout = st.number_input("Timeout (s)", min_value=0, value=900, step=30)

        def _parse_raios(t: str) -> list[float]:
            out = []
            for x in (t or "").split(","):
                x = x.strip()
                try:
                    out.append(float(x))
                except ValueError:
                    pass
            return out or [5.0, 8.0, 12.0]

        common_params = {
            "usar_avaliacao_llm": bool(usar_llm), "limite_llm": int(limite_llm),
            "min_anuncios": int(min_anuncios), "confianca_minima": float(confianca_min),
            "bloquear_llm_baixa_confianca": bool(bloquear_llm_baixa),
            "raio_inicial": float(raio_ini), "raios_expansao": _parse_raios(raios_txt),
            "fator_prudencia_cache": float(fator_prud), "roi_minimo_exportacao": float(roi_min_exp),
        }

        reprocess = st.toggle("Forçar reprocessamento", value=True)

        if modo == "Planilha":
            p = st.text_input("Caminho da planilha", "Lista_leiloes.xlsx")
            planilha_path = Path(p).expanduser()
            run_clicked = st.button("Executar pipeline", type="primary", width="stretch")
        else:
            st.markdown("##### Dados do imóvel")
            url_leilao = st.text_input("URL do leilão *")
            ibge_ufs = _ibge_estados()
            uf_options = [""] + [e["sigla"] for e in ibge_ufs]
            uf_labels = {e["sigla"]: f"{e['sigla']} — {e['nome']}" for e in ibge_ufs}
            estado_val = st.selectbox("Estado", options=uf_options,
                format_func=lambda x: "Selecione..." if x == "" else uf_labels.get(x, x), key="avulso_estado")
            ibge_cities = _ibge_cidades(estado_val) if estado_val else []
            cidade_val = st.selectbox("Cidade", options=[""] + ibge_cities,
                format_func=lambda x: "Selecione..." if x == "" else x, key="avulso_cidade") if ibge_cities else ""
            if not cidade_val:
                cidade_val = st.text_input("Ou digite a cidade", key="avulso_cidade_txt")
            bairro_vivareal_slug = ""
            bairros_vr = _bairros_vivareal_cached(estado_val, cidade_val) if estado_val and cidade_val else []
            if bairros_vr:
                slug_map = {slug: nome for slug, nome in bairros_vr}
                slug_options = [""] + [s for s, _ in bairros_vr]
                bairro_vivareal_slug = st.selectbox(
                    "Bairro (VivaReal)", options=slug_options,
                    format_func=lambda x: "Selecione..." if x == "" else slug_map.get(x, x),
                    key="avulso_bairro_vr",
                )
                bairro_val = slug_bairro_para_nome(bairro_vivareal_slug) if bairro_vivareal_slug else ""
            else:
                bairros_db = _distinct_col_filtered(SUPABASE_TABLE, "bairro",
                    {k: v for k, v in {"estado": estado_val, "cidade": cidade_val}.items() if v}) if estado_val or cidade_val else []
                bairros_all = sorted(set(bairros_db) - {""})
                bairro_val = st.selectbox("Bairro", options=[""] + bairros_all,
                    format_func=lambda x: "Selecione..." if x == "" else x, key="avulso_bairro") if bairros_all else ""
            if not bairro_val:
                bairro_val = st.text_input("Ou digite o bairro", key="avulso_bairro_txt")
            endereco = st.text_input("Endereço")
            data_leilao = st.text_input("Data leilão (YYYY-MM-DD)")
            c1, c2 = st.columns(2)
            tipo_imovel = c1.selectbox("Tipo", ["desconhecido", "apartamento", "casa", "casa_condominio", "terreno", "comercial"])
            conservacao = c2.selectbox("Conservação", ["desconhecido", "novo", "reformado", "regular", "necessita_reforma"])
            c3, c4 = st.columns(2)
            tipo_casa = c3.selectbox("Tipo casa", ["desconhecido", "terrea", "sobrado", "geminada"])
            padrao = c4.selectbox("Padrão reforma", ["baixo", "medio", "alto"], index=1)
            c5, c6, c7 = st.columns(3)
            quartos = _to_int_or_none(c5.text_input("Quartos", ""))
            vagas = _to_int_or_none(c6.text_input("Vagas", ""))
            andar = _to_int_or_none(c7.text_input("Andar", ""))
            c8, c9, c10 = st.columns(3)
            if tipo_imovel == "terreno":
                area_util = None
                area_total_val = _to_float_or_none(c8.text_input("Área do terreno m²", ""))
                c9.markdown("")
            else:
                area_util = _to_float_or_none(c8.text_input("Área útil m²", ""))
                area_total_val = _to_float_or_none(c9.text_input("Área total m²", "", help="Área do terreno (casas)"))
            valor_arr = _to_float_or_none(c10.text_input("Valor arrematação", ""))
            coleta_web = st.toggle("Coletar anúncios web antes", value=True)
            run_clicked = st.button("Executar pipeline avulso", type="primary", width="stretch")
            if run_clicked:
                _url_clean = url_leilao.strip()
                if not _url_clean:
                    st.error("Informe a URL do leilão.")
                    run_clicked = False
                else:
                    if not _url_clean.lower().startswith(("http://", "https://")):
                        _url_clean = "https://" + _url_clean
                    _geo_avulso = None
                    try:
                        from geocoding import geocodificar_endereco as _geocode
                        _geo_avulso = _geocode(
                            logradouro=endereco,
                            bairro=bairro_val,
                            cidade=cidade_val,
                            estado=estado_val,
                        )
                    except Exception:
                        pass
                    manual_form = {
                        "url_leilao": _url_clean, "cidade": cidade_val, "estado": estado_val,
                        "bairro": bairro_val, "endereco": endereco, "data_leilao": data_leilao,
                        "tipo_imovel": tipo_imovel, "conservacao": conservacao, "tipo_casa": tipo_casa,
                        "padrao_imovel": padrao, "quartos": quartos, "vagas": vagas,
                        "andar": andar, "area_util": area_util, "area_total": area_total_val, "valor_arrematacao": valor_arr, "status": "pendente",
                        "bairro_vivareal_slug": bairro_vivareal_slug,
                        **({"latitude": _geo_avulso[0], "longitude": _geo_avulso[1]} if _geo_avulso else {}),
                    }
                    planilha_path = _build_temp_xlsx(manual_form)

    # ────────── Leilões ────────────────────────────────────────────────────
    elif pagina == "🏠 Leilões":
        sb["lei_passados"] = st.toggle("Mostrar encerrados", value=False, key="lei_passados")
        sb["lei_descartados"] = st.toggle("Mostrar descartados", value=False, key="lei_descartados")
        sb["lei_roi_min"] = st.number_input("ROI mínimo (%)", value=-100.0, step=5.0, key="lei_roi_min")
        sb["lei_sort"] = st.selectbox("Ordenar por", ["Data leilão (próximos)", "ROI (maior)", "ROI (menor)", "Lance (menor)", "Lance (maior)"], key="lei_sort")
        st.divider()
        st.markdown("##### Resultado")

    # ────────── Anúncios ───────────────────────────────────────────────────
    elif pagina == "📋 Anúncios":
        st.markdown("##### Opções")
        sb["an_lim"] = st.slider("Linhas", 50, 800, 250, 50, key="an_lim")
        sb["an_arq"] = st.toggle("Incluir arquivados", value=False, disabled=not _anuncios_soft_delete_ok(), key="an_arq")
        sb["an_all"] = st.toggle("Selecionar todos", value=False, key="an_all")
        st.divider()
        st.markdown("##### Ações")
        an_recalc_cache_nome = st.text_input(
            "Nome do cache (ao recalcular)",
            value="",
            max_chars=200,
            key="an_recalc_cache_nome_inp",
            help="Obrigatório ao gravar novos caches a partir dos anúncios selecionados. Se houver mais de um grupo (cidade/bairro/tipo), serão usados sufixos (2), (3)…",
        )
        sb["an_del"] = st.button("Excluir selecionados", type="secondary", width="stretch", key="an_del_sb")
        sb["an_clear_sel"] = st.button("Limpar selecionados", type="secondary", width="stretch", key="an_clear_sel_sb")
        st.divider()
        st.markdown("##### Mapa (lista filtrada)")
        an_fd_sb = _ca_pool_filters_dict_from_state("an_tbl")
        df_an_sb = _query_anuncios(limit=int(sb["an_lim"]), include_arq=bool(sb.get("an_arq", False)))
        if not df_an_sb.empty:
            df_an_sb = df_an_sb.reset_index(drop=True).copy()
        an_mask_sb = _ca_mask_pool_like(df_an_sb, **an_fd_sb)
        df_an_vis_sb = df_an_sb[an_mask_sb].reset_index(drop=True)
        if _anuncios_soft_delete_ok() and not sb.get("an_arq", False) and "arquivado_em" in df_an_vis_sb.columns:
            df_an_vis_sb = df_an_vis_sb[df_an_vis_sb["arquivado_em"].isna()]
        _an_map_touch_viewport_cache(df_an_vis_sb)
        bounds_an_sb = st.session_state.get(_AN_SS_MAP_VIEWPORT_BOUNDS)
        if isinstance(bounds_an_sb, dict) and bounds_an_sb.get("_southWest") and bounds_an_sb.get("_northEast"):
            map_ids_an_sb = _pool_map_ids_in_viewport(df_an_vis_sb, bounds_an_sb, max_pins=800)
        else:
            map_ids_an_sb = _pool_map_visible_ids_from_df(df_an_vis_sb, max_pins=800)
        sb["an_pool_map_sel"] = st.button(
            "Marcar no filtro todos do mapa",
            disabled=not map_ids_an_sb,
            width="stretch",
            key="an_pool_map_sel_sb",
            help="Marca na lista os anúncios **visíveis na área atual do mapa** (zoom/pan). Interaja com o mapa na página após mudar filtros. Se o mapa ainda não devolveu o retângulo, usa só os pinos desenhados (até o limite do mapa).",
        )
        if sb["an_pool_map_sel"]:
            st.session_state[_AN_SS_SEL_IDS] = [str(x) for x in map_ids_an_sb]
            if not df_an_vis_sb.empty and "id" in df_an_vis_sb.columns:
                an_atk_sb = _ca_df_select_widget_key("an_list", df_an_vis_sb, "id")
                want_an = set(str(x) for x in map_ids_an_sb)
                row_ix_an_sb = [
                    i for i, rid in enumerate(df_an_vis_sb["id"].astype(str)) if str(rid) in want_an
                ]
                st.session_state[an_atk_sb] = _ca_widget_sel_state(row_ix_an_sb)
            st.rerun()
        if sb.get("an_clear_sel"):
            st.session_state[_AN_SS_SEL_IDS] = []
            st.session_state["an_ids_from_map"] = set()
            st.session_state.pop("_folium_popup_seen_an_pool_map", None)
            if not df_an_vis_sb.empty and "id" in df_an_vis_sb.columns:
                _atk_clr = _ca_df_select_widget_key("an_list", df_an_vis_sb, "id")
                st.session_state[_atk_clr] = _ca_widget_sel_state([])
            st.rerun()
        st.divider()
        st.markdown("##### Importar")

    # ────────── Cache ───────────────────────────────────────────────────────
    elif pagina == "🗄️ Cache":
        st.markdown("##### Ações")
        ca_merge_nome = st.text_input(
            "Nome do cache (ao mesclar)",
            value="",
            max_chars=200,
            key="ca_merge_nome_inp",
            help="Obrigatório ao unir duas ou mais entradas num único cache.",
        )
        sb["ca_del"] = st.button("Excluir selecionados", type="secondary", width="stretch", key="ca_del_sb")
        sb["ca_merge"] = st.button("Mesclar selecionados", type="primary", width="stretch", key="ca_merge_sb")
        st.divider()
        st.markdown("##### Anúncios (pool)")
        sb["ca_an_lim"] = st.slider("Anúncios (linhas)", 50, 800, 200, 50, key="ca_an_lim")
        pool_fd_sb = _ca_pool_filters_dict_from_state("ca_pool_f")
        df_an_sb = _query_anuncios(limit=int(sb["ca_an_lim"]))
        if not df_an_sb.empty:
            df_an_sb = df_an_sb.reset_index(drop=True).copy()
        pool_mask_sb = _ca_mask_pool_like(df_an_sb, **pool_fd_sb)
        df_pool_vis_sb = df_an_sb[pool_mask_sb].reset_index(drop=True)
        _ca_pool_touch_viewport_cache(df_pool_vis_sb)
        bounds_sb = st.session_state.get(_CA_SS_POOL_VIEWPORT_BOUNDS)
        if isinstance(bounds_sb, dict) and bounds_sb.get("_southWest") and bounds_sb.get("_northEast"):
            map_ids_sb = _pool_map_ids_in_viewport(df_pool_vis_sb, bounds_sb, max_pins=800)
        else:
            # Sem bounds vindos do Folium (ex.: primeiro paint): não use bbox de todos os pinos —
            # isso marcaria tudo. Alinhe ao que o mapa desenha (amostra até max_pins).
            map_ids_sb = _pool_map_visible_ids_from_df(df_pool_vis_sb, max_pins=800)
        sb["ca_pool_map_sel"] = st.button(
            "Marcar no filtro todos do mapa",
            disabled=not map_ids_sb,
            width="stretch",
            key="ca_pool_map_sel_sb",
            help="Marca os anúncios **visíveis na área atual do mapa** (zoom/pan). Após mudar filtros, interaja com o mapa na página para atualizar o recorte. Se o mapa ainda não devolveu o retângulo, marca só os pinos desenhados (até o limite do mapa).",
        )
        if sb["ca_pool_map_sel"]:
            _ca_set_pool_persist_selection_ids([str(x) for x in map_ids_sb])
            df_disp_sb = df_pool_vis_sb.copy()
            if "id" in df_disp_sb.columns:
                atk_sb = _ca_df_select_widget_key("ca_pool", df_disp_sb, "id")
                want_sb = set(map_ids_sb)
                row_ix_sb = [
                    i for i, rid in enumerate(df_disp_sb["id"].astype(str)) if str(rid) in want_sb
                ]
                st.session_state[atk_sb] = _ca_widget_sel_state(row_ix_sb)
            st.rerun()

    # ────────── Simulador ───────────────────────────────────────────────────
    elif pagina == "🧮 Simulador":
        st.markdown("##### Simulação")
        if st.button("Simular", type="primary", width="stretch", key="sim_sb_simular"):
            st.session_state["_sim_show_result"] = True
        if st.button("Nova simulação", width="stretch", key="sim_sb_nova"):
            st.session_state["_sim_show_result"] = False
            lid = st.session_state.get("_sim_last_iid")
            if isinstance(lid, str) and lid.strip():
                _sim_purge_keys_para_imovel(lid.strip())
            st.rerun()
        st.caption(
            "**Nova simulação** remove os valores editados dos parâmetros do imóvel selecionado e oculta o "
            "resultado até você clicar em **Simular** novamente."
        )


# ── Launch pipeline ───────────────────────────────────────────────────────────
if run_clicked and planilha_path is not None:
    atual = _task_get(st.session_state.get("active_task_id"))
    if atual and atual.get("status") == "running":
        st.warning("Já existe execução em andamento.")
    else:
        tid = str(uuid.uuid4())
        _task_update(tid, status="queued", created_at=time.time(), abort_requested=False)
        threading.Thread(
            target=_exec_background,
            kwargs={"task_id": tid, "modo": "avulso" if modo == "Imóvel avulso" else "planilha",
                    "path": planilha_path, "form": manual_form, "params": common_params,
                    "reprocess": reprocess, "coleta_web": coleta_web, "timeout": int(timeout)},
            daemon=True,
        ).start()
        st.session_state["active_task_id"] = tid
        st.toast("Pipeline iniciado!", icon="🚀")


# ═══════════════════════════════════════════════════════════════════════════════
#  EXECUTION STATUS BANNER (always visible)
# ═══════════════════════════════════════════════════════════════════════════════
def _render_status() -> None:
    tid = st.session_state.get("active_task_id")
    task = _task_get(tid)
    if task and task.get("status") == "running":
        started = float(task.get("started_at") or time.time())
        elapsed = max(0, int(time.time() - started))
        prog = task.get("progress") if isinstance(task.get("progress"), dict) else {}
        fase = str(prog.get("fase") or "iniciando")
        msg = str(prog.get("mensagem") or "Processando...")
        pct = float(prog.get("progress_pct") or 0.0)
        with st.container():
            c1, c2, c3 = st.columns([2, 1, 1])
            c1.info(f"Pipeline em execução — {elapsed}s · {msg}")
            c2.metric("Fase", fase)
            fase_pct = prog.get("fase_pct")
            c3.metric("Progresso", f"{int(float(fase_pct) * 100)}%" if isinstance(fase_pct, (int, float)) else f"{int(pct * 100)}%")
            tmt = int(timeout)
            st.progress(min(1.0, max(0.0, elapsed / tmt)) if tmt > 0 else min(1.0, pct))
            if st.button("Abortar", type="secondary"):
                _task_update(tid, abort_requested=True)
                st.warning("Sinal de aborto enviado.")
    elif task and task.get("status") in ("done", "aborted", "error"):
        st.session_state["ultimo_relatorio"] = task.get("relatorio")
        st.session_state["ultimo_reset"] = task.get("reset_stats")
        st.session_state["ultima_precoleta"] = task.get("precoleta")
        if task["status"] == "done":
            st.success(f"Pipeline concluído em {_fmt_seg(task.get('execution_seconds'))}")
        elif task["status"] == "aborted":
            st.warning(f"Execução interrompida ({_fmt_seg(task.get('execution_seconds'))})")
        else:
            st.error(f"Erro: {task.get('erro', '?')}")
        st.session_state["active_task_id"] = None
        if tid and st.session_state.get("_last_done") != tid:
            st.session_state["_last_done"] = tid
            _refresh()

if hasattr(st, "fragment"):
    @st.fragment(run_every=1)
    def _status_frag():
        _render_status()
    _status_frag()
else:
    _render_status()


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGES
# ═══════════════════════════════════════════════════════════════════════════════

# ── PAGE: Resumo ──────────────────────────────────────────────────────────────
if pagina == "📊 Resumo":
    rel = st.session_state.get("ultimo_relatorio")
    if not rel:
        st.markdown('<div class="empty-state"><div class="empty-icon">📊</div><div class="empty-text">Execute um processamento para ver o resumo aqui.</div></div>', unsafe_allow_html=True)
    else:
        def _fase(r, n):
            for f in r.get("fases", []):
                if f.get("nome") == n:
                    return f
            return {}

        resumo = rel.get("resumo", {}) or {}

        kpi_html = '<div class="kpi-row">'
        kpi_html += _kpi_card(_fmt_n(rel.get("total_urls_planilha", 0), 0), "URLs processadas")
        kpi_html += _kpi_card(_fmt_n(_fase(rel, "ingestao").get("detalhe", {}).get("inseridos", 0), 0), "Inseridos", "positive")
        kpi_html += _kpi_card(_fmt_n(resumo.get("cache_bairro_atualizado", 0), 0), "Cache atualizado")
        kpi_html += _kpi_card(_fmt_n(_fase(rel, "avaliacao_llm").get("detalhe", {}).get("chamadas", 0), 0), "Chamadas LLM")
        kpi_html += _kpi_card(_fmt_n(_fase(rel, "financeiro").get("detalhe", {}).get("processados", 0), 0), "Financeiro")
        kpi_html += _kpi_card(_fmt_seg(rel.get("tempo_execucao_seg")), "Tempo total", "accent")
        kpi_html += '</div>'
        st.markdown(kpi_html, unsafe_allow_html=True)

        fc_post = firecrawl_status()
        acct = firecrawl_account_credits() if fc_post["disponivel"] else None
        fc_html = '<div class="kpi-row">'
        fc_html += _kpi_card("Firecrawl" if fc_post["ativo"] else "DDGS", "Motor de busca")
        fc_html += _kpi_card(f'{_fmt_n(fc_post["credits_used_session"], 0)}/{_fmt_n(fc_post["credit_limit_session"], 0)}', "Créditos usados")
        fc_html += _kpi_card(_fmt_n(acct["remaining"], 0) if acct and acct.get("remaining") is not None else "n/a", "Saldo conta", "accent")
        fc_html += '</div>'
        st.markdown(fc_html, unsafe_allow_html=True)

        pre = st.session_state.get("ultima_precoleta")
        if pre and pre.get("ok"):
            st.success(f"Coleta web salvou {pre.get('salvos', 0)} anúncio(s)")
        rst = st.session_state.get("ultimo_reset")
        if rst:
            st.info(f"Reprocessamento: {rst.get('resetados', 0)}/{rst.get('candidatos', 0)} resetados")

        with st.expander("📋 Detalhes das fases"):
            fases = []
            for f in rel.get("fases", []):
                d = f.get("detalhe") if isinstance(f.get("detalhe"), dict) else {}
                linha = {"fase": f.get("nome", "")}
                linha.update(d)
                fases.append(linha)
            if fases:
                st.dataframe(_fmt_df(pd.DataFrame(fases)), width="stretch", hide_index=True)

        with st.expander("🔍 JSON bruto"):
            st.json(rel)


# ── PAGE: Leilões ─────────────────────────────────────────────────────────────
elif pagina == "🏠 Leilões":
    try:
        df_i = _query_table(SUPABASE_TABLE, limit=500)
        if df_i.empty:
            st.markdown('<div class="empty-state"><div class="empty-icon">🏠</div><div class="empty-text">Nenhum imóvel cadastrado ainda.</div></div>', unsafe_allow_html=True)
        else:
            has_data_leilao = "data_leilao" in df_i.columns
            has_arrematado_final = "valor_arrematado_final" in df_i.columns
            hoje = pd.Timestamp(datetime.now().date())

            if has_data_leilao:
                df_i["data_leilao"] = pd.to_datetime(df_i["data_leilao"], errors="coerce").dt.tz_localize(None)
            if "roi_projetado" in df_i.columns:
                df_i["roi_projetado"] = pd.to_numeric(df_i["roi_projetado"], errors="coerce")

            lei_fd = _lei_table_filters_expander_ui(key_prefix="lei_tbl", title="Filtros da tabela (leilões)", expanded=True)
            df_i = df_i[_ca_mask_leilao_like(df_i, **lei_fd)].reset_index(drop=True)

            if has_data_leilao and not sb.get("lei_passados", False):
                df_i = df_i[(df_i["data_leilao"].isna()) | (df_i["data_leilao"] >= hoje)]

            if "status" in df_i.columns and not sb.get("lei_descartados", False):
                df_i = df_i[~df_i["status"].astype(str).str.startswith("descartado")]

            roi_min = sb.get("lei_roi_min", -100.0)
            if "roi_projetado" in df_i.columns:
                df_i = df_i[df_i["roi_projetado"].fillna(roi_min) >= roi_min]

            status_opts = sorted(df_i["status"].dropna().unique().tolist()) if "status" in df_i.columns else []
            status_sel = st.selectbox("Status", ["Todos"] + status_opts, key="lei_status", label_visibility="collapsed")
            if status_sel != "Todos" and "status" in df_i.columns:
                df_i = df_i[df_i["status"] == status_sel]

            sort_opts = {"Data leilão (próximos)": ("data_leilao", True), "ROI (maior)": ("roi_projetado", False),
                         "ROI (menor)": ("roi_projetado", True), "Lance (menor)": ("valor_arrematacao", True),
                         "Lance (maior)": ("valor_arrematacao", False)}
            sort_sel = sb.get("lei_sort", "Data leilão (próximos)")
            sort_col, sort_asc = sort_opts.get(sort_sel, ("data_leilao", True))
            if sort_col in df_i.columns:
                df_i = df_i.sort_values(sort_col, ascending=sort_asc, na_position="last")

            if df_i.empty:
                st.markdown('<div class="empty-state"><div class="empty-icon">🔍</div><div class="empty-text">Nenhum imóvel atende os filtros selecionados.</div></div>', unsafe_allow_html=True)
            else:
                # ── KPIs (HTML cards) ──
                roi_col = pd.to_numeric(df_i.get("roi_projetado"), errors="coerce").dropna()
                positivos = int((roi_col > 0).sum()) if not roi_col.empty else 0
                pct_pos = int(positivos / len(df_i) * 100) if len(df_i) > 0 else 0
                proximos = int(df_i[df_i["data_leilao"] >= hoje].shape[0]) if has_data_leilao else 0

                kpi_html = '<div class="kpi-row">'
                kpi_html += _kpi_card(str(len(df_i)), "Total leilões")
                kpi_html += _kpi_card(f"{positivos} ({pct_pos}%)", "ROI positivo", "positive" if pct_pos > 50 else "")
                kpi_html += _kpi_card(f"{_fmt_n(roi_col.mean(), 1)}%" if not roi_col.empty else "-", "ROI médio", "positive" if not roi_col.empty and roi_col.mean() > 0 else "negative")
                kpi_html += _kpi_card(f"{_fmt_n(roi_col.max(), 1)}%" if not roi_col.empty else "-", "Melhor ROI", "accent")
                kpi_html += _kpi_card(str(proximos), "Leilões futuros")
                kpi_html += '</div>'
                st.markdown(kpi_html, unsafe_allow_html=True)

                # ── Two-panel layout: Lista | Detalhe ──
                df_i = df_i.reset_index(drop=True)
                col_lista, col_detalhe = st.columns([1.4, 1], gap="medium")

                with col_lista:
                    df_show = df_i.copy()
                    if has_data_leilao:
                        df_show["data_leilao"] = df_show["data_leilao"].dt.strftime("%d/%m/%Y").fillna("")
                    list_cols = [c for c in [
                        "data_leilao", "cidade", "bairro", "tipo_imovel",
                        "area_util", "area_total", "valor_arrematacao", "roi_projetado", "status",
                    ] if c in df_show.columns]

                    lei_list_cfg: dict[str, Any] = {
                        "data_leilao": st.column_config.TextColumn("Data"),
                        "cidade": st.column_config.TextColumn("Cidade"),
                        "bairro": st.column_config.TextColumn("Bairro"),
                        "tipo_imovel": st.column_config.TextColumn("Tipo"),
                        "area_util": st.column_config.TextColumn("Á. útil"),
                        "area_total": st.column_config.TextColumn("Á. total"),
                        "valor_arrematacao": st.column_config.TextColumn("Lance"),
                        "roi_projetado": st.column_config.TextColumn("ROI"),
                        "status": st.column_config.TextColumn("Status"),
                    }

                    selection = st.dataframe(
                        _fmt_df(df_show[list_cols]),
                        width="stretch", height=560, hide_index=True,
                        column_config=lei_list_cfg,
                        on_select="rerun", selection_mode="single-row",
                        key="lei_table_sel",
                    )

                    selected_rows = selection.get("selection", {}).get("rows", []) if isinstance(selection, dict) else []
                    selected_idx = selected_rows[0] if selected_rows else None

                    st.download_button("📥 Baixar CSV", df_i.to_csv(index=False).encode("utf-8"),
                                       "leilao_imoveis.csv", "text/csv", use_container_width=True)

                with col_detalhe:
                    if selected_idx is not None and selected_idx < len(df_i):
                        row = df_i.iloc[selected_idx]
                        rid = str(row.get("id") or "")

                        # ── Header card ──
                        d_cidade = _safe_str(row.get("cidade"), "-")
                        d_bairro = _safe_str(row.get("bairro"), "-")
                        d_estado = _safe_str(row.get("estado"), "-")
                        d_tipo = _safe_str(row.get("tipo_imovel"), "-").replace("_", " ").title()
                        url_lei = str(row.get("url_leilao") or "").strip()
                        d_data = row.get("data_leilao") if has_data_leilao else None
                        d_data_str = d_data.strftime("%d/%m/%Y") if pd.notna(d_data) else "-"
                        is_passado = has_data_leilao and pd.notna(d_data) and d_data < hoje

                        roi_v = row.get("roi_projetado")
                        header_badges = _roi_badge(roi_v) + " " + _status_badge(str(row.get("status") or ""))
                        if is_passado:
                            header_badges += " " + _badge("Encerrado", "gray")

                        link_html = f' <a href="{url_lei}" target="_blank" style="color:#4da6ff;font-size:0.78rem;text-decoration:none;">🔗 Abrir leilão</a>' if url_lei else ""
                        _d_area_total = _to_float(row.get("area_total"))
                        if d_tipo == "terreno":
                            _area_sub = f"Terreno {_fmt_n(_d_area_total, 1)} m²" if _d_area_total > 0 else "Terreno"
                        else:
                            _area_sub = f'{_fmt_n(row.get("area_util"), 1)} m²'
                            if _d_area_total > 0:
                                _area_sub += f' · Terreno {_fmt_n(_d_area_total, 1)} m²'
                        header_html = f'''<div class="lei-header">
                            <div class="lei-location">{d_cidade} / {d_bairro} — {d_estado}{link_html}</div>
                            <div class="lei-sub">{d_tipo} · {_area_sub} · {d_data_str} {header_badges}</div>
                        </div>'''
                        st.markdown(header_html, unsafe_allow_html=True)

                        # ── Info grid: lance + análise ──
                        lance_v = _to_float(row.get("valor_arrematacao"))
                        vm = _to_float(row.get("valor_mercado_estimado"))
                        vs = _to_float(row.get("valor_venda_sugerido"))
                        lm = _to_float(row.get("lance_maximo_recomendado"))
                        vl = _to_float(row.get("valor_venda_liquido"))
                        repos = row.get("potencial_reposicionamento_pct")

                        grid = '<div class="info-grid">'
                        grid += _info_item("Lance", _fmt_brl(lance_v))
                        grid += _info_item("Valor mercado", _fmt_brl(vm) if vm > 0 else "-")
                        grid += _info_item("Venda sugerida", _fmt_brl(vs) if vs > 0 else "-")
                        grid += _info_item("Lance máximo", _fmt_brl(lm) if lm > 0 else "-")
                        grid += _info_item("Venda líquida", _fmt_brl(vl) if vl > 0 else "-")
                        grid += _info_item("ROI projetado", f"{_fmt_n(roi_v, 1)}%" if pd.notna(roi_v) else "-")
                        grid += '</div>'
                        st.markdown(grid, unsafe_allow_html=True)

                        alerta = row.get("alerta_precificacao_baixa_amostragem")
                        if alerta:
                            st.warning(f"⚠️ {alerta}")

                        with st.expander("🗺️ Mapa — leilão e comparáveis do cache", expanded=True):
                            _lei_render_mapa_folium(row)

                        # ── Tabs: Cache | Recalcular | Resultado ──
                        tab_cache, tab_recalc, tab_resultado = st.tabs(["📍 Cache do bairro", "🔄 Recalcular", "🏷️ Resultado"])

                        with tab_cache:
                            _df_cache_lei = _df_cache_para_linha_leilao(row)
                            _lid_v = str(row.get("cache_media_bairro_id") or "").strip()

                            if _df_cache_lei.empty:
                                st.markdown('<div class="empty-state"><div class="empty-icon">📍</div><div class="empty-text">Nenhum cache encontrado para este bairro.</div></div>', unsafe_allow_html=True)
                            else:
                                _cr = _df_cache_lei.iloc[0]
                                _cr_bairro = _safe_str(_cr.get("bairro"), "-")
                                _cr_rel = _cr.get("_relevancia", 0.0)
                                _cr_tag = "exato" if _cr_rel >= 0.95 else "similar" if _cr_rel >= 0.5 else "região"
                                _cr_n_raw = _to_float(_cr.get("n_amostras"))
                                _cr_n = int(_cr_n_raw) if _cr_n_raw and not pd.isna(_cr_n_raw) else 0
                                _tag_variant = "green" if _cr_tag == "exato" else "yellow" if _cr_tag == "similar" else "blue"
                                _cr_id_s = str(_cr.get("id") or "").strip()
                                if _lid_v and _cr_id_s == _lid_v:
                                    st.caption("Cache vinculado a esta análise (gravado no imóvel).")

                                st.markdown(f'{_badge(_cr_tag, _tag_variant)} **{_cr_bairro}** · {_cr_n} amostra(s)', unsafe_allow_html=True)

                                _cr_pm2 = _to_float(_cr.get("preco_m2_medio"))
                                _cr_vmv = _to_float(_cr.get("valor_medio_venda"))
                                _cr_max = _to_float(_cr.get("maior_valor_venda"))
                                _cr_min = _to_float(_cr.get("menor_valor_venda"))

                                cache_grid = '<div class="info-grid">'
                                cache_grid += _info_item("R$/m²", _fmt_brl(_cr_pm2) if _cr_pm2 > 0 else "-")
                                cache_grid += _info_item("Venda média", _fmt_brl(_cr_vmv) if _cr_vmv > 0 else "-")
                                cache_grid += _info_item("Maior valor", _fmt_brl(_cr_max) if _cr_max > 0 else "-")
                                cache_grid += _info_item("Menor valor", _fmt_brl(_cr_min) if _cr_min > 0 else "-")
                                cache_grid += '</div>'
                                st.markdown(cache_grid, unsafe_allow_html=True)

                                if len(_df_cache_lei) > 1:
                                    with st.expander(f"Mais {len(_df_cache_lei) - 1} cache(s) encontrado(s)"):
                                        for _ci in range(1, min(len(_df_cache_lei), 6)):
                                            _cr2 = _df_cache_lei.iloc[_ci]
                                            _r2 = _cr2.get("_relevancia", 0.0)
                                            _t2 = "exato" if _r2 >= 0.95 else "similar" if _r2 >= 0.5 else "região"
                                            _t2v = "green" if _t2 == "exato" else "yellow" if _t2 == "similar" else "blue"
                                            _b2 = _safe_str(_cr2.get("bairro"), "-")
                                            _p2 = _to_float(_cr2.get("preco_m2_medio"))
                                            _v2 = _to_float(_cr2.get("valor_medio_venda"))
                                            _mx2 = _to_float(_cr2.get("maior_valor_venda"))
                                            _mn2 = _to_float(_cr2.get("menor_valor_venda"))
                                            st.markdown(
                                                f'{_badge(_t2, _t2v)} **{_b2}** — '
                                                f'R$/m²: {_fmt_brl(_p2) if _p2 > 0 else "-"} · '
                                                f'Média: {_fmt_brl(_v2) if _v2 > 0 else "-"} · '
                                                f'Maior: {_fmt_brl(_mx2) if _mx2 > 0 else "-"} · '
                                                f'Menor: {_fmt_brl(_mn2) if _mn2 > 0 else "-"}',
                                                unsafe_allow_html=True,
                                            )

                        with tab_recalc:
                            area_rc = _to_float(area_efetiva_de_registro(row))
                            lance_rc = _to_float(row.get("valor_arrematacao"))

                            if area_rc <= 0:
                                st.warning("Área não cadastrada — não é possível recalcular.")
                            elif lance_rc <= 0:
                                st.warning("Valor de arrematação não cadastrado.")
                            else:
                                rc_mode = st.radio(
                                    "Origem do R$/m²",
                                    ["Cache do bairro", "Valor manual"],
                                    horizontal=True,
                                    key=f"rc_mode_{rid[:8]}",
                                )

                                preco_m2_rc: float = 0.0

                                if rc_mode == "Cache do bairro":
                                    df_cache = _df_cache_para_linha_leilao(row)
                                    if df_cache.empty:
                                        st.info("Nenhum cache encontrado.")
                                    else:
                                        cache_options: list[str] = []
                                        cache_map: dict[str, float] = {}
                                        cache_id_map: dict[str, str] = {}
                                        for _, cr in df_cache.iterrows():
                                            pm2 = _to_float(cr.get("preco_m2_medio"))
                                            if pm2 <= 0:
                                                continue
                                            c_bairro = _safe_str(cr.get("bairro"), "-")
                                            c_cidade = _safe_str(cr.get("cidade"), "-")
                                            c_tipo = _safe_str(cr.get("tipo_imovel"), "-")
                                            c_fonte = _safe_str(cr.get("fonte"), "-")
                                            rel = cr.get("_relevancia", 0.0)
                                            tag = "exato" if rel >= 0.95 else "similar" if rel >= 0.5 else "região"
                                            lbl = f"[{tag}] {c_bairro} ({c_cidade}) | {c_tipo} | {_fmt_brl(pm2)}/m² ({c_fonte})"
                                            cache_options.append(lbl)
                                            cache_map[lbl] = pm2
                                            cid_row = str(cr.get("id") or "").strip()
                                            if cid_row:
                                                cache_id_map[lbl] = cid_row
                                        if cache_options:
                                            _lid_rc = str(row.get("cache_media_bairro_id") or "").strip()
                                            _def_ix = 0
                                            if _lid_rc:
                                                for _ix, _lb in enumerate(cache_options):
                                                    if cache_id_map.get(_lb) == _lid_rc:
                                                        _def_ix = _ix
                                                        break
                                            sel_cache = st.selectbox(
                                                "Cache disponível",
                                                cache_options,
                                                index=_def_ix,
                                                key=f"rc_cache_{rid[:8]}",
                                            )
                                            preco_m2_rc = cache_map.get(sel_cache, 0.0)
                                        else:
                                            st.info("Nenhum cache com preço/m² válido.")
                                else:
                                    preco_m2_rc = st.number_input("R$/m² manual", min_value=0.0, value=0.0, step=100.0, key=f"rc_m2_{rid[:8]}")

                                if preco_m2_rc > 0:
                                    novo_valor_mercado = round(preco_m2_rc * area_rc, 2)

                                    rcc1, rcc2 = st.columns(2)
                                    rc_com_lei = rcc1.number_input("Com. leiloeiro (%)", 0.0, 100.0, 5.0, 0.5, key=f"rc_cl_{rid[:8]}")
                                    rc_itbi = rcc2.number_input("ITBI (%)", 0.0, 100.0, 3.0, 0.5, key=f"rc_itbi_{rid[:8]}")
                                    rcc3, rcc4 = st.columns(2)
                                    rc_registro = rcc3.number_input("Registro (R$)", 0.0, value=round(lance_rc * 0.035, 2), step=500.0, key=f"rc_reg_{rid[:8]}")
                                    rc_reforma = rcc4.number_input("Reforma (R$)", 0.0, value=_to_float(row.get("custo_reforma_estimado")), step=1000.0, key=f"rc_ref_{rid[:8]}")
                                    rc_fat_liq = 0.92

                                    try:
                                        ent_rc = RoiCalculoEntrada(
                                            valor_lance=lance_rc,
                                            valor_venda_estimado=novo_valor_mercado,
                                            custo_reforma=rc_reforma,
                                            comissao_leiloeiro_pct=rc_com_lei,
                                            itbi_pct=rc_itbi,
                                            custos_registro=rc_registro,
                                            fator_liquidez_venda=rc_fat_liq,
                                        )
                                        res_rc = calcular_roi_liquido(ent_rc)
                                        lm_rc = calcular_lance_maximo_para_roi(
                                            valor_venda_estimado=novo_valor_mercado,
                                            roi_objetivo_pct=25.0,
                                            custo_reforma=rc_reforma,
                                            comissao_leiloeiro_pct=rc_com_lei,
                                            itbi_pct=rc_itbi,
                                            custos_registro=rc_registro,
                                            fator_liquidez_venda=rc_fat_liq,
                                        )

                                        roi_novo = res_rc.roi_liquido_pct
                                        rc_grid = '<div class="info-grid">'
                                        rc_grid += _info_item("ROI recalculado", f"{_fmt_n(roi_novo, 1)}%")
                                        rc_grid += _info_item("Valor mercado", _fmt_brl(novo_valor_mercado))
                                        rc_grid += _info_item("Lance máx (25%)", _fmt_brl(lm_rc))
                                        rc_grid += _info_item("Venda líquida", _fmt_brl(res_rc.valor_venda_liquido))
                                        rc_grid += '</div>'
                                        st.markdown(rc_grid, unsafe_allow_html=True)

                                        if st.button("💾 Salvar análise recalculada", type="primary", use_container_width=True, key=f"rc_save_{rid[:8]}"):
                                            campos_atualizar = {
                                                "valor_mercado_estimado": novo_valor_mercado,
                                                "valor_venda_sugerido": novo_valor_mercado,
                                                "roi_projetado": round(roi_novo, 4),
                                                "lance_maximo_recomendado": lm_rc,
                                                "valor_venda_liquido": res_rc.valor_venda_liquido,
                                                "custo_reforma_estimado": rc_reforma,
                                                "status": "analisado",
                                            }
                                            if rc_mode == "Cache do bairro" and "cache_id_map" in dir() and "sel_cache" in dir():
                                                try:
                                                    _cid_sv = cache_id_map.get(sel_cache)
                                                    if _cid_sv:
                                                        campos_atualizar["cache_media_bairro_id"] = _cid_sv
                                                except Exception:
                                                    pass
                                            elif rc_mode == "Valor manual":
                                                campos_atualizar["cache_media_bairro_id"] = None
                                            atualizar_leilao_imovel_campos(rid, campos_atualizar)
                                            st.success("Análise recalculada e salva!")
                                            _query_table.clear()
                                            _refresh()
                                    except Exception as e_rc:
                                        st.error(f"Erro no cálculo: {e_rc}")

                        with tab_resultado:
                            if not has_arrematado_final:
                                st.warning("Coluna `valor_arrematado_final` não existe no banco.")
                                st.code("ALTER TABLE public.leilao_imoveis ADD COLUMN IF NOT EXISTS valor_arrematado_final double precision;", language="sql")
                            else:
                                val_atual = _to_float(row.get("valor_arrematado_final"))
                                val_arr_final = st.number_input(
                                    "Valor arrematado (R$)", min_value=0.0,
                                    value=val_atual, step=1000.0, key=f"lei_arr_v_{rid[:8]}",
                                )
                                if val_arr_final > 0 and row.get("valor_arrematacao"):
                                    diff = val_arr_final - _to_float(row.get("valor_arrematacao"))
                                    diff_pct = (diff / _to_float(row.get("valor_arrematacao"))) * 100 if _to_float(row.get("valor_arrematacao")) > 0 else 0
                                    variant = "green" if diff_pct <= 0 else "red"
                                    st.markdown(f'Diferença do lance: **{_fmt_brl(diff)}** {_badge(f"{"+" if diff_pct > 0 else ""}{_fmt_n(diff_pct, 1)}%", variant)}', unsafe_allow_html=True)
                                if st.button("💾 Salvar valor arrematado", type="primary", use_container_width=True, key=f"lei_arr_s_{rid[:8]}"):
                                    if rid and val_arr_final > 0:
                                        atualizar_leilao_imovel_campos(rid, {"valor_arrematado_final": val_arr_final})
                                        st.success(f"Salvo: {_fmt_brl(val_arr_final)}")
                                        _query_table.clear()
                                        _refresh()
                                    else:
                                        st.warning("Informe um valor maior que zero.")
                    else:
                        st.markdown('<div class="empty-state"><div class="empty-icon">👈</div><div class="empty-text">Selecione um leilão na lista para ver os detalhes.</div></div>', unsafe_allow_html=True)

    except Exception as exc:
        st.exception(exc)


# ── PAGE: Anúncios ────────────────────────────────────────────────────────────
elif pagina == "📋 Anúncios":
    soft_ok = _anuncios_soft_delete_ok()

    limit_an = sb.get("an_lim", 250)
    show_arq = sb.get("an_arq", False)
    sel_all = sb.get("an_all", False)
    prev_sa = st.session_state.get("_an_prev_sel_all_sb")
    if prev_sa is True and not sel_all:
        st.session_state[_AN_SS_SEL_IDS] = []
    st.session_state["_an_prev_sel_all_sb"] = bool(sel_all)

    try:
        _an_consume_map_sel_query_param()
        df_an = _query_anuncios(limit=limit_an, include_arq=show_arq)
        if df_an.empty:
            st.markdown('<div class="empty-state"><div class="empty-icon">📋</div><div class="empty-text">Nenhum anúncio encontrado.</div></div>', unsafe_allow_html=True)
        else:
            an_fd = _ca_pool_filters_expander_ui(
                key_prefix="an_tbl",
                title="Filtros da tabela (anúncios)",
                expanded=True,
            )
            df_an = df_an[_ca_mask_pool_like(df_an, **an_fd)].reset_index(drop=True)
            if soft_ok and not show_arq and "arquivado_em" in df_an.columns:
                df_an = df_an[df_an["arquivado_em"].isna()]

            sel_ids: list[str] = []
            if not df_an.empty:
                pm2_all = pd.to_numeric(df_an.get("preco_m2"), errors="coerce").dropna() if "preco_m2" in df_an.columns else pd.Series(dtype=float)
                kpi_html = '<div class="kpi-row">'
                kpi_html += _kpi_card(str(len(df_an)), "Total anúncios")
                kpi_html += _kpi_card(_fmt_brl(pm2_all.mean()) if not pm2_all.empty else "-", "Média R$/m²")
                kpi_html += _kpi_card(_fmt_brl(pm2_all.median()) if not pm2_all.empty else "-", "Mediana R$/m²")
                kpi_html += _kpi_card(f"{_fmt_brl(pm2_all.min())} – {_fmt_brl(pm2_all.max())}" if not pm2_all.empty else "-", "Faixa R$/m²")
                kpi_html += '</div>'
                st.markdown(kpi_html, unsafe_allow_html=True)

                an_tbl_key = _ca_df_select_widget_key("an_list", df_an, "id")
                vid = set(df_an["id"].astype(str))
                _ca_prune_persist_to_valid_ids(_AN_SS_SEL_IDS, vid)
                _an_map_touch_viewport_cache(df_an)

                with st.expander("🗺️ Mapa dos anúncios (lista filtrada)", expanded=True):
                    _pool_render_mapa_folium(
                        df_an,
                        cache_map_select_mode=True,
                        merge_ids_session_key="an_ids_from_map",
                        viewport_bounds_state_key=_AN_SS_MAP_VIEWPORT_BOUNDS,
                    )

                merged_map = _an_merge_map_buffer_into_persist(vid)
                if sel_all and "id" in df_an.columns:
                    st.session_state[_AN_SS_SEL_IDS] = df_an["id"].astype(str).tolist()
                _ca_maybe_seed_df_selection(an_tbl_key, df_an, "id", _AN_SS_SEL_IDS)
                list_fp = hashlib.md5(",".join(sorted(vid)).encode()).hexdigest()[:20]
                prev_list_fp = st.session_state.get("_an_list_sel_fp")
                ps_set = {str(x) for x in (st.session_state.get(_AN_SS_SEL_IDS) or [])}
                if sel_all and "id" in df_an.columns:
                    st.session_state[an_tbl_key] = _ca_widget_sel_state(list(range(len(df_an))))
                elif merged_map or not ps_set or prev_list_fp != list_fp:
                    st.session_state["_an_list_sel_fp"] = list_fp
                    _an_sync_dataframe_selection_from_persist(df_an, an_tbl_key)

                an_show_cols = [c for c in [
                    "ultima_coleta_em", "cidade", "bairro", "estado",
                    "tipo_imovel", "valor_venda", "preco_m2", "area_construida_m2",
                    "quartos", "url_anuncio",
                ] if c in df_an.columns]
                if show_arq:
                    for extra in ("arquivado_em", "arquivado_motivo"):
                        if extra in df_an.columns and extra not in an_show_cols:
                            an_show_cols.append(extra)

                df_an_show = df_an[an_show_cols].copy()
                if "ultima_coleta_em" in df_an_show.columns:
                    df_an_show["ultima_coleta_em"] = (
                        pd.to_datetime(df_an_show["ultima_coleta_em"], errors="coerce").dt.strftime("%d/%m/%Y").fillna("")
                    )

                an_cfg: dict[str, Any] = {}
                if "url_anuncio" in df_an_show.columns:
                    an_cfg["url_anuncio"] = st.column_config.LinkColumn("Link", display_text="Abrir")
                for _anc, _lab in (
                    ("valor_venda", "Valor venda"),
                    ("preco_m2", "R$/m²"),
                    ("area_construida_m2", "Área m²"),
                ):
                    if _anc in df_an_show.columns:
                        an_cfg[_anc] = st.column_config.NumberColumn(_lab, format="%.2f")

                ev_an = st.dataframe(
                    df_an_show,
                    hide_index=True,
                    width="stretch",
                    height=520,
                    column_config=an_cfg if an_cfg else None,
                    key=an_tbl_key,
                    on_select="rerun",
                    selection_mode="multi-row",
                )
                sel_ix = _dataframe_selection_rows(ev_an)
                wk = st.session_state.get(an_tbl_key)
                sel_ix_ss: list[int] = []
                if isinstance(wk, dict):
                    try:
                        sel_ix_ss = [int(x) for x in (wk.get("selection") or {}).get("rows") or []]
                    except (TypeError, ValueError):
                        sel_ix_ss = []
                n_df = len(df_an)
                row_ix_use = [
                    int(i) for i in (sel_ix if sel_ix else sel_ix_ss) if 0 <= int(i) < n_df
                ]
                if "id" in df_an.columns:
                    if sel_all:
                        sel_ids = df_an["id"].astype(str).tolist()
                    elif row_ix_use:
                        sel_ids = df_an.iloc[row_ix_use]["id"].astype(str).tolist()
                    else:
                        sel_ids = []
                    st.session_state[_AN_SS_SEL_IDS] = list(sel_ids)
            else:
                st.session_state[_AN_SS_SEL_IDS] = []
                st.session_state["an_ids_from_map"] = set()

            if sel_ids:
                sel_df = df_an[df_an["id"].isin(sel_ids)].copy() if "id" in df_an.columns else pd.DataFrame()
                if not sel_df.empty:
                    pm2_v = _series_to_numeric(sel_df.get("preco_m2"))
                    if "area_construida_m2" in sel_df.columns and "valor_venda" in sel_df.columns:
                        a = _series_to_numeric(sel_df["area_construida_m2"])
                        v = _series_to_numeric(sel_df["valor_venda"])
                        pm2_v = pm2_v.fillna((v / a).where(a > 0))
                    pm2_v = pm2_v.dropna()
                    vendas_v = _series_to_numeric(sel_df.get("valor_venda")).dropna()
                    sel_html = '<div class="kpi-row">'
                    sel_html += _kpi_card(str(len(sel_ids)), "Selecionados", "accent")
                    sel_html += _kpi_card(_fmt_brl(pm2_v.mean()) if not pm2_v.empty else "-", "Média R$/m² (sel.)")
                    sel_html += _kpi_card(_fmt_brl(pm2_v.median()) if not pm2_v.empty else "-", "Mediana R$/m² (sel.)")
                    sel_html += _kpi_card(_fmt_brl(float(vendas_v.max())) if not vendas_v.empty else "-", "Maior valor de venda (sel.)")
                    sel_html += _kpi_card(_fmt_brl(float(vendas_v.min())) if not vendas_v.empty else "-", "Menor valor de venda (sel.)")
                    sel_html += '</div>'
                    st.markdown(sel_html, unsafe_allow_html=True)

            # Ações (sidebar + página)
            if sb.get("an_del"):
                if not sel_ids:
                    st.warning("Selecione anúncios primeiro.")
                else:
                    n = _arquivar_anuncios(sel_ids) if soft_ok else _delete_anuncios(sel_ids)
                    st.success(f"{'Arquivados' if soft_ok else 'Excluídos'}: {n}")
                    _query_anuncios.clear()
                    st.rerun()
            if st.button("Recalcular cache", type="primary", width="stretch", key="an_recalc_cache_btn"):
                if not sel_ids:
                    st.warning("Selecione anúncios primeiro.")
                elif not (an_recalc_cache_nome or "").strip():
                    st.warning("Informe o nome do cache na barra lateral antes de recalcular.")
                else:
                    res = _recalcular_cache(sel_ids, nome_cache_usuario=(an_recalc_cache_nome or "").strip())
                    st.success(f"Cache atualizado: {res.get('cache_atualizado', 0)} grupo(s)")
                    if res.get("detalhes"):
                        st.dataframe(pd.DataFrame(res["detalhes"]), hide_index=True, width="stretch")

            # Restaurar arquivados
            if soft_ok:
                with st.expander("Restaurar arquivados"):
                    dias = st.slider("Janela (dias)", 1, 90, 15, key="an_dias")
                    df_arch = _query_anuncios(limit=limit_an, only_arq=True)
                    if not df_arch.empty:
                        if "arquivado_em" in df_arch.columns:
                            ts = pd.to_datetime(df_arch["arquivado_em"], errors="coerce", utc=True)
                            df_arch = df_arch[ts >= pd.Timestamp(datetime.now(timezone.utc) - timedelta(days=dias))]
                        arch_fd = _ca_pool_filters_dict_from_state("an_tbl")
                        df_arch = df_arch[_ca_mask_pool_like(df_arch, **arch_fd)].reset_index(drop=True)
                        if not df_arch.empty:
                            df_arch = df_arch.copy()
                            df_arch["restaurar"] = False
                            cols_a = [c for c in ["restaurar", "arquivado_em", "cidade", "bairro", "tipo_imovel", "valor_venda", "url_anuncio"] if c in df_arch.columns]
                            arch_cfg: dict[str, Any] = {}
                            if "url_anuncio" in df_arch.columns:
                                arch_cfg["url_anuncio"] = st.column_config.LinkColumn("Link", display_text="Abrir")
                            ed_a = st.data_editor(
                                _fmt_df(df_arch[cols_a]), hide_index=True, width="stretch",
                                key="an_arch_ed", disabled=[c for c in cols_a if c != "restaurar"],
                                column_config=arch_cfg,
                            )
                            ids_rest = ed_a.loc[ed_a["restaurar"] == True, "id"].astype(str).tolist() if "id" in ed_a.columns else []  # noqa: E712
                            if "id" not in ed_a.columns and "id" in df_arch.columns:
                                mask_r = ed_a["restaurar"] == True  # noqa: E712
                                ids_rest = df_arch.loc[mask_r.values, "id"].astype(str).tolist() if mask_r.any() else []
                            if st.button("Restaurar selecionados", width="stretch", key="an_restore"):
                                if ids_rest:
                                    nr = _restaurar_anuncios(ids_rest)
                                    st.success(f"Restaurados: {nr}")
                                    _query_anuncios.clear()
                                    st.rerun()
                                else:
                                    st.warning("Selecione anúncios.")
                        else:
                            st.info("Nenhum arquivado na janela selecionada.")
                    else:
                        st.info("Nenhum arquivado encontrado.")

        # ── Importar anúncios (JSON ou avulso) ──
        st.divider()
        with st.expander("Importar anúncios manualmente", expanded=False):
            import_mode = st.radio("Modo de importação", ["Arquivo JSON", "Anúncio avulso"], horizontal=True, key="an_import_mode")

            if import_mode == "Arquivo JSON":
                uploaded = st.file_uploader("Selecione o arquivo JSON", type=["json"], key="an_json_upload")
                if uploaded is not None:
                    try:
                        raw_json = json.loads(uploaded.getvalue().decode("utf-8"))
                        items = raw_json if isinstance(raw_json, list) else [raw_json]
                    except (json.JSONDecodeError, UnicodeDecodeError) as je:
                        st.error(f"Erro ao ler JSON: {je}")
                        items = []

                    if items:
                        parsed = [_json_anuncio_to_row(it) for it in items]
                        errors: list[str] = []
                        for i, p in enumerate(parsed):
                            errs = []
                            if not p.get("url_anuncio"):
                                errs.append("URL ausente")
                            if p.get("valor_venda", 0) <= 0:
                                errs.append("preço inválido")
                            if p.get("area_construida_m2", 0) <= 0:
                                errs.append("área inválida")
                            if not p.get("cidade"):
                                errs.append("cidade ausente")
                            if not p.get("bairro"):
                                errs.append("bairro ausente")
                            p["_validation_errors"] = errs
                            p["_valid"] = len(errs) == 0

                        df_import = pd.DataFrame(parsed)
                        df_import.insert(0, "importar", True)

                        invalid_count = int((~df_import["_valid"]).sum())
                        if invalid_count > 0:
                            st.warning(f"{invalid_count} anúncio(s) com problemas (marcados em vermelho na coluna 'erros').")

                        display_cols = ["importar", "cidade", "bairro", "estado", "tipo_imovel",
                                        "area_construida_m2", "valor_venda", "preco_m2", "quartos", "url_anuncio"]
                        display_cols = [c for c in display_cols if c in df_import.columns]
                        if "_validation_errors" in df_import.columns:
                            df_import["erros"] = df_import["_validation_errors"].apply(lambda x: ", ".join(x) if x else "")
                            display_cols.append("erros")

                        imp_cfg: dict[str, Any] = {}
                        if "url_anuncio" in df_import.columns:
                            imp_cfg["url_anuncio"] = st.column_config.LinkColumn("Link", display_text="Abrir")

                        df_import_view = df_import[display_cols].copy()
                        for _ic in ("valor_venda", "preco_m2", "area_construida_m2"):
                            if _ic in df_import_view.columns:
                                df_import_view[_ic] = df_import_view[_ic].map(
                                    lambda x: _fmt_n(x, 2) if pd.notna(x) and x else ""
                                )

                        edited_import = st.data_editor(
                            df_import_view,
                            hide_index=True, width="stretch", height=min(400, 60 + len(df_import) * 36),
                            key="an_import_ed",
                            disabled=[c for c in display_cols if c != "importar"],
                            column_config=imp_cfg,
                        )

                        selected_mask = edited_import["importar"] == True  # noqa: E712
                        valid_mask = df_import["_valid"]
                        to_save_mask = selected_mask & valid_mask.values
                        n_selected = int(selected_mask.sum())
                        n_saveable = int(to_save_mask.sum())

                        imp_kpi = '<div class="kpi-row">'
                        imp_kpi += _kpi_card(str(len(df_import)), "Total no arquivo")
                        imp_kpi += _kpi_card(str(n_selected), "Selecionados", "accent")
                        imp_kpi += _kpi_card(str(n_saveable), "Válidos p/ gravar", "positive" if n_saveable > 0 else "")
                        imp_kpi += '</div>'
                        st.markdown(imp_kpi, unsafe_allow_html=True)

                        if st.button("Gravar selecionados", type="primary", width="stretch", key="an_import_save"):
                            if n_saveable == 0:
                                st.warning("Nenhum anúncio válido selecionado.")
                            else:
                                from geocoding import geocodificar_anuncios_batch

                                cli = get_supabase_client()
                                saved = 0
                                errs_save: list[str] = []
                                rows_to_save = df_import.loc[to_save_mask]
                                now_iso = datetime.now(timezone.utc).isoformat()
                                rows_batch: list[tuple[Any, dict[str, Any]]] = [
                                    (idx, row_imp.to_dict()) for idx, row_imp in rows_to_save.iterrows()
                                ]
                                dicts_only = [d for _, d in rows_batch]
                                for d in dicts_only:
                                    tt = d.get("titulo")
                                    if tt is None or (isinstance(tt, float) and pd.isna(tt)):
                                        d["titulo"] = ""
                                    else:
                                        d["titulo"] = str(tt).strip()
                                with st.spinner(
                                    f"Buscando geolocalização e gravando ({len(dicts_only)} anúncio(s))… "
                                    "Nominatim aceita ~1 requisição/s."
                                ):
                                    geocodificar_anuncios_batch(dicts_only)
                                for idx, row_d in rows_batch:
                                    try:
                                        payload = {
                                            "url_anuncio": row_d["url_anuncio"],
                                            "portal": row_d.get("portal", "manual"),
                                            "tipo_imovel": row_d.get("tipo_imovel", "desconhecido"),
                                            "logradouro": row_d.get("logradouro", "") or "",
                                            "bairro": row_d["bairro"],
                                            "cidade": row_d["cidade"],
                                            "estado": row_d.get("estado", "") or "",
                                            "area_construida_m2": float(row_d["area_construida_m2"]),
                                            "valor_venda": float(row_d["valor_venda"]),
                                            "transacao": "venda",
                                            "quartos": int(row_d["quartos"]) if row_d.get("quartos") is not None and pd.notna(row_d.get("quartos")) else None,
                                            "preco_m2": float(row_d["preco_m2"]) if row_d.get("preco_m2") and pd.notna(row_d.get("preco_m2")) else None,
                                            "metadados_json": row_d.get("metadados_json") if isinstance(row_d.get("metadados_json"), dict) else {},
                                            "ultima_coleta_em": now_iso,
                                            "primeiro_visto_em": now_iso,
                                        }
                                        gl = _geo_lat_lon_ok(row_d.get("latitude"), row_d.get("longitude"))
                                        if gl:
                                            payload["latitude"] = gl[0]
                                            payload["longitude"] = gl[1]
                                        cli.table(TABLE_ANUNCIOS_MERCADO).upsert(payload, on_conflict="url_anuncio").execute()
                                        saved += 1
                                    except Exception as e:
                                        errs_save.append(f"Linha {idx}: {e}")
                                if saved > 0:
                                    st.success(f"{saved} anúncio(s) gravado(s) com sucesso!")
                                    _query_anuncios.clear()
                                if errs_save:
                                    for err in errs_save[:10]:
                                        st.error(err)

            else:
                st.markdown('<div class="section-title">Inserir anúncio avulso</div>', unsafe_allow_html=True)
                av1, av2 = st.columns(2)
                av_url = av1.text_input("Link do anúncio *", key="av_url")
                av_tipo = av2.selectbox("Tipo", ["casa", "apartamento", "casa_condominio", "terreno", "comercial"], key="av_tipo")
                av3, av4 = st.columns(2)
                av_cidade = av3.text_input("Cidade *", key="av_cidade")
                av_estado = av4.text_input("Estado (UF) *", max_chars=2, key="av_estado")
                av5, av6 = st.columns(2)
                av_bairro = av5.text_input("Bairro *", key="av_bairro")
                av_endereco = av6.text_input("Endereço", key="av_endereco")
                av7, av8, av9 = st.columns(3)
                av_preco = av7.number_input("Preço (R$) *", min_value=0.0, step=1000.0, key="av_preco")
                _lbl_area = "Área do terreno (m²) *" if av_tipo == "terreno" else "Área (m²) *"
                av_area = av8.number_input(_lbl_area, min_value=0.0, step=1.0, key="av_area")
                if av_tipo == "terreno":
                    av_quartos = 0
                else:
                    av_quartos = av9.number_input("Quartos", min_value=0, step=1, key="av_quartos")

                if st.button("Gravar anúncio", type="primary", width="stretch", key="av_save"):
                    av_errors: list[str] = []
                    if not av_url.strip():
                        av_errors.append("Link obrigatório")
                    if av_preco <= 0:
                        av_errors.append("Preço deve ser maior que zero")
                    if av_area <= 0:
                        av_errors.append("Área deve ser maior que zero")
                    if not av_cidade.strip():
                        av_errors.append("Cidade obrigatória")
                    if not av_bairro.strip():
                        av_errors.append("Bairro obrigatório")
                    if av_errors:
                        for e in av_errors:
                            st.error(e)
                    else:
                        try:
                            cli = get_supabase_client()
                            now_iso = datetime.now(timezone.utc).isoformat()
                            payload = {
                                "url_anuncio": av_url.strip(),
                                "portal": portal_de_url(av_url.strip()),
                                "tipo_imovel": av_tipo,
                                "logradouro": av_endereco.strip(),
                                "bairro": av_bairro.strip(),
                                "cidade": av_cidade.strip(),
                                "estado": av_estado.strip().upper(),
                                "area_construida_m2": float(av_area),
                                "valor_venda": float(av_preco),
                                "transacao": "venda",
                                "quartos": int(av_quartos) if av_quartos > 0 else None,
                                "preco_m2": round(av_preco / av_area, 2),
                                "metadados_json": {},
                                "ultima_coleta_em": now_iso,
                                "primeiro_visto_em": now_iso,
                                "titulo": "",
                            }
                            with st.spinner("Buscando geolocalização (OpenStreetMap)…"):
                                row_geo = _enriquecer_anuncio_geolocation(payload)
                            gl = _geo_lat_lon_ok(row_geo.get("latitude"), row_geo.get("longitude"))
                            if gl:
                                payload["latitude"] = gl[0]
                                payload["longitude"] = gl[1]
                            payload.pop("titulo", None)
                            cli.table(TABLE_ANUNCIOS_MERCADO).upsert(payload, on_conflict="url_anuncio").execute()
                            st.success("Anúncio gravado com sucesso!")
                            _query_anuncios.clear()
                        except Exception as e:
                            st.error(f"Erro ao gravar: {e}")

    except Exception as exc:
        st.exception(exc)


# ── PAGE: Cache ───────────────────────────────────────────────────────────────
elif pagina == "🗄️ Cache":
    try:
        df_ca = _query_cache_bairro_all()
        if df_ca.empty:
            st.markdown('<div class="empty-state"><div class="empty-icon">🗄️</div><div class="empty-text">Nenhum cache encontrado.</div></div>', unsafe_allow_html=True)
        else:
            if "id" in df_ca.columns:
                _ca_prune_persist_to_valid_ids(
                    _CA_SS_SEL_CACHE_ROWS,
                    {str(x) for x in df_ca["id"].dropna().astype(str)},
                )
            df_ca = df_ca.reset_index(drop=True).copy()

            st.markdown('<div class="section-title">Entradas do cache</div>', unsafe_allow_html=True)
            df_ca_disp = df_ca.copy()
            if "atualizado_em" in df_ca_disp.columns:
                df_ca_disp["atualizado_em"] = (
                    pd.to_datetime(df_ca_disp["atualizado_em"], errors="coerce").dt.strftime("%d/%m/%Y").fillna("")
                )

            faixa_opts = ["(todas)"]
            if "faixa_area" in df_ca_disp.columns:
                faixa_opts.extend(
                    sorted(
                        {str(x).strip() for x in df_ca_disp["faixa_area"].dropna().astype(str).unique() if str(x).strip()},
                        key=str.lower,
                    )
                )
            with st.expander("Filtros da tabela (entradas do cache)", expanded=False):
                fe_c1, fe_c2, fe_c3 = st.columns(3)
                fe_cidade = fe_c1.text_input("Cidade (contém)", "", key="ca_ent_f_cid", placeholder="ex.: Gravataí")
                fe_bairro = fe_c2.text_input("Bairro (contém)", "", key="ca_ent_f_bai", placeholder="ex.: Centro")
                fe_estado = fe_c3.text_input("Estado / UF (contém)", "", key="ca_ent_f_uf", placeholder="ex.: RS")
                fe_outros = st.text_input(
                    "Outros campos (contém)",
                    "",
                    key="ca_ent_f_out",
                    placeholder="tipo, fonte, chave de segmento…",
                )
                fe_r1, fe_r2, fe_r3 = st.columns(3)
                fe_pm2_lo = fe_r1.number_input("R$/m² médio mín.", 0.0, step=50.0, key="ca_ent_f_pm2_lo", help="0 = sem limite inferior")
                fe_pm2_hi = fe_r2.number_input("R$/m² médio máx.", 0.0, step=50.0, key="ca_ent_f_pm2_hi", help="0 = sem limite superior")
                fe_r1, fe_r2, fe_r3 = st.columns(3)
                fe_vmv_lo = fe_r1.number_input("Venda média mín. (R$)", 0.0, step=10_000.0, key="ca_ent_f_vmv_lo", help="0 = sem limite inferior")
                fe_vmv_hi = fe_r2.number_input("Venda média máx. (R$)", 0.0, step=10_000.0, key="ca_ent_f_vmv_hi", help="0 = sem limite superior")
                fe_r1, fe_r2, fe_r3 = st.columns(3)
                fe_n_lo = int(
                    fe_r1.number_input(
                        "Amostras mín.",
                        min_value=0,
                        value=0,
                        step=1,
                        key="ca_ent_f_n_lo",
                        help="0 = sem limite inferior",
                    )
                )
                fe_n_hi = int(
                    fe_r2.number_input(
                        "Amostras máx.",
                        min_value=0,
                        value=0,
                        step=1,
                        key="ca_ent_f_n_hi",
                        help="0 = sem limite superior",
                    )
                )
                fe_faixa = st.selectbox("Faixa de área", faixa_opts, key="ca_ent_f_faixa") if len(faixa_opts) > 1 else "(todas)"

            ent_mask = _ca_mask_cache_entradas(
                df_ca_disp,
                cidade=str(fe_cidade),
                bairro=str(fe_bairro),
                estado=str(fe_estado),
                texto_outros=str(fe_outros),
                pm2_lo=float(fe_pm2_lo),
                pm2_hi=float(fe_pm2_hi),
                vmv_lo=float(fe_vmv_lo),
                vmv_hi=float(fe_vmv_hi),
                n_lo=fe_n_lo,
                n_hi=fe_n_hi,
                faixa=str(fe_faixa),
            )
            df_ca_disp = df_ca_disp[ent_mask].reset_index(drop=True)
            if df_ca_disp.empty and len(df_ca) > 0:
                st.warning("Nenhuma entrada do cache corresponde aos filtros da tabela.")
            pm2_ca = pd.to_numeric(df_ca_disp.get("preco_m2_medio"), errors="coerce").dropna()
            ca_kpi = '<div class="kpi-row">'
            ca_kpi += _kpi_card(str(len(df_ca_disp)), "Exibindo")
            ca_kpi += _kpi_card(str(len(df_ca)), "Total carregado")
            ca_kpi += _kpi_card(_fmt_brl(pm2_ca.mean()) if not pm2_ca.empty else "-", "Média R$/m²")
            ca_kpi += _kpi_card(_fmt_brl(pm2_ca.median()) if not pm2_ca.empty else "-", "Mediana R$/m²")
            ca_kpi += _kpi_card(f"{_fmt_brl(pm2_ca.min())} – {_fmt_brl(pm2_ca.max())}" if not pm2_ca.empty else "-", "Faixa R$/m²")
            ca_kpi += '</div>'
            st.markdown(ca_kpi, unsafe_allow_html=True)

            ca_sel_all = st.toggle(
                "Selecionar todos (filtrados)",
                value=False,
                key="ca_sel_all",
            )
            ca_display_vis = [c for c in [
                "nome_cache", "cidade", "bairro", "estado", "tipo_imovel",
                "preco_m2_medio", "valor_medio_venda", "maior_valor_venda",
                "menor_valor_venda", "n_amostras", "fonte", "atualizado_em",
            ] if c in df_ca_disp.columns]

            _ca_fmt_cols = {
                "preco_m2_medio": "R$/m²",
                "valor_medio_venda": "Venda média",
                "maior_valor_venda": "Maior valor",
                "menor_valor_venda": "Menor valor",
            }
            df_ca_show = df_ca_disp[ca_display_vis].copy()
            ca_col_cfg: dict[str, Any] = {}
            for _fc, _fl in _ca_fmt_cols.items():
                if _fc in df_ca_show.columns:
                    ca_col_cfg[_fc] = st.column_config.NumberColumn(_fl, format="%.2f")
            if "n_amostras" in df_ca_show.columns:
                ca_col_cfg["n_amostras"] = st.column_config.NumberColumn("Amostras", format="%d", step=1)

            ca_tbl_key = _ca_df_select_widget_key("ca_ent", df_ca_disp, "id")
            if "id" in df_ca_disp.columns:
                _ca_maybe_seed_df_selection(ca_tbl_key, df_ca_disp, "id", _CA_SS_SEL_CACHE_ROWS)
            if ca_sel_all and len(df_ca_show) > 0 and "id" in df_ca_disp.columns:
                st.session_state[ca_tbl_key] = _ca_widget_sel_state(list(range(len(df_ca_show))))

            ev_ca = st.dataframe(
                df_ca_show,
                hide_index=True,
                width="stretch",
                height=400,
                column_config=ca_col_cfg if ca_col_cfg else None,
                key=ca_tbl_key,
                on_select="rerun",
                selection_mode="multi-row",
            )
            sel_ix_ca = _dataframe_selection_rows(ev_ca)
            sel_ca_ids: list[str] = []
            if "id" in df_ca_disp.columns and sel_ix_ca:
                sel_ca_ids = df_ca_disp.iloc[sel_ix_ca]["id"].astype(str).tolist()
            sel_ca_rows = df_ca_disp.iloc[sel_ix_ca].copy() if sel_ix_ca else df_ca_disp.iloc[0:0].copy()
            st.session_state[_CA_SS_SEL_CACHE_ROWS] = list(sel_ca_ids)

            if not sel_ca_rows.empty:
                pm2_sel = pd.to_numeric(sel_ca_rows.get("preco_m2_medio"), errors="coerce").dropna()
                sel_ca_kpi = '<div class="kpi-row">'
                sel_ca_kpi += _kpi_card(str(len(sel_ca_rows)), "Selecionados", "accent")
                sel_ca_kpi += _kpi_card(_fmt_brl(pm2_sel.mean()) if not pm2_sel.empty else "-", "Média (sel.)")
                sel_ca_kpi += _kpi_card(_fmt_brl(pm2_sel.median()) if not pm2_sel.empty else "-", "Mediana (sel.)")
                sel_ca_kpi += '</div>'
                st.markdown(sel_ca_kpi, unsafe_allow_html=True)

            if sb.get("ca_del"):
                if not sel_ca_ids:
                    st.warning("Selecione entradas do cache primeiro.")
                else:
                    cli = get_supabase_client()
                    deleted = 0
                    removed_ids: set[str] = set()
                    for cid in sel_ca_ids:
                        try:
                            cli.table(CACHE_TABLE).delete().eq("id", cid).execute()
                            deleted += 1
                            removed_ids.add(str(cid))
                        except Exception:
                            pass
                    if deleted > 0:
                        st.session_state[_CA_SS_SEL_CACHE_ROWS] = [
                            x for x in sel_ca_ids if str(x) not in removed_ids
                        ]
                        st.success(f"{deleted} entrada(s) excluída(s).")
                        _query_cache_bairro_all.clear()
                        _refresh()

            if sb.get("ca_merge"):
                if not (ca_merge_nome or "").strip():
                    st.warning("Informe o nome do cache na barra lateral antes de mesclar.")
                elif len(sel_ca_ids) < 2:
                    st.warning("Selecione ao menos 2 entradas para mesclar.")
                else:
                    pm2_vals = pd.to_numeric(sel_ca_rows.get("preco_m2_medio"), errors="coerce").dropna()
                    if pm2_vals.empty:
                        st.error("Entradas selecionadas não possuem preço/m² válido.")
                    else:
                        first = sel_ca_rows.iloc[0]
                        new_pm2 = round(float(pm2_vals.median()), 2)
                        vv_all = pd.to_numeric(sel_ca_rows.get("valor_medio_venda"), errors="coerce").dropna()
                        mx_all = pd.to_numeric(sel_ca_rows.get("maior_valor_venda"), errors="coerce").dropna()
                        mn_all = pd.to_numeric(sel_ca_rows.get("menor_valor_venda"), errors="coerce").dropna()
                        na_all = pd.to_numeric(sel_ca_rows.get("n_amostras"), errors="coerce").dropna()
                        merged_an_ids: list[str] = []
                        for _, rmerge in sel_ca_rows.iterrows():
                            merged_an_ids.extend(_parse_anuncios_ids_field(rmerge.get("anuncios_ids")))
                        merged_an_ids = list(dict.fromkeys(merged_an_ids))
                        try:
                            cli = get_supabase_client()
                            for cid in sel_ca_ids:
                                try:
                                    cli.table(CACHE_TABLE).delete().eq("id", cid).execute()
                                except Exception:
                                    pass
                            rows_union = _fetch_anuncios_rows(merged_an_ids)
                            pay_merge = _recompute_cache_payload_from_anuncio_rows(rows_union)
                            ins_merge: dict[str, Any] | None = None
                            if pay_merge:
                                ins_merge = _inserir_cache_novo(
                                    cidade=_safe_str(first.get("cidade")),
                                    bairro=_safe_str(first.get("bairro")),
                                    estado=_safe_str(first.get("estado")),
                                    tipo_imovel=_safe_str(first.get("tipo_imovel"), "desconhecido"),
                                    preco_m2_medio=float(pay_merge["preco_m2_medio"]),
                                    fonte="frontend_merge",
                                    metadados_json=_merge_metadados_cache_edit(
                                        None,
                                        {
                                            "origem": "merge_manual",
                                            "n_fontes": len(sel_ca_ids),
                                            "valores_pm2_cache": pm2_vals.tolist(),
                                        },
                                    ),
                                    valor_medio_venda=pay_merge.get("valor_medio_venda"),
                                    maior_valor_venda=pay_merge.get("maior_valor_venda"),
                                    menor_valor_venda=pay_merge.get("menor_valor_venda"),
                                    n_amostras=pay_merge.get("n_amostras"),
                                    anuncios_ids=pay_merge.get("anuncios_ids"),
                                    nome_cache=(ca_merge_nome or "").strip(),
                                    client=cli,
                                )
                                pm2_merge_msg = float(pay_merge["preco_m2_medio"])
                            else:
                                ins_merge = _inserir_cache_novo(
                                    cidade=_safe_str(first.get("cidade")),
                                    bairro=_safe_str(first.get("bairro")),
                                    estado=_safe_str(first.get("estado")),
                                    tipo_imovel=_safe_str(first.get("tipo_imovel"), "desconhecido"),
                                    preco_m2_medio=new_pm2,
                                    fonte="frontend_merge",
                                    metadados_json=json.dumps({
                                        "origem": "merge_manual",
                                        "n_fontes": len(sel_ca_ids),
                                        "valores": pm2_vals.tolist(),
                                    }, ensure_ascii=False),
                                    valor_medio_venda=float(vv_all.mean()) if not vv_all.empty else None,
                                    maior_valor_venda=float(mx_all.max()) if not mx_all.empty else None,
                                    menor_valor_venda=float(mn_all.min()) if not mn_all.empty else None,
                                    n_amostras=int(na_all.sum()) if not na_all.empty else len(sel_ca_ids),
                                    anuncios_ids=",".join(merged_an_ids) if merged_an_ids else None,
                                    nome_cache=(ca_merge_nome or "").strip(),
                                    client=cli,
                                )
                                pm2_merge_msg = new_pm2
                            st.success(
                                f"Merge concluído: mediana {_fmt_brl(pm2_merge_msg)}/m² ({len(sel_ca_ids)} fontes → 1 entrada)"
                            )
                            new_row = (ins_merge or {}).get("data") if isinstance(ins_merge, dict) else None
                            if isinstance(new_row, list) and new_row and new_row[0].get("id"):
                                st.session_state[_CA_SS_SEL_CACHE_ROWS] = [str(new_row[0]["id"])]
                            _query_cache_bairro_all.clear()
                            _refresh()
                        except Exception as e:
                            st.error(f"Erro ao mesclar: {e}")

            st.divider()
            st.markdown(
                '<div class="section-title">Pool de anúncios e composição do cache</div>',
                unsafe_allow_html=True,
            )

            limit_ca_an = sb.get("ca_an_lim", 200)
            df_an_ca = _query_anuncios(limit=limit_ca_an)
            an_sel_ids: list[str] = []

            if not df_an_ca.empty and "id" in df_an_ca.columns:
                _ca_prune_persist_to_valid_ids(
                    _CA_SS_SEL_POOL_ROWS,
                    {str(x) for x in df_an_ca["id"].dropna().astype(str)},
                )
                _ca_sync_pool_sel_track_from_persist()
            if not df_an_ca.empty:
                df_an_ca = df_an_ca.reset_index(drop=True).copy()
            df_an_pool_base = df_an_ca.copy()
            _df_an_pre_tbl = len(df_an_pool_base)

            if df_an_pool_base.empty and sel_ca_rows.empty:
                st.info("Nenhum anúncio com os filtros da sidebar.")
            elif sel_ca_rows.empty:
                st.markdown('<div class="section-title">Pool de mercado</div>', unsafe_allow_html=True)
                pool_fd = _ca_pool_filters_expander_ui(
                    key_prefix="ca_pool_f",
                    title="Filtros do pool de mercado",
                    expanded=True,
                )
                pool_mask = _ca_mask_pool_like(df_an_pool_base, **pool_fd)
                df_pool_vis = df_an_pool_base[pool_mask].reset_index(drop=True)
                if df_pool_vis.empty and _df_an_pre_tbl > 0:
                    st.warning("Nenhum anúncio com os filtros da tabela deste painel.")
                df_an_disp = df_pool_vis.copy()
                an_vis_cols = [c for c in [
                    "cidade", "bairro", "estado", "tipo_imovel",
                    "valor_venda", "preco_m2", "area_construida_m2", "url_anuncio",
                ] if c in df_an_disp.columns]
                df_an_show = df_an_disp[an_vis_cols].copy()
                an_ca_cfg: dict[str, Any] = {}
                if "url_anuncio" in df_an_show.columns:
                    an_ca_cfg["url_anuncio"] = st.column_config.LinkColumn("Link", display_text="Abrir")
                for _anc, _lab in (
                    ("valor_venda", "Valor venda"),
                    ("preco_m2", "R$/m²"),
                    ("area_construida_m2", "Área m²"),
                ):
                    if _anc in df_an_show.columns:
                        an_ca_cfg[_anc] = st.column_config.NumberColumn(_lab, format="%.2f")
                an_tbl_key = _ca_df_select_widget_key("ca_pool", df_an_disp, "id")
                if "id" in df_an_disp.columns:
                    _ca_maybe_seed_df_selection(an_tbl_key, df_an_disp, "id", _CA_SS_SEL_POOL_ROWS)
                if not df_pool_vis.empty:
                    st.markdown("##### Mapa do pool")
                    _ca_pool_touch_viewport_cache(df_pool_vis)
                    _pool_render_mapa_folium(
                        df_pool_vis,
                        pins_fp_state_key="ca_pool_map_pins_fp",
                        folium_key="ca_pool_map",
                        show_clear_button=False,
                        use_popup_merge=True,
                        cache_map_select_mode=True,
                        map_height=_CA_CACHE_MAP_HEIGHT,
                        viewport_bounds_state_key=_CA_SS_POOL_VIEWPORT_BOUNDS,
                        popup_query_param="ca_pool_map_sel",
                        merge_ids_session_key=_CA_SS_POOL_MAP_IDS,
                    )
                    _ca_apply_pool_map_ids_to_selection(df_an_disp, an_tbl_key)
                an_sel_all = st.toggle(
                    "Selecionar todos (filtrados)",
                    value=False,
                    key="ca_an_sel_all",
                )
                if an_sel_all and len(df_an_show) > 0 and "id" in df_an_disp.columns:
                    st.session_state[an_tbl_key] = _ca_widget_sel_state(list(range(len(df_an_show))))
                ev_an = st.dataframe(
                    df_an_show,
                    hide_index=True,
                    width="stretch",
                    height=_CA_CACHE_MAP_HEIGHT,
                    column_config=an_ca_cfg if an_ca_cfg else None,
                    key=an_tbl_key,
                    on_select="rerun",
                    selection_mode="multi-row",
                )
                sel_ix_an = _dataframe_selection_rows(ev_an)
                if "id" in df_an_disp.columns and sel_ix_an:
                    an_sel_ids = df_an_disp.iloc[sel_ix_an]["id"].astype(str).tolist()
                else:
                    an_sel_ids = []
                _ca_set_pool_persist_selection_ids([str(x) for x in an_sel_ids])
                an_sel_df = (
                    df_pool_vis[df_pool_vis["id"].isin(an_sel_ids)].copy()
                    if an_sel_ids
                    else df_pool_vis.iloc[0:0].copy()
                )
                st.markdown(_ca_kpi_row_anuncios_df(an_sel_df, count_label="Pool selecionado"), unsafe_allow_html=True)
                ca_nm_criar = st.text_input(
                    "Nome do novo cache",
                    max_chars=200,
                    key="ca_pool_create_nome",
                    help="Obrigatório ao criar a partir do pool. Se houver vários grupos (cidade/bairro/tipo), serão usados sufixos (2), (3)…",
                )
                if st.button("Criar cache com selecionados", type="primary", width="stretch", key="ca_create_from_an"):
                    if not an_sel_ids:
                        st.warning("Selecione anúncios primeiro.")
                    elif not (ca_nm_criar or "").strip():
                        st.warning("Informe um nome para o cache.")
                    else:
                        res = _recalcular_cache(an_sel_ids, nome_cache_usuario=(ca_nm_criar or "").strip())
                        if res.get("cache_atualizado", 0) > 0:
                            st.success(f"Cache criado: {res['cache_atualizado']} grupo(s)")
                            _ca_set_pool_persist_selection_ids([str(x) for x in an_sel_ids])
                            _query_cache_bairro_all.clear()
                            if res.get("detalhes"):
                                st.dataframe(pd.DataFrame(res["detalhes"]), hide_index=True, width="stretch")
                            _refresh()
                        else:
                            st.warning("Não foi possível criar cache (sem dados válidos).")
            else:
                if len(sel_ca_rows) == 1:
                    ca_act_i = 0
                elif len(sel_ca_rows) <= 10:
                    ca_act_i = int(
                        st.radio(
                            "Cache ativo (destino das setas)",
                            options=list(range(len(sel_ca_rows))),
                            horizontal=True,
                            format_func=lambda i: _cache_row_tab_label(sel_ca_rows.iloc[int(i)]),
                            key="ca_active_radio",
                        )
                    )
                else:
                    ca_act_i = int(
                        st.selectbox(
                            "Cache ativo (destino das setas)",
                            options=list(range(len(sel_ca_rows))),
                            format_func=lambda j: _cache_row_tab_label(sel_ca_rows.iloc[int(j)]),
                            key="ca_active_pick",
                        )
                    )
                active_ca = sel_ca_rows.iloc[int(ca_act_i)]

                _ca_pc_table_h = _CA_CACHE_MAP_HEIGHT
                c_pool, c_comp = st.columns(2, gap="medium")
                with c_pool:
                    st.markdown('<div class="section-title">Pool de mercado</div>', unsafe_allow_html=True)
                    pool_fd = _ca_pool_filters_expander_ui(
                        key_prefix="ca_pool_f",
                        title="Filtros do pool de mercado",
                        expanded=True,
                    )
                    pool_mask = _ca_mask_pool_like(df_an_pool_base, **pool_fd)
                    df_pool_vis = df_an_pool_base[pool_mask].reset_index(drop=True)
                    if df_pool_vis.empty and _df_an_pre_tbl > 0:
                        st.warning("Nenhum anúncio com os filtros da tabela deste painel.")
                    df_an_disp = df_pool_vis.copy()
                    an_vis_cols = [c for c in [
                        "cidade", "bairro", "estado", "tipo_imovel",
                        "valor_venda", "preco_m2", "area_construida_m2", "url_anuncio",
                    ] if c in df_an_disp.columns]
                    df_an_show = df_an_disp[an_vis_cols].copy()
                    an_ca_cfg = {}
                    if "url_anuncio" in df_an_show.columns:
                        an_ca_cfg["url_anuncio"] = st.column_config.LinkColumn("Link", display_text="Abrir")
                    for _anc, _lab in (
                        ("valor_venda", "Valor venda"),
                        ("preco_m2", "R$/m²"),
                        ("area_construida_m2", "Área m²"),
                    ):
                        if _anc in df_an_show.columns:
                            an_ca_cfg[_anc] = st.column_config.NumberColumn(_lab, format="%.2f")
                    an_tbl_key = _ca_df_select_widget_key("ca_pool", df_an_disp, "id")
                    if "id" in df_an_disp.columns:
                        _ca_maybe_seed_df_selection(an_tbl_key, df_an_disp, "id", _CA_SS_SEL_POOL_ROWS)
                    if not df_pool_vis.empty:
                        st.markdown("##### Mapa do pool")
                        _ca_pool_touch_viewport_cache(df_pool_vis)
                        _pool_render_mapa_folium(
                            df_pool_vis,
                            pins_fp_state_key="ca_pool_map_pins_fp_pc",
                            folium_key="ca_pool_map_pc",
                            show_clear_button=False,
                            use_popup_merge=True,
                            cache_map_select_mode=True,
                            map_height=_CA_CACHE_MAP_HEIGHT,
                            viewport_bounds_state_key=_CA_SS_POOL_VIEWPORT_BOUNDS,
                            popup_query_param="ca_pool_map_sel",
                            merge_ids_session_key=_CA_SS_POOL_MAP_IDS,
                        )
                        _ca_apply_pool_map_ids_to_selection(df_an_disp, an_tbl_key)
                    an_sel_all = st.toggle(
                        "Selecionar todos (filtrados)",
                        value=False,
                        key="ca_an_sel_all",
                    )
                    if an_sel_all and len(df_an_show) > 0 and "id" in df_an_disp.columns:
                        st.session_state[an_tbl_key] = _ca_widget_sel_state(list(range(len(df_an_show))))
                    ev_an = st.dataframe(
                        df_an_show,
                        hide_index=True,
                        width="stretch",
                        height=_ca_pc_table_h,
                        column_config=an_ca_cfg if an_ca_cfg else None,
                        key=an_tbl_key,
                        on_select="rerun",
                        selection_mode="multi-row",
                    )
                    sel_ix_an = _dataframe_selection_rows(ev_an)
                    if "id" in df_an_disp.columns and sel_ix_an:
                        an_sel_ids = df_an_disp.iloc[sel_ix_an]["id"].astype(str).tolist()
                    else:
                        an_sel_ids = []
                    _ca_set_pool_persist_selection_ids([str(x) for x in an_sel_ids])
                    an_sel_df = (
                        df_pool_vis[df_pool_vis["id"].isin(an_sel_ids)].copy()
                        if an_sel_ids
                        else df_pool_vis.iloc[0:0].copy()
                    )
                    st.markdown(_ca_kpi_row_anuncios_df(an_sel_df, count_label="Pool selecionado"), unsafe_allow_html=True)
                    ca_nm_criar_pc = st.text_input(
                        "Nome do novo cache",
                        max_chars=200,
                        key="ca_pool_create_nome",
                        help="Obrigatório ao criar a partir do pool. Se houver vários grupos (cidade/bairro/tipo), serão usados sufixos (2), (3)…",
                    )
                    if st.button("Criar cache com selecionados", type="primary", width="stretch", key="ca_create_from_an_pc"):
                        if not an_sel_ids:
                            st.warning("Selecione anúncios primeiro.")
                        elif not (ca_nm_criar_pc or "").strip():
                            st.warning("Informe um nome para o cache.")
                        else:
                            res = _recalcular_cache(an_sel_ids, nome_cache_usuario=(ca_nm_criar_pc or "").strip())
                            if res.get("cache_atualizado", 0) > 0:
                                st.success(f"Cache criado: {res['cache_atualizado']} grupo(s)")
                                _ca_set_pool_persist_selection_ids([str(x) for x in an_sel_ids])
                                _query_cache_bairro_all.clear()
                                if res.get("detalhes"):
                                    st.dataframe(pd.DataFrame(res["detalhes"]), hide_index=True, width="stretch")
                                _refresh()
                            else:
                                st.warning("Não foi possível criar cache (sem dados válidos).")

                with c_comp:
                    st.markdown('<div class="section-title">Composição (cache ativo)</div>', unsafe_allow_html=True)
                    cid_m, raw_ids_m, ca_comp_ordered, comp_sel_rows = _render_cache_members_editor(
                        active_ca, table_height=_ca_pc_table_h,
                    )
                    if ca_comp_ordered:
                        st.markdown(
                            _ca_kpi_row_anuncios_df(pd.DataFrame(ca_comp_ordered), count_label="Na composição"),
                            unsafe_allow_html=True,
                        )
                    nome_comp_edit = st.text_input(
                        "Nome do cache (ao alterar composição)",
                        value=_safe_str(active_ca.get("nome_cache")),
                        max_chars=200,
                        key=f"ca_comp_nome_{cid_m or 'x'}",
                        help="Obrigatório ao aplicar inclusões ou remoções na composição.",
                    )
                    _, _ctr_ar, _ = st.columns([0.2, 0.6, 0.2])
                    with _ctr_ar:
                        _b_rm, _b_add = st.columns(2)
                        with _b_rm:
                            if st.button(
                                "←",
                                help="Remove do cache as linhas destacadas na tabela (clique na linha para selecionar).",
                                width="stretch",
                                key=f"ca_arrow_rm_{cid_m}",
                            ):
                                if not ca_comp_ordered:
                                    st.warning("Não há linhas de composição para processar.")
                                elif not comp_sel_rows:
                                    st.warning("Selecione uma ou mais linhas na tabela (clique na linha).")
                                else:
                                    ids_excluir: set[str] = set()
                                    for ri in comp_sel_rows:
                                        if 0 <= ri < len(ca_comp_ordered):
                                            iid = str(ca_comp_ordered[ri].get("id") or "").strip()
                                            if iid:
                                                ids_excluir.add(iid)
                                    if not ids_excluir:
                                        st.warning("Não foi possível obter IDs das linhas selecionadas.")
                                    else:
                                        new_ids = [x for x in raw_ids_m if x not in ids_excluir]
                                        ok, msg = _apply_cache_member_ids(
                                            cid_m,
                                            new_ids,
                                            metadados_prev=active_ca.get("metadados_json"),
                                            nome_cache=(nome_comp_edit or "").strip(),
                                        )
                                        if ok:
                                            st.success(msg)
                                            st.session_state[_CA_SS_SEL_CACHE_ROWS] = list(sel_ca_ids)
                                            _ca_set_pool_persist_selection_ids([str(x) for x in an_sel_ids])
                                            _query_cache_bairro_all.clear()
                                            _refresh()
                                        else:
                                            st.error(msg)
                        with _b_add:
                            if st.button(
                                "→",
                                help="Adiciona ao cache ativo os anúncios selecionados no pool (linhas destacadas à esquerda).",
                                width="stretch",
                                type="primary",
                                key=f"ca_arrow_add_{cid_m}",
                            ):
                                if not an_sel_ids:
                                    st.warning("Selecione uma ou mais linhas no pool à esquerda.")
                                else:
                                    merged = list(dict.fromkeys(raw_ids_m + an_sel_ids))
                                    ok, msg = _apply_cache_member_ids(
                                        cid_m,
                                        merged,
                                        metadados_prev=active_ca.get("metadados_json"),
                                        nome_cache=(nome_comp_edit or "").strip(),
                                    )
                                    if ok:
                                        st.success(msg)
                                        st.session_state[_CA_SS_SEL_CACHE_ROWS] = list(sel_ca_ids)
                                        _ca_set_pool_persist_selection_ids([str(x) for x in an_sel_ids])
                                        _query_cache_bairro_all.clear()
                                        _refresh()
                                    else:
                                        st.error(msg)

    except Exception as exc:
        st.exception(exc)


# ── PAGE: Simulador ───────────────────────────────────────────────────────────
elif pagina == "🧮 Simulador":
    try:
        df_sim = _query_table(SUPABASE_TABLE, limit=400)
        if df_sim.empty:
            st.markdown('<div class="empty-state"><div class="empty-icon">🧮</div><div class="empty-text">Nenhum imóvel para simulação.</div></div>', unsafe_allow_html=True)
        else:
            sim_lei_fd = _lei_table_filters_expander_ui(
                key_prefix="sim_lei_tbl",
                title="Filtros da tabela (imóveis)",
                expanded=True,
            )
            df_sim = df_sim[_ca_mask_leilao_like(df_sim, **sim_lei_fd)].reset_index(drop=True)
            opcoes = []
            mapa: dict[str, dict] = {}
            for _, r in df_sim.iterrows():
                rid = str(r.get("id") or "")
                if not rid:
                    continue
                lbl = f"{r.get('cidade') or '-'} / {r.get('bairro') or '-'} — {rid[:8]}"
                opcoes.append(lbl)
                mapa[lbl] = r.to_dict()
            if not opcoes:
                st.info("Sem imóveis válidos.")
            else:
                escolha = st.selectbox("Selecione o imóvel", opcoes, key="sim_sel_imovel")
                row = mapa[escolha]
                iid = str(row.get("id"))
                st.session_state["_sim_last_iid"] = iid
                url_leilao_sim = str(row.get("url_leilao") or "").strip()

                _sim_cidade = _safe_str(row.get("cidade"), "-")
                _sim_bairro_h = _safe_str(row.get("bairro"), "-")
                _sim_tipo = _safe_str(row.get("tipo_imovel"), "")
                _sim_tipo_norm = normalizar_tipo_imovel(_sim_tipo)
                _sim_area_h = _safe_str(row.get("area_util"), "")
                _sim_area_total_h = _safe_str(row.get("area_total"), "")
                _sim_header = f'<div class="lei-header"><div class="lei-title">📍 {_sim_cidade} — {_sim_bairro_h}</div><div class="lei-meta">'
                if _sim_tipo:
                    _sim_header += _badge(_sim_tipo.replace("_", " ").title())
                if _sim_tipo_norm == "terreno":
                    if _sim_area_total_h:
                        _sim_header += _badge(f'Terreno {_sim_area_total_h} m²')
                else:
                    if _sim_area_h:
                        _sim_header += _badge(f'{_sim_area_h} m²')
                    if _sim_area_total_h:
                        _sim_header += _badge(f'Terreno {_sim_area_total_h} m²')
                if url_leilao_sim:
                    _sim_header += f'<a href="{url_leilao_sim}" target="_blank" style="color:#60a5fa;text-decoration:none;font-size:0.85rem;">🔗 Abrir leilão</a>'
                _sim_header += '</div></div>'
                st.markdown(_sim_header, unsafe_allow_html=True)

                left, right = st.columns([1, 1.3], gap="medium")
                with left:
                    st.markdown('<div class="section-title">Parâmetros</div>', unsafe_allow_html=True)
                    area_imovel = _to_float(area_efetiva_de_registro(row))
                    sim_cidade = _safe_str(row.get("cidade"))
                    sim_bairro = _safe_str(row.get("bairro"))
                    sim_estado = _safe_str(row.get("estado"))
                    df_cache_sim = _buscar_cache_para_imovel(sim_cidade, sim_bairro, sim_estado)

                    _VENDA_OPTS: dict[str, str] = {"Manual": "manual", "Média R$/m²": "pm2", "Venda média": "vmv", "Maior valor": "max", "Menor valor": "min"}
                    _sim_l1a, _sim_l1b = st.columns(2, gap="small")
                    with _sim_l1a:
                        desconto_avista = st.number_input(
                            "Desc à vista %",
                            min_value=0.0,
                            max_value=99.0,
                            value=0.0,
                            step=0.5,
                            format="%0.1f",
                            key=f"sda_{iid}",
                            help="Desconto no caixa do lance; comissão do leiloeiro e ITBI s/ lance usam o lance nominal.",
                        )
                    with _sim_l1b:
                        lance = st.number_input(
                            "Lance (R$)",
                            min_value=0.0,
                            value=float(_to_float(row.get("valor_arrematacao"))),
                            step=1000.0,
                            format="%0.2f",
                            key=f"sl_{iid}",
                        )

                    _sim_l2a, _sim_l2b = st.columns(2, gap="small")
                    with _sim_l2a:
                        venda_modo = st.selectbox(
                            "Base venda",
                            list(_VENDA_OPTS.keys()),
                            key=f"sv_modo_{iid}",
                            help="Como sugerir o valor de venda a partir do cache.",
                        )
                    venda_key = _VENDA_OPTS[venda_modo]

                    venda_default = _to_float(row.get("valor_venda_sugerido") or row.get("valor_mercado_estimado"))
                    if venda_key != "manual" and not df_cache_sim.empty:
                        cache_row = df_cache_sim.iloc[0]
                        if venda_key == "pm2":
                            pm2_cache = _to_float(cache_row.get("preco_m2_medio"))
                            if pm2_cache > 0 and area_imovel > 0:
                                venda_default = round(pm2_cache * area_imovel, 2)
                        elif venda_key == "vmv":
                            v = _to_float(cache_row.get("valor_medio_venda"))
                            if v > 0:
                                venda_default = v
                        elif venda_key == "max":
                            v = _to_float(cache_row.get("maior_valor_venda"))
                            if v > 0:
                                venda_default = v
                        elif venda_key == "min":
                            v = _to_float(cache_row.get("menor_valor_venda"))
                            if v > 0:
                                venda_default = v

                    with _sim_l2b:
                        venda = st.number_input(
                            "Venda (R$)",
                            min_value=0.0,
                            value=float(venda_default),
                            step=1000.0,
                            format="%0.2f",
                            key=f"sv_{iid}_{venda_key}",
                        )

                    _REF_M2 = {"Personalizado": 0, "Leve — R$500/m²": 500, "Médio — R$1.000/m²": 1000, "Alto — R$1.500/m²": 1500, "Premium — R$2.500/m²": 2500}
                    _sim_r2a, _sim_r2b = st.columns(2, gap="small")
                    with _sim_r2a:
                        padrao_ref = st.selectbox(
                            "Reforma R$/m²",
                            options=list(_REF_M2.keys()),
                            key=f"ref_padrao_{iid}",
                        )
                    valor_ref = _REF_M2[padrao_ref]
                    default_reforma = round(area_imovel * valor_ref, 2) if valor_ref > 0 and area_imovel > 0 else _to_float(row.get("custo_reforma_estimado"))
                    with _sim_r2b:
                        reforma = st.number_input(
                            "Reforma (R$)",
                            min_value=0.0,
                            value=float(default_reforma),
                            step=500.0,
                            format="%0.2f",
                            key=f"sr_{iid}_{padrao_ref}",
                        )

                    _sim_r3a, _sim_r3b = st.columns(2, gap="small")
                    with _sim_r3a:
                        reg_pct = st.number_input(
                            "Registro %",
                            min_value=0.0,
                            max_value=20.0,
                            value=3.5,
                            step=0.5,
                            format="%0.1f",
                            key=f"srg_pct_{iid}",
                        )
                    default_registro = round(lance * reg_pct / 100.0, 2)
                    with _sim_r3b:
                        registro = st.number_input(
                            "Registro (R$)",
                            min_value=0.0,
                            value=float(default_registro),
                            step=500.0,
                            format="%0.2f",
                            key=f"srg_{iid}_{reg_pct}",
                        )

                    _sim_r4a, _sim_r4b = st.columns(2, gap="small")
                    with _sim_r4a:
                        com_lei = st.number_input(
                            "Leiloeiro %",
                            min_value=0.0,
                            max_value=100.0,
                            value=5.0,
                            step=0.5,
                            format="%0.1f",
                            key=f"scl_{iid}",
                        )
                    with _sim_r4b:
                        com_imob = st.number_input(
                            "Imobiliária %",
                            min_value=0.0,
                            max_value=100.0,
                            value=6.0,
                            step=0.5,
                            format="%0.1f",
                            key=f"sci_{iid}",
                        )

                    _sim_r5a, _sim_r5b, _sim_r5c = st.columns(3, gap="small")
                    with _sim_r5a:
                        itbi = st.number_input(
                            "ITBI %",
                            min_value=0.0,
                            max_value=100.0,
                            value=3.0,
                            step=0.5,
                            format="%0.1f",
                            key=f"si_{iid}",
                        )
                    with _sim_r5b:
                        fat_liq = st.number_input(
                            "Liquidez",
                            min_value=0.1,
                            max_value=1.5,
                            value=1.0,
                            step=0.01,
                            format="%0.2f",
                            key=f"sf_{iid}",
                            help="S/ venda bruta antes da imobiliária. 1 = 100%.",
                        )
                    with _sim_r5c:
                        roi_alvo = st.number_input(
                            "ROI alvo %",
                            min_value=-90.0,
                            max_value=500.0,
                            value=50.0,
                            step=1.0,
                            format="%0.1f",
                            key=f"sra_{iid}",
                        )

                    _sim_r6a, _sim_r6b = st.columns(2, gap="small")
                    with _sim_r6a:
                        itbi_sobre_venda = st.toggle(
                            "ITBI s/ venda",
                            value=False,
                            key=f"si_sv_{iid}",
                            help="ITBI sobre venda após liquidez; desligado = sobre o lance.",
                        )
                    with _sim_r6b:
                        vd_caixa = st.toggle("Venda direta Caixa", key=f"svc_{iid}")
                    tipo_ir = st.radio(
                        "IR (ganho de capital)",
                        ["Pessoa física", "Pessoa jurídica"],
                        horizontal=True,
                        key=f"sim_tipo_ir_{iid}",
                        help="PF: 15% sobre o lucro após comissão imobiliária. PJ: 6,7% sobre a venda após liquidez.",
                    )
                    pj_ir = tipo_ir == "Pessoa jurídica"

                    _sim_show = bool(st.session_state.get("_sim_show_result", True))
                    if _sim_show:
                        st.caption(
                            "Com o resultado visível, ROI e tabela **acompanham** os parâmetros à esquerda. "
                            "Use **Nova simulação** na barra lateral para zerar os campos e ocultar o resultado até **Simular** de novo."
                        )
                    else:
                        st.info(
                            "Clique em **Simular** na barra lateral para calcular e exibir ROI, venda líquida e a tabela de detalhes."
                        )

                    snap = None
                    if _sim_show:
                        snap = _sim_roi_leilao_snapshot(
                            lance=lance,
                            desconto_avista_pct=float(desconto_avista),
                            venda=venda,
                            com_imob=com_imob,
                            reforma=reforma,
                            registro=registro,
                            com_lei=com_lei,
                            itbi=itbi,
                            itbi_sobre_venda=bool(itbi_sobre_venda),
                            fat_liq=fat_liq,
                            roi_alvo=roi_alvo,
                            vd_caixa=bool(vd_caixa),
                            pessoa_juridica=pj_ir,
                        )

                    if _sim_show and snap is not None and not snap.get("ok"):
                        st.warning(
                            snap.get("erro") or "Não foi possível calcular o ROI com estes parâmetros."
                        )
                    elif _sim_show and snap is not None and snap.get("ok"):
                        res = snap["res"]
                        r = res.model_dump()
                        venda_pc = _to_float(snap["venda_pc"])
                        v_bruta_aj = _to_float(snap["venda_bruta_ajustada"])
                        inv = _to_float(r.get("investimento_total"))
                        itbi_m = _to_float(snap["itbi_monetario"])
                        reg_inf = _to_float(snap["registro_informado"])
                        com_imob_v = max(0.0, _to_float(snap["comissao_imob_valor"]))
                        lance_max = _to_float(snap["lance_max"])
                        roi_bruto = _to_float(snap["roi_bruto_pct"])
                        roi_liq = _to_float(snap["roi_liquido_pct"])
                        lucro_liq = _to_float(snap["lucro_liquido"])
                        lucro_antes_ir = _to_float(snap["lucro_antes_ir"])
                        ir_v = _to_float(snap["ir_valor"])

                        st.divider()
                        _roi_cls = "positive" if roi_liq >= 0 else "negative"
                        sim_kpi = '<div class="kpi-row sim-kpi">'
                        sim_kpi += _kpi_card(f'{_fmt_n(roi_liq, 2)}%', "ROI líquido", _roi_cls)
                        sim_kpi += _kpi_card(_fmt_brl(lucro_liq), "Lucro líquido")
                        sim_kpi += _kpi_card(f'{_fmt_n(roi_bruto, 2)}%', "ROI bruto", "accent" if roi_bruto >= 0 else "negative")
                        sim_kpi += _kpi_card(_fmt_brl(lance_max), "Lance máximo", "accent")
                        sim_kpi += '</div>'
                        st.markdown(sim_kpi, unsafe_allow_html=True)

                        lance_efetivo_inv = _to_float(
                            r.get("valor_lance_efetivo", lance)
                        )
                        inv_check = round(
                            lance_efetivo_inv
                            + _to_float(r.get("comissao_leiloeiro_valor"))
                            + (0.0 if snap["itbi_sobre_venda"] else itbi_m)
                            + (itbi_m + reg_inf if snap["itbi_sobre_venda"] else reg_inf)
                            + _to_float(reforma),
                            2,
                        )
                        pct_liq = _fmt_n(float(snap["fat_liq"]) * 100.0, 2)
                        itbi_lbl = (
                            f"ITBI ({_fmt_n(itbi, 2)}% s/ venda após liquidez)"
                            if snap["itbi_sobre_venda"]
                            else f"ITBI ({_fmt_n(itbi, 2)}% s/ lance nominal)"
                        )
                        ir_item = (
                            f"IR — Pessoa jurídica ({_fmt_n(_SIM_IR_PJ_PCT, 2)}% s/ venda após liquidez)"
                            if snap["pessoa_juridica"]
                            else f"IR — Pessoa física ({_fmt_n(_SIM_IR_PF_PCT, 2)}% s/ lucro após comissão imob.)"
                        )
                        d_av_tbl = _to_float(r.get("desconto_avista_pct_aplicado", 0.0))
                        lance_pago = _to_float(r.get("valor_lance_efetivo", lance))
                        df_sim_rows: list[dict[str, Any]] = [
                            {"Item": "Lance nominal (parâmetro)", "Valor": _fmt_brl(lance)},
                            {"Item": "Desconto à vista (%)", "Valor": f"{_fmt_n(d_av_tbl, 2)}%"},
                            {"Item": "Lance pago à vista (caixa)", "Valor": _fmt_brl(lance_pago)},
                            {
                                "Item": f"Com. leiloeiro ({_fmt_n(com_lei, 2)}% s/ lance nominal)",
                                "Valor": _fmt_brl(r.get("comissao_leiloeiro_valor")),
                            },
                            {"Item": itbi_lbl, "Valor": _fmt_brl(itbi_m)},
                            {
                                "Item": f"Registro / cartório (ref. {_fmt_n(reg_pct, 2)}% s/ lance nominal)",
                                "Valor": _fmt_brl(reg_inf),
                            },
                            {"Item": "Reforma (parâmetro)", "Valor": _fmt_brl(reforma)},
                            {"Item": "Investimento total", "Valor": _fmt_brl(inv)},
                            {"Item": "Venda bruta (parâmetro)", "Valor": _fmt_brl(snap["venda_bruta"])},
                            {
                                "Item": f"Venda bruta após liquidez ({pct_liq}% do bruto declarado)",
                                "Valor": _fmt_brl(v_bruta_aj),
                            },
                            {
                                "Item": f"Com. imobiliária ({_fmt_n(com_imob, 2)}% s/ venda após liquidez)",
                                "Valor": _fmt_brl(com_imob_v),
                            },
                            {
                                "Item": "Venda pós-imobiliária (base ROI bruto)",
                                "Valor": _fmt_brl(venda_pc),
                            },
                            {"Item": "Lucro antes do IR (receita pós-imob. − invest.)", "Valor": _fmt_brl(lucro_antes_ir)},
                            {"Item": "ROI bruto s/ investimento total", "Valor": f"{_fmt_n(roi_bruto, 2)}%"},
                            {"Item": ir_item, "Valor": _fmt_brl(ir_v)},
                            {"Item": "Lucro líquido (após IR)", "Valor": _fmt_brl(lucro_liq)},
                            {"Item": "ROI líquido s/ investimento total", "Valor": f"{_fmt_n(roi_liq, 2)}%"},
                        ]
                        if abs(inv_check - inv) > 0.02:
                            df_sim_rows.append(
                                {
                                    "Item": "⚠ Arredondamento",
                                    "Valor": f"Soma explícita dos custos {_fmt_brl(inv_check)} vs investimento {_fmt_brl(inv)}.",
                                }
                            )
                        st.dataframe(pd.DataFrame(df_sim_rows), hide_index=True, width="stretch")

                        if st.toggle("Aplicar no banco", key=f"apply_{iid}"):
                            if st.button("Gravar simulação", width="stretch", key=f"btn_apply_{iid}"):
                                atualizar_leilao_imovel_campos(iid, {
                                    "status": "analisado",
                                    "valor_venda_sugerido": snap["venda_bruta"],
                                    "valor_mercado_estimado": snap["venda_bruta"],
                                    "custo_reforma_estimado": reforma,
                                    "roi_projetado": snap["roi_liquido_pct"],
                                    "valor_venda_liquido": snap["venda_pc"],
                                    "lance_maximo_recomendado": lance_max,
                                    "fator_liquidez_venda": snap["fat_liq"],
                                })
                                st.success("Gravado com sucesso!")
                                _query_table.clear()
                                _refresh()

                with right:
                    st.markdown('<div class="section-title">Comparáveis</div>', unsafe_allow_html=True)
                    comp_fd = _ca_pool_filters_expander_ui(
                        key_prefix="sim_comp_tbl",
                        title="Filtros da tabela (comparáveis)",
                        expanded=True,
                    )
                    df_comp = _query_anuncios(limit=700)
                    if df_comp.empty:
                        st.info("Sem anúncios cadastrados.")
                    else:
                        df_comp = df_comp[_ca_mask_pool_like(df_comp, **comp_fd)].reset_index(drop=True)
                        cols_c = [c for c in ["cidade", "bairro", "tipo_imovel", "valor_venda", "preco_m2", "area_construida_m2", "url_anuncio"] if c in df_comp.columns]
                        if not df_comp.empty:
                            cc_cfg = {}
                            if "url_anuncio" in df_comp.columns:
                                cc_cfg["url_anuncio"] = st.column_config.LinkColumn("Link", display_text="Abrir")
                            st.dataframe(_fmt_df(df_comp[cols_c]), width="stretch", height=420, hide_index=True, column_config=cc_cfg)
                            if "preco_m2" in df_comp.columns:
                                pm2 = pd.to_numeric(df_comp["preco_m2"], errors="coerce").dropna()
                                if not pm2.empty:
                                    comp_kpi = '<div class="kpi-row">'
                                    comp_kpi += _kpi_card(str(len(df_comp)), "Qtd comparáveis")
                                    comp_kpi += _kpi_card(_fmt_brl(pm2.mean()), "Média R$/m²")
                                    comp_kpi += _kpi_card(_fmt_brl(pm2.median()), "Mediana R$/m²")
                                    comp_kpi += '</div>'
                                    st.markdown(comp_kpi, unsafe_allow_html=True)
                        else:
                            st.info("Nenhum comparável com os filtros atuais.")
    except Exception as exc:
        st.exception(exc)
