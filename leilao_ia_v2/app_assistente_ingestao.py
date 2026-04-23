"""
Interface Streamlit — análise de leilão (ingestão), comparáveis via Firecrawl Search e mapas.

Rota de evolução do assistente (multi-agente, tools de BD/filtros, onboarding): `AGENTS.md` na raiz.

Execute na raiz do repositório:
  streamlit run leilao_ia_v2/app_assistente_ingestao.py

Requer: OPENAI_API_KEY, Supabase e Firecrawl (ingestão de edital + busca de comparáveis) — ver `.env`.
O **modo** (Painel, Leilões, Simulação, Dados) e a **ingestão por URL** ficam na **barra lateral**; métricas e saldo Firecrawl também.
"""

from __future__ import annotations

import contextlib
import enum
import html
import json
import statistics
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# Streamlit coloca no path a pasta do script, não a raiz do repo — precisamos do pai para `import leilao_ia_v2`.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import logging
import uuid

import streamlit as st
from dotenv import load_dotenv

from leilao_ia_v2.exceptions import (
    EscolhaSobreDuplicataNecessaria,
    IngestaoSemConteudoEditalError,
    UrlInvalidaIngestaoError,
)
from leilao_ia_v2.normalizacao import normalizar_url_leilao
from leilao_ia_v2.persistence import (
    anuncios_mercado_repo,
    cache_media_bairro_repo,
    leilao_imoveis_repo,
)
from leilao_ia_v2.config.busca_mercado_parametros import (
    defaults_chaves_busca_mercado_session,
    parametros_de_session_state,
)
from leilao_ia_v2.pipeline.ingestao_edital import executar_ingestao_edital
from leilao_ia_v2.schemas.operacao_simulacao import (
    ModoPagamentoSimulacao,
    ModoReforma,
    ModoRoiDesejado,
    ModoValorVenda,
    OperacaoSimulacaoDocumento,
    SimulacaoOperacaoInputs,
    SimulacaoOperacaoOutputs,
    SimulacoesModalidadesBundle,
    parse_operacao_simulacao_json,
    parse_simulacoes_modalidades_json,
)
from leilao_ia_v2.services.conteudo_edital_heuristica import MENSAGEM_ACOES_USUARIO
from leilao_ia_v2.ui.app_theme import STREAMLIT_PAGE_CSS as _PAGE_CSS
from leilao_ia_v2.ui.dashboard_comparacao_modais import build_painel_simulacao_resumo_html
from leilao_ia_v2.ui.simulacao_estado import (
    TAGS,
    construir_inputs_de_sessao,
    derramar_inputs_no_session,
    simop_ensure_tempo_venda_global,
    simop_hidratou_chave,
    simop_key,
    simop_key_cmp_painel,
    simop_key_mpag,
    simop_key_tempo_venda_global,
    simop_key_ui_nicho_prazo_fin,
    simop_m_lab_to_tag,
)

_SIMOP_MPAG_LABS = ("À vista", "Parcelado (judicial)", "Financiado (bancário)")
_SIMOP_MPAG_TO_ENUM: dict[str, ModoPagamentoSimulacao] = {
    "À vista": ModoPagamentoSimulacao.VISTA,
    "Parcelado (judicial)": ModoPagamentoSimulacao.PRAZO,
    "Financiado (bancário)": ModoPagamentoSimulacao.FINANCIADO,
}

_SIMOP_REFUI_SPECS: tuple[tuple[str, str], ...] = (
    ("none", "Sem reforma"),
    ("basica", "500/m²"),
    ("media", "1k/m²"),
    ("completa", "1,5k/m²"),
    ("alto", "2,5k/m²"),
    ("manual", "R$ livre"),
)


def _simop_mpag_label_para_valor(lab: str) -> ModoPagamentoSimulacao:
    return _SIMOP_MPAG_TO_ENUM.get(lab, ModoPagamentoSimulacao.VISTA)


def _simop_mpag_valor_default_para_label(v: object) -> str:
    if isinstance(v, ModoPagamentoSimulacao):
        for lab, en in _SIMOP_MPAG_TO_ENUM.items():
            if en == v:
                return lab
    s = str(v or "").strip().lower()
    if s in ("prazo", ModoPagamentoSimulacao.PRAZO.value):
        return "Parcelado (judicial)"
    if s in ("financiado", ModoPagamentoSimulacao.FINANCIADO.value, "fin"):
        return "Financiado (bancário)"
    return "À vista"
from leilao_ia_v2.schemas.relatorio_mercado_contexto import parse_relatorio_mercado_contexto_json
from leilao_ia_v2.services.simulacao_operacao import (
    REFORMA_RS_M2,
    calcular_simulacao,
    resolver_valor_venda_estimado,
)
from leilao_ia_v2.services.cache_media_leilao import (
    CACHE_VOLUME_BAIXO_LIMITE,
    criar_cache_manual_de_anuncios,
    formatar_log_pos_cache,
    recalcular_caches_mercado_para_leilao,
    resolver_cache_media_pos_ingestao,
)
from leilao_ia_v2.services.geocoding import geocodificar_endereco, geocodificar_texto_livre
from leilao_ia_v2.services.saldos_providers import buscar_saldo_firecrawl_cached, invalidar_cache_saldos
from leilao_ia_v2.supabase_client import get_supabase_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Cards na área principal: sem lat/lon (mapa) e sem ultima_extracao_llm_modelo.
# Inclui data_leilao e valor_arrematacao (venda direta / Caixa costuma preencher só estes).
_CAMPOS_EXTRACAO: list[tuple[str, str]] = [
    ("URL do leilão", "url_leilao"),
    ("Endereço", "endereco"),
    ("Cidade", "cidade"),
    ("Estado (UF)", "estado"),
    ("Bairro", "bairro"),
    ("Tipo do imóvel", "tipo_imovel"),
    ("Conservação", "conservacao"),
    ("Tipo casa", "tipo_casa"),
    ("Andar", "andar"),
    ("Área útil (m²)", "area_util"),
    ("Área total (m²)", "area_total"),
    ("Quartos", "quartos"),
    ("Vagas", "vagas"),
    ("Padrão", "padrao_imovel"),
    ("1ª praça — data", "data_leilao_1_praca"),
    ("1ª praça — valor lance", "valor_lance_1_praca"),
    ("2ª praça — data", "data_leilao_2_praca"),
    ("2ª praça — valor lance", "valor_lance_2_praca"),
    ("Data (edital / leilão genérico)", "data_leilao"),
    ("Valor de arrematação (referência)", "valor_arrematacao"),
    ("Valor de avaliação", "valor_avaliacao"),
]


def _raw_extracao_ocultar(raw: Any) -> bool:
    """True = não exibir card (vazio, nulo ou zero numérico)."""
    if raw is None:
        return True
    if isinstance(raw, str) and raw.strip() == "":
        return True
    if isinstance(raw, (list, tuple, dict)) and len(raw) == 0:
        return True
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        if raw == 0 or raw == 0.0:
            return True
    return False


def _leilao_extra_tem_conteudo(extra: Any) -> bool:
    if not isinstance(extra, dict) or not extra:
        return False
    for v in extra.values():
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return True
        if isinstance(v, (list, tuple)) and len(v) > 0:
            return True
        if isinstance(v, dict) and len(v) > 0:
            return True
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v != 0:
            return True
    return False


def _leilao_extra_scalar_txt(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "sim" if v else "não"
    s = str(v).strip()
    return s if s else "—"


def _leilao_extra_como_texto(extra: dict[str, Any]) -> str:
    """Representação em texto simples (sem JSON) para exibição na UI."""
    lines: list[str] = []

    def walk(prefix: str, obj: Any) -> None:
        if isinstance(obj, dict):
            for k, val in obj.items():
                key = str(k)
                if isinstance(val, dict) and val:
                    lines.append(f"{prefix}{key}:")
                    walk(prefix + "  ", val)
                elif isinstance(val, list):
                    lines.append(f"{prefix}{key}:")
                    for i, item in enumerate(val):
                        if isinstance(item, dict) and item:
                            lines.append(f"{prefix}  [{i}]")
                            walk(prefix + "    ", item)
                        elif isinstance(item, list) and item:
                            lines.append(f"{prefix}  [{i}]")
                            walk(prefix + "    ", item)
                        else:
                            lines.append(f"{prefix}  - {_leilao_extra_scalar_txt(item)}")
                else:
                    lines.append(f"{prefix}{key}: {_leilao_extra_scalar_txt(val)}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, (dict, list)) and item:
                    lines.append(f"{prefix}[{i}]")
                    walk(prefix + "  ", item)
                else:
                    lines.append(f"{prefix}- {_leilao_extra_scalar_txt(item)}")

    walk("", extra)
    return "\n".join(lines) if lines else "—"


def _fmt_milhar_decimal_br(n: float | None, dec: int = 2) -> str:
    """Número com ponto de milhar e vírgula decimal (pt-BR), sem sufixo de moeda."""
    if n is None:
        return "—"
    v = float(n)
    neg = "-" if v < 0 else ""
    a = abs(v)
    s = f"{a:,.{dec}f}"
    s = s.replace(",", "§").replace(".", ",").replace("§", ".")
    return f"{neg}{s}"


def _fmt_pct_de_frac(frac: float | None, *, dec: int = 2) -> str:
    """Fração (0–1) exibida em % com formatação pt-BR (ex. 12,5 %)."""
    if frac is None:
        return "—"
    return _fmt_milhar_decimal_br(float(frac) * 100.0, dec=dec) + " %"


def _fmt_valor_campo(key: str, val: Any) -> str:
    if val is None or val == "":
        return "—"
    if "valor" in key or key == "valor_arrematacao":
        try:
            x = float(val)
            inteiro = int(abs(round(x * 100))) // 100
            cent = int(abs(round(x * 100))) % 100
            neg = "-" if x < 0 else ""
            corpo = f"{inteiro:,}".replace(",", ".")
            return f"{neg}R$ {corpo},{cent:02d}"
        except (TypeError, ValueError):
            return str(val)
    if "area" in key:
        try:
            return f"{float(val):.2f}".replace(".", ",") + " m²"
        except (TypeError, ValueError):
            return str(val)
    if key in ("latitude", "longitude"):
        try:
            return f"{float(val):.6f}"
        except (TypeError, ValueError):
            return str(val)
    return str(val)


def _html_card_campo_extracao(row: dict[str, Any], label: str, key: str) -> str | None:
    """Um card mini ou None se o campo não deve ser exibido."""
    raw = row.get(key)
    if _raw_extracao_ocultar(raw):
        return None
    esc_l = html.escape(label)
    if key == "url_leilao":
        url_s = str(raw).strip()
        if not url_s:
            return None
        uq = html.escape(url_s, quote=True)
        return (
            f'<div class="leilao-card leilao-card-mini"><div class="leilao-card-label">{esc_l}</div>'
            f'<div class="leilao-card-value">'
            f'<a href="{uq}" target="_blank" rel="noopener noreferrer">Link</a>'
            f"</div></div>"
        )
    disp = _fmt_valor_campo(key, raw)
    if disp == "—" or not str(disp).strip():
        return None
    esc_v = html.escape(disp)
    return (
        f'<div class="leilao-card leilao-card-mini"><div class="leilao-card-label">{esc_l}</div>'
        f'<div class="leilao-card-value">{esc_v}</div></div>'
    )


def _url_foto_imovel_valida(row: dict[str, Any]) -> str | None:
    u = str(row.get("url_foto_imovel") or "").strip()
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return None


def _render_foto_imovel_acima_endereco(row: dict[str, Any]) -> None:
    """Exibe a imagem do imóvel quando há URL; fica acima do card de endereço."""
    url_foto = _url_foto_imovel_valida(row)
    if not url_foto:
        return
    uq = html.escape(url_foto, quote=True)
    st.markdown(
        f'<div class="leilao-extracao-foto-wrap">'
        f'<img src="{uq}" alt="Foto do imóvel" loading="lazy" referrerpolicy="no-referrer" />'
        f"</div>",
        unsafe_allow_html=True,
    )
    st.caption("Foto do imóvel (edital)")


def _render_cards_extracao(
    row: dict[str, Any],
    *,
    caches: list[dict[str, Any]] | None = None,
    ads_map: dict[str, Any] | None = None,
) -> None:
    if caches is None or ads_map is None:
        caches, ads_map = _carregar_caches_e_anuncios_ui(row)
    tem_foto = _url_foto_imovel_valida(row) is not None
    _render_foto_imovel_acima_endereco(row)
    blocos: list[str] = []
    addr = _html_card_campo_extracao(row, "Endereço", "endereco")
    if addr:
        blocos.append(f'<div class="leilao-grid-mini leilao-grid-address-only">{addr}</div>')

    partes_meio: list[str] = []
    for label, key in _CAMPOS_EXTRACAO:
        if key in ("endereco", "url_leilao"):
            continue
        h = _html_card_campo_extracao(row, label, key)
        if h:
            partes_meio.append(h)
    link_h = _html_card_campo_extracao(row, "URL do leilão", "url_leilao")
    if link_h:
        partes_meio.append(link_h)
    if partes_meio:
        blocos.append(f'<div class="leilao-grid-mini">{"".join(partes_meio)}</div>')

    if blocos:
        st.markdown(
            f'<div class="leilao-extracao-cards-stack">{"".join(blocos)}</div>',
            unsafe_allow_html=True,
        )
    elif not tem_foto:
        st.caption("Nenhum campo preenchido para exibir em cards.")
    try:
        doc0 = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
        doc_sim = calcular_simulacao(
            row_leilao=row,
            inp=doc0.inputs,
            caches_ordenados=caches,
            ads_por_id=ads_map,
        )
        o_sim = doc_sim.outputs
    except Exception:
        logger.exception("calcular_simulacao (cards indicadores na análise)")
        o_sim = None
    if o_sim is not None:
        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Indicadores da operação (simulação)</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="sim-card-html">{_html_sim_venda_lucros_tres_cards(o_sim)}</div>',
                unsafe_allow_html=True,
            )
            st.caption(
                "Mesmos critérios da aba Simulação: parâmetros gravados em `operacao_simulacao_json` "
                "ou valores padrão até você gravar uma simulação."
            )
    extra = row.get("leilao_extra_json")
    if isinstance(extra, dict) and _leilao_extra_tem_conteudo(extra):
        with st.expander("Dados adicionais", expanded=False):
            st.text(_leilao_extra_como_texto(extra))


def _ids_cache_media_do_row(row: dict[str, Any]) -> list[str]:
    raw = row.get("cache_media_bairro_ids")
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if str(x).strip()]


def _parse_csv_uuids_ids_anuncios(raw: Any) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _carregar_caches_e_anuncios_ui(
    row: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """
    Devolve caches na ordem de ``cache_media_bairro_ids`` e mapa id→anúncio
    (união dos anúncios referenciados por esses caches).
    """
    ordem = _ids_cache_media_do_row(row)
    if not ordem:
        return [], {}
    cli = get_supabase_client()
    rows = cache_media_bairro_repo.buscar_por_ids(cli, ordem)
    by_id = {str(r.get("id") or ""): r for r in rows if r.get("id")}
    ordenados = [by_id[i] for i in ordem if i in by_id]
    todos_ids: set[str] = set()
    for c in ordenados:
        todos_ids.update(_parse_csv_uuids_ids_anuncios(c.get("anuncios_ids")))
    ads_list = anuncios_mercado_repo.buscar_por_ids(cli, list(todos_ids)) if todos_ids else []
    ads_map = {str(a.get("id") or ""): a for a in ads_list if a.get("id")}
    return ordenados, ads_map


def _refazer_calculo_simulacao_leilao(client: Any, leilao_id: str) -> tuple[bool, str]:
    """
    Reexecuta ``calcular_simulacao`` com os parâmetros gravados em ``operacao_simulacao_json``
    e a lista atual de caches no leilão; persiste o documento em ``operacao_simulacao_json``.
    Não regera ``relatorio_mercado_contexto_json`` (análise por LLM).
    """
    iid = str(leilao_id or "").strip()
    if not iid:
        return False, "ID de leilão inválido."
    row = leilao_imoveis_repo.buscar_por_id(iid, client)
    if not isinstance(row, dict):
        return False, "Leilão não encontrado."
    doc0 = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
    inp = doc0.inputs
    caches, ads_map = _carregar_caches_e_anuncios_ui(row)
    try:
        doc = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=caches, ads_por_id=ads_map)
        leilao_imoveis_repo.atualizar_operacao_simulacao_json(iid, doc.model_dump(mode="json"), client)
    except Exception as e:
        logger.exception("Refazer cálculo simulação (vínculo cache)")
        return False, f"Falha ao recalcular: {e}"
    return (
        True,
        "Simulação recalculada e gravada em `operacao_simulacao_json` (lucro/ROI com caches atuais).",
    )


def _metadados_cache_row_ui(cache_row: dict[str, Any]) -> dict[str, Any]:
    raw_m = cache_row.get("metadados_json")
    if isinstance(raw_m, dict):
        return raw_m
    if isinstance(raw_m, str) and raw_m.strip():
        try:
            return json.loads(raw_m)
        except json.JSONDecodeError:
            return {}
    return {}


def _cache_e_principal_simulacao(cache_row: dict[str, Any]) -> bool:
    """Linha usada para simulação de venda (exclui referência extra e terrenos)."""
    md = _metadados_cache_row_ui(cache_row)
    papel = str(md.get("cache_papel") or "").strip()
    if papel == "principal_simulacao":
        return True
    if papel in ("referencia_extra", "terrenos_referencia"):
        return False
    if str(md.get("modo_cache") or "").strip().lower() == "terrenos":
        return False
    if md.get("apenas_referencia") is True:
        return False
    if md.get("uso_simulacao") is False:
        return False
    return True


def _row_cache_principal_simulacao(caches: list[dict[str, Any]]) -> dict[str, Any] | None:
    for c in caches:
        if _cache_e_principal_simulacao(c):
            return c
    return None


def _lista_alertas_volume_amostras_cache(caches: list[dict[str, Any]]) -> list[tuple[str, int]]:
    """Só o cache principal de simulação: aviso se tiver menos de ``CACHE_VOLUME_BAIXO_LIMITE`` amostras."""
    row_p = _row_cache_principal_simulacao(caches)
    if row_p is None:
        return []
    try:
        n = int(row_p.get("n_amostras") or 0)
    except (TypeError, ValueError):
        n = 0
    if n >= CACHE_VOLUME_BAIXO_LIMITE:
        return []
    nome = str(row_p.get("nome_cache") or "Cache principal").strip() or "Cache principal"
    return [(nome, n)]


def _render_alerta_volume_amostras_cache(caches: list[dict[str, Any]]) -> None:
    alertas = _lista_alertas_volume_amostras_cache(caches)
    if not alertas:
        return
    nm, n = alertas[0]
    st.markdown(
        '<div class="leilao-alerta-amostras" role="alert">'
        '<p class="leilao-alerta-amostras-title">Volume de amostras baixo — risco à estimativa</p>'
        "<p class=\"leilao-alerta-amostras-body\">"
        f"O <strong>cache principal</strong> de simulação (<em>{html.escape(nm)}</em>) tem "
        f"<strong>{n}</strong> amostra(s) — abaixo de <strong>{CACHE_VOLUME_BAIXO_LIMITE}</strong>. "
        "A mediana e as médias de mercado ficam menos estáveis; alargue <strong>raio</strong> ou critérios em "
        "<strong>⚙️ Ajustes de busca</strong> ou aguarde mais listagens no banco.</p>"
        "</div>",
        unsafe_allow_html=True,
    )


# Borda (stroke) e preenchimento para ``CircleMarker`` — cores espaçadas no matiz para leitura rápida no mapa.
_MAPA_CORES_CACHE: tuple[tuple[str, str], ...] = (
    ("#1d4ed8", "#60a5fa"),  # azul royal
    ("#a21caf", "#e879f9"),  # magenta / fúcsia
    ("#c2410c", "#fb923c"),  # laranja
    ("#3f6212", "#bef264"),  # verde lima (borda oliva)
    ("#b45309", "#fcd34d"),  # âmbar / ouro
    ("#3730a3", "#a5b4fc"),  # índigo / lavanda
)


def _build_comparaveis_mapa_por_cache(
    caches: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Lista de anúncios para o mapa: cada ponto herda cor do **primeiro** cache (ordem do imóvel)
    que contém o ``id`` do anúncio. Cores repetem-se além de ``len(_MAPA_CORES_CACHE)``.
    """
    out: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for idx, c in enumerate(caches):
        stroke, fill = _MAPA_CORES_CACHE[idx % len(_MAPA_CORES_CACHE)]
        nome = str(c.get("nome_cache") or f"Cache {idx + 1}").strip() or f"Cache {idx + 1}"
        cid = str(c.get("id") or "").strip() or f"_idx{idx}"
        for ann_id in _parse_csv_uuids_ids_anuncios(c.get("anuncios_ids")):
            if ann_id in seen_ids or ann_id not in ads_map:
                continue
            seen_ids.add(ann_id)
            a = dict(ads_map[ann_id])
            a["_mapa_color"] = stroke
            a["_mapa_fill_color"] = fill
            a["_mapa_cache_index"] = idx
            a["_mapa_cache_nome"] = nome
            a["_mapa_cache_id"] = cid
            out.append(a)
    return out


def _legenda_entradas_mapa_caches(comps: list[dict[str, Any]]) -> list[tuple[int, str, str, str]]:
    """Índice de cache, nome exibido, cor borda, cor preenchimento (só entradas com meta de mapa)."""
    by_idx: dict[int, tuple[str, str, str]] = {}
    for a in comps:
        raw_i = a.get("_mapa_cache_index")
        if raw_i is None:
            continue
        try:
            i = int(raw_i)
        except (TypeError, ValueError):
            continue
        if i in by_idx:
            continue
        nome = str(a.get("_mapa_cache_nome") or f"Cache {i + 1}").strip() or f"Cache {i + 1}"
        stroke = str(a.get("_mapa_color") or "#38bdf8")
        fill = str(a.get("_mapa_fill_color") or "#0ea5e9")
        by_idx[i] = (nome, stroke, fill)
    return [(i, *by_idx[i]) for i in sorted(by_idx)]


def _render_legenda_cores_mapa_caches(comps: list[dict[str, Any]]) -> None:
    entradas = _legenda_entradas_mapa_caches(comps)
    if not entradas:
        return
    parts: list[str] = []
    for _i, nome, stroke, fill in entradas:
        ne = html.escape(nome)
        parts.append(
            "<span style=\"display:inline-flex;align-items:center;margin-right:1.1rem;"
            "margin-bottom:0.35rem;\">"
            f"<span style=\"display:inline-block;width:11px;height:11px;border-radius:50%;"
            f"background:{html.escape(fill)};border:2px solid {html.escape(stroke)};"
            "margin-right:0.4rem;opacity:0.92;box-sizing:border-box;\"></span>"
            f"<span style=\"font-size:0.86rem;color:#e8edf5;\">{ne}</span></span>"
        )
    st.markdown(
        '<p style="margin:0 0 0.45rem 0;font-size:0.72rem;text-transform:uppercase;letter-spacing:0.08em;'
        'color:#94a3b8;">Comparáveis por cache</p>'
        '<div style="margin-bottom:0.55rem;line-height:1.35;">'
        + "".join(parts)
        + "</div>",
        unsafe_allow_html=True,
    )


def _areas_e_valores_de_anuncios(ads: list[dict[str, Any]]) -> tuple[list[float], list[float]]:
    areas: list[float] = []
    vals: list[float] = []
    for a in ads:
        try:
            ar = float(a.get("area_construida_m2") or 0)
            v = float(a.get("valor_venda") or 0)
        except (TypeError, ValueError):
            continue
        if ar > 0:
            areas.append(ar)
        if v > 0:
            vals.append(v)
    return areas, vals


def _render_painel_cache_mercado(
    caches: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
) -> None:
    if not caches:
        st.markdown(
            '<p class="leilao-cache-empty">Nenhum cache de mercado vinculado a este imóvel '
            "(<code>cache_media_bairro_ids</code> vazio). Após criar o cache pela ferramenta "
            "de análise, os comparáveis aparecerão aqui.</p>",
            unsafe_allow_html=True,
        )
        return

    blocos: list[str] = []
    for c in caches:
        nome = html.escape(str(c.get("nome_cache") or "Cache de mercado").strip() or "Cache")
        tipo_seg = html.escape(str(c.get("tipo_imovel") or "—"))
        raw_meta = c.get("metadados_json")
        if isinstance(raw_meta, dict):
            md_c: dict = raw_meta
        elif isinstance(raw_meta, str) and raw_meta.strip():
            try:
                md_c = json.loads(raw_meta)
            except json.JSONDecodeError:
                md_c = {}
        else:
            md_c = {}
        if md_c.get("modo_cache") == "terrenos" or md_c.get("apenas_referencia") is True or md_c.get("uso_simulacao") is False:
            papel_html = ' <span style="color:#fb923c;font-weight:600;">[referência]</span>'
        else:
            papel_html = ' <span style="color:#4ade80;font-weight:600;">[simulação]</span>'
        n_db = c.get("n_amostras")
        ann_ids = _parse_csv_uuids_ids_anuncios(c.get("anuncios_ids"))
        ads_seg = [ads_map[i] for i in ann_ids if i in ads_map]
        areas, vals = _areas_e_valores_de_anuncios(ads_seg)

        if areas:
            med_m2 = statistics.mean(areas)
            mediana_m2 = statistics.median(areas)
            m_m2 = _fmt_valor_campo("area_util", med_m2)
            md_m2 = _fmt_valor_campo("area_util", mediana_m2)
        else:
            m_m2 = md_m2 = "—"

        if vals:
            vmin = min(vals)
            vmax = max(vals)
            vmed = statistics.mean(vals)
            s_min = _fmt_valor_campo("valor_venda", vmin)
            s_med = _fmt_valor_campo("valor_venda", vmed)
            s_max = _fmt_valor_campo("valor_venda", vmax)
        else:
            s_min = _fmt_valor_campo("valor_venda", c.get("menor_valor_venda"))
            s_med = _fmt_valor_campo("valor_venda", c.get("valor_medio_venda"))
            s_max = _fmt_valor_campo("valor_venda", c.get("maior_valor_venda"))

        kpi_row = (
            f'<div class="leilao-cache-kpi-row">'
            f'<div class="leilao-cache-kpi"><div class="lbl">Média m²</div><div class="val">{m_m2}</div></div>'
            f'<div class="leilao-cache-kpi"><div class="lbl">Mediana m²</div><div class="val">{md_m2}</div></div>'
            f'<div class="leilao-cache-kpi"><div class="lbl">Menor valor</div><div class="val">{s_min}</div></div>'
            f'<div class="leilao-cache-kpi"><div class="lbl">Valor médio</div><div class="val">{s_med}</div></div>'
            f'<div class="leilao-cache-kpi"><div class="lbl">Maior valor</div><div class="val">{s_max}</div></div>'
            f"</div>"
        )

        rows_html: list[str] = []
        if ads_seg:
            rows_html.append(
                "<tr><th>Endereço</th><th>Bairro</th><th>m²</th><th>Valor</th><th>Anúncio</th></tr>"
            )
            for a in ads_seg:
                ender = html.escape(str(a.get("logradouro") or "—").strip() or "—")
                bai = html.escape(str(a.get("bairro") or "—").strip() or "—")
                try:
                    am = float(a.get("area_construida_m2") or 0)
                    m2c = html.escape(_fmt_valor_campo("area_util", am) if am > 0 else "—")
                except (TypeError, ValueError):
                    m2c = "—"
                try:
                    vv = float(a.get("valor_venda") or 0)
                    vc = html.escape(_fmt_valor_campo("valor_venda", vv) if vv > 0 else "—")
                except (TypeError, ValueError):
                    vc = "—"
                url = str(a.get("url_anuncio") or "").strip()
                if url:
                    uq = html.escape(url, quote=True)
                    link_cell = f'<a href="{uq}" target="_blank" rel="noopener noreferrer">abrir</a>'
                else:
                    link_cell = "—"
                rows_html.append(f"<tr><td>{ender}</td><td>{bai}</td><td>{m2c}</td><td>{vc}</td><td>{link_cell}</td></tr>")
        else:
            rows_html.append(
                '<tr><td colspan="5" style="color:#94a3b8">Nenhum anúncio resolvido no banco para os IDs deste cache.</td></tr>'
            )

        cap_n = ""
        if n_db is not None:
            cap_n = html.escape(f" · n={n_db} (registro)")
        blocos.append(
            f'<div class="leilao-cache-segment">'
            f"<div><strong>{nome}</strong>{papel_html} · <span style=\"color:#94a3b8\">{tipo_seg}</span>{cap_n}</div>"
            f"{kpi_row}"
            f'<div class="leilao-cache-table-wrap"><table class="leilao-cache-table">{"".join(rows_html)}</table></div>'
            f"</div>"
        )

    st.markdown("".join(blocos), unsafe_allow_html=True)


def _simop_hidratar_modalidades(
    iid: str, row: dict[str, Any], legado: dict[str, Any] | None
) -> None:
    """Uma vez por imóvel: preenche chaves de sessão de vista/prazo/financiado a partir do banco/legado."""
    hk = simop_hidratou_chave(iid)
    if st.session_state.get(hk):
        return
    bundle = parse_simulacoes_modalidades_json(
        row.get("simulacoes_modalidades_json") if row else None,
        legado_operacao=legado,
    )
    for tag in TAGS:
        doc_t: OperacaoSimulacaoDocumento = getattr(bundle, tag)
        derramar_inputs_no_session(iid, tag, doc_t.inputs)
    t_raw = bundle.vista.inputs.tempo_estimado_venda_meses
    t_glob = float(t_raw) if t_raw is not None else 12.0
    st.session_state[simop_key_tempo_venda_global(iid)] = t_glob
    st.session_state[hk] = True


def _ref_mod_brl_da_sessao(
    iid: str, tag: str, ref_ui_labels: list[str], ref_ui_keys: list[str], rm0: float, inp0: SimulacaoOperacaoInputs
) -> tuple[ModoReforma, float]:
    ref_pick = str(st.session_state.get(simop_key(iid, tag, "refui_lbl"), "") or "")
    if not ref_pick:
        r0, _rbr = _reforma_ui_defaults(inp0)
        ref_pick = dict(zip(ref_ui_keys, ref_ui_labels)).get(r0, ref_ui_labels[0])
    rmanual = float(st.session_state.get(simop_key(iid, tag, "refmanual"), rm0) or 0.0)
    try:
        ix = ref_ui_labels.index(ref_pick)
    except ValueError:
        ix = 0
    uik = ref_ui_keys[ix]
    return _reforma_modo_valor_de_ui(uik, rmanual if uik == "manual" else 0.0)


def _area_m2_row_sim(row: dict[str, Any]) -> float:
    for k in ("area_util", "area_total"):
        try:
            v = float(row.get(k) or 0)
        except (TypeError, ValueError):
            continue
        if v > 0:
            return v
    return 0.0


def _preview_brl_leiloeiro(lance: float, pct: float, fixo: float) -> float:
    if fixo > 0:
        return round(fixo, 2)
    if lance > 0 and pct > 0:
        return round(lance * (pct / 100.0), 2)
    return 0.0


def _preview_brl_itbi(lance: float, pct: float, fixo: float) -> float:
    return _preview_brl_leiloeiro(lance, pct, fixo)


def _preview_brl_corretagem(venda: float, pct: float, fixo: float) -> float:
    if fixo > 0:
        return round(fixo, 2)
    if venda > 0 and pct > 0:
        return round(venda * (pct / 100.0), 2)
    return 0.0


def _preview_brl_reforma(area: float, ref_mod: ModoReforma, manual: float) -> tuple[float, str]:
    m = str(ref_mod.value if hasattr(ref_mod, "value") else ref_mod)
    if m == ModoReforma.MANUAL.value:
        return round(float(manual or 0), 2), "manual"
    r = float(REFORMA_RS_M2.get(m, 0.0))
    if area <= 0:
        return 0.0, m
    return round(area * r, 2), m


def _seg_sim_single(raw: Any, default: str) -> str:
    if raw is None:
        return default
    if isinstance(raw, list):
        return str(raw[0]) if raw else default
    return str(raw)


def _reforma_ui_defaults(inp0: SimulacaoOperacaoInputs) -> tuple[str, float]:
    if inp0.reforma_modo == ModoReforma.MANUAL:
        br = float(inp0.reforma_brl or 0)
        return ("manual", br) if br > 0 else ("none", 0.0)
    mp = {
        ModoReforma.BASICA: "basica",
        ModoReforma.MEDIA: "media",
        ModoReforma.COMPLETA: "completa",
        ModoReforma.ALTO_PADRAO: "alto",
    }
    k = mp.get(inp0.reforma_modo, "none")
    return k, float(inp0.reforma_brl or 0)


def _reforma_modo_valor_de_ui(ui_key: str, manual_r: float) -> tuple[ModoReforma, float]:
    if ui_key == "none":
        return ModoReforma.MANUAL, 0.0
    if ui_key == "manual":
        return ModoReforma.MANUAL, max(0.0, float(manual_r))
    um = {
        "basica": ModoReforma.BASICA,
        "media": ModoReforma.MEDIA,
        "completa": ModoReforma.COMPLETA,
        "alto": ModoReforma.ALTO_PADRAO,
    }
    return um[ui_key], 0.0


def _html_sim_res_card(
    label: str,
    valor: str,
    *,
    sub: str = "",
    accent: bool = False,
    val_class: str = "",
) -> str:
    esc_l = html.escape(label)
    esc_v = html.escape(valor)
    sub_h = f'<div class="sim-res-sub">{html.escape(sub)}</div>' if sub else ""
    ac = " sim-res-card--accent" if accent else ""
    vc = f" {val_class}" if val_class in ("ok", "err", "warn", "muted") else ""
    return (
        f'<div class="sim-res-card{ac}">'
        f'<div class="sim-res-lbl">{esc_l}</div>'
        f'<div class="sim-res-val{vc}">{esc_v}</div>{sub_h}</div>'
    )


def _html_sim_res_section(title: str, cards: list[str]) -> str:
    inner = "".join(cards)
    return (
        f'<div class="sim-fin-sec">'
        f'<div class="sim-fin-h">{html.escape(title)}</div>'
        f'<div class="sim-res-grid">{inner}</div>'
        f"</div>"
    )


# ROI bruto < 40 % (fração 0,4) → destaca vermelho (err) lucro bruto e líquido no painel financeiro.
_ROI_BRUTO_ALVO_MIN = 0.4


def _sim_val_class_lucro_bruto_liquido(o: SimulacaoOperacaoOutputs) -> tuple[str, str]:
    rb = o.roi_bruto
    if rb is not None and float(rb) < _ROI_BRUTO_ALVO_MIN:
        return "err", "err"
    cls_lb = "ok" if (o.lucro_bruto or 0) >= 0 else "err"
    cls_ll = "ok" if (o.lucro_liquido or 0) >= 0 else "err"
    return cls_lb, cls_ll


def _html_sim_venda_lucros_tres_cards(o: SimulacaoOperacaoOutputs) -> str:
    """Mesmos três cards do painel financeiro da simulação: venda estimada, lucro bruto, lucro líquido."""
    venda = _fmt_valor_campo("valor_venda", o.valor_venda_estimado)
    lucro_b = _fmt_valor_campo("valor_venda", o.lucro_bruto)
    roi_b = f"{(o.roi_bruto or 0) * 100:.2f} %" if o.roi_bruto is not None else "—"
    lucro_l = _fmt_valor_campo("valor_venda", o.lucro_liquido)
    roi_l = f"{(o.roi_liquido or 0) * 100:.2f} %" if o.roi_liquido is not None else "—"
    cls_lb, cls_ll = _sim_val_class_lucro_bruto_liquido(o)
    inner = "".join(
        [
            _html_sim_res_card("Venda estimada", venda, accent=True),
            _html_sim_res_card("Lucro bruto", lucro_b, sub=f"ROI bruto {roi_b}", val_class=cls_lb),
            _html_sim_res_card("Lucro líquido", lucro_l, sub=f"ROI líquido {roi_l}", val_class=cls_ll),
        ]
    )
    return f'<div class="sim-fin-sec"><div class="sim-res-grid">{inner}</div></div>'


def _html_simulacao_resultado_cards(o: SimulacaoOperacaoOutputs) -> str:
    """Cards HTML agrupados por etapa da operação (venda → custos → lucro/IR)."""
    venda = _fmt_valor_campo("valor_venda", o.valor_venda_estimado)
    lance_c = _fmt_valor_campo("valor_venda", o.lance_brl)
    lance_arrem_sub = "nominal (arrematação)"
    if o.desconto_pagamento_avista_ativo and (o.desconto_pagamento_avista_valor_brl or 0) > 0.01:
        lance_arrem_sub = "nominal — leiloeiro/ITBI/reg. (%) s/ cheio"
    sub_lbl = "sem corretagem"
    if (o.saldo_divida_quitacao_na_venda or 0) > 0.5:
        sub_lbl = "incl. quitação (caixa+saldo) · sem corretagem"
    sub = _fmt_valor_campo("valor_venda", o.subtotal_custos_operacao)
    ctot = _fmt_valor_campo("valor_venda", o.custo_total_com_corretagem)
    corr = _fmt_valor_campo("valor_venda", o.comissao_imobiliaria_brl)
    lucro_b = _fmt_valor_campo("valor_venda", o.lucro_bruto)
    roi_b = f"{(o.roi_bruto or 0) * 100:.2f} %" if o.roi_bruto is not None else "—"
    ir_ = _fmt_valor_campo("valor_venda", o.ir_calculado_brl)
    lucro_l = _fmt_valor_campo("valor_venda", o.lucro_liquido)
    roi_l = f"{(o.roi_liquido or 0) * 100:.2f} %" if o.roi_liquido is not None else "—"
    ref = _fmt_valor_campo("valor_venda", o.reforma_brl)
    cls_lb, cls_ll = _sim_val_class_lucro_bruto_liquido(o)
    clei_sub = ""
    if (o.comissao_leiloeiro_pct_efetivo or 0) > 0:
        clei_sub = f"{o.comissao_leiloeiro_pct_efetivo:.2f} % s/ lance"
    elif o.comissao_leiloeiro_brl and o.comissao_leiloeiro_brl > 0:
        clei_sub = "fixo"
    itbi_sub = ""
    if (o.itbi_pct_efetivo or 0) > 0:
        itbi_sub = f"{o.itbi_pct_efetivo:.2f} % s/ lance"
    elif o.itbi_brl and o.itbi_brl > 0:
        itbi_sub = "fixo (legado)"

    reg_sub = ""
    if (o.registro_pct_efetivo or 0) > 0:
        reg_sub = f"{o.registro_pct_efetivo:.2f} % s/ lance"
    elif o.registro_brl and o.registro_brl > 0:
        reg_sub = "fixo"

    ir_sub = ""
    if o.ir_usou_manual:
        ir_sub = "valor fixo informado"
    elif (o.base_ir or 0) > 0:
        ir_sub = f"Base p/ IR {_fmt_valor_campo('valor_venda', o.base_ir)}"

    sec_mercado = [
        _html_sim_res_card("Venda estimada", venda, accent=True),
        _html_sim_res_card("Corretagem (saída)", corr),
    ]
    sec_arrematacao = [
        _html_sim_res_card("Lance (arrematação)", lance_c, sub=lance_arrem_sub),
    ]
    if o.desconto_pagamento_avista_ativo and (o.lance_pago_apos_desconto_brl or 0) >= 0 and (
        abs((o.lance_brl or 0) - (o.lance_pago_apos_desconto_brl or 0)) > 0.01
    ):
        sec_arrematacao.append(
            _html_sim_res_card(
                "Lance pago (à vista)",
                _fmt_valor_campo("valor_venda", o.lance_pago_apos_desconto_brl),
                sub="caixa do imóvel após desconto",
            )
        )
    sec_desconto_av: list[str] = []
    if o.desconto_pagamento_avista_ativo and (o.desconto_pagamento_avista_valor_brl or 0) > 0.01:
        sec_desconto_av = [
            _html_sim_res_card(
                "Desconto (à vista)",
                _fmt_valor_campo("valor_venda", o.desconto_pagamento_avista_valor_brl),
                sub=f"{o.desconto_pagamento_avista_pct_efetivo or 0:.2f} % s/ lance nominal",
                accent=True,
            ),
        ]
    sec_tributos = [
        _html_sim_res_card(
            "Comissão leiloeiro",
            _fmt_valor_campo("valor_venda", o.comissao_leiloeiro_brl),
            sub=clei_sub,
        ),
        _html_sim_res_card(
            "ITBI",
            _fmt_valor_campo("valor_venda", o.itbi_brl),
            sub=itbi_sub,
        ),
        _html_sim_res_card("Registro", _fmt_valor_campo("valor_venda", o.registro_brl), sub=reg_sub),
    ]
    sec_custo_ref = [
        _html_sim_res_card("Condomínio atrasado", _fmt_valor_campo("valor_venda", o.condominio_atrasado_brl)),
        _html_sim_res_card("IPTU atrasado", _fmt_valor_campo("valor_venda", o.iptu_atrasado_brl)),
        _html_sim_res_card("Reforma", ref, sub=str(o.reforma_modo_resolvido or "—")),
        _html_sim_res_card("Desocupação", _fmt_valor_campo("valor_venda", o.desocupacao_brl)),
        _html_sim_res_card("Outros custos", _fmt_valor_campo("valor_venda", o.outros_custos_brl)),
    ]
    sec_totais = [
        _html_sim_res_card("Subtotal operação", sub, sub=sub_lbl),
        _html_sim_res_card("Custo total (op. + corret.)", ctot),
    ]
    rla = o.roi_liquido_anualizado
    rba = o.roi_bruto_anualizado
    anual_suf_l = f" · anual. {(rla * 100):.2f} %" if rla is not None else ""
    anual_suf_b = f" · anual. {(rba * 100):.2f} %" if rba is not None else ""
    sec_lucro = [
        _html_sim_res_card("Lucro bruto", lucro_b, sub=f"ROI bruto {roi_b}{anual_suf_b}", val_class=cls_lb),
        _html_sim_res_card("IR", ir_, sub=ir_sub),
        _html_sim_res_card("Lucro líquido", lucro_l, sub=f"ROI líq. {roi_l}{anual_suf_l}", val_class=cls_ll),
    ]
    lmx = o.lance_maximo_para_roi_desejado
    lmx_ok = lmx is not None and float(lmx) > 0
    lmax_val = _fmt_valor_campo("valor_venda", float(lmx)) if lmx_ok else "—"
    lmax_sub_parts: list[str] = []
    rinf = o.roi_desejado_pct_informado
    if rinf is not None and float(rinf) > 0:
        modo_inf = str(o.roi_desejado_modo_informado or "bruto").strip().lower()
        base_lbl = "ROI bruto" if modo_inf in ("bruto", "") else "ROI líquido"
        lmax_sub_parts.append(f"Meta {float(rinf):.2f} % · {base_lbl}")
    if o.lance_maximo_roi_notas:
        lmax_sub_parts.append(str(o.lance_maximo_roi_notas[0])[:160])
    lmax_sub = (
        " · ".join(lmax_sub_parts)
        if lmax_sub_parts
        else ("Informe ROI desejado > 0 para estimar o teto de lance." if not lmx_ok else "")
    )
    sec_lance_max = [
        _html_sim_res_card(
            "Lance máximo recomendado",
            lmax_val,
            sub=lmax_sub,
            accent=bool(lmx_ok),
        ),
    ]
    sec_horizonte: list[str] = []
    if (o.tempo_estimado_venda_meses_resolvido or 0) > 0 and str(o.modo_pagamento_resolvido or "").strip():
        mp_r = str(o.modo_pagamento_resolvido or "").replace("_", " ")
        sec_horizonte = [
            _html_sim_res_card(
                "Tempo até a venda (T)",
                f"{o.tempo_estimado_venda_meses_resolvido:.1f} meses",
                sub=mp_r,
            ),
            _html_sim_res_card(
                "Desembolso de caixa (até a venda)",
                _fmt_valor_campo("valor_venda", o.investimento_cash_ate_momento_venda or 0),
                sub="Inclui entrada, custos iniciais e parcelas vencidas em T (base p/ ROI).",
            ),
        ]
        if (o.saldo_divida_quitacao_na_venda or 0) > 0.5:
            sec_horizonte.append(
                _html_sim_res_card(
                    "Quitação (saldo na venda)",
                    _fmt_valor_campo("valor_venda", o.saldo_divida_quitacao_na_venda),
                    sub="Saldo a liquidar com o comprador/instituição no repasse.",
                )
            )
        if (o.pmt_mensal_resolvido or 0) > 0.5:
            sec_horizonte.append(
                _html_sim_res_card(
                    "Parcela (referência)",
                    _fmt_valor_campo("valor_venda", o.pmt_mensal_resolvido),
                    sub="SAC: 1.ª prestação (amort.+juros). Price: PMT fixa. Taxa a.a. → juros mensais compostos.",
                )
            )
        if (o.total_juros_ate_momento_venda or 0) > 0.5:
            sec_horizonte.append(
                _html_sim_res_card(
                    "Juros no período",
                    _fmt_valor_campo("valor_venda", o.total_juros_ate_momento_venda),
                    sub="Parte de juros nas parcelas até T (aprox. Price/SAC).",
                )
            )
    fin_blocks: list[str] = [
        _html_sim_res_section("Mercado", sec_mercado),
    ]
    if sec_horizonte:
        fin_blocks.append(_html_sim_res_section("Prazo até a venda", sec_horizonte))
    fin_blocks.append(_html_sim_res_section("Arrematação", sec_arrematacao))
    if sec_desconto_av:
        fin_blocks.append(_html_sim_res_section("Desconto à vista", sec_desconto_av))
    fin_blocks.extend(
        [
            _html_sim_res_section("Tributos", sec_tributos),
            _html_sim_res_section("Custo e reforma", sec_custo_ref),
            _html_sim_res_section("Totais", sec_totais),
            _html_sim_res_section("Lucro e IR", sec_lucro),
            _html_sim_res_section("Sensibilidade (ROI desejado)", sec_lance_max),
        ]
    )
    return "".join(fin_blocks)


def _lance_brl_da_simulacao_gravada(row: dict[str, Any]) -> float:
    """
    Lance persistido em ``simulacoes_modalidades_json`` / legado ``operacao_simulacao_json``
    (à vista: ``inputs.lance_brl``; se zero, tenta ``outputs.lance_brl``).
    """
    b0 = parse_simulacoes_modalidades_json(
        row.get("simulacoes_modalidades_json"),
        legado_operacao=row.get("operacao_simulacao_json"),
    )
    doc = b0.vista
    l_in = float(doc.inputs.lance_brl or 0)
    if l_in > 0:
        return l_in
    if doc.outputs is not None:
        l_out = float(doc.outputs.lance_brl or 0)
        if l_out > 0:
            return l_out
    return 0.0


def _row_tem_simulacao_gravada(row: dict[str, Any] | None) -> bool:
    """Há registo em ``simulacoes_modalidades_json`` ou em ``operacao_simulacao_json`` (inputs/outputs)."""
    if not row or not isinstance(row, dict):
        return False
    if row.get("simulacoes_modalidades_json"):
        return True
    oj = row.get("operacao_simulacao_json")
    if not oj or not isinstance(oj, dict):
        return False
    return bool(oj.get("outputs") or oj.get("inputs"))


def _def_lance_para_tag(
    iid: str, tag: str, row: dict[str, Any], it: SimulacaoOperacaoInputs
) -> float:
    l1 = _lance_valor_praca_row(row, segunda=False)
    l2 = _lance_valor_praca_row(row, segunda=True)
    # Lance e 2ª praça: chaves unificadas em ``vista`` (igual a ``construir_inputs_de_sessao``).
    salvo = float(st.session_state.get(simop_key(iid, "vista", "lance"), 0) or 0.0)
    if salvo <= 0:
        salvo = float(it.lance_brl or 0)
    u2b = bool(
        st.session_state.get(simop_key(iid, "vista", "lance_2a"), it.usar_lance_segunda_praca)
    )
    if salvo > 0:
        return salvo
    ref_l = l2 if u2b and l2 > 0 else l1
    return float(ref_l or 0) or _defaults_lance_row(row)


def _construir_inp_por_tag(
    iid: str,
    row: dict[str, Any],
    tag: str,
    caches: list[dict[str, Any]],
) -> tuple[SimulacaoOperacaoInputs, SimulacaoOperacaoInputs]:
    """``(inputs, doc_inputs_fallback)`` — o legado de ``doc`` para defaults vem do bundle/legado."""
    b0 = parse_simulacoes_modalidades_json(
        row.get("simulacoes_modalidades_json"),
        legado_operacao=row.get("operacao_simulacao_json"),
    )
    it0 = getattr(b0, tag).inputs
    ref_ui_keys = [a for a, _ in _SIMOP_REFUI_SPECS]
    ref_ui_labels = [b for _, b in _SIMOP_REFUI_SPECS]
    mvs = st.session_state.get(simop_key(iid, tag, "modo_val"))
    if mvs is None:
        mvs = str(getattr(it0.modo_valor_venda, "value", it0.modo_valor_venda))
    try:
        mv = ModoValorVenda(str(mvs))
    except Exception:
        mv = it0.modo_valor_venda
    vman = float(st.session_state.get(simop_key(iid, tag, "vmanual"), 0) or 0.0)
    dfl = _def_lance_para_tag(iid, tag, row, it0)
    ref_m, ref_b = _ref_mod_brl_da_sessao(
        iid, tag, ref_ui_labels, ref_ui_keys, float(it0.reforma_brl or 0.0), it0
    )
    cse = _simop_auto_cache_id(caches, it0)
    inp = construir_inputs_de_sessao(
        iid=iid,
        tag=tag,  # type: ignore[arg-type]
        inp0=it0,
        modo_valor=mv,
        v_manual_st=vman,
        def_lance=dfl,
        ref_mod=ref_m,
        reforma_brl_inp=ref_b,
        cache_sel=cse,
    )
    return inp, it0


def _render_aba_simulacao() -> None:
    """Formulário e resultados lado a lado; o leilão ativo vem da tabela no topo da página."""
    _STATUS_EXCLUI_MAPA = frozenset({"processando", "sem_conteudo", "url_invalida"})
    row_ex = st.session_state.get("ultimo_extracao")
    snap = st.session_state.snapshot or {}
    st_status = snap.get("status")
    mostrar = (
        bool(row_ex and isinstance(row_ex, dict) and (row_ex.get("id") or row_ex.get("url_leilao")))
        and st_status not in _STATUS_EXCLUI_MAPA
    )

    tem_lista = bool(st.session_state.get("_lista_topo_ids_f")) or bool(
        st.session_state.get("_rows_resumo_leiloes")
    )
    try:
        cli = get_supabase_client()
    except Exception:
        cli = None

    if not mostrar:
        if tem_lista:
            st.info(
                "Clique numa **linha** da tabela de leilões (acima) para carregar dados e simular, "
                "ou envie uma **URL** de edital pelo chat na barra lateral."
            )
        else:
            st.info(
                "Para simular, configure o **Supabase** para carregar a tabela de leilões no topo "
                "ou ingira um edital pela **barra lateral**."
            )
        return

    caches_ui, ads_map_ui = _carregar_caches_e_anuncios_ui(row_ex)

    _render_simulacao_operacao(row_ex, caches_ui, ads_map_ui)

    st.markdown("**Relatório HTML**")
    rid = str(row_ex.get("id") or "").strip()
    if rid and cli is not None:
        c_llm_a, c_llm_b, c_llm_c = st.columns([1, 1, 2])
        with c_llm_a:
            force_ctx = st.checkbox(
                "Forçar nova LLM",
                value=False,
                key="sim_ctx_force",
                help="Ignora análise já salva no banco e chama o modelo de novo.",
            )
        with c_llm_b:
            if st.button(
                "Gerar análise mercado (LLM)",
                key="sim_ctx_llm",
                help="Grava tópicos em relatorio_mercado_contexto_json para o relatório e para reutilização.",
            ):
                try:
                    from leilao_ia_v2.agents.agente_contexto_mercado_relatorio import (
                        garantir_contexto_mercado_relatorio,
                    )

                    _doc_c, meta = garantir_contexto_mercado_relatorio(
                        cli,
                        leilao_imovel_id=rid,
                        row=row_ex,
                        caches=caches_ui,
                        ads_por_id=ads_map_ui,
                        force=bool(force_ctx),
                    )
                    if not bool(meta.get("cache_hit")):
                        _acumular_metricas_sidebar(
                            {
                                "prompt_tokens": meta.get("prompt_tokens"),
                                "completion_tokens": meta.get("completion_tokens"),
                                "custo_usd_estimado": meta.get("custo_usd_estimado"),
                                "modelo": meta.get("modelo"),
                            },
                            row_ex,
                            firecrawl_chamadas_api_ingestao=0,
                            ultima_slot="contexto_relatorio",
                        )
                    fresh = leilao_imoveis_repo.buscar_por_id(rid, cli)
                    if fresh:
                        st.session_state["ultimo_extracao"] = fresh
                        row_ex = fresh
                    if meta.get("cache_hit"):
                        st.info(
                            "Já existia análise salva; nenhuma nova chamada à API. "
                            "Marque **Forçar nova LLM** e clique de novo para regenerar."
                        )
                    else:
                        st.success("Análise de mercado gerada e salva no imóvel.")
                except Exception as e_ctx:
                    logger.exception("Contexto mercado relatório (LLM)")
                    st.error(f"Falha na análise de mercado: {e_ctx}")
        with c_llm_c:
            st.caption(
                "Requer coluna `relatorio_mercado_contexto_json` (SQL `010_relatorio_mercado_contexto_json.sql`). "
                "O download do HTML usa o que estiver salvo, sem nova LLM."
            )
    elif rid and cli is None:
        st.caption("Configure o Supabase para gravar ou regenerar a análise de mercado.")

    try:
        from leilao_ia_v2.services.relatorio_simulacao_html import montar_html_relatorio_simulacao

        if not rid:
            raise ValueError("Imóvel sem id — não é possível montar o nome do arquivo do relatório.")

        snap_iid = str(st.session_state.get("_sim_report_doc_iid") or "")
        raw_snap = st.session_state.get("_sim_report_doc_json")
        if raw_snap and snap_iid == rid:
            doc_rep = parse_operacao_simulacao_json(raw_snap)
        else:
            doc_rep = parse_operacao_simulacao_json(row_ex.get("operacao_simulacao_json"))
        k_cmp = simop_key_cmp_painel(rid)
        # Mesma chave usada após o rádio de comparação (evita divergência com simop_key_cmp_painel).
        _cache_cmp = st.session_state.get(f"_rpt_painel_cmp|{rid}")
        if isinstance(_cache_cmp, dict) and str(_cache_cmp.get("sel") or "").strip():
            cmp_sel = str(_cache_cmp.get("sel") or "nenhum").strip().lower()
        else:
            cmp_sel = str(st.session_state.get(k_cmp) or "nenhum").strip().lower()
        html_rep = montar_html_relatorio_simulacao(
            row=row_ex,
            caches=caches_ui,
            ads_map=ads_map_ui,
            doc=doc_rep,
            cmp_painel=cmp_sel,
        )
        fn = (
            f"relatorio_simulacao_{rid[:8] or 'leilao'}_"
            f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.html"
        )
        st.download_button(
            "Gerar relatório HTML",
            data=html_rep.encode("utf-8"),
            file_name=fn,
            mime="text/html",
            key=f"sim_gerar_rel_{rid[:12] or 'x'}_{cmp_sel}",
            help="HTML único: edital, dados adicionais, análise de mercado (se você gerou via LLM), comparáveis, mapa e painel financeiro.",
            use_container_width=False,
        )
    except Exception as e:
        logger.exception("Relatório HTML simulação")
        st.caption(f"Relatório HTML indisponível: {e}")

    ids_top = list(st.session_state.get("_lista_topo_ids_f") or [])
    if cli is not None and ids_top:
        _sync_sim_pick_leilao_ids(ids_top)
        _sim_sincronizar_ultimo_com_pick(ids_top, cli)


def _float_lance_campo_row(row: dict[str, Any], key: str) -> float:
    v = row.get(key)
    if v is None:
        return 0.0
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return f if f > 0 else 0.0


def _lance_valor_praca_row(row: dict[str, Any], *, segunda: bool) -> float:
    """Lance mínimo da praça no edital (>0), ou 0."""
    k = "valor_lance_2_praca" if segunda else "valor_lance_1_praca"
    return _float_lance_campo_row(row, k)


def _dict_leilao_extra_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("leilao_extra_json")
    if isinstance(raw, dict) and raw:
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return d if isinstance(d, dict) else {}
    return {}


def _defaults_lance_row(row: dict[str, Any]) -> float:
    """
    Primeiro lance/valor útil: colunas 1ª/2ª/arrematação, depois o mesmo em ``leilao_extra_json``
    (igual à heurística de exclusão de anúncios do próprio leilão).
    """
    keys = ("valor_lance_1_praca", "valor_lance_2_praca", "valor_arrematacao")
    for d in (row, _dict_leilao_extra_row(row)):
        for k in keys:
            v = d.get(k)
            if v is None:
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f > 0:
                return f
    return 0.0


def _fmt_valor_referencia_edital_resumo(r: dict[str, Any]) -> str:
    """Vírgula: lance 1/2 ou valor_arrematacao (venda direta / Caixa)."""
    v = _defaults_lance_row(r)
    if v <= 0:
        return "—"
    return _fmt_valor_campo("valor_venda", v)


def _simop_auto_cache_id(caches: list[dict[str, Any]], inp0: SimulacaoOperacaoInputs) -> str | None:
    cache_ids = [str(c.get("id") or "") for c in caches if c.get("id")]
    if not cache_ids:
        return None
    pref = str(inp0.cache_media_bairro_id or "").strip()
    return pref if pref in cache_ids else cache_ids[0]


def _simop_labels_venda_com_valores(
    *,
    row: dict[str, Any],
    caches: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
    inp0: SimulacaoOperacaoInputs,
    vmanual_preview: float,
) -> dict[str, str]:
    """Rótulos do select de modo de venda, já com o valor estimado (atualiza a cada execução)."""
    cache_id = _simop_auto_cache_id(caches, inp0)
    base = inp0.model_copy(update={"cache_media_bairro_id": cache_id})
    short = {
        ModoValorVenda.CACHE_PRECO_M2_X_AREA.value: "Cache · R$/m²",
        ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value: "Cache · valor médio",
        ModoValorVenda.CACHE_MENOR_VALOR_VENDA.value: "Cache · menor valor",
        ModoValorVenda.ANUNCIOS_VALOR_MEDIO.value: "Anúncios · média R$",
        ModoValorVenda.ANUNCIOS_MENOR_VALOR.value: "Anúncios · menor valor",
        ModoValorVenda.ANUNCIOS_PRECO_M2_X_AREA.value: "Anúncios · R$/m²",
        ModoValorVenda.MANUAL.value: "Manual",
    }
    out: dict[str, str] = {}
    for modo in (
        ModoValorVenda.CACHE_PRECO_M2_X_AREA,
        ModoValorVenda.CACHE_VALOR_MEDIO_VENDA,
        ModoValorVenda.CACHE_MENOR_VALOR_VENDA,
        ModoValorVenda.ANUNCIOS_VALOR_MEDIO,
        ModoValorVenda.ANUNCIOS_MENOR_VALOR,
        ModoValorVenda.ANUNCIOS_PRECO_M2_X_AREA,
        ModoValorVenda.MANUAL,
    ):
        mv = modo.value
        if modo == ModoValorVenda.MANUAL:
            br = (
                _fmt_valor_campo("valor_venda", vmanual_preview)
                if vmanual_preview > 0
                else "informe o valor abaixo"
            )
            out[mv] = f"{short[mv]} — {br}"
            continue
        prov = base.model_copy(update={"modo_valor_venda": modo, "valor_venda_manual": None})
        v, _meta = resolver_valor_venda_estimado(
            row_leilao=row, inp=prov, caches_ordenados=caches, ads_por_id=ads_map
        )
        vb = _fmt_valor_campo("valor_venda", float(v)) if v and float(v) > 0 else "—"
        out[mv] = f"{short[mv]} — {vb}"
    return out


def _normalizar_selecao_modo_venda(
    raw: Any,
    modo_order: list[str],
    label_map: dict[str, str],
) -> str:
    """
    Garante chave canónica (``ModoValorVenda.value``) para o selectbox.
    O estado ou versões do Streamlit podem expor o **rótulo** (``format_func``) em vez da opção.
    """
    if raw in modo_order:
        return str(raw)
    s = (str(raw) if raw is not None else "").strip()
    for mv in modo_order:
        if label_map.get(mv) == s:
            return mv
    if modo_order:
        return modo_order[0]
    return ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value


def _html_analise_mercado_ctx_painel(row: dict[str, Any]) -> str:
    """HTML dos cards de ``relatorio_mercado_contexto_json`` (vazio se não houver tópicos)."""
    raw = row.get("relatorio_mercado_contexto_json")
    if isinstance(raw, str) and raw.strip():
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raw = {}
    if not isinstance(raw, dict) or not raw:
        return ""
    doc = parse_relatorio_mercado_contexto_json(raw)
    if not any((c.topicos or []) for c in doc.cards):
        return ""
    blocos: list[str] = []
    for c in doc.cards:
        topicos = [str(t).strip() for t in (c.topicos or []) if str(t).strip()]
        if not topicos:
            continue
        lis = "".join(f"<li>{html.escape(t)}</li>" for t in topicos)
        tit = html.escape((c.titulo or c.id).strip())
        blocos.append(
            f'<div class="sim-mercado-ctx-card"><div class="sim-mercado-ctx-tit">{tit}</div><ul>{lis}</ul></div>'
        )
    if not blocos:
        return ""
    return f'<div class="sim-mercado-ctx-grid">{"".join(blocos)}</div>'


def _render_analise_mercado_abaixo_painel(row: dict[str, Any]) -> None:
    frag = _html_analise_mercado_ctx_painel(row)
    if not frag:
        return
    with st.container(border=True):
        st.markdown('<div class="sim-card-head">Análise de mercado</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="sim-card-html sim-mercado-ctx-wrap">{frag}</div>',
            unsafe_allow_html=True,
        )


def _render_simulacao_operacao(
    row: dict[str, Any],
    caches: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
    *,
    form_column: Any | None = None,
    results_column: Any | None = None,
) -> None:
    """Simulação de custos, venda estimada (cache), IR editável, lucro/ROI — persiste em ``operacao_simulacao_json``.

    Formulário em fluxo único; o painel financeiro aparece abaixo dos campos
    e da área de persistência. ``form_column`` / ``results_column`` permanecem por compatibilidade
    (ambos ``None`` neste fluxo).
    """
    iid = str(row.get("id") or "").strip()
    if not iid:
        return

    _form_ctx = form_column if form_column is not None else contextlib.nullcontext()

    def _pinta_saida_sim(o_local: SimulacaoOperacaoOutputs | None, *, titulo: str | None = None) -> None:
        if not o_local:
            st.caption("Ajuste os parâmetros acima para ver o painel financeiro.")
            return
        if titulo:
            st.markdown(f'<p class="sim-cmp-painel-tit">{titulo}</p>', unsafe_allow_html=True)
        with st.container(border=True):
            # st.html: evita o parser Markdown, que trata linhas com recuo (4+ espaços) como código
            # e mostrava o detalhamento do painel como texto bruto.
            st.html(
                f'<div class="sim-res-col-scroll sim-card-html">{build_painel_simulacao_resumo_html(o_local)}</div>'
            )
        if o_local.lance_maximo_roi_notas:
            for n in o_local.lance_maximo_roi_notas:
                st.caption(n)
        if o_local.notas:
            for n in o_local.notas:
                st.caption(n)

    with _form_ctx:
        doc0 = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
        inp0 = doc0.inputs
        _simop_hidratar_modalidades(iid, row, row.get("operacao_simulacao_json"))
        mpk = simop_key_mpag(iid)
        if mpk not in st.session_state:
            st.session_state[mpk] = "À vista"
        k_cmp_painel = simop_key_cmp_painel(iid)
        if k_cmp_painel not in st.session_state:
            st.session_state[k_cmp_painel] = "nenhum"
        st.caption(
            "O **painel financeiro principal** é sempre **à vista**. Use a comparação abaixo do painel para ver, "
            "em paralelo, **parcelado judicial** ou **financiado** com os mesmos números de lance e custos. "
            "Ative o interruptor para editar % entrada, parcelas e juros do parcelado e do bancário."
        )

        def _sk(c: str) -> str:
            return simop_key(iid, "vista", c)

        bundle_live = parse_simulacoes_modalidades_json(
            row.get("simulacoes_modalidades_json"),
            legado_operacao=row.get("operacao_simulacao_json"),
        )
        inp0_tag: SimulacaoOperacaoInputs = bundle_live.vista.inputs
        usar_2_default = bool(getattr(inp0_tag, "usar_lance_segunda_praca", False))
        l1_ed = _lance_valor_praca_row(row, segunda=False)
        l2_ed = _lance_valor_praca_row(row, segunda=True)
        salvo_lance = float(inp0_tag.lance_brl or 0)
        u2b = bool(st.session_state.get(_sk("lance_2a"), usar_2_default))
        if salvo_lance > 0:
            def_lance = salvo_lance
        else:
            ref_l = l2_ed if u2b and l2_ed > 0 else l1_ed
            def_lance = float(ref_l or 0) or _defaults_lance_row(row)

        lk, t2k = _sk("lance"), _sk("lance_2a")
        # Sem 1ª/2ª praça: a hidratação pôs frequentemente lance=0; alinhar ao valor de referência (arrematação / extra).
        if l1_ed <= 0 and l2_ed <= 0 and def_lance > 0 and salvo_lance <= 0:
            try:
                _l_cur = float(st.session_state.get(lk) or 0)
            except (TypeError, ValueError):
                _l_cur = 0.0
            if _l_cur <= 0:
                st.session_state[lk] = float(def_lance)
        dsk = simop_key(iid, "vista", "descav")
        dsk_pct = simop_key(iid, "vista", "descav_pct")
        pr_e, pr_n, pr_jm = (
            simop_key(iid, "prazo", "pr_ent"),
            simop_key(iid, "prazo", "pr_n"),
            simop_key(iid, "prazo", "pr_jm"),
        )
        fn_e, fn_n, fn_tx, fn_sys = (
            simop_key(iid, "financiado", "fin_ent"),
            simop_key(iid, "financiado", "fin_n"),
            simop_key(iid, "financiado", "fin_tx"),
            simop_key(iid, "financiado", "fin_sys"),
        )
        k_nicho = simop_key_ui_nicho_prazo_fin(iid)
        if k_nicho not in st.session_state:
            st.session_state[k_nicho] = True
        tvk = simop_key_tempo_venda_global(iid)
        simop_ensure_tempo_venda_global(iid)

        def _on_praca_toggle() -> None:
            u2 = bool(st.session_state.get(t2k, False))
            nv = l2_ed if u2 else l1_ed
            if nv > 0:
                st.session_state[lk] = nv

        ref_ui_keys = [a for a, _ in _SIMOP_REFUI_SPECS]
        ref_ui_labels = [b for _, b in _SIMOP_REFUI_SPECS]
        rk0, rm0 = _reforma_ui_defaults(inp0_tag)
        try:
            ref_ui_ix = ref_ui_keys.index(rk0)
        except ValueError:
            ref_ui_ix = 0

        cache_sel = _simop_auto_cache_id(caches, inp0_tag)
        vmanual_preview = float(
            st.session_state.get(_sk("vmanual"), inp0_tag.valor_venda_manual or 0)
        )
        label_map = _simop_labels_venda_com_valores(
            row=row,
            caches=caches,
            ads_map=ads_map,
            inp0=inp0_tag,
            vmanual_preview=vmanual_preview,
        )
        modo_order = [
            ModoValorVenda.CACHE_PRECO_M2_X_AREA.value,
            ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value,
            ModoValorVenda.CACHE_MENOR_VALOR_VENDA.value,
            ModoValorVenda.ANUNCIOS_VALOR_MEDIO.value,
            ModoValorVenda.ANUNCIOS_MENOR_VALOR.value,
            ModoValorVenda.ANUNCIOS_PRECO_M2_X_AREA.value,
            ModoValorVenda.MANUAL.value,
        ]
        try:
            mi = modo_order.index(
                str(getattr(inp0_tag.modo_valor_venda, "value", inp0_tag.modo_valor_venda))
            )
        except ValueError:
            mi = 0
        st.number_input(
            "Tempo estimado até a venda (meses) — comum a todas as modalidades",
            min_value=0.5,
            max_value=360.0,
            step=0.5,
            format="%.1f",
            key=tvk,
            help="Mesmo T para **à vista**, **parcelado** e **financiado**: afeta fluxo, saldo, juros, ROI e anualização. "
            "A comparação de painéis (quando ativa) e cada modalidade usam este valor.",
        )
        st.toggle(
            "Mostrar opções de parcelado (judicial) e de financiamento (bancário)",
            key=k_nicho,
            help="Exibe entradas %, parcelas, juros, prazo, taxa a.a. e sistema (SAC/PRICE). Desligar só oculta; os valores seguem no cálculo em sessão e ao gravar.",
        )
        if st.session_state.get(k_nicho, True):
            st.markdown(
                '<div class="sim-card-head">Opções (parcelado judicial)</div>', unsafe_allow_html=True
            )
            rpc = st.columns(3)
            with rpc[0]:
                st.number_input(
                    "Entrada s/ lance (%)",
                    min_value=0.0,
                    max_value=95.0,
                    step=0.5,
                    format="%.2f",
                    key=pr_e,
                )
            with rpc[1]:
                st.number_input(
                    "Nº de parcelas",
                    min_value=1,
                    max_value=60,
                    step=1,
                    key=pr_n,
                )
            with rpc[2]:
                st.number_input(
                    "Juros % ao mês (s/ saldo)",
                    min_value=0.0,
                    max_value=5.0,
                    step=0.05,
                    format="%.2f",
                    key=pr_jm,
                    help="Referência: editais costumam usar ~1% a.m. em alguns casos; PGFN/CPC: IPCA-E + 1% no mês do pag. em execução fiscal (consulte o edital).",
                )
            st.markdown(
                '<div class="sim-card-head">Opções (financiamento bancário)</div>',
                unsafe_allow_html=True,
            )
            rfc = st.columns(4)
            with rfc[0]:
                st.number_input(
                    "Entrada (%)",
                    min_value=5.0,
                    max_value=50.0,
                    step=0.5,
                    format="%.2f",
                    key=fn_e,
                )
            with rfc[1]:
                st.number_input(
                    "Prazo (meses)",
                    min_value=12,
                    max_value=480,
                    step=12,
                    key=fn_n,
                )
            with rfc[2]:
                st.number_input(
                    "Juros a.a. (%)",
                    min_value=0.0,
                    max_value=20.0,
                    step=0.1,
                    format="%.2f",
                    key=fn_tx,
                )
            with rfc[3]:
                st.segmented_control(
                    "Sistema",
                    options=["SAC", "PRICE"],
                    key=fn_sys,
                )

        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Valor de venda estimado</div>',
                unsafe_allow_html=True,
            )
            # Streamlit: session com membro Enum + options=list[str] → coerção falha (Enum vs str).
            _mk_mv = _sk("modo_val")
            if _mk_mv in st.session_state and isinstance(st.session_state[_mk_mv], enum.Enum):
                st.session_state[_mk_mv] = st.session_state[_mk_mv].value
            if _mk_mv in st.session_state:
                _mv_ok = _normalizar_selecao_modo_venda(
                    st.session_state[_mk_mv], modo_order, label_map
                )
                if _mv_ok != st.session_state[_mk_mv]:
                    st.session_state[_mk_mv] = _mv_ok
            modo_sel = st.selectbox(
                "Fonte da estimativa",
                options=modo_order,
                index=min(mi, len(modo_order) - 1),
                format_func=lambda k: label_map.get(k, k),
                key=_mk_mv,
                help="Montantes calculados na hora. O cache do bairro é escolhido automaticamente pelo registro.",
            )
            modo_val_str = _normalizar_selecao_modo_venda(modo_sel, modo_order, label_map)
            if modo_val_str != modo_sel:
                st.session_state[_mk_mv] = modo_val_str
            modo = ModoValorVenda(modo_val_str)
            if modo == ModoValorVenda.MANUAL:
                st.number_input(
                    "Valor manual da venda (R$)",
                    min_value=0.0,
                    value=float(inp0_tag.valor_venda_manual or 0),
                    step=25_000.0,
                    key=_sk("vmanual"),
                )

        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Imposto de renda</div>',
                unsafe_allow_html=True,
            )
            ir_top = st.columns([1.15, 1.85], gap="small")
            with ir_top[0]:
                tipo_raw = st.segmented_control(
                    "Pessoa",
                    options=["PF", "PJ"],
                    default="PJ" if inp0_tag.tipo_pessoa == "PJ" else "PF",
                    key=_sk("tipo"),
                )
            tipo = _seg_sim_single(tipo_raw, "PF")
            with ir_top[1]:
                if tipo == "PF":
                    st.number_input(
                        "Alíquota IR (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(inp0_tag.ir_aliquota_pf_pct),
                        step=0.5,
                        format="%.2f",
                        key=_sk("ir_pf"),
                    )
                else:
                    st.number_input(
                        "Alíquota IR (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(inp0_tag.ir_aliquota_pj_pct),
                        step=0.1,
                        format="%.2f",
                        key=_sk("ir_pj"),
                    )

        ir_pf_pct = float(st.session_state.get(_sk("ir_pf"), inp0_tag.ir_aliquota_pf_pct))
        ir_pj_pct = float(st.session_state.get(_sk("ir_pj"), inp0_tag.ir_aliquota_pj_pct))

        prov_venda = inp0_tag.model_copy(
            update={
                "tipo_pessoa": "PJ" if tipo == "PJ" else "PF",
                "modo_valor_venda": modo,
                "valor_venda_manual": (
                    float(st.session_state.get(_sk("vmanual"), inp0_tag.valor_venda_manual or 0))
                    if modo == ModoValorVenda.MANUAL
                    else None
                ),
                "cache_media_bairro_id": cache_sel,
            }
        )
        venda_prev, _vmeta = resolver_valor_venda_estimado(
            row_leilao=row,
            inp=prov_venda,
            caches_ordenados=caches,
            ads_por_id=ads_map,
        )

        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Arrematação</div>',
                unsafe_allow_html=True,
            )
            col_sw, col_ref = st.columns([1.15, 2.85], gap="small")
            with col_sw:
                st.toggle(
                    "2ª praça",
                    key=t2k,
                    help="Desligado = referência da 1ª praça no edital. Ligado = referência da 2ª praça. "
                    "O lance em R$ é preenchido com o valor da praça quando existir no edital.",
                    on_change=_on_praca_toggle,
                )
            with col_ref:
                bits: list[str] = []
                if l1_ed > 0:
                    bits.append(
                        f'1ª <strong>{html.escape(_fmt_valor_campo("valor_venda", l1_ed))}</strong>'
                    )
                if l2_ed > 0:
                    bits.append(
                        f'2ª <strong>{html.escape(_fmt_valor_campo("valor_venda", l2_ed))}</strong>'
                    )
                if not bits:
                    v_ref = _defaults_lance_row(row)
                    if v_ref > 0:
                        bits.append(
                            f'<strong>{html.escape(_fmt_valor_campo("valor_venda", v_ref))}</strong>'
                        )
                if bits:
                    _tit_ref = (
                        "Lances no edital"
                        if (l1_ed > 0 or l2_ed > 0)
                        else "Referência do edital"
                    )
                    st.markdown(
                        f'<p class="sim-praca-ref">{_tit_ref}: ' + " · ".join(bits) + "</p>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("Sem valores de lance no edital — informe o lance abaixo.")
            st.number_input(
                "Lance (R$)",
                min_value=0.0,
                value=float(st.session_state.get(lk, def_lance)),
                step=5_000.0,
                key=lk,
            )
            r_av = st.columns([1.1, 1.9], gap="small")
            with r_av[0]:
                st.toggle(
                    "Desconto à vista",
                    key=dsk,
                    help="Reduz o caixa pago do lance. Comissão do leiloeiro e % de ITBI/registro permanecem sobre o lance nominal (cheio). Só aplica na modalidade **à vista**.",
                    disabled=False,
                )
            with r_av[1]:
                st.number_input(
                    "Desconto s/ lance (%)",
                    min_value=0.0,
                    max_value=99.0,
                    step=0.5,
                    format="%.2f",
                    key=dsk_pct,
                    disabled=(not bool(st.session_state.get(dsk, False))),
                    help="Típico em leilões com incentivo a pagamento único (ex.: 10%).",
                )

        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Tributos</div>',
                unsafe_allow_html=True,
            )
            r2a = st.columns(3, gap="small")
            with r2a[0]:
                st.number_input(
                    "Leiloeiro %",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(inp0_tag.comissao_leiloeiro_pct_sobre_arrematacao),
                    step=0.25,
                    format="%.2f",
                    key=_sk("cleipct"),
                )
            with r2a[1]:
                st.number_input(
                    "ITBI %",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(inp0_tag.itbi_pct_sobre_arrematacao),
                    step=0.25,
                    format="%.2f",
                    key=_sk("itbipct"),
                )
            legacy_reg_brl = float(inp0_tag.registro_brl or 0) > 0
            with r2a[2]:
                if legacy_reg_brl:
                    st.number_input(
                        "Registro R$",
                        min_value=0.0,
                        value=float(inp0_tag.registro_brl),
                        step=250.0,
                        key=_sk("regfix"),
                        help="Legado: gravar de novo usa %.",
                    )
                else:
                    st.number_input(
                        "Registro %",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(inp0_tag.registro_pct_sobre_arrematacao or 3.5),
                        step=0.1,
                        format="%.2f",
                        key=_sk("regpct"),
                    )
            lance = float(st.session_state.get(lk, def_lance))
            clei_pct = float(
                st.session_state.get(_sk("cleipct"), inp0_tag.comissao_leiloeiro_pct_sobre_arrematacao)
            )
            itbi_pct = float(st.session_state.get(_sk("itbipct"), inp0_tag.itbi_pct_sobre_arrematacao))
            if legacy_reg_brl:
                reg_brl_inp = float(st.session_state.get(_sk("regfix"), inp0_tag.registro_brl))
                reg_pct = float(inp0_tag.registro_pct_sobre_arrematacao or 0)
            else:
                reg_brl_inp = 0.0
                reg_pct = float(st.session_state.get(_sk("regpct"), inp0_tag.registro_pct_sobre_arrematacao or 3.5))
            clei_pv = _preview_brl_leiloeiro(lance, clei_pct, 0.0)
            itbi_pv = _preview_brl_itbi(lance, itbi_pct, 0.0)
            reg_pv = _preview_brl_itbi(lance, reg_pct, reg_brl_inp)
            st.markdown(
                '<div class="sim-kpi-strip">'
                f'<span>Leiloeiro <strong>{html.escape(_fmt_valor_campo("valor_venda", clei_pv))}</strong></span>'
                f'<span class="sim-kpi-dot">·</span>'
                f'<span>ITBI <strong>{html.escape(_fmt_valor_campo("valor_venda", itbi_pv))}</strong></span>'
                f'<span class="sim-kpi-dot">·</span>'
                f'<span>Registro <strong>{html.escape(_fmt_valor_campo("valor_venda", reg_pv))}</strong></span>'
                "</div>",
                unsafe_allow_html=True,
            )

        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Reforma e débitos do imóvel</div>',
                unsafe_allow_html=True,
            )
            r3 = st.columns(4, gap="small")
            with r3[0]:
                cond = st.number_input(
                    "Condomínio (R$)",
                    min_value=0.0,
                    value=float(inp0_tag.condominio_atrasado_brl),
                    step=250.0,
                    key=_sk("cond"),
                )
            with r3[1]:
                iptu = st.number_input(
                    "IPTU (R$)",
                    min_value=0.0,
                    value=float(inp0_tag.iptu_atrasado_brl),
                    step=250.0,
                    key=_sk("iptu"),
                )
            with r3[2]:
                desoc = st.number_input(
                    "Desocupação (R$)",
                    min_value=0.0,
                    value=float(inp0_tag.desocupacao_brl),
                    step=250.0,
                    key=_sk("des"),
                )
            with r3[3]:
                outros = st.number_input(
                    "Outros (R$)",
                    min_value=0.0,
                    value=float(inp0_tag.outros_custos_brl),
                    step=250.0,
                    key=_sk("out"),
                )
            ref_pick = st.selectbox(
                "Reforma estimada",
                options=ref_ui_labels,
                index=ref_ui_ix,
                key=_sk("refui_lbl"),
            )
            ref_ui_key = ref_ui_keys[ref_ui_labels.index(ref_pick)]
            ref_manual_val = 0.0
            if ref_ui_key == "manual":
                ref_manual_val = float(
                    st.number_input(
                        "R$ reforma (livre)",
                        min_value=0.0,
                        value=float(rm0 or inp0_tag.reforma_brl or 0),
                        step=5_000.0,
                        key=_sk("refmanual"),
                    )
                )
            ref_mod, reforma_brl_inp = _reforma_modo_valor_de_ui(ref_ui_key, ref_manual_val)
            area_sim = _area_m2_row_sim(row)
            ref_pv_brl, ref_pv_tag = _preview_brl_reforma(area_sim, ref_mod, ref_manual_val)
            st.caption(
                f"Prévia reforma: {_fmt_valor_campo('valor_venda', ref_pv_brl)} ({ref_pv_tag})"
                + (f" · {area_sim:.0f} m²" if area_sim > 0 else "")
            )

        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Corretagem (saída)</div>',
                unsafe_allow_html=True,
            )
            rr = st.columns([1, 1, 1.4], gap="small")
            with rr[0]:
                st.number_input(
                    "Fixo (R$)",
                    min_value=0.0,
                    value=float(inp0_tag.comissao_imobiliaria_brl),
                    step=500.0,
                    help="Se > 0, ignora o percentual.",
                    key=_sk("cimob"),
                )
            with rr[1]:
                st.number_input(
                    "% s/ venda",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(inp0_tag.comissao_imobiliaria_pct_sobre_venda),
                    step=0.1,
                    format="%.2f",
                    key=_sk("cimobpct"),
                )
            with rr[2]:
                cimob_brl = float(st.session_state.get(_sk("cimob"), inp0_tag.comissao_imobiliaria_brl))
                cimob_pct = float(
                    st.session_state.get(_sk("cimobpct"), inp0_tag.comissao_imobiliaria_pct_sobre_venda)
                )
                cim_pv = _preview_brl_corretagem(float(venda_prev), cimob_pct, cimob_brl)
                cim_hint = "fixo" if cimob_brl > 0 else (f"{cimob_pct:.2f} %" if cimob_pct > 0 else "—")
                st.markdown(
                    f'<div class="sim-kpi-strip">Estimativa venda <strong>{html.escape(_fmt_valor_campo("valor_venda", float(venda_prev)))}</strong>'
                    f' · Corretagem <strong>{html.escape(_fmt_valor_campo("valor_venda", cim_pv))}</strong> <span class="sim-kpi-muted">({html.escape(cim_hint)})</span></div>',
                    unsafe_allow_html=True,
                )

        _roi_ui_default = float(inp0_tag.roi_desejado_pct or 0) or 50.0
        with st.container(border=True):
            st.markdown(
                '<div class="sim-card-head">Sensibilidade (lance máximo)</div>',
                unsafe_allow_html=True,
            )
            r4 = st.columns([1.1, 1.2], gap="small")
            with r4[0]:
                st.number_input(
                    "ROI desejado %",
                    min_value=0.0,
                    max_value=200.0,
                    value=_roi_ui_default,
                    step=1.0,
                    format="%.2f",
                    help="0% desliga o cálculo de lance máximo (bissecção).",
                    key=_sk("roi_w"),
                )
            roi_seg_ix = 1 if inp0_tag.roi_desejado_modo == ModoRoiDesejado.LIQUIDO else 0
            with r4[1]:
                st.segmented_control(
                    "Base do ROI",
                    options=["Bruto", "Líquido"],
                    default=["Bruto", "Líquido"][roi_seg_ix],
                    key=_sk("roi_seg"),
                )

        v_manual_st = float(st.session_state.get(_sk("vmanual"), inp0_tag.valor_venda_manual or 0))
        ref_mod2, ref_brl2 = _ref_mod_brl_da_sessao(
            iid,
            "vista",
            ref_ui_labels,
            ref_ui_keys,
            float(inp0_tag.reforma_brl or 0.0),
            inp0_tag,
        )
        inp = construir_inputs_de_sessao(
            iid=iid,
            tag="vista",
            inp0=inp0_tag,
            modo_valor=modo,
            v_manual_st=v_manual_st,
            def_lance=def_lance,
            ref_mod=ref_mod2,
            reforma_brl_inp=ref_brl2,
            cache_sel=cache_sel,
        )

        doc = calcular_simulacao(row_leilao=row, inp=inp, caches_ordenados=caches, ads_por_id=ads_map)
        o = doc.outputs
        try:
            st.session_state["_sim_report_doc_json"] = doc.model_dump(mode="json")
            st.session_state["_sim_report_doc_iid"] = iid
        except Exception:
            logger.debug("Snapshot relatório simulação", exc_info=True)
        with st.container(border=True):
            st.markdown('<div class="sim-card-head">Persistência</div>', unsafe_allow_html=True)
            _tip_btn_persist = "primary" if _row_tem_simulacao_gravada(row) else "secondary"
            ab1, ab2, ab3 = st.columns(3, gap="small")
            with ab1:
                if st.button(
                    "Gravar simulação (à vista)",
                    type=_tip_btn_persist,
                    use_container_width=True,
                    key=_sk("save"),
                ):
                    try:
                        cli = get_supabase_client()
                        pay_doc = doc.model_dump(mode="json")
                        b_merge = parse_simulacoes_modalidades_json(
                            row.get("simulacoes_modalidades_json"),
                            legado_operacao=row.get("operacao_simulacao_json"),
                        )
                        b_new = b_merge.model_copy(update={"vista": doc})
                        leilao_imoveis_repo.atualizar_operacao_e_modalidades(
                            iid, pay_doc, b_new.model_dump(mode="json"), cli
                        )
                        atual = leilao_imoveis_repo.buscar_por_id(iid, cli)
                        if isinstance(atual, dict):
                            st.session_state["ultimo_extracao"] = atual
                        st.success("Simulação **à vista** e slot do bundle gravados (`operacao_simulacao_json` + `simulacoes_modalidades_json`).")
                    except Exception as e:
                        logger.exception("Gravar operacao_simulacao_json")
                        st.error(
                            f"Não foi possível gravar. Erro: {e}. "
                            "Execute `leilao_ia_v2/sql/008_operacao_simulacao_json.sql` (legado) e "
                            "`leilao_ia_v2/sql/011_simulacoes_modalidades_json.sql` (bundle) se a coluna não existir."
                        )
            with ab2:
                if st.button(
                    "Gravar as 3 modalidades",
                    type=_tip_btn_persist,
                    use_container_width=True,
                    key=f"simop_save3_{(iid or '')[:12]}",
                ):
                    try:
                        cli = get_supabase_client()
                        docs3: list[tuple[str, OperacaoSimulacaoDocumento]] = []
                        for t in TAGS:
                            inp3, _it3 = _construir_inp_por_tag(iid, row, t, caches)
                            docs3.append(
                                (t, calcular_simulacao(row_leilao=row, inp=inp3, caches_ordenados=caches, ads_por_id=ads_map))
                            )
                        b3 = SimulacoesModalidadesBundle(
                            vista=docs3[0][1],
                            prazo=docs3[1][1],
                            financiado=docs3[2][1],
                        )
                        doc0 = b3.vista
                        leilao_imoveis_repo.atualizar_operacao_e_modalidades(
                            iid, doc0.model_dump(mode="json"), b3.model_dump(mode="json"), cli
                        )
                        atual = leilao_imoveis_repo.buscar_por_id(iid, cli)
                        if isinstance(atual, dict):
                            st.session_state["ultimo_extracao"] = atual
                        st.success("Três simulações gravadas; legado = **à vista** (como o painel principal).")
                    except Exception as e:
                        logger.exception("Gravar 3 simulacoes")
                        st.error(
                            f"Falha ao gravar o bundle. Erro: {e}. "
                            "Aplique `leilao_ia_v2/sql/011_simulacoes_modalidades_json.sql` no Supabase se faltar a coluna."
                        )
            with ab3:
                st.caption(
                    "Gravar **(à vista)**: atualiza o relatório/legado e o slot **vista** do bundle. "
                    "**Gravar as 3** recalcula e salva vista + parcelado + financiado; o legado segue a simulação **à vista**. "
                    "Lucro bruto = venda − custos − corretagem; com desconto à vista, subtotal com **lance pago**; ITBI comiss. no nominal."
                )

        o_cmp: SimulacaoOperacaoOutputs | None = None
        _tit_cmp: str | None = None
        if results_column is None:
            _cmp_opcoes: tuple[str, ...] = ("nenhum", "prazo", "financiado")
            _cmp_labels: dict[str, str] = {
                "nenhum": "Não comparar (só à vista)",
                "prazo": "Comparar com parcelado (judicial)",
                "financiado": "Comparar com financiado (bancário)",
            }
            st.radio(
                "Comparar com outra modalidade (painel adicional)",
                options=list(_cmp_opcoes),
                key=k_cmp_painel,
                format_func=lambda k: _cmp_labels.get(k, k),
                horizontal=True,
            )
            _cmp_sel = str(st.session_state.get(k_cmp_painel) or "nenhum")
            if _cmp_sel == "prazo":
                _inp2 = construir_inputs_de_sessao(
                    iid=iid,
                    tag="prazo",
                    inp0=inp0_tag,
                    modo_valor=modo,
                    v_manual_st=v_manual_st,
                    def_lance=def_lance,
                    ref_mod=ref_mod2,
                    reforma_brl_inp=ref_brl2,
                    cache_sel=cache_sel,
                )
                _d2 = calcular_simulacao(
                    row_leilao=row, inp=_inp2, caches_ordenados=caches, ads_por_id=ads_map
                )
                o_cmp = _d2.outputs
                _tit_cmp = "Parcelado (judicial) — comparação"
            elif _cmp_sel == "financiado":
                _inp2 = construir_inputs_de_sessao(
                    iid=iid,
                    tag="financiado",
                    inp0=inp0_tag,
                    modo_valor=modo,
                    v_manual_st=v_manual_st,
                    def_lance=def_lance,
                    ref_mod=ref_mod2,
                    reforma_brl_inp=ref_brl2,
                    cache_sel=cache_sel,
                )
                _d2 = calcular_simulacao(
                    row_leilao=row, inp=_inp2, caches_ordenados=caches, ads_por_id=ads_map
                )
                o_cmp = _d2.outputs
                _tit_cmp = "Financiado (bancário) — comparação"
            if o_cmp is not None:
                c_l, c_r = st.columns(2, gap="large")
                with c_l:
                    _pinta_saida_sim(o, titulo="À vista — painel principal")
                with c_r:
                    _pinta_saida_sim(o_cmp, titulo=_tit_cmp or "Comparação")
                _n1 = o.lance_maximo_roi_notas or []
                _n2 = o_cmp.lance_maximo_roi_notas or []
                for n in {*(_n1 + _n2)}:
                    st.caption(n)
                _a1 = o.notas or []
                _a2 = o_cmp.notas or []
                for n in {*(_a1 + _a2)}:
                    st.caption(n)
            else:
                _pinta_saida_sim(o, titulo="À vista")
                if o and o.lance_maximo_roi_notas:
                    for n in o.lance_maximo_roi_notas:
                        st.caption(n)
                if o and o.notas:
                    for n in o.notas:
                        st.caption(n)
            # Cache para o «Gerar relatório HTML»: mesmos resultados e seleção de comparação que a tela.
            st.session_state[f"_rpt_painel_cmp|{iid}"] = {
                "sel": str(st.session_state.get(k_cmp_painel) or "nenhum"),
                "out": o_cmp.model_dump(mode="json") if o_cmp is not None else None,
            }
            _render_analise_mercado_abaixo_painel(row)

    if results_column is not None:
        with results_column:
            _pinta_saida_sim(o, titulo="À vista")
            _render_analise_mercado_abaixo_painel(row)
        st.session_state[f"_rpt_painel_cmp|{iid}"] = {"sel": "nenhum", "out": None}


def _render_mapa_folium_row(
    row: dict[str, Any],
    *,
    comparaveis: list[dict[str, Any]] | None = None,
) -> None:
    """
    Mapa interativo: leilão (vermelho) e comparáveis do(s) cache(s).

    Quando cada comparável traz ``_mapa_color`` / ``_mapa_fill_color`` (via
    ``_build_comparaveis_mapa_por_cache``), as cores diferenciam o cache de origem
    e uma legenda é exibida; caso contrário, todos os pontos usam o azul padrão.
    """
    try:
        import folium
        from folium.plugins import MarkerCluster
        from streamlit_folium import st_folium
    except ImportError:
        st.warning(
            "Instale as dependências do mapa: `pip install folium streamlit-folium` "
            "(já listadas em `leilao_ia_v2/requirements.txt`)."
        )
        return

    comps = list(comparaveis or [])
    la: float | None = None
    lo: float | None = None
    lat0, lon0 = row.get("latitude"), row.get("longitude")
    if lat0 is not None and lon0 is not None:
        try:
            la = float(lat0)
            lo = float(lon0)
        except (TypeError, ValueError):
            la = lo = None

    default_stroke, default_fill = _MAPA_CORES_CACHE[0]
    comp_coords: list[tuple[float, float, str, str, str, str]] = []
    for a in comps:
        alat, alon = a.get("latitude"), a.get("longitude")
        if alat is None or alon is None:
            continue
        try:
            fa, fo = float(alat), float(alon)
        except (TypeError, ValueError):
            continue
        try:
            am = float(a.get("area_construida_m2") or 0)
            vv = float(a.get("valor_venda") or 0)
        except (TypeError, ValueError):
            am = vv = 0.0
        m2s = _fmt_valor_campo("area_util", am) if am > 0 else "m² —"
        vs = _fmt_valor_campo("valor_venda", vv) if vv > 0 else "valor —"
        cn = str(a.get("_mapa_cache_nome") or "").strip()
        tip = f"{m2s} · {vs}" + (f" · {cn}" if cn else "")
        url = str(a.get("url_anuncio") or "").strip()
        stroke = str(a.get("_mapa_color") or "").strip() or default_stroke
        fill = str(a.get("_mapa_fill_color") or "").strip() or default_fill
        comp_coords.append((fa, fo, tip, url, stroke, fill))

    if la is None and lo is None and not comp_coords:
        st.info("Não há coordenadas geográficas para este registro nem para os comparáveis — mapa não exibido.")
        return

    cidade = str(row.get("cidade") or "").strip()
    bairro = str(row.get("bairro") or "").strip()
    titulo_txt = "Leilão"
    if cidade or bairro:
        titulo_txt = f"Leilão · {cidade}" + (f" / {bairro}" if bairro else "")
    url_lei = str(row.get("url_leilao") or "").strip()
    titulo_esc = html.escape(titulo_txt)
    if url_lei:
        uq = html.escape(url_lei, quote=True)
        popup_html_lei = (
            f"<span>{titulo_esc}</span><br>"
            f'<a href="{uq}" target="_blank" rel="noopener noreferrer">Link leilão</a>'
        )
    else:
        popup_html_lei = titulo_esc

    tooltip_lei = str(row.get("endereco") or titulo_txt)[:240]

    if la is not None and lo is not None:
        center = [la, lo]
        zoom0 = 15
    else:
        center = [comp_coords[0][0], comp_coords[0][1]]
        zoom0 = 14

    m = folium.Map(location=center, zoom_start=zoom0, control_scale=True, tiles="OpenStreetMap")

    if any(a.get("_mapa_cache_index") is not None for a in comps):
        _render_legenda_cores_mapa_caches(comps)

    if la is not None and lo is not None:
        folium.Marker(
            location=[la, lo],
            popup=folium.Popup(popup_html_lei, max_width=320),
            tooltip=folium.Tooltip(html.escape(tooltip_lei), sticky=True),
            icon=folium.Icon(color="red"),
        ).add_to(m)

    # Comparáveis: cluster + spiderfy (Leaflet.markercluster) para separar pontos sobrepostos ao dar zoom / clicar.
    cluster_alvo: folium.map.Layer | folium.map.Map = m
    if comp_coords:
        cluster_alvo = MarkerCluster(
            name="Comparáveis (cache)",
            overlay=True,
            control=True,
            spiderfyOnMaxZoom=True,
            showCoverageOnHover=False,
            zoomToBoundsOnClick=True,
            maxClusterRadius=50,
            spiderfyDistanceMultiplier=1.35,
            removeOutsideVisibleBounds=True,
        ).add_to(m)

    for fa, fo, tip, url, stroke, fill in comp_coords:
        if url:
            uqa = html.escape(url, quote=True)
            pop = folium.Popup(
                f'<a href="{uqa}" target="_blank" rel="noopener noreferrer">Abrir anúncio</a>',
                max_width=300,
            )
        else:
            pop = None
        folium.CircleMarker(
            location=[fa, fo],
            radius=8,
            color=stroke,
            weight=2,
            fill=True,
            fill_color=fill,
            fill_opacity=0.82,
            popup=pop,
            tooltip=folium.Tooltip(html.escape(tip), sticky=True),
        ).add_to(cluster_alvo)

    all_lats: list[float] = []
    all_lons: list[float] = []
    if la is not None and lo is not None:
        all_lats.append(la)
        all_lons.append(lo)
    for fa, fo, _, _, _, _ in comp_coords:
        all_lats.append(fa)
        all_lons.append(fo)
    if len(all_lats) >= 2:
        m.fit_bounds(
            [[min(all_lats), min(all_lons)], [max(all_lats), max(all_lons)]],
            padding=(28, 28),
            max_zoom=17,
        )

    rid = str(row.get("id") or url_lei or "map")
    map_key = f"ingestao_folium_{rid}"[:120]
    st_folium(
        m,
        width=None,
        height=440,
        use_container_width=True,
        key=map_key,
        returned_objects=[],
    )
    sem_geo = sum(1 for a in comps if a.get("latitude") is None or a.get("longitude") is None)
    if sem_geo and comps:
        st.caption(
            f"{sem_geo} comparável(is) sem latitude/longitude no banco — listados à direita, mas fora do mapa."
        )
    if len(comp_coords) >= 2:
        st.caption(
            "Comparáveis: cores por cache (legenda acima). Ao aproximar o zoom, agrupamentos podem abrir em "
            "**aranha (spiderfy)** ou use **clique no número** do agrupamento para separar anúncios no mesmo ponto."
        )


_TODOS_FILTRO = "Todos"


def _as_date_only(v: Any) -> date | None:
    """Converte valor do banco/JSON em ``date`` (só dia)."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    s10 = s[:10]
    try:
        return date.fromisoformat(s10)
    except ValueError:
        return None


def _fmt_data_br(d: date | None) -> str:
    if d is None:
        return "—"
    return f"{d.day:02d}/{d.month:02d}/{d.year}"


def _row_resumo_leilao_para_tabela_exibicao(r: dict[str, Any]) -> dict[str, Any]:
    """Uma linha de resumo para a tabela de leitura dos leilões."""
    pd_ = _proxima_data_leilao_row(r)
    _end = (str(r.get("endereco") or "").strip() or "—")[:220]
    _url = (str(r.get("url_leilao") or "").strip() or "—")
    return {
        "id": str(r.get("id") or ""),
        "Próxima": _fmt_data_br(pd_),
        "1ª praça": _fmt_data_br(_as_date_only(r.get("data_leilao_1_praca"))),
        "2ª praça": _fmt_data_br(_as_date_only(r.get("data_leilao_2_praca"))),
        "Lance / ref.": _fmt_valor_referencia_edital_resumo(r),
        "UF": str(r.get("estado") or "").strip()[:2] or "—",
        "Cidade": str(r.get("cidade") or "") or "—",
        "Bairro": str(r.get("bairro") or "") or "—",
        "Tipo": str(r.get("tipo_imovel") or "") or "—",
        "Endereço": _end,
        "URL": _url,
    }


def _tabela_celula_vazia(s: Any) -> bool:
    t = str(s or "").strip()
    return t in ("", "—", "–", "-")


def _parse_data_br_celula_tabela(s: Any) -> date | None:
    """Célula de edição: dd/mm/aaaa, ISO, ou vazio/— → None."""
    if _tabela_celula_vazia(s):
        return None
    t = str(s).strip()
    s10 = t[:10]
    if "/" in t:
        partes = t.replace(" ", "").split("/")
        if len(partes) == 3:
            try:
                d_, m_, y_ = int(partes[0]), int(partes[1]), int(partes[2])
                return date(y_, m_, d_)
            except (ValueError, OSError, TypeError, OverflowError):
                return None
    try:
        return date.fromisoformat(s10)
    except ValueError:
        return None


def _texto_tabela_para_db(s: Any) -> str:
    if _tabela_celula_vazia(s):
        return ""
    return str(s).strip()


def _montar_patch_leilao_tabela_row(
    r0: dict[str, Any], ed: "pd.Series[Any]"
) -> tuple[dict[str, Any], str | None]:
    """Converte linha editada (rótulos PT) em campos do Supabase. (patch, erro); patch vazio = nada a gravar."""
    patch: dict[str, Any] = {}
    o_uf = str(r0.get("estado") or "").strip().upper()[:2]
    raw_uf = str(ed.get("UF") or "").strip().upper().replace(" ", "")
    n_uf = raw_uf[:2] if len(raw_uf) >= 2 else raw_uf
    if raw_uf:
        if len(n_uf) != 2 or not n_uf.isalpha():
            return {}, "UF: use 2 letras (ex.: SP)."
        if n_uf != o_uf:
            patch["estado"] = n_uf

    for col_key, k_db in (
        ("Cidade", "cidade"),
        ("Bairro", "bairro"),
        ("Tipo", "tipo_imovel"),
    ):
        n_ = _texto_tabela_para_db(ed.get(col_key))
        o_ = str(r0.get(k_db) or "").strip()
        if n_ != o_:
            patch[k_db] = n_

    n_end = _texto_tabela_para_db(ed.get("Endereço"))
    o_end = str(r0.get("endereco") or "").strip()
    if n_end != o_end:
        patch["endereco"] = n_end

    n_url_raw = _texto_tabela_para_db(ed.get("URL"))
    o_url = str(r0.get("url_leilao") or "").strip()
    if n_url_raw != o_url:
        if not n_url_raw:
            return {}, "URL do leilão não pode ser vazia — reverta o valor original."
        try:
            patch["url_leilao"] = normalizar_url_leilao(n_url_raw)
        except Exception as e:
            return {}, f"URL inválida: {e}"

    d1o = _as_date_only(r0.get("data_leilao_1_praca"))
    d1e = _parse_data_br_celula_tabela(ed.get("1ª praça"))
    if ed.get("1ª praça") is not None and not _tabela_celula_vazia(ed.get("1ª praça")) and d1e is None:
        return {}, "Data 1ª praça: use dd/mm/aaaa ou deixe vazio (—)."
    d1e_norm = d1e.isoformat() if d1e else None
    d1o_norm = d1o.isoformat() if d1o else None
    if d1e_norm != d1o_norm:
        patch["data_leilao_1_praca"] = d1e_norm

    d2o = _as_date_only(r0.get("data_leilao_2_praca"))
    d2e = _parse_data_br_celula_tabela(ed.get("2ª praça"))
    if ed.get("2ª praça") is not None and not _tabela_celula_vazia(ed.get("2ª praça")) and d2e is None:
        return {}, "Data 2ª praça: use dd/mm/aaaa ou deixe vazio (—)."
    d2e_norm = d2e.isoformat() if d2e else None
    d2o_norm = d2o.isoformat() if d2o else None
    if d2e_norm != d2o_norm:
        patch["data_leilao_2_praca"] = d2e_norm

    return patch, None


def _candidatas_datas_leilao_row(r: dict[str, Any]) -> list[date]:
    """1ª/2ª praça e, se houver, ``data_leilao`` (genérico — ex. Caixa / venda direta com data única)."""
    out: list[date] = []
    for k in ("data_leilao_1_praca", "data_leilao_2_praca", "data_leilao"):
        d = _as_date_only(r.get(k))
        if d is not None:
            out.append(d)
    return out


def _modalidade_venda_da_row(r: dict[str, Any]) -> str | None:
    ex = r.get("leilao_extra_json")
    if isinstance(ex, dict):
        m = ex.get("modalidade_venda")
        if m in ("venda_direta", "leilao"):
            return str(m)
    return None


def _incluir_em_faixa_por_venda_direta_sem_praças(
    r: dict[str, Any], faixa: str
) -> bool:
    """
    Compra / venda direta (ex. Caixa) sem 1ª/2ª praça nem ``data_leilao``: a oferta continua ativa
    e deve aparecer em «A partir de hoje» / janelas — não excluir por falta de data.
    """
    if faixa not in ("A partir de hoje", "Próximos 7 dias", "Próximos 30 dias"):
        return False
    if _candidatas_datas_leilao_row(r):
        return False
    if _modalidade_venda_da_row(r) == "venda_direta":
        return True
    if (
        _float_lance_campo_row(r, "valor_lance_1_praca")
        or _float_lance_campo_row(r, "valor_lance_2_praca")
        or _float_lance_campo_row(r, "valor_arrematacao")
    ):
        return True
    return False


def _proxima_data_leilao_row(r: dict[str, Any]) -> date | None:
    """Próxima data: 1ª/2ª praça ou ``data_leilao``; futura preferida; se só passado, a mais recente."""
    today = date.today()
    ds = _candidatas_datas_leilao_row(r)
    if not ds:
        return None
    fut = [d for d in ds if d >= today]
    if fut:
        return min(fut)
    return max(ds)


def _sort_key_leilao_data_proxima(r: dict[str, Any]) -> tuple[int, int, str]:
    """Ordena: futuros (asc), depois passados (mais recente primeiro), sem data por último."""
    today = date.today()
    ds = _candidatas_datas_leilao_row(r)
    if not ds:
        return (2, 0, str(r.get("id") or ""))
    fut = [d for d in ds if d >= today]
    if fut:
        return (0, min(fut).toordinal(), str(r.get("id") or ""))
    return (1, -max(ds).toordinal(), str(r.get("id") or ""))


def _ordenar_leiloes_por_data_proxima(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=_sort_key_leilao_data_proxima)


_FAIXAS_DATA_LEILAO: tuple[str, ...] = (
    "Todas as datas",
    "A partir de hoje",
    "Próximos 7 dias",
    "Próximos 30 dias",
    "Sem data cadastrada",
    "Somente já realizados",
)


def _row_match_faixa_data_leilao(r: dict[str, Any], faixa: str) -> bool:
    if faixa == "Todas as datas":
        return True
    today = date.today()
    ds = _candidatas_datas_leilao_row(r)
    pd_ = _proxima_data_leilao_row(r)
    if faixa == "Sem data cadastrada":
        return len(ds) == 0
    if faixa == "Somente já realizados":
        return bool(ds) and all(d < today for d in ds)
    if pd_ is None:
        return _incluir_em_faixa_por_venda_direta_sem_praças(r, faixa)
    if faixa == "A partir de hoje":
        return pd_ >= today
    if faixa == "Próximos 7 dias":
        return today <= pd_ <= today + timedelta(days=7)
    if faixa == "Próximos 30 dias":
        return today <= pd_ <= today + timedelta(days=30)
    return True


def _filtrar_rows_tabela_leiloes_topo(
    rows: list[dict[str, Any]],
    *,
    estado: str,
    cidade: str,
    bairro: str,
    tipo: str,
    faixa_data: str,
    texto_livre: str,
) -> list[dict[str, Any]]:
    def norm(s: str) -> str:
        return str(s or "").strip().lower()

    def norm_filt(x: str) -> str:
        xs = str(x or "").strip()
        if not xs or xs == _TODOS_FILTRO:
            return ""
        return xs.lower()

    e, c, b, t = norm_filt(estado), norm_filt(cidade), norm_filt(bairro), norm_filt(tipo)
    tx = norm(texto_livre)
    out: list[dict[str, Any]] = []
    for r in rows:
        if not _row_match_faixa_data_leilao(r, faixa_data):
            continue
        if e and norm(r.get("estado")) != e:
            continue
        if c and norm(r.get("cidade")) != c:
            continue
        if b and norm(r.get("bairro")) != b:
            continue
        if t and norm(r.get("tipo_imovel")) != t:
            continue
        if tx and tx not in norm(_label_resumo_leilao_row(r)):
            continue
        out.append(r)
    return out


def _opts_filtro_dropdown(valores: list[str]) -> list[str]:
    u = sorted({str(v).strip() for v in valores if str(v).strip()}, key=lambda s: s.lower())
    return [_TODOS_FILTRO] + u


def _label_resumo_leilao_row(r: dict[str, Any]) -> str:
    cid = str(r.get("cidade") or "?").strip() or "?"
    est = str(r.get("estado") or "").strip()
    if est:
        cid = f"{cid}/{est}"
    bai = str(r.get("bairro") or "?").strip() or "?"
    tip = str(r.get("tipo_imovel") or "?").strip() or "?"
    end = str(r.get("endereco") or "").strip()
    if len(end) > 42:
        end = end[:39] + "…"
    url = str(r.get("url_leilao") or "").strip()
    if len(url) > 40:
        url = url[:37] + "…"
    ped = str(r.get("id") or "")[:8]
    parts = [f"{cid} · {bai}", tip]
    if end:
        parts.append(end)
    if url:
        parts.append(url)
    return f"[{ped}] " + " · ".join(parts)


def _sync_sim_pick_leilao_ids(ids: list[str]) -> None:
    """Alinha ``sim_pick_leilao`` com ``ultimo_extracao`` quando o id está na lista filtrada."""
    if not ids:
        return
    cur = str((st.session_state.get("ultimo_extracao") or {}).get("id") or "")
    pick = st.session_state.get("sim_pick_leilao")

    if cur in ids:
        if str(pick) != cur:
            st.session_state["sim_pick_leilao"] = cur
        st.session_state["dash_pick_leilao"] = cur
        return

    p = str(pick) if pick is not None else ""
    if p in ids:
        st.session_state["dash_pick_leilao"] = p
        return
    st.session_state["sim_pick_leilao"] = ids[0]
    st.session_state["dash_pick_leilao"] = ids[0]


def _sim_sincronizar_ultimo_com_pick(ids: list[str], cli: Any) -> None:
    """Se o id em ``sim_pick_leilao`` difere do carregado, busca o registro completo e atualiza a sessão."""
    pick = str(st.session_state.get("sim_pick_leilao") or "")
    if pick not in ids:
        return
    ult = st.session_state.get("ultimo_extracao") or {}
    ult_id = str(ult.get("id") or "")
    if pick == ult_id:
        return
    try:
        full = leilao_imoveis_repo.buscar_por_id(pick, cli)
    except Exception:
        logger.exception("Falha ao carregar leilão (aba Simulação)")
        return
    if not isinstance(full, dict):
        return
    st.session_state["ultimo_extracao"] = full
    st.session_state["dash_pick_leilao"] = pick
    st.session_state.snapshot = {
        "url": str(full.get("url_leilao") or "—"),
        "status": "carregado_banco",
        "id": full.get("id"),
        "modelo": "—",
        "tokens": "—",
        "nota": "Carregado pela lista da aba Simulação.",
    }
    st.rerun()


def _painel_sincronizar_ultimo_com_picker(ids: list[str], cli: Any) -> None:
    """Se o id escolhido no seletor difere do registro exibido, recarrega do banco (imóvel + snapshot)."""
    pick = str(st.session_state.get("dash_pick_leilao") or "")
    if pick not in ids:
        return
    ult = st.session_state.get("ultimo_extracao") or {}
    ult_id = str(ult.get("id") or "")
    if pick == ult_id:
        return
    try:
        full = leilao_imoveis_repo.buscar_por_id(pick, cli)
    except Exception:
        logger.exception("Falha ao carregar leilão por id (lista)")
        return
    if not isinstance(full, dict):
        return
    st.session_state["ultimo_extracao"] = full
    st.session_state["sim_pick_leilao"] = pick
    st.session_state.snapshot = {
        "url": str(full.get("url_leilao") or "—"),
        "status": "carregado_banco",
        "id": full.get("id"),
        "modelo": "—",
        "tokens": "—",
        "nota": "Carregado da lista de leilões.",
    }
    st.rerun()


def _fmt_rs_m2_resumo(val: float | None) -> str:
    if val is None or val <= 0:
        return "—"
    s = f"{float(val):,.2f}"
    if "," in s and "." in s:
        s = s.replace(",", "_T_").replace(".", ",").replace("_T_", ".")
    return f"{s} R$/m²"


def _agregar_caches_para_painel_sim(
    caches_sel: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Estatísticas dos caches selecionados: prefere a união dos anúncios (média / min / max de valor e R$/m²);
    se não houver anúncios resolvidos, usa médias dos registros de cache e min/max ``menor_valor_venda`` /
    ``maior_valor_venda`` quando existirem.
    """
    if not caches_sel:
        return {
            "media_pm2": None,
            "media_valor": None,
            "min_valor": None,
            "max_valor": None,
            "fonte": "vazio",
            "n_anuncios": 0,
        }
    vals: list[float] = []
    pm2s: list[float] = []
    vistos: set[str] = set()
    for c in caches_sel:
        for aid in _parse_csv_uuids_ids_anuncios(c.get("anuncios_ids")):
            if aid in vistos:
                continue
            ad = ads_map.get(aid)
            if not isinstance(ad, dict):
                continue
            vistos.add(aid)
            try:
                v = float(ad.get("valor_venda") or 0)
                ar = float(ad.get("area_construida_m2") or 0)
            except (TypeError, ValueError):
                continue
            if v > 0:
                vals.append(v)
            if v > 0 and ar > 0:
                pm2s.append(v / ar)
    if vals:
        return {
            "media_pm2": statistics.mean(pm2s) if pm2s else None,
            "media_valor": statistics.mean(vals),
            "min_valor": min(vals),
            "max_valor": max(vals),
            "fonte": "anuncios",
            "n_anuncios": len(vals),
        }
    pm2_row = [
        float(c.get("preco_m2_medio") or 0)
        for c in caches_sel
        if float(c.get("preco_m2_medio") or 0) > 0
    ]
    vm_row = [
        float(c.get("valor_medio_venda") or 0)
        for c in caches_sel
        if float(c.get("valor_medio_venda") or 0) > 0
    ]
    menores: list[float] = []
    maiores: list[float] = []
    for c in caches_sel:
        for key, bucket in (("menor_valor_venda", menores), ("maior_valor_venda", maiores)):
            raw = c.get(key)
            if raw is None:
                continue
            try:
                x = float(raw)
            except (TypeError, ValueError):
                continue
            if x > 0:
                bucket.append(x)
    media_pm2 = statistics.mean(pm2_row) if pm2_row else None
    media_valor = statistics.mean(vm_row) if vm_row else None
    min_v = min(menores) if menores else (min(vm_row) if vm_row else None)
    max_v = max(maiores) if maiores else (max(vm_row) if vm_row else None)
    return {
        "media_pm2": media_pm2,
        "media_valor": media_valor,
        "min_valor": min_v,
        "max_valor": max_v,
        "fonte": "cache_row",
        "n_anuncios": 0,
    }


def _proposta_frase_busca_imovel_id(iid: str) -> str:
    """Texto padrão da pesquisa Firecrawl a partir do registo no Supabase (mesma regra do pipeline)."""
    from leilao_ia_v2.fc_search.query_builder import montar_frase_busca_mercado
    from leilao_ia_v2.normalizacao import normalizar_tipo_imovel

    try:
        cli = get_supabase_client()
    except Exception:
        return ""
    r = leilao_imoveis_repo.buscar_por_id(str(iid).strip(), cli)
    if not isinstance(r, dict) or not r:
        return ""
    try:
        t = str(normalizar_tipo_imovel(r.get("tipo_imovel")) or "apartamento")
    except Exception:
        t = "apartamento"
    return (montar_frase_busca_mercado(r, t) or "").strip()


def _executar_pendente_frase_firecrawl_pos_ingest(frase_digitada: str) -> None:
    """Corre o Firecrawl Search pós-ingestão com a frase escolhida e depois o cache automático."""
    p = st.session_state.get("fc_pendente_pos_ingest")
    if not isinstance(p, dict) or not p.get("leilao_imovel_id"):
        return
    frase = (frase_digitada or "").strip()
    if len(frase) < 8:
        st.error("A frase de busca deve ter pelo menos 8 caracteres (requisito do Firecrawl Search).")
        return
    pl = p.get("payload_comparaveis")
    if not isinstance(pl, dict):
        st.error("Dados pendentes inválidos — volte a ingerir o edital.")
        return
    try:
        cli = get_supabase_client()
    except Exception as e:
        st.error(f"Supabase indisponível: {e}")
        return
    from leilao_ia_v2.fc_search.pipeline import complementar_anuncios_firecrawl_search

    lid = str(p.get("leilao_imovel_id") or "").strip()
    cap0 = int(p.get("restante_fc_antes_comparaveis") or 0)
    ign = bool(p.get("ignorar_cache_firecrawl", False))
    with st.spinner("Firecrawl Search (comparáveis) e montagem de cache…"):
        try:
            salvos, diag_fc, n_api = complementar_anuncios_firecrawl_search(
                cli,
                leilao_imovel_id=lid,
                cidade=str(pl.get("cidade") or ""),
                estado_raw=str(pl.get("estado_raw") or ""),
                bairro=str(pl.get("bairro") or ""),
                tipo_imovel=str(pl.get("tipo_imovel") or "apartamento"),
                area_ref=float(pl.get("area_ref") or 0),
                ignorar_cache_firecrawl=ign,
                max_chamadas_api=cap0,
                frase_busca_override=frase,
            )
        except Exception as e:
            st.error(f"Falha no Firecrawl Search: {e}")
            logger.exception("Pendente pós-ingest: complementar")
            return
        rest2 = max(0, cap0 - int(n_api or 0))
        try:
            cres = resolver_cache_media_pos_ingestao(
                cli,
                lid,
                ignorar_cache_firecrawl=ign,
                max_chamadas_api_firecrawl=rest2,
                frase_busca_firecrawl_override=frase,
            )
        except Exception as e:
            st.error(f"Falha ao montar cache: {e}")
            logger.exception("Pendente pós-ingest: resolver cache")
            return
    st.session_state.pop("fc_pendente_pos_ingest", None)
    for k in list(st.session_state.keys()):
        if k.startswith("fc_pendente_frase_draft"):
            st.session_state.pop(k, None)
    st.success(
        f"Concluído: comparáveis gravados={int(salvos or 0)} · cache={'OK' if cres.ok else cres.mensagem}"
    )
    st.text((diag_fc or "")[:4000] + ("\n" + formatar_log_pos_cache(cres))[:2000])
    try:
        fresh = leilao_imoveis_repo.buscar_por_id(lid, cli)
    except Exception:
        fresh = None
    if isinstance(fresh, dict) and fresh.get("id"):
        st.session_state["ultimo_extracao"] = fresh
    st.rerun()


def _render_pendente_frase_firecrawl_pos_ingest() -> None:
    p = st.session_state.get("fc_pendente_pos_ingest")
    if not isinstance(p, dict) or not p.get("leilao_imovel_id"):
        return
    proposta = str(p.get("frase_proposta") or "")
    lidp = str(p.get("leilao_imovel_id") or "").strip()
    kdraft = f"fc_pendente_frase_draft_{lidp}"
    st.session_state.setdefault(kdraft, proposta)
    with st.container(border=True):
        st.warning("**Pendente:** confirmação da frase de **Firecrawl Search** (anúncios na web) antes de gastar créditos.")
        st.caption("Edite a frase se quiser — o sistema usa exatamente o texto abaixo na pesquisa.")
        t = st.text_area(
            "Frase de busca a usar",
            key=kdraft,
            height=100,
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Executar busca e montar cache", type="primary", key="fc_pendente_executar"):
                _executar_pendente_frase_firecrawl_pos_ingest(t)
        with c2:
            if st.button("Descartar pendência", key="fc_pendente_descartar"):
                st.session_state.pop("fc_pendente_pos_ingest", None)
                st.session_state.pop(kdraft, None)
                st.rerun()


def _html_kpis_caches_simulacao(agg: dict[str, Any]) -> str:
    pm2_t = html.escape(_fmt_rs_m2_resumo(agg.get("media_pm2")))
    vm_t = html.escape(_fmt_valor_campo("valor_venda", agg.get("media_valor")))
    mn_t = html.escape(_fmt_valor_campo("valor_venda", agg.get("min_valor")))
    mx_t = html.escape(_fmt_valor_campo("valor_venda", agg.get("max_valor")))
    return (
        '<div class="leilao-cache-kpi-row">'
        '<div class="leilao-cache-kpi"><div class="lbl">Média R$/m²</div>'
        f'<div class="val">{pm2_t}</div></div>'
        '<div class="leilao-cache-kpi"><div class="lbl">Média valor venda</div>'
        f'<div class="val">{vm_t}</div></div>'
        '<div class="leilao-cache-kpi"><div class="lbl">Menor valor</div>'
        f'<div class="val">{mn_t}</div></div>'
        '<div class="leilao-cache-kpi"><div class="lbl">Maior valor</div>'
        f'<div class="val">{mx_t}</div></div>'
        "</div>"
    )


def _render_bloco_recalcular_caches_mercado(imovel_id: str) -> None:
    """Recálculo: desvincula caches, opcional apaga orfãos, cria novos (aba Simulação)."""
    iid = str(imovel_id or "").strip()
    if not iid:
        return
    with st.expander("Recalcular caches de mercado", expanded=False):
        st.caption(
            "Remove os **vínculos** atuais deste imóvel, monta **novas** entradas em `cache_media_bairro` "
            "a partir de anúncios (raio, mín. amostras, **máx. anúncios** por bloco = **Ajustes de busca** na barra) e liga o resultado em "
            "`cache_media_bairro_ids`. Não altera o relatório LLM (`relatorio_mercado_contexto_json`) nem "
            "`operacao_simulacao_json` — pode querer recalcular a simulação depois. "
            "**Vários imóveis podem reutilizar o mesmo** registo de cache; ao apagar, só removemos "
            "linhas a que **nenhum** outro leilão ainda aponte (excepto o que desvinculámos agora)."
        )
        apagar_orfaos = st.checkbox(
            "Apagar do banco linhas de cache deixadas orfãs (sem nenhum leilão a referenciar)",
            value=True,
            key=f"recache_orfao_{iid}",
        )
        ign_fc = st.checkbox(
            "Ignorar cache em disco (Firecrawl em URLs já raspadas)",
            value=False,
            key=f"recache_ignfc_{iid}",
        )
        bp = parametros_de_session_state(st.session_state)
        sk = f"recache_frase_draft_{iid}"
        if bp.confirmar_frase_firecrawl_search:
            st.session_state.setdefault(sk, _proposta_frase_busca_imovel_id(iid))
            st.text_area(
                "Frase de busca (Firecrawl Search)",
                key=sk,
                height=90,
                help="Texto exato a usar na pesquisa web (mínimo 8 caracteres). Ajuste em **Ajustes de busca** para não pedir confirmação.",
            )
        if st.button(
            "Recalcular caches agora",
            key=f"recache_run_{iid}",
            type="primary",
        ):
            try:
                cli = get_supabase_client()
            except Exception as e:
                st.error(f"Supabase indisponível: {e}")
                return
            frase_oc: str | None = None
            if bp.confirmar_frase_firecrawl_search:
                fr = (st.session_state.get(sk) or "").strip()
                if len(fr) < 8:
                    st.error("A frase de busca deve ter pelo menos 8 caracteres.")
                    return
                frase_oc = fr
            with st.spinner("A desvincular, eventualmente apagar orfãs e a criar novos caches…"):
                r = recalcular_caches_mercado_para_leilao(
                    cli,
                    iid,
                    apagar_caches_sem_outro_vinculo=apagar_orfaos,
                    ignorar_cache_firecrawl=ign_fc,
                    raio_km=float(bp.raio_km),
                    max_chamadas_api_firecrawl=int(bp.max_firecrawl_creditos_analise),
                    frase_busca_firecrawl_override=frase_oc,
                )
            if r.ok:
                st.success(r.mensagem)
                st.text(formatar_log_pos_cache(r))
                try:
                    fresh = leilao_imoveis_repo.buscar_por_id(iid, cli)
                except Exception:
                    fresh = None
                if isinstance(fresh, dict) and fresh.get("id"):
                    st.session_state["ultimo_extracao"] = fresh
                st.rerun()
            else:
                st.error(r.mensagem)
                if (r.log_diagnostico or "").strip():
                    st.text(r.log_diagnostico)


def _render_painel_caches_leilao_selecionado_simulacao() -> None:
    """Ao lado da tabela de leilões (aba Simulação): caches do imóvel selecionado + KPIs agregados."""
    with st.container(border=True):
        st.markdown('<div class="sim-card-head">Caches do leilão</div>', unsafe_allow_html=True)
        row = st.session_state.get("ultimo_extracao")
        if not isinstance(row, dict) or not row.get("id"):
            st.caption("Selecione um leilão na tabela ao lado para listar os caches vinculados.")
            return
        iid = str(row.get("id") or "").strip()
        try:
            caches, ads_map = _carregar_caches_e_anuncios_ui(row)
        except Exception:
            logger.exception("Caches painel simulação (_carregar_caches_e_anuncios_ui)")
            st.caption("Não foi possível carregar os caches (Supabase ou rede).")
            return
        if not caches:
            st.caption("Este leilão ainda não tem entradas em `cache_media_bairro_ids` — use **Recalcular** abaixo para gerar a partir de anúncios.")
        else:
            kpi_slot = st.empty()
            legenda_slot = st.empty()

            rows_ix: list[int] = []
            sig = "|".join(str(c.get("id") or "") for c in caches)
            df_key = f"sim_panel_caches_{iid}_{len(sig)}_{abs(hash(sig)) % (10**9)}"
            df_rows: list[dict[str, Any]] = []
            for c in caches:
                md = _metadados_cache_row_ui(c)
                ref = (
                    str(md.get("modo_cache") or "").strip().lower() == "terrenos"
                    or md.get("apenas_referencia") is True
                    or md.get("uso_simulacao") is False
                )
                papel = "Referência" if ref else "Simulação"
                df_rows.append(
                    {
                        "Segmento": (str(c.get("nome_cache") or "—").strip())[:72],
                        "n": int(c.get("n_amostras") or 0),
                        "R$/m² méd.": round(float(c.get("preco_m2_medio") or 0), 2),
                        "Valor médio": round(float(c.get("valor_medio_venda") or 0), 2),
                        "Papel": papel,
                    }
                )
            df = pd.DataFrame(df_rows)
            sel_def: dict[str, Any] = {
                "selection": {"rows": list(range(len(caches))), "columns": [], "cells": []},
            }
            h = min(420, max(180, 56 + len(caches) * 32))
            ev = st.dataframe(
                df,
                width="stretch",
                height=h,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
                key=df_key,
                selection_default=sel_def,
            )
            if isinstance(ev, dict):
                rows_ix = list((ev.get("selection") or {}).get("rows") or [])
            elif hasattr(ev, "selection") and ev.selection is not None:
                rows_ix = list(ev.selection.rows or [])
            chosen = [caches[i] for i in rows_ix if 0 <= i < len(caches)] if rows_ix else list(caches)
            agg = _agregar_caches_para_painel_sim(chosen, ads_map)
            kpi_slot.markdown(
                f'<div class="sim-card-html">{_html_kpis_caches_simulacao(agg)}</div>',
                unsafe_allow_html=True,
            )
            nsel = len(chosen)
            if agg.get("fonte") == "anuncios":
                legenda_slot.caption(
                    f"Resumo: **{agg.get('n_anuncios', 0)}** anúncios nos **{nsel}** cache(s) destacados. "
                    "Sem linhas selecionadas, usamos **todos** os caches."
                )
            elif agg.get("fonte") == "cache_row":
                legenda_slot.caption(
                    f"Médias e extremos a partir dos registros de cache (**{nsel}**). "
                    "Sem seleção na tabela, agregamos **todos**."
                )
            else:
                legenda_slot.caption(
                    "Sem dados numéricos para o resumo. Marque caches na tabela abaixo; vazio = todos."
                )
        _render_bloco_recalcular_caches_mercado(iid)


def _lista_topo_reset_sel_se_invalido(key: str, opcoes: list[str]) -> None:
    v = st.session_state.get(key)
    if v is not None and v not in opcoes:
        st.session_state[key] = _TODOS_FILTRO


def _leiloes_df_ler_indices_selecionados(key: str) -> list[int]:
    """``st.dataframe`` com ``on_select``: índices de linha na sessão (run anterior)."""
    raw = st.session_state.get(key)
    if raw is None:
        return []
    try:
        sel = raw.get("selection") if hasattr(raw, "get") else None
        if sel is None:
            return []
        rows = sel.get("rows") if hasattr(sel, "get") else None
        if not rows:
            return []
        return [int(x) for x in rows]
    except (TypeError, ValueError, AttributeError):
        return []


def _render_painel_tabela_leiloes_topo() -> None:
    """Topo (Análise / Simulação): tabela de leilões + filtros em dropdown; ordenação por data mais próxima."""
    try:
        cli = get_supabase_client()
    except Exception:
        st.session_state["_rows_resumo_leiloes"] = []
        st.session_state["_lista_topo_ids_f"] = []
        st.caption("Supabase indisponível — lista de leilões não carregada.")
        return

    try:
        raw = leilao_imoveis_repo.listar_resumo_recentes(cli, limite=500)
    except Exception:
        logger.exception("listar_resumo_recentes (tabela topo)")
        st.warning("Não foi possível listar leilões do banco.")
        st.session_state["_rows_resumo_leiloes"] = []
        st.session_state["_lista_topo_ids_f"] = []
        return

    rows_sorted = _ordenar_leiloes_por_data_proxima(raw)
    st.session_state["_rows_resumo_leiloes"] = rows_sorted

    with st.container(border=True):
        st.markdown('<div class="sim-card-head">Leilões cadastrados</div>', unsafe_allow_html=True)
        if not rows_sorted:
            st.caption("Ainda não há registros em `leilao_imoveis`.")
            st.session_state["_lista_topo_ids_f"] = []
            return

        st.markdown('<div class="lista-leiloes-filtros">', unsafe_allow_html=True)
        est_opts = _opts_filtro_dropdown([str(r.get("estado") or "") for r in rows_sorted])
        est_sel = str(st.session_state.get("lista_topo_filt_estado") or _TODOS_FILTRO)
        rows_e = (
            rows_sorted
            if est_sel == _TODOS_FILTRO
            else [r for r in rows_sorted if str(r.get("estado") or "").strip() == est_sel]
        )
        cid_opts = _opts_filtro_dropdown([str(r.get("cidade") or "") for r in rows_e])
        _lista_topo_reset_sel_se_invalido("lista_topo_filt_cidade", cid_opts)
        cid_sel = str(st.session_state.get("lista_topo_filt_cidade") or _TODOS_FILTRO)
        rows_ec = (
            rows_e
            if cid_sel == _TODOS_FILTRO
            else [r for r in rows_e if str(r.get("cidade") or "").strip() == cid_sel]
        )
        bai_opts = _opts_filtro_dropdown([str(r.get("bairro") or "") for r in rows_ec])
        _lista_topo_reset_sel_se_invalido("lista_topo_filt_bairro", bai_opts)
        bai_sel = str(st.session_state.get("lista_topo_filt_bairro") or _TODOS_FILTRO)
        rows_ecb = (
            rows_ec
            if bai_sel == _TODOS_FILTRO
            else [r for r in rows_ec if str(r.get("bairro") or "").strip() == bai_sel]
        )
        tip_opts = _opts_filtro_dropdown([str(r.get("tipo_imovel") or "") for r in rows_ecb])
        _lista_topo_reset_sel_se_invalido("lista_topo_filt_tipo", tip_opts)

        g1, g2, g3, g4, g5 = st.columns([0.9, 1.1, 1.1, 1.15, 1.35], gap="small")
        with g1:
            st.selectbox("UF", options=est_opts, key="lista_topo_filt_estado")
        with g2:
            st.selectbox("Cidade", options=cid_opts, key="lista_topo_filt_cidade")
        with g3:
            st.selectbox("Bairro", options=bai_opts, key="lista_topo_filt_bairro")
        with g4:
            st.selectbox("Tipo do imóvel", options=tip_opts, key="lista_topo_filt_tipo")
        with g5:
            st.selectbox(
                "Data do leilão",
                options=list(_FAIXAS_DATA_LEILAO),
                key="lista_topo_filt_faixa_data",
            )
        st.text_input(
            "Busca livre (endereço, URL…)",
            key="lista_topo_filt_texto",
            placeholder="Trecho do rótulo",
        )
        st.markdown("</div>", unsafe_allow_html=True)

        rows_f = _filtrar_rows_tabela_leiloes_topo(
            rows_sorted,
            estado=str(st.session_state.get("lista_topo_filt_estado") or _TODOS_FILTRO),
            cidade=str(st.session_state.get("lista_topo_filt_cidade") or _TODOS_FILTRO),
            bairro=str(st.session_state.get("lista_topo_filt_bairro") or _TODOS_FILTRO),
            tipo=str(st.session_state.get("lista_topo_filt_tipo") or _TODOS_FILTRO),
            faixa_data=str(st.session_state.get("lista_topo_filt_faixa_data") or _FAIXAS_DATA_LEILAO[0]),
            texto_livre=str(st.session_state.get("lista_topo_filt_texto") or ""),
        )
        ids_f = [str(r.get("id")) for r in rows_f if r.get("id")]
        st.session_state["_lista_topo_ids_f"] = ids_f

        if not ids_f:
            st.caption("Nenhum leilão com os filtros atuais — use **Todos** ou limpe a busca livre.")
            return

        df_rows: list[dict[str, Any]] = [_row_resumo_leilao_para_tabela_exibicao(r) for r in rows_f]
        df = pd.DataFrame(df_rows)
        h = min(520, max(200, 52 + len(rows_f) * 33))
        col_order = list(df_rows[0].keys()) if df_rows else list(df.columns)

        _df_key = f"lista_leiloes_df_{abs(hash(tuple(ids_f)))}"[:110]
        _rws = _leiloes_df_ler_indices_selecionados(_df_key)
        if _rws and 0 <= _rws[0] < len(rows_f):
            _pid = str(rows_f[_rws[0]].get("id") or "")
            if _pid:
                st.session_state["dash_pick_leilao"] = _pid
                st.session_state["sim_pick_leilao"] = _pid
        else:
            _ult_u = str((st.session_state.get("ultimo_extracao") or {}).get("id") or "")
            if _ult_u in ids_f:
                st.session_state["dash_pick_leilao"] = _ult_u
            elif str(st.session_state.get("dash_pick_leilao") or "") not in ids_f:
                st.session_state["dash_pick_leilao"] = ids_f[0]
            st.session_state["sim_pick_leilao"] = str(st.session_state.get("dash_pick_leilao") or "")

        _painel_sincronizar_ultimo_com_picker(ids_f, cli)

        _dp = str(st.session_state.get("dash_pick_leilao") or "")
        try:
            _ix_def = next(i for i, rr in enumerate(rows_f) if str(rr.get("id")) == _dp)
        except StopIteration:
            _ix_def = 0
        _sel_def: dict[str, Any] = {
            "selection": {"rows": [_ix_def], "columns": [], "cells": []},
        }

        st.caption(
            "Tabela de **só leitura**. **Clique numa linha** para carregar o imóvel em **Dados extraídos**, no **mapa** e na **Simulação**."
        )
        st.dataframe(
            df,
            width="stretch",
            height=h,
            column_order=col_order,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key=_df_key,
            selection_default=_sel_def,
        )

        if str(st.session_state.get("assistente_modo") or "") != "ingestao":
            return

        st.markdown("**Edição (registo selecionado acima)**")
        st.caption(
            "Aplica-se ao imóvel **da linha selecionada** na tabela. Datas: **dd/mm/aaaa** ou **—** para limpar. "
            "Colunas id, Próxima e Lance / ref. são somente leitura."
        )
        _r_edit = str(st.session_state.get("dash_pick_leilao") or "")
        r0_solo = next((x for x in rows_f if str(x.get("id")) == _r_edit), None)
        if r0_solo is not None and _r_edit:
            dis_cols: tuple[str, ...] = ("id", "Próxima", "Lance / ref.")
            df_1 = pd.DataFrame([_row_resumo_leilao_para_tabela_exibicao(r0_solo)])
            _ed_solo = f"leiloes_solo_{_r_edit}"
            edited1 = st.data_editor(
                df_1,
                width="stretch",
                height=120,
                hide_index=True,
                num_rows="fixed",
                disabled=dis_cols,
                column_config={
                    "id": st.column_config.TextColumn("id", disabled=True, width="small", help="UUID (somente leitura)"),
                    "Próxima": st.column_config.TextColumn("Próxima", disabled=True, width="small"),
                    "1ª praça": st.column_config.TextColumn("1ª praça", width="small"),
                    "2ª praça": st.column_config.TextColumn("2ª praça", width="small"),
                    "Lance / ref.": st.column_config.TextColumn("Lance / ref.", width="small", disabled=True),
                    "UF": st.column_config.TextColumn("UF", max_chars=2, width="small"),
                    "Cidade": st.column_config.TextColumn("Cidade", width="medium"),
                    "Bairro": st.column_config.TextColumn("Bairro", width="small"),
                    "Tipo": st.column_config.TextColumn("Tipo", width="small"),
                    "Endereço": st.column_config.TextColumn("Endereço", width="large"),
                    "URL": st.column_config.TextColumn("URL", width="large"),
                },
                column_order=col_order,
                key=_ed_solo,
            )
            if st.button("Gravar alterações no Supabase", use_container_width=True, key="lista_topo_salvar_solo"):
                iid0 = str(r0_solo.get("id") or "")
                n_ed = len(edited1) if edited1 is not None and hasattr(edited1, "iloc") else 0
                if not iid0 or n_ed < 1:
                    st.error("Registo inválido — recarregue a página.")
                else:
                    edr = edited1.iloc[0]
                    if str(edr.get("id") or "").strip() != iid0:
                        st.error("A linha de edição não corresponde ao registo. Recarregue a página.")
                    else:
                        patch, err1 = _montar_patch_leilao_tabela_row(r0_solo, edr)
                        if err1:
                            st.error(err1)
                        elif not patch:
                            st.info("Nenhuma alteração a gravar.")
                        else:
                            try:
                                leilao_imoveis_repo.atualizar_leilao_imovel(iid0, patch, cli)
                                st.session_state.pop("_rows_resumo_leiloes", None)
                                st.success("Registo atualizado no Supabase.")
                                try:
                                    up = leilao_imoveis_repo.buscar_por_id(
                                        str(st.session_state.get("dash_pick_leilao") or iid0), cli
                                    )
                                    if isinstance(up, dict):
                                        st.session_state["ultimo_extracao"] = up
                                except Exception:
                                    logger.exception("refresh ultimo após gravação (edição tabela leilões)")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                                logger.exception("atualizar leilão tabela solo iid=%s", iid0[:12])
        else:
            st.caption("Selecione um imóvel na tabela (clique numa **linha**) para editar aqui.")


def _render_mapa_resumo_leiloes(rows: list[dict[str, Any]], selected_id: str | None) -> None:
    """Mapa Folium: todos os leilões com coordenadas; o selecionado em vermelho."""
    try:
        import folium
        from folium.plugins import MarkerCluster
        from streamlit_folium import st_folium
    except ImportError:
        st.warning(
            "Instale as dependências do mapa: `pip install folium streamlit-folium` "
            "(já listadas em `leilao_ia_v2/requirements.txt`)."
        )
        return

    pts: list[tuple[dict[str, Any], float, float]] = []
    for r in rows:
        la, lo = r.get("latitude"), r.get("longitude")
        if la is None or lo is None:
            continue
        try:
            fa, fo = float(la), float(lo)
        except (TypeError, ValueError):
            continue
        pts.append((r, fa, fo))
    if not pts:
        st.caption("Nenhum leilão na lista com latitude/longitude — mapa não exibido.")
        return

    sel = str(selected_id or "").strip()
    others = [(r, fa, fo) for r, fa, fo in pts if str(r.get("id") or "") != sel]
    selected_pts = [(r, fa, fo) for r, fa, fo in pts if str(r.get("id") or "") == sel]

    center_lat = sum(p[1] for p in pts) / len(pts)
    center_lon = sum(p[2] for p in pts) / len(pts)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=11, control_scale=True, tiles="OpenStreetMap")

    cluster = MarkerCluster(
        name="Leilões",
        overlay=True,
        control=True,
        spiderfyOnMaxZoom=True,
        showCoverageOnHover=False,
        zoomToBoundsOnClick=True,
        maxClusterRadius=55,
        spiderfyDistanceMultiplier=1.35,
        removeOutsideVisibleBounds=True,
    ).add_to(m)

    for r, fa, fo in others:
        titulo = html.escape(_label_resumo_leilao_row(r)[:200])
        url_lei = str(r.get("url_leilao") or "").strip()
        if url_lei:
            uq = html.escape(url_lei, quote=True)
            pop = folium.Popup(
                f"{titulo}<br/><a href=\"{uq}\" target=\"_blank\" rel=\"noopener noreferrer\">Link leilão</a>",
                max_width=320,
            )
        else:
            pop = folium.Popup(titulo, max_width=320)
        folium.CircleMarker(
            location=[fa, fo],
            radius=7,
            color="#38bdf8",
            weight=2,
            fill=True,
            fill_color="#0ea5e9",
            fill_opacity=0.8,
            popup=pop,
            tooltip=folium.Tooltip(titulo, sticky=True),
        ).add_to(cluster)

    for r, fa, fo in selected_pts:
        titulo = html.escape(_label_resumo_leilao_row(r)[:200])
        url_lei = str(r.get("url_leilao") or "").strip()
        if url_lei:
            uq = html.escape(url_lei, quote=True)
            pop = folium.Popup(
                f"{titulo}<br/><span style=\"color:#b91c1c\"><strong>Selecionado</strong></span><br/>"
                f'<a href="{uq}" target="_blank" rel="noopener noreferrer">Link leilão</a>',
                max_width=320,
            )
        else:
            pop = folium.Popup(
                f"{titulo}<br/><span style=\"color:#b91c1c\"><strong>Selecionado</strong></span>",
                max_width=320,
            )
        folium.Marker(
            location=[fa, fo],
            popup=pop,
            tooltip=folium.Tooltip("Selecionado · " + titulo, sticky=True),
            icon=folium.Icon(color="red"),
        ).add_to(m)

    if len(pts) >= 2:
        lats = [p[1] for p in pts]
        lons = [p[2] for p in pts]
        m.fit_bounds(
            [[min(lats), min(lons)], [max(lats), max(lons)]],
            padding=(24, 24),
            max_zoom=16,
        )

    map_key = f"dash_resumo_map_{sel or 'all'}"[:120]
    st_folium(
        m,
        width=None,
        height=400,
        use_container_width=True,
        key=map_key,
        returned_objects=[],
    )
    if len(pts) >= 2:
        st.caption(
            "Pontos azuis: leilões cadastrados; vermelho: a linha selecionada na tabela. "
            "Agrupamentos podem abrir em **aranha (spiderfy)** ao aproximar o zoom."
        )


def _render_painel_leiloes_cadastrados_ingestao() -> None:
    """Mapa resumido; a tabela de leilões fica no topo (compartilhada com a aba Simulação)."""
    rows = list(st.session_state.get("_rows_resumo_leiloes") or [])
    ids = [str(r.get("id")) for r in rows if r.get("id")]
    if not ids:
        with st.expander("Mapa — leilões cadastrados", expanded=False):
            st.caption("Sem pontos para exibir — aguarde a lista no topo ou configure o Supabase.")
        return

    sel_id = str(st.session_state.get("dash_pick_leilao") or "")
    if sel_id not in ids:
        sel_id = ids[0]
    with st.expander("Mapa — leilões cadastrados", expanded=False):
        _render_mapa_resumo_leiloes(rows, sel_id if sel_id in ids else None)


_MODO_CHAVE = "assistente_modo"
_MODOS_VALIDOS = frozenset({"inicio", "ingestao", "simulacao", "anuncios"})
# Navegação a partir de cards do painel: não pode atribuir a ``_MODO_CHAVE`` no mesmo run de
# certos callbacks — definimos o pendente e aplicamos no início de ``main()``, antes dos widgets.
_PENDING_MODO = "_pending_set_assistente_modo"


def _aplicar_modo_pendente_antes_dos_widgets() -> None:
    p = st.session_state.pop(_PENDING_MODO, None)
    if p is not None and str(p) in _MODOS_VALIDOS:
        st.session_state[_MODO_CHAVE] = str(p)


def _init_session() -> None:
    st.session_state.setdefault("agno_session_id", str(uuid.uuid4()))
    st.session_state.setdefault("assistente_modo", "inicio")
    if str(st.session_state.get("assistente_modo") or "") not in _MODOS_VALIDOS:
        st.session_state["assistente_modo"] = "inicio"
    st.session_state.setdefault("snapshot", {})
    st.session_state.setdefault("pending_duplicate_url", None)
    st.session_state.setdefault("pending_duplicate_registro", None)
    st.session_state.setdefault("ultimo_extracao", None)
    st.session_state.setdefault("ultima_metricas_llm", {})
    st.session_state.setdefault("ultima_metricas_contexto_relatorio", {})
    st.session_state.setdefault("tokens_prompt_sessao", 0)
    st.session_state.setdefault("tokens_completion_sessao", 0)
    st.session_state.setdefault("custo_usd_sessao", 0.0)
    st.session_state.setdefault("firecrawl_creditos_usados_sessao", 0)
    for _k, _v in defaults_chaves_busca_mercado_session().items():
        st.session_state.setdefault(_k, _v)


def _fmt_usd_sidebar(val: Any) -> str:
    try:
        x = round(float(val or 0), 10)
    except (TypeError, ValueError):
        return "—"
    if abs(x) < 1e-12:
        return "US$ 0,00"
    s = f"{x:.8f}".rstrip("0").rstrip(".")
    if "." in s:
        intp, frac = s.split(".", 1)
        return f"US$ {intp},{frac}"
    return f"US$ {s}"


def _acumular_metricas_sidebar(
    metricas: dict[str, Any] | None,
    row: dict[str, Any] | None,
    *,
    firecrawl_chamadas_api_extra: int = 0,
    firecrawl_chamadas_api_ingestao: int | None = None,
    ultima_slot: str = "ingestao",
) -> None:
    """
    Atualiza tokens da última extração, totais da sessão e créditos Firecrawl.

    - Tokens/custo: soma sempre os valores passados em ``metricas`` (prompt + completion + custo USD).
    - Firecrawl: ``firecrawl_chamadas_api_ingestao`` = chamadas reais contabilizadas pelo pipeline (edital + comparáveis + cache);
      se ``None``, mantém heurística antiga (+1 quando edital_fonte=firecrawl e houve tokens).
    - ``ultima_slot``: ``ingestao`` grava em ``ultima_metricas_llm``; ``contexto_relatorio`` em
      ``ultima_metricas_contexto_relatorio`` (não sobrescreve a última extração de edital).
    Ao final, invalida o cache do saldo Firecrawl para a próxima leitura na sidebar vir da API.
    """
    m = dict(metricas or {})
    pt = int(m.get("prompt_tokens") or 0)
    ct = int(m.get("completion_tokens") or 0)
    try:
        custo_add = float(m.get("custo_usd_estimado") or 0)
    except (TypeError, ValueError):
        custo_add = 0.0

    dirty = False
    if pt or ct or custo_add > 0 or m:
        if str(ultima_slot or "").strip().lower() == "contexto_relatorio":
            st.session_state["ultima_metricas_contexto_relatorio"] = m
        else:
            st.session_state["ultima_metricas_llm"] = m
        dirty = True
    if pt or ct:
        st.session_state["tokens_prompt_sessao"] = st.session_state.get("tokens_prompt_sessao", 0) + pt
        st.session_state["tokens_completion_sessao"] = st.session_state.get("tokens_completion_sessao", 0) + ct
    if custo_add > 0:
        st.session_state["custo_usd_sessao"] = float(st.session_state.get("custo_usd_sessao", 0.0)) + custo_add

    fc_inc = int(firecrawl_chamadas_api_extra or 0)
    if firecrawl_chamadas_api_ingestao is not None:
        fc_inc += max(0, int(firecrawl_chamadas_api_ingestao))
    elif (
        row
        and (pt or ct)
        and str(ultima_slot or "").strip().lower() != "contexto_relatorio"
        and str(row.get("edital_fonte") or "").strip().lower() == "firecrawl"
    ):
        fc_inc += 1
    if fc_inc > 0:
        st.session_state["firecrawl_creditos_usados_sessao"] = (
            st.session_state.get("firecrawl_creditos_usados_sessao", 0) + fc_inc
        )
        dirty = True

    if dirty:
        invalidar_cache_saldos()


def _html_sidebar_metric_cards() -> str:
    um = st.session_state.get("ultima_metricas_llm") or {}
    lp, lc = um.get("prompt_tokens"), um.get("completion_tokens")
    tok_last = f"{lp if lp is not None else '—'} / {lc if lc is not None else '—'}"
    uctx = st.session_state.get("ultima_metricas_contexto_relatorio") or {}
    lpc, lcc = uctx.get("prompt_tokens"), uctx.get("completion_tokens")
    tok_ctx = f"{lpc if lpc is not None else '—'} / {lcc if lcc is not None else '—'}"
    tps = int(st.session_state.get("tokens_prompt_sessao", 0) or 0)
    tcs = int(st.session_state.get("tokens_completion_sessao", 0) or 0)
    fc_n = int(st.session_state.get("firecrawl_creditos_usados_sessao", 0) or 0)
    fc_txt = html.escape(buscar_saldo_firecrawl_cached())

    def card(lbl: str, val: str, span2: bool = False) -> str:
        sp = " leilao-span2" if span2 else ""
        return (
            f'<div class="leilao-sidebar-metric-card{sp}">'
            f'<div class="lbl">{html.escape(lbl)}</div>'
            f'<div class="val">{val}</div></div>'
        )

    parts = ['<div class="leilao-sidebar-metrics-grid">']
    parts.append(card("Tokens última (prompt / concl.)", html.escape(tok_last)))
    parts.append(card("Custo USD última", html.escape(_fmt_usd_sidebar(um.get("custo_usd_estimado")))))
    parts.append(card("Tokens análise relatório (últ.)", html.escape(tok_ctx)))
    parts.append(card("Custo USD análise relatório (últ.)", html.escape(_fmt_usd_sidebar(uctx.get("custo_usd_estimado")))))
    parts.append(card("Tokens sessão (prompt / concl.)", html.escape(f"{tps} / {tcs}")))
    parts.append(card("Custo USD sessão", html.escape(_fmt_usd_sidebar(st.session_state.get("custo_usd_sessao", 0.0)))))
    parts.append(card("Firecrawl usados (sessão)", html.escape(str(fc_n))))
    parts.append(card("Saldo Firecrawl (API)", fc_txt, span2=True))
    parts.append("</div>")
    return "".join(parts)


def _vj_build_df_leiloes(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["Cidade", "UF", "Nº caches", "URL (trecho)"])
    out: list[dict[str, Any]] = []
    for r in rows:
        cids = r.get("cache_media_bairro_ids") or []
        n = len(cids) if isinstance(cids, list) else 0
        u = str(r.get("url_leilao") or "")
        out.append(
            {
                "Cidade": (str(r.get("cidade") or "—"))[:28],
                "UF": (str(r.get("estado") or "—"))[:4],
                "Nº caches": n,
                "URL (trecho)": (u[:58] + "…") if len(u) > 58 else u,
            }
        )
    return pd.DataFrame(out)


def _vj_build_df_caches(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["Nome", "Cidade", "UF", "n", "id"])
    out = []
    for c in rows:
        out.append(
            {
                "Nome": (str(c.get("nome_cache") or "—"))[:44],
                "Cidade": (str(c.get("cidade") or "—"))[:22],
                "UF": (str(c.get("estado") or "—"))[:4],
                "n": c.get("n_amostras"),
                "id": str(c.get("id") or "")[:8] + "…" if c.get("id") else "—",
            }
        )
    return pd.DataFrame(out)


def _vj_label_leilao(r: dict[str, Any]) -> str:
    u_raw = str(r.get("url_leilao") or "")
    u = u_raw[:56] + ("…" if len(u_raw) > 56 else "")
    return f"{(str(r.get('cidade') or '—'))[:22]} · {(str(r.get('estado') or '—'))[:3]} — {u}"


def _vj_label_cache(c: dict[str, Any]) -> str:
    return (
        f"{(str(c.get('nome_cache') or '—'))[:40]} · {(str(c.get('cidade') or '—'))[:18]}"
        f" · n={(c.get('n_amostras') if c.get('n_amostras') is not None else '—')}"
    )


def _ad_df_anuncios(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Tabela resumida para a aba de anúncios."""
    if not rows:
        return pd.DataFrame()
    out: list[dict[str, Any]] = []
    for r in rows:
        lat, lon = r.get("latitude"), r.get("longitude")
        out.append(
            {
                "id": str(r.get("id") or "")[:8] + "…" if r.get("id") else "—",
                "bairro": (r.get("bairro") or "")[:40],
                "cidade": (r.get("cidade") or "")[:32],
                "UF": (r.get("estado") or "")[:2],
                "m²": r.get("area_construida_m2"),
                "R$": r.get("valor_venda"),
                "lat": lat,
                "lon": lon,
                "tipo": (r.get("tipo_imovel") or "")[:20],
                "url": (str(r.get("url_anuncio") or ""))[:56] + "…" if r.get("url_anuncio") else "",
            }
        )
    return pd.DataFrame(out)


def _ad_df_caches(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": str(r.get("id") or "")[:8] + "…" if r.get("id") else "—",
                "nome_cache": (str(r.get("nome_cache") or "—"))[:40],
                "chave_segmento": (str(r.get("chave_segmento") or ""))[:48],
                "bairro": (r.get("bairro") or "")[:28],
                "cidade": (r.get("cidade") or "")[:24],
                "n": r.get("n_amostras"),
                "R$/m²": r.get("preco_m2_medio"),
                "atualizado": str(r.get("atualizado_em") or "")[:19],
            }
        )
    return pd.DataFrame(out)


def _render_aba_anuncios() -> None:
    """
    Anúncios de mercado e ``cache_media_bairro``: listar, filtrar, editar, re-geocodificar, excluir.
    """
    st.caption(
        "Consulta e manutenção em **anuncios_mercado** e **cache_media_bairro** (Supabase). "
        "Ações destrutivas exigem confirmação."
    )
    t_ad, t_ca, t_lj = st.tabs(["Anúncios de mercado", "Cache de média", "Vínculo com leilões"])
    # ----- Anúncios -----
    with t_ad:
        try:
            cli = get_supabase_client()
        except Exception as e:
            st.error(f"Supabase indisponível: {e}")
            return
        c1, c2, c3, c4 = st.columns(4, gap="small")
        with c1:
            ad_est = st.text_input("UF (2 letras, opcional)", value="", key="ad_estado", max_chars=2)
        with c2:
            st.text_input("Cidade (contém)", value="", key="ad_cidade")
        with c3:
            st.text_input("Bairro (contém)", value="", key="ad_bairro")
        with c4:
            st.text_input("URL (contém)", value="", key="ad_url")
        c5, c6, c7, c8 = st.columns(4, gap="small")
        with c5:
            st.text_input("Tipo imóvel (ex.: apartamento)", value="", key="ad_tipo")
        with c6:
            st.selectbox("Transação", options=["(todas)", "venda", "aluguel"], key="ad_trans")
        with c7:
            st.checkbox("Só sem coordenadas (lat/lon nulos)", value=False, key="ad_semgeo")
        with c8:
            nlim = st.number_input("Máx. linhas", min_value=20, max_value=500, value=200, step=20, key="ad_limite")

        if st.button("Carregar anúncios", type="primary", key="ad_btn_carregar"):
            tr = st.session_state.get("ad_trans")
            trans = "" if tr == "(todas)" else str(tr or "")
            try:
                rows = anuncios_mercado_repo.listar_filtro_ui(
                    cli,
                    estado=str(ad_est or "").strip().upper()[:2],
                    cidade_contem=str(st.session_state.get("ad_cidade") or "").strip(),
                    bairro_contem=str(st.session_state.get("ad_bairro") or "").strip(),
                    tipo_imovel=str(st.session_state.get("ad_tipo") or "").strip(),
                    url_contem=str(st.session_state.get("ad_url") or "").strip(),
                    transacao=trans,
                    sem_coordenadas=bool(st.session_state.get("ad_semgeo")),
                    limite=int(nlim or 200),
                )
                st.session_state["ad_ads_rows"] = rows
            except Exception as e:
                st.session_state["ad_ads_rows"] = []
                st.error(f"Falha ao listar: {e}")

        rows = st.session_state.get("ad_ads_rows") or []
        if not rows:
            st.info("Ajuste filtros e clique em **Carregar anúncios**.")
        else:
            st.caption(f"**{len(rows)}** registro(s) (limite do carregamento).")
            dfa = _ad_df_anuncios(rows)
            dfa_key = f"ad_df_ads_{len(rows)}"
            h = min(480, max(200, 48 + min(len(rows), 25) * 36))
            ev = st.dataframe(
                dfa,
                width="stretch",
                height=h,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
                key=dfa_key,
            )
            r_ix: list[int] = []
            if isinstance(ev, dict):
                r_ix = list((ev.get("selection") or {}).get("rows") or [])
            elif hasattr(ev, "selection") and ev.selection is not None:
                r_ix = list(getattr(ev.selection, "rows", None) or [])
            sel: list[dict[str, Any]] = [rows[i] for i in r_ix if 0 <= i < len(rows)]
            st.caption(f"Selecionados na tabela: **{len(sel)}** (use para geocodificar ou excluir).")

            st.text_input(
                "Endereço para geocodificar (opcional)",
                value="",
                key="ad_geo_endereco_livre",
                placeholder="Ex.: Rua das Flores, 120 — Moema, São Paulo, SP",
                help=(
                    "Se preencher, este texto é enviado ao Nominatim (texto livre) e o mesmo par lat/lon "
                    "é gravado em **todos** os anúncios selecionados. Deixe vazio para geocodificar **cada** "
                    "linha com logradouro, bairro, cidade e UF já guardados no banco (comportamento anterior)."
                ),
            )
            j1, j2, j3, j4 = st.columns(4, gap="small")
            with j1:
                if st.button("Re-geocodificar selecionados (Nominatim)", key="ad_btn_geo", disabled=not sel):
                    override = str(st.session_state.get("ad_geo_endereco_livre") or "").strip()
                    if override:
                        c = geocodificar_texto_livre(override)
                        if c:
                            for b in sel:
                                anuncios_mercado_repo.atualizar_geolocalizacao(
                                    cli, str(b["id"]), c[0], c[1]
                                )
                            st.success(
                                f"**1** ponto geográfico (texto livre) aplicado a **{len(sel)}** anúncio(s)."
                            )
                        else:
                            st.warning(
                                "Não foi possível obter coordenadas para o endereço informado. "
                                "Ajuste o texto, inclua cidade e UF, ou deixe o campo vazio e tente de novo com os dados do banco."
                            )
                    else:
                        tot = 0
                        prog = st.progress(0)
                        ntot = max(len(sel), 1)
                        for n, b in enumerate(sel, start=1):
                            c = geocodificar_endereco(
                                logradouro=str(b.get("logradouro") or ""),
                                bairro=str(b.get("bairro") or ""),
                                cidade=str(b.get("cidade") or ""),
                                estado=str(b.get("estado") or ""),
                            )
                            if c:
                                anuncios_mercado_repo.atualizar_geolocalizacao(
                                    cli, str(b["id"]), c[0], c[1]
                                )
                                tot += 1
                            prog.progress(min(1.0, n / ntot))
                        prog.empty()
                        st.success(f"Coordenadas gravadas: **{tot}** / {len(sel)}.")
            with j2:
                c_del = st.checkbox("Confirmo exclusão permanente (selecionados)", key="ad_conf_del", value=False)
                if st.button("Excluir selecionados", type="primary", key="ad_btn_del", disabled=not (sel and c_del)):
                    for b in sel:
                        anuncios_mercado_repo.apagar_por_id(cli, str(b.get("id")))
                    st.session_state["ad_ads_rows"] = []
                    st.rerun()
            with j3:
                if rows:
                    csv_bytes = dfa.to_csv(index=False).encode("utf-8")
                    st.download_button("CSV (tabela exibida)", data=csv_bytes, file_name="anuncios_mercado.csv", mime="text/csv", key="ad_dl")
            with j4:
                st.empty()

            st.markdown("##### Criar cache de média a partir da seleção")
            st.caption(
                "Usa os anúncios marcados na tabela. É necessário **lat/lon** em cada amostra (re-geocodifique se faltar). "
                "Cidade/bairro/UF do cache seguem o **primeiro** anúncio válido; o ponto de referência é o **centróide** das coordenadas."
            )
            cn1, cn2 = st.columns((2, 1), gap="medium")
            with cn1:
                nome_cache_novo = st.text_input(
                    "Nome do cache",
                    value="",
                    key="ad_nome_cache_novo",
                    placeholder="Ex.: Ref. manual — Moema 80–120 m²",
                    help="Aparece em nome_cache e na listagem de caches.",
                )
            with cn2:
                st.write("")
                st.write("")
                criar_cache_ok = st.button(
                    "Criar cache com selecionados",
                    type="primary",
                    key="ad_btn_criar_cache",
                    disabled=not sel,
                    help="Grava uma linha em cache_media_bairro com as médias da amostra.",
                )
            if criar_cache_ok:
                ok_cc, msg_cc, new_cid = criar_cache_manual_de_anuncios(
                    cli,
                    sel,
                    str(st.session_state.get("ad_nome_cache_novo") or nome_cache_novo or "").strip(),
                )
                if ok_cc:
                    st.success(f"{msg_cc} **ID:** `{new_cid}` — pode vincular no leilão na aba **Vínculo com leilões** ou reabrir em **Cache de média**.")
                else:
                    st.error(msg_cc)

            st.markdown("##### Editar um anúncio (por id)")
            pick = st.selectbox(
                "Registro (mesmo lote carregado)",
                options=range(len(rows)),
                format_func=lambda i: f"{(rows[i].get('bairro') or '—')[:30]} | id={str(rows[i].get('id'))[:8]}…",
                key="ad_pick_edit",
            )
            b0 = rows[int(pick)]
            _eid = str(b0.get("id") or "")[:12]
            with st.form("ad_form_edit", clear_on_submit=False):
                f_log = st.text_input("Logradouro", value=str(b0.get("logradouro") or ""), key=f"ad_f_log_{_eid}")
                f_bai = st.text_input("Bairro", value=str(b0.get("bairro") or ""), key=f"ad_f_bai_{_eid}")
                f_cid = st.text_input("Cidade", value=str(b0.get("cidade") or ""), key=f"ad_f_cid_{_eid}")
                f_uf = st.text_input("UF", value=str(b0.get("estado") or ""), key=f"ad_f_uf_{_eid}", max_chars=2)
                a1, a2, a3 = st.columns(3)
                with a1:
                    f_area = st.number_input("Área m²", value=float(b0.get("area_construida_m2") or 0), min_value=0.1, key=f"ad_f_area_{_eid}")
                with a2:
                    f_val = st.number_input("Valor venda (R$)", value=float(b0.get("valor_venda") or 0), min_value=0.01, key=f"ad_f_val_{_eid}")
                with a3:
                    f_lat = st.number_input(
                        "Latitude (manual)",
                        value=float(b0["latitude"]) if b0.get("latitude") is not None else 0.0,
                        format="%.6f",
                        key=f"ad_f_lat_{_eid}",
                    )
                a4, a5 = st.columns(2)
                with a4:
                    f_lon = st.number_input(
                        "Longitude (manual)",
                        value=float(b0["longitude"]) if b0.get("longitude") is not None else 0.0,
                        format="%.6f",
                        key=f"ad_f_lon_{_eid}",
                    )
                with a5:
                    f_qua = st.number_input("Quartos", value=int(b0.get("quartos") or 0), min_value=0, key=f"ad_f_q_{_eid}")
                inc_geo = st.checkbox("Atualizar também lat/lon", value=True, key=f"ad_f_incgeo_{_eid}")
                sub = st.form_submit_button("Gravar alterações", type="primary")
                if sub:
                    campos = {
                        "logradouro": f_log.strip(),
                        "bairro": f_bai.strip(),
                        "cidade": f_cid.strip(),
                        "estado": f_uf.strip().upper()[:2] if f_uf.strip() else f_uf.strip(),
                        "area_construida_m2": f_area,
                        "valor_venda": f_val,
                        "quartos": f_qua,
                    }
                    if inc_geo:
                        campos["latitude"] = f_lat
                        campos["longitude"] = f_lon
                    try:
                        anuncios_mercado_repo.atualizar_campos(cli, str(b0["id"]), campos)
                        st.success("Anúncio atualizado. Use **Carregar anúncios** de novo se precisar refrescar a tabela.")
                    except Exception as e:
                        st.error(str(e))

    # ----- Caches -----
    with t_ca:
        try:
            cli = get_supabase_client()
        except Exception as e:
            st.error(f"Supabase indisponível: {e}")
            return
        g1, g2, g3, g4 = st.columns(4, gap="small")
        with g1:
            st.text_input("UF (2 letras)", value="", key="ca_estado", max_chars=2)
        with g2:
            st.text_input("Cidade (contém)", value="", key="ca_cidade")
        with g3:
            st.text_input("Bairro (contém)", value="", key="ca_bairro")
        with g4:
            nlim_c = st.number_input("Máx. linhas", min_value=20, max_value=400, value=200, step=20, key="ca_limite")
        g5, g6, g7 = st.columns(3, gap="small")
        with g5:
            st.text_input("geo_bucket (contém)", value="", key="ca_geo")
        with g6:
            st.text_input("chave_segmento (contém)", value="", key="ca_seg")
        with g7:
            st.text_input("chave_bairro (contém)", value="", key="ca_chb")
        if st.button("Carregar caches", type="primary", key="ca_btn_carregar"):
            try:
                cr = cache_media_bairro_repo.listar_filtro_ui(
                    cli,
                    estado=str(st.session_state.get("ca_estado") or "").strip().upper()[:2],
                    cidade_contem=str(st.session_state.get("ca_cidade") or "").strip(),
                    bairro_contem=str(st.session_state.get("ca_bairro") or "").strip(),
                    geo_bucket=str(st.session_state.get("ca_geo") or "").strip(),
                    chave_segmento_contem=str(st.session_state.get("ca_seg") or "").strip(),
                    chave_bairro_contem=str(st.session_state.get("ca_chb") or "").strip(),
                    limite=int(nlim_c or 200),
                )
                st.session_state["ad_cache_rows"] = cr
            except Exception as e:
                st.session_state["ad_cache_rows"] = []
                st.error(f"Falha ao listar caches: {e}")
        c_rows = st.session_state.get("ad_cache_rows") or []
        if not c_rows:
            st.info("Ajuste filtros e clique em **Carregar caches**.")
        else:
            st.caption(f"**{len(c_rows)}** registro(s). `chave_segmento` é única — apagar recria outra no próximo cálculo.")
            dfc = _ad_df_caches(c_rows)
            evc = st.dataframe(
                dfc,
                width="stretch",
                height=min(480, 200 + 32 * min(len(c_rows), 20)),
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
                key=f"ad_df_cache_{len(c_rows)}",
            )
            c_ix: list[int] = []
            if isinstance(evc, dict):
                c_ix = list((evc.get("selection") or {}).get("rows") or [])
            elif hasattr(evc, "selection") and evc.selection is not None:
                c_ix = list(getattr(evc.selection, "rows", None) or [])
            c_sel = [c_rows[i] for i in c_ix if 0 <= i < len(c_rows)]
            st.caption(f"Selecionados: **{len(c_sel)}**")
            ccf = st.checkbox("Confirmo excluir caches selecionados (Supabase)", value=False, key="ca_conf_del")
            if st.button("Excluir caches selecionados", type="primary", disabled=not (c_sel and ccf), key="ca_btn_del"):
                for c in c_sel:
                    cache_media_bairro_repo.apagar_por_id(cli, str(c.get("id")))
                st.session_state["ad_cache_rows"] = []
                st.rerun()
            st.download_button("CSV (tabela exibida)", dfc.to_csv(index=False).encode("utf-8"), file_name="cache_media_bairro.csv", mime="text/csv", key="ca_dl")

    # ----- Leilão: vínculo -----
    with t_lj:
        st.caption(
            "Liga imóveis de leilão a linhas de **cache de média** (`cache_media_bairro_ids`). "
            "Alterações disparam o **recálculo da simulação** (lucro, ROI) em `operacao_simulacao_json` — "
            "não regera o texto do relatório de mercado (LLM)."
        )
        st.info(
            "**Por leilão** — escolhe o imóvel e adiciona ou remove caches (fluxo principal).  "
            "**Por cache** — consulta em quantos leilões um cache entra.  "
            "Os dados vêm do Supabase sempre que esta aba é carregada.",
            icon="ℹ️",
        )
        try:
            cli2 = get_supabase_client()
        except Exception as e:
            st.error(f"Supabase: {e}")
            return
        try:
            all_lj: list[dict[str, Any]] = leilao_imoveis_repo.listar_para_vinculo_cache(cli2, limite=500)
        except Exception as e:
            st.error(f"Falha ao listar leilões: {e}")
            return
        try:
            all_ca: list[dict[str, Any]] = cache_media_bairro_repo.listar_resumo_vinculo(cli2, limite=800)
        except Exception as e:
            st.error(f"Falha ao listar caches: {e}")
            return
        if not all_lj and not all_ca:
            st.info("Ainda não há leilões nem caches para vincular.")
            return
        by_cache_id: dict[str, list[str]] = {}
        for lj in all_lj:
            lid = str(lj.get("id") or "")
            if not lid:
                continue
            for cid in list(lj.get("cache_media_bairro_ids") or []):
                s = str(cid).strip()
                if not s:
                    continue
                by_cache_id.setdefault(s, []).append(lid)
        c_by: dict[str, dict[str, Any]] = {str(c.get("id") or ""): c for c in all_ca if c.get("id")}
        lj_by: dict[str, dict[str, Any]] = {str(x.get("id") or ""): x for x in all_lj if x.get("id")}

        tab_por_lj, tab_por_c, tab_help = st.tabs(["Trabalhar com um leilão", "Consultar um cache", "Ajuda"])

        with tab_por_lj:
            if not all_lj:
                st.warning("Não há leilões no banco para este painel.")
            else:
                leilao_id_opts = [str(x.get("id")) for x in all_lj if x.get("id")]
                leil_id = st.selectbox(
                    "1 — Imóvel de leilão",
                    options=leilao_id_opts,
                    format_func=lambda i: _vj_label_leilao(lj_by.get(i, {})),
                    key="vj_sel_leilao_id",
                )
                row_lj = lj_by.get(str(leil_id))
                id_ord: list[str] = []
                if row_lj and isinstance(row_lj.get("cache_media_bairro_ids"), list):
                    id_ord = [str(x).strip() for x in row_lj["cache_media_bairro_ids"] if str(x).strip()]
                cur_ids: set[str] = set(id_ord)
                linked_rows: list[dict[str, Any]] = [c_by[i] for i in id_ord if i in c_by]
                m1, m2 = st.columns(2)
                with m1:
                    st.metric("Caches vinculados a este leilão", len(id_ord))
                with m2:
                    st.metric("Caches cadastrados (sistema)", len(all_ca))
                st.caption("A **ordem** da lista de IDs no banco importa na análise; novos acréscimos vão em geral **para o fim**.")
                st.markdown("**2 — Caches já ligados** (só leitura; para editar use as secções abaixo)")
                if not linked_rows:
                    st.info("Ainda **não há** cache vinculado. Use a secção abaixo para adicionar.")
                else:
                    df_c_link = _vj_build_df_caches(linked_rows)
                    st.dataframe(df_c_link, width="stretch", height=min(360, 120 + 36 * min(len(linked_rows), 12)), hide_index=True)
                st.divider()
                st.markdown("**Adicionar** outro cache a este leilão")
                cands = [c for c in all_ca if str(c.get("id") or "") not in cur_ids]
                add_ids = [str(c.get("id")) for c in cands if c.get("id")]
                c_add = st.selectbox(
                    "Cache disponível (ainda não ligado a este leilão)",
                    options=[""] + add_ids,
                    format_func=lambda x: (
                        "— escolher um cache na lista —" if not x else _vj_label_cache(c_by.get(x, {}))
                    ),
                    key="vj_add_cache_in",
                )
                if st.button("Vincular cache acima a este leilão", type="primary", key="vj_btn_in", disabled=not bool(c_add)):
                    leilao_imoveis_repo.anexar_cache_media_bairro_ids(str(leil_id), [str(c_add)], cli2)
                    ok_r, msg_r = _refazer_calculo_simulacao_leilao(cli2, str(leil_id))
                    if ok_r:
                        st.success("Vínculo criado. " + msg_r)
                    else:
                        st.warning("Vínculo guardado, mas " + msg_r)
                    st.rerun()
                st.divider()
                st.markdown("**Remover** um cache deste leilão (não apaga a linha em `cache_media_bairro`)")
                rem_ids = [i for i in id_ord if i in c_by]
                c_rem = st.selectbox(
                    "Qual cache deseja desvincular?",
                    options=[""] + rem_ids,
                    format_func=lambda x: (
                        "— nenhum para remover —" if not x else _vj_label_cache(c_by.get(x, {}))
                    ),
                    key="vj_out_cache",
                    disabled=not rem_ids,
                )
                if st.button("Desvincular o cache selecionado", key="vj_btn_out", disabled=not bool(c_rem), type="secondary"):
                    leilao_imoveis_repo.remover_cache_media_bairro_id(str(leil_id), str(c_rem), cli2)
                    ok_r, msg_r = _refazer_calculo_simulacao_leilao(cli2, str(leil_id))
                    if ok_r:
                        st.success("Vínculo removido. " + msg_r)
                    else:
                        st.warning("Remoção guardada, mas " + msg_r)
                    st.rerun()
                st.divider()
                st.markdown("**Zerar tudo** neste leilão")
                st.caption("Remove todos os IDs de cache deste imóvel. A simulação recalcula com zero caches vinculados.")
                c_z = st.checkbox("Confirmo remover todos os vínculos de cache deste leilão", key="vj_z_all")
                if st.button("Zerar todos os vínculos de cache", type="secondary", key="vj_btn_zero", disabled=not c_z):
                    leilao_imoveis_repo.definir_cache_media_bairro_ids(str(leil_id), [], cli2)
                    ok_r, msg_r = _refazer_calculo_simulacao_leilao(cli2, str(leil_id))
                    if ok_r:
                        st.success("Lista de caches limpa. " + msg_r)
                    else:
                        st.warning("Lista limpa, mas " + msg_r)
                    st.rerun()

        with tab_por_c:
            st.markdown("##### Onde um cache entra?")
            st.caption("Útil para ver duplicatas ou reutilização entre análises. Para alterar vínculos, use a aba **Trabalhar com um leilão**.")
            if not all_ca:
                st.info("Ainda não há linhas em `cache_media_bairro`.")
            else:
                cache_id_opts = [str(c.get("id")) for c in all_ca if c.get("id")]
                c_pick = st.selectbox(
                    "Cache de média",
                    options=cache_id_opts,
                    format_func=lambda i: _vj_label_cache(c_by.get(i, {})),
                    key="vj_sel_cache_only",
                )
                c_pick = str(c_pick or "")
                ref_lj = by_cache_id.get(c_pick) or []
                st.metric("Leilões com este cache", len(ref_lj))
                if not ref_lj:
                    st.info("Nenhum leilão referencia este cache neste conjunto de dados (ou lista desatualizada; recarregue a aba do modo **Anúncios e caches**).")
                else:
                    ex_rows = [lj_by[uid] for uid in ref_lj if uid in lj_by]
                    df_lx = _vj_build_df_leiloes(ex_rows)
                    st.dataframe(
                        df_lx,
                        width="stretch",
                        height=min(380, 120 + 34 * min(len(ex_rows), 10)),
                        hide_index=True,
                    )

        with tab_help:
            st.markdown(
                """
**O que é o vínculo?** Cada leilão guarda no Supabase um array `cache_media_bairro_ids` (UUIDs
de `cache_media_bairro`). A simulação (aba *Simulação*) usa esses caches para preço m², médias, etc.

**O que acontece ao vincular ou desvincular?** O sistema reexecuta o cálculo com os *mesmos* parâmetros
já guardados em `operacao_simulacao_json` e grava o resultado de novo. Assim, lucro e ROI refletem a nova amostra.

**O que *não* muda?** O relatório de análise de mercado (texto/cards) continua a ser o que estava; para o
atualizar seria outro passo (agente/LLM).

**Dica:** A ordem dos IDs no leilão importa na UI de análise; ao adicionar, o cache novo passa a entrar
no fim do array, salvo ajuste manual noutro ecrã.
"""
            )


def _ignorar_cache_firecrawl_sidebar() -> bool:
    return bool(st.session_state.get("sidebar_ingest_ignorar_cache_firecrawl"))


def _aplicar_snapshot_apos_ingestao_ok(r: Any) -> None:
    m = r.metricas_llm or {}
    st.session_state.snapshot = {
        "url": r.url_leilao,
        "status": r.modo,
        "id": r.id,
        "modelo": m.get("modelo"),
        "tokens": f"{m.get('prompt_tokens')} / {m.get('completion_tokens')}",
        "nota": (r.log or "")[:400],
    }
    cli = get_supabase_client()
    row_up = leilao_imoveis_repo.buscar_por_url_leilao(normalizar_url_leilao(r.url_leilao), cli)
    st.session_state["ultimo_extracao"] = row_up
    _acumular_metricas_sidebar(
        m,
        row_up if isinstance(row_up, dict) else None,
        firecrawl_chamadas_api_ingestao=r.firecrawl_chamadas_api_total,
    )


def _render_bloco_duplicata_ingestao() -> None:
    st.warning("Esta URL já existe no banco. Escolha como proceder.")
    st.json(st.session_state.pending_duplicate_registro)
    ign = _ignorar_cache_firecrawl_sidebar()
    c1, c2 = st.columns(2)
    cli = get_supabase_client()
    with c1:
        if st.button("Sobrescrever registro", type="primary", key="dup_ingest_overwrite"):
            r = executar_ingestao_edital(
                st.session_state.pending_duplicate_url,
                cli,
                sobrescrever_duplicata=True,
                ignorar_cache_firecrawl=ign,
            )
            st.session_state.pending_duplicate_url = None
            st.session_state.pending_duplicate_registro = None
            _aplicar_snapshot_apos_ingestao_ok(r)
            st.session_state["assistente_modo"] = "ingestao"
            st.session_state.pop("_dash_rows", None)
            st.success("Atualizado.")
            st.rerun()
    with c2:
        if st.button("Manter registro atual", key="dup_ingest_keep"):
            url_dup = st.session_state.pending_duplicate_url
            r = executar_ingestao_edital(
                url_dup,
                cli,
                sobrescrever_duplicata=False,
                ignorar_cache_firecrawl=ign,
            )
            st.session_state.pending_duplicate_url = None
            st.session_state.pending_duplicate_registro = None
            m = r.metricas_llm or {}
            st.session_state.snapshot = {
                "url": r.url_leilao,
                "status": r.modo,
                "nota": r.log or "",
            }
            row_kept = leilao_imoveis_repo.buscar_por_url_leilao(
                normalizar_url_leilao(url_dup), cli
            )
            st.session_state["ultimo_extracao"] = row_kept
            _acumular_metricas_sidebar(
                m,
                row_kept if isinstance(row_kept, dict) else None,
                firecrawl_chamadas_api_ingestao=r.firecrawl_chamadas_api_total,
            )
            st.session_state["assistente_modo"] = "ingestao"
            st.info("Sem alterações no banco.")
            st.rerun()


def _executar_ingestao_url_sidebar() -> None:
    url = (st.session_state.get("sidebar_ingest_url") or "").strip()
    if not url:
        st.sidebar.error("Cole a URL do leilão.")
        return
    ign = _ignorar_cache_firecrawl_sidebar()
    cli = get_supabase_client()
    try:
        with st.spinner("Processando edital…"):
            r = executar_ingestao_edital(
                url, cli, sobrescrever_duplicata=None, ignorar_cache_firecrawl=ign
            )
    except EscolhaSobreDuplicataNecessaria as dup:
        st.session_state.pending_duplicate_url = url
        st.session_state.pending_duplicate_registro = dup.registro_existente
        st.rerun()
        return
    except IngestaoSemConteudoEditalError as e:
        st.session_state["ultimo_extracao"] = None
        st.session_state.snapshot = {
            "url": url,
            "status": "sem_conteudo",
            "nota": e.motivo,
        }
        st.session_state["assistente_modo"] = "ingestao"
        st.session_state.pop("_dash_rows", None)
        st.sidebar.warning("Nada foi gravado — conteúdo insuficiente para edital.")
        st.rerun()
        return
    except UrlInvalidaIngestaoError as e:
        st.session_state["ultimo_extracao"] = None
        st.session_state.snapshot = {"url": url, "status": "url_invalida", "nota": str(e)}
        st.session_state["assistente_modo"] = "ingestao"
        st.session_state.pop("_dash_rows", None)
        st.sidebar.error("URL inválida ou página indisponível.")
        st.rerun()
        return
    _aplicar_snapshot_apos_ingestao_ok(r)
    st.session_state["assistente_modo"] = "ingestao"
    st.session_state.pop("_dash_rows", None)
    st.sidebar.success("Ingestão concluída — aba Leilão.")
    st.rerun()


def _render_sidebar_marca() -> None:
    st.sidebar.markdown(
        '<div class="lnav-brand-wrap"><p class="lnav-brand-t">Leilão IA</p></div>',
        unsafe_allow_html=True,
    )


def _render_sidebar_navegacao_modos() -> None:
    modo = str(st.session_state.get("assistente_modo") or "inicio")
    nav: tuple[tuple[str, str], ...] = (
        ("inicio", "📊  Painel"),
        ("ingestao", "🏛️  Leilão"),
        ("simulacao", "📐  Simulador"),
        ("anuncios", "🗄️  Dados"),
    )
    for mid, label in nav:
        active = modo == mid
        if st.sidebar.button(
            label,
            key=f"lnav_modo_{mid}",
            use_container_width=True,
            type="primary" if active else "secondary",
        ):
            st.session_state["assistente_modo"] = mid
            st.rerun()


def _render_sidebar_ingest_url() -> None:
    st.sidebar.text_input(
        "URL do leilão",
        placeholder="https://…",
        key="sidebar_ingest_url",
    )
    st.sidebar.checkbox(
        "Ignorar cache em disco do Firecrawl",
        key="sidebar_ingest_ignorar_cache_firecrawl",
        value=False,
    )
    if st.sidebar.button("▶  Processar URL", type="primary", use_container_width=True, key="sidebar_ingest_run"):
        _executar_ingestao_url_sidebar()


_BM_APPLY_DEFAULTS_FLAG = "_bm_apply_defaults"


def _render_sidebar_ajustes_busca() -> None:
    defaults = defaults_chaves_busca_mercado_session()
    if st.session_state.pop(_BM_APPLY_DEFAULTS_FLAG, False):
        for rk, rv in defaults.items():
            st.session_state[rk] = rv
    else:
        for k, v in defaults.items():
            st.session_state.setdefault(k, v)
    with st.sidebar.expander("⚙️ Ajustes de busca", expanded=False):
        st.number_input(
            "Metragem mínima (% da ref.)",
            min_value=30,
            max_value=120,
            step=1,
            key="bm_area_pct_min",
            help="Limite inferior na URL = área do edital × este % ÷ 100 (ex.: 100 m² e 65% → ~65 m²).",
        )
        st.number_input(
            "Metragem máxima (% da ref.)",
            min_value=80,
            max_value=350,
            step=1,
            key="bm_area_pct_max",
            help="Limite superior na URL (ex.: 145% de 100 m² → ~145 m²).",
        )
        st.number_input(
            "Raio (km)",
            min_value=0.5,
            max_value=80.0,
            step=0.5,
            format="%.1f",
            key="bm_raio_km",
            help="Anúncios mais longe do imóvel do leilão são ignorados ao montar o cache de média.",
        )
        st.number_input(
            "Mín. amostras (cache)",
            min_value=1,
            max_value=25,
            step=1,
            key="bm_min_amostras_cache",
            help="Mínimo de comparáveis válidos (raio + faixa de área) para aceitar/reutilizar cache.",
        )
        st.number_input(
            "Máx. anúncios (cache principal)",
            min_value=1,
            max_value=50,
            step=1,
            key="bm_cache_max_principal",
            help="Teto de anúncios no registo de cache “principal” (simulação). O excedente reparte-se em caches de lote de referência.",
        )
        st.number_input(
            "Máx. anúncios (lote de referência)",
            min_value=1,
            max_value=50,
            step=1,
            key="bm_cache_max_lote",
            help="Tamanho de cada lote de anúncios em caches de referência (e terrenos em partes).",
        )
        st.number_input(
            "Máx. créditos Firecrawl por análise",
            min_value=1,
            max_value=50,
            step=1,
            key="bm_max_firecrawl_creditos",
            help="Teto de chamadas API (1 search + N scrapes) numa mesma ingestão: edital, comparáveis e montagem de cache partilham este saldo.",
        )
        st.checkbox(
            "Confirmar frase (Firecrawl Search)",
            key="bm_confirmar_frase_fc_search",
            help="Se ativo, a frase de busca na web é mostrada para confirmar ou editar antes de pesquisar (ingestão e recálculo de cache).",
        )
        if st.button("Repor padrões", key="bm_reset_defaults", use_container_width=True):
            st.session_state[_BM_APPLY_DEFAULTS_FLAG] = True
            st.rerun()


def _render_sidebar_app() -> None:
    _render_sidebar_marca()
    st.sidebar.markdown('<div class="lnav-sep" aria-hidden="true"></div>', unsafe_allow_html=True)
    _render_sidebar_navegacao_modos()
    st.sidebar.markdown('<div class="lnav-sep" aria-hidden="true"></div>', unsafe_allow_html=True)
    _render_sidebar_ingest_url()
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Uso e saldos")
    st.sidebar.markdown(_html_sidebar_metric_cards(), unsafe_allow_html=True)
    if st.sidebar.button("↻ Atualizar saldo Firecrawl", key="btn_atualizar_saldos", use_container_width=True):
        invalidar_cache_saldos()
        st.rerun()
    st.sidebar.markdown("### Comparáveis")
    _render_sidebar_ajustes_busca()


def _dash_txt_card_resumo_local(x: Any) -> str:
    """Texto do botão nas oportunidades: estado, cidade, bairro e tipo (ex.: apartamento, casa)."""
    uf = (getattr(x, "estado", None) or "").strip() or "—"
    cid = (getattr(x, "cidade", None) or "").strip() or "—"
    bai = (getattr(x, "bairro", None) or "").strip() or "—"
    tip = (getattr(x, "tipo_imovel", None) or "").strip() or "—"
    return f"{uf}  ·  {cid}\n{bai}  ·  {tip}"


def _dash_linha_oportunidade_com_foto(x: Any, *, texto_botao: str, st_key: str, modo_aba: str) -> None:
    """Uma linha de oportunidade: miniatura (``url_foto_imovel``) + botão de navegação."""
    c_img, c_txt = st.columns([0.2, 0.8], gap="small")
    with c_img:
        u = getattr(x, "url_foto_imovel", None)
        if u:
            uq = html.escape(str(u), quote=True)
            st.markdown(
                f'<div class="dash-op-thumb-wrap"><img src="{uq}" alt="" loading="lazy" '
                f'referrerpolicy="no-referrer" width="72" height="72" '
                "style=\"display:block;width:72px;height:72px;object-fit:cover;border-radius:10px;"
                'border:1px solid rgba(255,255,255,0.08);\" /></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="dash-op-thumb-ph" title="Sem foto no edital"></div>',
                unsafe_allow_html=True,
            )
    with c_txt:
        if st.button(texto_botao, key=st_key, use_container_width=True, type="secondary"):
            _abrir_leilao_e_mudar_aba(x.id, modo_aba)


def _abrir_leilao_e_mudar_aba(iid: str, modo: str) -> None:
    """Carrega o leilão e navega para ``ingestao`` (análise) ou ``simulacao`` (após o próximo run)."""
    m = (modo or "ingestao").strip()
    if m not in _MODOS_VALIDOS or m in ("inicio", "anuncios"):
        m = "ingestao"
    cli = get_supabase_client()
    row = leilao_imoveis_repo.buscar_por_id(iid, cli)
    if not row:
        st.error("Registro não encontrado.")
        return
    st.session_state["ultimo_extracao"] = row
    st.session_state[_PENDING_MODO] = m
    st.rerun()


_DASH_NOMES_MES: tuple[str, ...] = (
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
)


def _render_calendario_interativo_dash(d: Any) -> None:
    """Calendário com dias em bolinhas: alterna `st.session_state['_dash_dia_filtro']` (iso ou None)."""
    from calendar import monthcalendar
    from datetime import date

    y, m0 = st.session_state.get("_dash_cal_ym", (d.agora.year, d.agora.month))
    y = int(max(2018, min(2040, y)))
    m0 = int(max(1, min(12, m0)))
    st.session_state["_dash_cal_ym"] = (y, m0)

    c_prev, c_tit, c_next = st.columns([1, 4, 1])
    with c_prev:
        if st.button("‹", key="dash_cal_prev", use_container_width=False, type="tertiary"):
            if m0 == 1:
                y, m0 = y - 1, 12
            else:
                m0 -= 1
            st.session_state["_dash_cal_ym"] = (y, m0)
            st.rerun()
    with c_tit:
        st.markdown(
            f"<p style='text-align:center;margin:0.15rem 0 0.35rem 0;font-size:0.95rem;font-weight:650'>{_DASH_NOMES_MES[m0 - 1]} {y}</p>",
            unsafe_allow_html=True,
        )
    with c_next:
        if st.button("›", key="dash_cal_next", use_container_width=False, type="tertiary"):
            if m0 == 12:
                y, m0 = y + 1, 1
            else:
                m0 += 1
            st.session_state["_dash_cal_ym"] = (y, m0)
            st.rerun()

    if st.session_state.get("_dash_dia_filtro"):
        c_lim_a, c_lim_b, c_lim_c = st.columns([2, 1, 2])
        with c_lim_b:
            if st.button("✕", key="dash_cal_ver_todos", use_container_width=False, type="tertiary"):
                st.session_state.pop("_dash_dia_filtro", None)
                st.rerun()

    semana_dias = "Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"
    hd = st.columns(7)
    for hi, wn in enumerate(semana_dias):
        with hd[hi]:
            st.caption(wn)

    for wk in monthcalendar(y, m0):
        linha = st.columns(7)
        for di, dnum in enumerate(wk):
            with linha[di]:
                if dnum == 0:
                    st.empty()
                    continue
                try:
                    dt = date(y, m0, dnum)
                except ValueError:
                    st.empty()
                    continue
                k = dt.isoformat()
                tem_leilao = bool(d.calendario.get(k, []))
                if tem_leilao:
                    selecionado = st.session_state.get("_dash_dia_filtro") == k
                    if st.button(
                        str(dnum),
                        key=f"dash_cald_{k}",
                        use_container_width=False,
                        type="primary" if selecionado else "tertiary",
                    ):
                        cur = st.session_state.get("_dash_dia_filtro")
                        st.session_state["_dash_dia_filtro"] = None if cur == k else k
                        st.rerun()
                else:
                    st.markdown(
                        f'<p style="text-align:center;color:#64748b;font-size:0.78rem;font-weight:500;'
                        f'margin:0.35rem 0 0.5rem 0;padding:0;">{dnum}</p>',
                        unsafe_allow_html=True,
                    )

def _render_painel_inicial() -> None:
    from leilao_ia_v2.ui.dashboard_comparacao_modais import CSS_DASH
    from leilao_ia_v2.ui.dashboard_inicio import (
        agregar_listas_por_dia,
        CSS_STREAMLIT_PAINEL_INICIO,
        processar_rows_dashboard,
    )

    try:
        cli = get_supabase_client()
    except Exception as e:
        st.error(f"Supabase indisponível: {e}")
        return

    if st.button(
        "Atualizar dados",
        key="dash_btn_refresh",
        use_container_width=True,
    ):
        st.session_state.pop("_dash_rows", None)
        st.session_state.pop("_dash_dia_filtro", None)
        st.session_state.pop("_dash_cal_ym", None)
        st.rerun()

    rows = st.session_state.get("_dash_rows")
    if rows is None:
        try:
            with st.spinner("Carregando leilões…"):
                rows = leilao_imoveis_repo.listar_para_dashboard(cli, limite=400)
            st.session_state["_dash_rows"] = rows
        except Exception as e:
            st.error(f"Falha ao listar: {e}")
            return

    d = processar_rows_dashboard(rows)
    st.session_state.setdefault("_dash_dia_filtro", None)
    if "_dash_cal_ym" not in st.session_state:
        st.session_state["_dash_cal_ym"] = (d.agora.year, d.agora.month)

    st.markdown(CSS_DASH, unsafe_allow_html=True)
    st.markdown(CSS_STREAMLIT_PAINEL_INICIO, unsafe_allow_html=True)
    st.markdown(d.to_html_kpis_sozinho(), unsafe_allow_html=True)
    col_cal, col_lmb = st.columns(2, gap="large")
    with col_cal:
        with st.container(border=True):
            st.markdown(
                '<span class="dc-badge" style="display:block;margin:0.1rem 0 0.5rem 0.05rem">'
                "Calendário — próxima praça</span>",
                unsafe_allow_html=True,
            )
            _render_calendario_interativo_dash(d)
    with col_lmb:
        st.markdown(d.to_html_lembretes_secao(), unsafe_allow_html=True)

    filtro_iso = st.session_state.get("_dash_dia_filtro")
    if filtro_iso:
        d_alvo = date.fromisoformat(str(filtro_iso))
        prox_s, top_s, pnd_s = agregar_listas_por_dia(rows, d_alvo)
    else:
        prox_s, top_s, pnd_s = d.proximos, d.top_lucro, d.pendentes

    st.markdown(
        '<h2 class="dc-h2" style="font-size:1.12rem; margin:0.5rem 0 0.6rem 0">Oportunidades</h2>',
        unsafe_allow_html=True,
    )
    a1, a2, a3 = st.columns(3, gap="large")
    with a1:
        with st.container(border=True):
            st.markdown(
                '<span class="dc-badge" style="display:block;margin:0.1rem 0 0.4rem 0.05rem">'
                "Próximos leilões</span>",
                unsafe_allow_html=True,
            )
            for i, x in enumerate(prox_s[:8]):
                _dash_linha_oportunidade_com_foto(
                    x,
                    texto_botao=_dash_txt_card_resumo_local(x),
                    st_key=f"dbc_prox_{i}_{x.id}",
                    modo_aba="ingestao",
                )
    with a2:
        with st.container(border=True):
            st.markdown(
                '<span class="dc-badge" style="display:block;margin:0.1rem 0 0.4rem 0.05rem">'
                "Maior lucro líquido (simulado)</span>",
                unsafe_allow_html=True,
            )
            for i, x in enumerate(top_s[:8]):
                _dash_linha_oportunidade_com_foto(
                    x,
                    texto_botao=_dash_txt_card_resumo_local(x),
                    st_key=f"dbc_luc_{i}_{x.id}",
                    modo_aba="simulacao",
                )
    with a3:
        with st.container(border=True):
            st.markdown(
                '<span class="dc-badge" style="display:block;margin:0.1rem 0 0.4rem 0.05rem">Pendências</span>',
                unsafe_allow_html=True,
            )
            for i, x in enumerate(pnd_s[:8]):
                _dash_linha_oportunidade_com_foto(
                    x,
                    texto_botao=_dash_txt_card_resumo_local(x),
                    st_key=f"dbc_pnd_{i}_{x.id}",
                    modo_aba="simulacao",
                )
    st.divider()


def _render_conteudo_principal() -> None:
    modo = str(st.session_state.get("assistente_modo") or "inicio")
    if modo == "inicio":
        titulo_hero = "Painel inicial"
    elif modo == "simulacao":
        titulo_hero = "Simulação de operação"
    elif modo == "anuncios":
        titulo_hero = "Anúncios e caches (Supabase)"
    elif modo == "ingestao":
        titulo_hero = "Leilões"
    else:
        titulo_hero = "Leilões"
    hero_p1 = ""
    hero_extra = ""
    hero_rodape = ""
    st.markdown(
        '<div class="leilao-wrap"><div class="leilao-hero">'
        f"<h1>{html.escape(titulo_hero)}</h1>"
        f"{hero_p1}"
        f"{hero_extra}"
        f"{hero_rodape}"
        "</div></div>",
        unsafe_allow_html=True,
    )

    if st.session_state.pending_duplicate_url:
        _render_bloco_duplicata_ingestao()
        st.stop()

    _render_pendente_frase_firecrawl_pos_ingest()

    _sp_hero = st.session_state.snapshot or {}
    _st_hero = _sp_hero.get("status")
    if _st_hero == "sem_conteudo":
        with st.expander("Página sem edital reconhecido — nada foi gravado", expanded=True):
            st.markdown(f"**Motivo:** {html.escape(str(_sp_hero.get('nota') or '—'))}")
            st.info(MENSAGEM_ACOES_USUARIO)
    elif _st_hero == "url_invalida":
        st.error(str(_sp_hero.get("nota") or "URL inválida ou conteúdo indisponível."))

    if modo == "inicio":
        _render_painel_inicial()
        return

    if modo == "anuncios":
        _render_aba_anuncios()
        return

    if modo in ("ingestao", "simulacao"):
        if modo == "simulacao":
            _col_leiloes, _col_caches = st.columns([1.02, 0.98], gap="medium")
            with _col_leiloes:
                _render_painel_tabela_leiloes_topo()
            with _col_caches:
                _render_painel_caches_leilao_selecionado_simulacao()
        else:
            _render_painel_tabela_leiloes_topo()
        row_alerta = st.session_state.get("ultimo_extracao")
        if isinstance(row_alerta, dict) and row_alerta.get("id") and row_alerta.get("cache_media_bairro_ids"):
            try:
                caches_alerta, _ = _carregar_caches_e_anuncios_ui(row_alerta)
                _render_alerta_volume_amostras_cache(caches_alerta)
            except Exception:
                logger.debug("Alerta volume de amostras (cache)", exc_info=True)
    if modo == "ingestao":
        _render_painel_leiloes_cadastrados_ingestao()

    if modo == "simulacao":
        _render_aba_simulacao()
        return

    _STATUS_EXCLUI_MAPA = frozenset({"processando", "sem_conteudo", "url_invalida"})

    row_ex = st.session_state.get("ultimo_extracao")
    snap = st.session_state.snapshot or {}
    st_status = snap.get("status")
    mostrar_extracao_mapa = (
        bool(row_ex and isinstance(row_ex, dict) and (row_ex.get("id") or row_ex.get("url_leilao")))
        and st_status not in _STATUS_EXCLUI_MAPA
    )
    if mostrar_extracao_mapa:
        titulo_cards = "Dados extraídos"
        if st_status == "duplicata":
            titulo_cards = "Dados extraídos (registro existente — duplicata)"
        elif st_status == "ignorado_duplicata":
            titulo_cards = "Dados extraídos (sem alterações no banco)"
        caches_ui, ads_map_ui = _carregar_caches_e_anuncios_ui(row_ex)
        comparaveis_mapa = _build_comparaveis_mapa_por_cache(caches_ui, ads_map_ui)
        with st.expander(titulo_cards, expanded=False):
            c_extr, c_cache = st.columns((1, 1), gap="medium")
            with c_extr:
                st.markdown(
                    '<p class="leilao-cache-col-title">Imóvel (extração do edital)</p>',
                    unsafe_allow_html=True,
                )
                _render_cards_extracao(row_ex, caches=caches_ui, ads_map=ads_map_ui)
            with c_cache:
                st.markdown(
                    '<p class="leilao-cache-col-title">Cache de mercado usado</p>',
                    unsafe_allow_html=True,
                )
                _render_painel_cache_mercado(caches_ui, ads_map_ui)
        with st.expander("Mapa interativo", expanded=False):
            _render_mapa_folium_row(row_ex, comparaveis=comparaveis_mapa)


def main() -> None:
    st.set_page_config(
        page_title="Leilão IA — Painel e análise",
        page_icon="🏛️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    load_dotenv(_REPO_ROOT / ".env")
    _init_session()
    _aplicar_modo_pendente_antes_dos_widgets()
    st.markdown(_PAGE_CSS, unsafe_allow_html=True)

    _render_sidebar_app()
    _render_conteudo_principal()


if __name__ == "__main__":
    main()
