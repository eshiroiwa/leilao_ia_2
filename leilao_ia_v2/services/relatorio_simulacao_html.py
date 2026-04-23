"""
Relatório HTML estático da aba Simulação (dados extraídos, cache principal, painel financeiro).

Usado via import tardio a partir do app Streamlit para reutilizar formatação e cards do painel.
"""

from __future__ import annotations

import html
import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

from leilao_ia_v2.schemas.operacao_simulacao import (
    OperacaoSimulacaoDocumento,
    SimulacaoOperacaoOutputs,
)
from leilao_ia_v2.ui.dashboard_comparacao_modais import (
    PAINEL_SIMULACAO_RESUMO_DASH_STYLES,
    build_painel_simulacao_resumo_html,
)
from leilao_ia_v2.schemas.relatorio_mercado_contexto import parse_relatorio_mercado_contexto_json
from leilao_ia_v2.services.simulacao_operacao import calcular_simulacao

_REL_CSS = """
:root { --bg:#0b1220; --card:#151f33; --bd:#2a3f5c; --txt:#e8edf5; --muted:#94a3b8; --acc:#2dd4bf; --ok:#4ade80; --err:#f87171; --warn:#fbbf24; }
* { box-sizing: border-box; }
body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; background:linear-gradient(165deg,#0b1220 0%,#0f172a 50%,#0c4a6e 120%); color:var(--txt); min-height:100vh; }
.wrap { max-width: 1080px; margin: 0 auto; padding: 28px 20px 20px; }
h1 { font-size: 1.55rem; font-weight: 700; margin: 0 0 10px; letter-spacing: -0.02em; background: linear-gradient(120deg,#5eead4,#38bdf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }
.tit-end { font-size: 1.42rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; color: #e8edf5; margin: 0 0 22px; line-height: 1.35; }
.sub { color: var(--muted); font-size: 0.92rem; margin-bottom: 20px; }
.rel-foot { text-align: center; color: var(--muted); font-size: 0.78rem; padding: 22px 12px 8px; margin-top: 36px; border-top: 1px solid rgba(51,65,85,0.75); }
a.lei-top { color: var(--acc); word-break: break-all; }
.sec { margin-bottom: 32px; }
.sec-h { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.14em; color: var(--muted); margin: 0 0 14px; font-weight: 600; }
.hero { display: grid; grid-template-columns: 1fr minmax(280px, 360px); gap: 22px; align-items: start; margin-bottom: 8px; }
@media (max-width: 720px) { .hero { grid-template-columns: 1fr; } }
.foto { border-radius: 14px; overflow: hidden; border: 1px solid var(--bd); background: #0c1322; max-height: 420px; display:flex; align-items:center; justify-content:center; }
.foto img { width: 100%; max-height: 420px; object-fit: contain; display: block; }
.rel-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(188px, 1fr)); gap: 12px; }
.rel-card { background: var(--card); border: 1px solid var(--bd); border-radius: 14px; padding: 14px 16px; box-shadow: 0 4px 18px rgba(0,0,0,0.25); }
.rel-card .l { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted); margin-bottom: 6px; }
.rel-card .v { font-size: 0.98rem; font-weight: 600; line-height: 1.35; word-break: break-word; font-variant-numeric: tabular-nums; }
.rel-kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-bottom: 16px; }
.rel-kpi { background: var(--card); border: 1px solid var(--bd); border-radius: 12px; padding: 12px 14px; }
.rel-kpi .l { font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 6px; }
.rel-kpi .v { font-size: 0.95rem; font-weight: 600; color: var(--acc); }
.rel-map-wrap { margin-top: 20px; }
.rel-map-wrap #report-map { height: 400px; width: 100%; border-radius: 12px; border: 1px solid var(--bd); background: #0c1322; }
.leaflet-popup-content .rel-map-popup-list { font-size: 0.86rem; line-height: 1.45; color: #0f172a; }
.leaflet-popup-content .rel-map-popup-list ul { margin: 8px 0 0; padding-left: 1.15em; max-height: 220px; overflow-y: auto; }
.leaflet-popup-content .rel-map-popup-list a { color: #0369a1; word-break: break-all; }
.rel-map-auction-pin.leaflet-div-icon {
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
}
.sim-fin-sec { margin: 0 0 18px 0; }
.sim-fin-h { font-size: 0.68rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; margin: 0 0 10px 0; }
.sim-res-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px; margin: 0; }
.sim-res-card { background: linear-gradient(165deg, rgba(30,41,59,0.95) 0%, rgba(15,23,42,0.92) 100%); border: 1px solid rgba(94, 234, 212, 0.2); border-radius: 12px; padding: 12px 12px 10px; text-align: center; box-shadow: 0 2px 12px rgba(0,0,0,0.28); }
.sim-res-card--accent { border-color: rgba(45, 212, 191, 0.45); background: linear-gradient(165deg, rgba(17, 94, 89, 0.28) 0%, rgba(24, 32, 48, 0.92) 100%); }
.sim-res-lbl { font-size: 0.64rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; font-weight: 600; line-height: 1.25; }
.sim-res-val { font-size: 1rem; font-weight: 700; color: #5eead4; margin-top: 8px; line-height: 1.15; font-variant-numeric: tabular-nums; }
.sim-res-val.muted { color: var(--muted); font-weight: 500; font-size: 0.92rem; }
.sim-res-val.ok { color: var(--ok); }
.sim-res-val.err { color: var(--err); }
.sim-res-val.warn { color: var(--warn); }
.sim-res-sub { font-size: 0.62rem; color: rgba(148, 163, 184, 0.88); margin-top: 8px; line-height: 1.35; }
.rel-extra-dados { max-height: 640px; overflow: auto; }
.rel-ctx-sec { margin-top: 4px; }
.rel-ctx-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px; margin-top: 12px; }
.rel-ctx-card { background: var(--card); border: 1px solid var(--bd); border-radius: 14px; padding: 14px 16px; box-shadow: 0 4px 14px rgba(0,0,0,0.2); }
.rel-ctx-card .rel-ctx-tit { font-size: 0.78rem; font-weight: 700; color: var(--acc); margin-bottom: 10px; line-height: 1.3; }
.rel-ctx-card ul { margin: 0; padding-left: 1.1em; color: var(--txt); font-size: 0.86rem; line-height: 1.45; }
.rel-ctx-card li { margin-bottom: 6px; }
.rel-sim-cmp-paineis { display: grid; grid-template-columns: 1fr 1fr; gap: 1.1rem; align-items: start; }
@media (max-width: 900px) { .rel-sim-cmp-paineis { grid-template-columns: 1fr; } }
.rel-sim-painel-slot { min-width: 0; }
.rel-sim-cmp-tit {
  font-size: 0.72rem; font-weight: 700; color: #5eead4; letter-spacing: 0.04em; text-transform: uppercase;
  margin: 0.15rem 0 0.45rem 0; padding: 0 0.1rem;
}
.rel-sim-painel-borda {
  border: 1px solid var(--bd); border-radius: 16px; overflow: hidden; background: rgba(15, 23, 42, 0.35);
}
.rel-sim-embed .dc-root.sp-sim-financeiro { margin: 0; border-radius: 0; box-shadow: none; }
"""


def _metadados_cache_dict(c: dict[str, Any]) -> dict[str, Any]:
    raw = c.get("metadados_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            import json

            return dict(json.loads(raw))
        except Exception:
            return {}
    return {}


def _cache_e_principal_simulacao(cache_row: dict[str, Any]) -> bool:
    md = _metadados_cache_dict(cache_row)
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


def _html_papel_cache_span(cache_row: dict[str, Any]) -> str:
    """Etiqueta [simulação] ou [referência] (mesma lógica do painel de caches)."""
    md = _metadados_cache_dict(cache_row)
    if str(md.get("modo_cache") or "").strip().lower() == "terrenos":
        return ' <span style="color:#fb923c;font-weight:600;">[referência]</span>'
    if md.get("apenas_referencia") is True or md.get("uso_simulacao") is False:
        return ' <span style="color:#fb923c;font-weight:600;">[referência]</span>'
    return ' <span style="color:#4ade80;font-weight:600;">[simulação]</span>'


def _html_sec_todos_caches(caches: list[dict[str, Any]]) -> str:
    """KPIs por cada linha de ``cache_media_bairro`` vinculada ao leilão."""
    if not caches:
        return '<p class="sub">Nenhum cache de média vinculado a este leilão.</p>'
    from leilao_ia_v2 import app_assistente_ingestao as ag

    partes: list[str] = []
    for c in caches:
        if not isinstance(c, dict):
            continue
        nome = html.escape(str(c.get("nome_cache") or "Cache de mercado").strip() or "Cache")
        tipo = html.escape(str(c.get("tipo_imovel") or "—"))
        try:
            n_am = int(c.get("n_amostras") or 0)
        except (TypeError, ValueError):
            n_am = 0
        try:
            pm2 = float(c.get("preco_m2_medio") or 0)
        except (TypeError, ValueError):
            pm2 = 0.0
        try:
            vm = float(c.get("valor_medio_venda") or 0)
        except (TypeError, ValueError):
            vm = 0.0
        pm2_s = _fmt_rs_m2_br(pm2)
        vm_s = ag._fmt_valor_campo("valor_venda", vm) if vm > 0 else "—"
        papel = _html_papel_cache_span(c)
        kpi = (
            f'<div class="rel-kpi-row" style="margin-top:8px">'
            f'<div class="rel-kpi"><div class="l">Amostras</div><div class="v">{n_am}</div></div>'
            f'<div class="rel-kpi"><div class="l">Preço médio / m²</div><div class="v">{html.escape(pm2_s)}</div></div>'
            f'<div class="rel-kpi"><div class="l">Valor médio venda</div><div class="v">{html.escape(vm_s)}</div></div>'
            f"</div>"
        )
        partes.append(
            '<div class="rel-cache-bloco" style="margin-bottom:20px;padding-bottom:16px;'
            'border-bottom:1px solid var(--bd);">'
            f'<h3 class="rel-cache-subh" style="font-size:0.95rem;font-weight:700;margin:0 0 6px 0;">'
            f"{nome}{papel} · <span style=\"color:var(--muted);font-weight:500;\">{tipo}</span></h3>"
            f"{kpi}</div>"
        )
    if not partes:
        return '<p class="sub">Nenhum cache de média vinculado a este leilão.</p>'
    return "".join(partes)


def _parse_csv_anuncio_ids(raw: Any) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _leilao_extra_json_como_dict(row: dict[str, Any]) -> dict[str, Any] | None:
    raw = row.get("leilao_extra_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            o = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return o if isinstance(o, dict) else None
    return None


def _html_secao_analise_mercado_ctx(row: dict[str, Any]) -> str:
    """Bloco ``relatorio_mercado_contexto_json`` (cards com tópicos)."""
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
        blocos.append(f'<div class="rel-ctx-card"><div class="rel-ctx-tit">{tit}</div><ul>{lis}</ul></div>')
    if not blocos:
        return ""
    return (
        '<div class="sec rel-ctx-sec">'
        '<h2 class="sec-h">Análise de mercado e bairro</h2>'
        f'<div class="rel-ctx-grid">{"".join(blocos)}</div>'
        "</div>"
    )


def _html_secao_dados_adicionais(row: dict[str, Any], ag: Any) -> str:
    """Bloco ``leilao_extra_json`` (formas de pagamento, observações, etc.), alinhado à aba de análise."""
    extra = _leilao_extra_json_como_dict(row)
    if not extra or not ag._leilao_extra_tem_conteudo(extra):
        return ""
    txt = ag._leilao_extra_como_texto(extra).strip()
    if not txt or txt == "—":
        return ""
    return (
        '<div class="sec">'
        '<h2 class="sec-h">Dados adicionais</h2>'
        '<div class="rel-card rel-extra-dados" style="white-space:pre-wrap;font-size:0.87rem;'
        'line-height:1.55;color:var(--txt);">'
        f"{html.escape(txt)}</div></div>"
    )


def _fmt_rs_m2_br(v: float) -> str:
    if v <= 0:
        return "—"
    s = f"{float(v):,.2f}"
    if "," in s and "." in s:
        s = s.replace(",", "_T_").replace(".", ",").replace("_T_", ".")
    return s + " R$/m²"


def _monta_painel_rel_embed(o: SimulacaoOperacaoOutputs) -> str:
    """Fragmento de painel (sem CSS); estilos em ``PAINEL_SIMULACAO_RESUMO_DASH_STYLES`` no head."""
    return build_painel_simulacao_resumo_html(
        o,
        embutir_css=False,
        incluir_cabecalho_rodape=False,
    )


def _o_cmp_do_cache_streamlit(iid: str, sel_esperada: str) -> SimulacaoOperacaoOutputs | None:
    """
    Lê a saída do painel de comparação gravada em sessão em ``_rpt_painel_cmp|{iid}``
    (mesma simulação que a aba mostra). Fora do Streamlit / sem cache, devolve None.
    """
    try:
        import streamlit as st

        raw = st.session_state.get(f"_rpt_painel_cmp|{iid}")
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    if str(raw.get("sel") or "").strip().lower() != sel_esperada.strip().lower():
        return None
    out = raw.get("out")
    if not out:
        return None
    try:
        return SimulacaoOperacaoOutputs.model_validate(out)
    except Exception:
        return None


def _html_secao_paineis_simulacao(
    ag: Any,
    row: dict[str, Any],
    caches: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
    o: SimulacaoOperacaoOutputs,
    cmp_painel: str,
) -> tuple[str, list[SimulacaoOperacaoOutputs]]:
    """Mesmo layout do painel da aba Simulação; coluna extra se comparação ativa. Devolve HTML + outputs p/ notas."""
    bloco_unico = f'<div class="rel-sim-embed rel-sim-painel-borda">{_monta_painel_rel_embed(o)}</div>'

    cp = (cmp_painel or "nenhum").strip().lower()
    if cp not in ("prazo", "financiado"):
        return f'<div class="rel-sim-cmp-unico">{bloco_unico}</div>', [o]

    iid = str(row.get("id") or "").strip()
    if not iid:
        return f'<div class="rel-sim-cmp-unico">{bloco_unico}</div>', [o]

    tag = "prazo" if cp == "prazo" else "financiado"
    tit_cmp = (
        "Parcelado (judicial) — comparação" if tag == "prazo" else "Financiado (bancário) — comparação"
    )
    o2: SimulacaoOperacaoOutputs | None = _o_cmp_do_cache_streamlit(iid, cp)
    if o2 is None:
        try:
            inp2, _t0 = ag._construir_inp_por_tag(iid, row, tag, caches)
            d2 = calcular_simulacao(
                row_leilao=row, inp=inp2, caches_ordenados=caches, ads_por_id=ads_map
            )
            o2 = d2.outputs
        except Exception:
            logger.exception("Relatório HTML: simulação do painel de comparação (fallback recálculo)")
            o2 = None

    if o2 is None:
        return f'<div class="rel-sim-cmp-unico">{bloco_unico}</div>', [o]

    slot_v = (
        '<div class="rel-sim-painel-slot">'
        f'<p class="rel-sim-cmp-tit">{html.escape("À vista — painel principal")}</p>'
        f'{bloco_unico}</div>'
    )
    bloco_cmp = f'<div class="rel-sim-embed rel-sim-painel-borda">{_monta_painel_rel_embed(o2)}</div>'
    slot_c = (
        '<div class="rel-sim-painel-slot">'
        f'<p class="rel-sim-cmp-tit">{html.escape(tit_cmp)}</p>'
        f"{bloco_cmp}</div>"
    )
    return f'<div class="rel-sim-cmp-paineis">{slot_v}{slot_c}</div>', [o, o2]


def _map_comparativos_fragments(
    row: dict[str, Any],
    caches: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
) -> tuple[str, str, str]:
    """
    Devolve (css_link_head, html_secao_mapa, scripts_fim_body) para Leaflet + OSM.
    Vazio se não houver nenhuma coordenada (leilão ou anúncios de **qualquer** cache vinculado).
    """
    auction: dict[str, Any] | None = None
    lat0, lon0 = row.get("latitude"), row.get("longitude")
    if lat0 is not None and lon0 is not None:
        try:
            fa, fo = float(lat0), float(lon0)
            if -90 <= fa <= 90 and -180 <= fo <= 180:
                logr = str(row.get("endereco") or "").strip()
                if len(logr) > 52:
                    logr = logr[:49] + "…"
                ttl = "Imóvel em leilão"
                if logr:
                    ttl = f"Leilão — {logr}"
                auction = {"lat": fa, "lng": fo, "title": ttl}
                ulei = str(row.get("url_leilao") or "").strip()
                if ulei.startswith("http://") or ulei.startswith("https://"):
                    auction["url"] = ulei
        except (TypeError, ValueError):
            pass

    markers: list[dict[str, Any]] = []
    vistos: set[str] = set()
    if caches:
        from leilao_ia_v2 import app_assistente_ingestao as ag

        for c in caches:
            if not isinstance(c, dict):
                continue
            for aid in _parse_csv_anuncio_ids(c.get("anuncios_ids")):
                if aid in vistos:
                    continue
                vistos.add(aid)
                ad = ads_map.get(aid)
                if not isinstance(ad, dict):
                    continue
                alat, alon = ad.get("latitude"), ad.get("longitude")
                if alat is None or alon is None:
                    continue
                try:
                    fa, fo = float(alat), float(alon)
                except (TypeError, ValueError):
                    continue
                if not (-90 <= fa <= 90 and -180 <= fo <= 180):
                    continue
                try:
                    vv = float(ad.get("valor_venda") or 0)
                except (TypeError, ValueError):
                    vv = 0.0
                try:
                    ar = float(ad.get("area_construida_m2") or 0)
                except (TypeError, ValueError):
                    ar = 0.0
                vs = ag._fmt_valor_campo("valor_venda", vv) if vv > 0 else "—"
                ars = ag._fmt_valor_campo("area_util", ar) if ar > 0 else "—"
                if vs != "—" or ars != "—":
                    title = f"{vs} · {ars}"
                else:
                    title = f"Comparável {len(markers) + 1}"
                url_a = str(ad.get("url_anuncio") or "").strip()
                url_ok = url_a if (url_a.startswith("http://") or url_a.startswith("https://")) else ""
                markers.append(
                    {
                        "lat": fa,
                        "lng": fo,
                        "title": title,
                        "url": url_ok,
                    }
                )

    if auction is None and not markers:
        return "", "", ""

    payload = {"auction": auction, "markers": markers}
    json_txt = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    head = (
        '<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin="anonymous" />'
    )
    sec = (
        '<div class="rel-map-wrap"><div id="report-map"></div></div>'
        f'<script type="application/json" id="rel-map-json">{json_txt}</script>'
    )
    scripts = (
        '<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin="anonymous"></script>'
        "<script>"
        "(function(){"
        'var jel=document.getElementById("rel-map-json");'
        'var mapEl=document.getElementById("report-map");'
        "if(!jel||!mapEl||typeof L===\"undefined\")return;"
        "var MAP;try{MAP=JSON.parse(jel.textContent);}catch(e){return;}"
        'function esc(t){return String(t==null?"":t).replace(/&/g,"&amp;").replace(/</g,"&lt;")'
        '.replace(/>/g,"&gt;").replace(/"/g,"&quot;");}'
        'function safeHref(u){u=String(u||"").trim();return(/^https?:\\/\\//i.test(u))?u:"#";}'
        "function popupOne(it){"
        "var h=it.title?esc(it.title):'Comparável';"
        "var href=safeHref(it.url);"
        "if(href!=='#')h+='<br><a href=\"'+esc(href)+'\" target=\"_blank\" rel=\"noopener noreferrer\">Abrir link do site</a>';"
        "return h;}"
        "function popupGroup(items){"
        "if(items.length===1)return popupOne(items[0]);"
        "var h='<div class=\"rel-map-popup-list\"><strong>'+items.length+' anúncios neste ponto</strong><ul>';"
        "for(var i=0;i<items.length;i++){"
        "var it=items[i];var lab=it.title?esc(it.title):('Anúncio '+(i+1));"
        "var href=safeHref(it.url);"
        "if(href!=='#')h+='<li><a href=\"'+esc(href)+'\" target=\"_blank\" rel=\"noopener noreferrer\">'+lab+'</a></li>';"
        "else h+='<li>'+lab+'</li>';}"
        "return h+'</ul></div>';}"
        "function gkey(lat,lng){return Math.round(lat*1e5)/1e5+\",\"+Math.round(lng*1e5)/1e5;}"
        "var pts=[];"
        'var map=L.map("report-map",{scrollWheelZoom:false});'
        'L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",'
        '{maxZoom:19,attribution:"&copy; OpenStreetMap contributors"}).addTo(map);'
        "if(MAP.auction){var a=MAP.auction;var pop=a.title||'Imóvel em leilão';"
        "var uh=safeHref(a.url);if(uh!=='#')pop+='<br><a href=\"'+esc(uh)+'\" target=\"_blank\" rel=\"noopener noreferrer\">Abrir página do leilão</a>';"
        "else pop+='<br><span style=\"color:#64748b;font-size:0.9em;\">URL do leilão não cadastrada no registro.</span>';"
        "var pinH='<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"38\" height=\"50\" viewBox=\"0 0 24 36\" focusable=\"false\" aria-hidden=\"true\">"
        "<path fill=\"#f59e0b\" stroke=\"#78350f\" stroke-width=\"1.15\" d=\"M12 1.1C5.7 1.1 1 5.85 1 12.05c0 7.35 10.15 22.05 11.05 23.85.9-1.8 10.95-16.5 10.95-23.85C23 5.85 18.3 1.1 12 1.1z\"/>"
        "<circle cx=\"12\" cy=\"12.2\" r=\"3.6\" fill=\"rgba(255,255,255,0.93)\"/></svg>';"
        "var aIcon=L.divIcon({className:'rel-map-auction-pin',html:pinH,iconSize:[38,50],iconAnchor:[19,50],popupAnchor:[0,-46]});"
        "L.marker([a.lat,a.lng],{icon:aIcon,zIndexOffset:2500}).addTo(map).bindPopup(pop,{maxWidth:320});pts.push([a.lat,a.lng]);}"
        "var groups={};"
        "(MAP.markers||[]).forEach(function(m){"
        "var k=gkey(m.lat,m.lng);if(!groups[k])groups[k]={items:[]};groups[k].items.push(m);});"
        "Object.keys(groups).forEach(function(k){"
        "var items=groups[k].items,n=items.length,m0=items[0];"
        "var r=n>1?10:8,w=n>1?3:2;"
        'L.circleMarker([m0.lat,m0.lng],{radius:r,color:"#0369a1",weight:w,fillColor:"#38bdf8",fillOpacity:0.92})'
        ".addTo(map).bindPopup(popupGroup(items),{maxWidth:320,maxHeight:280,autoPan:true});"
        "pts.push([m0.lat,m0.lng]);});"
        "if(pts.length===0)return;"
        "if(pts.length===1)map.setView(pts[0],14);"
        "else map.fitBounds(L.latLngBounds(pts),{padding:[32,32],maxZoom:16});"
        "})();"
        "</script>"
    )
    return head, sec, scripts


def montar_html_relatorio_simulacao(
    *,
    row: dict[str, Any],
    caches: list[dict[str, Any]],
    ads_map: dict[str, dict[str, Any]],
    doc: OperacaoSimulacaoDocumento,
    cmp_painel: str = "nenhum",
) -> str:
    """
    ``cmp_painel``: ``nenhum`` (só à vista) | ``prazo`` | ``financiado`` — alinhado ao rádio da simulação.
    Fora do Streamlit, omitir ou usar ``nenhum`` (relatório só com painel à vista).
    """
    from leilao_ia_v2 import app_assistente_ingestao as ag

    doc2 = doc
    if doc2.outputs is None:
        doc2 = calcular_simulacao(
            row_leilao=row,
            inp=doc2.inputs,
            caches_ordenados=caches,
            ads_por_id=ads_map,
        )
    o = doc2.outputs or SimulacaoOperacaoOutputs()

    endereco_linha = str(row.get("endereco") or "").strip() or "—"
    endereco_tit = html.escape(endereco_linha)
    cid = html.escape(str(row.get("cidade") or "—"))
    uf = html.escape(str(row.get("estado") or "—"))
    agora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    url_lei = str(row.get("url_leilao") or "").strip()
    url_lei_h = html.escape(url_lei, quote=True)

    foto_u = ag._url_foto_imovel_valida(row)
    foto_html = ""
    if foto_u:
        fq = html.escape(foto_u, quote=True)
        foto_html = f'<div class="foto"><img src="{fq}" alt="Foto do imóvel" loading="lazy" referrerpolicy="no-referrer" /></div>'

    cards_ex: list[str] = []
    for label, key in ag._CAMPOS_EXTRACAO:
        if key == "url_leilao":
            continue
        raw = row.get(key)
        if ag._raw_extracao_ocultar(raw):
            continue
        disp = ag._fmt_valor_campo(key, raw)
        if disp == "—" or not str(disp).strip():
            continue
        cards_ex.append(
            '<div class="rel-card"><div class="l">' + html.escape(label) + "</div>"
            '<div class="v">' + html.escape(disp) + "</div></div>"
        )
    grid_ex = '<div class="rel-grid">' + "".join(cards_ex) + "</div>" if cards_ex else "<p class=\"sub\">Sem campos extraídos para exibir.</p>"
    sec_adicionais = _html_secao_dados_adicionais(row, ag)
    sec_ctx_mercado = _html_secao_analise_mercado_ctx(row)

    _principal = _row_cache_principal_simulacao(caches)
    sec_cache = _html_sec_todos_caches(caches)
    if _principal is not None and len(caches) > 1:
        sec_cache = (
            '<p class="sub" style="margin:0 0 14px 0;">A simulação do painel financeiro abaixo usa o cache '
            "marcado como <strong>[simulação]</strong> (principal) quando existir.</p>" + sec_cache
        )
    map_head, map_sec, map_scripts = _map_comparativos_fragments(row, caches, ads_map)

    fin_html, _outs_cmp = _html_secao_paineis_simulacao(
        ag, row, caches, ads_map, o, cmp_painel
    )
    notas_fin = ""
    extra_lines: list[str] = []
    for ox in _outs_cmp:
        for n in ox.lance_maximo_roi_notas or []:
            t = str(n).strip()
            if t:
                s = html.escape(t)
                if s not in extra_lines:
                    extra_lines.append(s)
        for n in ox.notas or []:
            t = str(n).strip()
            if t:
                s = html.escape(t)
                if s not in extra_lines:
                    extra_lines.append(s)
    if extra_lines:
        lis = "".join(f"<li>{ln}</li>" for ln in extra_lines)
        notas_fin = f'<div class="rel-card" style="margin-top:16px;"><div class="l">Notas do cálculo</div><ul style="margin:8px 0 0 18px;color:var(--muted);font-size:0.88rem;line-height:1.5;">{lis}</ul></div>'

    col_text = (
        "<div>"
        f'<p class="sub" style="margin-top:0;">{cid} / {uf}</p>'
        + (
            f'<p class="sub"><a class="lei-top" href="{url_lei_h}" target="_blank" rel="noopener noreferrer">Link do leilão</a></p>'
            if url_lei
            else ""
        )
        + "</div>"
    )
    col_foto = foto_html or (
        '<div class="foto" style="min-height:120px;color:var(--muted);font-size:0.88rem;align-items:center;">Sem foto</div>'
    )
    hero_inner = col_text + col_foto

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Relatório — Simulação</title>
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,100..1000;1,9..40,100..1000&display=swap" rel="stylesheet" />
<style>{_REL_CSS}</style>
{PAINEL_SIMULACAO_RESUMO_DASH_STYLES}
{map_head}
</head>
<body>
<div class="wrap">
  <h1>Relatório de simulação</h1>
  <p class="tit-end">{endereco_tit}</p>
  <div class="hero">
    {hero_inner}
  </div>

  <div class="sec">
    <h2 class="sec-h">Dados extraídos do edital</h2>
    {grid_ex}
  </div>
  {sec_adicionais}
  {sec_ctx_mercado}

  <div class="sec">
    <h2 class="sec-h">Anúncios comparativos</h2>
    {sec_cache}
    {map_sec}
  </div>

  <div class="sec">
    <h2 class="sec-h">Painel financeiro</h2>
    {fin_html}
    {notas_fin}
  </div>

  <footer class="rel-foot">Gerado em {html.escape(agora)}</footer>
</div>
{map_scripts}
</body>
</html>"""
