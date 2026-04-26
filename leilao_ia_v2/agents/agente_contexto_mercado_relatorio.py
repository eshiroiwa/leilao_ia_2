"""
Agente dedicado: gera e persiste ``relatorio_mercado_contexto_json`` (uma chamada OpenAI + Supabase).

Reutilização: se já existir documento válido e ``force=False``, não chama a API.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from supabase import Client

from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.schemas.relatorio_mercado_contexto import (
    CARD_IDS_ORDEM,
    RELATORIO_MERCADO_CONTEXTO_VERSAO,
    RelatorioMercadoContextoDocumento,
    parse_relatorio_mercado_contexto_json,
)
from leilao_ia_v2.services.contexto_mercado_relatorio_llm import (
    gerar_contexto_mercado_relatorio_llm,
    montar_texto_entrada_contexto,
)
from leilao_ia_v2.services.relatorio_mercado_inteligencia import (
    assinatura_cache_principal,
    avaliar_validade_relatorio,
    calcular_qualidade_relatorio,
    evidencias_por_card,
    extrair_sinais_objetivos_decisao,
    extrair_sinais_objetivos_por_cards,
    gerar_insights_decisao,
    montar_contexto_minimo_decisao,
    montar_contexto_populacao_bairro,
)

logger = logging.getLogger(__name__)


def _parse_csv_anuncio_ids(raw: Any) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _contar_anuncios_resolvidos(cache_principal: dict[str, Any] | None, ads_por_id: dict[str, dict[str, Any]]) -> int:
    if not cache_principal:
        return 0
    n = 0
    for aid in _parse_csv_anuncio_ids(cache_principal.get("anuncios_ids")):
        if isinstance(ads_por_id.get(aid), dict):
            n += 1
    return n


def _metadados_cache_dict(c: dict[str, Any]) -> dict[str, Any]:
    raw = c.get("metadados_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
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


def garantir_contexto_mercado_relatorio(
    client: Client,
    *,
    leilao_imovel_id: str,
    row: dict[str, Any],
    caches: list[dict[str, Any]],
    ads_por_id: dict[str, dict[str, Any]],
    force: bool = False,
) -> tuple[RelatorioMercadoContextoDocumento, dict[str, Any]]:
    """
    Devolve (documento, métricas). Se reutilizar cache persistido, métricas terão ``cache_hit=True``
    e zeros de tokens (exceto custo 0).
    """
    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        raise ValueError("leilao_imovel_id vazio")

    raw_ex = row.get("relatorio_mercado_contexto_json")
    if not force and isinstance(raw_ex, str) and raw_ex.strip():
        try:
            raw_ex = json.loads(raw_ex)
        except json.JSONDecodeError:
            raw_ex = {}
    if (
        not force
        and isinstance(raw_ex, dict)
        and int(raw_ex.get("versao") or 0) == RELATORIO_MERCADO_CONTEXTO_VERSAO
        and str(raw_ex.get("gerado_em_iso") or "").strip()
        and isinstance(raw_ex.get("cards"), list)
        and len(raw_ex["cards"]) >= len(CARD_IDS_ORDEM)
    ):
        doc = parse_relatorio_mercado_contexto_json(raw_ex)
        return doc, {
            "modelo": str(raw_ex.get("modelo") or ""),
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "custo_usd_estimado": 0.0,
            "cache_hit": True,
        }

    cache_p = _row_cache_principal_simulacao(caches)
    n_res = _contar_anuncios_resolvidos(cache_p, ads_por_id)
    texto = montar_texto_entrada_contexto(
        row=row,
        cache_principal=cache_p,
        n_anuncios_resolvidos=n_res,
    )
    doc, metricas = gerar_contexto_mercado_relatorio_llm(texto)
    ttl_horas_raw = str(os.getenv("RELATORIO_MERCADO_TTL_HORAS", "168") or "168").strip()
    try:
        ttl_horas = max(24, min(int(ttl_horas_raw), 24 * 90))
    except Exception:
        ttl_horas = 168
    qual = calcular_qualidade_relatorio(
        cache_principal=cache_p,
        ads_por_id=ads_por_id,
        bairro_alvo=str(row.get("bairro") or ""),
    )
    ev_por = evidencias_por_card(qualidade=qual, bairro_alvo=str(row.get("bairro") or ""))
    cards_enriquecidos = []
    for c in doc.cards:
        ev = str(ev_por.get(c.id) or "").strip()
        tops = list(c.topicos or [])
        if ev and not any("base:" in str(t).lower() for t in tops):
            tops.append(ev)
        cards_enriquecidos.append(c.model_copy(update={"evidencia": ev, "topicos": tops[:14]}))
    sinais_cards = extrair_sinais_objetivos_por_cards(cards_enriquecidos)
    insights_auto = gerar_insights_decisao(row=row, qualidade=qual, sinais=sinais_cards)

    def _merge_lista(a: list[str], b: list[str], limite: int) -> list[str]:
        out: list[str] = []
        for item in list(a or []) + list(b or []):
            s = str(item or "").strip()
            if not s:
                continue
            if s in out:
                continue
            out.append(s)
            if len(out) >= limite:
                break
        return out

    opp = _merge_lista(list(doc.insights_oportunidade or []), list(insights_auto["insights_oportunidade"]), 8)
    risk = _merge_lista(list(doc.insights_risco or []), list(insights_auto["insights_risco"]), 8)
    chk = _merge_lista(list(doc.checklist_diligencia or []), list(insights_auto["checklist_diligencia"]), 10)
    estrategia = str(doc.estrategia_sugerida or "").strip() or str(insights_auto["estrategia_sugerida"])
    tese = str(doc.tese_acao or "").strip() or str(insights_auto["tese_acao"])
    ctx_min = montar_contexto_minimo_decisao(row=row, qualidade=qual)
    ctx_pb_auto = montar_contexto_populacao_bairro(row=row, qualidade=qual)
    pop = _merge_lista(list(doc.dados_populacao_cidade or []), list(ctx_pb_auto["dados_populacao_cidade"]), 6)
    bairro_info = _merge_lista(list(doc.informacoes_bairro or []), list(ctx_pb_auto["informacoes_bairro"]), 8)

    sinais = extrair_sinais_objetivos_decisao(
        insights_oportunidade=opp,
        insights_risco=risk,
        estrategia_sugerida=estrategia,
        tese_acao=tese,
    )
    if not opp and not risk:
        sinais = sinais_cards

    assinatura = assinatura_cache_principal(cache_p)
    validade = avaliar_validade_relatorio(
        gerado_em_iso=doc.gerado_em_iso,
        ttl_horas=ttl_horas,
        cache_principal_id=str((cache_p or {}).get("id") or ""),
        assinatura_cache=assinatura,
        cache_principal_atual=cache_p,
    )
    validade.update(
        {
            "cache_principal_id": str((cache_p or {}).get("id") or ""),
            "assinatura_cache_principal": assinatura,
        }
    )
    doc = doc.model_copy(
        update={
            "cards": cards_enriquecidos,
            "sinais_decisao": sinais,
            "qualidade": qual,
            "validade": validade,
            "insights_oportunidade": opp,
            "insights_risco": risk,
            "checklist_diligencia": chk,
            "dados_populacao_cidade": pop,
            "informacoes_bairro": bairro_info,
            "contexto_minimo": ctx_min,
            "estrategia_sugerida": estrategia,
            "tese_acao": tese,
        }
    )
    payload = doc.model_dump(mode="json")
    leilao_imoveis_repo.atualizar_leilao_imovel(
        lid,
        {"relatorio_mercado_contexto_json": payload},
        client,
    )
    metricas = dict(metricas)
    metricas["cache_hit"] = False
    logger.info(
        "Contexto mercado relatório gravado leilao=%s tokens=%s/%s",
        lid[:8],
        metricas.get("prompt_tokens"),
        metricas.get("completion_tokens"),
    )
    return doc, metricas
