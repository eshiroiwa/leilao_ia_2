"""Leitura e escrita na tabela `leilao_imoveis`."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from supabase import Client

from leilao_ia_v2.constants import TABELA_LEILAO_IMOVEIS
from leilao_ia_v2.normalizacao import (
    candidatas_url_leilao_para_busca,
    normalizar_url_leilao,
    valores_id_numericos_grandes_na_query,
)

logger = logging.getLogger(__name__)


# Colunas que o agente pós-cache não deve regravar se já houver simulação persistida
# (o motor correcto é ``calcular_simulacao`` + ``indicadores_de_operacao_simulacao_json`` ao gravar).
COLUNAS_INDICADORES_SOMENTE_SIMULACAO: tuple[str, ...] = (
    "valor_mercado_estimado",
    "custo_reforma_estimado",
    "roi_projetado",
    "lance_maximo_recomendado",
    "lucro_bruto_projetado",
    "lucro_liquido_projetado",
    "roi_liquido_projetado",
)


def leilao_tem_indicadores_simulacao_gravados(operacao_simulacao_json: Any) -> bool:
    """
    Indica se existe documento de operação (legado) com ``outputs`` preenchidos —
    a fonte de verdade passa a ser o JSON, não a heurística pós-cache de mercado.
    """
    if not isinstance(operacao_simulacao_json, dict):
        return False
    o = operacao_simulacao_json.get("outputs")
    if not isinstance(o, dict) or not o:
        return False
    if o.get("valor_venda_estimado") is None and o.get("lucro_bruto") is None:
        return False
    return True


def leilao_tem_simulacao_utilizador_gravada(row: dict[str, Any] | None) -> bool:
    """
    ``True`` só quando o utilizador **gravou** outputs de simulação (à vista ou no bundle),
    e não meramente ``inputs``/defaults (JSON mínimo). Até lá, a UI e colunas de ideia
    inicial devem seguir só o **pós-cache** (``roi_pos_cache_leilao``).
    """
    if not row or not isinstance(row, dict):
        return False
    if leilao_tem_indicadores_simulacao_gravados(row.get("operacao_simulacao_json")):
        return True
    sm = row.get("simulacoes_modalidades_json")
    if not isinstance(sm, dict):
        return False
    for k in ("vista", "prazo", "financiado"):
        if leilao_tem_indicadores_simulacao_gravados(sm.get(k)):
            return True
    return False


def indicadores_de_operacao_simulacao_json(operacao_simulacao_json: dict[str, Any]) -> dict[str, Any]:
    """
    Mapeia ``outputs`` de ``operacao_simulacao_json`` (legado, modalidade ativa) para
    as colunas desnormalizadas usadas em listas / dashboard, para ficarem alinhadas ao
    que foi calculado e gravado no documento (evita mostrar ainda o snapshot do
    pós-cache de mercado, que usava outro fluxo e não era reposto ao **Gravar simulação**).
    """
    o = (operacao_simulacao_json or {}).get("outputs")
    if not isinstance(o, dict) or not o:
        return {}

    def _f(k: str) -> float | None:
        v = o.get(k)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    out: dict[str, Any] = {}
    vve = _f("valor_venda_estimado")
    if vve is not None:
        out["valor_mercado_estimado"] = round(vve, 2)
    rfb = _f("reforma_brl")
    if rfb is not None:
        out["custo_reforma_estimado"] = round(rfb, 2)
    lb = _f("lucro_bruto")
    if lb is not None:
        out["lucro_bruto_projetado"] = round(lb, 2)
    ll = _f("lucro_liquido")
    if ll is not None:
        out["lucro_liquido_projetado"] = round(ll, 2)
    rb = _f("roi_bruto")
    if rb is not None:
        out["roi_projetado"] = rb
    rliq = _f("roi_liquido")
    if rliq is not None:
        out["roi_liquido_projetado"] = rliq
    lmax = o.get("lance_maximo_para_roi_desejado")
    if lmax is not None:
        try:
            out["lance_maximo_recomendado"] = round(float(lmax), 2)
        except (TypeError, ValueError):
            pass
    return out


def buscar_por_id(imovel_id: str, client: Client) -> Optional[dict[str, Any]]:
    iid = str(imovel_id or "").strip()
    if not iid:
        return None
    resp = client.table(TABELA_LEILAO_IMOVEIS).select("*").eq("id", iid).limit(1).execute()
    rows = getattr(resp, "data", None) or []
    return rows[0] if rows else None


def listar_resumo_recentes(
    client: Client,
    *,
    limite: int = 200,
) -> list[dict[str, Any]]:
    """
    Lista imóveis recentes para o painel (rótulo + mapa).

    Ordena no servidor por ``edital_coletado_em`` (mais recente primeiro); a UI reordena por
    **data de leilão mais próxima** (1ª/2ª praça) quando exibe a tabela.
    """
    lim = max(1, min(int(limite or 200), 500))
    resp = (
        client.table(TABELA_LEILAO_IMOVEIS)
        .select(
            "id,url_leilao,cidade,estado,bairro,endereco,tipo_imovel,latitude,longitude,"
            "edital_coletado_em,data_leilao_1_praca,data_leilao_2_praca,data_leilao,"
            "valor_lance_1_praca,valor_lance_2_praca,valor_arrematacao,leilao_extra_json,"
            "operacao_simulacao_json,simulacoes_modalidades_json,"
            "valor_mercado_estimado,custo_reforma_estimado,roi_projetado,lance_maximo_recomendado,"
            "valor_maximo_regiao_estimado,valor_minimo_regiao_estimado,"
            "lucro_bruto_projetado,lucro_liquido_projetado,roi_liquido_projetado"
        )
        .order("edital_coletado_em", desc=True)
        .limit(lim)
        .execute()
    )
    return list(getattr(resp, "data", None) or [])


def listar_para_dashboard(
    client: Client,
    *,
    limite: int = 400,
) -> list[dict[str, Any]]:
    """
    Dados enriquecidos para o painel inicial: simulação, relatório de mercado, datas e cache.
    Ordena por ``edital_coletado_em`` (mais recente primeiro) — a UI agrega e reordena.
    """
    lim = max(1, min(int(limite or 400), 500))
    resp = (
        client.table(TABELA_LEILAO_IMOVEIS)
        .select(
            "id,url_leilao,cidade,estado,bairro,endereco,tipo_imovel,latitude,longitude,"
            "data_leilao_1_praca,data_leilao_2_praca,data_leilao,edital_coletado_em,"
            "url_foto_imovel,"
            "operacao_simulacao_json,simulacoes_modalidades_json,relatorio_mercado_contexto_json,"
            "cache_media_bairro_ids,leilao_extra_json,"
            "roi_projetado,valor_mercado_estimado,lucro_liquido_projetado"
        )
        .order("edital_coletado_em", desc=True)
        .limit(lim)
        .execute()
    )
    return list(getattr(resp, "data", None) or [])


def listar_para_vinculo_cache(
    client: Client,
    *,
    limite: int = 500,
) -> list[dict[str, Any]]:
    """
    Lista leilões com ``cache_media_bairro_ids`` para a UI de vínculo (ordem: edital mais recente).
    """
    lim = max(1, min(int(limite or 500), 800))
    resp = (
        client.table(TABELA_LEILAO_IMOVEIS)
        .select("id,url_leilao,cidade,estado,bairro,cache_media_bairro_ids,edital_coletado_em")
        .order("edital_coletado_em", desc=True)
        .limit(lim)
        .execute()
    )
    return list(getattr(resp, "data", None) or [])


def _buscar_por_fragmento_id_query(url: str, client: Client) -> Optional[dict[str, Any]]:
    """Último recurso: `ilike` com id numérico (ex. hdnimovel) quando a string difere do legado."""
    ncanon = normalizar_url_leilao(url)
    for frag in valores_id_numericos_grandes_na_query(url):
        if not frag.isdigit():
            continue
        try:
            resp = (
                client.table(TABELA_LEILAO_IMOVEIS)
                .select("*")
                .ilike("url_leilao", f"%{frag}%")
                .order("edital_coletado_em", desc=True)
                .limit(5)
                .execute()
            )
        except Exception:
            logger.debug("busca ilike url_leilao por fragmento", exc_info=True)
            continue
        rows = getattr(resp, "data", None) or []
        for row in rows:
            u0 = str(row.get("url_leilao") or "")
            if normalizar_url_leilao(u0) == ncanon:
                return row
        if len(rows) == 1:
            return rows[0]
    return None


def buscar_por_url_leilao(url: str, client: Client) -> Optional[dict[str, Any]]:
    """
    Localiza o imóvel pela URL, incluindo variações legadas (http/https, barra no path,
    hdnimovel via ilike) alinhadas a `normalizar_url_leilao`.
    """
    cands = candidatas_url_leilao_para_busca(url)
    if cands:
        resp = (
            client.table(TABELA_LEILAO_IMOVEIS)
            .select("*")
            .in_("url_leilao", cands)
            .order("edital_coletado_em", desc=True)
            .limit(1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if rows:
            return rows[0]
    return _buscar_por_fragmento_id_query(url, client)


def inserir_leilao_imovel(payload: dict[str, Any], client: Client) -> dict[str, Any]:
    logger.info("Supabase: insert url_leilao=%s", payload.get("url_leilao", "")[:80])
    resp = client.table(TABELA_LEILAO_IMOVEIS).insert(payload).execute()
    data = getattr(resp, "data", None)
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return {}


def atualizar_leilao_imovel(imovel_id: str, campos: dict[str, Any], client: Client) -> None:
    logger.info("Supabase: update id=%s keys=%s", imovel_id, list(campos.keys()))
    client.table(TABELA_LEILAO_IMOVEIS).update(campos).eq("id", imovel_id).execute()


def atualizar_operacao_simulacao_json(
    imovel_id: str,
    operacao_simulacao_json: dict[str, Any],
    client: Client,
) -> None:
    """Persiste o documento JSON e sincroniza colunas de indicadores com ``outputs``."""
    payload: dict[str, Any] = {"operacao_simulacao_json": operacao_simulacao_json}
    payload.update(indicadores_de_operacao_simulacao_json(operacao_simulacao_json))
    atualizar_leilao_imovel(imovel_id, payload, client)


def atualizar_operacao_e_modalidades(
    imovel_id: str,
    operacao_simulacao_json: dict[str, Any],
    simulacoes_modalidades_json: dict[str, Any],
    client: Client,
) -> None:
    """
    Atualiza legado (modalidade ativa) e o bundle de três modalidades numa única escrita
    e repete na linha as colunas de lucro/ROI/valor (fonte: ``outputs`` do legado = à vista).
    """
    payload: dict[str, Any] = {
        "operacao_simulacao_json": operacao_simulacao_json,
        "simulacoes_modalidades_json": simulacoes_modalidades_json,
    }
    payload.update(indicadores_de_operacao_simulacao_json(operacao_simulacao_json))
    atualizar_leilao_imovel(imovel_id, payload, client)


def definir_cache_media_bairro_ids(imovel_id: str, ids: list[str], client: Client) -> None:
    """Substitui a lista ``cache_media_bairro_ids`` pela ordem fornecida (sem duplicar)."""
    limpos: list[str] = []
    seen: set[str] = set()
    for x in ids:
        s = str(x).strip()
        if s and s not in seen:
            limpos.append(s)
            seen.add(s)
    atualizar_leilao_imovel(imovel_id, {"cache_media_bairro_ids": limpos}, client)


def anexar_cache_media_bairro_ids(imovel_id: str, novos_ids: list[str], client: Client) -> list[str]:
    """Acrescenta UUIDs de cache ao array ``cache_media_bairro_ids`` sem duplicar."""
    row = buscar_por_id(imovel_id, client)
    if not row:
        return []
    cur = list(row.get("cache_media_bairro_ids") or [])
    seen = set(str(x) for x in cur)
    for x in novos_ids:
        s = str(x).strip()
        if s and s not in seen:
            cur.append(s)
            seen.add(s)
    atualizar_leilao_imovel(imovel_id, {"cache_media_bairro_ids": cur}, client)
    return cur


def remover_cache_media_bairro_id(imovel_id: str, cache_id: str, client: Client) -> list[str]:
    """Remove um UUID de ``cache_media_bairro_ids`` (mantém ordem dos restantes)."""
    row = buscar_por_id(imovel_id, client)
    if not row:
        return []
    rem = str(cache_id or "").strip()
    if not rem:
        return list(row.get("cache_media_bairro_ids") or [])
    cur = [str(x).strip() for x in (row.get("cache_media_bairro_ids") or []) if str(x).strip()]
    new = [x for x in cur if x != rem]
    atualizar_leilao_imovel(imovel_id, {"cache_media_bairro_ids": new}, client)
    return new


def agora_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def listar_ids_leilao_que_incluem_cache_id(cache_id: str, client: Client) -> list[str]:
    """
    IDs em ``leilao_imoveis`` cujo array ``cache_media_bairro_ids`` contém o UUID do cache.

    O mesmo registo de ``cache_media_bairro`` pode ser partilhado por vários imóveis (reutilização
    no mesmo *geo bucket* / cidade). Usado para decidir se uma linha de cache é órfã.
    """
    cid = str(cache_id or "").strip()
    if not cid:
        return []
    try:
        resp = (
            client.table(TABELA_LEILAO_IMOVEIS)
            .select("id")
            .contains("cache_media_bairro_ids", [cid])
            .execute()
        )
    except Exception:
        # Fallback: leituras em lote (evita operador que falhe em esquemas antigos)
        return _listar_ids_com_cache_id_fallback(client, cid)
    return [str(r.get("id") or "") for r in (getattr(resp, "data", None) or []) if r.get("id")]


def _listar_ids_com_cache_id_fallback(client: Client, cache_id: str) -> list[str]:
    resp = client.table(TABELA_LEILAO_IMOVEIS).select("id,cache_media_bairro_ids").limit(2000).execute()
    out: list[str] = []
    for r in getattr(resp, "data", None) or []:
        arr = r.get("cache_media_bairro_ids") or []
        if not isinstance(arr, (list, tuple)):
            continue
        if any(str(x).strip() == cache_id for x in arr):
            iid = str(r.get("id") or "").strip()
            if iid:
                out.append(iid)
    return out
