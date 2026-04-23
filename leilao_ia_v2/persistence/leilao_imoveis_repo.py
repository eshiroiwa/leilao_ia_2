"""Leitura e escrita na tabela `leilao_imoveis`."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from supabase import Client

from leilao_ia_v2.constants import TABELA_LEILAO_IMOVEIS

logger = logging.getLogger(__name__)


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


def buscar_por_url_leilao(url: str, client: Client) -> Optional[dict[str, Any]]:
    resp = (
        client.table(TABELA_LEILAO_IMOVEIS)
        .select("*")
        .eq("url_leilao", url)
        .limit(1)
        .execute()
    )
    rows = getattr(resp, "data", None) or []
    return rows[0] if rows else None


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
    """Persiste apenas o documento JSON da simulação de operação."""
    atualizar_leilao_imovel(
        imovel_id,
        {"operacao_simulacao_json": operacao_simulacao_json},
        client,
    )


def atualizar_simulacoes_modalidades_json(
    imovel_id: str,
    simulacoes_modalidades_json: dict[str, Any],
    client: Client,
) -> None:
    """Persiste o bundle das três modalidades (vista, prazo, financiado). Requer coluna 011 no Supabase."""
    atualizar_leilao_imovel(
        imovel_id,
        {"simulacoes_modalidades_json": simulacoes_modalidades_json},
        client,
    )


def atualizar_operacao_e_modalidades(
    imovel_id: str,
    operacao_simulacao_json: dict[str, Any],
    simulacoes_modalidades_json: dict[str, Any],
    client: Client,
) -> None:
    """Atualiza legado (modalidade ativa) e o bundle de três modalidades numa única escrita."""
    atualizar_leilao_imovel(
        imovel_id,
        {
            "operacao_simulacao_json": operacao_simulacao_json,
            "simulacoes_modalidades_json": simulacoes_modalidades_json,
        },
        client,
    )


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
