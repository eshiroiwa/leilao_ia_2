"""Insert em ``cache_media_bairro``."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from supabase import Client

from leilao_ia_v2.constants import TABELA_CACHE_MEDIA_BAIRRO

logger = logging.getLogger(__name__)


def _executar_write_seguro(
    query: Any,
    *,
    op_nome: str,
    tabela: str = TABELA_CACHE_MEDIA_BAIRRO,
) -> Any:
    resp = query.execute()
    if resp is None:
        raise RuntimeError(f"Supabase {op_nome} retornou resposta nula ({tabela}).")
    if not hasattr(resp, "data"):
        logger.warning("Supabase %s sem atributo data (%s).", op_nome, tabela)
    return resp


def inserir(client: Client, linha: dict[str, Any]) -> Optional[str]:
    """Insere uma linha de cache e devolve o ``id`` (UUID) ou None."""
    payload = dict(linha)
    payload.setdefault("atualizado_em", datetime.now(timezone.utc).isoformat())
    if not payload.get("chave_segmento"):
        logger.warning("cache_media_bairro: chave_segmento vazia — insert abortado")
        return None
    resp = _executar_write_seguro(
        client.table(TABELA_CACHE_MEDIA_BAIRRO).insert(payload),
        op_nome="insert",
    )
    data = getattr(resp, "data", None)
    if isinstance(data, list) and data:
        return str(data[0].get("id") or "") or None
    if isinstance(data, dict):
        return str(data.get("id") or "") or None
    return None


def buscar_por_ids(client: Client, ids: list[str]) -> list[dict[str, Any]]:
    """Lê linhas de ``cache_media_bairro`` pelos UUIDs (lotes de até 50)."""
    limpos = [str(i).strip() for i in ids if str(i).strip()]
    if not limpos:
        return []
    out: list[dict[str, Any]] = []
    for i in range(0, len(limpos), 50):
        chunk = limpos[i : i + 50]
        resp = client.table(TABELA_CACHE_MEDIA_BAIRRO).select("*").in_("id", chunk).execute()
        out.extend(list(getattr(resp, "data", None) or []))
    return out


def listar_candidatos_reuso(
    client: Client,
    *,
    geo_bucket: str,
    estado_sigla: str,
    cidade: str,
    limite: int = 48,
) -> list[dict[str, Any]]:
    """
    Caches no mesmo ``geo_bucket`` + UF + cidade (``ilike``), para avaliar reutilização.
    Ordenação: mais amostras primeiro, depois ``atualizado_em`` (desc) em memória.
    """
    gb = (geo_bucket or "").strip()
    uf = (estado_sigla or "").strip().upper()[:2]
    cid = (cidade or "").strip()
    if not gb or len(uf) != 2 or not cid:
        return []
    lim = max(1, min(int(limite or 48), 100))
    resp = (
        client.table(TABELA_CACHE_MEDIA_BAIRRO)
        .select("*")
        .eq("geo_bucket", gb)
        .eq("estado", uf)
        .ilike("cidade", f"%{cid}%")
        .limit(lim)
        .execute()
    )
    rows = list(getattr(resp, "data", None) or [])

    def _key(r: dict[str, Any]) -> tuple[int, str]:
        try:
            n = int(r.get("n_amostras") or 0)
        except (TypeError, ValueError):
            n = 0
        ts = str(r.get("atualizado_em") or "")
        return (-n, ts)

    rows.sort(key=_key)
    return rows


def buscar_por_id(cache_id: str, client: Client) -> Optional[dict[str, Any]]:
    iid = str(cache_id or "").strip()
    if not iid:
        return None
    resp = client.table(TABELA_CACHE_MEDIA_BAIRRO).select("*").eq("id", iid).limit(1).execute()
    rows = getattr(resp, "data", None) or []
    return rows[0] if rows else None


_LISTAR_CACHE_UI_MAX = 400


def listar_resumo_vinculo(
    client: Client,
    *,
    limite: int = 600,
) -> list[dict[str, Any]]:
    """
    Lista caches para a UI de vínculo (id, nome, local, n amostras).
    """
    lim = max(1, min(int(limite or 600), 1000))
    resp = (
        client.table(TABELA_CACHE_MEDIA_BAIRRO)
        .select("id,nome_cache,cidade,estado,n_amostras,atualizado_em")
        .order("atualizado_em", desc=True)
        .limit(lim)
        .execute()
    )
    return list(getattr(resp, "data", None) or [])


def listar_filtro_ui(
    client: Client,
    *,
    estado: str = "",
    cidade_contem: str = "",
    bairro_contem: str = "",
    geo_bucket: str = "",
    chave_segmento_contem: str = "",
    chave_bairro_contem: str = "",
    limite: int = 200,
) -> list[dict[str, Any]]:
    """
    Lista caches de média com filtros opcionais. Ordenação: ``atualizado_em`` (mais recente primeiro).
    """
    lim = max(1, min(int(limite or 200), _LISTAR_CACHE_UI_MAX))
    q = client.table(TABELA_CACHE_MEDIA_BAIRRO).select("*")
    uf = (estado or "").strip().upper()[:2]
    if len(uf) == 2:
        q = q.eq("estado", uf)
    if (cidade_contem or "").strip():
        q = q.ilike("cidade", f"%{(cidade_contem or '').strip()}%")
    if (bairro_contem or "").strip():
        q = q.ilike("bairro", f"%{(bairro_contem or '').strip()}%")
    if (geo_bucket or "").strip():
        q = q.ilike("geo_bucket", f"%{(geo_bucket or '').strip()}%")
    if (chave_segmento_contem or "").strip():
        q = q.ilike("chave_segmento", f"%{(chave_segmento_contem or '').strip()}%")
    if (chave_bairro_contem or "").strip():
        q = q.ilike("chave_bairro", f"%{(chave_bairro_contem or '').strip()}%")
    q = q.order("atualizado_em", desc=True).limit(lim)
    resp = q.execute()
    return list(getattr(resp, "data", None) or [])


def apagar_por_id(client: Client, cache_id: str) -> None:
    cid = str(cache_id or "").strip()
    if not cid:
        return
    _executar_write_seguro(
        client.table(TABELA_CACHE_MEDIA_BAIRRO).delete().eq("id", cid),
        op_nome="delete",
    )
