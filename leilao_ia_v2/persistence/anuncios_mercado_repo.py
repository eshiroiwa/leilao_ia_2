"""Upsert em ``anuncios_mercado`` (comparáveis Viva Real e outros)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from supabase import Client

from leilao_ia_v2.constants import TABELA_ANUNCIOS_MERCADO

logger = logging.getLogger(__name__)

_BATCH = 50
_LISTAGEM_MAX = 900


def _executar_write_seguro(
    query: Any,
    *,
    op_nome: str,
    tabela: str = TABELA_ANUNCIOS_MERCADO,
) -> Any:
    resp = query.execute()
    if resp is None:
        raise RuntimeError(f"Supabase {op_nome} retornou resposta nula ({tabela}).")
    if not hasattr(resp, "data"):
        logger.warning("Supabase %s sem atributo data (%s).", op_nome, tabela)
    return resp


def _to_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def upsert_lote(client: Client, linhas: list[dict[str, Any]]) -> int:
    """
    Insere ou atualiza anúncios em lote (``on_conflict=url_anuncio``).
    Cada dict deve incluir pelo menos: url_anuncio, portal, tipo_imovel, bairro, cidade,
    estado, area_construida_m2, valor_venda, transacao (default venda), metadados_json.
    """
    if not linhas:
        return 0
    agora = datetime.now(timezone.utc).isoformat()
    norm: list[dict[str, Any]] = []
    descartadas = 0
    for raw in linhas:
        r = dict(raw)
        r["url_anuncio"] = str(r.get("url_anuncio") or "").strip()
        if not r["url_anuncio"]:
            descartadas += 1
            continue
        r["portal"] = str(r.get("portal") or "vivareal.com.br").strip()[:200]
        r["tipo_imovel"] = str(r.get("tipo_imovel") or "desconhecido").strip()
        r["logradouro"] = str(r.get("logradouro") or "").strip()
        r["bairro"] = str(r.get("bairro") or "").strip()
        r["cidade"] = str(r.get("cidade") or "").strip()
        r["estado"] = str(r.get("estado") or "").strip()
        r["transacao"] = str(r.get("transacao") or "venda").strip().lower()
        if r["transacao"] not in ("venda", "aluguel"):
            r["transacao"] = "venda"
        r.setdefault("metadados_json", {})
        if not isinstance(r["metadados_json"], dict):
            r["metadados_json"] = {}
        a = _to_float(r.get("area_construida_m2"))
        v = _to_float(r.get("valor_venda"))
        if a is None or v is None or a <= 0.0 or v <= 0.0:
            descartadas += 1
            continue
        r["area_construida_m2"] = a
        r["valor_venda"] = v
        preco_m2_in = _to_float(r.get("preco_m2"))
        r["preco_m2"] = round(preco_m2_in, 2) if (preco_m2_in is not None and preco_m2_in > 0) else round(v / a, 2)
        r["ultima_coleta_em"] = agora
        r.setdefault("primeiro_visto_em", agora)
        norm.append(r)

    if not norm:
        return 0

    for i in range(0, len(norm), _BATCH):
        batch = norm[i : i + _BATCH]
        _executar_write_seguro(
            client.table(TABELA_ANUNCIOS_MERCADO).upsert(batch, on_conflict="url_anuncio"),
            op_nome="upsert",
        )
    logger.info("Supabase: upsert anuncios_mercado count=%s descartadas=%s", len(norm), descartadas)
    return len(norm)


def listar_por_cidade_estado_tipos(
    client: Client,
    *,
    cidade: str,
    estado_sigla: str,
    tipos_imovel: list[str],
    limite: int = _LISTAGEM_MAX,
) -> list[dict[str, Any]]:
    """Lista anúncios de venda por cidade, UF (2 letras) e tipos (IN)."""
    cid = (cidade or "").strip()
    uf = (estado_sigla or "").strip().upper()[:2]
    tipos = [t.strip().lower() for t in tipos_imovel if str(t).strip()]
    if not cid or not uf or len(uf) != 2 or not tipos:
        return []
    lim = max(50, min(int(limite or _LISTAGEM_MAX), _LISTAGEM_MAX))
    q = (
        client.table(TABELA_ANUNCIOS_MERCADO)
        .select("*")
        .eq("transacao", "venda")
        .ilike("cidade", f"%{cid}%")
        .eq("estado", uf)
    )
    if len(tipos) == 1:
        q = q.eq("tipo_imovel", tipos[0])
    else:
        q = q.in_("tipo_imovel", tipos)
    resp = q.limit(lim).execute()
    return list(getattr(resp, "data", None) or [])


def atualizar_geolocalizacao(
    client: Client,
    anuncio_id: str,
    latitude: float,
    longitude: float,
) -> None:
    _executar_write_seguro(
        client.table(TABELA_ANUNCIOS_MERCADO)
        .update({"latitude": latitude, "longitude": longitude})
        .eq("id", str(anuncio_id)),
        op_nome="update geolocalizacao",
    )


def buscar_por_ids(client: Client, ids: list[str]) -> list[dict[str, Any]]:
    """Lê anúncios pelos UUIDs (lotes de até 50)."""
    limpos = [str(i).strip() for i in ids if str(i).strip()]
    if not limpos:
        return []
    out: list[dict[str, Any]] = []
    for i in range(0, len(limpos), 50):
        chunk = limpos[i : i + 50]
        resp = client.table(TABELA_ANUNCIOS_MERCADO).select("*").in_("id", chunk).execute()
        out.extend(list(getattr(resp, "data", None) or []))
    return out


_LISTAR_UI_MAX = 500


def listar_filtro_ui(
    client: Client,
    *,
    estado: str = "",
    cidade_contem: str = "",
    bairro_contem: str = "",
    tipo_imovel: str = "",
    url_contem: str = "",
    transacao: str = "",
    sem_coordenadas: bool = False,
    limite: int = 200,
) -> list[dict[str, Any]]:
    """
    Lista anúncios com filtros opcionais (UI administrativa). Ordenação: coleta mais recente primeiro.
    """
    lim = max(1, min(int(limite or 200), _LISTAR_UI_MAX))
    q = client.table(TABELA_ANUNCIOS_MERCADO).select("*")
    uf = (estado or "").strip().upper()[:2]
    if len(uf) == 2:
        q = q.eq("estado", uf)
    if (cidade_contem or "").strip():
        q = q.ilike("cidade", f"%{(cidade_contem or '').strip()}%")
    if (bairro_contem or "").strip():
        q = q.ilike("bairro", f"%{(bairro_contem or '').strip()}%")
    if (tipo_imovel or "").strip():
        q = q.eq("tipo_imovel", (tipo_imovel or "").strip().lower())
    t = (transacao or "").strip().lower()
    if t in ("venda", "aluguel"):
        q = q.eq("transacao", t)
    if (url_contem or "").strip():
        q = q.ilike("url_anuncio", f"%{(url_contem or '').strip()}%")
    if sem_coordenadas:
        q = q.or_("latitude.is.null,longitude.is.null")
    q = q.order("ultima_coleta_em", desc=True).limit(lim)
    resp = q.execute()
    return list(getattr(resp, "data", None) or [])


def atualizar_campos(
    client: Client,
    anuncio_id: str,
    campos: dict[str, Any],
) -> None:
    """
    Atualiza colunas mutáveis; recalcula ``preco_m2`` se área ou valor forem passados.
    Não use para trocar ``url_anuncio`` (chave de negócio); prefira deletar e reingerir.
    """
    iid = str(anuncio_id or "").strip()
    if not iid or not campos:
        return
    p = {k: v for k, v in campos.items() if k != "id"}
    if "area_construida_m2" in p or "valor_venda" in p:
        cur = (
            client.table(TABELA_ANUNCIOS_MERCADO)
            .select("area_construida_m2, valor_venda")
            .eq("id", iid)
            .limit(1)
            .execute()
        )
        rows = getattr(cur, "data", None) or []
        if not rows:
            return
        base_a = float(rows[0].get("area_construida_m2") or 0)
        base_v = float(rows[0].get("valor_venda") or 0)
        if "area_construida_m2" in p:
            try:
                base_a = float(p["area_construida_m2"])
            except (TypeError, ValueError):
                pass
        if "valor_venda" in p:
            try:
                base_v = float(p["valor_venda"])
            except (TypeError, ValueError):
                pass
        p["area_construida_m2"] = base_a
        p["valor_venda"] = base_v
        if base_a > 0:
            p["preco_m2"] = round(base_v / base_a, 2)
        else:
            p["preco_m2"] = None
    _executar_write_seguro(
        client.table(TABELA_ANUNCIOS_MERCADO).update(p).eq("id", iid),
        op_nome="update campos",
    )


def apagar_por_id(client: Client, anuncio_id: str) -> None:
    iid = str(anuncio_id or "").strip()
    if not iid:
        return
    _executar_write_seguro(
        client.table(TABELA_ANUNCIOS_MERCADO).delete().eq("id", iid),
        op_nome="delete",
    )
