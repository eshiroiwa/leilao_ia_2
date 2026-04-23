"""
Eficiência de tokens: cache de média de preço/m² com SEGMENTO de mercado (tipo, conservação,
andar, casa térrea/sobrado, eixo de rua) + triagem heurística antes de LLM caro.

DDL Supabase: ver `supabase_ddls_leilao_ia.sql` na raiz do projeto.

Chaves:
  - chave_bairro: só geografia (cidade|bairro ou estado).
  - chave_segmento: única; inclui dimensões normalizadas (tipo_imovel, conservacao, …).

Fluxo: busca hierárquica por segmento (rua -> sem rua -> sem faixa/conservação -> tipo) e,
se miss e `fallback_geografico`, usa a linha mais recente com mesmo chave_bairro.
Para `casa`, o hard stop do fallback amplo é controlado por `CACHE_HARD_STOP_CASA_ESTRITO` (default true).
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from dotenv import load_dotenv
from postgrest.exceptions import APIError
from pydantic import BaseModel, ConfigDict, Field
from supabase import Client

from agno.tools import tool

from ingestion_agent import get_supabase_client
from leilao_constants import (
    andar_de_registro,
    area_efetiva_de_registro,
    faixa_andar_de_numero,
    faixa_area_de_metragem,
    normalizar_conservacao,
    normalizar_tipo_casa,
    normalizar_tipo_imovel,
    segmento_mercado_de_registro,
    valor_arrematacao_de_registro,
)

load_dotenv()

logger = logging.getLogger(__name__)

CACHE_TABLE = "cache_media_bairro"
HARD_STOP_CASA_CACHE_ESTRITO = str(
    os.getenv("CACHE_HARD_STOP_CASA_ESTRITO", "true")
).strip().lower() not in ("0", "false", "nao", "não", "off")

_missing_cache_table_logged = False


def _is_cache_table_unavailable(exc: BaseException) -> bool:
    """PostgREST: tabela ausente ou ainda não no schema cache (ex.: PGRST205)."""
    if isinstance(exc, APIError):
        if exc.code == "PGRST205":
            return True
        msg = (exc.message or "") + str(exc)
        return CACHE_TABLE in msg and "could not find the table" in msg.lower()
    return False


def _log_cache_table_missing_once() -> None:
    global _missing_cache_table_logged
    if _missing_cache_table_logged:
        return
    _missing_cache_table_logged = True
    logger.warning(
        "Tabela %r inexistente ou fora do schema PostgREST — triagem segue sem cache de bairro. "
        "Crie a tabela no Supabase (SQL no topo de token_efficiency.py) e recarregue o schema se precisar.",
        CACHE_TABLE,
    )


def _is_cache_segment_query_unavailable(exc: BaseException) -> bool:
    if not isinstance(exc, APIError):
        return False
    msg = f"{getattr(exc, 'message', '')} {exc}".lower()
    return "chave_segmento" in msg and ("column" in msg or "does not exist" in msg)


def normalizar_chave_bairro(cidade: str, bairro: str = "", estado: str = "") -> str:
    """Chave geográfica estável. Se `bairro` vazio, usa `estado` ou 'geral'."""

    def slug(part: str) -> str:
        part = unicodedata.normalize("NFKD", part.strip()).encode("ascii", "ignore").decode("ascii")
        part = part.lower()
        part = re.sub(r"[^a-z0-9]+", "-", part).strip("-")
        return part or "x"

    regiao = (bairro or "").strip() or (estado or "").strip() or "geral"
    return f"{slug(cidade)}|{slug(regiao)}"


def normalizar_chave_segmento(chave_bairro_geo: str, seg: dict[str, str]) -> str:
    """Compõe chave única: geografia + (microgeo opcional) + tipo, conservação, casa, andar, área, rua."""

    def part(key: str, default: str = "desconhecido") -> str:
        v = (seg.get(key) or default or "").strip().lower()
        v = unicodedata.normalize("NFKD", v).encode("ascii", "ignore").decode("ascii")
        v = re.sub(r"[^a-z0-9]+", "-", v).strip("-")
        return v or "x"

    geo_bucket = part("geo_bucket", "")
    geo_part = f"|geo={geo_bucket}" if geo_bucket not in ("x", "") else ""
    return (
        f"{chave_bairro_geo}{geo_part}|tipo={part('tipo_imovel')}|cons={part('conservacao')}"
        f"|cas={part('tipo_casa', '-')}|and={part('faixa_andar', '-')}"
        f"|area={part('faixa_area', '-')}|rua={part('logradouro_chave', '-')}"
    )


def merge_segmento_mercado(
    registro: Optional[dict[str, Any]] = None,
    overrides: Optional[dict[str, Optional[str]]] = None,
) -> dict[str, str]:
    base = segmento_mercado_de_registro(registro or {})
    if not overrides:
        return base
    o = {str(k): v for k, v in overrides.items() if v is not None and str(v).strip()}
    if t := o.get("tipo_imovel"):
        base["tipo_imovel"] = normalizar_tipo_imovel(t)
    if c := o.get("conservacao"):
        base["conservacao"] = normalizar_conservacao(c)
    if o.get("tipo_casa"):
        base["tipo_casa"] = normalizar_tipo_casa(o["tipo_casa"], base["tipo_imovel"])
    if o.get("faixa_andar"):
        base["faixa_andar"] = _faixa_andar_de_texto(o["faixa_andar"])
    if o.get("andar") is not None:
        try:
            an = int(float(str(o["andar"]).replace(",", ".")))
            if base["tipo_imovel"] == "apartamento":
                base["faixa_andar"] = faixa_andar_de_numero(an)
        except (TypeError, ValueError):
            pass
    if lk := o.get("logradouro_chave"):
        base["logradouro_chave"] = _slug_segment_part(lk)
    if gb := o.get("geo_bucket"):
        base["geo_bucket"] = _slug_segment_part(gb)
    if fa := o.get("faixa_area"):
        base["faixa_area"] = _slug_segment_part(fa)
    if base["tipo_imovel"] != "casa":
        base["tipo_casa"] = "-"
        if base["faixa_andar"] == "casa":
            base["faixa_andar"] = faixa_andar_de_numero(andar_de_registro(registro or {}))
    return base


def _slug_segment_part(val: str) -> str:
    v = unicodedata.normalize("NFKD", val.strip()).encode("ascii", "ignore").decode("ascii")
    v = re.sub(r"[^a-z0-9]+", "-", v.lower()).strip("-")
    return v or "-"


def _faixa_andar_de_texto(s: str) -> str:
    t = s.strip().lower()
    for a in ("terreo", "térreo", "ground"):
        if a in t:
            return "terreo"
    if "baixo" in t:
        return "baixo"
    if "medio" in t or "médio" in t:
        return "medio"
    if "alto" in t:
        return "alto"
    if t in ("casa", "-", ""):
        return "-"
    return _slug_segment_part(s)


def _parse_float_any(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    if not s or s == ".":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _geo_bucket_de_coords(lat: float, lon: float, passo_graus: float = 0.005) -> str:
    """Grid geográfico simples (~550m em latitude com passo 0.005)."""
    if passo_graus <= 0:
        passo_graus = 0.005
    lat_b = round(round(lat / passo_graus) * passo_graus, 4)
    lon_b = round(round(lon / passo_graus) * passo_graus, 4)
    lat_h = "N" if lat_b >= 0 else "S"
    lon_h = "E" if lon_b >= 0 else "W"
    return f"{lat_h}{abs(lat_b):.4f}_{lon_h}{abs(lon_b):.4f}"


def geo_bucket_de_registro(registro: Optional[dict[str, Any]]) -> str:
    if not registro:
        return ""
    lat = _parse_float_any(registro.get("latitude") or registro.get("lat"))
    lon = _parse_float_any(registro.get("longitude") or registro.get("lon"))
    if lat is None or lon is None:
        raw_md = registro.get("metadados_json")
        md: dict[str, Any] = {}
        if isinstance(raw_md, str):
            try:
                md = json.loads(raw_md)
            except json.JSONDecodeError:
                md = {}
        elif isinstance(raw_md, dict):
            md = raw_md
        lat = _parse_float_any(md.get("latitude") or md.get("lat"))
        lon = _parse_float_any(md.get("longitude") or md.get("lon"))
    if lat is None or lon is None:
        return ""
    return _geo_bucket_de_coords(lat, lon)


def nome_cache_automatico(
    cidade: str,
    bairro: str,
    estado: str = "",
    tipo_imovel: str = "",
    *,
    sufixo: str | None = None,
) -> str:
    """
    Rótulo legível para o cache (sincronização automática ou fallback sem nome explícito).
    """
    c = (cidade or "").strip()
    b = (bairro or "").strip()
    e = (estado or "").strip().upper()
    t = (tipo_imovel or "").strip() or "desconhecido"
    partes: list[str] = []
    if c:
        partes.append(c)
    loc = b or e or "geral"
    if loc and (not partes or loc.casefold() not in partes[0].casefold()):
        partes.append(loc)
    elif not partes:
        partes.append("cache")
    head = " · ".join(partes)
    if t and t != "desconhecido":
        head = f"{head} · {t}"
    if sufixo:
        s = sufixo.strip()
        if s:
            head = f"{head} · {s}"
    return head[:200]


class CacheMediaBairroSalvar(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    cidade: str = Field(..., min_length=1)
    bairro: str = Field(default="", max_length=200)
    estado: str = Field(default="", max_length=50)
    preco_m2_medio: float = Field(..., gt=0)
    fonte: str = Field(default="datazap", max_length=120)
    metadados_json: Optional[str] = Field(default=None, max_length=8000)
    tipo_imovel: str = Field(default="desconhecido", max_length=40)
    conservacao: str = Field(default="desconhecido", max_length=40)
    tipo_casa: str = Field(default="-", max_length=40)
    faixa_andar: str = Field(default="-", max_length=40)
    faixa_area: str = Field(default="-", max_length=20)
    logradouro_chave: str = Field(default="-", max_length=200)
    geo_bucket: str = Field(default="", max_length=40)
    lat_ref: Optional[float] = Field(default=None, ge=-90, le=90)
    lon_ref: Optional[float] = Field(default=None, ge=-180, le=180)
    valor_medio_venda: Optional[float] = Field(default=None, ge=0)
    maior_valor_venda: Optional[float] = Field(default=None, ge=0)
    menor_valor_venda: Optional[float] = Field(default=None, ge=0)
    n_amostras: Optional[int] = Field(default=None, ge=0)
    anuncios_ids: Optional[str] = Field(default=None, max_length=5000)
    nome_cache: Optional[str] = Field(default=None, max_length=240)


class TriagemResultado(BaseModel):
    """Resultado da triagem heurística (sem chamadas ao LLM)."""

    descartar: bool = Field(
        ...,
        description="True = não vale processar com agente caro / edital",
    )
    preco_m2_leilao: Optional[float] = None
    preco_m2_medio_referencia: Optional[float] = None
    motivo: str
    chave_bairro: Optional[str] = None
    chave_segmento: Optional[str] = None
    cache_fallback_geografico: bool = Field(
        default=False,
        description="True se a média veio só de chave_bairro (sem match de segmento)",
    )
    cache_granularidade_match: Optional[str] = Field(
        default=None,
        description="Nível do cache usado: ex. segmento_microgeo_rua_exato, segmento_sem_rua, geo_fallback",
    )


def _parse_atualizado_em(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
    else:
        s = str(val).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _cache_dentro_do_prazo(row: dict[str, Any], max_idade_dias: int) -> bool:
    if max_idade_dias <= 0:
        return True
    ts = _parse_atualizado_em(row.get("atualizado_em"))
    if ts is None:
        return True
    limite = datetime.now(timezone.utc) - timedelta(days=max_idade_dias)
    return ts >= limite


def _supabase_select_cache_por_segmento_legacy(
    client: Client, chave_segmento: str, chave_bairro: str, fallback_geografico: bool
) -> tuple[list[dict[str, Any]], bool, Optional[str]]:
    """
    Retorna (linhas, usou_fallback_geografico).
    """
    try:
        resp = (
            client.table(CACHE_TABLE)
            .select("*")
            .eq("chave_segmento", chave_segmento)
            .limit(1)
            .execute()
        )
        rows = list(getattr(resp, "data", None) or [])
        if rows:
            return rows, False, "segmento_exato_legacy"
    except APIError as e:
        if _is_cache_table_unavailable(e):
            _log_cache_table_missing_once()
            return [], False, None
        if _is_cache_segment_query_unavailable(e):
            logger.info("Cache sem coluna chave_segmento disponível; usando fallback por chave_bairro")
            if not fallback_geografico:
                return [], False, None
            resp = (
                client.table(CACHE_TABLE)
                .select("*")
                .eq("chave_bairro", chave_bairro)
                .order("atualizado_em", desc=True)
                .limit(1)
                .execute()
            )
            rows = list(getattr(resp, "data", None) or [])
            return rows, bool(rows), ("geo_fallback_sem_chave_segmento" if rows else None)
        logger.debug("Select cache por chave_segmento: %s", e)
    if not fallback_geografico:
        return [], False, None
    try:
        resp = (
            client.table(CACHE_TABLE)
            .select("*")
            .eq("chave_bairro", chave_bairro)
            .order("atualizado_em", desc=True)
            .limit(1)
            .execute()
        )
        rows = list(getattr(resp, "data", None) or [])
        return rows, bool(rows), ("geo_fallback_legacy" if rows else None)
    except APIError as e:
        if _is_cache_table_unavailable(e):
            _log_cache_table_missing_once()
            return [], False, None
        raise


def _supabase_select_cache_hierarquico(
    client: Client,
    *,
    chave_segmento: str,
    chave_bairro: str,
    seg: dict[str, str],
    fallback_geografico: bool,
) -> tuple[list[dict[str, Any]], bool, Optional[str]]:
    """
    Busca no cache com granularidade progressiva:
      1) rua + segmento completo (incl. faixa_area)
      2) sem rua (com faixa_area)
      3) sem rua, sem faixa de andar (com faixa_area)
      4) sem rua/faixa/rua, sem faixa_area (tipo + conservação + casa)
      5) conservação genérica (tipo + casa)
      6) tipo apenas
    Se falhar, recua para estratégia legacy (chave_segmento exata).
    """
    tipo = seg.get("tipo_imovel") or "desconhecido"
    cons = seg.get("conservacao") or "desconhecido"
    tc = seg.get("tipo_casa") or "-"
    faixa = seg.get("faixa_andar") or "-"
    fa = seg.get("faixa_area") or "-"
    rua = seg.get("logradouro_chave") or "-"
    geo = seg.get("geo_bucket") or ""

    niveis: list[tuple[str, dict[str, str]]] = [
        (
            "segmento_microgeo_rua_exato",
            {
                "geo_bucket": geo,
                "tipo_imovel": tipo,
                "conservacao": cons,
                "tipo_casa": tc,
                "faixa_andar": faixa,
                "faixa_area": fa,
                "logradouro_chave": rua,
            },
        ),
        (
            "segmento_microgeo_sem_rua",
            {
                "geo_bucket": geo,
                "tipo_imovel": tipo,
                "conservacao": cons,
                "tipo_casa": tc,
                "faixa_andar": faixa,
                "faixa_area": fa,
                "logradouro_chave": "-",
            },
        ),
        (
            "segmento_rua_exato",
            {
                "geo_bucket": "",
                "tipo_imovel": tipo,
                "conservacao": cons,
                "tipo_casa": tc,
                "faixa_andar": faixa,
                "faixa_area": fa,
                "logradouro_chave": rua,
            },
        ),
        (
            "segmento_sem_rua",
            {
                "geo_bucket": "",
                "tipo_imovel": tipo,
                "conservacao": cons,
                "tipo_casa": tc,
                "faixa_andar": faixa,
                "faixa_area": fa,
                "logradouro_chave": "-",
            },
        ),
        (
            "segmento_sem_rua_sem_faixa_andar",
            {
                "geo_bucket": "",
                "tipo_imovel": tipo,
                "conservacao": cons,
                "tipo_casa": tc,
                "faixa_andar": "-",
                "faixa_area": fa,
                "logradouro_chave": "-",
            },
        ),
        (
            "segmento_sem_rua_sem_faixa_area",
            {
                "geo_bucket": "",
                "tipo_imovel": tipo,
                "conservacao": cons,
                "tipo_casa": tc,
                "faixa_andar": "-",
                "faixa_area": "-",
                "logradouro_chave": "-",
            },
        ),
        (
            "segmento_conservacao_generica",
            {
                "geo_bucket": "",
                "tipo_imovel": tipo,
                "conservacao": "desconhecido",
                "tipo_casa": tc,
                "faixa_andar": "-",
                "faixa_area": "-",
                "logradouro_chave": "-",
            },
        ),
        (
            "segmento_tipo_apenas",
            {
                "geo_bucket": "",
                "tipo_imovel": tipo,
                "conservacao": "desconhecido",
                "tipo_casa": "-",
                "faixa_andar": "-",
                "faixa_area": "-",
                "logradouro_chave": "-",
            },
        ),
    ]
    eh_casa = (tipo or "").strip().lower() == "casa"
    niveis_permitidos_casa = {
        "segmento_microgeo_rua_exato",
        "segmento_microgeo_sem_rua",
        "segmento_rua_exato",
        "segmento_sem_rua",
        "segmento_sem_rua_sem_faixa_andar",
    }

    try:
        vistos: set[str] = set()
        for nivel, filtros in niveis:
            if eh_casa and nivel not in niveis_permitidos_casa:
                continue
            if filtros["geo_bucket"] == "" and "microgeo" in nivel:
                continue
            chave = normalizar_chave_segmento(chave_bairro, filtros)
            if chave in vistos:
                continue
            vistos.add(chave)
            resp = (
                client.table(CACHE_TABLE)
                .select("*")
                .eq("chave_segmento", chave)
                .limit(1)
                .execute()
            )
            rows = list(getattr(resp, "data", None) or [])
            if rows:
                return rows, False, nivel
    except APIError as e:
        if _is_cache_table_unavailable(e):
            _log_cache_table_missing_once()
            return [], False, None
        if _is_cache_segment_query_unavailable(e):
            logger.info("Busca hierárquica por chave_segmento indisponível; usando fallback por chave_bairro")
            if not fallback_geografico:
                return [], False, None
            resp = (
                client.table(CACHE_TABLE)
                .select("*")
                .eq("chave_bairro", chave_bairro)
                .order("atualizado_em", desc=True)
                .limit(1)
                .execute()
            )
            rows = list(getattr(resp, "data", None) or [])
            return rows, bool(rows), ("geo_fallback_sem_chave_segmento" if rows else None)
        logger.debug("Busca hierárquica por chave_segmento falhou; recuando para legacy: %s", e)
        return _supabase_select_cache_por_segmento_legacy(
            client, chave_segmento, chave_bairro, fallback_geografico
        )

    if eh_casa and HARD_STOP_CASA_CACHE_ESTRITO:
        # Hard stop: para casas, não usa níveis abaixo de segmento_sem_rua nem geo_fallback.
        return [], False, "hard_stop_casa_sem_match_especifico"

    if not fallback_geografico:
        return [], False, None

    try:
        resp = (
            client.table(CACHE_TABLE)
            .select("*")
            .eq("chave_bairro", chave_bairro)
            .order("atualizado_em", desc=True)
            .limit(1)
            .execute()
        )
        rows = list(getattr(resp, "data", None) or [])
        return rows, bool(rows), ("geo_fallback" if rows else None)
    except APIError as e:
        if _is_cache_table_unavailable(e):
            _log_cache_table_missing_once()
            return [], False, None
        raise


def _id_de_resposta_upsert_cache(up: Any) -> Optional[str]:
    """Lê o UUID retornado pelo PostgREST após upsert (Prefer: return=representation)."""
    if up is None:
        return None
    data = getattr(up, "data", None)
    if isinstance(data, list) and data:
        rid = data[0].get("id")
        return str(rid).strip() if rid else None
    if isinstance(data, dict):
        rid = data.get("id")
        return str(rid).strip() if rid else None
    return None


def id_cache_media_por_chave_segmento(
    chave_segmento: str,
    *,
    client: Optional[Client] = None,
) -> Optional[str]:
    """Resolve o id da linha em ``cache_media_bairro`` pela chave de segmento (única)."""
    ck = (chave_segmento or "").strip()
    if not ck:
        return None
    cli = client or get_supabase_client()
    try:
        resp = cli.table(CACHE_TABLE).select("id").eq("chave_segmento", ck).limit(1).execute()
        rows = list(getattr(resp, "data", None) or [])
        if not rows:
            return None
        rid = rows[0].get("id")
        return str(rid).strip() if rid else None
    except APIError as e:
        if _is_cache_table_unavailable(e):
            _log_cache_table_missing_once()
            return None
        logger.debug("id_cache_media_por_chave_segmento: %s", e)
        return None
    except Exception:
        logger.debug("id_cache_media_por_chave_segmento falhou", exc_info=True)
        return None


def _supabase_upsert_cache(client: Client, row: dict[str, Any]) -> Any:
    try:
        return client.table(CACHE_TABLE).upsert(row, on_conflict="chave_segmento").execute()
    except APIError as e:
        if _is_cache_table_unavailable(e):
            _log_cache_table_missing_once()
            return None
        try:
            leg = {
                "chave_bairro": row["chave_bairro"],
                "cidade": row["cidade"],
                "bairro": row["bairro"],
                "preco_m2_medio": row["preco_m2_medio"],
                "fonte": row["fonte"],
            }
            if row.get("metadados_json") is not None:
                leg["metadados_json"] = row["metadados_json"]
            return client.table(CACHE_TABLE).upsert(leg, on_conflict="chave_bairro").execute()
        except APIError:
            raise e


def buscar_media_bairro_no_cache(
    cidade: str,
    bairro: str = "",
    *,
    estado: str = "",
    registro: Optional[dict[str, Any]] = None,
    segmento: Optional[dict[str, Optional[str]]] = None,
    fallback_geografico: bool = True,
    client: Optional[Client] = None,
    max_idade_dias: int = 90,
) -> Optional[dict[str, Any]]:
    """
    Retorna linha do cache (preco_m2_medio) ou None.
    Usa `chave_segmento` a partir de `registro` e/ou `segmento`; opcionalmente cai para só geografia.
    """
    chave_geo = normalizar_chave_bairro(cidade, bairro, estado)
    seg = merge_segmento_mercado(registro, segmento)
    if "geo_bucket" not in seg or not (seg.get("geo_bucket") or "").strip():
        gb = geo_bucket_de_registro(registro)
        if gb:
            seg["geo_bucket"] = gb
    chave_seg = normalizar_chave_segmento(chave_geo, seg)
    cli = client or get_supabase_client()
    rows, usou_fb, granularidade = _supabase_select_cache_hierarquico(
        cli,
        chave_segmento=chave_seg,
        chave_bairro=chave_geo,
        seg=seg,
        fallback_geografico=fallback_geografico,
    )
    if not rows:
        logger.info("Cache miss: segmento=%s geo=%s", chave_seg, chave_geo)
        return None
    row = rows[0]
    if not _cache_dentro_do_prazo(row, max_idade_dias):
        logger.info("Cache expirado: %s", chave_seg)
        return None
    row = {
        **row,
        "_cache_fallback_geografico": usou_fb,
        "_cache_granularidade_match": granularidade,
    }
    logger.info(
        "Cache hit: preco_m2=%s segmento=%s fallback_geo=%s granularidade=%s",
        row.get("preco_m2_medio"),
        chave_seg,
        usou_fb,
        granularidade,
    )
    return row


def salvar_media_bairro_no_cache(
    payload: CacheMediaBairroSalvar,
    *,
    client: Optional[Client] = None,
) -> dict[str, Any]:
    """Grava ou atualiza média R$/m² (upsert por chave_segmento quando suportado pelo BD)."""
    chave_geo = normalizar_chave_bairro(payload.cidade, payload.bairro, payload.estado)
    bairro_grava = (payload.bairro or "").strip() or (payload.estado or "").strip() or "geral"
    seg = {
        "tipo_imovel": payload.tipo_imovel,
        "conservacao": payload.conservacao,
        "tipo_casa": payload.tipo_casa,
        "faixa_andar": payload.faixa_andar,
        "faixa_area": payload.faixa_area,
        "logradouro_chave": payload.logradouro_chave,
        "geo_bucket": payload.geo_bucket,
    }
    chave_seg = normalizar_chave_segmento(chave_geo, seg)
    row: dict[str, Any] = {
        "chave_bairro": chave_geo,
        "chave_segmento": chave_seg,
        "cidade": payload.cidade.strip(),
        "bairro": bairro_grava,
        "estado": (payload.estado or "").strip() or None,
        "tipo_imovel": payload.tipo_imovel,
        "conservacao": payload.conservacao,
        "tipo_casa": payload.tipo_casa,
        "faixa_andar": payload.faixa_andar,
        "faixa_area": payload.faixa_area,
        "logradouro_chave": payload.logradouro_chave,
        "geo_bucket": (payload.geo_bucket or "").strip() or None,
        "lat_ref": payload.lat_ref,
        "lon_ref": payload.lon_ref,
        "preco_m2_medio": float(payload.preco_m2_medio),
        "fonte": payload.fonte.strip(),
    }
    nome_c = (payload.nome_cache or "").strip()
    if not nome_c:
        nome_c = nome_cache_automatico(
            payload.cidade,
            payload.bairro,
            payload.estado,
            payload.tipo_imovel,
        )
    row["nome_cache"] = nome_c[:240]
    if payload.valor_medio_venda is not None:
        row["valor_medio_venda"] = round(float(payload.valor_medio_venda), 2)
    if payload.maior_valor_venda is not None:
        row["maior_valor_venda"] = round(float(payload.maior_valor_venda), 2)
    if payload.menor_valor_venda is not None:
        row["menor_valor_venda"] = round(float(payload.menor_valor_venda), 2)
    if payload.n_amostras is not None:
        row["n_amostras"] = int(payload.n_amostras)
    if payload.anuncios_ids is not None:
        row["anuncios_ids"] = payload.anuncios_ids
    if row["estado"] is None:
        row.pop("estado", None)
    if row.get("geo_bucket") is None:
        row.pop("geo_bucket", None)
    if row.get("lat_ref") is None:
        row.pop("lat_ref", None)
    if row.get("lon_ref") is None:
        row.pop("lon_ref", None)
    if payload.metadados_json is not None:
        row["metadados_json"] = payload.metadados_json
    cli = client or get_supabase_client()
    up = _supabase_upsert_cache(cli, row)
    if up is None:
        return {
            "ok": False,
            "chave_bairro": chave_geo,
            "chave_segmento": chave_seg,
            "preco_m2_medio": payload.preco_m2_medio,
            "skipped": True,
            "motivo": "tabela_cache_indisponivel",
        }
    resolved_id = _id_de_resposta_upsert_cache(up) or id_cache_media_por_chave_segmento(chave_seg, client=cli)
    logger.info("Cache salvo: segmento=%s = R$ %.2f/m²", chave_seg, payload.preco_m2_medio)
    return {
        "ok": True,
        "chave_bairro": chave_geo,
        "chave_segmento": chave_seg,
        "preco_m2_medio": payload.preco_m2_medio,
        "cache_media_bairro_id": resolved_id,
    }


def triagem_preco_m2_acima_media_bairro(
    valor_lance: float,
    area_m2: float,
    preco_m2_medio_bairro: Optional[float],
) -> TriagemResultado:
    """
    Se preço/m² do leilão > média do bairro, retorna descartar=True (economiza tokens do LLM).
    Sem média de referência, não descarta (não há base para comparar).
    """
    if area_m2 <= 0:
        return TriagemResultado(
            descartar=False,
            motivo="area_m2_invalida_triagem_nao_aplicada",
        )
    if valor_lance <= 0:
        return TriagemResultado(
            descartar=False,
            motivo="valor_lance_invalido_triagem_nao_aplicada",
        )
    pm2 = valor_lance / area_m2
    if preco_m2_medio_bairro is None:
        return TriagemResultado(
            descartar=False,
            preco_m2_leilao=round(pm2, 4),
            motivo="sem_media_bairro_prosseguir_ou_obter_cache",
        )
    ref = float(preco_m2_medio_bairro)
    if pm2 > ref:
        return TriagemResultado(
            descartar=True,
            preco_m2_leilao=round(pm2, 4),
            preco_m2_medio_referencia=ref,
            motivo="preco_m2_leilao_maior_que_media_bairro",
        )
    return TriagemResultado(
        descartar=False,
        preco_m2_leilao=round(pm2, 4),
        preco_m2_medio_referencia=ref,
        motivo="preco_m2_leilao_aceitavel_vs_media_bairro",
    )


def triagem_heuristica_antes_do_llm(
    valor_lance: float,
    area_m2: float,
    cidade: str,
    bairro: str = "",
    *,
    estado: str = "",
    registro: Optional[dict[str, Any]] = None,
    fallback_geografico: bool = True,
    client: Optional[Client] = None,
    max_idade_cache_dias: int = 90,
) -> TriagemResultado:
    """
    Usa cache segmentado (tipo, conservação, andar, rua) quando possível; cai para só geografia se permitido.
    """
    chave_geo = normalizar_chave_bairro(cidade, bairro, estado)
    seg = merge_segmento_mercado(registro, None)
    chave_seg = normalizar_chave_segmento(chave_geo, seg)
    row = buscar_media_bairro_no_cache(
        cidade,
        bairro,
        estado=estado,
        registro=registro,
        fallback_geografico=fallback_geografico,
        client=client,
        max_idade_dias=max_idade_cache_dias,
    )
    fb = False
    gran = None
    if row:
        fb = bool(row.pop("_cache_fallback_geografico", False))
        gran = row.pop("_cache_granularidade_match", None)
    media = float(row["preco_m2_medio"]) if row and row.get("preco_m2_medio") is not None else None
    out = triagem_preco_m2_acima_media_bairro(valor_lance, area_m2, media)
    return out.model_copy(
        update={
            "chave_bairro": chave_geo,
            "chave_segmento": chave_seg,
            "cache_fallback_geografico": fb,
            "cache_granularidade_match": gran,
        }
    )


def triagem_heuristica_de_registro_leilao(
    registro: dict[str, Any],
    *,
    cidade: Optional[str] = None,
    bairro: Optional[str] = None,
    estado: Optional[str] = None,
    fallback_geografico: bool = True,
    client: Optional[Client] = None,
    max_idade_cache_dias: int = 90,
) -> TriagemResultado:
    """
    Extrai lance/área/cidade/bairro do dict (ex.: linha do Supabase) e executa a triagem.
    Aceita `valor_arrematacao` ou lances legados; `area_util` ou `area_m2`.
    Com coluna `bairro` no BD ou planilha (overlay); se vazio, usa `estado` para cache.
    """
    cid = (cidade or registro.get("cidade") or registro.get("cidade_imovel") or "").strip()
    est = (estado or registro.get("estado") or registro.get("uf") or "").strip()
    bai = (bairro or registro.get("bairro") or registro.get("bairro_imovel") or "").strip()
    if not cid:
        return TriagemResultado(
            descartar=False,
            motivo="sem_cidade_triagem_nao_executada",
        )
    if not bai and not est:
        return TriagemResultado(
            descartar=False,
            motivo="sem_bairro_nem_estado_para_cache_triagem_nao_executada",
        )
    lance = valor_arrematacao_de_registro(registro)
    if lance is None:
        return TriagemResultado(
            descartar=False,
            motivo="sem_valor_arrematacao_triagem_nao_executada",
        )
    area_f = area_efetiva_de_registro(registro)
    return triagem_heuristica_antes_do_llm(
        lance,
        area_f,
        cid,
        bairro=bai,
        estado=est,
        registro=registro,
        fallback_geografico=fallback_geografico,
        client=client,
        max_idade_cache_dias=max_idade_cache_dias,
    )


# --- Tools Agno (valuation / orquestração) -----------------------------------------------------


@tool(
    show_result=True,
    instructions=(
        "Sempre chame ANTES de consultar_referencia_mercado_datazap. Se retornar hit com preco_m2_medio, "
        "reutilize esse valor para vários imóveis do mesmo bairro e evite chamadas repetidas à API."
    ),
)
def buscar_cache_media_bairro_supabase(
    cidade: str,
    bairro: str = "",
    estado: str = "",
    max_idade_dias: int = 90,
) -> str:
    """Consulta média R$/m² do bairro no Supabase (cache). Retorna JSON com hit/miss e dados."""
    try:
        row = buscar_media_bairro_no_cache(
            cidade, bairro, estado=estado, max_idade_dias=max_idade_dias
        )
        if row is None:
            return json.dumps(
                {"hit": False, "chave_bairro": normalizar_chave_bairro(cidade, bairro, estado)},
                ensure_ascii=False,
            )
        return json.dumps({"hit": True, "cache": row}, ensure_ascii=False, default=str)
    except Exception as e:
        logger.exception("Erro ao ler cache de bairro")
        return json.dumps({"hit": False, "erro": str(e)}, ensure_ascii=False)


@tool(
    show_result=True,
    instructions=(
        "Após obter média de R$/m² via DataZap ou pesquisa confiável, grave no cache para reutilização. "
        "JSON: cidade, preco_m2_medio, fonte; bairro e/ou estado (se sem bairro no imóvel)."
    ),
)
def salvar_cache_media_bairro_supabase(payload_json: str) -> str:
    """Persiste média do bairro (upsert) para economizar chamadas futuras à API."""
    try:
        data = json.loads(payload_json)
        p = CacheMediaBairroSalvar.model_validate(data)
        r = salvar_media_bairro_no_cache(p)
        return json.dumps(r, ensure_ascii=False)
    except Exception as e:
        logger.exception("Erro ao salvar cache de bairro")
        return json.dumps({"ok": False, "erro": str(e)}, ensure_ascii=False)


__all__ = [
    "CACHE_TABLE",
    "CacheMediaBairroSalvar",
    "nome_cache_automatico",
    "TriagemResultado",
    "buscar_cache_media_bairro_supabase",
    "buscar_media_bairro_no_cache",
    "id_cache_media_por_chave_segmento",
    "merge_segmento_mercado",
    "geo_bucket_de_registro",
    "normalizar_chave_bairro",
    "normalizar_chave_segmento",
    "salvar_cache_media_bairro_supabase",
    "salvar_media_bairro_no_cache",
    "triagem_heuristica_antes_do_llm",
    "triagem_heuristica_de_registro_leilao",
    "triagem_preco_m2_acima_media_bairro",
]
