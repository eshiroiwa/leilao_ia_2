"""
Consulta opcional ao saldo de créditos Firecrawl para UI Streamlit.

Cache em memória (TTL curto) para não disparar HTTP a cada rerun.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

logger = logging.getLogger(__name__)

_CACHE: dict[str, Any] = {"ts": 0.0, "fc": "", "gcp": {}}
_TTL_SEC = 45.0


def _fmt_num_br(n: float | int) -> str:
    s = f"{float(n):,.0f}" if isinstance(n, int) or float(n).is_integer() else f"{float(n):,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_brl(n: float | int) -> str:
    return f"R$ {_fmt_num_br(n)}"


def _float_env(name: str, default: float = 0.0) -> float:
    raw = str(os.getenv(name, "") or "").strip().replace(",", ".")
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _validar_table_id_bq(table_id: str) -> bool:
    """
    Aceita apenas formato project.dataset.table.
    Não permite backticks ou outros chars perigosos.
    """
    t = str(table_id or "").strip()
    if not t:
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_\-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", t))


def buscar_saldo_firecrawl_texto() -> str:
    """Saldo de créditos Firecrawl (GET /v2/team/credit-usage)."""
    key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    if not key:
        return "— (sem FIRECRAWL_API_KEY)"
    try:
        import httpx

        r = httpx.get(
            "https://api.firecrawl.dev/v2/team/credit-usage",
            headers={"Authorization": f"Bearer {key}"},
            timeout=15.0,
        )
        if r.status_code >= 400:
            return f"n/d (HTTP {r.status_code})"
        body = r.json()
        data = body.get("data") or body
        rem = data.get("remainingCredits")
        if rem is None:
            rem = data.get("remaining_credits")
        if rem is not None:
            return _fmt_num_br(float(rem)) + " créditos"
        return "n/d (resposta sem remainingCredits)"
    except Exception as e:
        logger.info("Firecrawl credit-usage: %s", e)
        return "n/d"


def buscar_saldo_firecrawl_cached() -> str:
    global _CACHE
    now = time.time()
    ts = float(_CACHE.get("ts") or 0)
    if ts > 0 and (now - ts) < _TTL_SEC:
        return str(_CACHE.get("fc", ""))
    fc = buscar_saldo_firecrawl_texto()
    _CACHE = {"ts": now, "fc": fc}
    return fc


def buscar_gastos_google_maps_mes() -> dict[str, Any]:
    """
    Lê custo mensal de Google Maps/Geocoding via export de Billing no BigQuery.

    Requer env:
      - GCP_BILLING_BQ_TABLE=project.dataset.gcp_billing_export_v1_xxx
    Opcional:
      - GCP_BILLING_PROJECT_ID=project para autenticação BigQuery
      - GOOGLE_MAPS_BUDGET_BRL_MENSAL=orçamento mensal em BRL
    """
    table_id = str(os.getenv("GCP_BILLING_BQ_TABLE", "") or "").strip()
    if not table_id:
        return {
            "ok": False,
            "status": "— (sem GCP_BILLING_BQ_TABLE)",
            "total_brl": 0.0,
            "geocoding_brl": 0.0,
            "addr_validation_brl": 0.0,
            "places_brl": 0.0,
            "budget_brl": _float_env("GOOGLE_MAPS_BUDGET_BRL_MENSAL", 0.0),
            "budget_pct": 0.0,
            "risco": "n/d",
        }
    if not _validar_table_id_bq(table_id):
        return {
            "ok": False,
            "status": "n/d (GCP_BILLING_BQ_TABLE inválido)",
            "total_brl": 0.0,
            "geocoding_brl": 0.0,
            "addr_validation_brl": 0.0,
            "places_brl": 0.0,
            "budget_brl": _float_env("GOOGLE_MAPS_BUDGET_BRL_MENSAL", 0.0),
            "budget_pct": 0.0,
            "risco": "n/d",
        }
    try:
        from google.cloud import bigquery
    except Exception:
        return {
            "ok": False,
            "status": "n/d (google-cloud-bigquery não instalado)",
            "total_brl": 0.0,
            "geocoding_brl": 0.0,
            "addr_validation_brl": 0.0,
            "places_brl": 0.0,
            "budget_brl": _float_env("GOOGLE_MAPS_BUDGET_BRL_MENSAL", 0.0),
            "budget_pct": 0.0,
            "risco": "n/d",
        }
    query = f"""
    WITH b AS (
      SELECT
        DATE(usage_start_time) AS d,
        LOWER(IFNULL(service.description, '')) AS svc,
        LOWER(IFNULL(sku.description, '')) AS sku,
        CAST(IFNULL(cost, 0) AS FLOAT64)
          + IFNULL((SELECT SUM(CAST(c.amount AS FLOAT64)) FROM UNNEST(credits) c), 0) AS net_cost
      FROM `{table_id}`
      WHERE DATE(usage_start_time) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
        AND DATE(usage_start_time) < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
    )
    SELECT
      ROUND(SUM(
        CASE
          WHEN REGEXP_CONTAINS(CONCAT(svc, ' ', sku),
            r'(google maps|geocoding|address validation|places|routes|distance matrix|directions|map tiles)')
          THEN net_cost ELSE 0
        END
      ), 4) AS total_brl,
      ROUND(SUM(
        CASE WHEN REGEXP_CONTAINS(sku, r'geocod') THEN net_cost ELSE 0 END
      ), 4) AS geocoding_brl,
      ROUND(SUM(
        CASE WHEN REGEXP_CONTAINS(sku, r'address validation') THEN net_cost ELSE 0 END
      ), 4) AS addr_validation_brl,
      ROUND(SUM(
        CASE WHEN REGEXP_CONTAINS(sku, r'places') THEN net_cost ELSE 0 END
      ), 4) AS places_brl
    FROM b
    """
    try:
        project_id = str(os.getenv("GCP_BILLING_PROJECT_ID", "") or "").strip() or None
        client = bigquery.Client(project=project_id) if project_id else bigquery.Client()
        row = next(iter(client.query(query).result()), None)
        total = float(getattr(row, "total_brl", 0.0) or 0.0) if row is not None else 0.0
        geoc = float(getattr(row, "geocoding_brl", 0.0) or 0.0) if row is not None else 0.0
        addrv = float(getattr(row, "addr_validation_brl", 0.0) or 0.0) if row is not None else 0.0
        places = float(getattr(row, "places_brl", 0.0) or 0.0) if row is not None else 0.0
        budget = _float_env("GOOGLE_MAPS_BUDGET_BRL_MENSAL", 0.0)
        pct = (total / budget * 100.0) if budget > 0 else 0.0
        risco = "baixo"
        if budget <= 0:
            risco = "n/d"
        elif pct >= 100:
            risco = "estourado"
        elif pct >= 90:
            risco = "alto"
        elif pct >= 70:
            risco = "médio"
        return {
            "ok": True,
            "status": "ok",
            "total_brl": total,
            "geocoding_brl": geoc,
            "addr_validation_brl": addrv,
            "places_brl": places,
            "budget_brl": budget,
            "budget_pct": pct,
            "risco": risco,
        }
    except Exception as e:
        logger.info("Google Maps Billing (BQ): %s", e)
        return {
            "ok": False,
            "status": "n/d",
            "total_brl": 0.0,
            "geocoding_brl": 0.0,
            "addr_validation_brl": 0.0,
            "places_brl": 0.0,
            "budget_brl": _float_env("GOOGLE_MAPS_BUDGET_BRL_MENSAL", 0.0),
            "budget_pct": 0.0,
            "risco": "n/d",
        }


def buscar_gastos_google_maps_mes_cached() -> dict[str, Any]:
    global _CACHE
    now = time.time()
    ts = float(_CACHE.get("ts") or 0)
    if ts > 0 and (now - ts) < _TTL_SEC:
        gcp = _CACHE.get("gcp")
        if isinstance(gcp, dict):
            return gcp
    gcp = buscar_gastos_google_maps_mes()
    _CACHE = {"ts": now, "fc": str(_CACHE.get("fc", "")), "gcp": gcp}
    return gcp


def resumo_google_maps_para_ui(data: dict[str, Any]) -> dict[str, str]:
    total = float(data.get("total_brl") or 0.0)
    geoc = float(data.get("geocoding_brl") or 0.0)
    addrv = float(data.get("addr_validation_brl") or 0.0)
    places = float(data.get("places_brl") or 0.0)
    budget = float(data.get("budget_brl") or 0.0)
    pct = float(data.get("budget_pct") or 0.0)
    risco = str(data.get("risco") or "n/d")
    pct_txt = f"{pct:.1f}%"
    if budget <= 0:
        pct_txt = "—"
    return {
        "status": str(data.get("status") or "n/d"),
        "total": _fmt_brl(total),
        "geocoding": _fmt_brl(geoc),
        "addr_validation": _fmt_brl(addrv),
        "places": _fmt_brl(places),
        "budget": (_fmt_brl(budget) if budget > 0 else "—"),
        "budget_pct": pct_txt,
        "risco": risco,
    }


def invalidar_cache_saldos() -> None:
    global _CACHE
    _CACHE = {"ts": 0.0, "fc": "", "gcp": {}}
