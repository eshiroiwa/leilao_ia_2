"""
Consulta opcional ao saldo de créditos Firecrawl para UI Streamlit.

Cache em memória (TTL curto) para não disparar HTTP a cada rerun.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

_CACHE: dict[str, Any] = {"ts": 0.0, "fc": ""}
_TTL_SEC = 45.0


def _fmt_num_br(n: float | int) -> str:
    s = f"{float(n):,.0f}" if isinstance(n, int) or float(n).is_integer() else f"{float(n):,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


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


def invalidar_cache_saldos() -> None:
    global _CACHE
    _CACHE = {"ts": 0.0, "fc": ""}
