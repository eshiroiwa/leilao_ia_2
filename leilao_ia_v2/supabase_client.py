"""Cliente Supabase (service_role no backend), sem depender do código de referência."""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Optional

from dotenv import load_dotenv

import leilao_ia_v2.compat_gotrue_httpx  # noqa: F401
from supabase import Client, create_client

load_dotenv()

logger = logging.getLogger(__name__)

_supabase_anon_warned = False


def supabase_jwt_role_from_key(key: str) -> Optional[str]:
    try:
        parts = (key or "").strip().split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        pad = "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(payload_b64 + pad)
        data = json.loads(raw.decode("utf-8"))
        r = data.get("role")
        return str(r) if r is not None else None
    except Exception:
        return None


def get_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
    )
    if not url or not key:
        raise RuntimeError(
            "Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY (recomendado com RLS) no .env"
        )
    global _supabase_anon_warned
    role = supabase_jwt_role_from_key(key)
    if role == "anon" and not _supabase_anon_warned:
        _supabase_anon_warned = True
        logger.warning(
            "Supabase com JWT role=anon: com RLS ativo inserts podem falhar. Use SUPABASE_SERVICE_ROLE_KEY."
        )
    return create_client(url, key)
