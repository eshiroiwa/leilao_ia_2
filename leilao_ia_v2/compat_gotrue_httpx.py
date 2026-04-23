"""
Compatibilidade gotrue + httpx (supabase-py 2.3.x): proxy → proxies.
Duplicado aqui para não importar o pacote `codigo referencia` (somente leitura).
"""

from __future__ import annotations

import sys

import httpx


def apply_gotrue_httpx_proxy_compat() -> None:
    import gotrue.http_clients as hc

    class SyncClient(httpx.Client):
        def __init__(self, *args, proxy=None, **kwargs):
            if proxy is not None and kwargs.get("proxies") is None:
                kwargs["proxies"] = proxy
            super().__init__(*args, **kwargs)

        def aclose(self) -> None:
            self.close()

    class AsyncClient(httpx.AsyncClient):
        def __init__(self, *args, proxy=None, **kwargs):
            if proxy is not None and kwargs.get("proxies") is None:
                kwargs["proxies"] = proxy
            super().__init__(*args, **kwargs)

    hc.SyncClient = SyncClient
    hc.AsyncClient = AsyncClient

    prefix = "gotrue."
    for name, mod in list(sys.modules.items()):
        if not name.startswith(prefix) or mod is None:
            continue
        if hasattr(mod, "SyncClient"):
            mod.SyncClient = SyncClient
        if hasattr(mod, "AsyncClient"):
            mod.AsyncClient = AsyncClient


apply_gotrue_httpx_proxy_compat()
