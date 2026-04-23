import os
from unittest.mock import patch

from leilao_ia_v2.services import saldos_providers as sp


def test_buscar_saldo_sem_api_key():
    with patch.dict(os.environ, {"FIRECRAWL_API_KEY": ""}, clear=False):
        t = sp.buscar_saldo_firecrawl_texto()
    assert "sem" in t.lower() or "chave" in t.lower() or "—" in t


def test_cache_invalidar_e_ttl():
    sp.invalidar_cache_saldos()
    with patch.object(sp, "buscar_saldo_firecrawl_texto", return_value="42 créditos") as m:
        a = sp.buscar_saldo_firecrawl_cached()
        b = sp.buscar_saldo_firecrawl_cached()
    assert a == "42 créditos" == b
    assert m.call_count == 1
