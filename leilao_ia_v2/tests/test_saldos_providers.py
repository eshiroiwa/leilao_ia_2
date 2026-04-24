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


def test_validar_table_id_bq():
    assert sp._validar_table_id_bq("proj.dataset.gcp_billing_export_v1_abc")
    assert not sp._validar_table_id_bq("proj.dataset")
    assert not sp._validar_table_id_bq("`proj.dataset.table`")
    assert not sp._validar_table_id_bq("proj.dataset.table;drop")


def test_google_maps_sem_tabela_configurada():
    with patch.dict(os.environ, {"GCP_BILLING_BQ_TABLE": ""}, clear=False):
        out = sp.buscar_gastos_google_maps_mes()
    assert out["ok"] is False
    assert "sem GCP_BILLING_BQ_TABLE" in str(out["status"])


def test_google_maps_table_id_invalido():
    with patch.dict(os.environ, {"GCP_BILLING_BQ_TABLE": "invalido"}, clear=False):
        out = sp.buscar_gastos_google_maps_mes()
    assert out["ok"] is False
    assert "inválido" in str(out["status"])


def test_resumo_google_maps_ui_formatacao():
    data = {
        "status": "ok",
        "total_brl": 123.4,
        "geocoding_brl": 100.0,
        "addr_validation_brl": 20.0,
        "places_brl": 3.4,
        "budget_brl": 200.0,
        "budget_pct": 61.7,
        "risco": "baixo",
    }
    r = sp.resumo_google_maps_para_ui(data)
    assert r["status"] == "ok"
    assert r["total"].startswith("R$")
    assert r["budget_pct"] == "61.7%"
    assert r["risco"] == "baixo"


def test_cache_google_maps_ttl():
    sp.invalidar_cache_saldos()
    sample = {"ok": True, "status": "ok", "total_brl": 1.0}
    with patch.object(sp, "buscar_gastos_google_maps_mes", return_value=sample) as m:
        a = sp.buscar_gastos_google_maps_mes_cached()
        b = sp.buscar_gastos_google_maps_mes_cached()
    assert a == b
    assert m.call_count == 1
