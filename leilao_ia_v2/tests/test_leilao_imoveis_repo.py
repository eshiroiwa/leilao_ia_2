from unittest.mock import MagicMock, patch

from leilao_ia_v2.constants import TABELA_LEILAO_IMOVEIS
from leilao_ia_v2.persistence import leilao_imoveis_repo as lim


def test_buscar_por_url_leilao_encontrado():
    client = MagicMock()
    resp = MagicMock()
    resp.data = [{"id": "u1", "url_leilao": "https://leilao.example/x"}]
    lim_ = MagicMock()
    lim_.execute.return_value = resp
    eq = MagicMock()
    eq.limit.return_value = lim_
    sel = MagicMock()
    sel.eq.return_value = eq
    tbl = MagicMock()
    tbl.select.return_value = sel
    client.table.return_value = tbl

    row = lim.buscar_por_url_leilao("https://leilao.example/x", client)
    assert row["id"] == "u1"
    client.table.assert_called_with(TABELA_LEILAO_IMOVEIS)


def test_remover_cache_media_bairro_id():
    client = MagicMock()
    row = {"id": "L1", "cache_media_bairro_ids": ["c1", "c2", "c3"]}
    with patch.object(lim, "buscar_por_id", return_value=row):
        with patch.object(lim, "atualizar_leilao_imovel") as m_up:
            out = lim.remover_cache_media_bairro_id("L1", "c2", client)
    assert out == ["c1", "c3"]
    m_up.assert_called_once()
    _iid, campos, _cli = m_up.call_args[0]
    assert campos["cache_media_bairro_ids"] == ["c1", "c3"]


def test_listar_resumo_recentes():
    client = MagicMock()
    resp = MagicMock()
    resp.data = [
        {
            "id": "a1",
            "url_leilao": "https://x",
            "cidade": "Rio",
            "estado": "RJ",
            "latitude": -22.9,
            "longitude": -43.2,
        }
    ]
    lim_ = MagicMock()
    lim_.execute.return_value = resp
    ord_ = MagicMock()
    ord_.limit.return_value = lim_
    sel = MagicMock()
    sel.order.return_value = ord_
    tbl = MagicMock()
    tbl.select.return_value = sel
    client.table.return_value = tbl

    rows = lim.listar_resumo_recentes(client, limite=50)
    assert len(rows) == 1
    assert rows[0]["id"] == "a1"
    tbl.select.assert_called_once()
    sel.order.assert_called_once_with("edital_coletado_em", desc=True)
    ord_.limit.assert_called_once_with(50)


def test_buscar_por_url_leilao_vazio():
    client = MagicMock()
    resp = MagicMock()
    resp.data = []
    lim_ = MagicMock()
    lim_.execute.return_value = resp
    eq = MagicMock()
    eq.limit.return_value = lim_
    sel = MagicMock()
    sel.eq.return_value = eq
    tbl = MagicMock()
    tbl.select.return_value = sel
    client.table.return_value = tbl
    assert lim.buscar_por_url_leilao("https://nada", client) is None


def test_agora_utc_iso_formato():
    s = lim.agora_utc_iso()
    assert "T" in s and "+" in s or s.endswith("Z") or ":" in s


def test_listar_ids_leilao_que_incluem_cache_id():
    client = MagicMock()
    resp = MagicMock()
    resp.data = [{"id": "L2"}, {"id": "L9"}]
    exec_ = MagicMock()
    exec_.execute.return_value = resp
    after_select = MagicMock()
    after_select.contains.return_value = exec_
    tbl = MagicMock()
    tbl.select.return_value = after_select
    client.table.return_value = tbl

    out = lim.listar_ids_leilao_que_incluem_cache_id("cache-uuid-1", client)
    assert out == ["L2", "L9"]
    after_select.contains.assert_called_with("cache_media_bairro_ids", ["cache-uuid-1"])


def test_listar_ids_fallback_encontra():
    client = MagicMock()
    resp_obj = MagicMock()
    resp_obj.data = [
        {"id": "A", "cache_media_bairro_ids": ["x", "cx"]},
        {"id": "B", "cache_media_bairro_ids": []},
    ]
    lim_ret = MagicMock()
    lim_ret.execute.return_value = resp_obj
    sel_ret = MagicMock()
    sel_ret.limit.return_value = lim_ret
    tbl = MagicMock()
    tbl.select.return_value = sel_ret
    client.table.return_value = tbl
    out = lim._listar_ids_com_cache_id_fallback(  # noqa: SLF001
        client,
        "cx",
    )
    assert out == ["A"]
