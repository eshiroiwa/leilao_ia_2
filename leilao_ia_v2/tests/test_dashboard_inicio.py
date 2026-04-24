from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from leilao_ia_v2.ui.dashboard_inicio import (
    filtrar_rows_dashboard,
    gerar_relatorio_html_prioridade_maxima_lote,
    processar_rows_dashboard,
)


_TZ_SP = ZoneInfo("America/Sao_Paulo")


def _row_base(iid: str, d_iso: str) -> dict[str, object]:
    return {
        "id": iid,
        "cidade": "Sao Paulo",
        "estado": "SP",
        "bairro": "Centro",
        "tipo_imovel": "apartamento",
        "url_leilao": f"https://exemplo.com/{iid}",
        "endereco": "Rua Exemplo, 100",
        "data_leilao_1_praca": d_iso,
        "operacao_simulacao_json": None,
        "simulacoes_modalidades_json": None,
        "cache_media_bairro_ids": ["c1"],
    }


def test_processar_dashboard_prioriza_roi_ou_lucro():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=1)).isoformat()),
            "roi_projetado": 0.55,
            "lucro_liquido_projetado": 120_000.0,
        },
        {
            **_row_base("b", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.30,
            "lucro_liquido_projetado": 600_000.0,
        },
        {
            **_row_base("c", (hoje + timedelta(days=3)).isoformat()),
            "roi_projetado": 0.45,
            "lucro_liquido_projetado": 90_000.0,
        },
    ]
    d = processar_rows_dashboard(rows)
    assert d.priorizados == 2
    assert d.priorizados_prox_7d == 2
    assert [x.id for x in d.priorizados_lista[:2]] == ["a", "b"]


def test_processar_dashboard_kpis_medios_priorizados():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=1)).isoformat()),
            "roi_projetado": 0.6,
            "lucro_liquido_projetado": 200_000.0,
        },
        {
            **_row_base("b", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.3,
            "lucro_liquido_projetado": 700_000.0,
        },
    ]
    d = processar_rows_dashboard(rows)
    assert d.ticket_medio_lucro_priorizados is not None
    assert abs(d.ticket_medio_lucro_priorizados - 450_000.0) < 0.01
    assert d.roi_medio_priorizados is not None
    assert abs(d.roi_medio_priorizados - 0.45) < 1e-6


def test_filtrar_rows_dashboard_priorizados():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=1)).isoformat()),
            "roi_projetado": 0.55,
            "lucro_liquido_projetado": 100_000.0,
        },
        {
            **_row_base("b", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.30,
            "lucro_liquido_projetado": 600_000.0,
        },
        {
            **_row_base("c", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.30,
            "lucro_liquido_projetado": 100_000.0,
        },
    ]
    out = filtrar_rows_dashboard(rows, "priorizados")
    assert [str(r.get("id")) for r in out] == ["a", "b"]


def test_filtrar_rows_dashboard_composto_and():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.7,
            "lucro_liquido_projetado": 800_000.0,
            "relatorio_mercado_contexto_json": {},
        },
        {
            **_row_base("b", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.7,
            "lucro_liquido_projetado": 800_000.0,
            "relatorio_mercado_contexto_json": {"ok": True},
        },
    ]
    out = filtrar_rows_dashboard(rows, ["priorizados", "prox7", "sem_mercado"])
    assert [str(r.get("id")) for r in out] == ["a"]


def test_filtrar_rows_dashboard_csv_equivale_lista():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=3)).isoformat()),
            "roi_projetado": 0.7,
            "lucro_liquido_projetado": 800_000.0,
            "relatorio_mercado_contexto_json": {},
        },
    ]
    out_csv = filtrar_rows_dashboard(rows, "priorizados,prox7,sem_mercado")
    out_lst = filtrar_rows_dashboard(rows, ["priorizados", "prox7", "sem_mercado"])
    assert [str(r.get("id")) for r in out_csv] == [str(r.get("id")) for r in out_lst]


def test_processar_dashboard_prioriza_mais_score_quando_mesma_data():
    hoje = datetime.now(_TZ_SP).date()
    dref = (hoje + timedelta(days=2)).isoformat()
    rows = [
        {
            **_row_base("a", dref),
            "roi_projetado": 0.45,
            "lucro_liquido_projetado": 100_000.0,
        },
        {
            **_row_base("b", dref),
            "roi_projetado": 0.90,
            "lucro_liquido_projetado": 800_000.0,
        },
    ]
    d = processar_rows_dashboard(rows)
    assert d.proximos
    assert d.proximos[0].id == "b"


def test_gerar_relatorio_html_prioridade_maxima_lote_contem_so_filtrados():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=1)).isoformat()),
            "roi_projetado": 0.7,
            "lucro_liquido_projetado": 800_000.0,
            "url_foto_imovel": "https://imgs.exemplo.com/a.jpg",
        },
        {
            **_row_base("b", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.3,
            "lucro_liquido_projetado": 100_000.0,
        }
    ]
    d = processar_rows_dashboard(rows)
    assert d.priorizados_lista
    filtrados = [x for x in d.priorizados_lista if x.id == "a"]
    html = gerar_relatorio_html_prioridade_maxima_lote(filtrados)
    assert "https://exemplo.com/a" in html
    assert "https://exemplo.com/b" not in html
    assert "https://imgs.exemplo.com/a.jpg" in html
    assert "Link do leilão" in html
    assert "Abrir simulação completa" not in html
