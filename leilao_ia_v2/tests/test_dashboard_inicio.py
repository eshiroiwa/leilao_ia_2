import os
from datetime import datetime, timedelta
from unittest.mock import patch
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


def test_processar_dashboard_calcula_sensibilidade_e_retorno_por_capital():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.5,
            "lucro_liquido_projetado": 250_000.0,
        }
    ]
    d = processar_rows_dashboard(rows)
    assert d.priorizados_lista
    o = d.priorizados_lista[0]
    assert o.capital_imobilizado is not None
    assert abs(float(o.capital_imobilizado) - 500_000.0) < 0.01
    assert o.retorno_por_capital is not None
    assert abs(float(o.retorno_por_capital) - 0.5) < 1e-6
    assert o.roi_conservador is not None and o.roi_bruto is not None and o.roi_agressivo is not None
    assert float(o.roi_conservador) < float(o.roi_bruto) < float(o.roi_agressivo)
    assert o.lucro_conservador is not None and o.lucro_liq is not None and o.lucro_agressivo is not None
    assert float(o.lucro_conservador) < float(o.lucro_liq) < float(o.lucro_agressivo)


def test_processar_dashboard_rankeia_eficiencia_capital():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=1)).isoformat()),
            "roi_projetado": 0.40,
            "lucro_liquido_projetado": 200_000.0,
        },
        {
            **_row_base("b", (hoje + timedelta(days=1)).isoformat()),
            "roi_projetado": 0.80,
            "lucro_liquido_projetado": 200_000.0,
        },
    ]
    d = processar_rows_dashboard(rows)
    assert d.eficiencia_capital
    assert d.eficiencia_capital[0].id == "b"


def test_perfil_risco_conservador_e_mais_estrito_que_agressivo():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=3)).isoformat()),
            "roi_projetado": 0.8,
            "lucro_liquido_projetado": 300_000.0,
            "relatorio_mercado_contexto_json": None,
            "cache_media_bairro_ids": [],
        }
    ]
    d_cons = processar_rows_dashboard(rows, perfil_risco="conservador")
    d_agr = processar_rows_dashboard(rows, perfil_risco="agressivo")
    assert d_cons.priorizados_lista and d_agr.priorizados_lista
    o_cons = d_cons.priorizados_lista[0]
    o_agr = d_agr.priorizados_lista[0]
    assert o_cons.roi_conservador is not None and o_agr.roi_conservador is not None
    assert o_cons.roi_conservador < o_agr.roi_conservador


def test_processar_dashboard_define_semaforo_decisao():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.7,
            "lucro_liquido_projetado": 300_000.0,
            "relatorio_mercado_contexto_json": {
                "versao": 1,
                "gerado_em_iso": datetime.now(_TZ_SP).isoformat(),
                "cards": [
                    {
                        "id": "procura_imoveis",
                        "titulo": "Procura",
                        "topicos": ["Alta procura e boa liquidez no micro-mercado."],
                        "evidencia": "Base: 22 amostras; 72% mesmo bairro; 91% geo válida.",
                    }
                ],
                "sinais_decisao": {
                    "liquidez_bairro": 72,
                    "pressao_concorrencia": 45,
                    "fit_imovel_bairro": 68,
                },
                "qualidade": {"score_qualidade": 78},
                "validade": {"expirado": False},
            },
        }
    ]
    d = processar_rows_dashboard(rows)
    assert d.proximos
    assert d.proximos[0].semaforo_decisao in {"Comprar", "Negociar lance", "Evitar"}
    assert d.proximos[0].semaforo_justificativa


def test_env_override_score_pesos_mercado_altera_ordenacao():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.5,
            "lucro_liquido_projetado": 250_000.0,
            "relatorio_mercado_contexto_json": {
                "versao": 1,
                "gerado_em_iso": datetime.now(_TZ_SP).isoformat(),
                "sinais_decisao": {"liquidez_bairro": 80, "pressao_concorrencia": 40, "fit_imovel_bairro": 75},
                "qualidade": {"score_qualidade": 75},
                "validade": {"expirado": False},
                "cards": [{"id": "procura_imoveis", "titulo": "Procura", "topicos": ["Alta procura"]}],
            },
        },
        {
            **_row_base("b", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.5,
            "lucro_liquido_projetado": 250_000.0,
            "relatorio_mercado_contexto_json": {
                "versao": 1,
                "gerado_em_iso": datetime.now(_TZ_SP).isoformat(),
                "sinais_decisao": {"liquidez_bairro": 35, "pressao_concorrencia": 75, "fit_imovel_bairro": 35},
                "qualidade": {"score_qualidade": 40},
                "validade": {"expirado": False},
                "cards": [{"id": "volume_anuncios", "titulo": "Volume", "topicos": ["Muita oferta"]}],
            },
        },
    ]
    d_base = processar_rows_dashboard(rows)
    assert d_base.proximos
    with patch.dict(
        os.environ,
        {
            "DASHBOARD_SCORE_PESO_LIQUIDEZ": "50",
            "DASHBOARD_SCORE_PESO_CONCORRENCIA": "50",
            "DASHBOARD_SCORE_PESO_FIT": "40",
            "DASHBOARD_SCORE_PESO_QUALIDADE_RELATORIO": "40",
        },
        clear=False,
    ):
        d_peso = processar_rows_dashboard(rows)
    assert d_peso.proximos
    assert d_peso.proximos[0].id == "a"


def test_env_override_thresholds_semaforo_comprar():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=3)).isoformat()),
            "roi_projetado": 0.7,
            "lucro_liquido_projetado": 300_000.0,
            "relatorio_mercado_contexto_json": {
                "versao": 1,
                "gerado_em_iso": datetime.now(_TZ_SP).isoformat(),
                "sinais_decisao": {"liquidez_bairro": 70, "pressao_concorrencia": 45, "fit_imovel_bairro": 70},
                "qualidade": {"score_qualidade": 80},
                "validade": {"expirado": False},
                "cards": [{"id": "procura_imoveis", "titulo": "Procura", "topicos": ["Alta procura"]}],
            },
        }
    ]
    d_base = processar_rows_dashboard(rows)
    assert d_base.proximos
    sema_base = d_base.proximos[0].semaforo_decisao
    with patch.dict(
        os.environ,
        {
            "DASHBOARD_SEMAFORO_ROI_COMPRAR": "0.95",
            "DASHBOARD_SEMAFORO_EFICIENCIA_COMPRAR": "0.95",
            "DASHBOARD_SEMAFORO_QUALIDADE_COMPRAR": "95",
        },
        clear=False,
    ):
        d_lim = processar_rows_dashboard(rows)
    assert d_lim.proximos
    assert sema_base in {"Comprar", "Negociar lance", "Evitar"}
    assert d_lim.proximos[0].semaforo_decisao != "Comprar"


def test_modo_hibrido_desligado_nao_aplica_comprometimento_caixa():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.8,
            "lucro_liquido_projetado": 400_000.0,
            "relatorio_mercado_contexto_json": {
                "versao": 1,
                "gerado_em_iso": datetime.now(_TZ_SP).isoformat(),
                "sinais_decisao": {"liquidez_bairro": 75, "pressao_concorrencia": 45, "fit_imovel_bairro": 72},
                "qualidade": {"score_qualidade": 82},
                "validade": {"expirado": False},
                "cards": [{"id": "procura_imoveis", "titulo": "Procura", "topicos": ["Alta procura"]}],
            },
        }
    ]
    d = processar_rows_dashboard(
        rows,
        hibrido_ativo=False,
        caixa_disponivel_brl=500_000.0,
        caixa_reserva_brl=100_000.0,
    )
    assert d.proximos
    o = d.proximos[0]
    assert o.hibrido_ativo is False
    assert o.capital_comprometido_pct is None


def test_modo_hibrido_ligado_aplica_comprometimento_caixa_no_semaforo():
    hoje = datetime.now(_TZ_SP).date()
    rows = [
        {
            **_row_base("a", (hoje + timedelta(days=2)).isoformat()),
            "roi_projetado": 0.8,
            "lucro_liquido_projetado": 400_000.0,
            "relatorio_mercado_contexto_json": {
                "versao": 1,
                "gerado_em_iso": datetime.now(_TZ_SP).isoformat(),
                "sinais_decisao": {"liquidez_bairro": 75, "pressao_concorrencia": 45, "fit_imovel_bairro": 72},
                "qualidade": {"score_qualidade": 82},
                "validade": {"expirado": False},
                "cards": [{"id": "procura_imoveis", "titulo": "Procura", "topicos": ["Alta procura"]}],
            },
        }
    ]
    d = processar_rows_dashboard(
        rows,
        hibrido_ativo=True,
        caixa_disponivel_brl=500_000.0,
        caixa_reserva_brl=100_000.0,
    )
    assert d.proximos
    o = d.proximos[0]
    assert o.hibrido_ativo is True
    assert o.capital_comprometido_pct is not None
    assert o.capital_comprometido_pct > 90
    assert o.semaforo_decisao == "Evitar"
