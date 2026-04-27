"""Testes de ``leilao_ia_v2.fc_search`` (sem chamadas de rede)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from leilao_ia_v2.fc_search.query_builder import (
    montar_frase_busca,
    montar_frase_busca_mercado,
    montar_frases_busca_mercado_em_camadas,
)
from leilao_ia_v2.fc_search.urls import extrair_urls_da_busca, selecionar_urls_para_scrape


def test_montar_frase_busca_basica():
    row = {
        "tipo_imovel": "casa",
        "area_util": 100.0,
        "endereco": "Rua Henrique Homem de Melo",
        "bairro": "Lessa",
        "cidade": "Pindamonhangaba",
        "estado": "SP",
    }
    s = montar_frase_busca(row)
    assert s == s.lower()
    assert "casa" in s
    assert "100" in s
    assert "henrique" in s
    assert "lessa" in s
    assert "pindamonhangaba" in s
    assert "sp" in s
    assert "na rua henrique homem de melo" in s
    assert "bairro" not in s
    assert "n." not in s and "nº" not in s
    assert "cep" not in s


def test_montar_frase_busca_sp_sem_numero_cep():
    row = {
        "tipo_imovel": "apartamento",
        "area_util": 39.0,
        "endereco": (
            "RUA BARAO CARLOS DE SOUSA ANHUMAS, N. 380 APTO. 24 BL 15, JARDIM RECANTO VERDE - "
            "CEP: 02364-000, SAO PAULO - SAO PAULO"
        ),
        "bairro": "JARDIM RECANTO VERDE",
        "cidade": "São Paulo",
        "estado": "SP",
    }
    s = montar_frase_busca(row)
    assert s == (
        "apartamento, de 39m², à venda, na rua barao carlos de sousa anhumas, "
        "jardim recanto verde, são paulo, sp"
    )
    assert "380" not in s
    assert "cep" not in s
    assert "pindamonhangaba" not in s


def test_montar_frase_busca_mercado_terreno():
    row = {
        "tipo_imovel": "casa",
        "area_util": 200.0,
        "endereco": "Rua X",
        "bairro": "Centro",
        "cidade": "Florianópolis",
        "estado": "SC",
    }
    q = montar_frase_busca_mercado(row, "terreno")
    assert "terreno" in q.lower()
    assert "200" in q


def test_montar_frases_busca_mercado_em_camadas():
    row = {
        "tipo_imovel": "casa",
        "area_util": 120.0,
        "endereco": "Rua das Flores, 123",
        "bairro": "Centro",
        "cidade": "Taubaté",
        "estado": "SP",
    }
    qs = montar_frases_busca_mercado_em_camadas(
        row,
        "casa",
        bairro_canonico="Chácara do Visconde",
        bairro_aliases=["Chacara Visconde"],
    )
    assert len(qs) >= 3
    assert any("na rua das flores" in q for q in qs)  # Q1
    assert any("taubaté, sp" in q and "na rua" not in q and "chácara do visconde" not in q for q in qs)  # Q2
    assert any("chácara do visconde" in q for q in qs)  # Q3


def test_montar_frases_camadas_prioriza_empreendimento_quando_disponivel():
    row = {
        "tipo_imovel": "apartamento",
        "area_util": 78.0,
        "endereco": "Rua Y, 100",
        "bairro": "Centro",
        "cidade": "Campinas",
        "estado": "SP",
        "leilao_extra_json": {
            "nome_condominio": "Condomínio Residencial Parque das Flores",
        },
    }
    qs = montar_frases_busca_mercado_em_camadas(row, "apartamento")
    assert qs
    assert "apartamentos à venda no condomínio ou prédio residencial parque das flores, campinas, sp".lower() in qs[0]


def test_montar_frases_camadas_usa_empreendimento_em_observacoes():
    row = {
        "tipo_imovel": "casa",
        "cidade": "Taubaté",
        "estado": "SP",
        "leilao_extra_json": {
            "observacoes_markdown": "CONDOMÍNIO RESIDENCIAL VILLAGIO DI ITÁLIA\nValor de avaliação: R$ 435.000,00"
        },
    }
    qs = montar_frases_busca_mercado_em_camadas(row, "casa")
    assert qs
    assert "villagio di itália" in qs[0]


def test_extrair_e_selecionar_urls():
    web = [
        {"url": "https://www.vivareal.com.br/venda/sp/cidade/apartamento/", "title": "x", "description": ""},
        {"url": "https://www.google.com/search?q=x", "title": "g", "description": ""},
        {"url": "https://www.zapimoveis.com.br/venda/", "title": "z", "description": ""},
    ]
    urls = extrair_urls_da_busca(web)
    assert any("vivareal" in u for u in urls)
    assert all("google" not in u for u in urls)
    sel = selecionar_urls_para_scrape(urls, max_urls=2)
    assert len(sel) <= 2
    # Ordem da busca é VR antes do Zap; scrape prioriza diversidade (Zap antes de VR).
    assert "zapimoveis" in sel[0]


def test_extrair_urls_aceita_host_kenlo():
    web = [
        {
            "url": "https://portal.kenlo.com.br/imoveis/taubate/condominio-villagio-d-italia",
            "title": "k",
            "description": "",
        }
    ]
    urls = extrair_urls_da_busca(web)
    assert urls
    assert "kenlo.com.br" in urls[0]


def test_selecionar_urls_prioriza_outros_portais_antes_de_vivareal():
    urls = [
        "https://www.vivareal.com.br/v1",
        "https://www.zapimoveis.com.br/z1",
    ]
    sel = selecionar_urls_para_scrape(urls, max_urls=2)
    assert sel[0] == "https://www.zapimoveis.com.br/z1"
    assert sel[1] == "https://www.vivareal.com.br/v1"


def test_selecionar_urls_um_host_vivareal():
    urls = [
        "https://www.vivareal.com.br/a",
        "https://www.vivareal.com.br/b",
    ]
    assert len(selecionar_urls_para_scrape(urls, max_urls=5)) == 1


def test_extrair_urls_document_com_metadata():
    """Itens ``web`` no formato Document (URL só em metadata) — cenário comum com scrape na busca."""
    web = [
        {
            "markdown": "# x",
            "metadata": {
                "url": "https://www.imovelweb.com.br/listagem",
                "title": "t",
            },
        }
    ]
    urls = extrair_urls_da_busca(web)
    assert urls == ["https://www.imovelweb.com.br/listagem"]


@patch("leilao_ia_v2.fc_search.pipeline.firecrawl_edital.scrape_url_markdown")
@patch("leilao_ia_v2.fc_search.pipeline.executar_busca_web")
@patch("leilao_ia_v2.fc_search.pipeline.persistir_cards_anuncios_mercado", return_value=2)
@patch("leilao_ia_v2.fc_search.pipeline.geocodificar_anuncios_batch")
def test_pipeline_complementar_mock(
    mock_geo, mock_persist, mock_search, mock_scrape, monkeypatch
):
    monkeypatch.setenv("FC_SEARCH_MAX_SCRAPE_URLS", "2")
    mock_search.return_value = (
        [
            {
                "url": "https://www.zapimoveis.com.br/venda/imoveis/sp/",
                "title": "z",
                "description": "",
            }
        ],
        1,
    )
    mock_scrape.return_value = (
        "[Casa 100m² R$ 500.000](https://www.zapimoveis.com.br/imovel/venda-casa-123/)",
        {},
    )

    from leilao_ia_v2.fc_search.pipeline import complementar_anuncios_firecrawl_search

    row = {
        "id": "L-1",
        "tipo_imovel": "casa",
        "area_util": 100.0,
        "endereco": "Rua X",
        "bairro": "Centro",
        "cidade": "Campinas",
        "estado": "SP",
        "latitude": -22.9,
        "longitude": -47.06,
        "leilao_extra_json": {"bairro_canonico": "Centro"},
    }
    cli = MagicMock()
    with patch("leilao_ia_v2.fc_search.pipeline.leilao_imoveis_repo.buscar_por_id", return_value=row):
        n, diag, n_api = complementar_anuncios_firecrawl_search(
            cli,
            leilao_imovel_id="L-1",
            cidade="Campinas",
            estado_raw="SP",
            bairro="Centro",
            tipo_imovel="casa",
            area_ref=100.0,
            ignorar_cache_firecrawl=False,
        )
    assert n == 2
    assert n_api >= 2
    assert "markdown_chars=" in diag
    mock_geo.assert_called_once()
    mock_persist.assert_called_once()
    kwargs = mock_persist.call_args.kwargs
    assert kwargs.get("bairro_canonico") == "Centro"
    assert kwargs.get("lat_ref") == -22.9
    assert kwargs.get("lon_ref") == -47.06
