from unittest.mock import MagicMock, patch

from leilao_ia_v2.services import cache_media_leilao as cml


def _anuncio(i, lat, lon, area, valor):
    return {
        "id": f"id-{i}",
        "url_anuncio": f"https://www.vivareal.com.br/x{i}",
        "portal": "vivareal.com.br",
        "tipo_imovel": "apartamento",
        "bairro": "Centro",
        "cidade": "Porto Alegre",
        "estado": "RS",
        "area_construida_m2": area,
        "valor_venda": valor,
        "transacao": "venda",
        "latitude": lat,
        "longitude": lon,
        "metadados_json": {},
    }


def test_filtrar_amostras_tres_dentro_raio():
    ref_lat, ref_lon = -30.03, -51.22
    cands = [
        _anuncio(1, -30.031, -51.221, 88.0, 400_000),
        _anuncio(2, -30.032, -51.220, 90.0, 410_000),
        _anuncio(3, -30.030, -51.219, 92.0, 420_000),
    ]
    out = cml._filtrar_amostras(cands, ref_lat, ref_lon, 90.0, raio_km=5.0)
    assert len(out) == 3


def _anuncio_terreno(i, lat, lon, area_m2, valor):
    """Terreno no mesmo formato de listagem BD (área em ``area_construida_m2``)."""
    a = _anuncio(i, lat, lon, area_m2, valor)
    a["tipo_imovel"] = "terreno"
    return a


def test_filtrar_amostras_terreno_fora_faixa_edital_excluido_com_faixa_incluido_sem_faixa():
    """
    Referência de área do imóvel (casa ~90 m²): com faixa ativa, lote 500 m² cai fora dos fatores
    default; segmento terreno/lote desliga a faixa para não esvaziar o cache de terrenos.
    """
    ref_lat, ref_lon = -30.03, -51.22
    area_ref_casa = 90.0
    terreno_grande = _anuncio_terreno(1, -30.031, -51.221, 500.0, 800_000)

    com_faixa = cml._filtrar_amostras(
        [terreno_grande],
        ref_lat,
        ref_lon,
        area_ref_casa,
        raio_km=5.0,
        aplicar_faixa_area_edital=True,
    )
    assert com_faixa == []

    sem_faixa = cml._filtrar_amostras(
        [terreno_grande],
        ref_lat,
        ref_lon,
        area_ref_casa,
        raio_km=5.0,
        aplicar_faixa_area_edital=False,
    )
    assert len(sem_faixa) == 1
    assert sem_faixa[0]["id"] == terreno_grande["id"]


def test_tipos_somente_terreno_ou_lote():
    assert cml._tipos_somente_terreno_ou_lote(["terreno"]) is True
    assert cml._tipos_somente_terreno_ou_lote(["lote", "terreno"]) is True
    assert cml._tipos_somente_terreno_ou_lote(["terreno", "apartamento"]) is False
    assert cml._tipos_somente_terreno_ou_lote(["casa"]) is False


def test_fatias_amostras_cache():
    ads = [{"id": str(i)} for i in range(25)]
    pri, secs = cml._fatias_amostras_cache(ads, 10, 10)
    assert len(pri) == 10
    assert len(secs) == 2
    assert len(secs[0]) == 10
    assert len(secs[1]) == 5


def test_criar_cache_sucesso_mock():
    leilao = {
        "id": "L1",
        "cidade": "Porto Alegre",
        "estado": "RS",
        "bairro": "Centro",
        "tipo_imovel": "apartamento",
        "area_util": 90.0,
        "latitude": -30.03,
        "longitude": -51.22,
        "endereco": "Rua X",
        "conservacao": "usado",
        "tipo_casa": None,
        "andar": None,
        "leilao_extra_json": {},
    }
    amostras = [
        _anuncio(1, -30.031, -51.221, 88.0, 400_000),
        _anuncio(2, -30.032, -51.220, 90.0, 410_000),
        _anuncio(3, -30.030, -51.219, 92.0, 420_000),
    ]
    cli = MagicMock()
    with patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.buscar_por_id", return_value=leilao):
        with patch(
            "leilao_ia_v2.services.cache_media_leilao._montar_amostras_para_tipos",
            return_value=(amostras, False, "", "", 0),
        ):
            with patch("leilao_ia_v2.services.cache_media_leilao.cache_media_bairro_repo.inserir", return_value="cache-uuid"):
                with patch(
                    "leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.anexar_cache_media_bairro_ids",
                ) as ax:
                    r = cml.criar_caches_media_para_leilao(cli, "L1")
    assert r.ok
    assert len(r.caches_criados) == 1
    ax.assert_called_once()


def test_criar_cache_falha_insuficiente():
    leilao = {
        "id": "L2",
        "cidade": "X",
        "estado": "RS",
        "bairro": "Y",
        "tipo_imovel": "apartamento",
        "area_util": 50.0,
        "latitude": -30.0,
        "longitude": -51.0,
        "endereco": "",
        "conservacao": "usado",
        "tipo_casa": None,
        "andar": None,
        "leilao_extra_json": {},
    }
    cli = MagicMock()
    with patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.buscar_por_id", return_value=leilao):
        with patch(
            "leilao_ia_v2.services.cache_media_leilao._montar_amostras_para_tipos",
            return_value=([], False, "poucos", "diag", 0),
        ):
            r = cml.criar_caches_media_para_leilao(cli, "L2")
    assert not r.ok
    assert "poucos" in r.mensagem or "Nenhuma amostra" in r.mensagem or "insuficientes" in r.mensagem


def test_resolver_pos_ingestao_reutiliza_cache_existente():
    leilao = {
        "id": "L1",
        "cidade": "Porto Alegre",
        "estado": "RS",
        "bairro": "Centro",
        "tipo_imovel": "apartamento",
        "area_util": 90.0,
        "latitude": -30.03,
        "longitude": -51.22,
        "endereco": "Rua X",
        "conservacao": "usado",
        "tipo_casa": None,
        "andar": None,
        "leilao_extra_json": {},
    }
    cache_row = {
        "id": "reuse-uuid",
        "nome_cache": "Cache existente",
        "n_amostras": 3,
        "anuncios_ids": "a1,a2,a3",
        "metadados_json": {"modo_cache": "principal", "tipo_segmento": "apartamento"},
        "tipo_imovel": "apartamento",
    }
    ads = [
        _anuncio(1, -30.031, -51.221, 88.0, 400_000),
        _anuncio(2, -30.032, -51.220, 90.0, 410_000),
        _anuncio(3, -30.030, -51.219, 92.0, 420_000),
    ]
    for i, a in enumerate(ads, start=1):
        a["id"] = f"a{i}"

    cli = MagicMock()
    with patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.buscar_por_id", return_value=leilao):
        with patch(
            "leilao_ia_v2.services.cache_media_leilao.cache_media_bairro_repo.listar_candidatos_reuso",
            return_value=[cache_row],
        ):
            with patch(
                "leilao_ia_v2.services.cache_media_leilao.anuncios_mercado_repo.buscar_por_ids",
                return_value=ads,
            ):
                with patch(
                    "leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.definir_cache_media_bairro_ids",
                ) as dfn:
                    r = cml.resolver_cache_media_pos_ingestao(cli, "L1")
    assert r.ok
    assert r.reutilizou_existente is True
    assert r.usou_firecrawl_extra is False
    assert len(r.caches_criados) == 1
    dfn.assert_called_once_with("L1", ["reuse-uuid"], cli)


def test_criar_cache_manual_de_anuncios_ok():
    cli = MagicMock()
    ads = [
        _anuncio(1, -30.0, -51.0, 80.0, 400_000),
        _anuncio(2, -30.01, -51.01, 90.0, 450_000),
    ]
    with patch.object(cml.cache_media_bairro_repo, "inserir", return_value="new-uuid-1"):
        ok, msg, cid = cml.criar_cache_manual_de_anuncios(cli, ads, "Meu cache manual")
    assert ok is True
    assert cid == "new-uuid-1"
    assert "2 amostra" in msg


def test_criar_cache_manual_de_anuncios_sem_coordenadas():
    a = _anuncio(1, -30.0, -51.0, 80.0, 400_000)
    a.pop("latitude", None)
    a.pop("longitude", None)
    ok, _, _ = cml.criar_cache_manual_de_anuncios(MagicMock(), [a], "X")
    assert ok is False


def test_criar_cache_manual_nome_vazio():
    ok, _, _ = cml.criar_cache_manual_de_anuncios(MagicMock(), [_anuncio(1, -30.0, -51.0, 80.0, 400_000)], "  ")
    assert ok is False


def test_recalcular_caches_mercado_chama_definir_apagar_criar():
    leilao = {
        "id": "L9",
        "cidade": "X",
        "estado": "RS",
        "bairro": "Y",
        "tipo_imovel": "apartamento",
        "area_util": 50.0,
        "latitude": -30.0,
        "longitude": -51.0,
        "endereco": "",
        "conservacao": "usado",
        "tipo_casa": None,
        "andar": None,
        "leilao_extra_json": {},
        "cache_media_bairro_ids": ["old-c1", "old-c2"],
    }
    cli = MagicMock()
    with (
        patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.buscar_por_id", return_value=leilao),
        patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.definir_cache_media_bairro_ids") as m_def,
        patch(
            "leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.listar_ids_leilao_que_incluem_cache_id",
            return_value=[],
        ) as m_list,
        patch("leilao_ia_v2.services.cache_media_leilao.cache_media_bairro_repo.apagar_por_id") as m_del,
        patch.object(cml, "criar_caches_media_para_leilao") as m_criar,
    ):
        m_criar.return_value = cml.ResultadoCriacaoCacheLeilao(
            True, "Criado(s) 1 cache(s).", caches_criados=[{"id": "n1", "n_amostras": 3}]
        )
        r = cml.recalcular_caches_mercado_para_leilao(
            cli,
            "L9",
            apagar_caches_sem_outro_vinculo=True,
            max_chamadas_api_firecrawl=5,
        )
    m_def.assert_called_once_with("L9", [], cli)
    assert m_list.call_count == 2
    assert m_del.call_count == 2
    m_criar.assert_called_once()
    assert r.ok
    apagados = {m_del.call_args_list[i][0][1] for i in range(2)}
    assert apagados == {"old-c1", "old-c2"}
