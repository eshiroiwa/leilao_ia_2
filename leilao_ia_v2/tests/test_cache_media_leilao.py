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


def test_filtrar_amostras_prioriza_empreendimento_e_relaxa_faixa_area():
    ref_lat, ref_lon = -23.01, -45.57
    c1 = _anuncio(1, -23.011, -45.571, 300.0, 1_000_000)
    c1["titulo"] = "Casa no Condomínio Residencial Villagio Di Itália"
    c2 = _anuncio(2, -23.0105, -45.5705, 90.0, 450_000)
    c2["titulo"] = "Casa em bairro próximo"
    out = cml._filtrar_amostras(
        [c2, c1],
        ref_lat,
        ref_lon,
        88.3,
        raio_km=6.0,
        aplicar_faixa_area_edital=True,
        empreendimento_referencia="Condomínio Residencial Villagio Di Itália",
    )
    ids = [str(x.get("id")) for x in out]
    assert ids[0] == "id-1"


def test_filtrar_amostras_mesmo_empreendimento_aceita_sem_coordenadas():
    ref_lat, ref_lon = -23.01, -45.57
    c1 = _anuncio(1, None, None, 310.0, 1_050_000)
    c1["titulo"] = "Casa no Condomínio Residencial Villagio Di Itália"
    out = cml._filtrar_amostras(
        [c1],
        ref_lat,
        ref_lon,
        88.3,
        raio_km=6.0,
        aplicar_faixa_area_edital=True,
        empreendimento_referencia="Condomínio Residencial Villagio Di Itália",
    )
    assert len(out) == 1


def test_leilao_indica_condominio_ignora_texto_juridico_padrao():
    le = {
        "descricao": "",
        "endereco": "",
        "leilao_extra_json": {
            "observacoes_markdown": (
                "REGRAS PARA PAGAMENTO DAS DESPESAS (caso existam):\n"
                "Condomínio: Sob responsabilidade do comprador, até o limite de 10%.\n"
                "Tributos: Sob responsabilidade do comprador."
            )
        },
    }
    assert cml._leilao_indica_condominio(le) is False


def test_leilao_indica_condominio_quando_nome_real_presente():
    le = {
        "descricao": "Casa em condomínio residencial Villagio di Italia",
        "endereco": "",
        "leilao_extra_json": {},
    }
    assert cml._leilao_indica_condominio(le) is True


def test_selecionar_amostras_apoio_escala_menor():
    ref = 250.0
    cands = [
        _anuncio(1, -30.031, -51.221, 150.0, 400_000),
        _anuncio(2, -30.032, -51.220, 170.0, 410_000),
        _anuncio(3, -30.030, -51.219, 200.0, 420_000),
        _anuncio(4, -30.030, -51.218, 245.0, 430_000),  # fora do apoio menor
    ]
    out = cml._selecionar_amostras_apoio_escala(
        cands,
        area_ref=ref,
        fator_min=0.60,
        fator_max=0.95,
        min_amostras=3,
        limite=10,
    )
    assert len(out) == 3
    assert {str(x["id"]) for x in out} == {"id-1", "id-2", "id-3"}


def test_selecionar_amostras_apoio_escala_respeita_ids_excluir():
    ref = 250.0
    cands = [
        _anuncio(1, -30.031, -51.221, 150.0, 400_000),
        _anuncio(2, -30.032, -51.220, 170.0, 410_000),
        _anuncio(3, -30.030, -51.219, 200.0, 420_000),
    ]
    out = cml._selecionar_amostras_apoio_escala(
        cands,
        area_ref=ref,
        fator_min=0.60,
        fator_max=0.95,
        ids_excluir={"id-2"},
        min_amostras=2,
        limite=10,
    )
    ids = {str(x["id"]) for x in out}
    assert "id-2" not in ids
    assert ids == {"id-1", "id-3"}


def test_url_indica_cidade_diferente_do_alvo_detecta_franca_vs_aparecida():
    url = (
        "https://www.chavesnamao.com.br/imovel/"
        "casa-a-venda-3-quartos-sp-franca-vila-nicacio-220m2-RS750000/id-39723738/"
    )
    assert cml._url_indica_cidade_diferente_do_alvo(url, "Aparecida") is True


def test_url_indica_cidade_diferente_do_alvo_aceita_slug_bairro_cidade():
    url = (
        "https://www.vivareal.com.br/imovel/"
        "lote-terreno-ponte-alta-aparecida-160m2-venda-RS165000-id-2870099796/"
    )
    assert cml._url_indica_cidade_diferente_do_alvo(url, "Aparecida") is False


def test_preco_anuncio_inconsistente_detecta_titulo_divergente():
    a = _anuncio(1, -30.03, -51.22, 100.0, 1_890_000)
    a["titulo"] = "**R$ 2.790.000** CondomínioR$ 1.050]("
    a["url_anuncio"] = "https://www.chavesnamao.com.br/imovel/x-RS2790000/id-1/"
    assert cml._preco_anuncio_inconsistente(a) is True


def test_sanear_amostras_remove_inconsistente_e_duplicado():
    a1 = _anuncio(1, -30.03, -51.22, 100.0, 1_000_000)
    a1["titulo"] = "**R$ 1.000.000**"
    a2 = dict(a1)
    a2["id"] = "id-2"
    a2["url_anuncio"] = "https://www.vivareal.com.br/dup-2"
    a2["area_construida_m2"] = 101.0
    a3 = _anuncio(3, -30.04, -51.23, 95.0, 700_000)
    a3["titulo"] = "**R$ 1.500.000**"  # inconsistente forte
    out = cml._sanear_amostras_para_cache([a1, a2, a3], [])
    assert len(out) == 1
    assert out[0]["id"] == "id-1"


def test_sanear_amostras_pode_preservar_similares_sem_dedupe():
    a1 = _anuncio(1, -30.03, -51.22, 100.0, 400_000)
    a2 = dict(a1)
    a2["id"] = "id-2"
    a2["url_anuncio"] = "https://www.vivareal.com.br/dup-2"
    a2["area_construida_m2"] = 101.0
    a2["valor_venda"] = 401_000
    out = cml._sanear_amostras_para_cache([a1, a2], [], deduplicar_similares=False)
    assert len(out) == 2


def test_sanear_amostras_remove_outliers_extremos():
    base_vals = [380_000, 395_000, 410_000, 420_000, 430_000, 445_000]
    ads = [_anuncio(i + 1, -30.03 - i * 0.001, -51.22 - i * 0.001, 100.0, v) for i, v in enumerate(base_vals)]
    alto = _anuncio(101, -30.05, -51.24, 100.0, 1_200_000)
    baixo = _anuncio(102, -30.06, -51.25, 100.0, 97_000)
    out = cml._sanear_amostras_para_cache(ads + [alto, baixo], [])
    ids = {str(x.get("id")) for x in out}
    assert "id-101" not in ids
    assert "id-102" not in ids
    assert len(out) == 6


def test_sanear_amostras_nao_remove_outlier_com_poucas_amostras():
    ads = [
        _anuncio(1, -30.03, -51.22, 100.0, 380_000),
        _anuncio(2, -30.031, -51.221, 100.0, 390_000),
        _anuncio(3, -30.032, -51.222, 100.0, 410_000),
        _anuncio(4, -30.033, -51.223, 100.0, 420_000),
        _anuncio(5, -30.034, -51.224, 100.0, 1_200_000),
    ]
    out = cml._sanear_amostras_para_cache(ads, [])
    assert len(out) == 5


def test_analise_liquidez_metragem_detecta_outlier_forte():
    areas = [70.0, 72.0, 74.0, 75.0, 78.0, 80.0, 82.0, 84.0, 86.0, 88.0]
    a = cml._analise_liquidez_metragem(220.0, areas)
    assert a["status"] == "ok"
    assert a["alerta_outlier_metragem"] is True
    assert a["outlier_forte_iqr"] is True
    assert "Liquidez pode ser menor" in str(a.get("mensagem_alerta") or "")


def test_analise_liquidez_metragem_sem_alerta_na_faixa_tipica():
    areas = [70.0, 72.0, 74.0, 75.0, 78.0, 80.0, 82.0, 84.0, 86.0, 88.0]
    a = cml._analise_liquidez_metragem(80.0, areas)
    assert a["status"] == "ok"
    assert a["alerta_outlier_metragem"] is False
    assert int(a["fit_metragem_score"]) >= 60


def test_analise_fit_multidimensional_detecta_outlier():
    metricas = []
    for i in range(30):
        metricas.append(
            {
                "area_m2": 80.0 + float(i % 5),
                "preco_m2": 4200.0 + float((i % 4) * 100),
                "dist_km": 0.4 + float(i % 3) * 0.1,
                "tipo_eq": 1.0,
            }
        )
    out = cml._analise_fit_multidimensional(
        area_imovel_m2=220.0,
        pm2_imovel=12000.0,
        metricas_amostra=metricas,
    )
    assert out["status_multidimensional"] == "ok"
    assert out["alerta_outlier_multidimensional"] is True
    assert int(out["fit_multidimensional_score"]) < 45


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


def test_amostras_reuso_validas_exige_mesmo_bairro_minimo():
    cli = MagicMock()
    cache_row = {"anuncios_ids": "a1,a2,a3"}
    ads = [
        _anuncio(1, -30.031, -51.221, 88.0, 400_000),
        _anuncio(2, -30.032, -51.220, 90.0, 410_000),
        _anuncio(3, -30.030, -51.219, 92.0, 420_000),
    ]
    for i, a in enumerate(ads, start=1):
        a["id"] = f"a{i}"
        a["bairro"] = "Crispim"
    with patch(
        "leilao_ia_v2.services.cache_media_leilao.anuncios_mercado_repo.buscar_por_ids",
        return_value=ads,
    ):
        out = cml._amostras_reuso_validas(
            cli,
            cache_row,
            -30.03,
            -51.22,
            90.0,
            ["apartamento"],
            raio_km=5.0,
            bairro_referencia="Feital",
            min_amostras_mesmo_bairro=2,
        )
    assert out is None


def test_amostras_reuso_validas_nao_reusa_quando_tipo_nao_confere():
    cli = MagicMock()
    cache_row = {"anuncios_ids": "a1,a2,a3"}
    ads = [
        _anuncio(1, -30.031, -51.221, 88.0, 400_000),
        _anuncio(2, -30.032, -51.220, 90.0, 410_000),
        _anuncio(3, -30.030, -51.219, 92.0, 420_000),
    ]
    for i, a in enumerate(ads, start=1):
        a["id"] = f"a{i}"
        a["tipo_imovel"] = "casa"
    with patch(
        "leilao_ia_v2.services.cache_media_leilao.anuncios_mercado_repo.buscar_por_ids",
        return_value=ads,
    ):
        out = cml._amostras_reuso_validas(
            cli,
            cache_row,
            -30.03,
            -51.22,
            90.0,
            ["terreno", "lote"],
            raio_km=5.0,
            bairro_referencia="Centro",
            min_amostras_mesmo_bairro=0,
        )
    assert out is None


def test_amostras_reuso_validas_nao_reusa_quando_cache_esta_misto():
    cli = MagicMock()
    cache_row = {"anuncios_ids": "a1,a2,a3"}
    ads = [
        _anuncio(1, -30.031, -51.221, 88.0, 400_000),
        _anuncio(2, -30.032, -51.220, 90.0, 410_000),
        _anuncio(3, -30.030, -51.219, 92.0, 420_000),
    ]
    ads[0]["id"] = "a1"
    ads[0]["tipo_imovel"] = "terreno"
    ads[1]["id"] = "a2"
    ads[1]["tipo_imovel"] = "casa"
    ads[2]["id"] = "a3"
    ads[2]["tipo_imovel"] = "casa"
    with patch(
        "leilao_ia_v2.services.cache_media_leilao.anuncios_mercado_repo.buscar_por_ids",
        return_value=ads,
    ):
        out = cml._amostras_reuso_validas(
            cli,
            cache_row,
            -30.03,
            -51.22,
            90.0,
            ["casa"],
            raio_km=5.0,
            bairro_referencia="Centro",
            min_amostras_mesmo_bairro=0,
        )
    assert out is None


def test_tentar_geocodificar_prioriza_bairro_alvo(monkeypatch):
    cli = MagicMock()
    cands = [
        {"id": "1", "bairro": "Crispim", "cidade": "Pindamonhangaba", "estado": "SP"},
        {"id": "2", "bairro": "Feital", "cidade": "Pindamonhangaba", "estado": "SP"},
        {"id": "3", "bairro": "Crispim", "cidade": "Pindamonhangaba", "estado": "SP"},
    ]
    monkeypatch.setattr(cml, "MAX_ANUNCIOS_GEOCODE", 2)
    ordem: list[str] = []

    def _geo(batch, **_kw):
        ordem.extend([str(x.get("bairro") or "") for x in batch])

    monkeypatch.setattr(cml, "geocodificar_anuncios_batch", _geo)
    monkeypatch.setattr(cml.anuncios_mercado_repo, "atualizar_geolocalizacao", lambda *_a, **_k: None)
    n = cml._tentar_geocodificar_sem_coordenadas(
        cli,
        cands,
        cidade="Pindamonhangaba",
        estado_sigla="SP",
        bairro_fb="Feital",
        bairro_alvo="Feital",
    )
    assert n == 0
    assert ordem
    assert ordem[0] == "Feital"


def test_tentar_geocodificar_fallback_centroide_bairro(monkeypatch):
    cli = MagicMock()
    q = cli.table.return_value
    q.select.return_value = q
    q.eq.return_value = q
    q.ilike.return_value = q
    q.limit.return_value = q
    q.execute.return_value = MagicMock(
        data=[
            {"latitude": -22.93, "longitude": -45.45},
            {"latitude": -22.94, "longitude": -45.44},
        ]
    )
    cand = [{"id": "a1", "bairro": "Feital", "cidade": "Pindamonhangaba", "estado": "SP"}]
    monkeypatch.setattr(cml, "MAX_ANUNCIOS_GEOCODE", 1)
    monkeypatch.setattr(cml, "geocodificar_anuncios_batch", lambda *_a, **_k: None)
    calls: list[tuple[str, float, float]] = []

    def _upd(_cli, aid, lat, lon):
        calls.append((aid, lat, lon))

    monkeypatch.setattr(cml.anuncios_mercado_repo, "atualizar_geolocalizacao", _upd)
    n = cml._tentar_geocodificar_sem_coordenadas(
        cli,
        cand,
        cidade="Pindamonhangaba",
        estado_sigla="SP",
        bairro_fb="Feital",
        bairro_alvo="Feital",
    )
    assert n == 1
    assert calls
    assert calls[0][0] == "a1"


def test_ordenar_amostras_prioriza_mesmo_bairro():
    ads = [
        {"id": "a1", "bairro": "Crispim"},
        {"id": "a2", "bairro": "Feital"},
        {"id": "a3", "bairro": "Crispim"},
        {"id": "a4", "bairro": "Feital"},
    ]
    out = cml._ordenar_amostras_priorizando_mesmo_bairro(ads, "Feital")
    ids = [str(x.get("id")) for x in out]
    assert ids[:2] == ["a2", "a4"]


def test_ordenar_amostras_para_cache_principal_prioriza_distancia_e_qualidade_geo():
    ads = [
        {
            "id": "a1",
            "bairro": "Centro",
            "logradouro": "Rua X",
            "latitude": -30.05,
            "longitude": -51.25,
            "area_construida_m2": 100.0,
            "metadados_json": {},
        },
        {
            "id": "a2",
            "bairro": "Centro",
            "logradouro": "Rua X, 123",  # mais específico
            "latitude": -30.031,
            "longitude": -51.221,
            "area_construida_m2": 100.0,
            "metadados_json": {},
        },
        {
            "id": "a3",
            "bairro": "Centro",
            "logradouro": "Rua X",
            "latitude": -30.031,
            "longitude": -51.221,
            "area_construida_m2": 100.0,
            "metadados_json": {"origem_geo": "fallback_centroide_bairro"},
        },
        {
            "id": "a4",
            "bairro": "Outro",
            "logradouro": "Rua X, 10",
            "latitude": -30.031,
            "longitude": -51.221,
            "area_construida_m2": 100.0,
            "metadados_json": {},
        },
    ]
    out = cml._ordenar_amostras_para_cache_principal(
        ads,
        bairro_referencia="Centro",
        lat0=-30.03,
        lon0=-51.22,
        area_ref=0.0,
    )
    ids = [str(x.get("id")) for x in out]
    assert ids == ["a2", "a4", "a3", "a1"]


def test_ordenar_amostras_para_cache_principal_modo_legado_prioriza_bairro():
    ads = [
        {"id": "a1", "bairro": "Centro", "latitude": -30.05, "longitude": -51.25, "area_construida_m2": 100.0},
        {"id": "a2", "bairro": "Outro", "latitude": -30.031, "longitude": -51.221, "area_construida_m2": 100.0},
    ]
    out = cml._ordenar_amostras_para_cache_principal(
        ads,
        bairro_referencia="Centro",
        lat0=-30.03,
        lon0=-51.22,
        area_ref=0.0,
        geo_first_enabled=False,
    )
    ids = [str(x.get("id")) for x in out]
    assert ids == ["a1", "a2"]


def test_flag_por_cidade_modo_piloto():
    with patch.dict("os.environ", {"CACHE_GEO_FIRST_ENABLED": "pilot"}, clear=False):
        assert cml._cache_geo_first_enabled("Taubaté") is True
        assert cml._cache_geo_first_enabled("Curitiba") is False


def test_garantir_bairro_canonico_leilao_persiste_em_extra():
    cli = MagicMock()
    leilao = {
        "id": "L1",
        "cidade": "Taubaté",
        "bairro": "Chacara do Visconde",
        "leilao_extra_json": {},
    }
    with patch("leilao_ia_v2.services.cache_media_leilao._bairro_canonico_enabled", return_value=True):
        with patch(
            "leilao_ia_v2.services.cache_media_leilao.reverse_geocodificar_bairro",
            return_value=("Chácara do Visconde", "nominatim_reverse"),
        ):
            with patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.atualizar_leilao_imovel") as upd:
                b_inf, b_ref = cml._garantir_bairro_canonico_leilao(cli, leilao, lat0=-23.0, lon0=-45.5)
    assert b_inf == "Chacara do Visconde"
    assert "visconde" in b_ref.lower()
    upd.assert_called_once()


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
        "bairro": "Centro",
        "nome_cache": "Cache existente",
        "n_amostras": 4,
        "anuncios_ids": "a1,a2,a3,a4",
        "metadados_json": {"modo_cache": "principal", "tipo_segmento": "apartamento"},
        "tipo_imovel": "apartamento",
    }
    ads = [
        _anuncio(1, -30.031, -51.221, 88.0, 400_000),
        _anuncio(2, -30.032, -51.220, 90.0, 410_000),
        _anuncio(3, -30.030, -51.219, 92.0, 420_000),
        _anuncio(4, -30.029, -51.218, 91.0, 415_000),
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
    assert "reuso_bairro=mesmo_bairro" in (r.log_diagnostico or "")
    dfn.assert_called_once_with("L1", ["reuse-uuid"], cli)


def test_resolver_pos_ingestao_prioriza_reuso_mesmo_bairro():
    leilao = {
        "id": "L1",
        "cidade": "Porto Alegre",
        "estado": "RS",
        "bairro": "Centro Histórico",
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
    cache_outro = {
        "id": "reuse-outro",
        "bairro": "Menino Deus",
        "nome_cache": "Cache outro bairro",
        "n_amostras": 12,
        "metadados_json": {"modo_cache": "principal", "tipo_segmento": "apartamento"},
        "tipo_imovel": "apartamento",
    }
    cache_mesmo = {
        "id": "reuse-centro",
        "bairro": "Centro Historico",
        "nome_cache": "Cache mesmo bairro",
        "n_amostras": 8,
        "metadados_json": {"modo_cache": "principal", "tipo_segmento": "apartamento"},
        "tipo_imovel": "apartamento",
    }
    cli = MagicMock()
    with patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.buscar_por_id", return_value=leilao):
        with patch(
            "leilao_ia_v2.services.cache_media_leilao.cache_media_bairro_repo.listar_candidatos_reuso",
            return_value=[cache_outro, cache_mesmo],
        ):
            with patch(
                "leilao_ia_v2.services.cache_media_leilao._amostras_reuso_validas",
                return_value=[{"id": "a1"}],
            ):
                with patch(
                    "leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.definir_cache_media_bairro_ids",
                ) as dfn:
                    r = cml.resolver_cache_media_pos_ingestao(cli, "L1")
    assert r.ok
    assert r.reutilizou_existente is True
    assert "reuso_bairro=mesmo_bairro" in (r.log_diagnostico or "")
    dfn.assert_called_once_with("L1", ["reuse-centro"], cli)


def test_resolver_pos_ingestao_loga_fallback_outro_bairro():
    leilao = {
        "id": "L1",
        "cidade": "Porto Alegre",
        "estado": "RS",
        "bairro": "Centro Histórico",
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
    cache_outro = {
        "id": "reuse-outro",
        "bairro": "Menino Deus",
        "nome_cache": "Cache outro bairro",
        "n_amostras": 12,
        "metadados_json": {"modo_cache": "principal", "tipo_segmento": "apartamento"},
        "tipo_imovel": "apartamento",
    }
    cli = MagicMock()
    with patch("leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.buscar_por_id", return_value=leilao):
        with patch(
            "leilao_ia_v2.services.cache_media_leilao.cache_media_bairro_repo.listar_candidatos_reuso",
            return_value=[cache_outro],
        ):
            with patch(
                "leilao_ia_v2.services.cache_media_leilao._amostras_reuso_validas",
                return_value=[{"id": "a1"}],
            ):
                with patch(
                    "leilao_ia_v2.services.cache_media_leilao.leilao_imoveis_repo.definir_cache_media_bairro_ids",
                ) as dfn:
                    r = cml.resolver_cache_media_pos_ingestao(cli, "L1")
    assert r.ok
    assert "reuso_bairro=fallback_outro_bairro" in (r.log_diagnostico or "")
    dfn.assert_called_once_with("L1", ["reuse-outro"], cli)


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
