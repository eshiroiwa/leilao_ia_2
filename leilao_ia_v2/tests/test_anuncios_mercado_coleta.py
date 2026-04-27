from __future__ import annotations

from unittest.mock import MagicMock

from leilao_ia_v2.services import anuncios_mercado_coleta as amc


def _card_base(**ov):
    c = {
        "url_anuncio": "https://site.exemplo/imovel/1",
        "portal": "zapimoveis.com.br",
        "area_m2": 50.0,
        "valor_venda": 200_000.0,
        "logradouro": "Rua A",
        "titulo": "Apartamento 2 quartos",
        "bairro": "Centro",
        "cidade": "Campinas",
        "estado": "SP",
        "latitude": -22.9,
        "longitude": -47.06,
    }
    c.update(ov)
    return c


def test_persistir_cards_descarta_titulo_invalido(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [_card_base(titulo="Mensagem]("), _card_base(url_anuncio="https://site.exemplo/imovel/2")],
        cidade="Campinas",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="apartamento",
    )
    assert n == 1
    assert len(capt["rows"]) == 1
    assert capt["rows"][0]["url_anuncio"] == "https://site.exemplo/imovel/2"


def test_persistir_cards_exigir_geo_descarta_sem_lat_lon(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(latitude=None, longitude=None),
            _card_base(url_anuncio="https://site.exemplo/imovel/2"),
        ],
        cidade="Campinas",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="apartamento",
        exigir_geolocalizacao=True,
    )
    assert n == 1
    assert len(capt["rows"]) == 1
    assert capt["rows"][0]["url_anuncio"] == "https://site.exemplo/imovel/2"


def test_persistir_cards_descarta_url_com_cidade_diferente(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(
                url_anuncio=(
                    "https://www.chavesnamao.com.br/imovel/"
                    "casa-a-venda-3-quartos-sp-franca-vila-nicacio-220m2-RS750000/id-39723738/"
                ),
                portal="chavesnamao.com.br",
            ),
            _card_base(url_anuncio="https://site.exemplo/imovel/ok"),
        ],
        cidade="Aparecida",
        estado_raw="SP",
        bairro="Ponte Alta",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
    )
    assert n == 1
    assert len(capt["rows"]) == 1
    assert capt["rows"][0]["url_anuncio"] == "https://site.exemplo/imovel/ok"


def test_persistir_cards_aceita_url_vivareal_bairro_cidade(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(
                url_anuncio=(
                    "https://www.vivareal.com.br/imovel/lote-terreno-ponte-alta-aparecida-160m2-"
                    "venda-RS165000-id-2870099796/"
                ),
                portal="vivareal.com.br",
                _tipo_detectado="terreno",
            )
        ],
        cidade="Aparecida",
        estado_raw="SP",
        bairro="Ponte Alta",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="terreno",
    )
    assert n == 1
    assert len(capt["rows"]) == 1


def test_persistir_cards_preenche_score_geo_no_metadados(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [_card_base(url_anuncio="https://site.exemplo/imovel/geo")],
        cidade="Campinas",
        estado_raw="SP",
        bairro="Centro",
        bairro_canonico="Centro",
        lat_ref=-22.90,
        lon_ref=-47.06,
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="apartamento",
    )
    assert n == 1
    meta = capt["rows"][0]["metadados_json"]
    assert "score_geo" in meta
    assert 0.0 <= float(meta["score_geo"]) <= 100.0


def test_persistir_cards_respeita_tipo_detectado_apartamento(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(
                url_anuncio="https://site.exemplo/imovel/apto",
                titulo="Apartamento 2 quartos no centro",
                _tipo_detectado="apartamento",
            )
        ],
        cidade="Campinas",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
    )
    assert n == 1
    assert capt["rows"][0]["tipo_imovel"] == "apartamento"


def test_persistir_cards_nao_rejeita_url_taubate_com_sufixo_m2(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(
                url_anuncio=(
                    "https://www.chavesnamao.com.br/imovel/"
                    "casa-a-venda-3-quartos-com-garagem-sp-taubate-estiva-88m2-RS510000/id-35363069/"
                ),
                portal="chavesnamao.com.br",
            )
        ],
        cidade="Taubate",
        estado_raw="SP",
        bairro="Chacara do Visconde",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
    )
    assert n == 1
    assert len(capt["rows"]) == 1


def test_persistir_cards_diagnostico_descartes(monkeypatch):
    cli = MagicMock()

    def _upsert(_cli, rows):
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    diag: dict = {}
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(titulo="Mensagem]("),
            _card_base(url_anuncio="https://site.exemplo/imovel/ok"),
        ],
        cidade="Campinas",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="apartamento",
        diagnostico_saida=diag,
    )
    assert n == 1
    assert int(diag.get("cards_recebidos") or 0) == 2
    assert int(diag.get("cards_validos_pre_upsert") or 0) == 1
    assert int(diag.get("descartes_total") or 0) == 1
    assert int((diag.get("descartes_por_motivo") or {}).get("titulo_invalido") or 0) == 1


def test_persistir_cards_titulo_fallback_por_url(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(
                titulo="Mensagem](",
                url_anuncio="https://www.zapimoveis.com.br/imovel/venda-casa-de-condominio-3-quartos-taubate-sp-120m2-id-12345/",
            )
        ],
        cidade="Taubate",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
    )
    assert n == 1
    assert "venda casa de condominio" in str(capt["rows"][0]["titulo"]).lower()


def test_persistir_cards_promove_casa_condominio_quando_leilao_indica_condominio(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [_card_base(titulo="Casa com 3 quartos", _tipo_detectado="")],
        cidade="Taubate",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
        leilao_row={"leilao_extra_json": {"observacoes_markdown": "CONDOMÍNIO RESIDENCIAL VILLAGIO DI ITÁLIA"}},
    )
    assert n == 1
    assert capt["rows"][0]["tipo_imovel"] == "casa_condominio"


def test_persistir_cards_preserva_bairro_do_anuncio_por_url(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [
            _card_base(
                bairro="CHACARA DO VISCONDE",
                url_anuncio=(
                    "https://www.zapimoveis.com.br/imovel/venda-casa-de-condominio-3-quartos-"
                    "parque-sao-cristovao-taubate-sp-120m2-id-1/"
                ),
            )
        ],
        cidade="Taubate",
        estado_raw="SP",
        bairro="CHACARA DO VISCONDE",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
    )
    assert n == 1
    assert "Parque Sao Cristovao".lower() in str(capt["rows"][0]["bairro"]).lower()


def test_persistir_cards_nao_promove_condominio_por_texto_juridico_padrao(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [_card_base(titulo="Casa com 3 quartos", _tipo_detectado="")],
        cidade="Taubate",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
        leilao_row={
            "leilao_extra_json": {
                "observacoes_markdown": (
                    "REGRAS PARA PAGAMENTO DAS DESPESAS (caso existam):\n"
                    "Condomínio: Sob responsabilidade do comprador, até o limite de 10%.\n"
                    "Tributos: Sob responsabilidade do comprador."
                )
            }
        },
    )
    assert n == 1
    assert capt["rows"][0]["tipo_imovel"] == "casa"


def test_persistir_cards_nao_grava_bairro_do_leilao_sem_evidencia_do_anuncio(monkeypatch):
    cli = MagicMock()
    capt = {"rows": []}

    def _upsert(_cli, rows):
        capt["rows"] = list(rows)
        return len(rows)

    monkeypatch.setattr(amc.anuncios_mercado_repo, "upsert_lote", _upsert)
    n = amc.persistir_cards_anuncios_mercado(
        cli,
        [_card_base(url_anuncio="https://site.exemplo/imovel/123", bairro="Centro", titulo="Casa 3 quartos")],
        cidade="Campinas",
        estado_raw="SP",
        bairro="Centro",
        leilao_imovel_id="L1",
        url_listagem="x",
        tipo_imovel_fallback="casa",
    )
    assert n == 1
    assert capt["rows"][0]["bairro"] == ""
