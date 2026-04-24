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
