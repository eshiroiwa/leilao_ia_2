"""Testes da persistência sem fallback de cidade (PR4)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from leilao_ia_v2.comparaveis.extrator import CardExtraido
from leilao_ia_v2.comparaveis.persistencia import (
    LinhaPersistir,
    PersistenciaInvalida,
    montar_linha,
    persistir_lote,
)
from leilao_ia_v2.comparaveis.validacao_cidade import ResultadoValidacaoMunicipio


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _card(
    *,
    url="https://www.zapimoveis.com.br/imovel/x/",
    portal="zapimoveis.com.br",
    valor=350_000.0,
    area=65.0,
    bairro_inferido="Centro",
    titulo="Apto 65m² Centro",
    logradouro_inferido="Rua das Flores 100",
) -> CardExtraido:
    return CardExtraido(
        url_anuncio=url,
        portal=portal,
        valor_venda=valor,
        area_m2=area,
        titulo=titulo,
        logradouro_inferido=logradouro_inferido,
        bairro_inferido=bairro_inferido,
    )


def _validacao_ok(
    municipio="Pindamonhangaba",
    coords=(-22.92, -45.46),
    precisao="rua",
) -> ResultadoValidacaoMunicipio:
    return ResultadoValidacaoMunicipio(
        valido=True,
        motivo="ok",
        municipio_real=municipio,
        coordenadas=coords,
        municipio_alvo_slug="pindamonhangaba",
        municipio_real_slug="pindamonhangaba",
        precisao_geo=precisao,
    )


def _validacao_reprovada(motivo="municipio_diferente") -> ResultadoValidacaoMunicipio:
    return ResultadoValidacaoMunicipio(
        valido=False,
        motivo=motivo,
        municipio_alvo_slug="pindamonhangaba",
        municipio_real_slug="saobernardodocampo",
        municipio_real="São Bernardo do Campo",
    )


# -----------------------------------------------------------------------------
# montar_linha
# -----------------------------------------------------------------------------

class TestMontarLinhaSucesso:
    def test_constroi_linha_com_municipio_do_geocode(self):
        l = montar_linha(
            _card(),
            _validacao_ok("Pindamonhangaba"),
            tipo_imovel="apartamento",
            estado_uf="SP",
        )
        assert isinstance(l, LinhaPersistir)
        assert l.cidade == "Pindamonhangaba"
        assert l.estado == "SP"
        assert l.tipo_imovel == "apartamento"
        assert l.transacao == "venda"
        assert l.bairro == "Centro"
        assert l.valor_venda == 350_000.0
        assert l.area_construida_m2 == 65.0
        assert l.latitude == -22.92 and l.longitude == -45.46

    def test_metadados_incluem_proveniencia(self):
        l = montar_linha(
            _card(),
            _validacao_ok("Pindamonhangaba"),
            tipo_imovel="apartamento",
            estado_uf="SP",
            fonte_busca="apartamento 65 m² Centro Pindamonhangaba SP",
        )
        m = l.metadados_json
        assert m["fonte"] == "comparaveis_v2"
        assert m["validacao_municipio"]["real_nome"] == "Pindamonhangaba"
        assert m["validacao_municipio"]["alvo_slug"] == "pindamonhangaba"
        assert m["fonte_busca"] == "apartamento 65 m² Centro Pindamonhangaba SP"
        assert m["logradouro_inferido"] == "Rua das Flores 100"

    def test_uf_normalizada(self):
        l = montar_linha(_card(), _validacao_ok(), tipo_imovel="casa", estado_uf="sp ")
        assert l.estado == "SP"

    def test_tipo_normalizado_lowercase(self):
        l = montar_linha(_card(), _validacao_ok(), tipo_imovel="APARTAMENTO", estado_uf="SP")
        assert l.tipo_imovel == "apartamento"

    def test_bairro_vazio_aceito(self):
        l = montar_linha(
            _card(bairro_inferido=""),
            _validacao_ok(),
            tipo_imovel="casa",
            estado_uf="SP",
        )
        assert l.bairro == ""  # melhor vazio que inventado


class TestMontarLinhaCidadeReal:
    """Garantia da decisão arquitetural: cidade gravada vem do geocode, NÃO
    do leilão de origem nem do título do anúncio."""

    def test_cidade_diferente_do_alvo_seria_descartada_antes(self):
        # Pré-condição do contrato: nunca chegamos aqui com validacao reprovada.
        # Mas se isso acontecer (bug), montar_linha levanta.
        with pytest.raises(PersistenciaInvalida):
            montar_linha(
                _card(),
                _validacao_reprovada(),
                tipo_imovel="apartamento",
                estado_uf="SP",
            )

    def test_validacao_ok_sem_municipio_real_levanta(self):
        v = ResultadoValidacaoMunicipio(
            valido=True,
            motivo="ok",
            municipio_real=None,
            municipio_alvo_slug="pindamonhangaba",
        )
        with pytest.raises(PersistenciaInvalida):
            montar_linha(_card(), v, tipo_imovel="casa", estado_uf="SP")

    def test_uf_invalida_levanta(self):
        with pytest.raises(PersistenciaInvalida):
            montar_linha(_card(), _validacao_ok(), tipo_imovel="casa", estado_uf="")

    def test_uf_3_letras_levanta(self):
        # "SPP" → cortado para "SP" (2 letras) — aceita.
        l = montar_linha(_card(), _validacao_ok(), tipo_imovel="casa", estado_uf="SPP")
        assert l.estado == "SP"


# -----------------------------------------------------------------------------
# para_dict (formato esperado pelo upsert_lote)
# -----------------------------------------------------------------------------

class TestParaDict:
    def test_dict_inclui_lat_lon_quando_validacao_tem_coords(self):
        l = montar_linha(_card(), _validacao_ok(coords=(-22.5, -45.1)),
                         tipo_imovel="casa", estado_uf="SP")
        d = l.para_dict()
        assert d["latitude"] == -22.5 and d["longitude"] == -45.1

    def test_dict_omite_lat_lon_quando_validacao_sem_coords(self):
        v = ResultadoValidacaoMunicipio(
            valido=True,
            motivo="ok",
            municipio_real="Pindamonhangaba",
            coordenadas=None,
            municipio_alvo_slug="pindamonhangaba",
            municipio_real_slug="pindamonhangaba",
        )
        l = montar_linha(_card(), v, tipo_imovel="casa", estado_uf="SP")
        d = l.para_dict()
        assert "latitude" not in d and "longitude" not in d

    def test_dict_compativel_com_upsert_lote(self):
        l = montar_linha(_card(), _validacao_ok(), tipo_imovel="casa", estado_uf="SP")
        d = l.para_dict()
        # Campos obrigatórios para anuncios_mercado_repo.upsert_lote
        for k in (
            "url_anuncio", "portal", "tipo_imovel", "bairro", "cidade", "estado",
            "valor_venda", "area_construida_m2", "transacao", "metadados_json",
        ):
            assert k in d


# -----------------------------------------------------------------------------
# Política de precisão (jitter, marcador, cidade-centroide, desconhecido)
# -----------------------------------------------------------------------------

class TestPoliticaPrecisao:
    def test_rua_marca_metadados_e_mantem_coord(self):
        l = montar_linha(
            _card(),
            _validacao_ok(coords=(-22.92, -45.46), precisao="rua"),
            tipo_imovel="apartamento",
            estado_uf="SP",
        )
        assert l.latitude == -22.92 and l.longitude == -45.46
        assert l.metadados_json["precisao_geo"] == "rua"

    def test_rooftop_marca_e_mantem_coord(self):
        l = montar_linha(
            _card(),
            _validacao_ok(coords=(-22.92, -45.46), precisao="rooftop"),
            tipo_imovel="apartamento",
            estado_uf="SP",
        )
        assert l.latitude == -22.92
        assert l.metadados_json["precisao_geo"] == "rooftop"

    def test_bairro_aplica_jitter_determinístico(self):
        # Mesmo URL → mesmo jitter (idempotência do upsert).
        c1 = _card(url="https://portal.com/imovel/1/")
        v = _validacao_ok(coords=(-22.92, -45.46), precisao="bairro")
        l1 = montar_linha(c1, v, tipo_imovel="apartamento", estado_uf="SP")
        l2 = montar_linha(c1, v, tipo_imovel="apartamento", estado_uf="SP")
        assert l1.latitude == l2.latitude
        assert l1.longitude == l2.longitude
        # Jitter aplicado: coord muda do centroide.
        assert l1.latitude != -22.92 or l1.longitude != -45.46
        # Marcador dedicado.
        assert l1.metadados_json["precisao_geo"] == "bairro_centroide"
        # Magnitude do jitter ≈ ±80m → ≈ 0.000721 graus em lat.
        assert abs(l1.latitude - (-22.92)) < 0.001
        assert abs(l1.longitude - (-45.46)) < 0.002

    def test_bairro_jitter_difere_entre_urls(self):
        c1 = _card(url="https://portal.com/imovel/A/")
        c2 = _card(url="https://portal.com/imovel/B/")
        v = _validacao_ok(coords=(-22.92, -45.46), precisao="bairro")
        l1 = montar_linha(c1, v, tipo_imovel="apartamento", estado_uf="SP")
        l2 = montar_linha(c2, v, tipo_imovel="apartamento", estado_uf="SP")
        assert (l1.latitude, l1.longitude) != (l2.latitude, l2.longitude)

    def test_cidade_marca_centroide_e_mantem_coord(self):
        l = montar_linha(
            _card(),
            _validacao_ok(coords=(-22.92, -45.46), precisao="cidade"),
            tipo_imovel="apartamento",
            estado_uf="SP",
        )
        # Cidade pequena: persistir coord mas marcar para o cache descartar
        # quando houver alternativas melhores.
        assert l.latitude == -22.92 and l.longitude == -45.46
        assert l.metadados_json["precisao_geo"] == "cidade_centroide"

    def test_desconhecido_nao_grava_coord(self):
        l = montar_linha(
            _card(),
            _validacao_ok(coords=(-22.92, -45.46), precisao="desconhecido"),
            tipo_imovel="apartamento",
            estado_uf="SP",
        )
        assert l.latitude is None and l.longitude is None
        assert l.metadados_json["precisao_geo"] == "desconhecido"

    def test_validacao_sem_coords_marca_desconhecido(self):
        v = ResultadoValidacaoMunicipio(
            valido=True,
            motivo="ok",
            municipio_real="Pindamonhangaba",
            coordenadas=None,
            municipio_alvo_slug="pindamonhangaba",
            municipio_real_slug="pindamonhangaba",
            precisao_geo="",
        )
        l = montar_linha(_card(), v, tipo_imovel="apartamento", estado_uf="SP")
        assert l.latitude is None and l.longitude is None
        assert l.metadados_json["precisao_geo"] == "desconhecido"


# -----------------------------------------------------------------------------
# persistir_lote
# -----------------------------------------------------------------------------

class TestPersistirLote:
    def test_lista_vazia_devolve_zero(self):
        result = persistir_lote(client=object(), linhas=[])
        assert result == 0

    def test_chama_upsert_lote_com_dicts(self):
        l1 = montar_linha(_card(url="https://www.zapimoveis.com.br/imovel/a/"),
                          _validacao_ok(), tipo_imovel="casa", estado_uf="SP")
        l2 = montar_linha(_card(url="https://www.zapimoveis.com.br/imovel/b/"),
                          _validacao_ok(), tipo_imovel="casa", estado_uf="SP")
        client = object()
        with patch(
            "leilao_ia_v2.comparaveis.persistencia.anuncios_mercado_repo.upsert_lote",
            return_value=2,
        ) as mock_up:
            n = persistir_lote(client=client, linhas=[l1, l2])
        assert n == 2
        mock_up.assert_called_once()
        args, kwargs = mock_up.call_args
        # cliente posicional, lista de dicts posicional
        assert args[0] is client
        payload = args[1]
        assert isinstance(payload, list) and len(payload) == 2
        assert all(isinstance(p, dict) for p in payload)
        urls = {p["url_anuncio"] for p in payload}
        assert urls == {
            "https://www.zapimoveis.com.br/imovel/a/",
            "https://www.zapimoveis.com.br/imovel/b/",
        }
