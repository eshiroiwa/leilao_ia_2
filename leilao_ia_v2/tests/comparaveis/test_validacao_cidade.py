"""Testes da validação dura de município (PR1).

Cobre o cenário Pindamonhangaba → São Bernardo descrito pelo usuário e os
caminhos de fallback Google → Nominatim (e vice-versa). Todas as chamadas HTTP
são mockadas via ``unittest.mock.patch`` para os testes serem rápidos e
determinísticos.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from leilao_ia_v2.comparaveis import validacao_cidade as vc


# ----------------------- helpers de mock --------------------------------------

class _FakeHttp:
    """Acumula chamadas e devolve respostas planeadas conforme a URL bater."""

    def __init__(self, plano: list[tuple[str, dict | list | None]]):
        self.plano = list(plano)
        self.chamadas: list[str] = []

    def __call__(self, url, *, headers=None, timeout=12.0):
        self.chamadas.append(url)
        for prefix, resposta in self.plano:
            if prefix in url:
                return resposta
        return None


# ----------------------- _slug ------------------------------------------------

class TestSlug:
    @pytest.mark.parametrize(
        "entrada,esperado",
        [
            ("São Paulo", "saopaulo"),
            ("Pindamonhangaba", "pindamonhangaba"),
            (" PINDAMONHANGABA ", "pindamonhangaba"),
            ("São Bernardo do Campo", "saobernardodocampo"),
            ("Mogi das Cruzes", "mogidascruzes"),
            ("", ""),
            ("Áéíóú", "aeiou"),
        ],
    )
    def test_slug(self, entrada, esperado):
        assert vc._slug(entrada) == esperado


# ----------------------- _construir_query_sem_cidade --------------------------

class TestQuerySemCidade:
    def test_omite_cidade(self):
        q = vc._construir_query_sem_cidade(
            logradouro="Rua das Flores 123", bairro="Centro", estado_uf="SP"
        )
        assert "Rua das Flores 123" in q
        assert "Centro" in q
        assert "SP" in q
        assert "Brasil" in q
        assert "Pindamonhangaba" not in q

    def test_so_uf_devolve_vazio(self):
        # Sem logradouro NEM bairro a query é considerada insuficiente:
        # "SP, Brasil" geocodificaria para o centroide do estado, o que faria
        # qualquer cidade-alvo do estado passar como válida.
        q = vc._construir_query_sem_cidade(logradouro="", bairro="", estado_uf="SP")
        assert q == ""

    def test_so_bairro_e_uf_aceito(self):
        q = vc._construir_query_sem_cidade(logradouro="", bairro="Centro", estado_uf="SP")
        assert q == "Centro, SP, Brasil"

    def test_so_logradouro_e_uf_aceito(self):
        q = vc._construir_query_sem_cidade(logradouro="Rua A 10", bairro="", estado_uf="SP")
        assert q == "Rua A 10, SP, Brasil"

    def test_partes_vazias_filtradas(self):
        q = vc._construir_query_sem_cidade(
            logradouro="Rua X", bairro="", estado_uf="SP"
        )
        assert q == "Rua X, SP, Brasil"


# ----------------------- geocode_sem_cidade -----------------------------------

class TestGeocodeSemCidade:
    def test_google_quando_chave_disponivel(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        fake = _FakeHttp(
            [
                (
                    "maps.googleapis.com",
                    {
                        "status": "OK",
                        "results": [
                            {"geometry": {"location": {"lat": -22.92, "lng": -45.46}}}
                        ],
                    },
                )
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake):
            coords = vc.geocode_sem_cidade(
                logradouro="Rua A 10", bairro="Centro", estado_uf="SP"
            )
        assert coords == (-22.92, -45.46)
        assert any("maps.googleapis.com" in c for c in fake.chamadas)

    def test_google_falha_cai_para_nominatim(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        fake = _FakeHttp(
            [
                ("maps.googleapis.com", {"status": "ZERO_RESULTS", "results": []}),
                (
                    "nominatim.openstreetmap.org",
                    [{"lat": "-22.93", "lon": "-45.47"}],
                ),
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            coords = vc.geocode_sem_cidade(
                logradouro="Rua B 20", bairro="Centro", estado_uf="SP"
            )
        assert coords == (-22.93, -45.47)
        # Tem que ter tentado Google primeiro e Nominatim depois.
        assert "maps.googleapis.com" in fake.chamadas[0]
        assert "nominatim.openstreetmap.org" in fake.chamadas[1]

    def test_nominatim_quando_sem_chave_google(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)
        fake = _FakeHttp(
            [("nominatim.openstreetmap.org", [{"lat": "-22.93", "lon": "-45.47"}])]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            coords = vc.geocode_sem_cidade(
                logradouro="Rua C", bairro="Centro", estado_uf="SP"
            )
        assert coords == (-22.93, -45.47)
        # Não deve nunca chamar Google.
        assert all("maps.googleapis.com" not in c for c in fake.chamadas)

    def test_query_vazia_nao_chama_http(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)
        fake = _FakeHttp([])
        with patch.object(vc, "_http_get_json", side_effect=fake):
            coords = vc.geocode_sem_cidade(
                logradouro="", bairro="", estado_uf=""
            )
        assert coords is None
        assert fake.chamadas == []


# ----------------------- reverse_municipio ------------------------------------

class TestReverseMunicipio:
    def test_google_devolve_locality(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        fake = _FakeHttp(
            [
                (
                    "maps.googleapis.com",
                    {
                        "status": "OK",
                        "results": [
                            {
                                "address_components": [
                                    {"long_name": "São Bernardo do Campo", "types": ["locality", "political"]},
                                    {"long_name": "Brasil", "types": ["country"]},
                                ]
                            }
                        ],
                    },
                )
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake):
            nome = vc.reverse_municipio(-23.69, -46.56)
        assert nome == "São Bernardo do Campo"

    def test_google_devolve_admin_level_2(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        fake = _FakeHttp(
            [
                (
                    "maps.googleapis.com",
                    {
                        "status": "OK",
                        "results": [
                            {
                                "address_components": [
                                    {"long_name": "Pindamonhangaba", "types": ["administrative_area_level_2"]},
                                ]
                            }
                        ],
                    },
                )
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake):
            assert vc.reverse_municipio(-22.92, -45.46) == "Pindamonhangaba"

    def test_google_falha_cai_para_nominatim(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        fake = _FakeHttp(
            [
                ("maps.googleapis.com", {"status": "ZERO_RESULTS"}),
                (
                    "nominatim.openstreetmap.org",
                    {"address": {"city": "Pindamonhangaba"}},
                ),
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            assert vc.reverse_municipio(-22.92, -45.46) == "Pindamonhangaba"

    def test_nominatim_town_quando_sem_chave(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)
        fake = _FakeHttp(
            [("nominatim.openstreetmap.org", {"address": {"town": "Taubaté"}})]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            assert vc.reverse_municipio(-23.0, -45.5) == "Taubaté"


# ----------------------- validar_municipio_card -------------------------------

class TestValidarMunicipioCard:
    """Cenários ponta-a-ponta com Google mockado (caminho primário)."""

    def _setup_google(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")

    def test_cidade_alvo_vazia_descartado(self, monkeypatch):
        self._setup_google(monkeypatch)
        with patch.object(vc, "_http_get_json", lambda *a, **k: None):
            r = vc.validar_municipio_card(
                logradouro="Rua X", bairro="Centro", estado_uf="SP", cidade_alvo=""
            )
        assert r.deve_descartar
        assert r.motivo == "cidade_alvo_vazia"

    def test_geocode_falhou_descartado(self, monkeypatch):
        self._setup_google(monkeypatch)
        # Google responde ZERO_RESULTS e Nominatim também não tem nada.
        fake = _FakeHttp(
            [
                ("maps.googleapis.com", {"status": "ZERO_RESULTS", "results": []}),
                ("nominatim.openstreetmap.org", []),
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            r = vc.validar_municipio_card(
                logradouro="Rua Inexistente",
                bairro="",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
            )
        assert r.deve_descartar
        assert r.motivo == "geocode_falhou"

    def test_reverse_falhou_descartado(self, monkeypatch):
        self._setup_google(monkeypatch)
        chamadas = []

        def fake(url, headers=None, timeout=12.0):
            chamadas.append(url)
            if "maps.googleapis.com/maps/api/geocode" in url and "latlng=" not in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -22.92, "lng": -45.46}}}
                    ],
                }
            return {"status": "ZERO_RESULTS"}

        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            r = vc.validar_municipio_card(
                logradouro="Rua X 100",
                bairro="Centro",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
            )
        assert r.deve_descartar
        assert r.motivo == "reverse_falhou"
        assert r.coordenadas == (-22.92, -45.46)

    def test_municipio_diferente_descartado_pinda_x_sao_bernardo(self, monkeypatch):
        """Cenário-bug exacto descrito pelo usuário: card pretensamente
        de Pindamonhangaba mas que geocodifica para São Bernardo do Campo."""
        self._setup_google(monkeypatch)

        def fake(url, headers=None, timeout=12.0):
            if "maps.googleapis.com/maps/api/geocode" in url and "latlng=" not in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -23.69, "lng": -46.56}}}
                    ],
                }
            if "latlng=" in url:
                return {
                    "status": "OK",
                    "results": [
                        {
                            "address_components": [
                                {"long_name": "São Bernardo do Campo", "types": ["locality"]}
                            ]
                        }
                    ],
                }
            return None

        with patch.object(vc, "_http_get_json", side_effect=fake):
            r = vc.validar_municipio_card(
                logradouro="Av. Kennedy 1500",
                bairro="Anchieta",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
            )
        assert r.deve_descartar
        assert r.motivo == "municipio_diferente"
        assert r.municipio_real == "São Bernardo do Campo"
        assert r.municipio_alvo_slug == "pindamonhangaba"
        assert r.municipio_real_slug == "saobernardodocampo"

    def test_municipio_igual_aceito(self, monkeypatch):
        self._setup_google(monkeypatch)

        def fake(url, headers=None, timeout=12.0):
            if "maps.googleapis.com/maps/api/geocode" in url and "latlng=" not in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -22.924, "lng": -45.461}}}
                    ],
                }
            if "latlng=" in url:
                return {
                    "status": "OK",
                    "results": [
                        {
                            "address_components": [
                                {"long_name": "Pindamonhangaba", "types": ["locality"]}
                            ]
                        }
                    ],
                }
            return None

        with patch.object(vc, "_http_get_json", side_effect=fake):
            r = vc.validar_municipio_card(
                logradouro="Rua das Flores 12",
                bairro="Centro",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
            )
        assert r.valido is True
        assert r.motivo == "ok"
        assert r.municipio_real == "Pindamonhangaba"
        assert r.coordenadas == (-22.924, -45.461)

    def test_acentuacao_e_caixa_diferente_aceitos(self, monkeypatch):
        """`São Paulo` vs `SAO PAULO` devem ser considerados o mesmo município."""
        self._setup_google(monkeypatch)

        def fake(url, headers=None, timeout=12.0):
            if "maps.googleapis.com/maps/api/geocode" in url and "latlng=" not in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -23.55, "lng": -46.63}}}
                    ],
                }
            if "latlng=" in url:
                return {
                    "status": "OK",
                    "results": [
                        {
                            "address_components": [
                                {"long_name": "São Paulo", "types": ["locality"]}
                            ]
                        }
                    ],
                }
            return None

        with patch.object(vc, "_http_get_json", side_effect=fake):
            r = vc.validar_municipio_card(
                logradouro="Av. Paulista 1000",
                bairro="Bela Vista",
                estado_uf="SP",
                cidade_alvo="SAO PAULO",
            )
        assert r.valido is True
