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
        assert coords is not None
        assert coords[0:2] == (-22.92, -45.46)
        assert isinstance(coords[2], str)
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
        assert coords is not None
        assert coords[0:2] == (-22.93, -45.47)
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
        assert coords is not None
        assert coords[0:2] == (-22.93, -45.47)
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


# ----------------------- camada texto local (cidade_no_markdown) ---------------

class TestCamadaTextoLocal:
    """Quando o extrator viu a cidade-alvo no markdown, validamos sem geocode
    (e obtemos coords COM cidade)."""

    def test_match_textual_aceita_sem_consultar_reverse(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        chamadas: list[str] = []

        def fake(url, headers=None, timeout=12.0):
            chamadas.append(url)
            # Geocode COM cidade devolve coords precisas.
            if "address=" in url and "Pindamonhangaba" in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -22.92, "lng": -45.46}}}
                    ],
                }
            return None

        with patch.object(vc, "_http_get_json", side_effect=fake):
            r = vc.validar_municipio_card(
                logradouro="Rua Cônego João",
                bairro="Santana",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
                cidade_no_markdown="Pindamonhangaba",
            )
        assert r.valido is True
        assert r.motivo == "ok_texto_local"
        assert r.municipio_real == "Pindamonhangaba"
        assert r.coordenadas == (-22.92, -45.46)
        # NUNCA chamou reverse (latlng=)
        assert all("latlng=" not in u for u in chamadas)

    def test_texto_local_acentuacao_caixa(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)
        with patch.object(vc, "_http_get_json", lambda *a, **k: None), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            r = vc.validar_municipio_card(
                logradouro="Rua X",
                bairro="Centro",
                estado_uf="SP",
                cidade_alvo="São Paulo",
                cidade_no_markdown="SAO PAULO",
            )
        # Mesmo sem chave Google e Nominatim devolvendo None, aceitamos via texto.
        assert r.valido is True
        assert r.motivo == "ok_texto_local"
        assert r.coordenadas is None  # geocode com cidade falhou, mas validamos mesmo assim

    def test_texto_local_diferente_da_cidade_alvo_nao_aplica(self, monkeypatch):
        """Se cidade_no_markdown não bate com a cidade-alvo, ignora a camada 1."""
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        # Cai na camada 2 normalmente — mockamos geocode + reverse OK.
        def fake(url, headers=None, timeout=12.0):
            if "maps.googleapis.com/maps/api/geocode" in url and "latlng=" not in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -22.92, "lng": -45.46}}}
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
                logradouro="Rua X",
                bairro="Centro",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
                cidade_no_markdown="Outracidade",
            )
        # Caiu na camada 2 (reverse OK)
        assert r.valido is True
        assert r.motivo == "ok"


# ----------------------- camada página confirmada (rescue) --------------------

class TestCamadaPaginaConfirmada:
    """Quando geocode falha/diverge mas a página foi confirmada, aceitamos."""

    def test_geocode_diverge_mas_pagina_confirmada(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")

        def fake(url, headers=None, timeout=12.0):
            # Geocode SEM cidade devolve SBC (errado)
            if "address=" in url and "Pindamonhangaba" not in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -23.69, "lng": -46.56}}}
                    ],
                }
            # Reverse devolve SBC
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
            # Geocode COM cidade devolve coords corretas
            if "address=" in url and "Pindamonhangaba" in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -22.93, "lng": -45.47}}}
                    ],
                }
            return None

        with patch.object(vc, "_http_get_json", side_effect=fake):
            r = vc.validar_municipio_card(
                logradouro="Rua X",
                bairro="Santana",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
                pagina_confirmada=True,
            )
        assert r.valido is True
        assert r.motivo == "ok_pagina_confirmada"
        assert r.municipio_real == "Pindamonhangaba"
        assert r.coordenadas == (-22.93, -45.47)

    def test_geocode_falhou_mas_pagina_confirmada(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")

        def fake(url, headers=None, timeout=12.0):
            if "address=" in url and "Pindamonhangaba" in url:
                return {
                    "status": "OK",
                    "results": [
                        {"geometry": {"location": {"lat": -22.93, "lng": -45.47}}}
                    ],
                }
            return {"status": "ZERO_RESULTS", "results": []}

        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            r = vc.validar_municipio_card(
                logradouro="Rua Y",
                bairro="",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
                pagina_confirmada=True,
            )
        assert r.valido is True
        assert r.motivo == "ok_pagina_confirmada"

    def test_pagina_NAO_confirmada_e_geocode_diverge_descarta(self, monkeypatch):
        """Sem rescue: comportamento idêntico ao antigo (regressão)."""
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")

        def fake(url, headers=None, timeout=12.0):
            if "address=" in url and "Pindamonhangaba" not in url:
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
                logradouro="Rua X",
                bairro="Anchieta",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
                pagina_confirmada=False,
            )
        assert r.deve_descartar
        assert r.motivo == "municipio_diferente"


# ----------------------- obter_coordenadas_com_cidade -------------------------

class TestObterCoordenadasComCidade:
    def test_query_inclui_cidade(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        capturadas: list[str] = []

        def fake(url, headers=None, timeout=12.0):
            capturadas.append(url)
            return {
                "status": "OK",
                "results": [
                    {"geometry": {"location": {"lat": -22.92, "lng": -45.46}}}
                ],
            }

        with patch.object(vc, "_http_get_json", side_effect=fake):
            coords = vc.obter_coordenadas_com_cidade(
                logradouro="Rua X 100",
                bairro="Centro",
                cidade="Pindamonhangaba",
                estado_uf="SP",
            )
        assert coords is not None
        assert coords[0:2] == (-22.92, -45.46)
        assert isinstance(coords[2], str)
        assert any("Pindamonhangaba" in u for u in capturadas)

    def test_sem_cidade_devolve_none(self):
        coords = vc.obter_coordenadas_com_cidade(
            logradouro="Rua X", bairro="", cidade="", estado_uf="SP"
        )
        assert coords is None

    def test_fallback_nominatim_quando_google_falha(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        fake = _FakeHttp(
            [
                ("maps.googleapis.com", {"status": "ZERO_RESULTS"}),
                (
                    "nominatim.openstreetmap.org",
                    [{"lat": "-22.93", "lon": "-45.47"}],
                ),
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake), patch.object(
            vc, "_rate_limit_nominatim", lambda: None
        ):
            coords = vc.obter_coordenadas_com_cidade(
                logradouro="Rua X",
                bairro="Centro",
                cidade="Pindamonhangaba",
                estado_uf="SP",
            )
        assert coords is not None
        assert coords[0:2] == (-22.93, -45.47)


# ----------------------- classificação de precisão ---------------------------

class TestClassificarPrecisaoGoogle:
    def test_rooftop(self):
        r = {"geometry": {"location_type": "ROOFTOP"}, "types": ["street_address"]}
        assert vc._classificar_precisao_google(r) == vc.PRECISAO_ROOFTOP

    def test_range_interpolated_eh_rooftop(self):
        r = {"geometry": {"location_type": "RANGE_INTERPOLATED"}, "types": ["route"]}
        assert vc._classificar_precisao_google(r) == vc.PRECISAO_ROOFTOP

    def test_route_geometric_center(self):
        r = {"geometry": {"location_type": "GEOMETRIC_CENTER"}, "types": ["route"]}
        assert vc._classificar_precisao_google(r) == vc.PRECISAO_RUA

    def test_sublocality(self):
        r = {
            "geometry": {"location_type": "GEOMETRIC_CENTER"},
            "types": ["sublocality_level_1", "political"],
        }
        assert vc._classificar_precisao_google(r) == vc.PRECISAO_BAIRRO

    def test_locality(self):
        r = {
            "geometry": {"location_type": "APPROXIMATE"},
            "types": ["locality", "political"],
        }
        assert vc._classificar_precisao_google(r) == vc.PRECISAO_CIDADE

    def test_geometric_center_sem_types_eh_rua(self):
        r = {"geometry": {"location_type": "GEOMETRIC_CENTER"}, "types": []}
        assert vc._classificar_precisao_google(r) == vc.PRECISAO_RUA

    def test_approximate_sem_types_eh_cidade(self):
        r = {"geometry": {"location_type": "APPROXIMATE"}, "types": []}
        assert vc._classificar_precisao_google(r) == vc.PRECISAO_CIDADE

    def test_vazio_eh_desconhecido(self):
        assert vc._classificar_precisao_google({}) == vc.PRECISAO_DESCONHECIDA


class TestClassificarPrecisaoNominatim:
    def test_house(self):
        r = {"class": "place", "type": "house", "addresstype": "house"}
        assert vc._classificar_precisao_nominatim(r) == vc.PRECISAO_ROOFTOP

    def test_highway_residential(self):
        r = {"class": "highway", "type": "residential", "addresstype": "road"}
        assert vc._classificar_precisao_nominatim(r) == vc.PRECISAO_RUA

    def test_suburb(self):
        r = {"class": "place", "type": "suburb", "addresstype": "suburb"}
        assert vc._classificar_precisao_nominatim(r) == vc.PRECISAO_BAIRRO

    def test_city(self):
        r = {"class": "place", "type": "city", "addresstype": "city"}
        assert vc._classificar_precisao_nominatim(r) == vc.PRECISAO_CIDADE

    def test_vazio_eh_desconhecido(self):
        assert vc._classificar_precisao_nominatim({}) == vc.PRECISAO_DESCONHECIDA


class TestPrecisaoFluiNoResultado:
    """Garante que ResultadoValidacaoMunicipio.precisao_geo é preenchido."""

    def test_ok_texto_local_propaga_precisao(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
        fake = _FakeHttp(
            [
                (
                    "maps.googleapis.com",
                    {
                        "status": "OK",
                        "results": [
                            {
                                "geometry": {
                                    "location": {"lat": -22.92, "lng": -45.46},
                                    "location_type": "GEOMETRIC_CENTER",
                                },
                                "types": ["sublocality"],
                            }
                        ],
                    },
                )
            ]
        )
        with patch.object(vc, "_http_get_json", side_effect=fake):
            r = vc.validar_municipio_card(
                logradouro="Rua A",
                bairro="Centro",
                estado_uf="SP",
                cidade_alvo="Pindamonhangaba",
                cidade_no_markdown="Pindamonhangaba",
            )
        assert r.valido is True
        assert r.motivo == "ok_texto_local"
        assert r.precisao_geo == vc.PRECISAO_BAIRRO
