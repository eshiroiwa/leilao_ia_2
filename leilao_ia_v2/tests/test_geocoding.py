import os
from unittest.mock import MagicMock, patch

from leilao_ia_v2.services import geocoding as geo


def test_extrair_logradouro_de_url_vivareal_slug():
    u = "https://www.vivareal.com.br/imovel/venda-apartamento-rua-das-flores-centro-12345678"
    s = geo.extrair_logradouro_de_url(u)
    assert "flores" in s.lower() or "rua" in s.lower()


def test_extrair_logradouro_de_url_vazio():
    assert geo.extrair_logradouro_de_url("") == ""


def test_geocodificar_endereco_rua_structured():
    with patch.dict(os.environ, {"GEOCODING_PROVIDER": "nominatim"}, clear=False):
        with patch.object(geo, "_geocode_structured_cached", return_value=(-23.5, -46.6)) as m_struct:
            with patch.object(geo, "_geocode_cached", return_value=None):
                r = geo.geocodificar_endereco(
                    logradouro="Avenida Paulista, 1000",
                    cidade="São Paulo",
                    estado="SP",
                )
    assert r == (-23.5, -46.6)
    m_struct.assert_called()


def test_geocode_provider_google_usa_google_primeiro_e_fallback_nominatim():
    with patch.dict(os.environ, {"GEOCODING_PROVIDER": "google"}, clear=False):
        with patch.object(geo, "_geocode_google_cached", return_value=None) as m_g:
            with patch.object(geo, "_geocode_cached", return_value=(-1.0, -2.0)) as m_n:
                r = geo._geocode_by_provider("Rua Teste, Brasil")
    assert r == (-1.0, -2.0)
    m_g.assert_called_once()
    m_n.assert_called_once()


def test_geocode_structured_provider_google_prefere_google():
    with patch.dict(os.environ, {"GEOCODING_PROVIDER": "google"}, clear=False):
        with patch.object(geo, "_geocode_google_structured_cached", return_value=(-3.0, -4.0)) as m_g:
            with patch.object(geo, "_geocode_structured_cached", return_value=(-9.0, -9.0)) as m_n:
                r = geo._geocode_structured_by_provider("Av. X, 10", "São Paulo", "São Paulo")
    assert r == (-3.0, -4.0)
    m_g.assert_called_once()
    m_n.assert_not_called()


def test_geocode_structured_provider_google_fallback_nominatim():
    with patch.dict(os.environ, {"GEOCODING_PROVIDER": "google"}, clear=False):
        with patch.object(geo, "_geocode_google_structured_cached", return_value=None) as m_g:
            with patch.object(geo, "_geocode_structured_cached", return_value=(-9.0, -9.0)) as m_n:
                r = geo._geocode_structured_by_provider("Av. X, 10", "São Paulo", "São Paulo")
    assert r == (-9.0, -9.0)
    m_g.assert_called_once()
    m_n.assert_called_once()


def test_geocodificar_endereco_google_structured():
    with patch.dict(os.environ, {"GEOCODING_PROVIDER": "google"}, clear=False):
        with patch.object(geo, "_geocode_google_structured_cached", return_value=(-23.5, -46.6)) as m_g:
            with patch.object(geo, "_geocode_structured_cached", return_value=None):
                with patch.object(geo, "_geocode_google_cached", return_value=None):
                    with patch.object(geo, "_geocode_cached", return_value=None):
                        r = geo.geocodificar_endereco(
                            logradouro="Avenida Paulista, 1000",
                            cidade="São Paulo",
                            estado="SP",
                        )
    assert r == (-23.5, -46.6)
    m_g.assert_called()


def test_geocodificar_texto_livre_chama_provider():
    with patch.dict(os.environ, {"GEOCODING_PROVIDER": "nominatim"}, clear=False):
        with patch.object(geo, "_geocode_by_provider", return_value=(-20.0, -43.0)) as m:
            r = geo.geocodificar_endereco(
                logradouro="",
                bairro="",
                cidade="",
                estado="",
            )
    assert r is None
    m.assert_not_called()


def test_geocodificar_texto_livre_curto_retorna_none():
    assert geo.geocodificar_texto_livre("  a  ") is None
    assert geo.geocodificar_texto_livre("") is None


def test_geocodificar_texto_livre_chama_cached():
    with patch.object(geo, "_geocode_by_provider", return_value=(-20.0, -43.0)) as m:
        r = geo.geocodificar_texto_livre("Rua Teste, 1, Ouro Preto, MG")
    assert r == (-20.0, -43.0)
    m.assert_called_once()
    args, _ = m.call_args
    assert "Ouro Preto" in args[0]
    assert "Brasil" in args[0]


def test_geocodificar_endereco_sem_centro_cidade_quando_desligado():
    """Sem logradouro: com bairro+cidade, não deve usar fallback só-cidade se desligado."""
    with patch.object(geo, "_geocode_structured_by_provider", return_value=None):
        with patch.object(geo, "_geocode_by_provider", return_value=None):
            r = geo.geocodificar_endereco(
                logradouro="",
                bairro="Bairro Inexistente No OSM XYZ123",
                cidade="Cidade Inexistente ABC987",
                estado="SP",
                permitir_fallback_bairro=True,
                permitir_fallback_centro_cidade=False,
            )
    assert r is None


def test_geocodificar_anuncios_batch_passa_flag_centro_cidade():
    anuncios = [
        {
            "url_anuncio": "https://www.vivareal.com.br/x",
            "titulo": "Apartamento",
            "bairro": "Moema",
            "cidade": "São Paulo",
            "estado": "SP",
            "logradouro": "",
        }
    ]
    with patch.object(geo, "geocodificar_endereco", return_value=(-1.0, -2.0)) as m_geo:
        n = geo.geocodificar_anuncios_batch(
            anuncios,
            cidade="São Paulo",
            estado="SP",
            permitir_fallback_centro_cidade=False,
        )
    assert n == 1
    assert anuncios[0]["latitude"] == -1.0
    assert anuncios[0]["longitude"] == -2.0
    assert m_geo.call_args.kwargs.get("permitir_fallback_centro_cidade") is False


def test_sanear_logradouro_markdown_card_remove_barras_finais():
    assert geo.sanear_logradouro_markdown_card("Rua das Flores, 12\\\\") == "Rua das Flores, 12"
    assert geo.sanear_logradouro_markdown_card("Av. Brasil  \\\n") == "Av. Brasil"


def test_melhor_logradouro_janela_proximo_url_prefere_linha_perto_do_link():
    bloco = """
    Resumo do imóvel
    https://exemplo.com/x
    Rua XV de Novembro, 400 — complemento
    Valor R$ 1
    """.strip()
    idx = bloco.find("https://")
    r = geo.melhor_logradouro_janela_proximo_url(bloco, idx)
    assert "XV" in r or "novembro" in r.lower()


def test_logradouro_tem_ou_nao_numero():
    assert geo._logradouro_tem_numero_imovel("Rua Barão Carlos de Sousa Anhumas") is False
    assert geo._logradouro_tem_numero_imovel("Avenida Paulista, 1000") is True
    assert geo._logradouro_tem_numero_imovel("Rua X, 380 Apto 24") is True
    assert geo._logradouro_tem_numero_imovel("Rua Y, 12") is True
    assert geo._logradouro_tem_numero_imovel("") is False
    # Metragem no texto (listagens) não conta como número de porta
    assert geo._logradouro_tem_numero_imovel("Rua Barão Carlos, 38 m²") is False
    assert geo._logradouro_tem_numero_imovel("Rua X, 100 m2") is False


def test_rua_sem_numero_tenta_bairro_antes_da_rua():
    m_struct = MagicMock(return_value=(-23.0, -46.0))

    def moema_primeiro(q: str) -> tuple | None:
        if "Moema" in q and "São Paulo" in q:
            return (-22.0, -47.0)
        return None

    with patch.object(geo, "_geocode_structured_by_provider", m_struct):
        with patch.object(geo, "_geocode_by_provider", side_effect=moema_primeiro):
            r = geo.geocodificar_endereco(
                logradouro="Rua Sem Número Aqui Muito Longo",
                bairro="Moema",
                cidade="São Paulo",
                estado="SP",
                permitir_fallback_centro_cidade=False,
            )
    assert r == (-22.0, -47.0)
    m_struct.assert_not_called()


def test_melhor_logradouro_captura_endereco_rotulo_chaves():
    bloco = """
    https://www.chavesnamao.com.br/imovel/x
    Endereço: Av. Paulista, 1578 - Bela Vista
    Metragem 90 m²
    """.strip()
    idx = bloco.find("https://")
    r = geo.melhor_logradouro_janela_proximo_url(bloco, idx)
    assert "Paulista" in r
