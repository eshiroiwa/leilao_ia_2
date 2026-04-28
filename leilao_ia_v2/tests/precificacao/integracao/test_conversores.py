"""
Testes dos conversores row Supabase ↔ tipos do domínio precificação.
"""

from __future__ import annotations

from leilao_ia_v2.precificacao.dominio import (
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_DESCONHECIDA,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
)
from leilao_ia_v2.precificacao.integracao.conversores import (
    anuncio_row_para_amostra,
    leilao_row_para_alvo,
)


class TestLeilaoRowParaAlvo:
    def test_lance_2_praca_tem_prioridade_sobre_1_praca(self):
        row = {
            "cidade": "Pindamonhangaba",
            "estado": "SP",
            "bairro": "Araretama",
            "tipo_imovel": "apartamento",
            "area_util": 58.0,
            "valor_lance_1_praca": 200_000.0,
            "valor_lance_2_praca": 145_000.0,
            "latitude": -22.9,
            "longitude": -45.4,
        }
        a = leilao_row_para_alvo(row)
        assert a.cidade == "Pindamonhangaba"
        assert a.estado_uf == "SP"
        assert a.tipo_imovel == "apartamento"
        assert a.area_m2 == 58.0
        assert a.lance_minimo == 145_000.0  # 2ª praça vence
        assert a.latitude == -22.9
        assert a.longitude == -45.4

    def test_fallback_para_1_praca_quando_2_ausente(self):
        row = {
            "cidade": "X", "estado": "SP", "tipo_imovel": "casa",
            "area_util": 80,
            "valor_lance_1_praca": 200_000.0,
            "valor_lance_2_praca": None,
        }
        a = leilao_row_para_alvo(row)
        assert a.lance_minimo == 200_000.0

    def test_lance_zero_eh_ignorado(self):
        row = {
            "cidade": "X", "estado": "SP", "tipo_imovel": "casa",
            "area_util": 80,
            "valor_lance_1_praca": 0,
            "valor_lance_2_praca": 0,
        }
        a = leilao_row_para_alvo(row)
        assert a.lance_minimo is None

    def test_area_util_tem_prioridade_sobre_construida(self):
        row = {
            "cidade": "X", "estado": "SP", "tipo_imovel": "casa",
            "area_util": 80,
            "area_construida": 120,
        }
        a = leilao_row_para_alvo(row)
        assert a.area_m2 == 80.0

    def test_estado_normalizado_para_2_letras_maiusculas(self):
        a = leilao_row_para_alvo({"cidade": "X", "estado": "sp ", "tipo_imovel": "casa", "area_util": 80})
        assert a.estado_uf == "SP"

    def test_dados_minimos_funcionam(self):
        a = leilao_row_para_alvo({"cidade": "X", "estado": "SP"})
        assert a.cidade == "X"
        assert a.area_m2 == 0.0
        assert a.lance_minimo is None
        assert a.tipo_imovel == "desconhecido"


class TestAnuncioRowParaAmostra:
    def _row_base(self, **over):
        base = {
            "url_anuncio": "https://portal.com/x",
            "tipo_imovel": "apartamento",
            "area_construida_m2": 60.0,
            "valor_venda": 300_000.0,
            "metadados_json": {"precisao_geo": "rua"},
        }
        base.update(over)
        return base

    def test_conversao_ok(self):
        a = anuncio_row_para_amostra(self._row_base(), distancia_km=0.4, raio_origem_m=500)
        assert a is not None
        assert a.url == "https://portal.com/x"
        assert a.area_m2 == 60.0
        assert a.valor_anuncio == 300_000.0
        assert a.precisao_geo == PRECISAO_RUA
        assert a.distancia_km == 0.4
        assert a.raio_origem_m == 500
        assert a.preco_m2 == 5000.0

    def test_url_vazia_descarta(self):
        assert anuncio_row_para_amostra(self._row_base(url_anuncio=""), distancia_km=0, raio_origem_m=0) is None

    def test_area_zero_descarta(self):
        assert anuncio_row_para_amostra(self._row_base(area_construida_m2=0), distancia_km=0, raio_origem_m=0) is None

    def test_valor_negativo_descarta(self):
        assert anuncio_row_para_amostra(self._row_base(valor_venda=-1), distancia_km=0, raio_origem_m=0) is None

    def test_sem_metadados_assume_desconhecida(self):
        a = anuncio_row_para_amostra(self._row_base(metadados_json=None), distancia_km=0, raio_origem_m=0)
        assert a is not None
        assert a.precisao_geo == PRECISAO_DESCONHECIDA

    def test_precisao_bairro_centroide_mapeada(self):
        a = anuncio_row_para_amostra(
            self._row_base(metadados_json={"precisao_geo": "bairro_centroide"}),
            distancia_km=0, raio_origem_m=0,
        )
        assert a.precisao_geo == PRECISAO_BAIRRO

    def test_precisao_cidade_centroide_mapeada(self):
        a = anuncio_row_para_amostra(
            self._row_base(metadados_json={"precisao_geo": "cidade_centroide"}),
            distancia_km=0, raio_origem_m=0,
        )
        assert a.precisao_geo == PRECISAO_CIDADE

    def test_precisao_rooftop_mapeada(self):
        a = anuncio_row_para_amostra(
            self._row_base(metadados_json={"precisao_geo": "rooftop"}),
            distancia_km=0, raio_origem_m=0,
        )
        assert a.precisao_geo == PRECISAO_ROOFTOP

    def test_precisao_desconhecida_typo_legado(self):
        # Tolerância a typo antigo "desconhecida" (vs "desconhecido").
        a = anuncio_row_para_amostra(
            self._row_base(metadados_json={"precisao_geo": "desconhecida"}),
            distancia_km=0, raio_origem_m=0,
        )
        assert a.precisao_geo == PRECISAO_DESCONHECIDA
