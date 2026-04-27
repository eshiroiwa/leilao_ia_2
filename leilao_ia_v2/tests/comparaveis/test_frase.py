"""Testes da construção de UMA frase de busca focada (PR1)."""

from __future__ import annotations

import pytest

from leilao_ia_v2.comparaveis.frase import (
    FraseBusca,
    montar_frase_busca,
)


class TestFraseBasica:
    def test_apenas_cidade_e_uf(self):
        f = montar_frase_busca(cidade="Pindamonhangaba", estado_uf="SP")
        assert isinstance(f, FraseBusca)
        assert f.texto == "Pindamonhangaba SP"
        assert f.componentes == {"cidade": "Pindamonhangaba", "uf": "SP"}

    def test_cidade_vazia_invalida(self):
        f = montar_frase_busca(cidade="", estado_uf="SP")
        assert f.vazia
        assert f.componentes.get("motivo_vazio") == "cidade_ou_uf_ausente"

    def test_uf_vazia_invalida(self):
        f = montar_frase_busca(cidade="Taubaté", estado_uf="")
        assert f.vazia

    def test_uf_normalizada_para_2_letras(self):
        f = montar_frase_busca(cidade="Taubaté", estado_uf="sp ")
        assert f.componentes["uf"] == "SP"
        assert f.texto.endswith("SP")


class TestFraseTipo:
    @pytest.mark.parametrize(
        "tipo_in,tipo_canonico",
        [
            ("Apartamento", "apartamento"),
            ("APARTAMENTO PADRÃO", "apartamento"),
            ("Casa", "casa"),
            ("sobrado", "sobrado"),
            ("LOTE", "terreno"),
            ("terreno", "terreno"),
            ("gleba", "terreno"),
            ("loja", "loja"),
            ("galpão", "galpão"),
        ],
    )
    def test_tipo_canonicalizado(self, tipo_in, tipo_canonico):
        f = montar_frase_busca(
            cidade="Pindamonhangaba", estado_uf="SP", tipo_imovel=tipo_in
        )
        assert tipo_canonico in f.texto
        assert f.componentes["tipo"] == tipo_canonico

    def test_tipo_desconhecido_omitido(self):
        f = montar_frase_busca(
            cidade="Pindamonhangaba", estado_uf="SP", tipo_imovel="xpto blabla"
        )
        assert "xpto" not in f.texto
        assert "tipo" not in f.componentes


class TestFraseAreaPlausivel:
    def test_area_plausivel_incluida(self):
        f = montar_frase_busca(
            cidade="Taubaté", estado_uf="SP", tipo_imovel="apartamento", area_m2=68.0
        )
        assert "68 m²" in f.texto
        assert f.componentes["area_m2"] == "68"

    def test_area_arredondada(self):
        f = montar_frase_busca(cidade="Taubaté", estado_uf="SP", area_m2=67.6)
        assert "68 m²" in f.texto

    @pytest.mark.parametrize("a", [0, 1, 14, 0.0, 1001, 999999, -10])
    def test_area_implausivel_omitida(self, a):
        f = montar_frase_busca(
            cidade="Taubaté", estado_uf="SP", tipo_imovel="apartamento", area_m2=a
        )
        assert "m²" not in f.texto
        assert "area_m2" not in f.componentes

    def test_area_none_omitida(self):
        f = montar_frase_busca(cidade="Taubaté", estado_uf="SP", area_m2=None)
        assert "m²" not in f.texto


class TestFraseBairro:
    def test_bairro_incluido(self):
        f = montar_frase_busca(
            cidade="Taubaté",
            estado_uf="SP",
            tipo_imovel="apartamento",
            bairro="Vila São José",
        )
        assert "Vila São José" in f.texto
        assert f.componentes["bairro"] == "Vila São José"

    def test_bairro_vazio_omitido(self):
        f = montar_frase_busca(
            cidade="Taubaté", estado_uf="SP", tipo_imovel="apartamento", bairro=""
        )
        assert "bairro" not in f.componentes

    def test_caracteres_especiais_removidos(self):
        f = montar_frase_busca(
            cidade="Taubaté", estado_uf="SP", bairro='"Centro" (zona) [norte]'
        )
        assert '"' not in f.texto
        assert "(" not in f.texto and "[" not in f.texto


class TestFraseFocoNaCidade:
    """A frase termina sempre com 'cidade UF', evitando que motores
    de busca interpretem a frase como uma busca em outro município."""

    def test_termina_com_cidade_uf(self):
        f = montar_frase_busca(
            cidade="Pindamonhangaba",
            estado_uf="SP",
            tipo_imovel="apartamento",
            bairro="Centro",
            area_m2=70,
        )
        assert f.texto.endswith("Pindamonhangaba SP")

    def test_ordem_completa(self):
        """tipo + área + bairro + cidade + UF, nesta ordem (regressão)."""
        f = montar_frase_busca(
            cidade="Pindamonhangaba",
            estado_uf="SP",
            tipo_imovel="apartamento",
            bairro="Centro",
            area_m2=70,
        )
        assert f.texto == "apartamento 70 m² Centro Pindamonhangaba SP"
