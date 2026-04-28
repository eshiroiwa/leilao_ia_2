"""Testes da construção de UMA frase de busca natural.

A frase usa **plural** + ``"à venda em"`` + bairro/cidade/UF, alinhando-se com
a forma como portais imobiliários (Viva Real, Zap, Chaves na Mão, etc.)
indexam suas páginas de listagem. Não inclui área em m² (restritivo demais).
"""

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
        assert f.texto == "imóveis à venda em Pindamonhangaba SP"
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
        "tipo_in,tipo_singular,plural_no_texto",
        [
            ("Apartamento", "apartamento", "apartamentos"),
            ("APARTAMENTO PADRÃO", "apartamento", "apartamentos"),
            ("Casa", "casa", "casas"),
            ("sobrado", "sobrado", "sobrados"),
            ("LOTE", "terreno", "terrenos"),
            ("terreno", "terreno", "terrenos"),
            ("gleba", "terreno", "terrenos"),
            ("loja", "loja", "lojas"),
            ("galpão", "galpão", "galpões"),
        ],
    )
    def test_tipo_canonicalizado_em_plural(self, tipo_in, tipo_singular, plural_no_texto):
        f = montar_frase_busca(
            cidade="Pindamonhangaba", estado_uf="SP", tipo_imovel=tipo_in
        )
        assert f.texto.startswith(plural_no_texto + " à venda")
        assert f.componentes["tipo"] == tipo_singular

    def test_tipo_desconhecido_usa_imoveis(self):
        f = montar_frase_busca(
            cidade="Pindamonhangaba", estado_uf="SP", tipo_imovel="xpto blabla"
        )
        assert f.texto.startswith("imóveis à venda em")
        assert "xpto" not in f.texto
        assert "tipo" not in f.componentes


class TestFraseSemArea:
    """Área em m² é intencionalmente descartada para favorecer páginas de
    listagem (que tipicamente agregam vários tamanhos)."""

    @pytest.mark.parametrize("area", [50.0, 67.5, 99.99, 200, 1, None])
    def test_area_nunca_no_texto(self, area):
        f = montar_frase_busca(
            cidade="Taubaté",
            estado_uf="SP",
            tipo_imovel="apartamento",
            area_m2=area,
        )
        assert "m²" not in f.texto
        assert "area_m2" not in f.componentes


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
        # Ordem: <plural> à venda em <bairro> <cidade> <UF>
        assert f.texto == "apartamentos à venda em Vila São José Taubaté SP"

    def test_bairro_vazio_omitido(self):
        f = montar_frase_busca(
            cidade="Taubaté", estado_uf="SP", tipo_imovel="apartamento", bairro=""
        )
        assert "bairro" not in f.componentes
        assert f.texto == "apartamentos à venda em Taubaté SP"

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

    def test_ordem_completa_pinda(self):
        """Cenário do bug: tipo plural + 'à venda em' + bairro + cidade + UF."""
        f = montar_frase_busca(
            cidade="Pindamonhangaba",
            estado_uf="SP",
            tipo_imovel="apartamento",
            bairro="Santana",
            area_m2=65,
        )
        assert f.texto == "apartamentos à venda em Santana Pindamonhangaba SP"
