"""
Testes do filtro de "lixo" + cap pré-persistência (Sprint 1 — higiene).

Cobre:

- ``_eh_lixo_geo``: definição operacional de lixo (precisão fraca + sem
  rua + sem bairro inferido).
- ``filtrar_e_capar``: ativação condicional do filtro (cidade pequena
  preserva tudo), cap top-N por score, contadores corretos.
"""

from __future__ import annotations

import pytest

from leilao_ia_v2.comparaveis.extrator import CardExtraido
from leilao_ia_v2.comparaveis.filtragem_persistencia import (
    MAX_PERSISTIR_POR_INGESTAO,
    MIN_CARDS_BONS_PARA_DESCARTAR_LIXO,
    _eh_lixo_geo,
    filtrar_e_capar,
)
from leilao_ia_v2.comparaveis.validacao_cidade import (
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_DESCONHECIDA,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    ResultadoValidacaoMunicipio,
)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _card(
    *,
    url="https://portal.com/x/",
    valor=300_000.0,
    area=70.0,
    logradouro="",
    bairro="",
) -> CardExtraido:
    return CardExtraido(
        url_anuncio=url,
        portal="zapimoveis.com.br",
        valor_venda=valor,
        area_m2=area,
        titulo="Apto",
        logradouro_inferido=logradouro,
        bairro_inferido=bairro,
    )


def _val(precisao: str, coords=(-22.92, -45.46)) -> ResultadoValidacaoMunicipio:
    return ResultadoValidacaoMunicipio(
        valido=True,
        motivo="ok",
        municipio_real="Pindamonhangaba",
        coordenadas=coords,
        municipio_alvo_slug="pindamonhangaba",
        municipio_real_slug="pindamonhangaba",
        precisao_geo=precisao,
    )


# -----------------------------------------------------------------------------
# _eh_lixo_geo
# -----------------------------------------------------------------------------

class TestEhLixoGeo:
    def test_rooftop_nunca_eh_lixo(self):
        assert _eh_lixo_geo(_card(), _val(PRECISAO_ROOFTOP)) is False

    def test_rua_nunca_eh_lixo(self):
        assert _eh_lixo_geo(_card(), _val(PRECISAO_RUA)) is False

    def test_bairro_nunca_eh_lixo(self):
        # bairro_centroide já tem informação geográfica útil.
        assert _eh_lixo_geo(_card(), _val(PRECISAO_BAIRRO)) is False

    def test_cidade_sem_logradouro_nem_bairro_eh_lixo(self):
        c = _card(logradouro="", bairro="")
        assert _eh_lixo_geo(c, _val(PRECISAO_CIDADE)) is True

    def test_cidade_com_bairro_inferido_nao_eh_lixo(self):
        # Tem informação textual de bairro — útil para precificação por
        # sub-região mesmo se geocode falhou.
        c = _card(logradouro="", bairro="Centro")
        assert _eh_lixo_geo(c, _val(PRECISAO_CIDADE)) is False

    def test_cidade_com_logradouro_inferido_nao_eh_lixo(self):
        c = _card(logradouro="Rua A 1", bairro="")
        assert _eh_lixo_geo(c, _val(PRECISAO_CIDADE)) is False

    def test_desconhecida_sem_detalhe_eh_lixo(self):
        c = _card(logradouro="", bairro="")
        assert _eh_lixo_geo(c, _val(PRECISAO_DESCONHECIDA)) is True


# -----------------------------------------------------------------------------
# filtrar_e_capar — filtro de lixo
# -----------------------------------------------------------------------------

class TestFiltroLixo:
    def test_lista_vazia_devolve_zero(self):
        r = filtrar_e_capar([], area_alvo=70.0)
        assert r.cards_aprovados == []
        assert r.n_descartados_lixo == 0
        assert r.n_acima_do_cap == 0

    def test_filtro_ativa_quando_ha_amostras_boas_suficientes(self):
        # 6 bons (rua) + 4 lixo (cidade sem detalhe) → filtro ativa.
        bons = [
            (_card(url=f"https://b/{i}/", logradouro="Rua X 1"), _val(PRECISAO_RUA))
            for i in range(MIN_CARDS_BONS_PARA_DESCARTAR_LIXO)
        ]
        lixo = [
            (_card(url=f"https://l/{i}/"), _val(PRECISAO_CIDADE))
            for i in range(4)
        ]
        r = filtrar_e_capar(bons + lixo, area_alvo=70.0, cap=20)
        assert r.n_descartados_lixo == 4
        assert len(r.cards_aprovados) == MIN_CARDS_BONS_PARA_DESCARTAR_LIXO

    def test_cidade_pequena_preserva_lixo(self):
        # Sem cards bons → cidade pequena, manter o que houver.
        cards = [
            (_card(url=f"https://l/{i}/"), _val(PRECISAO_CIDADE))
            for i in range(5)
        ]
        r = filtrar_e_capar(cards, area_alvo=70.0, cap=20)
        assert r.n_descartados_lixo == 0
        assert len(r.cards_aprovados) == 5

    def test_filtro_desativa_quando_apenas_5_bons(self):
        # 5 bons (< 6 = MIN) + 3 lixo → filtro NÃO ativa, tudo passa.
        bons = [
            (_card(url=f"https://b/{i}/", logradouro="Rua X 1"), _val(PRECISAO_RUA))
            for i in range(MIN_CARDS_BONS_PARA_DESCARTAR_LIXO - 1)
        ]
        lixo = [(_card(url=f"https://l/{i}/"), _val(PRECISAO_CIDADE)) for i in range(3)]
        r = filtrar_e_capar(bons + lixo, area_alvo=70.0, cap=20)
        assert r.n_descartados_lixo == 0
        assert len(r.cards_aprovados) == 5 + 3


# -----------------------------------------------------------------------------
# filtrar_e_capar — cap top-N por score
# -----------------------------------------------------------------------------

class TestCap:
    def test_cap_default_eh_dez(self):
        assert MAX_PERSISTIR_POR_INGESTAO == 10

    def test_cap_corta_excedentes(self):
        # 15 cards iguais; cap=10 → corta 5.
        cards = [
            (
                _card(url=f"https://x/{i}/", area=70.0, logradouro="Rua X 1"),
                _val(PRECISAO_RUA),
            )
            for i in range(15)
        ]
        r = filtrar_e_capar(cards, area_alvo=70.0, cap=10)
        assert len(r.cards_aprovados) == 10
        assert r.n_acima_do_cap == 5

    def test_cap_prioriza_maior_score_de_fit(self):
        # 1 card com area exata + 9 cards com area 4× a alvo (score baixo).
        # Com cap=1, deve sobrar o de area exata.
        alvo_area = 70.0
        c_perfeito = _card(
            url="https://perfeito/",
            area=alvo_area,
            logradouro="Rua X 1",
        )
        ruins = [
            (
                _card(url=f"https://r/{i}/", area=alvo_area * 4, logradouro="Rua Y"),
                _val(PRECISAO_RUA),
            )
            for i in range(9)
        ]
        cards = [(c_perfeito, _val(PRECISAO_RUA))] + ruins
        r = filtrar_e_capar(cards, area_alvo=alvo_area, cap=1)
        assert len(r.cards_aprovados) == 1
        assert r.cards_aprovados[0][0].url_anuncio == "https://perfeito/"
        assert r.n_acima_do_cap == 9

    def test_score_grava_para_cada_card(self):
        cards = [
            (_card(url="https://a/", logradouro="Rua A"), _val(PRECISAO_RUA)),
            (_card(url="https://b/", logradouro="Rua B"), _val(PRECISAO_RUA)),
        ]
        r = filtrar_e_capar(cards, area_alvo=70.0, cap=10)
        assert "https://a/" in r.scores
        assert "https://b/" in r.scores
        assert all(0.0 <= s <= 1.0 for s in r.scores.values())

    def test_cap_zero_descarta_tudo(self):
        cards = [
            (_card(url="https://a/", logradouro="Rua A"), _val(PRECISAO_RUA)),
        ]
        r = filtrar_e_capar(cards, area_alvo=70.0, cap=0)
        assert r.cards_aprovados == []
        assert r.n_acima_do_cap == 1


# -----------------------------------------------------------------------------
# Integração: filtro + cap juntos
# -----------------------------------------------------------------------------

class TestIntegracao:
    def test_cenario_29_cards_apenas_alguns_uteis(self):
        # Cenário inspirado no caso Pinda: 6 bons (rua/bairro) + 23 lixo
        # (cidade sem detalhe). Resultado esperado: descartar 23 lixos,
        # sobrar 6 bons (cap=10 não corta porque sobra menos que 10).
        bons = [
            (
                _card(url=f"https://bom/{i}/", area=70.0 + i, logradouro=f"Rua {i}"),
                _val(PRECISAO_RUA if i % 2 else PRECISAO_BAIRRO),
            )
            for i in range(6)
        ]
        lixo = [
            (_card(url=f"https://lixo/{i}/"), _val(PRECISAO_CIDADE)) for i in range(23)
        ]
        r = filtrar_e_capar(bons + lixo, area_alvo=70.0, cap=10)
        assert r.n_descartados_lixo == 23
        assert r.n_acima_do_cap == 0
        assert len(r.cards_aprovados) == 6

    def test_cenario_29_cards_todos_uteis_corta_no_cap(self):
        # 29 cards com bairro inferido (não são lixo) → cap=10 corta 19.
        cards = [
            (
                _card(
                    url=f"https://x/{i}/",
                    area=70.0 + (i % 5),
                    logradouro=f"Rua {i}",
                    bairro="Centro",
                ),
                _val(PRECISAO_RUA),
            )
            for i in range(29)
        ]
        r = filtrar_e_capar(cards, area_alvo=70.0, cap=10)
        assert r.n_descartados_lixo == 0
        assert r.n_acima_do_cap == 19
        assert len(r.cards_aprovados) == 10
