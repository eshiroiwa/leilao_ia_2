"""Testes do refino top-N individual.

Cobre:

- ``calcular_score_fit``: pesos do score (área + bónus logradouro + outlier preço).
- ``refinar_cards_top_n``: scrape, re-extracção, re-geocode e política de fallback.
"""

from __future__ import annotations

from leilao_ia_v2.comparaveis.extrator import CardExtraido
from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.refino_individual import (
    MAX_REFINO_TOP_N,
    STATUS_GEOCODE_FALHOU,
    STATUS_OK_PAGINA,
    STATUS_OK_TITULO,
    STATUS_REVERTIDO,
    STATUS_SCRAPE_FALHOU,
    calcular_score_fit,
    refinar_cards_top_n,
)
from leilao_ia_v2.comparaveis.scrape import ResultadoScrape
from leilao_ia_v2.comparaveis.validacao_cidade import (
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    ResultadoValidacaoMunicipio,
)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _card(
    *,
    url="https://portal.com/imovel/x/",
    portal="zapimoveis.com.br",
    area=65.0,
    valor=350_000.0,
    titulo="Apartamento 65m²",
    logradouro_inferido="",
    bairro="Centro",
) -> CardExtraido:
    return CardExtraido(
        url_anuncio=url,
        portal=portal,
        valor_venda=valor,
        area_m2=area,
        titulo=titulo,
        logradouro_inferido=logradouro_inferido,
        bairro_inferido=bairro,
    )


def _val(precisao=PRECISAO_BAIRRO, coords=(-22.92, -45.46), municipio="Pindamonhangaba"):
    return ResultadoValidacaoMunicipio(
        valido=True,
        motivo="ok_texto_local",
        municipio_real=municipio,
        coordenadas=coords,
        municipio_alvo_slug="pindamonhangaba",
        municipio_real_slug="pindamonhangaba",
        precisao_geo=precisao,
    )


def _scrape_ok(url, markdown="md", fonte="firecrawl"):
    return ResultadoScrape(
        url=url,
        markdown=markdown,
        executado=True,
        custo_creditos=1,
        fonte=fonte,
        motivo_nao_executado="",
    )


def _scrape_fail(url, motivo="rede"):
    return ResultadoScrape(
        url=url,
        markdown="",
        executado=False,
        custo_creditos=0,
        fonte="",
        motivo_nao_executado=motivo,
    )


# -----------------------------------------------------------------------------
# calcular_score_fit
# -----------------------------------------------------------------------------

class TestCalcularScoreFit:
    def test_area_igual_alvo_eh_maximo(self):
        c = _card(area=65, valor=350_000, logradouro_inferido="Rua A")
        s = calcular_score_fit(c, area_alvo=65, mediana_preco_m2=350_000 / 65)
        assert s > 0.95  # 0.5*1 + 0.3*1 + 0.2*1 = 1.0

    def test_area_distante_reduz_score(self):
        c1 = _card(area=65, valor=350_000)
        c2 = _card(area=130, valor=700_000)  # área 2x → sim_area = 0
        s1 = calcular_score_fit(c1, area_alvo=65, mediana_preco_m2=5400)
        s2 = calcular_score_fit(c2, area_alvo=65, mediana_preco_m2=5400)
        assert s1 > s2

    def test_logradouro_inferido_da_bonus(self):
        c_sem = _card(area=65, valor=350_000, logradouro_inferido="")
        c_com = _card(area=65, valor=350_000, logradouro_inferido="Rua A")
        m = 350_000 / 65
        assert calcular_score_fit(c_com, area_alvo=65, mediana_preco_m2=m) > calcular_score_fit(
            c_sem, area_alvo=65, mediana_preco_m2=m
        )

    def test_outlier_preco_penaliza(self):
        c_no_meio = _card(area=65, valor=350_000)  # 5384/m²
        c_outlier = _card(area=65, valor=1_400_000)  # ~21500/m² (>>4x mediana)
        m = 5400.0
        s1 = calcular_score_fit(c_no_meio, area_alvo=65, mediana_preco_m2=m)
        s2 = calcular_score_fit(c_outlier, area_alvo=65, mediana_preco_m2=m)
        assert s1 > s2

    def test_score_em_zero_um(self):
        c = _card(area=65, valor=350_000)
        s = calcular_score_fit(c, area_alvo=65, mediana_preco_m2=5400)
        assert 0.0 <= s <= 1.0

    def test_area_alvo_none_eh_neutro(self):
        c = _card(area=65, valor=350_000)
        s = calcular_score_fit(c, area_alvo=None, mediana_preco_m2=None)
        # 0.5*0.5 + 0.3*0.5 + 0.2*0 = 0.4 (sem logradouro inferido)
        assert 0.35 <= s <= 0.45


# -----------------------------------------------------------------------------
# refinar_cards_top_n
# -----------------------------------------------------------------------------

class TestRefinarCardsTopN:
    def test_lista_vazia_devolve_vazio(self):
        orc = OrcamentoFirecrawl(cap=20)
        r = refinar_cards_top_n(
            [],
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
        )
        assert r.cards_finais == []
        assert r.creditos_gastos == 0

    def test_refino_substitui_cards_e_marca_precisao(self):
        orc = OrcamentoFirecrawl(cap=20)
        cards = [
            (_card(url=f"https://portal/{i}/", area=65), _val(PRECISAO_BAIRRO))
            for i in range(3)
        ]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "**Endereço:** Rua das Flores, 100")

        def fake_extrai(md):
            return ("Rua das Flores, 100", "Centro")

        def fake_obter(*, logradouro, bairro, cidade, estado_uf):
            return (-22.93, -45.47, PRECISAO_ROOFTOP)

        def fake_reverse(lat, lon):
            return "Pindamonhangaba"

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=fake_extrai,
            fn_obter_coords=fake_obter,
            fn_reverse=fake_reverse,
        )
        assert r.n_refinados == 3
        assert r.creditos_gastos == 3
        assert all(v.precisao_geo == PRECISAO_ROOFTOP for _, v in r.cards_finais)

    def test_descarta_quando_cidade_diferente_e_muitas_amostras(self):
        orc = OrcamentoFirecrawl(cap=20)
        # 10 cards aprovados; refinar 1 que aterre em outra cidade → descartar.
        cards = [
            (_card(url=f"https://portal/{i}/"), _val(PRECISAO_BAIRRO))
            for i in range(10)
        ]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        # Reverte sempre para outra cidade.
        def fake_reverse(lat, lon):
            return "São Bernardo do Campo"

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X, 1", ""),
            fn_obter_coords=lambda **kw: (-23.69, -46.56, PRECISAO_RUA),
            fn_reverse=fake_reverse,
            max_top_n=8,
        )
        # Todos os 8 refinados aterram em outra cidade. Vamos descartando
        # enquanto ainda houver folga (>= min=4 cards restantes); a partir
        # do 7º a folga acaba e a política reverte (mantém o card antigo).
        # 10 cards, descarta 6 (restantes=4 ainda >=4 no limite), reverte 2.
        assert r.n_descartados_cidade_diferente + r.n_revertidos == 8
        assert r.n_descartados_cidade_diferente >= 5
        assert r.n_revertidos >= 1
        # Saída: 10 - descartados.
        assert len(r.cards_finais) == 10 - r.n_descartados_cidade_diferente

    def test_reverte_quando_cidade_diferente_mas_poucas_amostras(self):
        orc = OrcamentoFirecrawl(cap=20)
        # Apenas 3 cards aprovados (< min=4) → política manda REVERTER.
        cards = [
            (_card(url=f"https://portal/{i}/"), _val(PRECISAO_BAIRRO))
            for i in range(3)
        ]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X, 1", ""),
            fn_obter_coords=lambda **kw: (-23.69, -46.56, PRECISAO_RUA),
            fn_reverse=lambda lat, lon: "São Bernardo do Campo",
            max_top_n=8,
        )
        assert r.n_revertidos > 0
        assert r.n_descartados_cidade_diferente == 0
        # Reverte = mantém todos com coords antigas (centroide bairro).
        assert len(r.cards_finais) == 3
        assert all(v.precisao_geo == PRECISAO_BAIRRO for _, v in r.cards_finais)

    def test_scrape_falhou_nao_quebra(self):
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(), _val(PRECISAO_BAIRRO))]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_fail(url, "fc_error")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("", ""),
            fn_obter_coords=lambda **kw: None,
            fn_reverse=lambda lat, lon: None,
        )
        assert r.n_scrape_falhou == 1
        assert r.n_refinados == 0
        # Card original mantido intacto.
        assert len(r.cards_finais) == 1
        assert r.cards_finais[0][1].precisao_geo == PRECISAO_BAIRRO

    def test_geocode_falhou_mantem_original(self):
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(), _val(PRECISAO_BAIRRO))]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X 1", ""),
            fn_obter_coords=lambda **kw: None,
            fn_reverse=lambda lat, lon: None,
        )
        assert r.n_geocode_falhou == 1
        assert r.cards_finais[0][1].precisao_geo == PRECISAO_BAIRRO

    def test_orcamento_esgotado_para_loop(self):
        orc = OrcamentoFirecrawl(cap=2)  # apenas 2 scrapes possíveis
        cards = [(_card(url=f"https://portal/{i}/"), _val(PRECISAO_BAIRRO)) for i in range(5)]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X 1", ""),
            fn_obter_coords=lambda **kw: (-22.93, -45.47, PRECISAO_RUA),
            fn_reverse=lambda lat, lon: "Pindamonhangaba",
        )
        assert r.creditos_gastos == 2
        assert r.n_refinados == 2

    def test_max_top_n_default_oito(self):
        assert MAX_REFINO_TOP_N == 8

    def test_max_top_n_limita_scrapes(self):
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(url=f"https://portal/{i}/"), _val(PRECISAO_BAIRRO)) for i in range(15)]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X 1", ""),
            fn_obter_coords=lambda **kw: (-22.93, -45.47, PRECISAO_RUA),
            fn_reverse=lambda lat, lon: "Pindamonhangaba",
            max_top_n=5,
        )
        assert r.creditos_gastos == 5
        assert r.n_refinados == 5
        assert len(r.cards_finais) == 15  # outros 10 mantêm-se intactos


# -----------------------------------------------------------------------------
# Marcadores ``refinado_top_n`` / ``refino_status`` propagados aos cards
# (alimentam ``persistencia.montar_linha`` e por sua vez ``metadados_json``).
# -----------------------------------------------------------------------------

class TestMarcadoresRefino:
    def test_card_nao_refinado_fica_intacto_quando_orcamento_zera(self):
        # Apenas 1 scrape possível: dos 5 cards, só 1 entra no refino;
        # os outros 4 devem manter ``refinado_top_n=False``, ``refino_status=""``.
        orc = OrcamentoFirecrawl(cap=1)
        cards = [(_card(url=f"https://portal/{i}/"), _val(PRECISAO_BAIRRO)) for i in range(5)]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X 1", ""),
            fn_obter_coords=lambda **kw: (-22.93, -45.47, PRECISAO_ROOFTOP),
            fn_reverse=lambda lat, lon: "Pindamonhangaba",
        )
        refinados = [c for c, _ in r.cards_finais if c.refinado_top_n]
        nao_refinados = [c for c, _ in r.cards_finais if not c.refinado_top_n]
        assert len(refinados) == 1
        assert len(nao_refinados) == 4
        assert all(c.refino_status == "" for c in nao_refinados)

    def test_sucesso_com_pagina_marca_ok_pagina(self):
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(), _val(PRECISAO_BAIRRO))]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "**Endereço:** Rua Nova, 10")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua Nova, 10", "Centro"),
            fn_obter_coords=lambda **kw: (-22.93, -45.47, PRECISAO_ROOFTOP),
            fn_reverse=lambda lat, lon: "Pindamonhangaba",
        )
        card_final, _ = r.cards_finais[0]
        assert card_final.refinado_top_n is True
        assert card_final.refino_status == STATUS_OK_PAGINA
        assert card_final.logradouro_inferido == "Rua Nova, 10"

    def test_sucesso_sem_pagina_mas_com_titulo_marca_ok_titulo(self):
        # Página individual scraped mas sem rua (extrai_endereco devolve "");
        # o card original tinha logradouro no título → re-geocode usa esse.
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(logradouro_inferido="Rua Antiga 5"), _val(PRECISAO_BAIRRO))]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "(página sem endereço)")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("", ""),
            fn_obter_coords=lambda **kw: (-22.93, -45.47, PRECISAO_RUA),
            fn_reverse=lambda lat, lon: "Pindamonhangaba",
        )
        card_final, _ = r.cards_finais[0]
        assert card_final.refinado_top_n is True
        assert card_final.refino_status == STATUS_OK_TITULO
        assert card_final.logradouro_inferido == "Rua Antiga 5"

    def test_scrape_falhou_marca_card_com_status(self):
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(), _val(PRECISAO_BAIRRO))]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_fail(url, "rede")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("", ""),
            fn_obter_coords=lambda **kw: None,
            fn_reverse=lambda lat, lon: None,
        )
        card_final, _ = r.cards_finais[0]
        assert card_final.refinado_top_n is True
        assert card_final.refino_status == STATUS_SCRAPE_FALHOU

    def test_geocode_falhou_marca_card_com_status(self):
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(), _val(PRECISAO_BAIRRO))]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X 1", ""),
            fn_obter_coords=lambda **kw: None,
            fn_reverse=lambda lat, lon: None,
        )
        card_final, _ = r.cards_finais[0]
        assert card_final.refinado_top_n is True
        assert card_final.refino_status == STATUS_GEOCODE_FALHOU

    def test_revertido_marca_card_com_status(self):
        # Apenas 2 cards (< min=4) → política manda REVERTER quando cidade muda.
        orc = OrcamentoFirecrawl(cap=20)
        cards = [(_card(url=f"https://portal/{i}/"), _val(PRECISAO_BAIRRO)) for i in range(2)]

        def fake_scrape(url, *, orcamento, cliente=None):
            orcamento.consumir_scrape(url=url)
            return _scrape_ok(url, "x")

        r = refinar_cards_top_n(
            cards,
            cidade_alvo="Pindamonhangaba",
            estado_uf="SP",
            area_alvo=65.0,
            orcamento=orc,
            min_amostras=4,
            fn_scrape=fake_scrape,
            fn_extrai_endereco=lambda md: ("Rua X 1", ""),
            fn_obter_coords=lambda **kw: (-23.69, -46.56, PRECISAO_RUA),
            fn_reverse=lambda lat, lon: "São Bernardo do Campo",
        )
        # Todos 2 devem estar marcados como revertidos.
        cartas_marcadas = [c for c, _ in r.cards_finais if c.refino_status == STATUS_REVERTIDO]
        assert len(cartas_marcadas) == 2
        # Coords antigas preservadas (não foi alterada na revalidação).
        assert all(v.precisao_geo == PRECISAO_BAIRRO for _, v in r.cards_finais)
