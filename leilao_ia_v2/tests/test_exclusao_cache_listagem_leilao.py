"""Heurística: excluir do cache anúncios alinhados a lance 1ª/2ª praça + sinais."""

from leilao_ia_v2.services.exclusao_cache_listagem_leilao import (
    anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance,
    filtrar_anuncios_mantendo_apenas_mercado_comparavel,
    lances_praca_para_comparar,
)


def _lei(**kw):
    base = {
        "id": "x",
        "bairro": "centro",
        "area_util": 90.0,
        "valor_lance_1_praca": None,
        "valor_lance_2_praca": None,
    }
    base.update(kw)
    return base


def _ad(**kw):
    base = {
        "titulo": "Apartamento",
        "url_anuncio": "https://vivo.com/imovel",
        "valor_venda": 500_000.0,
        "area_construida_m2": 90.0,
        "bairro": "centro",
        "metadados_json": {},
    }
    base.update(kw)
    return base


def test_lances_somente_1_praca():
    l = _lei(valor_lance_1_praca=100_000.0, valor_lance_2_praca=0.0)
    names = [x[0] for x in lances_praca_para_comparar(l)]
    assert names == ["1_praca"]


def test_lances_1_e_2_praca():
    l = _lei(valor_lance_1_praca=200_000.0, valor_lance_2_praca=150_000.0)
    pairs = lances_praca_para_comparar(l)
    assert [p[0] for p in pairs] == ["1_praca", "2_praca"]


def test_lance_somente_em_leilao_extra_json():
    l = {
        "id": "x",
        "bairro": "Centro",
        "area_util": 80.0,
        "valor_lance_1_praca": None,
        "valor_lance_2_praca": None,
        "leilao_extra_json": {"valor_lance_1_praca": 400_000.0},
    }
    pairs = lances_praca_para_comparar(l)
    assert pairs == [("1_praca", 400_000.0)]


def test_somente_valor_arrematacao_eh_referencia_de_preco():
    l = {
        "id": "x",
        "bairro": "CENTRO",
        "area_util": 83.0,
        "cidade": "SAO BERNARDO DO CAMPO",
        "valor_lance_1_praca": None,
        "valor_lance_2_praca": None,
        "valor_arrematacao": 214_089.0,
        "leilao_extra_json": {},
    }
    assert lances_praca_para_comparar(l) == [("arrematacao", 214_089.0)]


def test_exclui_caso_sbc_mesmo_valor_que_valor_arrematacao():
    l = {
        "id": "8e",
        "bairro": "Centro",
        "area_util": 83.0,
        "cidade": "SAO BERNARDO DO CAMPO",
        "valor_lance_1_praca": None,
        "valor_lance_2_praca": None,
        "valor_arrematacao": 214_089.0,
    }
    a = {
        "titulo": "Apartamento para comprar com 83 - 84 m²",
        "url_anuncio": "https://www.vivareal.com.br/imovel/...-id-2882048477/",
        "valor_venda": 214_089.0,
        "area_construida_m2": 83.0,
        "bairro": "CENTRO",
        "cidade": "SAO BERNARDO DO CAMPO",
        "logradouro": "Rua Noêmia Rossi Roquetti",
        "metadados_json": {},
    }
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is True


def test_exclui_centro_so_no_titulo_bairro_field_errado():
    """Bairro 'Centro' do edital aparece no título; campo bairro do portal vem vazio ou outro."""
    l = _lei(
        valor_lance_1_praca=250_000.0,
        bairro="Centro",
        area_util=80.0,
        cidade="São Bernardo do Campo",
    )
    a = _ad(
        valor_venda=250_000.0,
        bairro="",
        cidade="Sao Bernardo do Campo",
        area_construida_m2=80.0,
        titulo="Apartamento 2 dorms no centro",
        url_anuncio="https://zapimoveis.com/x",
    )
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is True


def test_nao_exclui_sem_sinais_mesmo_preco_igual_1_praca():
    """Preço = 1ª praça mas bairro/área diferentes do edital — sem palavras de leilão → mantém no cache."""
    l = _lei(valor_lance_1_praca=500_000.0, bairro="Moinhos de Vento", area_util=90.0)
    a = _ad(
        titulo="Apartamento à venda",
        url_anuncio="https://zapimoveis.com/venda",
        valor_venda=500_000.0,
        bairro="Cidade Baixa",
        area_construida_m2=110.0,
    )
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is False


def test_exclui_preco_1_praca_mais_titulo_leilao():
    l = _lei(valor_lance_1_praca=500_000.0, valor_lance_2_praca=None)
    a = _ad(titulo="Leilão judicial — oportunidade", valor_venda=500_000.0)
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is True


def test_exclui_preco_2_praca_somente_2a():
    l = _lei(valor_lance_1_praca=None, valor_lance_2_praca=300_000.0)
    a = _ad(
        url_anuncio="https://exemplo.com/processo-judicial-imovel",
        valor_venda=300_000.0,
    )
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is True


def test_exclui_preco_igual_lance_mesmo_bairro_sem_palavra_leilao():
    """Ficha de portal com o mesmo R$ do lance e bairro do edital, sem título de leilão — exclui."""
    l = _lei(valor_lance_1_praca=500_000.0, bairro="Centro", area_util=90.0)
    a = _ad(
        valor_venda=500_000.0,
        bairro="Centro",
        area_construida_m2=70.0,
        titulo="Apartamento 3 quartos",
        url_anuncio="https://vivareal.com/x",
    )
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is True


def test_exclui_bairro_mais_area_bem_cercado():
    l = _lei(
        bairro="Higienópolis",
        area_util=88.0,
        valor_lance_1_praca=600_000.0,
    )
    a = _ad(
        bairro="Higienópolis",
        area_construida_m2=88.0,
        valor_venda=600_000.0,
        titulo="Apartamento 3q",
    )
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is True


def test_filtro_lista_mantem_quando_leilao_none():
    ads = [_ad()]
    out = filtrar_anuncios_mantendo_apenas_mercado_comparavel(ads, None)
    assert out == ads


def test_incluir_em_cache_false_exclui_sempre():
    l = _lei(valor_lance_1_praca=1.0)
    a = _ad(
        metadados_json={"incluir_em_cache": False},
        titulo="qualquer",
        valor_venda=9_999_999.0,
    )
    assert anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, l) is True
