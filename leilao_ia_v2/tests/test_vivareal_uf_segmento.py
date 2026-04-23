from leilao_ia_v2.vivareal.uf_segmento import (
    estado_livre_para_sigla_uf,
    estado_para_uf_segmento_vivareal,
    segmentos_uf_urls_listagem_vivareal,
)


def test_estado_para_uf_segmento_sigla():
    assert estado_para_uf_segmento_vivareal("SP") == "sp"
    assert estado_para_uf_segmento_vivareal("rj") == "rj"


def test_estado_para_uf_segmento_nome():
    assert estado_para_uf_segmento_vivareal("Minas Gerais") == "minas-gerais"


def test_estado_livre_para_sigla_uf():
    assert estado_livre_para_sigla_uf("SP") == "SP"
    assert estado_livre_para_sigla_uf("São Paulo") == "SP"
    assert estado_livre_para_sigla_uf("minas-gerais") == "MG"


def test_segmentos_rs():
    assert segmentos_uf_urls_listagem_vivareal("rs") == ["rio-grande-do-sul", "rs"]
    assert segmentos_uf_urls_listagem_vivareal("rio-grande-do-sul") == ["rio-grande-do-sul", "rs"]


def test_segmentos_sp():
    assert segmentos_uf_urls_listagem_vivareal("sp") == ["sp"]
