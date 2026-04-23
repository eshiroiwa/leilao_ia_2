from leilao_ia_v2.vivareal.tipo_path import SEGMENTOS_TIPO_PATH_VIVAREAL, tipo_imovel_para_segmento_vivareal


def test_tipo_apartamento_residencial():
    assert tipo_imovel_para_segmento_vivareal("apartamento") == "apartamento_residencial"


def test_tipo_casa_comercial_flag():
    assert tipo_imovel_para_segmento_vivareal("loja", uso_comercial=True) == "ponto-comercial_comercial"


def test_segmentos_canonicos_contem_apartamento():
    assert "apartamento_residencial" in SEGMENTOS_TIPO_PATH_VIVAREAL


def test_desconhecido_none():
    assert tipo_imovel_para_segmento_vivareal("desconhecido") is None
    assert tipo_imovel_para_segmento_vivareal("") is None
