from leilao_ia_v2.vivareal.slug import slug_vivareal
from leilao_ia_v2.vivareal.zonas_rio import inferir_zona_rio_por_bairro, rio_capital_cidade_slug
from leilao_ia_v2.vivareal.zonas_sao_paulo import inferir_zona_sao_paulo_por_bairro, sao_paulo_capital_cidade_slug


def test_slug_vivareal_acentos():
    assert slug_vivareal("São Paulo") == "sao-paulo"
    assert slug_vivareal("  Dr. Lessa  ") == "dr-lessa"


def test_inferir_zona_ipanema():
    assert inferir_zona_rio_por_bairro("Ipanema") == "zona-sul"


def test_inferir_zona_desconhecida():
    assert inferir_zona_rio_por_bairro("") is None
    assert inferir_zona_rio_por_bairro("bairro-que-nao-existe-no-mapa") is None


def test_rio_capital_slug():
    assert rio_capital_cidade_slug() == "rio-de-janeiro"


def test_sp_capital_slug():
    assert sao_paulo_capital_cidade_slug() == "sao-paulo"


def test_inferir_zona_pinheiros():
    assert inferir_zona_sao_paulo_por_bairro("Pinheiros") == "zona-oeste"


def test_inferir_zona_moema():
    assert inferir_zona_sao_paulo_por_bairro("Moema") == "zona-sul"
