from unittest.mock import MagicMock

from leilao_ia_v2.constants import TABELA_ANUNCIOS_MERCADO
from leilao_ia_v2.persistence import anuncios_mercado_repo as am


def test_upsert_lote_vazio():
    cli = MagicMock()
    assert am.upsert_lote(cli, []) == 0
    cli.table.assert_not_called()


def test_upsert_lote_uma_linha():
    cli = MagicMock()
    up = MagicMock()
    up.execute.return_value = MagicMock()
    tbl = MagicMock()
    tbl.upsert.return_value = up
    cli.table.return_value = tbl
    n = am.upsert_lote(
        cli,
        [
            {
                "url_anuncio": "https://www.vivareal.com.br/imovel/x",
                "portal": "vivareal.com.br",
                "tipo_imovel": "apartamento",
                "logradouro": "Rua A",
                "bairro": "Moema",
                "cidade": "São Paulo",
                "estado": "SP",
                "area_construida_m2": 80.0,
                "valor_venda": 500_000.0,
                "metadados_json": {"leilao_imovel_id": "abc"},
            }
        ],
    )
    assert n == 1
    cli.table.assert_called_with(TABELA_ANUNCIOS_MERCADO)
