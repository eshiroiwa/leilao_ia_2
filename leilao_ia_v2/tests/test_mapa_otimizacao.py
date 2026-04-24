from leilao_ia_v2.app_assistente_ingestao import (
    _agrupar_pontos_comparaveis_para_mapa,
    _agrupar_pontos_resumo_para_mapa,
)


def test_agrupar_pontos_comparaveis_mesma_coord_mesma_cor():
    pts = [
        (-22.9, -45.4, "A", "https://a", "#111", "#222"),
        (-22.9, -45.4, "B", "", "#111", "#222"),
    ]
    out = _agrupar_pontos_comparaveis_para_mapa(pts)
    assert len(out) == 1
    _, _, _, url, _, _, n = out[0]
    assert n == 2
    assert url == "https://a"


def test_agrupar_pontos_comparaveis_mesma_coord_cor_diferente_nao_mescla():
    pts = [
        (-22.9, -45.4, "A", "https://a", "#111", "#222"),
        (-22.9, -45.4, "B", "https://b", "#333", "#444"),
    ]
    out = _agrupar_pontos_comparaveis_para_mapa(pts)
    assert len(out) == 2


def test_agrupar_pontos_resumo_por_coordenada():
    rows = [
        ({"id": "1", "bairro": "A"}, -22.91, -45.41),
        ({"id": "2", "bairro": "B"}, -22.91, -45.41),
        ({"id": "3", "bairro": "C"}, -22.92, -45.42),
    ]
    out = _agrupar_pontos_resumo_para_mapa(rows)
    assert len(out) == 2
    ns = sorted([n for _, _, _, n in out], reverse=True)
    assert ns[0] == 2
