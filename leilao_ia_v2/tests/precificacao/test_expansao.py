"""
Testes da política de expansão progressiva (raio → área → tipo).
"""

from __future__ import annotations

from leilao_ia_v2.precificacao.dominio import Amostra
from leilao_ia_v2.precificacao.expansao import PoliticaExpansao, coletar_amostras


def _amostra(raio: int = 500) -> Amostra:
    return Amostra(
        url=f"https://x.com/{raio}",
        valor_anuncio=400_000,
        area_m2=80,
        tipo_imovel="apartamento",
        distancia_km=raio / 1000,
        precisao_geo="rua",
        raio_origem_m=raio,
    )


class _BuscadorFake:
    """Buscador que devolve N amostras conforme o degrau atual."""

    def __init__(self, devoluções):
        self.devoluções = list(devoluções)
        self.chamadas = []

    def __call__(self, *, raio_m, area_relax_pct, permitir_tipo_proximo):
        self.chamadas.append({
            "raio_m": raio_m,
            "area_relax_pct": area_relax_pct,
            "permitir_tipo_proximo": permitir_tipo_proximo,
        })
        n = self.devoluções.pop(0) if self.devoluções else 0
        return [_amostra(raio_m) for _ in range(n)]


class TestColetarAmostras:
    def test_para_no_primeiro_raio_se_atinge_n_minimo(self):
        # 6 amostras já no raio inicial — para imediatamente.
        f = _BuscadorFake([6, 999, 999, 999, 999])
        r = coletar_amostras(fn_buscar=f, politica=PoliticaExpansao(n_minimo_alvo=6))
        assert len(r.amostras) == 6
        assert r.raio_final_m == 500
        assert r.niveis_expansao_aplicados == 0
        assert r.area_relax_aplicada == 0.25
        assert r.tipo_relax_aplicado is False
        assert len(f.chamadas) == 1

    def test_expande_raio_quando_insuficiente(self):
        # Insuficiente em 500, atinge em 1000.
        f = _BuscadorFake([2, 6, 999, 999])
        r = coletar_amostras(fn_buscar=f, politica=PoliticaExpansao(n_minimo_alvo=6))
        assert len(r.amostras) == 6
        assert r.raio_final_m == 1000
        assert r.niveis_expansao_aplicados == 1
        assert len(f.chamadas) == 2

    def test_expande_area_quando_raio_max_nao_basta(self):
        # 0 em todos os raios; só aparece quando relaxa área.
        f = _BuscadorFake([0, 0, 0, 8, 999])
        r = coletar_amostras(fn_buscar=f, politica=PoliticaExpansao(n_minimo_alvo=6))
        assert len(r.amostras) == 8
        assert r.area_relax_aplicada == 0.35
        assert r.tipo_relax_aplicado is False
        # 3 chamadas de raio + 1 de área relaxada = 4
        assert len(f.chamadas) == 4
        assert r.niveis_expansao_aplicados == 3

    def test_expande_tipo_no_ultimo_degrau(self):
        # Nada nos primeiros 4 degraus; só com tipo próximo.
        f = _BuscadorFake([0, 0, 0, 0, 7])
        r = coletar_amostras(fn_buscar=f, politica=PoliticaExpansao(n_minimo_alvo=6))
        assert len(r.amostras) == 7
        assert r.tipo_relax_aplicado is True
        assert r.area_relax_aplicada == 0.35
        assert r.raio_final_m == 2000
        assert r.niveis_expansao_aplicados == 4

    def test_devolve_melhor_tentativa_quando_nunca_atinge(self):
        # Nunca atinge 6 — devolve a melhor (a maior).
        f = _BuscadorFake([1, 2, 3, 4, 5])
        r = coletar_amostras(fn_buscar=f, politica=PoliticaExpansao(n_minimo_alvo=6))
        assert len(r.amostras) == 5
        # Última chamada (tipo próximo) deu 5 — é a melhor.
        assert r.tipo_relax_aplicado is True
        assert r.niveis_expansao_aplicados == 4

    def test_ordem_dos_kwargs_passados_ao_buscador(self):
        f = _BuscadorFake([0, 0, 0, 0, 0])
        coletar_amostras(fn_buscar=f, politica=PoliticaExpansao())
        # Primeiras 3 chamadas: variação de raio, área inicial, tipo exato.
        assert f.chamadas[0]["raio_m"] == 500
        assert f.chamadas[0]["area_relax_pct"] == 0.25
        assert f.chamadas[0]["permitir_tipo_proximo"] is False
        assert f.chamadas[1]["raio_m"] == 1000
        assert f.chamadas[2]["raio_m"] == 2000
        # 4ª: relaxa área.
        assert f.chamadas[3]["area_relax_pct"] == 0.35
        assert f.chamadas[3]["permitir_tipo_proximo"] is False
        # 5ª: relaxa tipo.
        assert f.chamadas[4]["permitir_tipo_proximo"] is True
