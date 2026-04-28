"""
Testes do adapter ``buscador_supabase`` com client Supabase mockado.

Cobre:

- Construção da query (cidade, UF, tipo, faixa de área).
- Mapeamento de tipos próximos (casa↔sobrado).
- Filtro Haversine no segundo passo.
- Comportamento quando alvo sem coordenadas.
- Configuração de ``incluir_sem_geo``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from leilao_ia_v2.precificacao.dominio import ImovelAlvo
from leilao_ia_v2.precificacao.integracao.buscador_supabase import (
    BuscaSupabaseConfig,
    construir_buscador,
)


# -----------------------------------------------------------------------------
# Mock chain do Supabase: client.table().select().eq().eq().ilike()...
# -----------------------------------------------------------------------------

class _Chain:
    """Captura todas as chamadas encadeadas e devolve self até execute()."""

    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows
        self.captured: dict[str, Any] = {}

    def select(self, _cols, *_a, **_kw):
        return self

    def eq(self, col, val):
        self.captured.setdefault("eq", []).append((col, val))
        return self

    def in_(self, col, vals):
        self.captured["in_"] = (col, list(vals))
        return self

    def ilike(self, col, val):
        self.captured["ilike"] = (col, val)
        return self

    def gte(self, col, val):
        self.captured["gte"] = (col, val)
        return self

    def lte(self, col, val):
        self.captured["lte"] = (col, val)
        return self

    def limit(self, n):
        self.captured["limit"] = n
        return self

    def execute(self):
        resp = MagicMock()
        resp.data = self.rows
        return resp


def _mk_client(rows: list[dict[str, Any]]) -> tuple[Any, _Chain]:
    chain = _Chain(rows)
    client = MagicMock()
    client.table.return_value = chain
    return client, chain


def _row(*, lat, lon, valor=300_000, area=60, tipo="apartamento", cidade="Pinda"):
    return {
        "url_anuncio": f"https://x.com/{lat}/{lon}",
        "tipo_imovel": tipo,
        "cidade": cidade,
        "estado": "SP",
        "bairro": "Centro",
        "area_construida_m2": area,
        "valor_venda": valor,
        "latitude": lat,
        "longitude": lon,
        "metadados_json": {"precisao_geo": "rua"},
    }


def _alvo(*, lat=-22.9, lon=-45.4, area=60.0, tipo="apartamento"):
    return ImovelAlvo(
        cidade="Pinda", estado_uf="SP", bairro="Centro", tipo_imovel=tipo,
        area_m2=area, latitude=lat, longitude=lon,
    )


class TestQueryStructure:
    def test_filtros_basicos_aplicados_no_supabase(self):
        client, chain = _mk_client([])
        buscar = construir_buscador(client=client, alvo=_alvo())
        buscar(raio_m=500, area_relax_pct=0.25, permitir_tipo_proximo=False)
        eqs = dict(chain.captured.get("eq", []))
        assert eqs.get("estado") == "SP"
        assert eqs.get("transacao") == "venda"
        assert eqs.get("tipo_imovel") == "apartamento"  # tipo único → eq, não in_
        assert chain.captured["ilike"] == ("cidade", "%Pinda%")
        # área 60 ± 25% → [45, 75]
        assert chain.captured["gte"] == ("area_construida_m2", 45.0)
        assert chain.captured["lte"] == ("area_construida_m2", 75.0)

    def test_tipo_proximo_para_casa_usa_in(self):
        client, chain = _mk_client([])
        buscar = construir_buscador(client=client, alvo=_alvo(tipo="casa"))
        buscar(raio_m=2000, area_relax_pct=0.35, permitir_tipo_proximo=True)
        col, vals = chain.captured["in_"]
        assert col == "tipo_imovel"
        assert set(vals) == {"casa", "sobrado", "casa_condominio"}

    def test_tipo_proximo_para_apartamento_mantem_apartamento(self):
        client, chain = _mk_client([])
        buscar = construir_buscador(client=client, alvo=_alvo(tipo="apartamento"))
        buscar(raio_m=500, area_relax_pct=0.25, permitir_tipo_proximo=True)
        # Apartamento não está em _TIPOS_PROXIMOS → fica como tipo único → eq
        eqs = dict(chain.captured.get("eq", []))
        assert eqs.get("tipo_imovel") == "apartamento"

    def test_area_relax_pct_aumenta_a_faixa(self):
        client, chain = _mk_client([])
        buscar = construir_buscador(client=client, alvo=_alvo(area=100))
        buscar(raio_m=500, area_relax_pct=0.50, permitir_tipo_proximo=False)
        assert chain.captured["gte"] == ("area_construida_m2", 50.0)
        assert chain.captured["lte"] == ("area_construida_m2", 150.0)

    def test_alvo_sem_area_nao_consulta(self):
        client, chain = _mk_client([])
        alvo_sem_area = ImovelAlvo(
            cidade="X", estado_uf="SP", bairro="", tipo_imovel="casa", area_m2=0,
        )
        buscar = construir_buscador(client=client, alvo=alvo_sem_area)
        out = buscar(raio_m=500, area_relax_pct=0.25, permitir_tipo_proximo=False)
        assert out == []
        client.table.assert_not_called()


class TestFiltroHaversine:
    def test_descarta_amostras_fora_do_raio(self):
        # Alvo: (-22.9, -45.4). 0.01° lat ≈ 1.1km
        rows = [
            _row(lat=-22.9, lon=-45.4),       # 0 km
            _row(lat=-22.91, lon=-45.4),      # ~1.1 km
            _row(lat=-22.95, lon=-45.4),      # ~5.5 km
        ]
        client, _ = _mk_client(rows)
        buscar = construir_buscador(client=client, alvo=_alvo())
        # Raio 2km — só os dois primeiros entram.
        out = buscar(raio_m=2000, area_relax_pct=0.25, permitir_tipo_proximo=False)
        assert len(out) == 2
        assert all(a.distancia_km <= 2.0 for a in out)

    def test_amostras_sem_lat_lon_descartadas_por_default(self):
        rows = [_row(lat=-22.9, lon=-45.4), _row(lat=None, lon=None)]
        client, _ = _mk_client(rows)
        buscar = construir_buscador(client=client, alvo=_alvo())
        out = buscar(raio_m=2000, area_relax_pct=0.25, permitir_tipo_proximo=False)
        assert len(out) == 1

    def test_incluir_sem_geo_aceita_amostras_sem_coords(self):
        rows = [_row(lat=-22.9, lon=-45.4), _row(lat=None, lon=None)]
        client, _ = _mk_client(rows)
        buscar = construir_buscador(
            client=client, alvo=_alvo(),
            config=BuscaSupabaseConfig(incluir_sem_geo=True),
        )
        out = buscar(raio_m=2000, area_relax_pct=0.25, permitir_tipo_proximo=False)
        assert len(out) == 2

    def test_alvo_sem_coordenadas_devolve_tudo_sem_filtrar(self):
        rows = [
            _row(lat=-22.9, lon=-45.4),
            _row(lat=-30.0, lon=-50.0),  # longe — mas devolveria mesmo assim
        ]
        client, _ = _mk_client(rows)
        alvo_sem_geo = ImovelAlvo(
            cidade="Pinda", estado_uf="SP", bairro="", tipo_imovel="apartamento",
            area_m2=60, latitude=None, longitude=None,
        )
        buscar = construir_buscador(client=client, alvo=alvo_sem_geo)
        out = buscar(raio_m=500, area_relax_pct=0.25, permitir_tipo_proximo=False)
        # Mesmo o "longe" entra pois não há referência espacial.
        assert len(out) == 2


class TestRobustez:
    def test_query_que_falha_devolve_lista_vazia(self):
        client = MagicMock()
        chain = MagicMock()
        chain.execute.side_effect = RuntimeError("DB down")
        # Devolve self em todos os métodos de query, mas execute explode.
        for m in ("select", "eq", "in_", "ilike", "gte", "lte", "limit"):
            setattr(chain, m, MagicMock(return_value=chain))
        client.table.return_value = chain
        buscar = construir_buscador(client=client, alvo=_alvo())
        out = buscar(raio_m=500, area_relax_pct=0.25, permitir_tipo_proximo=False)
        assert out == []
