"""
Política de expansão progressiva de critérios de busca.

Quando a busca inicial (raio 500m, área ±25%, tipo exato) não devolve
amostras suficientes, relaxamos critérios em ordem **do menos invasivo
para o mais invasivo**:

1. Raio 500 → 1000 → 2000 m (degraus 1 e 2).
2. Tolerância de área 25% → 35% (degrau 3).
3. Permitir tipo aproximado (ex.: casa ↔ sobrado) (degrau 4).

Cada degrau aplicado **rebaixa a confiança** em ``niveis_expansao``,
porque amostras mais distantes/heterogêneas valem menos.

A obtenção de amostras propriamente dita é injetada via callback
``fn_buscar_amostras`` — assim não dependemos do banco aqui e tudo
fica testável.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from leilao_ia_v2.precificacao.dominio import Amostra, ResultadoExpansao


# Tipo da função de busca injetada. Recebe parâmetros de relaxamento e
# devolve amostras já filtradas (raio, área, tipo) — a função é responsável
# por consultar Supabase + Haversine + filtros.
BuscadorAmostras = Callable[..., list[Amostra]]


@dataclass(frozen=True)
class PoliticaExpansao:
    """Configura a busca progressiva por amostras.

    - ``n_minimo_alvo``: tamanho de amostra que satisfaz a busca; assim
      que atinge, parar de expandir.
    - ``raio_inicial_m``: primeiro raio (default 500m, "vizinhança imediata").
    - ``raios_expansao_m``: raios subsequentes a tentar.
    - ``area_relax_pct_inicial``: tolerância de área inicial (0.25 = ±25%).
    - ``area_relax_pct_max``: tolerância máxima após expansão (0.35).
    - ``permitir_tipo_proximo``: se True, último degrau aceita tipos
      aproximados (mapeamento controlado pelo buscador).
    """

    n_minimo_alvo: int = 6
    raio_inicial_m: int = 500
    raios_expansao_m: tuple[int, ...] = (1000, 2000)
    area_relax_pct_inicial: float = 0.25
    area_relax_pct_max: float = 0.35
    permitir_tipo_proximo: bool = True


def coletar_amostras(
    *,
    fn_buscar: BuscadorAmostras,
    politica: PoliticaExpansao = PoliticaExpansao(),
) -> ResultadoExpansao:
    """Aplica os degraus de expansão e devolve a melhor coleção obtida.

    A cada chamada de ``fn_buscar``, passa via kwargs:
        - ``raio_m`` (int)
        - ``area_relax_pct`` (float)
        - ``permitir_tipo_proximo`` (bool)

    Retorna assim que ``len(amostras) >= n_minimo_alvo``, ou — caso nunca
    atinja — devolve a melhor tentativa (a mais ampla testada).
    """
    melhor: list[Amostra] = []
    raio_da_melhor: int = politica.raio_inicial_m
    area_da_melhor: float = politica.area_relax_pct_inicial
    tipo_da_melhor: bool = False
    niveis_aplicados_da_melhor: int = 0

    raios_a_tentar: tuple[int, ...] = (politica.raio_inicial_m, *politica.raios_expansao_m)

    # Degrau 1+2: variar raio mantendo área inicial e tipo exato.
    for idx, raio in enumerate(raios_a_tentar):
        amostras = list(fn_buscar(
            raio_m=raio,
            area_relax_pct=politica.area_relax_pct_inicial,
            permitir_tipo_proximo=False,
        ))
        if len(amostras) > len(melhor):
            melhor = amostras
            raio_da_melhor = raio
            area_da_melhor = politica.area_relax_pct_inicial
            tipo_da_melhor = False
            niveis_aplicados_da_melhor = idx
        if len(amostras) >= politica.n_minimo_alvo:
            return ResultadoExpansao(
                amostras=tuple(amostras),
                raio_final_m=raio,
                area_relax_aplicada=politica.area_relax_pct_inicial,
                tipo_relax_aplicado=False,
                niveis_expansao_aplicados=idx,
            )

    # Degrau 3: maior raio + relaxa área até o máximo.
    raio_max = raios_a_tentar[-1]
    if politica.area_relax_pct_max > politica.area_relax_pct_inicial:
        amostras = list(fn_buscar(
            raio_m=raio_max,
            area_relax_pct=politica.area_relax_pct_max,
            permitir_tipo_proximo=False,
        ))
        nivel_atual = len(politica.raios_expansao_m) + 1
        if len(amostras) > len(melhor):
            melhor = amostras
            raio_da_melhor = raio_max
            area_da_melhor = politica.area_relax_pct_max
            tipo_da_melhor = False
            niveis_aplicados_da_melhor = nivel_atual
        if len(amostras) >= politica.n_minimo_alvo:
            return ResultadoExpansao(
                amostras=tuple(amostras),
                raio_final_m=raio_max,
                area_relax_aplicada=politica.area_relax_pct_max,
                tipo_relax_aplicado=False,
                niveis_expansao_aplicados=nivel_atual,
            )

    # Degrau 4: máximo raio + máxima área + tipo próximo.
    if politica.permitir_tipo_proximo:
        amostras = list(fn_buscar(
            raio_m=raio_max,
            area_relax_pct=politica.area_relax_pct_max,
            permitir_tipo_proximo=True,
        ))
        nivel_atual = len(politica.raios_expansao_m) + 2
        if len(amostras) > len(melhor):
            melhor = amostras
            raio_da_melhor = raio_max
            area_da_melhor = politica.area_relax_pct_max
            tipo_da_melhor = True
            niveis_aplicados_da_melhor = nivel_atual
        if len(amostras) >= politica.n_minimo_alvo:
            return ResultadoExpansao(
                amostras=tuple(amostras),
                raio_final_m=raio_max,
                area_relax_aplicada=politica.area_relax_pct_max,
                tipo_relax_aplicado=True,
                niveis_expansao_aplicados=nivel_atual,
            )

    return ResultadoExpansao(
        amostras=tuple(melhor),
        raio_final_m=raio_da_melhor,
        area_relax_aplicada=area_da_melhor,
        tipo_relax_aplicado=tipo_da_melhor,
        niveis_expansao_aplicados=niveis_aplicados_da_melhor,
    )
