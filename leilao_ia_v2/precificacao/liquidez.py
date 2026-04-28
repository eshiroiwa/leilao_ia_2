"""
Avaliação de liquidez por desvio de área.

Premissa: imóveis muito maiores ou muito menores que o padrão do bairro
têm risco de liquidez (mercado mais raso, prazo de venda maior, mais
desconto necessário). Isso justifica:

1. **Desconto sobre o valor estimado** (``fator_aplicado``);
2. **Rebaixamento do veredito** em N níveis na escala FORTE → EVITAR.

A razão usada é ``area_alvo / mediana(area_amostras)``. Faixas calibradas
para mercado residencial brasileiro:

+--------------------+--------+---------+--------+---------------+
| razão              | sev    | fator   | rebaixa | rótulo       |
+--------------------+--------+---------+---------+--------------+
| [0.75, 1.35)       | ok     | 1.00    | 0       | "ok"         |
| [0.55, 0.75)       | media  | 0.92    | 1       | "menor"      |
| [1.35, 1.80)       | media  | 0.92    | 1       | "maior"      |
| [0.00, 0.55)       | alta   | 0.85    | 2       | "muito menor"|
| [1.80, +inf)       | alta   | 0.85    | 2       | "muito maior"|
+--------------------+--------+---------+---------+--------------+
"""

from __future__ import annotations

from dataclasses import dataclass

from leilao_ia_v2.precificacao.dominio import AlertaLiquidez


SEVERIDADE_OK: str = "ok"
SEVERIDADE_MEDIA: str = "media"
SEVERIDADE_ALTA: str = "alta"


@dataclass(frozen=True)
class _RegraLiquidez:
    razao_min: float
    razao_max: float
    severidade: str
    fator: float
    rebaixa: int
    rotulo: str


_REGRAS: tuple[_RegraLiquidez, ...] = (
    _RegraLiquidez(0.75, 1.35, SEVERIDADE_OK, 1.00, 0, "alvo dentro do padrão do bairro"),
    _RegraLiquidez(0.55, 0.75, SEVERIDADE_MEDIA, 0.92, 1, "alvo menor que a maioria das amostras"),
    _RegraLiquidez(1.35, 1.80, SEVERIDADE_MEDIA, 0.92, 1, "alvo maior que a maioria das amostras"),
    _RegraLiquidez(0.0, 0.55, SEVERIDADE_ALTA, 0.85, 2, "alvo muito menor que o padrão (fora do mercado típico)"),
    _RegraLiquidez(1.80, float("inf"), SEVERIDADE_ALTA, 0.85, 2, "alvo muito maior que o padrão (fora do mercado típico)"),
)


def avaliar_liquidez(*, area_alvo: float, mediana_area_amostras: float) -> AlertaLiquidez:
    """Devolve o :class:`AlertaLiquidez` adequado para o par (alvo, amostras).

    Quando dados são insuficientes (qualquer área <= 0), devolve
    severidade ``ok`` neutra — preferimos silêncio a alarme falso.
    """
    if area_alvo <= 0 or mediana_area_amostras <= 0:
        return AlertaLiquidez(
            razao_area=1.0,
            severidade=SEVERIDADE_OK,
            mensagem="dados insuficientes para avaliar liquidez por área",
            fator_aplicado=1.0,
            rebaixa_niveis=0,
        )
    razao = area_alvo / mediana_area_amostras
    for regra in _REGRAS:
        if regra.razao_min <= razao < regra.razao_max:
            return AlertaLiquidez(
                razao_area=round(razao, 3),
                severidade=regra.severidade,
                mensagem=regra.rotulo,
                fator_aplicado=regra.fator,
                rebaixa_niveis=regra.rebaixa,
            )
    return AlertaLiquidez(
        razao_area=round(razao, 3),
        severidade=SEVERIDADE_OK,
        mensagem="razão fora das faixas conhecidas — tratada como neutra",
        fator_aplicado=1.0,
        rebaixa_niveis=0,
    )
