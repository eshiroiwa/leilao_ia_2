"""
Estatística robusta para precificação por comparáveis.

Decisões e justificativas:

- **Mediana, não média**: anúncios têm cauda longa (alguns muito caros);
  a mediana é insensível a esses outliers.
- **CV via MAD**: Coeficiente de Variação = ``MAD / mediana × 100``,
  onde ``MAD = mediana(|x - mediana(x)|)``. MAD é o equivalente robusto
  do desvio-padrão; menos puxado por valores extremos.
- **Descarte boxplot (Tukey)**: pontos fora de
  ``[Q1 - 1.5·IQR, Q3 + 1.5·IQR]`` são removidos. Só ativa para ``n ≥ 4``
  (sob isso, IQR é instável e descartar 1 de 3 mata o conjunto).
- **Percentis com interpolação linear** (mesmo método do ``numpy.percentile``
  default), implementado sem ``numpy`` para não puxar dependência pesada.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ResultadoOutliers:
    valores_dentro: tuple[float, ...]
    n_descartados: int
    limite_inferior: float
    limite_superior: float


def mediana(valores: Iterable[float]) -> float:
    """Mediana clássica (interpola entre os dois centrais quando n par).

    Devolve ``0.0`` se vazio.
    """
    ord_v = sorted(float(v) for v in valores)
    n = len(ord_v)
    if n == 0:
        return 0.0
    meio = n // 2
    if n % 2 == 1:
        return ord_v[meio]
    return (ord_v[meio - 1] + ord_v[meio]) / 2.0


def percentil(valores: Iterable[float], p: float) -> float:
    """Percentil interpolado (método linear).

    Args:
        valores: iterável de números.
        p: percentil em [0, 100].

    Returns:
        Valor do percentil. ``0.0`` se entrada vazia. Se p está fora de
        [0, 100], é clampado.
    """
    ord_v = sorted(float(v) for v in valores)
    n = len(ord_v)
    if n == 0:
        return 0.0
    p = max(0.0, min(100.0, float(p)))
    if n == 1:
        return ord_v[0]
    pos = (p / 100.0) * (n - 1)
    inteiro = int(pos)
    frac = pos - inteiro
    if inteiro + 1 >= n:
        return ord_v[-1]
    return ord_v[inteiro] + frac * (ord_v[inteiro + 1] - ord_v[inteiro])


def iqr(valores: Iterable[float]) -> float:
    """Amplitude interquartil = P75 - P25. ``0.0`` se vazio."""
    lst = list(valores)
    if not lst:
        return 0.0
    return percentil(lst, 75) - percentil(lst, 25)


def mad(valores: Iterable[float]) -> float:
    """Mediana dos desvios absolutos em torno da mediana.

    Robusta contra outliers — ao contrário do desvio-padrão, MAD não é
    inflada por 1-2 pontos extremos.
    """
    lst = [float(v) for v in valores]
    if not lst:
        return 0.0
    m = mediana(lst)
    desvios = [abs(v - m) for v in lst]
    return mediana(desvios)


def cv_robusto_pct(valores: Iterable[float]) -> float:
    """Coeficiente de variação robusto: ``MAD / mediana × 100`` em %.

    Devolve ``0.0`` quando a mediana é zero (não dá para normalizar).
    Cap superior em ``999.0`` para não inflar logs com infinitos.
    """
    lst = [float(v) for v in valores]
    if not lst:
        return 0.0
    m = mediana(lst)
    if m <= 0:
        return 0.0
    return min(999.0, mad(lst) / m * 100.0)


def descartar_outliers_boxplot(
    valores: Iterable[float],
    *,
    n_minimo_para_filtrar: int = 4,
    k: float = 1.5,
) -> ResultadoOutliers:
    """Aplica descarte de Tukey (boxplot).

    Args:
        valores: amostras a filtrar.
        n_minimo_para_filtrar: se a entrada tem menos que isso, **não**
            filtra (com poucos pontos, o IQR é demasiado instável e o
            descarte é mais nocivo que útil).
        k: multiplicador do IQR. Default 1.5 (clássico de Tukey). Use
            3.0 para "outliers extremos" só.

    Returns:
        :class:`ResultadoOutliers` com os valores que passaram + contagem
        descartados + limites efetivos.
    """
    lst = sorted(float(v) for v in valores)
    n = len(lst)
    if n < n_minimo_para_filtrar:
        return ResultadoOutliers(
            valores_dentro=tuple(lst),
            n_descartados=0,
            limite_inferior=lst[0] if lst else 0.0,
            limite_superior=lst[-1] if lst else 0.0,
        )
    q1 = percentil(lst, 25)
    q3 = percentil(lst, 75)
    iqr_v = q3 - q1
    li = q1 - k * iqr_v
    ls = q3 + k * iqr_v
    dentro = tuple(v for v in lst if li <= v <= ls)
    return ResultadoOutliers(
        valores_dentro=dentro,
        n_descartados=n - len(dentro),
        limite_inferior=li,
        limite_superior=ls,
    )
