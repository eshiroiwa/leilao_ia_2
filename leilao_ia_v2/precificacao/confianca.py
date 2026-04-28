"""
Avaliação da confiança da estimativa.

Combinação de três sinais ortogonais:

- **N útil** (após descarte de outliers): mais amostras = mais confiança.
- **CV robusto** (MAD/mediana): mais disperso = menos confiança.
- **Fração de precisão alta** (rooftop|rua): amostras com endereço exato
  são mais informativas que centroides de bairro/cidade.

Tabela de níveis (todos os critérios precisam bater):

+--------------+-------+----------+----------------+------+
| nível        | N>=   | CV<=     | fração alta>=  | score|
+--------------+-------+----------+----------------+------+
| ALTA         | 12    | 20%      | 0.50           | 0.95 |
| MEDIA        | 6     | 35%      | 0.30           | 0.65 |
| BAIXA        | 3     | qualquer | qualquer       | 0.35 |
| INSUFICIENTE | <3    | -        | -              | 0.00 |
+--------------+-------+----------+----------------+------+
"""

from __future__ import annotations

from leilao_ia_v2.precificacao.dominio import (
    CONFIANCA_ALTA,
    CONFIANCA_BAIXA,
    CONFIANCA_INSUFICIENTE,
    CONFIANCA_MEDIA,
    Confianca,
)


N_MIN_INSUFICIENTE: int = 3   # < esse valor → INSUFICIENTE
N_MIN_MEDIA: int = 6
N_MIN_ALTA: int = 12

CV_MAX_ALTA_PCT: float = 20.0
CV_MAX_MEDIA_PCT: float = 35.0

FRAC_PRECISAO_MIN_ALTA: float = 0.50
FRAC_PRECISAO_MIN_MEDIA: float = 0.30


def avaliar_confianca(
    *,
    n_uteis: int,
    cv_pct: float,
    fracao_precisao_alta: float,
) -> Confianca:
    """Devolve o nível de confiança a partir dos três sinais.

    Args:
        n_uteis: número de amostras restantes APÓS descarte de outliers.
        cv_pct: coeficiente de variação robusto, em %.
        fracao_precisao_alta: proporção [0,1] das amostras com precisão
            geográfica rooftop ou rua.
    """
    n = int(n_uteis)
    cv = float(cv_pct)
    frac = max(0.0, min(1.0, float(fracao_precisao_alta)))

    if n < N_MIN_INSUFICIENTE:
        return Confianca(
            nivel=CONFIANCA_INSUFICIENTE,
            motivo=f"apenas {n} amostra(s) útil(s) — precisa de pelo menos {N_MIN_INSUFICIENTE}",
            score=0.0,
        )

    if (
        n >= N_MIN_ALTA
        and cv <= CV_MAX_ALTA_PCT
        and frac >= FRAC_PRECISAO_MIN_ALTA
    ):
        return Confianca(
            nivel=CONFIANCA_ALTA,
            motivo=f"n={n}, CV={cv:.0f}%, {int(frac*100)}% com endereço preciso",
            score=0.95,
        )

    if (
        n >= N_MIN_MEDIA
        and cv <= CV_MAX_MEDIA_PCT
        and frac >= FRAC_PRECISAO_MIN_MEDIA
    ):
        return Confianca(
            nivel=CONFIANCA_MEDIA,
            motivo=f"n={n}, CV={cv:.0f}%, {int(frac*100)}% com endereço preciso",
            score=0.65,
        )

    return Confianca(
        nivel=CONFIANCA_BAIXA,
        motivo=f"n={n}, CV={cv:.0f}% — amostras escassas ou dispersas",
        score=0.35,
    )
