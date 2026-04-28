"""
Veredito automático: o lance mínimo do leilão é uma boa oportunidade?

Compara ``lance_minimo`` à faixa estimada (P20–P80) e ao valor central:

+--------------------------------------+----------------+
| condição                             | nível          |
+--------------------------------------+----------------+
| lance <= 0.85 × P20                  | FORTE          |
| lance <= P20                         | OPORTUNIDADE   |
| lance <= valor_estimado              | NEUTRA         |
| lance <= P80                         | RISCO          |
| lance >  P80                         | EVITAR         |
+--------------------------------------+----------------+

O nível **bruto** é então rebaixado por:

- Confiança ``BAIXA``  → -1 nível
- Liquidez ``media``   → -1 nível (já vem em ``alerta.rebaixa_niveis``)
- Liquidez ``alta``    → -2 níveis
- Confiança ``INSUFICIENTE`` → veredito vira ``INSUFICIENTE`` (não compara)

Casos especiais:
- Sem ``lance_minimo`` → ``SEM_LANCE``.
- Sem amostras úteis → ``INSUFICIENTE``.
"""

from __future__ import annotations

from typing import Optional

from leilao_ia_v2.precificacao.dominio import (
    CONFIANCA_BAIXA,
    CONFIANCA_INSUFICIENTE,
    ESCALA_VEREDITO,
    VEREDITO_EVITAR,
    VEREDITO_FORTE,
    VEREDITO_INSUFICIENTE,
    VEREDITO_NEUTRA,
    VEREDITO_OPORTUNIDADE,
    VEREDITO_RISCO,
    VEREDITO_SEM_LANCE,
    AlertaLiquidez,
    Confianca,
    Veredito,
)


# Limite "ganga" — quanto abaixo da P20 conta como FORTE oportunidade.
LIMIAR_FORTE_FRAC_P20: float = 0.85


def _classificar_bruto(
    *,
    lance: float,
    valor_estimado: float,
    p20: float,
    p80: float,
) -> str:
    if lance <= LIMIAR_FORTE_FRAC_P20 * p20:
        return VEREDITO_FORTE
    if lance <= p20:
        return VEREDITO_OPORTUNIDADE
    if lance <= valor_estimado:
        return VEREDITO_NEUTRA
    if lance <= p80:
        return VEREDITO_RISCO
    return VEREDITO_EVITAR


def _rebaixar(nivel: str, n_niveis: int) -> str:
    """Rebaixa ``nivel`` em ``n_niveis`` posições na escala FORTE→EVITAR.

    Idempotente para níveis fora da escala (como SEM_LANCE/INSUFICIENTE).
    """
    if nivel not in ESCALA_VEREDITO or n_niveis <= 0:
        return nivel
    idx_atual = ESCALA_VEREDITO.index(nivel)
    idx_novo = max(0, idx_atual - n_niveis)
    return ESCALA_VEREDITO[idx_novo]


def computar_veredito(
    *,
    lance_minimo: Optional[float],
    valor_estimado: Optional[float],
    p20_total: Optional[float],
    p80_total: Optional[float],
    confianca: Confianca,
    alerta_liquidez: AlertaLiquidez,
) -> Veredito:
    """Devolve o veredito final, já com rebaixamento aplicado."""

    if confianca.nivel == CONFIANCA_INSUFICIENTE:
        return Veredito(
            nivel=VEREDITO_INSUFICIENTE,
            descricao="amostras insuficientes para emitir veredito (precisa de N>=3)",
        )

    if lance_minimo is None or lance_minimo <= 0:
        return Veredito(
            nivel=VEREDITO_SEM_LANCE,
            descricao="lance mínimo não informado — veredito indisponível",
        )

    if not (valor_estimado and p20_total and p80_total):
        return Veredito(
            nivel=VEREDITO_INSUFICIENTE,
            descricao="estimativa de valor inválida — veredito indisponível",
        )

    bruto = _classificar_bruto(
        lance=float(lance_minimo),
        valor_estimado=float(valor_estimado),
        p20=float(p20_total),
        p80=float(p80_total),
    )

    n_rebaixar = alerta_liquidez.rebaixa_niveis
    if confianca.nivel == CONFIANCA_BAIXA:
        n_rebaixar += 1

    final = _rebaixar(bruto, n_rebaixar)
    rebaixado = final != bruto

    desconto_p20 = (
        (1.0 - float(lance_minimo) / float(p20_total)) * 100.0
        if p20_total > 0
        else None
    )
    if desconto_p20 is not None:
        desconto_p20 = round(desconto_p20, 1)

    return Veredito(
        nivel=final,
        descricao=_montar_descricao(final, bruto, n_rebaixar, desconto_p20),
        rebaixado=rebaixado,
        desconto_vs_p20_pct=desconto_p20,
    )


def _montar_descricao(
    final: str,
    bruto: str,
    n_rebaixar: int,
    desconto_p20: Optional[float],
) -> str:
    if final == VEREDITO_FORTE:
        return (
            f"Lance significativamente abaixo da faixa esperada ({desconto_p20}% abaixo de P20)."
            " Forte indício de oportunidade."
        )
    if final == VEREDITO_OPORTUNIDADE:
        return (
            f"Lance abaixo de P20 ({desconto_p20}% de margem). Boa oportunidade,"
            " sujeita às condições do imóvel."
        )
    if final == VEREDITO_NEUTRA:
        return "Lance dentro da faixa central esperada — preço de mercado."
    if final == VEREDITO_RISCO:
        return "Lance acima do valor estimado, ainda dentro de P80 — margem apertada."
    if final == VEREDITO_EVITAR:
        return "Lance acima de P80 — provável sobrepreço para o padrão da região."
    return final
