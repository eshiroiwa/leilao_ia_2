"""
Parcelamento (judicial) e financiamento bancário: amortização Price (parcelas fixas) e SAC.

Parâmetros de referência (mercado BR, ordem de grandeza; editáveis na UI):
- **Judicial (CPC/art. 895)**: comum 25% de entrada, saldo em até 30 prestações; juros/índices
  variam (IPCA-E, 1% a.m. em alguns TJs) — a UI usa juros % ao mês.
- **Financiamento (SFH)**: juros de referência muitas vezes em faixa ~10,5–12% a.a. + TR; entrada
  frequentemente 20% (com possibilidade de 5% com FGTS/regras de edital); prazo típ. até 360 meses.
"""

from __future__ import annotations


def taxa_mensal_de_anual(taxa_anual_pct: float) -> float:
    """Converte juros anual (%) em taxa composta ao mês."""
    ta = max(0.0, float(taxa_anual_pct) or 0.0) / 100.0
    if ta <= 0:
        return 0.0
    return (1.0 + ta) ** (1.0 / 12.0) - 1.0


def pmt_price(principal: float, i_mes: float, n: int) -> float:
    """Prestação (tabela Price), i_mes = taxa ao mês (decimal)."""
    p = max(0.0, float(principal) or 0.0)
    n = max(0, int(n) or 0)
    if p <= 0 or n <= 0:
        return 0.0
    if i_mes and i_mes > 0:
        one = 1.0 + float(i_mes)
        return float(p) * (float(i_mes) * one**n) / (one**n - 1.0)
    return p / n


def saldo_devedor_price_apos_t_parcelas(principal: float, i_mes: float, n: int, t: int) -> float:
    """
    Saldo devedor após `t` meses, sem prepayment (fórmula de amortização francês),
    t limitado a [0, n].
    """
    p = max(0.0, float(principal) or 0.0)
    n = max(0, int(n) or 0)
    t = max(0, min(int(t) or 0, n))
    if p <= 0 or n <= 0:
        return 0.0
    if t >= n:
        return 0.0
    if not i_mes or i_mes <= 0:
        return p * (1.0 - t / n)
    i = float(i_mes)
    pmt_ = pmt_price(p, i, n)
    one = 1.0 + i
    return float(p) * one**t - pmt_ * ((one**t - 1.0) / i)


def total_parcelas_price_acumuladas(principal: float, i_mes: float, n: int, t: int) -> float:
    """Soma de prestações pagas nos primeiros t meses (cada mês 1 prestação)."""
    pmt_ = pmt_price(principal, i_mes, n)
    k = max(0, min(int(t) or 0, int(n) or 0))
    return round(pmt_ * k, 2)


# --- SAC (Sistema de Amortização Constante) ---


def primeira_prestacao_sac(principal: float, i_mes: float, n: int) -> float:
    """
    1.ª prestação no SAC: amortização constante ``P/n`` + juros sobre o saldo inicial ``P·i``.

    (As prestações seguintes diminuem porque o juros incide sobre o saldo decrescente.)
    """
    p = max(0.0, float(principal) or 0.0)
    n = max(1, int(n) or 1)
    if p <= 0:
        return 0.0
    a = p / n
    i = float(i_mes) if (i_mes and i_mes > 0) else 0.0
    return round(a + p * i, 2)


def soma_juros_sac_ate_t(principal: float, i_mes: float, n: int, t: int) -> float:
    """Juros acumulados nos primeiros t meses (SAC)."""
    p = max(0.0, float(principal) or 0.0)
    n = max(1, int(n) or 1)
    t = max(0, int(t) or 0)
    if p <= 0 or t == 0:
        return 0.0
    i = float(i_mes) if (i_mes and i_mes > 0) else 0.0
    a = p / n
    total_j = 0.0
    saldo = p
    for _ in range(min(t, n)):
        j = saldo * i
        total_j += j
        saldo = max(0.0, saldo - a)
    return round(total_j, 2)


def soma_prestacoes_sac_ate_t(principal: float, i_mes: float, n: int, t: int) -> float:
    p = max(0.0, float(principal) or 0.0)
    n = max(1, int(n) or 1)
    t = max(0, int(t) or 0)
    if p <= 0 or t == 0:
        return 0.0
    i = float(i_mes) if (i_mes and i_mes > 0) else 0.0
    a = p / n
    saldo = p
    soma = 0.0
    for _ in range(min(t, n)):
        j = saldo * i
        ptot = a + j
        soma += ptot
        saldo = max(0.0, saldo - a)
    return round(soma, 2)


def saldo_devedor_sac_apos_t(principal: float, n: int, t: int) -> float:
    """Saldos de principal após t amortizações (SAC, sem juros no saldo de principal puro)."""
    p = max(0.0, float(principal) or 0.0)
    n = max(1, int(n) or 1)
    t = max(0, min(int(t) or 0, n))
    a = p / n
    return max(0.0, round(p - a * t, 2))


def principal_ja_pagamento_price_apos_t(principal: float, i_mes: float, n: int, t: int) -> float:
    p = max(0.0, float(principal) or 0.0)
    return max(0.0, p - saldo_devedor_price_apos_t_parcelas(p, i_mes, n, t))


def juros_acumulados_price_ate_t(principal: float, i_mes: float, n: int, t: int) -> float:
    k = max(0, min(int(t) or 0, n))
    pmt_ = pmt_price(principal, i_mes, n)
    pp = principal_ja_pagamento_price_apos_t(principal, i_mes, n, k)
    return max(0.0, round(pmt_ * k - pp, 2))


