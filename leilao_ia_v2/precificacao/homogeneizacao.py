"""
Homogeneização de amostras (ajustes pré-estatística).

Converte cada amostra em **R$/m² ajustado** aplicando fatores que
neutralizam vieses conhecidos:

1. **Fator de oferta** (NBR 14653-2, 9.6) — anúncios são preços PEDIDOS,
   tipicamente 5-15% acima da venda real. Default ``0.90``.

2. **Fator de área (Heineck)** — imóveis menores tendem a ter R$/m² maior.
   Fórmula clássica: ``F = (A_amostra / A_alvo) ** k``, com ``k=0.125``
   para residenciais (intervalo típico em literatura: 0.10–0.25). Sem
   este ajuste, amostras de área diferente da alvo produzem viés
   sistemático na estimativa.

   Verificação rápida da direção do fator:
   - amostra=50m² (R$/m²=4000), alvo=100m²: ``F = 0.5^0.125 ≈ 0.917``,
     ajustado = 3670 (alvo maior → R$/m² menor — correto).
   - amostra=200m² (R$/m²=3000), alvo=100m²: ``F = 2^0.125 ≈ 1.090``,
     ajustado = 3270 (alvo menor → R$/m² maior — correto).

Ambas as funções são puras (sem efeitos colaterais).
"""

from __future__ import annotations

from leilao_ia_v2.precificacao.dominio import Amostra, AmostraHomogeneizada


# Default conservador. Pode ser parametrizado por cidade/tipo no futuro.
FATOR_OFERTA_DEFAULT: float = 0.90

# Expoente Heineck para ajuste de R$/m² em função da área.
# 0.125 = ~12.5% de variação por dobramento de área. Conservador.
EXPOENTE_HEINECK: float = 0.125


def fator_oferta(default: float = FATOR_OFERTA_DEFAULT) -> float:
    """Devolve o fator de oferta a aplicar (sempre <= 1.0).

    Função trivial mas existente para que evoluções futuras (variar por
    cidade/tipo/portal) tenham um único ponto de mudança.
    """
    return float(default)


def fator_area_heineck(
    *,
    area_amostra: float,
    area_alvo: float,
    expoente: float = EXPOENTE_HEINECK,
) -> float:
    """Devolve o multiplicador de R$/m² da amostra para chegar ao R$/m² do alvo.

    Args:
        area_amostra: área da amostra observada (m²).
        area_alvo: área do imóvel a precificar (m²).
        expoente: ``k`` na fórmula ``(A_amostra/A_alvo)^k``. Default 0.125.

    Returns:
        Multiplicador adimensional. ``1.0`` quando alguma área é não-positiva
        (sem informação para corrigir; fica neutro).
    """
    if area_amostra <= 0 or area_alvo <= 0:
        return 1.0
    return (float(area_amostra) / float(area_alvo)) ** float(expoente)


def homogeneizar(
    amostra: Amostra,
    *,
    area_alvo: float,
    fator_oferta_valor: float = FATOR_OFERTA_DEFAULT,
    expoente_heineck: float = EXPOENTE_HEINECK,
) -> AmostraHomogeneizada:
    """Aplica fator oferta + fator área a uma amostra individual.

    Devolve sempre uma :class:`AmostraHomogeneizada` (nunca ``None``);
    quando a amostra tem dados inválidos (área ≤ 0 ou valor ≤ 0), o
    ``preco_m2_ajustado`` fica em ``0.0`` e os fatores em ``1.0`` —
    o motor descarta esses casos na estatística.
    """
    bruto = amostra.preco_m2
    if bruto <= 0 or area_alvo <= 0:
        return AmostraHomogeneizada(
            origem=amostra,
            preco_m2_bruto=bruto,
            preco_m2_ajustado=0.0,
            fator_oferta=1.0,
            fator_area=1.0,
        )
    f_oferta = fator_oferta(fator_oferta_valor)
    f_area = fator_area_heineck(
        area_amostra=amostra.area_m2,
        area_alvo=area_alvo,
        expoente=expoente_heineck,
    )
    ajustado = bruto * f_oferta * f_area
    return AmostraHomogeneizada(
        origem=amostra,
        preco_m2_bruto=round(bruto, 2),
        preco_m2_ajustado=round(ajustado, 2),
        fator_oferta=round(f_oferta, 4),
        fator_area=round(f_area, 4),
    )
