"""
Filtragem e *cap* de cards aprovados **antes** da persistência em Supabase.

Roda **depois** do refino top-N (que já tentou melhorar o geocode dos N
cards mais bem ranqueados). O objetivo aqui é ortogonal ao refino:

- **Filtro de "lixo"**: cards com geocode genérico (centroide de cidade)
  *e* sem rua/bairro inferido têm valor muito baixo para precificação —
  vão todos para o mesmo ponto, distorcem média e poluem o cache.
  São descartados **somente quando já temos amostras melhores no mesmo
  lote** (defesa contra cidades muito pequenas onde o centroide é a
  única opção possível).

- **Cap top-N**: limite duro de cards a persistir por ingestão. Cards
  acima do cap são descartados; preferimos os de maior *score de fit*
  (mesma fórmula usada pelo refino — área + bónus logradouro + outlier
  preço) — porque é com esses que iremos efetivamente precificar.

Funções puras, sem efeitos colaterais — testáveis sem mocks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from leilao_ia_v2.comparaveis.extrator import CardExtraido
from leilao_ia_v2.comparaveis.refino_individual import calcular_score_fit
from leilao_ia_v2.comparaveis.validacao_cidade import (
    PRECISAO_BAIRRO,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    ResultadoValidacaoMunicipio,
)


# Cap default de cards por ingestão. 10 é equilíbrio: suficiente para
# uma estatística inicial (mediana + IQR ficam estáveis em N≥6) sem
# inflar o cache com lixo em ingestões "ricas" (29 cards de uma listagem).
MAX_PERSISTIR_POR_INGESTAO: int = 10

# Quantos cards "melhores" (precisão >= bairro_centroide) precisam existir
# no lote para descartar os de precisão "cidade_centroide" sem detalhe.
# Em cidades muito pequenas onde TUDO é cidade_centroide, este limiar
# evita esvaziar o lote inteiro.
MIN_CARDS_BONS_PARA_DESCARTAR_LIXO: int = 6


# Conjunto de precisões consideradas "boas o suficiente" para que o
# anúncio agregue informação geográfica útil ao cache.
_PRECISOES_BOAS = frozenset({PRECISAO_ROOFTOP, PRECISAO_RUA, PRECISAO_BAIRRO})


@dataclass(frozen=True)
class ResultadoFiltragem:
    """Resultado da etapa de filtragem + cap.

    Campos:
        cards_aprovados: lista final (já filtrada e capada) a persistir.
        n_descartados_lixo: quantos descartados por serem ``cidade_centroide``
            sem rua/bairro inferido (com folga de amostras melhores).
        n_acima_do_cap: quantos descartados por excederem o cap.
        scores: dicionário url → score (para auditoria/log).
    """

    cards_aprovados: list[tuple[CardExtraido, ResultadoValidacaoMunicipio]]
    n_descartados_lixo: int = 0
    n_acima_do_cap: int = 0
    scores: dict[str, float] = field(default_factory=dict)


def _eh_lixo_geo(card: CardExtraido, validacao: ResultadoValidacaoMunicipio) -> bool:
    """Devolve True quando o card é "lixo" geográfico.

    Definição operacional de "lixo":

    - geocode caiu no **centroide da cidade** (precisão fraca), E
    - **não tem logradouro inferido**, E
    - **não tem bairro inferido**.

    Cards assim só geram um ponto-pin no centro da cidade que repete-se
    em todas as ingestões da mesma cidade — não ajudam a precificar
    nada e enviesam médias por bairro.

    Cards com precisão melhor (rua/bairro/rooftop), ou com bairro/
    logradouro identificáveis no anúncio (mesmo que o geocode tenha
    falhado), **não** são considerados lixo: têm informação textual
    útil para precificação por sub-região.
    """
    precisao = (validacao.precisao_geo or "").strip().lower()
    if precisao in _PRECISOES_BOAS:
        return False
    # Aqui precisão é "cidade" (centroide) ou desconhecida.
    tem_logradouro = bool((card.logradouro_inferido or "").strip())
    tem_bairro = bool((card.bairro_inferido or "").strip())
    return not tem_logradouro and not tem_bairro


def filtrar_e_capar(
    cards: list[tuple[CardExtraido, ResultadoValidacaoMunicipio]],
    *,
    area_alvo: Optional[float],
    cap: int = MAX_PERSISTIR_POR_INGESTAO,
    min_cards_bons: int = MIN_CARDS_BONS_PARA_DESCARTAR_LIXO,
) -> ResultadoFiltragem:
    """Aplica filtro de lixo + cap top-N por score de fit.

    Ordem das etapas (intencional):

    1. **Conta cards "bons"** (precisão >= bairro_centroide). Se ≥
       ``min_cards_bons``, ativa o filtro de lixo.
    2. **Filtro de lixo**: descarta ``cidade_centroide`` sem rua/bairro.
    3. **Score de fit** para cada sobrevivente.
    4. **Cap top-N**: ordena por score desc e corta em ``cap``.

    Args:
        cards: lista (card, validacao) que sobreviveu à validação de
            cidade e ao refino.
        area_alvo: área do imóvel do leilão (para o score de fit).
        cap: máximo de cards a persistir.
        min_cards_bons: limiar para ativar o filtro de lixo. Em cidades
            pequenas onde tudo é centroide, **não** filtra (não temos
            alternativa).

    Returns:
        :class:`ResultadoFiltragem`.
    """
    if not cards:
        return ResultadoFiltragem(cards_aprovados=[])

    n_bons = sum(
        1
        for _c, v in cards
        if (v.precisao_geo or "").strip().lower() in _PRECISOES_BOAS
    )

    n_descartados_lixo = 0
    if n_bons >= min_cards_bons:
        sobreviventes: list[tuple[CardExtraido, ResultadoValidacaoMunicipio]] = []
        for c, v in cards:
            if _eh_lixo_geo(c, v):
                n_descartados_lixo += 1
                continue
            sobreviventes.append((c, v))
    else:
        sobreviventes = list(cards)

    if not sobreviventes:
        return ResultadoFiltragem(
            cards_aprovados=[],
            n_descartados_lixo=n_descartados_lixo,
            n_acima_do_cap=0,
            scores={},
        )

    # Mediana de preço/m² do conjunto sobrevivente — usada pelo score
    # para penalizar outliers de preço (ex.: anúncios com preço fora
    # da realidade do segmento). Se não conseguirmos calcular (todos
    # com preço_m2 inválido), o score apenas ignora esse termo.
    valores_preco_m2 = [c.preco_m2 for c, _ in sobreviventes if c.preco_m2 > 0]
    mediana_preco_m2: Optional[float] = None
    if valores_preco_m2:
        ord_v = sorted(valores_preco_m2)
        meio = len(ord_v) // 2
        if len(ord_v) % 2 == 1:
            mediana_preco_m2 = ord_v[meio]
        else:
            mediana_preco_m2 = (ord_v[meio - 1] + ord_v[meio]) / 2.0

    scored: list[tuple[float, CardExtraido, ResultadoValidacaoMunicipio]] = []
    scores_map: dict[str, float] = {}
    for c, v in sobreviventes:
        s = calcular_score_fit(
            c, area_alvo=area_alvo, mediana_preco_m2=mediana_preco_m2
        )
        scored.append((s, c, v))
        scores_map[c.url_anuncio] = round(s, 4)

    # Cap: top-N por score desc.
    scored.sort(key=lambda t: t[0], reverse=True)
    cap_efetivo = max(0, int(cap))
    selecionados = scored[:cap_efetivo]
    n_acima_do_cap = max(0, len(scored) - cap_efetivo)

    aprovados_finais = [(c, v) for _s, c, v in selecionados]
    return ResultadoFiltragem(
        cards_aprovados=aprovados_finais,
        n_descartados_lixo=n_descartados_lixo,
        n_acima_do_cap=n_acima_do_cap,
        scores=scores_map,
    )
