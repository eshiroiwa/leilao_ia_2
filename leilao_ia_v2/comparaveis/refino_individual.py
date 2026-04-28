"""
Refino top-N: gasta scrapes individuais para subir a precisão dos comparáveis.

A maioria dos cards extraídos vem de **páginas de listagem** (Viva Real, Zap,
Chaves na Mão), onde os anúncios raramente expõem rua + número. O geocode
desses cards cai em centroide de bairro (boa) ou de cidade (ruim, pile-up).

Este módulo, executado **depois** da validação dos cards, faz:

1. **Score de fit** — para cada card aprovado, calcula um score que
   prioriza: similaridade de área com o leilão, bónus para cards que já
   têm logradouro inferido do título, penalidade para cards com preço
   muito longe da mediana (outliers).
2. **Selecção top-N** — agarra os N cards com melhor score.
3. **Scrape individual** — gasta 1 crédito Firecrawl por card (respeita o
   ``orcamento`` cedido), reusando cache em disco quando possível.
4. **Re-extracção de endereço** — usa
   :func:`extrator.extrair_endereco_anuncio_individual` para extrair
   ``logradouro + bairro`` do markdown do anúncio individual.
5. **Re-geocode** — chama :func:`validacao_cidade.obter_coordenadas_com_cidade`
   com o novo logradouro. Faz **reverse-geocode** na nova coord para
   verificar que o município ainda confere com o alvo.
6. **Política de falha de cidade** (decisão pergunta 3): se a nova coord
   geocodifica para outra cidade, decisão depende do volume:

   - Se ainda restam ``>= min_amostras`` cards aprovados após descartar
     este → DESCARTA o card refinado.
   - Caso contrário → mantém o card mas REVERTE para a coord antiga
     (centroide do bairro/cidade). Em cidades pequenas isto é a única
     forma de não terminar com 0 amostras.

O resultado é uma nova lista de cards que substitui a anterior na
persistência: a precisão geográfica sobe de ``bairro``/``cidade`` para
``rua``/``rooftop`` na maior parte dos casos.

**Custo**: até N créditos Firecrawl por ingestão (N=8 por defeito) — ver
:data:`MAX_REFINO_TOP_N`. O caller (pipeline) só invoca o refino se houver
orçamento sobrando.

API exposta::

    refinar_cards_top_n(
        cards_validados,    # list[tuple[CardExtraido, ResultadoValidacaoMunicipio]]
        leilao,             # LeilaoAlvo (cidade, UF, área de referência)
        orcamento,          # OrcamentoFirecrawl (mutado)
        min_amostras,       # int — limiar para reverter vs descartar
        ...
    ) -> ResultadoRefino
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from leilao_ia_v2.comparaveis.extrator import (
    CardExtraido,
    extrair_endereco_anuncio_individual,
)
from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.scrape import ResultadoScrape, scrape_url
from leilao_ia_v2.comparaveis.validacao_cidade import (
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_DESCONHECIDA,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    ResultadoValidacaoMunicipio,
    obter_coordenadas_com_cidade,
    reverse_municipio,
)

logger = logging.getLogger(__name__)


# Quantos cards (no máximo) entram para refino individual.
# Decisão pergunta 2-C ("agressivo"): até 8 scrapes individuais por ingestão.
MAX_REFINO_TOP_N: int = 8

# Pesos do score de fit (decisão pergunta 1-B):
# - 0.50 — similaridade de área com o leilão.
# - 0.30 — penalidade por outlier de preço (distância à mediana).
# - 0.20 — bónus se o card já tem logradouro inferido do título.
_PESO_AREA = 0.50
_PESO_OUTLIER_PRECO = 0.30
_PESO_BONUS_LOGRADOURO = 0.20

# Precisões consideradas "fracas" — alvo principal do refino. Cards com
# rooftop/rua já são bons; o refino só lhes mexe se sobrar orçamento e
# se forem prioritários por outros critérios.
_PRECISOES_FRACAS: frozenset[str] = frozenset({PRECISAO_BAIRRO, PRECISAO_CIDADE, PRECISAO_DESCONHECIDA, ""})


@dataclass(frozen=True)
class ResultadoRefino:
    """Resultado imutável do refino top-N — substitui a lista original."""

    cards_finais: list[tuple[CardExtraido, ResultadoValidacaoMunicipio]]
    n_refinados: int = 0
    n_descartados_cidade_diferente: int = 0
    n_revertidos: int = 0
    n_scrape_falhou: int = 0
    n_extrai_endereco_vazio: int = 0
    n_geocode_falhou: int = 0
    creditos_gastos: int = 0
    detalhes: list[str] = field(default_factory=list)


def calcular_score_fit(
    card: CardExtraido,
    *,
    area_alvo: Optional[float],
    mediana_preco_m2: Optional[float],
) -> float:
    """Calcula score de fit em [0, 1] para priorizar cards.

    Pesos: 0.50 área + 0.30 outlier preço + 0.20 bónus logradouro.

    Args:
        card: card a pontuar.
        area_alvo: área do leilão. Se ``None`` ou ``<=0``, peso de área vira 0.5
            (neutro) — sem informação para discriminar.
        mediana_preco_m2: mediana do preço/m² do conjunto. Se ``None``,
            outlier vale 0.5 (neutro).

    Returns:
        Float em [0, 1]. Mais alto = melhor candidato a refinar.
    """
    sim_area = _similaridade_area(card.area_m2, area_alvo)
    sim_preco = _similaridade_preco_m2(card.preco_m2, mediana_preco_m2)
    bonus_log = 1.0 if (card.logradouro_inferido or "").strip() else 0.0
    score = (
        _PESO_AREA * sim_area
        + _PESO_OUTLIER_PRECO * sim_preco
        + _PESO_BONUS_LOGRADOURO * bonus_log
    )
    return max(0.0, min(1.0, score))


def _similaridade_area(area_card: float, area_alvo: Optional[float]) -> float:
    """1.0 se card tem exactamente a área alvo; cai linearmente até 0.

    Distância normalizada: ``|area - area_alvo| / area_alvo``. Acima de 1.0
    (i.e. card duas vezes maior/menor) score = 0.
    """
    if not area_alvo or area_alvo <= 0:
        return 0.5
    if area_card <= 0:
        return 0.0
    delta_rel = abs(area_card - area_alvo) / float(area_alvo)
    return max(0.0, 1.0 - delta_rel)


def _similaridade_preco_m2(preco_m2_card: float, mediana: Optional[float]) -> float:
    """Penaliza outliers: 1.0 quando preço/m² igual à mediana, 0 quando >2x ou <0.5x."""
    if not mediana or mediana <= 0:
        return 0.5
    if preco_m2_card <= 0:
        return 0.0
    razao = preco_m2_card / float(mediana)
    if razao <= 0:
        return 0.0
    if razao >= 1.0:
        delta = razao - 1.0
    else:
        delta = (1.0 / razao) - 1.0
    return max(0.0, 1.0 - delta)


def _mediana_preco_m2(cards: list[CardExtraido]) -> Optional[float]:
    valores = [c.preco_m2 for c in cards if c.preco_m2 > 0]
    if not valores:
        return None
    return statistics.median(valores)


def _ranquear_para_refino(
    aprovados: list[tuple[CardExtraido, ResultadoValidacaoMunicipio]],
    *,
    area_alvo: Optional[float],
) -> list[tuple[float, int]]:
    """Devolve [(score, idx)] ordenado por score descendente.

    Cards com precisão já alta (rooftop/rua) recebem um deboost porque
    refiná-los traz pouco valor — preferimos gastar créditos onde a precisão
    é fraca.
    """
    cards = [c for c, _ in aprovados]
    mediana_pm2 = _mediana_preco_m2(cards)

    pontuados: list[tuple[float, int]] = []
    for idx, (card, val) in enumerate(aprovados):
        score = calcular_score_fit(
            card, area_alvo=area_alvo, mediana_preco_m2=mediana_pm2
        )
        precisao = (val.precisao_geo or "").strip().lower()
        if precisao not in _PRECISOES_FRACAS:
            score *= 0.4
        pontuados.append((score, idx))

    pontuados.sort(key=lambda x: -x[0])
    return pontuados


# Tipos das funções injectáveis (testes substituem para evitar rede).
_TipoFnScrape = Callable[..., ResultadoScrape]
_TipoFnExtraiEndereco = Callable[[str], tuple[str, str]]
_TipoFnObterCoords = Callable[..., Optional[tuple[float, float, str]]]
_TipoFnReverse = Callable[[float, float], Optional[str]]


def refinar_cards_top_n(
    cards_validados: list[tuple[CardExtraido, ResultadoValidacaoMunicipio]],
    *,
    cidade_alvo: str,
    estado_uf: str,
    area_alvo: Optional[float],
    orcamento: OrcamentoFirecrawl,
    min_amostras: int,
    cliente_firecrawl: Any = None,
    fn_scrape: _TipoFnScrape = scrape_url,
    fn_extrai_endereco: _TipoFnExtraiEndereco = extrair_endereco_anuncio_individual,
    fn_obter_coords: _TipoFnObterCoords = obter_coordenadas_com_cidade,
    fn_reverse: _TipoFnReverse = reverse_municipio,
    max_top_n: int = MAX_REFINO_TOP_N,
) -> ResultadoRefino:
    """Executa o refino top-N. Devolve a nova lista de cards (substitui a anterior).

    Política de cidade-diferente após re-geocode (decisão pergunta 3):

    - Se sobram ``>= min_amostras`` cards aprovados após descartar este
      → DESCARTA o card refinado.
    - Caso contrário → mantém o card com a coord antiga (REVERTE).

    Args:
        cards_validados: lista de tuplas (card, validacao) já aprovadas.
        cidade_alvo: município do leilão (para re-geocode com cidade).
        estado_uf: UF de 2 letras (consistente).
        area_alvo: área do imóvel do leilão (para o score de fit).
        orcamento: contador Firecrawl (mutado in-place).
        min_amostras: limiar de "muitas amostras" (vem de
            ``BuscaMercadoParametros.min_amostras_cache``).
        cliente_firecrawl: passado a ``fn_scrape`` quando precisa.
        fn_*: hooks injectáveis para teste.
        max_top_n: cap superior de refinos (default ``MAX_REFINO_TOP_N=8``).

    Returns:
        :class:`ResultadoRefino` com a nova lista e métricas detalhadas.
    """
    if not cards_validados:
        return ResultadoRefino(cards_finais=[])

    ranking = _ranquear_para_refino(cards_validados, area_alvo=area_alvo)

    # Lista de saída começa por uma cópia da entrada — vamos substituir
    # individualmente cada card refinado pelo seu sucessor (ou descartar).
    cards_finais: list[tuple[CardExtraido, ResultadoValidacaoMunicipio]] = list(
        cards_validados
    )

    n_refinados = 0
    n_descartados = 0
    n_revertidos = 0
    n_scrape_falhou = 0
    n_extrai_vazio = 0
    n_geocode_falhou = 0
    creditos_inicial = orcamento.gasto
    detalhes: list[str] = []
    indices_descartados: set[int] = set()

    n_planejados = min(max_top_n, len(ranking))
    detalhes.append(
        f"refino: candidatos={len(ranking)} top_n={n_planejados} "
        f"cap={orcamento.cap} restam={orcamento.restante}"
    )

    for score, idx in ranking[:n_planejados]:
        if not orcamento.pode_scrape():
            detalhes.append(f"refino: orçamento esgotado em idx={idx}")
            break

        card_orig, val_orig = cards_finais[idx]
        url = card_orig.url_anuncio

        sc = fn_scrape(url, orcamento=orcamento, cliente=cliente_firecrawl)
        if not sc.teve_sucesso:
            n_scrape_falhou += 1
            detalhes.append(
                f"refino[{idx}]: scrape falhou ({sc.motivo_nao_executado or '?'})"
            )
            continue

        novo_logr, novo_bairro = fn_extrai_endereco(sc.markdown)
        bairro_efetivo = novo_bairro or card_orig.bairro_inferido
        logr_efetivo = novo_logr or card_orig.logradouro_inferido

        if not novo_logr:
            n_extrai_vazio += 1
            detalhes.append(f"refino[{idx}]: extrair_endereco vazio (mantém antigo)")

        coords = fn_obter_coords(
            logradouro=logr_efetivo,
            bairro=bairro_efetivo,
            cidade=cidade_alvo,
            estado_uf=estado_uf,
        )
        if coords is None:
            n_geocode_falhou += 1
            detalhes.append(f"refino[{idx}]: re-geocode falhou (mantém antigo)")
            continue

        nova_lat, nova_lon, nova_precisao = coords[0], coords[1], coords[2]

        # Defesa em profundidade: a nova coord aterra na mesma cidade?
        municipio_real = fn_reverse(nova_lat, nova_lon)
        if municipio_real and _slug_local(municipio_real) != _slug_local(cidade_alvo):
            # Política pergunta 3: descartar se houver folga, reverter se não.
            n_aprovados_restantes = len(cards_finais) - len(indices_descartados) - 1
            if n_aprovados_restantes >= int(min_amostras or 0):
                indices_descartados.add(idx)
                n_descartados += 1
                detalhes.append(
                    f"refino[{idx}]: cidade nova={municipio_real!r} ≠ alvo "
                    f"→ DESCARTA (restantes={n_aprovados_restantes} >= min={min_amostras})"
                )
            else:
                n_revertidos += 1
                detalhes.append(
                    f"refino[{idx}]: cidade nova={municipio_real!r} ≠ alvo "
                    f"→ REVERTE (restantes={n_aprovados_restantes} < min={min_amostras})"
                )
            continue

        # Substitui pelo card refinado (URL imutável; mantém preço e área).
        card_refinado = CardExtraido(
            url_anuncio=card_orig.url_anuncio,
            portal=card_orig.portal,
            valor_venda=card_orig.valor_venda,
            area_m2=card_orig.area_m2,
            titulo=card_orig.titulo,
            logradouro_inferido=logr_efetivo,
            bairro_inferido=bairro_efetivo,
            cidade_no_markdown=card_orig.cidade_no_markdown,
        )
        val_refinada = ResultadoValidacaoMunicipio(
            valido=True,
            motivo=val_orig.motivo + "+refinado",
            municipio_real=val_orig.municipio_real or cidade_alvo,
            coordenadas=(nova_lat, nova_lon),
            municipio_alvo_slug=val_orig.municipio_alvo_slug,
            municipio_real_slug=val_orig.municipio_real_slug or val_orig.municipio_alvo_slug,
            precisao_geo=nova_precisao,
        )
        cards_finais[idx] = (card_refinado, val_refinada)
        n_refinados += 1
        detalhes.append(
            f"refino[{idx}]: ok score={score:.2f} precisao={nova_precisao!r} "
            f"logr={logr_efetivo[:40]!r}"
        )

    if indices_descartados:
        cards_finais = [c for i, c in enumerate(cards_finais) if i not in indices_descartados]

    creditos_gastos = orcamento.gasto - creditos_inicial
    detalhes.append(
        f"refino: refinados={n_refinados} descartados={n_descartados} "
        f"revertidos={n_revertidos} scrape_falhou={n_scrape_falhou} "
        f"creditos_gastos={creditos_gastos}"
    )

    return ResultadoRefino(
        cards_finais=cards_finais,
        n_refinados=n_refinados,
        n_descartados_cidade_diferente=n_descartados,
        n_revertidos=n_revertidos,
        n_scrape_falhou=n_scrape_falhou,
        n_extrai_endereco_vazio=n_extrai_vazio,
        n_geocode_falhou=n_geocode_falhou,
        creditos_gastos=creditos_gastos,
        detalhes=detalhes,
    )


def _slug_local(s: str) -> str:
    """Slug determinístico (sem accent + lowercase + alfanum) para comparar cidades.

    Duplicado leve de :func:`validacao_cidade._slug` para evitar import
    privado entre módulos do mesmo pacote.
    """
    import re
    import unicodedata

    if not s:
        return ""
    base = "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", "", base)
