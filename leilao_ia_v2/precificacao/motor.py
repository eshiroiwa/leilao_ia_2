"""
Motor de precificação — orquestrador puro.

Recebe:
- :class:`ImovelAlvo` (alvo do leilão);
- callback ``fn_buscar_amostras`` que devolve :class:`Amostra` filtradas
  por (raio, tolerância de área, permitir tipo próximo);
- (opcionais) :class:`PoliticaExpansao`, fator de oferta, expoente Heineck.

Devolve um :class:`ResultadoPrecificacao` com tudo necessário para a UI
mostrar valor + faixa + confiança + veredito + alertas. **Não** fala com
banco nem rede; deixa essas responsabilidades ao chamador.
"""

from __future__ import annotations

from typing import Optional

from leilao_ia_v2.precificacao.confianca import avaliar_confianca
from leilao_ia_v2.precificacao.dominio import (
    AmostraHomogeneizada,
    Confianca,
    EstatisticaResumo,
    ImovelAlvo,
    PRECISOES_ALTO_DETALHE,
    ResultadoExpansao,
    ResultadoPrecificacao,
    Veredito,
    VEREDITO_INSUFICIENTE,
)
from leilao_ia_v2.precificacao.estatistica import (
    cv_robusto_pct,
    descartar_outliers_boxplot,
    iqr,
    mediana,
    percentil,
)
from leilao_ia_v2.precificacao.expansao import (
    BuscadorAmostras,
    PoliticaExpansao,
    coletar_amostras,
)
from leilao_ia_v2.precificacao.homogeneizacao import (
    EXPOENTE_HEINECK,
    FATOR_OFERTA_DEFAULT,
    homogeneizar,
)
from leilao_ia_v2.precificacao.liquidez import avaliar_liquidez
from leilao_ia_v2.precificacao.veredito import computar_veredito


def precificar(
    *,
    alvo: ImovelAlvo,
    fn_buscar_amostras: BuscadorAmostras,
    politica: Optional[PoliticaExpansao] = None,
    fator_oferta_valor: float = FATOR_OFERTA_DEFAULT,
    expoente_heineck: float = EXPOENTE_HEINECK,
) -> ResultadoPrecificacao:
    """Executa o pipeline completo: expansão → homogeneização → estatística
    → liquidez → confiança → veredito.

    Args:
        alvo: imóvel do leilão.
        fn_buscar_amostras: callback que sabe consultar o banco e devolver
            amostras filtradas. Deve aceitar ``raio_m``, ``area_relax_pct``,
            ``permitir_tipo_proximo``.
        politica: regras de expansão (default: :class:`PoliticaExpansao`).
        fator_oferta_valor: fator de oferta (default ``0.90``).
        expoente_heineck: expoente Heineck para fator de área (default ``0.125``).

    Returns:
        :class:`ResultadoPrecificacao` sempre — nunca levanta exceção por
        amostras vazias; nesse caso devolve veredito ``INSUFICIENTE``.
    """
    politica = politica or PoliticaExpansao()

    expansao = coletar_amostras(fn_buscar=fn_buscar_amostras, politica=politica)

    homogs: tuple[AmostraHomogeneizada, ...] = tuple(
        homogeneizar(
            a,
            area_alvo=alvo.area_m2,
            fator_oferta_valor=fator_oferta_valor,
            expoente_heineck=expoente_heineck,
        )
        for a in expansao.amostras
    )

    homogs_validos = tuple(h for h in homogs if h.preco_m2_ajustado > 0)

    if len(homogs_validos) < 3:
        return _resultado_insuficiente(alvo=alvo, expansao=expansao, homogs=homogs)

    valores_ajustados = [h.preco_m2_ajustado for h in homogs_validos]

    out = descartar_outliers_boxplot(valores_ajustados)
    valores_uteis = list(out.valores_dentro)
    n_outlier = out.n_descartados

    if len(valores_uteis) < 3:
        return _resultado_insuficiente(alvo=alvo, expansao=expansao, homogs=homogs)

    med = mediana(valores_uteis)
    p20 = percentil(valores_uteis, 20)
    p80 = percentil(valores_uteis, 80)
    iqr_v = iqr(valores_uteis)
    cv = cv_robusto_pct(valores_uteis)

    estat = EstatisticaResumo(
        n_total=len(homogs),
        n_uteis=len(valores_uteis),
        n_descartados_outlier=n_outlier,
        mediana_r_m2=round(med, 2),
        p20_r_m2=round(p20, 2),
        p80_r_m2=round(p80, 2),
        iqr_r_m2=round(iqr_v, 2),
        cv_pct=round(cv, 2),
    )

    areas_amostras = [h.origem.area_m2 for h in homogs_validos if h.origem.area_m2 > 0]
    mediana_area = mediana(areas_amostras) if areas_amostras else 0.0
    alerta_liq = avaliar_liquidez(area_alvo=alvo.area_m2, mediana_area_amostras=mediana_area)

    fracao_alta = (
        sum(1 for h in homogs_validos if h.origem.precisao_geo in PRECISOES_ALTO_DETALHE)
        / len(homogs_validos)
    )
    confianca_base = avaliar_confianca(
        n_uteis=estat.n_uteis,
        cv_pct=estat.cv_pct,
        fracao_precisao_alta=fracao_alta,
    )
    # Expansão também rebaixa: cada degrau aplicado tira um pouco de score.
    confianca = _aplicar_penalidade_expansao(confianca_base, expansao.niveis_expansao_aplicados)

    valor_estimado = med * alvo.area_m2 * alerta_liq.fator_aplicado
    p20_total = p20 * alvo.area_m2 * alerta_liq.fator_aplicado
    p80_total = p80 * alvo.area_m2 * alerta_liq.fator_aplicado

    veredito = computar_veredito(
        lance_minimo=alvo.lance_minimo,
        valor_estimado=valor_estimado,
        p20_total=p20_total,
        p80_total=p80_total,
        confianca=confianca,
        alerta_liquidez=alerta_liq,
    )

    return ResultadoPrecificacao(
        alvo=alvo,
        valor_estimado=round(valor_estimado, 2),
        p20_total=round(p20_total, 2),
        p80_total=round(p80_total, 2),
        estatistica=estat,
        confianca=confianca,
        veredito=veredito,
        alerta_liquidez=alerta_liq,
        expansao=expansao,
        amostras_homogeneizadas=homogs,
    )


def _aplicar_penalidade_expansao(base: Confianca, niveis: int) -> Confianca:
    """Cada degrau de expansão tira ``0.05`` do score, com piso em ``0.0``.

    Não muda o ``nivel`` (continua ALTA/MEDIA/BAIXA conforme N+CV+precisão);
    apenas o ``score`` numérico para ordenação/filtragem fina.
    """
    if niveis <= 0:
        return base
    novo_score = max(0.0, base.score - 0.05 * niveis)
    return Confianca(nivel=base.nivel, motivo=base.motivo, score=round(novo_score, 2))


def _resultado_insuficiente(
    *,
    alvo: ImovelAlvo,
    expansao: ResultadoExpansao,
    homogs: tuple[AmostraHomogeneizada, ...],
) -> ResultadoPrecificacao:
    """Atalho quando não há amostras suficientes para precificar."""
    confianca = Confianca(
        nivel="INSUFICIENTE",
        motivo=f"apenas {len(homogs)} amostra(s) válida(s) — precisa de pelo menos 3",
        score=0.0,
    )
    veredito = Veredito(
        nivel=VEREDITO_INSUFICIENTE,
        descricao="amostras insuficientes para emitir veredito (precisa de N>=3 após filtros)",
    )
    alerta_liq = avaliar_liquidez(area_alvo=alvo.area_m2, mediana_area_amostras=0.0)
    return ResultadoPrecificacao(
        alvo=alvo,
        valor_estimado=None,
        p20_total=None,
        p80_total=None,
        estatistica=None,
        confianca=confianca,
        veredito=veredito,
        alerta_liquidez=alerta_liq,
        expansao=expansao,
        amostras_homogeneizadas=homogs,
    )
