"""
Pacote ``precificacao`` — núcleo de avaliação de imóveis de leilão.

Pipeline puro (sem rede/banco):

    coletar_amostras → homogeneizar → estatística robusta → liquidez
    → confiança → veredito

Ponto de entrada: :func:`precificar`. Tipos de domínio em :mod:`.dominio`.

Exemplo mínimo::

    from leilao_ia_v2.precificacao import precificar, ImovelAlvo, Amostra

    def buscar(*, raio_m, area_relax_pct, permitir_tipo_proximo):
        # consulta Supabase aqui (filtra por tipo, área±area_relax_pct, raio)
        ...

    alvo = ImovelAlvo(
        cidade="Pindamonhangaba", estado_uf="SP",
        bairro="Araretama", tipo_imovel="apartamento",
        area_m2=58, latitude=-22.9, longitude=-45.4,
        lance_minimo=145_000,
    )
    resultado = precificar(alvo=alvo, fn_buscar_amostras=buscar)
    print(resultado.valor_estimado, resultado.veredito.nivel)
"""

from leilao_ia_v2.precificacao.dominio import (
    AlertaLiquidez,
    Amostra,
    AmostraHomogeneizada,
    CONFIANCA_ALTA,
    CONFIANCA_BAIXA,
    CONFIANCA_INSUFICIENTE,
    CONFIANCA_MEDIA,
    Confianca,
    ESCALA_VEREDITO,
    EstatisticaResumo,
    ImovelAlvo,
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_DESCONHECIDA,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    PRECISOES_ALTO_DETALHE,
    ResultadoExpansao,
    ResultadoPrecificacao,
    VEREDITO_EVITAR,
    VEREDITO_FORTE,
    VEREDITO_INSUFICIENTE,
    VEREDITO_NEUTRA,
    VEREDITO_OPORTUNIDADE,
    VEREDITO_RISCO,
    VEREDITO_SEM_LANCE,
    Veredito,
)
from leilao_ia_v2.precificacao.expansao import (
    BuscadorAmostras,
    PoliticaExpansao,
    coletar_amostras,
)
from leilao_ia_v2.precificacao.homogeneizacao import (
    EXPOENTE_HEINECK,
    FATOR_OFERTA_DEFAULT,
    fator_area_heineck,
    fator_oferta,
    homogeneizar,
)
from leilao_ia_v2.precificacao.liquidez import avaliar_liquidez
from leilao_ia_v2.precificacao.confianca import avaliar_confianca
from leilao_ia_v2.precificacao.estatistica import (
    cv_robusto_pct,
    descartar_outliers_boxplot,
    iqr,
    mad,
    mediana,
    percentil,
)
from leilao_ia_v2.precificacao.motor import precificar
from leilao_ia_v2.precificacao.veredito import computar_veredito

__all__ = [
    # tipos
    "AlertaLiquidez",
    "Amostra",
    "AmostraHomogeneizada",
    "Confianca",
    "EstatisticaResumo",
    "ImovelAlvo",
    "PoliticaExpansao",
    "ResultadoExpansao",
    "ResultadoPrecificacao",
    "Veredito",
    # constantes
    "BuscadorAmostras",
    "CONFIANCA_ALTA",
    "CONFIANCA_BAIXA",
    "CONFIANCA_INSUFICIENTE",
    "CONFIANCA_MEDIA",
    "ESCALA_VEREDITO",
    "EXPOENTE_HEINECK",
    "FATOR_OFERTA_DEFAULT",
    "PRECISAO_BAIRRO",
    "PRECISAO_CIDADE",
    "PRECISAO_DESCONHECIDA",
    "PRECISAO_ROOFTOP",
    "PRECISAO_RUA",
    "PRECISOES_ALTO_DETALHE",
    "VEREDITO_EVITAR",
    "VEREDITO_FORTE",
    "VEREDITO_INSUFICIENTE",
    "VEREDITO_NEUTRA",
    "VEREDITO_OPORTUNIDADE",
    "VEREDITO_RISCO",
    "VEREDITO_SEM_LANCE",
    # funções
    "avaliar_confianca",
    "avaliar_liquidez",
    "coletar_amostras",
    "computar_veredito",
    "cv_robusto_pct",
    "descartar_outliers_boxplot",
    "fator_area_heineck",
    "fator_oferta",
    "homogeneizar",
    "iqr",
    "mad",
    "mediana",
    "percentil",
    "precificar",
]
