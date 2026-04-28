"""
Tipos de domínio do pacote ``precificacao``.

Todas as estruturas são :class:`dataclass(frozen=True)` para evitar mutação
acidental — o pipeline é "pure-data in, pure-data out".

Não importam Supabase nem Firecrawl; são apenas valores. A obtenção das
amostras (consulta ao banco, filtro geográfico) acontece **fora** deste
pacote, via função injectável passada ao :func:`motor.precificar`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# Constantes públicas — usadas em comparações em vez de strings literais
# para evitar typos.
PRECISAO_ROOFTOP: str = "rooftop"
PRECISAO_RUA: str = "rua"
PRECISAO_BAIRRO: str = "bairro_centroide"
PRECISAO_CIDADE: str = "cidade_centroide"
PRECISAO_DESCONHECIDA: str = "desconhecido"

# Precisões consideradas "de alto detalhe" para o cálculo de fração da
# confiança (rooftop = ponto exato, rua = quadra/segmento — ambas dão
# distância <100m do imóvel real).
PRECISOES_ALTO_DETALHE: frozenset[str] = frozenset({PRECISAO_ROOFTOP, PRECISAO_RUA})

# Veredito final — escala monotônica usada para "rebaixar" por confiança baixa
# ou alerta de liquidez. Listadas do pior para o melhor.
VEREDITO_EVITAR: str = "EVITAR"
VEREDITO_RISCO: str = "RISCO"
VEREDITO_NEUTRA: str = "NEUTRA"
VEREDITO_OPORTUNIDADE: str = "OPORTUNIDADE"
VEREDITO_FORTE: str = "FORTE"
VEREDITO_INSUFICIENTE: str = "INSUFICIENTE"   # quando N útil < 3
VEREDITO_SEM_LANCE: str = "SEM_LANCE"          # quando lance_minimo não veio

ESCALA_VEREDITO: tuple[str, ...] = (
    VEREDITO_EVITAR,
    VEREDITO_RISCO,
    VEREDITO_NEUTRA,
    VEREDITO_OPORTUNIDADE,
    VEREDITO_FORTE,
)

# Confiança
CONFIANCA_ALTA: str = "ALTA"
CONFIANCA_MEDIA: str = "MEDIA"
CONFIANCA_BAIXA: str = "BAIXA"
CONFIANCA_INSUFICIENTE: str = "INSUFICIENTE"


@dataclass(frozen=True)
class ImovelAlvo:
    """Imóvel do leilão a ser precificado.

    ``lance_minimo`` é opcional — sem ele, o motor devolve estimativa de
    valor + faixa, mas o veredito vira ``SEM_LANCE`` (sistema não tem como
    julgar oportunidade sem o lance de referência).
    """

    cidade: str
    estado_uf: str
    bairro: str
    tipo_imovel: str
    area_m2: float
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    lance_minimo: Optional[float] = None


@dataclass(frozen=True)
class Amostra:
    """Anúncio comparável vindo de ``anuncios_mercado``.

    ``raio_origem_m`` indica em qual passo da expansão progressiva esta
    amostra foi capturada (500/1000/2000m). Usado pela auditoria para
    saber se a precificação dependeu de amostras "longe".
    """

    url: str
    valor_anuncio: float
    area_m2: float
    tipo_imovel: str
    distancia_km: float
    precisao_geo: str = PRECISAO_DESCONHECIDA
    raio_origem_m: int = 0

    @property
    def preco_m2(self) -> float:
        return self.valor_anuncio / self.area_m2 if self.area_m2 > 0 else 0.0


@dataclass(frozen=True)
class AmostraHomogeneizada:
    """Amostra com R$/m² ajustado por fatores de homogeneização.

    Mantemos a amostra original para auditoria (a UI pode mostrar tanto
    o preço bruto quanto o ajustado).
    """

    origem: Amostra
    preco_m2_bruto: float
    preco_m2_ajustado: float
    fator_oferta: float
    fator_area: float


@dataclass(frozen=True)
class EstatisticaResumo:
    """Estatística robusta do conjunto homogeneizado.

    - **mediana**: tendência central (resistente a outliers).
    - **p20/p80**: faixa central de 60% — usada para o veredito.
    - **iqr**: amplitude interquartil (Q3 - Q1).
    - **cv_pct**: coeficiente de variação MAD/mediana × 100, em %.
      Mais robusto que stddev/média porque MAD não é puxado por outliers.
    """

    n_total: int
    n_uteis: int
    n_descartados_outlier: int
    mediana_r_m2: float
    p20_r_m2: float
    p80_r_m2: float
    iqr_r_m2: float
    cv_pct: float


@dataclass(frozen=True)
class AlertaLiquidez:
    """Avalia se o imóvel-alvo está fora do padrão de área do bairro.

    Imóveis muito maiores ou menores que a mediana das amostras têm risco
    de liquidez (mais difíceis de vender), o que justifica desconto sobre
    o valor estimado e rebaixamento do veredito.

    - ``severidade``: ``ok`` | ``media`` | ``alta``.
    - ``fator_aplicado``: multiplica o valor estimado (1.0 / 0.92 / 0.85).
    - ``rebaixa_niveis``: 0 / 1 / 2 níveis no veredito.
    """

    razao_area: float          # area_alvo / mediana_area_amostras
    severidade: str
    mensagem: str
    fator_aplicado: float
    rebaixa_niveis: int


@dataclass(frozen=True)
class Confianca:
    """Confiança da estimativa.

    - ``score`` ∈ [0,1] permite ordenar/filtrar precificações.
    - ``motivo`` é uma string curta para a UI ("n=4 amostras úteis, CV=42%").
    """

    nivel: str
    motivo: str
    score: float


@dataclass(frozen=True)
class Veredito:
    """Decisão final: oportunidade ou risco?

    - ``nivel``: FORTE | OPORTUNIDADE | NEUTRA | RISCO | EVITAR | INSUFICIENTE | SEM_LANCE.
    - ``rebaixado``: True se confiança baixa OU alerta de liquidez forçaram a descer.
    - ``desconto_vs_p20_pct``: (1 - lance/P20)·100 — quão abaixo da P20 o lance está.
    - ``descricao``: frase pronta para a UI.
    """

    nivel: str
    descricao: str
    rebaixado: bool = False
    desconto_vs_p20_pct: Optional[float] = None


@dataclass(frozen=True)
class ResultadoExpansao:
    """Resultado da política de expansão progressiva."""

    amostras: tuple[Amostra, ...]
    raio_final_m: int
    area_relax_aplicada: float       # 0.25 inicial → 0.35 expandido
    tipo_relax_aplicado: bool        # ex.: casa ↔ sobrado
    niveis_expansao_aplicados: int   # 0..N — quanto rebaixar a confiança


@dataclass(frozen=True)
class ResultadoPrecificacao:
    """Resultado final do motor de precificação.

    Campos pensados para popular um card na UI sem cálculo extra:

    - ``valor_estimado`` = mediana × área × fator_liquidez.
    - ``faixa = (p20_total, p80_total)`` — intervalo de 60%.
    """

    alvo: ImovelAlvo
    valor_estimado: Optional[float]
    p20_total: Optional[float]
    p80_total: Optional[float]
    estatistica: Optional[EstatisticaResumo]
    confianca: Confianca
    veredito: Veredito
    alerta_liquidez: AlertaLiquidez
    expansao: ResultadoExpansao
    amostras_homogeneizadas: tuple[AmostraHomogeneizada, ...] = field(default_factory=tuple)
