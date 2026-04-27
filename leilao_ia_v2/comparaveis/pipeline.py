"""
Orquestrador end-to-end do pacote `comparaveis`.

Fluxo (cada etapa parametrizada por funções injectáveis para os testes):

1. **Frase** — :func:`comparaveis.frase.montar_frase_busca` produz UMA frase
   focada em ``cidade + UF`` (decisão "1 search por leilão").
2. **Search** — :func:`comparaveis.busca.executar_search` consulta o Firecrawl
   respeitando o orçamento. Devolve URLs já filtradas por aproveitabilidade.
3. **Para cada URL aceite, enquanto houver orçamento**:
   a. **Scrape** — :func:`comparaveis.scrape.scrape_url` (com cache em disco
      gratuito).
   b. **Pré-filtro de página** — :func:`comparaveis.pagina_filtro.avaliar_pagina`
      descarta páginas cujo título/breadcrumb não menciona a cidade-alvo
      (mas registamos esse "desperdício" nas métricas).
   c. **Extração** — :func:`comparaveis.extrator.extrair_cards` produz cards
      crus (sem cidade definitiva).
   d. **Validação dura** — :func:`comparaveis.validacao_cidade.validar_municipio_card`
      geocodifica cada card SEM fornecer cidade e descarta os que não
      pertencem ao município alvo.
4. **Persistência** — :func:`comparaveis.persistencia.persistir_lote` faz
   upsert em ``anuncios_mercado`` com a cidade que veio do reverse-geocode.

Princípios aplicados:

- **Nunca excede o orçamento** (cap 15 cr por chamada, decisão 2-C).
- **Nunca grava cidade do leilão** num card que não foi geocoded para lá.
- **Funções injectáveis** para todos os efeitos colaterais (Firecrawl,
  geocoder, Supabase) — torna o pipeline 100% testável sem rede.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from leilao_ia_v2.comparaveis.busca import (
    ResultadoBusca,
    executar_search,
)
from leilao_ia_v2.comparaveis.extrator import (
    CardExtraido,
    extrair_cards,
)
from leilao_ia_v2.comparaveis.frase import (
    FraseBusca,
    montar_frase_busca,
)
from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.pagina_filtro import (
    ResultadoFiltroPagina,
    StatusPagina,
    avaliar_pagina,
)
from leilao_ia_v2.comparaveis.persistencia import (
    LinhaPersistir,
    montar_linha,
    persistir_lote,
)
from leilao_ia_v2.comparaveis.scrape import (
    ResultadoScrape,
    scrape_url,
)
from leilao_ia_v2.comparaveis.validacao_cidade import (
    ResultadoValidacaoMunicipio,
    validar_municipio_card,
)

logger = logging.getLogger(__name__)


# Tipos das funções injectáveis (sem rede em testes).
_TipoFnSearch = Callable[..., ResultadoBusca]
_TipoFnScrape = Callable[..., ResultadoScrape]
_TipoFnFiltro = Callable[..., ResultadoFiltroPagina]
_TipoFnExtrai = Callable[[str], list[CardExtraido]]
_TipoFnValida = Callable[..., ResultadoValidacaoMunicipio]
_TipoFnPersist = Callable[[Any, list[LinhaPersistir]], int]


@dataclass(frozen=True)
class LeilaoAlvo:
    """Dados mínimos do leilão necessários para a busca de comparáveis."""

    cidade: str
    estado_uf: str
    tipo_imovel: str
    bairro: str = ""
    area_m2: Optional[float] = None


@dataclass(frozen=True)
class EstatisticasPipeline:
    """Métricas serializáveis para logs / UI."""

    frase_busca: str = ""
    urls_busca: int = 0
    urls_aceites_busca: int = 0
    urls_descartadas_busca: int = 0
    paginas_scrapadas: int = 0
    paginas_cache_hit: int = 0
    paginas_filtro_rejeitado: int = 0
    cards_extraidos: int = 0
    cards_aprovados_validacao: int = 0
    cards_descartados_validacao: int = 0
    motivos_descarte_validacao: dict[str, int] = field(default_factory=dict)
    persistidos: int = 0
    creditos_gastos: int = 0
    creditos_cap: int = 0
    abortado: bool = False
    motivo_aborto: str = ""

    def resumo(self) -> dict[str, Any]:
        """Versão serializável (dict) para emitir nos logs / metrics."""
        return {
            "frase_busca": self.frase_busca,
            "urls_busca": self.urls_busca,
            "urls_aceites_busca": self.urls_aceites_busca,
            "urls_descartadas_busca": self.urls_descartadas_busca,
            "paginas_scrapadas": self.paginas_scrapadas,
            "paginas_cache_hit": self.paginas_cache_hit,
            "paginas_filtro_rejeitado": self.paginas_filtro_rejeitado,
            "cards_extraidos": self.cards_extraidos,
            "cards_aprovados_validacao": self.cards_aprovados_validacao,
            "cards_descartados_validacao": self.cards_descartados_validacao,
            "motivos_descarte_validacao": dict(self.motivos_descarte_validacao),
            "persistidos": self.persistidos,
            "creditos_gastos": self.creditos_gastos,
            "creditos_cap": self.creditos_cap,
            "abortado": self.abortado,
            "motivo_aborto": self.motivo_aborto,
        }


@dataclass(frozen=True)
class ResultadoPipeline:
    """Resultado final imutável."""

    leilao: LeilaoAlvo
    linhas_persistidas: tuple[LinhaPersistir, ...]
    estatisticas: EstatisticasPipeline


def executar_pipeline(
    leilao: LeilaoAlvo,
    *,
    orcamento: OrcamentoFirecrawl,
    supabase_client: Any = None,
    cliente_firecrawl: Any = None,
    cidades_conhecidas: Optional[list[str]] = None,
    # Hooks (defaults usam as implementações reais; testes substituem.)
    fn_montar_frase: Callable[..., FraseBusca] = montar_frase_busca,
    fn_search: _TipoFnSearch = executar_search,
    fn_scrape: _TipoFnScrape = scrape_url,
    fn_filtro_pagina: _TipoFnFiltro = avaliar_pagina,
    fn_extrai_cards: _TipoFnExtrai = extrair_cards,
    fn_valida_municipio: _TipoFnValida = validar_municipio_card,
    fn_persistir: _TipoFnPersist = persistir_lote,
    persistir: bool = True,
) -> ResultadoPipeline:
    """Executa o pipeline completo e devolve o resultado + métricas.

    Args:
        leilao: parâmetros do leilão alvo (cidade, UF, tipo, área, bairro).
        orcamento: contador Firecrawl (mutado in-place).
        supabase_client: cliente Supabase para persistência. Se ``None`` E
            ``persistir=True``, levanta ValueError. Em testes use um mock
            ou ``persistir=False`` para apenas dry-run.
        cliente_firecrawl: cliente Firecrawl para search/scrape (None em
            produção; testes substituem para mockar a rede).
        cidades_conhecidas: lista para detectar cidades concorrentes em
            páginas (passada ao filtro de página).
        fn_*: hooks injectáveis — em produção use os defaults; em testes
            substitua para mockar etapa por etapa.
        persistir: se ``False``, calcula tudo mas pula o upsert (dry-run).

    Returns:
        :class:`ResultadoPipeline` com linhas persistidas + estatísticas.
    """
    if persistir and supabase_client is None:
        raise ValueError(
            "supabase_client obrigatório quando persistir=True (use persistir=False para dry-run)."
        )

    motivos_validacao: dict[str, int] = {}

    frase = fn_montar_frase(
        cidade=leilao.cidade,
        estado_uf=leilao.estado_uf,
        tipo_imovel=leilao.tipo_imovel,
        bairro=leilao.bairro,
        area_m2=leilao.area_m2,
    )
    if frase.vazia:
        return _resultado_abortado(
            leilao,
            orcamento,
            motivo="frase_vazia",
            motivos_validacao=motivos_validacao,
        )

    busca = fn_search(
        frase.texto,
        limit=10,
        orcamento=orcamento,
        cliente=cliente_firecrawl,
    )
    if not busca.executada:
        return _resultado_abortado(
            leilao,
            orcamento,
            motivo=f"busca_nao_executada:{busca.motivo_nao_executada}",
            frase=frase.texto,
            motivos_validacao=motivos_validacao,
        )

    estats = {
        "urls_busca": busca.total_resultados,
        "urls_aceites_busca": len(busca.urls_aceites),
        "urls_descartadas_busca": len(busca.urls_descartadas),
        "paginas_scrapadas": 0,
        "paginas_cache_hit": 0,
        "paginas_filtro_rejeitado": 0,
        "cards_extraidos": 0,
        "cards_aprovados_validacao": 0,
        "cards_descartados_validacao": 0,
    }

    linhas_a_persistir: list[LinhaPersistir] = []

    for url in busca.urls_aceites:
        if not orcamento.pode_scrape():
            logger.info(
                "Pipeline: orçamento esgotado antes de scrapear %s — interrompendo loop.",
                url[:80],
            )
            break

        sc = fn_scrape(url, orcamento=orcamento, cliente=cliente_firecrawl)
        if not sc.teve_sucesso:
            continue
        estats["paginas_scrapadas"] += 1
        if sc.fonte == "cache":
            estats["paginas_cache_hit"] += 1

        filtro = fn_filtro_pagina(
            sc.markdown,
            cidade_alvo=leilao.cidade,
            cidades_conhecidas=cidades_conhecidas,
        )
        if filtro.status == StatusPagina.REJEITADA:
            estats["paginas_filtro_rejeitado"] += 1
            logger.info(
                "Pipeline: pagina rejeitada (motivo=%s, concorrentes=%s) %s",
                filtro.motivo,
                ",".join(filtro.cidades_concorrentes) or "-",
                url[:80],
            )
            continue

        cards = fn_extrai_cards(sc.markdown)
        estats["cards_extraidos"] += len(cards)

        for card in cards:
            validacao = fn_valida_municipio(
                logradouro=card.logradouro_inferido,
                bairro=card.bairro_inferido,
                estado_uf=leilao.estado_uf,
                cidade_alvo=leilao.cidade,
            )
            if validacao.deve_descartar:
                estats["cards_descartados_validacao"] += 1
                motivos_validacao[validacao.motivo] = motivos_validacao.get(validacao.motivo, 0) + 1
                continue
            try:
                linha = montar_linha(
                    card,
                    validacao,
                    tipo_imovel=leilao.tipo_imovel,
                    estado_uf=leilao.estado_uf,
                    fonte_busca=frase.texto,
                )
            except Exception:
                logger.exception("Falha a montar linha para card %s — descartando.", card.url_anuncio)
                estats["cards_descartados_validacao"] += 1
                motivos_validacao["erro_montar_linha"] = motivos_validacao.get("erro_montar_linha", 0) + 1
                continue

            linhas_a_persistir.append(linha)
            estats["cards_aprovados_validacao"] += 1

    persistidos = 0
    if persistir and linhas_a_persistir:
        try:
            persistidos = fn_persistir(supabase_client, linhas_a_persistir)
        except Exception:
            logger.exception("Falha a persistir lote de comparáveis.")
            persistidos = 0

    estatisticas = EstatisticasPipeline(
        frase_busca=frase.texto,
        urls_busca=estats["urls_busca"],
        urls_aceites_busca=estats["urls_aceites_busca"],
        urls_descartadas_busca=estats["urls_descartadas_busca"],
        paginas_scrapadas=estats["paginas_scrapadas"],
        paginas_cache_hit=estats["paginas_cache_hit"],
        paginas_filtro_rejeitado=estats["paginas_filtro_rejeitado"],
        cards_extraidos=estats["cards_extraidos"],
        cards_aprovados_validacao=estats["cards_aprovados_validacao"],
        cards_descartados_validacao=estats["cards_descartados_validacao"],
        motivos_descarte_validacao=motivos_validacao,
        persistidos=persistidos,
        creditos_gastos=orcamento.gasto,
        creditos_cap=orcamento.cap,
    )

    return ResultadoPipeline(
        leilao=leilao,
        linhas_persistidas=tuple(linhas_a_persistir),
        estatisticas=estatisticas,
    )


def _resultado_abortado(
    leilao: LeilaoAlvo,
    orcamento: OrcamentoFirecrawl,
    *,
    motivo: str,
    frase: str = "",
    motivos_validacao: Optional[dict[str, int]] = None,
) -> ResultadoPipeline:
    return ResultadoPipeline(
        leilao=leilao,
        linhas_persistidas=(),
        estatisticas=EstatisticasPipeline(
            frase_busca=frase,
            creditos_gastos=orcamento.gasto,
            creditos_cap=orcamento.cap,
            motivos_descarte_validacao=motivos_validacao or {},
            abortado=True,
            motivo_aborto=motivo,
        ),
    )
