"""
Wrapper de Firecrawl Scrape com orçamento e cache em disco.

Diferenças em relação ao módulo antigo (`services/firecrawl_edital.py`):

- O orçamento é consultado **antes** da chamada — se o cap não permite, devolve
  resultado com ``executado=False`` e ``motivo_nao_executado`` em vez de
  silenciosamente exceder.
- O cache em disco do módulo `services.disk_cache` continua a ser respeitado
  (o cache é gratuito — *não* consome o orçamento). Quando há hit, devolvemos
  o markdown sem registar consumo.
- A interface é estritamente limitada a "URL → markdown"; nenhum efeito
  colateral de persistência ou parsing acontece aqui (separação de responsabilidades).

Como em :mod:`comparaveis.busca`, o caller pode injetar um ``cliente`` Firecrawl
mockado para testes; em produção, deixe ``None`` e a função instancia a partir
de ``FIRECRAWL_API_KEY``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

from leilao_ia_v2.comparaveis.orcamento import (
    OrcamentoExcedido,
    OrcamentoFirecrawl,
    custo_scrape,
)
from leilao_ia_v2.services import disk_cache

logger = logging.getLogger(__name__)


try:
    from firecrawl import Firecrawl
except ImportError:
    Firecrawl = None  # type: ignore[misc, assignment]


@dataclass(frozen=True)
class ResultadoScrape:
    """Resultado imutável de uma chamada Firecrawl Scrape.

    Atributos:
        url: URL solicitada (igual ao argumento, normalizada com `.strip()`).
        markdown: conteúdo retornado (string vazia se não foi possível scrapear).
        executado: ``True`` se a chamada chegou a ser feita (mesmo se cache hit).
        custo_creditos: créditos consumidos (0 quando cache hit ou não-executado).
        fonte: ``"cache"`` quando veio do disco, ``"firecrawl"`` quando da API,
            ``""`` quando não-executado.
        motivo_nao_executado: explicação textual quando ``executado=False``.
    """

    url: str
    markdown: str = ""
    executado: bool = False
    custo_creditos: int = 0
    fonte: str = ""
    motivo_nao_executado: str = ""

    @property
    def teve_sucesso(self) -> bool:
        return self.executado and bool(self.markdown.strip())


class FirecrawlScrapeIndisponivel(RuntimeError):
    """Levantada quando a SDK não está instalada ou ``FIRECRAWL_API_KEY`` falta."""


def _result_to_dict(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    return {}


def _construir_cliente(api_key: str) -> Any:  # pragma: no cover - exercitado via mocks
    if Firecrawl is None:
        raise FirecrawlScrapeIndisponivel(
            "firecrawl-py não está instalado; `pip install firecrawl-py`."
        )
    return Firecrawl(api_key=api_key)


def scrape_url(
    url: str,
    *,
    orcamento: OrcamentoFirecrawl,
    cliente: Optional[Any] = None,
    ignorar_cache: bool = False,
    gravar_cache: bool = True,
) -> ResultadoScrape:
    """Faz scrape de uma URL respeitando o orçamento Firecrawl.

    Args:
        url: URL a scrapear.
        orcamento: contador a ser consultado e atualizado quando a API for invocada.
        cliente: opcional — instância pré-construída do Firecrawl (testes).
        ignorar_cache: força chamada à API mesmo havendo cache hit (raro;
            útil para revalidar páginas que mudaram).
        gravar_cache: se ``True`` (padrão), grava o markdown obtido no cache em
            disco para reuso futuro gratuito.

    Returns:
        :class:`ResultadoScrape`. **Nunca** levanta exceção por orçamento
        insuficiente — devolve ``executado=False`` para o caller decidir.

    Raises:
        FirecrawlScrapeIndisponivel: SDK ausente ou API key não configurada
            (problema de ambiente, não de orçamento).
    """
    u = (url or "").strip()
    if not u:
        return ResultadoScrape(url="", executado=False, motivo_nao_executado="url_vazia")

    if not ignorar_cache:
        cached = disk_cache.ler_markdown_cache(u)
        if cached is not None and cached.strip():
            logger.info("Scrape cache hit (sem custo): %s", u[:120])
            return ResultadoScrape(
                url=u,
                markdown=cached,
                executado=True,
                custo_creditos=0,
                fonte="cache",
            )

    if not orcamento.pode_scrape():
        return ResultadoScrape(
            url=u,
            executado=False,
            motivo_nao_executado=(
                f"orcamento_insuficiente: scrape custaria {custo_scrape()} cr, "
                f"restam {orcamento.restante} de {orcamento.cap}."
            ),
        )

    if cliente is None:
        api_key = (os.getenv("FIRECRAWL_API_KEY") or "").strip()
        if not api_key:
            raise FirecrawlScrapeIndisponivel("FIRECRAWL_API_KEY ausente.")
        cliente = _construir_cliente(api_key)

    logger.info("Firecrawl Scrape: url=%s", u[:120])
    try:
        raw = cliente.scrape(u, formats=["markdown"])
    except OrcamentoExcedido:
        raise
    except Exception as exc:
        logger.warning("Firecrawl Scrape falhou: %s", exc, exc_info=True)
        return ResultadoScrape(
            url=u,
            executado=False,
            motivo_nao_executado=f"erro_api: {exc.__class__.__name__}",
        )

    custo = orcamento.consumir_scrape(url=u)
    payload = _result_to_dict(raw)
    markdown = str(payload.get("markdown") or "").strip()

    if not markdown:
        logger.warning("Firecrawl Scrape: markdown vazio para %s", u[:120])
        return ResultadoScrape(
            url=u,
            executado=True,
            custo_creditos=custo,
            fonte="firecrawl",
            motivo_nao_executado="markdown_vazio",
        )

    if gravar_cache:
        try:
            disk_cache.gravar_markdown_cache(u, markdown)
        except Exception:
            logger.debug("Falha a gravar cache de scrape (%s)", u[:80], exc_info=True)

    return ResultadoScrape(
        url=u,
        markdown=markdown,
        executado=True,
        custo_creditos=custo,
        fonte="firecrawl",
    )
