"""
Wrapper de Firecrawl Search com orçamento e filtragem de URLs aproveitáveis.

Diferenças face ao módulo antigo (`leilao_ia_v2/fc_search/search_client.py`):

- **Não decide o limit sozinho**: o caller passa explicitamente, e o orçamento
  é validado *antes* da chamada (`OrcamentoFirecrawl.pode_search`). Isto remove
  o caminho silencioso de gastar mais créditos do que o configurado.
- **Conta créditos pelo custo real** (`custo_search` = 2 / 10 results), em vez
  de fixar 1 crédito por chamada como o velho fazia.
- **Filtra URLs aproveitáveis** logo na origem usando
  :func:`comparaveis.extrator.url_eh_anuncio_aproveitavel`, deduplica e ordena
  por relevância (a ordem que o Firecrawl devolve já é boa proxy de relevância).
- **Devolve estrutura imutável** (:class:`ResultadoBusca`) com listas separadas
  para URLs aceites e descartadas — útil para logs e métricas no pipeline.

Esta função é o único ponto onde o pacote `comparaveis` invoca a API Firecrawl
para *search* (scrape vive em :mod:`comparaveis.scrape`).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

from leilao_ia_v2.comparaveis.extrator import url_eh_anuncio_aproveitavel
from leilao_ia_v2.comparaveis.orcamento import (
    OrcamentoExcedido,
    OrcamentoFirecrawl,
    custo_search,
)

logger = logging.getLogger(__name__)


try:
    from firecrawl import Firecrawl
except ImportError:
    Firecrawl = None  # type: ignore[misc, assignment]


_LIMIT_PADRAO = 10
_LIMIT_MAX = 20  # Firecrawl cobra 2 cr / 10 results — limit > 20 raramente vale a pena.


@dataclass(frozen=True)
class ResultadoBusca:
    """Resultado imutável de uma chamada Firecrawl Search.

    Atributos:
        urls_aceites: URLs únicas que passaram pelos filtros (ordem da resposta).
        urls_descartadas: URLs rejeitadas (com motivo no log, não em estrutura).
        custo_creditos: créditos efetivamente consumidos no orçamento.
        executada: ``True`` se a chamada chegou a ser feita; ``False`` se foi
            bloqueada por orçamento ou por argumentos inválidos.
        motivo_nao_executada: explicação textual quando ``executada=False``.
    """

    urls_aceites: tuple[str, ...] = field(default_factory=tuple)
    urls_descartadas: tuple[str, ...] = field(default_factory=tuple)
    custo_creditos: int = 0
    executada: bool = False
    motivo_nao_executada: str = ""

    @property
    def total_resultados(self) -> int:
        return len(self.urls_aceites) + len(self.urls_descartadas)


class FirecrawlSearchIndisponivel(RuntimeError):
    """Levantada quando a SDK não está instalada ou ``FIRECRAWL_API_KEY`` falta."""


def _result_to_dict(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    return {}


def _construir_cliente(api_key: str) -> Any:  # pragma: no cover - exercitado via mocks
    if Firecrawl is None:
        raise FirecrawlSearchIndisponivel(
            "firecrawl-py não está instalado; `pip install firecrawl-py`."
        )
    return Firecrawl(api_key=api_key)


def _normalizar_urls_da_resposta(payload: dict[str, Any]) -> list[str]:
    """Extrai URLs da resposta do Firecrawl Search.

    A SDK pode devolver objetos com `url` ou `link`; normalizamos para um
    string-único e mantemos a ordem original (relevância).
    """
    web = payload.get("web") or []
    if not isinstance(web, list):
        return []
    out: list[str] = []
    for item in web:
        if isinstance(item, dict):
            u = item.get("url") or item.get("link") or ""
        else:
            u = getattr(item, "url", None) or getattr(item, "link", None) or ""
        u = str(u or "").strip()
        if u:
            out.append(u)
    return out


def executar_search(
    query: str,
    *,
    limit: int = _LIMIT_PADRAO,
    orcamento: OrcamentoFirecrawl,
    cliente: Optional[Any] = None,
) -> ResultadoBusca:
    """Executa Firecrawl Search com gates de orçamento e filtro de URLs.

    Args:
        query: frase de busca (gerada por :func:`comparaveis.frase.montar_frase_busca`).
        limit: número máximo de resultados a pedir (clampado a [1, 20]).
        orcamento: contador a ser consultado/atualizado (mutado in-place).
        cliente: opcional — instância já-construída do Firecrawl (para testes).
            Em produção, deixe ``None`` e a função instancia a partir de
            ``FIRECRAWL_API_KEY``.

    Returns:
        :class:`ResultadoBusca`. Quando o orçamento não permite, devolve um
        resultado com ``executada=False`` e ``motivo_nao_executada`` preenchido
        — **nunca** levanta exceção por orçamento (o caller decide se aborta).

    Raises:
        FirecrawlSearchIndisponivel: SDK ausente ou API key não configurada
            (problemas de ambiente, não de orçamento).
    """
    q = (query or "").strip()
    if not q:
        return ResultadoBusca(executada=False, motivo_nao_executada="query_vazia")

    lim = max(1, min(_LIMIT_MAX, int(limit or _LIMIT_PADRAO)))

    if not orcamento.pode_search(limit=lim):
        return ResultadoBusca(
            executada=False,
            motivo_nao_executada=(
                f"orcamento_insuficiente: search com limit={lim} custaria "
                f"{custo_search(lim)} cr, restam {orcamento.restante} de {orcamento.cap}."
            ),
        )

    if cliente is None:
        api_key = (os.getenv("FIRECRAWL_API_KEY") or "").strip()
        if not api_key:
            raise FirecrawlSearchIndisponivel("FIRECRAWL_API_KEY ausente.")
        cliente = _construir_cliente(api_key)

    logger.info("Firecrawl Search: query=%r limit=%s", q[:200], lim)
    try:
        raw = cliente.search(q, limit=lim)
    except OrcamentoExcedido:
        raise
    except Exception as exc:
        logger.warning("Firecrawl Search falhou: %s", exc, exc_info=True)
        return ResultadoBusca(
            executada=False,
            motivo_nao_executada=f"erro_api: {exc.__class__.__name__}",
        )

    custo = orcamento.consumir_search(limit=lim, query=q)
    payload = _result_to_dict(raw)
    urls_brutas = _normalizar_urls_da_resposta(payload)

    aceites: list[str] = []
    descartadas: list[str] = []
    visto: set[str] = set()
    for u in urls_brutas:
        chave = u.split("#", 1)[0]
        if chave in visto:
            continue
        visto.add(chave)
        if url_eh_anuncio_aproveitavel(u):
            aceites.append(u)
        else:
            descartadas.append(u)

    logger.info(
        "Firecrawl Search: %s aceites, %s descartadas (custo=%s cr)",
        len(aceites),
        len(descartadas),
        custo,
    )
    return ResultadoBusca(
        urls_aceites=tuple(aceites),
        urls_descartadas=tuple(descartadas),
        custo_creditos=custo,
        executada=True,
    )
