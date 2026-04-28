"""
Contador determinístico de créditos Firecrawl para o pipeline de comparáveis.

A documentação do Firecrawl cobra:

- ``search``: **2 créditos por bloco de 10 resultados** (``limit=10`` → 2;
  ``limit=11`` → 4 porque arredonda para o próximo múltiplo de 10).
- ``scrape``: **1 crédito por chamada** (independente do tamanho da página).

Este módulo expõe uma API explícita que **bloqueia** chamadas que ultrapassem o
cap configurado, em vez de apenas avisar depois — defesa em profundidade contra
gasto descontrolado de créditos.

Uso típico::

    o = OrcamentoFirecrawl(cap=15)
    if o.pode_search(limit=10):
        web, _ = client.search(query, limit=10)
        o.consumir_search(limit=10)
    for url in urls_a_scrapear:
        if not o.pode_scrape():
            break
        markdown, _ = client.scrape(url)
        o.consumir_scrape(url)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Literal

logger = logging.getLogger(__name__)


CAP_PADRAO_CREDITOS: int = 20


def custo_search(limit: int) -> int:
    """Calcula o custo real (em créditos) de uma chamada `Firecrawl.search`.

    O Firecrawl cobra 2 créditos por *bloco* de até 10 resultados solicitados,
    com base no parâmetro ``limit`` (não no número de resultados retornados).

    >>> custo_search(1)
    2
    >>> custo_search(10)
    2
    >>> custo_search(11)
    4
    >>> custo_search(20)
    4
    >>> custo_search(21)
    6
    """
    n = max(1, int(limit or 0))
    return 2 * math.ceil(n / 10)


def custo_scrape() -> int:
    """Custo real de uma chamada `Firecrawl.scrape` (1 crédito)."""
    return 1


@dataclass
class EventoCredito:
    """Registo de uma chamada que consumiu créditos."""

    tipo: Literal["search", "scrape"]
    custo: int
    detalhe: str = ""


class OrcamentoExcedido(RuntimeError):
    """Levantada quando se tenta consumir créditos acima do cap configurado."""


@dataclass
class OrcamentoFirecrawl:
    """Contador de créditos Firecrawl com cap duro por ingestão.

    Atributos:
        cap: limite máximo de créditos permitidos (default ``CAP_PADRAO_CREDITOS``).
        gasto: créditos já consumidos (read-only via property externa, mas mutável
            internamente para permitir testes determinísticos).
        eventos: lista de :class:`EventoCredito` na ordem em que aconteceram.

    O design é **defensivo**: as funções `consumir_*` levantam :class:`OrcamentoExcedido`
    se a chamada ultrapassar o cap. Isto força o caller a usar `pode_*` antes,
    tornando o comportamento explícito (em vez de silenciosamente exceder o budget
    como o pipeline antigo).
    """

    cap: int = CAP_PADRAO_CREDITOS
    gasto: int = 0
    eventos: list[EventoCredito] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.cap <= 0:
            raise ValueError(f"cap deve ser positivo, recebeu {self.cap}")
        if self.gasto < 0:
            raise ValueError(f"gasto inicial não pode ser negativo, recebeu {self.gasto}")
        if self.gasto > self.cap:
            raise ValueError(
                f"gasto inicial {self.gasto} já excede cap {self.cap}"
            )

    @property
    def restante(self) -> int:
        """Créditos ainda disponíveis (nunca negativo)."""
        return max(0, self.cap - self.gasto)

    def pode_search(self, *, limit: int) -> bool:
        """True se uma chamada `search` com este `limit` cabe no orçamento atual."""
        return custo_search(limit) <= self.restante

    def pode_scrape(self) -> bool:
        """True se ainda há pelo menos 1 crédito disponível para scrape."""
        return custo_scrape() <= self.restante

    def consumir_search(self, *, limit: int, query: str = "") -> int:
        """Regista uma chamada `search`. Levanta `OrcamentoExcedido` se ultrapassar o cap.

        Devolve o custo consumido nesta chamada.
        """
        custo = custo_search(limit)
        if custo > self.restante:
            raise OrcamentoExcedido(
                f"search com limit={limit} consumiria {custo} cr; "
                f"restam {self.restante} de {self.cap}."
            )
        self.gasto += custo
        self.eventos.append(
            EventoCredito(tipo="search", custo=custo, detalhe=f"limit={limit} q={query[:80]!r}")
        )
        logger.info(
            "Orçamento Firecrawl: +%s search (limit=%s) → gasto=%s/%s",
            custo, limit, self.gasto, self.cap,
        )
        return custo

    def consumir_scrape(self, *, url: str = "") -> int:
        """Regista uma chamada `scrape`. Levanta `OrcamentoExcedido` se ultrapassar o cap.

        Devolve 1 (custo do scrape).
        """
        custo = custo_scrape()
        if custo > self.restante:
            raise OrcamentoExcedido(
                f"scrape consumiria {custo} cr; restam {self.restante} de {self.cap}."
            )
        self.gasto += custo
        self.eventos.append(
            EventoCredito(tipo="scrape", custo=custo, detalhe=url[:120])
        )
        logger.info(
            "Orçamento Firecrawl: +%s scrape → gasto=%s/%s url=%s",
            custo, self.gasto, self.cap, url[:80],
        )
        return custo

    def resumo(self) -> dict[str, object]:
        """Snapshot serializável do estado atual (útil para logs e métricas)."""
        n_search = sum(1 for e in self.eventos if e.tipo == "search")
        n_scrape = sum(1 for e in self.eventos if e.tipo == "scrape")
        return {
            "cap": self.cap,
            "gasto": self.gasto,
            "restante": self.restante,
            "n_search": n_search,
            "n_scrape": n_scrape,
            "eventos": [
                {"tipo": e.tipo, "custo": e.custo, "detalhe": e.detalhe}
                for e in self.eventos
            ],
        }
