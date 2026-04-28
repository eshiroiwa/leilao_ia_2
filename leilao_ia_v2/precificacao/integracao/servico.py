"""
Ponto único de entrada para precificar um leilão a partir do seu ``id``
e gravar o resultado em ``leilao_extra_json.precificacao_v2``.

Este módulo conecta os pedaços:

1. lê a row de ``leilao_imoveis``;
2. converte para :class:`ImovelAlvo`;
3. constroi o callback de busca via :func:`construir_buscador`;
4. chama :func:`leilao_ia_v2.precificacao.precificar`;
5. persiste via :func:`gravar_resultado`.

A ingestão consome ``precificar_leilao()`` num try/except — não pode
quebrar o pipeline em caso de problema (cidade pequena, banco lento,
etc.). Quando ``persistir=False``, devolve o resultado em memória sem
escrever (útil para previews na UI ou para testes).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from supabase import Client

from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.precificacao import (
    PoliticaExpansao,
    ResultadoPrecificacao,
    precificar,
)
from leilao_ia_v2.precificacao.integracao.buscador_supabase import (
    BuscaSupabaseConfig,
    construir_buscador,
)
from leilao_ia_v2.precificacao.integracao.conversores import leilao_row_para_alvo
from leilao_ia_v2.precificacao.integracao.persistencia import gravar_resultado

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResultadoServico:
    """Wrapper do resultado para callers que querem inspecionar status.

    - ``ok``: True se conseguiu produzir uma precificação (mesmo
      ``INSUFICIENTE`` conta como ``ok``; só fica ``False`` quando a row
      do leilão não existe ou ``area_m2`` é inválida).
    - ``motivo``: explicação curta para logs/UI.
    - ``resultado``: o :class:`ResultadoPrecificacao`, ou ``None``.
    - ``persistido``: True se gravou no banco com sucesso.
    """

    ok: bool
    motivo: str
    resultado: Optional[ResultadoPrecificacao] = None
    persistido: bool = False


def precificar_leilao(
    client: Client,
    leilao_imovel_id: str,
    *,
    persistir: bool = True,
    politica: Optional[PoliticaExpansao] = None,
    config_busca: Optional[BuscaSupabaseConfig] = None,
) -> ResultadoServico:
    """Executa a precificação ponta-a-ponta para um leilão.

    Args:
        client: Supabase client.
        leilao_imovel_id: UUID do registo em ``leilao_imoveis``.
        persistir: se ``True``, grava em ``leilao_extra_json.precificacao_v2``.
        politica: regras de expansão (default :class:`PoliticaExpansao`).
        config_busca: parâmetros do adapter Supabase (default rigoroso).

    Returns:
        :class:`ResultadoServico` — nunca levanta exceção.
    """
    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        return ResultadoServico(False, "id vazio")

    row = leilao_imoveis_repo.buscar_por_id(lid, client)
    if not row:
        return ResultadoServico(False, "leilão não encontrado")

    alvo = leilao_row_para_alvo(row)
    if alvo.area_m2 <= 0:
        return ResultadoServico(False, "leilão sem área positiva — não dá para precificar")

    try:
        fn_buscar = construir_buscador(client=client, alvo=alvo, config=config_busca)
        resultado = precificar(
            alvo=alvo,
            fn_buscar_amostras=fn_buscar,
            politica=politica or PoliticaExpansao(),
        )
    except Exception:
        logger.exception("precificar_leilao: motor falhou (lid=%s)", lid[:12])
        return ResultadoServico(False, "falha no motor de precificação")

    if not persistir:
        return ResultadoServico(
            True,
            f"calculado (sem persistir): veredito={resultado.veredito.nivel}",
            resultado=resultado,
        )

    try:
        gravar_resultado(
            client,
            lid,
            resultado,
            leilao_extra_json_atual=row.get("leilao_extra_json"),
        )
        return ResultadoServico(
            True,
            f"persistido: veredito={resultado.veredito.nivel} confianca={resultado.confianca.nivel}",
            resultado=resultado,
            persistido=True,
        )
    except Exception:
        logger.exception("precificar_leilao: falha ao persistir (lid=%s)", lid[:12])
        return ResultadoServico(
            True,  # cálculo OK, persistência que falhou
            "calculado mas não persistido (erro no banco)",
            resultado=resultado,
            persistido=False,
        )
