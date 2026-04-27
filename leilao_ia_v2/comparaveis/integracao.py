"""
Adaptador entre o pipeline de ingestão do edital e o módulo `comparaveis` (v2).

Este módulo é a **única porta** pela qual o pipeline antigo
(`pipeline/ingestao_edital.py`) invoca a busca de comparáveis. Ele:

1. Lê a flag de ambiente :data:`FLAG_ENV` (``LEILAO_IA_COMPARAVEIS_NOVO``).
2. Quando **off** (default), delega ao caminho antigo
   :func:`services.comparaveis_pos_ingestao.executar_comparaveis_apos_ingestao_leilao`,
   preservando a compatibilidade total enquanto o novo módulo está em rollout.
3. Quando **on**, executa :func:`comparaveis.pipeline.executar_pipeline` e
   converte o :class:`EstatisticasPipeline` para o **mesmo formato de dict**
   que o caminho antigo devolve — o pipeline de ingestão lê apenas dicts e
   não precisa de saber qual versão correu.

Princípios:
- **Mesma assinatura** que o caminho antigo → swap sem migração de chamadores.
- **Mesmo formato de retorno** (chaves: ``ok``, ``omitido``, ``motivo``,
  ``anuncios_salvos``, ``url_listagem``, ``n_geocodificados``,
  ``markdown_insuficiente``, ``firecrawl_chamadas_api``,
  ``diagnostico_firecrawl_search``, ``falha_por_filtros_persistencia``).
- ``aguarda_confirmacao_frase`` **não é suportado** na v2 — a frase é
  determinística (uma só, focada). Se o utilizador quiser confirmar, faz-lo
  ANTES de habilitar a flag.
- **Defesa em profundidade**: se a v2 falhar com excepção inesperada, o
  router devolve ``ok=False`` em vez de propagar, mantendo o resto do pipeline
  vivo.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from supabase import Client

from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.pipeline import (
    LeilaoAlvo,
    ResultadoPipeline,
    executar_pipeline,
)
from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM
from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

logger = logging.getLogger(__name__)


FLAG_ENV = "LEILAO_IA_COMPARAVEIS_NOVO"
"""Variável de ambiente que activa a v2. Valores aceites como `on`:
``"1"``, ``"true"``, ``"on"``, ``"yes"``, ``"sim"`` (case-insensitive)."""

URL_LISTAGEM_V2 = "comparaveis_v2"


def flag_v2_ativa() -> bool:
    """Devolve ``True`` se a flag ``LEILAO_IA_COMPARAVEIS_NOVO`` está activa.

    Aceita ``"1"``, ``"true"``, ``"on"``, ``"yes"``, ``"sim"`` (case-insensitive).
    Tudo o resto (incluindo ausente/vazio) é tratado como off.
    """
    raw = (os.getenv(FLAG_ENV) or "").strip().lower()
    return raw in {"1", "true", "on", "yes", "sim"}


def executar_comparaveis_pos_ingestao(
    client: Client,
    *,
    leilao_imovel_id: str,
    extn: ExtracaoEditalLLM,
    ignorar_cache_firecrawl: bool = False,
    max_chamadas_api_firecrawl: int | None = None,
) -> dict[str, Any]:
    """Router: chama v2 (sob flag) ou v1 (default).

    Args:
        client: cliente Supabase (passado adiante a v1 ou à persistência v2).
        leilao_imovel_id: id do leilão recém ingerido (usado pela v1; a v2
            não precisa porque os anúncios em ``anuncios_mercado`` não são
            por leilão — a vinculação é feita depois via ``cache_media_bairro``).
        extn: extração do edital (cidade, UF, bairro, tipo, área).
        ignorar_cache_firecrawl: se ``True``, força nova chamada à API
            mesmo havendo cache em disco. **Hoje a v2 ignora este flag**
            porque o cache de scrape é gratuito e queremos sempre reusá-lo.
        max_chamadas_api_firecrawl: cap de créditos para esta etapa (igual
            semântica nas duas versões — `max_firecrawl_creditos_analise`).
            ``None`` deixa cada implementação aplicar o seu default.

    Returns:
        Dict no mesmo formato que o caminho antigo. Ver
        :func:`services.comparaveis_pos_ingestao.formatar_log_pos_ingestao`
        para a lista de chaves consumidas pelo pipeline de ingestão.
    """
    if flag_v2_ativa():
        logger.info("Comparaveis: rota v2 ativa (LEILAO_IA_COMPARAVEIS_NOVO=1).")
        try:
            return _executar_v2(
                client,
                leilao_imovel_id=leilao_imovel_id,
                extn=extn,
                max_chamadas_api_firecrawl=max_chamadas_api_firecrawl,
            )
        except Exception:
            logger.exception("Comparaveis v2: falha — devolvendo erro sem propagar.")
            return {
                "ok": False,
                "erro": "comparaveis_v2_excecao_ver_log",
                "firecrawl_chamadas_api": 0,
            }

    # Default: caminho antigo (v1) — preserva comportamento existente.
    from leilao_ia_v2.services.comparaveis_pos_ingestao import (
        executar_comparaveis_apos_ingestao_leilao,
    )

    return executar_comparaveis_apos_ingestao_leilao(
        client,
        leilao_imovel_id=leilao_imovel_id,
        extn=extn,
        ignorar_cache_firecrawl=ignorar_cache_firecrawl,
        max_chamadas_api_firecrawl=max_chamadas_api_firecrawl,
    )


# -----------------------------------------------------------------------------
# Implementação v2
# -----------------------------------------------------------------------------

def _executar_v2(
    client: Client,
    *,
    leilao_imovel_id: str,
    extn: ExtracaoEditalLLM,
    max_chamadas_api_firecrawl: int | None,
) -> dict[str, Any]:
    cidade = (extn.cidade or "").strip()
    estado_raw = (extn.estado or "").strip()
    bairro = (extn.bairro or "").strip()

    if not cidade or not estado_raw:
        return _omitido("sem_cidade_ou_estado")

    if not (os.getenv("FIRECRAWL_API_KEY") or "").strip():
        return _omitido("FIRECRAWL_API_KEY_ausente")

    cap_fc = _resolver_cap(max_chamadas_api_firecrawl)
    if cap_fc <= 0:
        return _omitido(
            "firecrawl_orcamento_analise_esgotado",
            diagnostico=(
                "Orçamento de créditos Firecrawl esgotado para esta etapa "
                "(edital já consumiu o teto da análise)."
            ),
        )

    uf = (
        estado_livre_para_sigla_uf(estado_raw)
        or str(estado_raw or "").strip()[:2].upper()
    )
    if len(uf) != 2:
        return _omitido(f"uf_invalida:{estado_raw!r}")

    tipo_imovel = (extn.tipo_imovel or "apartamento").strip().lower()
    area_ref = _area_referencia(extn)

    leilao = LeilaoAlvo(
        cidade=cidade,
        estado_uf=uf,
        tipo_imovel=tipo_imovel,
        bairro=bairro,
        area_m2=area_ref if area_ref > 0 else None,
    )
    orcamento = OrcamentoFirecrawl(cap=cap_fc)

    resultado = executar_pipeline(
        leilao,
        orcamento=orcamento,
        supabase_client=client,
    )

    return _resultado_para_dict(resultado, orcamento)


def _resultado_para_dict(
    resultado: ResultadoPipeline,
    orcamento: OrcamentoFirecrawl,
) -> dict[str, Any]:
    """Converte :class:`ResultadoPipeline` para o formato consumido pelo pipeline."""
    s = resultado.estatisticas

    if s.abortado:
        return {
            "ok": True,
            "omitido": True,
            "motivo": s.motivo_aborto or "abortado",
            "anuncios_salvos": 0,
            "url_listagem": URL_LISTAGEM_V2,
            "n_geocodificados": 0,
            "markdown_insuficiente": True,
            "firecrawl_chamadas_api": _chamadas_api(s),
            "diagnostico_firecrawl_search": _diagnostico(s, orcamento),
            "falha_por_filtros_persistencia": False,
        }

    persistidos = int(s.persistidos)
    descartes = int(s.cards_descartados_validacao)
    cards_recebidos = int(s.cards_extraidos)
    falha_por_filtros = persistidos == 0 and cards_recebidos > 0 and descartes > 0

    return {
        "ok": True,
        "anuncios_salvos": persistidos,
        "url_listagem": URL_LISTAGEM_V2,
        "n_geocodificados": int(s.cards_aprovados_validacao),
        "markdown_insuficiente": persistidos == 0,
        "firecrawl_chamadas_api": _chamadas_api(s),
        "diagnostico_firecrawl_search": _diagnostico(s, orcamento),
        "falha_por_filtros_persistencia": falha_por_filtros,
    }


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _omitido(motivo: str, *, diagnostico: str = "") -> dict[str, Any]:
    return {
        "ok": True,
        "omitido": True,
        "motivo": motivo,
        "anuncios_salvos": 0,
        "url_listagem": URL_LISTAGEM_V2,
        "n_geocodificados": 0,
        "markdown_insuficiente": False,
        "firecrawl_chamadas_api": 0,
        "diagnostico_firecrawl_search": diagnostico,
        "falha_por_filtros_persistencia": False,
    }


def _resolver_cap(cap_arg: int | None) -> int:
    if cap_arg is not None:
        try:
            return max(0, int(cap_arg))
        except (TypeError, ValueError):
            return 0
    try:
        from leilao_ia_v2.config.busca_mercado_parametros import get_busca_mercado_parametros

        return int(get_busca_mercado_parametros().max_firecrawl_creditos_analise)
    except Exception:
        logger.warning("Não consegui ler max_firecrawl_creditos_analise — usando 15.", exc_info=True)
        return 15


def _area_referencia(extn: ExtracaoEditalLLM) -> float:
    """Devolve a melhor área disponível: util > total > 0."""
    try:
        if extn.area_util is not None and float(extn.area_util) > 0:
            return float(extn.area_util)
        if extn.area_total is not None and float(extn.area_total) > 0:
            return float(extn.area_total)
    except (TypeError, ValueError):
        pass
    return 0.0


def _chamadas_api(s: Any) -> int:
    """Conta chamadas REAIS à API Firecrawl (search + scrapes não-cache).

    O caminho antigo conta uma "chamada" ≈ uma round-trip à API.
    Cache hits NÃO contam. Search conta como 1 (mesmo custando 2 cr).
    """
    paginas = int(getattr(s, "paginas_scrapadas", 0) or 0)
    cache_hits = int(getattr(s, "paginas_cache_hit", 0) or 0)
    scrapes_pagos = max(0, paginas - cache_hits)
    search_calls = 1 if int(getattr(s, "urls_busca", 0) or 0) > 0 else 0
    return search_calls + scrapes_pagos


def _diagnostico(s: Any, orcamento: OrcamentoFirecrawl) -> str:
    """Diagnóstico compacto, single-line, para anexar ao log de ingestão."""
    motivos = getattr(s, "motivos_descarte_validacao", {}) or {}
    motivos_str = ",".join(f"{k}={v}" for k, v in sorted(motivos.items())) or "-"
    return " | ".join(
        [
            "v2",
            f"frase={getattr(s, 'frase_busca', '')[:120]!r}",
            f"urls={int(getattr(s, 'urls_busca', 0) or 0)}",
            f"aceites={int(getattr(s, 'urls_aceites_busca', 0) or 0)}",
            f"scrapes={int(getattr(s, 'paginas_scrapadas', 0) or 0)}",
            f"cache_hits={int(getattr(s, 'paginas_cache_hit', 0) or 0)}",
            f"filtro_rejeitou={int(getattr(s, 'paginas_filtro_rejeitado', 0) or 0)}",
            f"cards={int(getattr(s, 'cards_extraidos', 0) or 0)}",
            f"persistidos={int(getattr(s, 'persistidos', 0) or 0)}",
            f"descartes_validacao={int(getattr(s, 'cards_descartados_validacao', 0) or 0)}",
            f"motivos_descarte={motivos_str}",
            f"creditos={int(orcamento.gasto)}/{int(orcamento.cap)}",
        ]
    )
