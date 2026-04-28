"""
Adaptador entre o pipeline de ingestão do edital e o módulo `comparaveis`.

Este módulo é a **única porta** pela qual o resto da aplicação invoca a busca
de comparáveis. Ele:

1. Constrói um :class:`LeilaoAlvo` a partir da extração do edital.
2. Cria um :class:`OrcamentoFirecrawl` com o cap recebido.
3. Executa :func:`comparaveis.pipeline.executar_pipeline`.
4. Converte o :class:`EstatisticasPipeline` para o **mesmo formato de dict**
   que o resto do pipeline espera consumir
   (:func:`pipeline.ingestao_edital`, :func:`services.cache_media_leilao`).

Defesa em profundidade: se o pipeline levantar excepção inesperada, devolvemos
``ok=False`` em vez de propagar — o resto da ingestão (gravação do leilão,
cache automático, log) continua a correr.
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


URL_LISTAGEM = "comparaveis_v2"
"""Marca de proveniência usada em logs / campo ``url_listagem`` do dict de retorno."""


def formatar_log_pos_ingestao(resumo: dict[str, Any]) -> str:
    """Texto curto para anexar a ``ultima_ingestao_log_text``.

    Aceita o dict produzido por :func:`executar_comparaveis_pos_ingestao`
    (mesmas chaves que o caminho antigo).
    """
    if resumo.get("omitido"):
        return f"Comparáveis: omitido — {resumo.get('motivo', '')}"
    if not resumo.get("ok"):
        return f"Comparáveis: falha — {resumo.get('erro', resumo)}"
    parts = [
        "Comparáveis (v2): OK",
        f"anuncios_salvos={resumo.get('anuncios_salvos', 0)}",
        f"url_listagem={str(resumo.get('url_listagem') or '')[:120]}",
        f"geocodificados={resumo.get('n_geocodificados', 0)}",
    ]
    if resumo.get("markdown_insuficiente"):
        parts.append("nenhum_card_persistido")
    nfc = int(resumo.get("firecrawl_chamadas_api") or 0)
    if nfc > 0:
        parts.append(f"firecrawl_api_calls={nfc}")
    base = " | ".join(parts)
    diag = str(resumo.get("diagnostico_firecrawl_search") or "").strip()
    if diag:
        if len(diag) > 8000:
            diag = diag[:8000] + "\n… (diagnóstico truncado)"
        base = f"{base}\n--- Comparáveis (resumo) ---\n{diag}"
    return base


def executar_comparaveis_pos_ingestao(
    client: Client,
    *,
    leilao_imovel_id: str,  # noqa: ARG001 - mantido por compat de assinatura; v2 não precisa
    extn: ExtracaoEditalLLM,
    ignorar_cache_firecrawl: bool = False,  # noqa: ARG001 - cache de scrape é sempre reusado (gratuito)
    max_chamadas_api_firecrawl: int | None = None,
    leilao_dict: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Executa a busca de comparáveis após a ingestão do edital.

    Args:
        client: cliente Supabase usado para upsert em ``anuncios_mercado``.
        leilao_imovel_id: id do leilão recém ingerido (não usado pela v2 — os
            anúncios em ``anuncios_mercado`` não são por leilão; a vinculação
            é feita depois via ``cache_media_bairro``). Mantido por compat
            com chamadores que ainda passam.
        extn: extração do edital (cidade, UF, bairro, tipo, área).
        ignorar_cache_firecrawl: ignorado pela v2 — o cache de scrape em disco
            é sempre reusado porque é gratuito (não consome créditos Firecrawl).
        max_chamadas_api_firecrawl: cap de créditos para esta etapa
            (semântica: ``max_firecrawl_creditos_analise``). ``None`` usa o
            default lido de :func:`config.busca_mercado_parametros`.

    Returns:
        Dict com chaves: ``ok``, ``omitido``, ``motivo``, ``anuncios_salvos``,
        ``url_listagem``, ``n_geocodificados``, ``markdown_insuficiente``,
        ``firecrawl_chamadas_api``, ``diagnostico_firecrawl_search``,
        ``falha_por_filtros_persistencia``.
    """
    try:
        return _executar(
            client,
            extn=extn,
            max_chamadas_api_firecrawl=max_chamadas_api_firecrawl,
            leilao_dict=leilao_dict,
        )
    except Exception:
        logger.exception("Comparaveis: falha — devolvendo erro sem propagar.")
        return {
            "ok": False,
            "erro": "comparaveis_excecao_ver_log",
            "firecrawl_chamadas_api": 0,
        }


# -----------------------------------------------------------------------------
# Implementação
# -----------------------------------------------------------------------------

def _executar(
    client: Client,
    *,
    extn: ExtracaoEditalLLM,
    max_chamadas_api_firecrawl: int | None,
    leilao_dict: dict[str, Any] | None = None,
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
        min_amostras_refino=_resolver_min_amostras_refino(),
        leilao_dict=leilao_dict or _leilao_dict_de_extn(extn),
    )

    return _resultado_para_dict(resultado, orcamento)


def _leilao_dict_de_extn(extn: ExtracaoEditalLLM) -> dict[str, Any]:
    """Constrói um pseudo-dict do leilão a partir do :class:`ExtracaoEditalLLM`.

    Inclui os campos consultados por
    :func:`services.normalizacao_anuncio.leilao_indica_condominio` para que
    a promoção ``casa → casa_condominio`` funcione já no caminho
    pós-ingestão de URL — mesmo quando o caller não dispõe do registro
    cru do Supabase.
    """
    descricao_partes: list[str] = []
    for attr in (
        "descricao",
        "descricao_imovel",
        "observacoes",
        "edital_resumo",
    ):
        v = getattr(extn, attr, None)
        if v:
            descricao_partes.append(str(v))
    return {
        "cidade": (extn.cidade or "").strip(),
        "estado": (extn.estado or "").strip(),
        "bairro": (extn.bairro or "").strip(),
        "tipo_imovel": (extn.tipo_imovel or "").strip().lower(),
        "endereco": str(getattr(extn, "endereco", "") or "").strip(),
        "descricao": "\n".join(descricao_partes).strip(),
    }


def executar_comparaveis_para_cache(
    client: Client,
    *,
    cidade: str,
    estado_raw: str,
    bairro: str,
    tipo_imovel: str,
    area_ref: float = 0.0,
    max_chamadas_api: int | None = None,
    leilao_dict: dict[str, Any] | None = None,
) -> tuple[int, int, bool]:
    """Helper para o cache de média complementar amostras via Firecrawl.

    Devolve ``(n_anuncios_gravados, n_chamadas_api_estimadas,
    falha_por_filtros_persistencia)``.

    Diferente de :func:`executar_comparaveis_pos_ingestao`, esta variante
    aceita os campos individuais (a chamadora não tem um ``ExtracaoEditalLLM``).
    Em caso de qualquer erro, devolve ``(0, 0, False)``.

    O terceiro elemento — ``falha_por_filtros_persistencia`` — é ``True`` quando
    o Firecrawl retornou cards mas TODOS foram descartados pelos filtros do
    pipeline (cidade errada, validação geográfica, etc.). O caller pode usar
    esse sinal para evitar repetir a mesma busca na mesma sessão (é
    determinística — se falhou agora, vai falhar de novo).
    """
    cidade_l = (cidade or "").strip()
    estado_l = (estado_raw or "").strip()
    if not cidade_l or not estado_l:
        return 0, 0, False
    if not (os.getenv("FIRECRAWL_API_KEY") or "").strip():
        return 0, 0, False
    cap_fc = _resolver_cap(max_chamadas_api)
    if cap_fc <= 0:
        return 0, 0, False

    uf = estado_livre_para_sigla_uf(estado_l) or estado_l[:2].upper()
    if len(uf) != 2:
        return 0, 0, False

    leilao = LeilaoAlvo(
        cidade=cidade_l,
        estado_uf=uf,
        tipo_imovel=(tipo_imovel or "apartamento").strip().lower(),
        bairro=(bairro or "").strip(),
        area_m2=float(area_ref) if area_ref and float(area_ref) > 0 else None,
    )
    orcamento = OrcamentoFirecrawl(cap=cap_fc)
    try:
        resultado = executar_pipeline(
            leilao,
            orcamento=orcamento,
            supabase_client=client,
            min_amostras_refino=_resolver_min_amostras_refino(),
            leilao_dict=leilao_dict,
        )
    except Exception:
        logger.exception("Comparaveis (cache): falha — devolvendo (0,0,False).")
        return 0, 0, False

    s = resultado.estatisticas
    falha_filtros = (
        int(s.persistidos) == 0
        and int(s.cards_extraidos) > 0
        and int(s.cards_descartados_validacao) > 0
    )
    return int(s.persistidos), _chamadas_api(s), bool(falha_filtros)


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
            "url_listagem": URL_LISTAGEM,
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
        "url_listagem": URL_LISTAGEM,
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
        "url_listagem": URL_LISTAGEM,
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
        logger.warning("Não consegui ler max_firecrawl_creditos_analise — usando 20.", exc_info=True)
        return 20


def _resolver_min_amostras_refino() -> int:
    """Lê ``min_amostras_cache`` da config e usa como limiar para o refino.

    Política da pergunta 3: se após descartar um card refinado (cuja nova
    coord caiu noutro município) ainda houver ``>= min_amostras`` cards
    aprovados, descartar; caso contrário reverter para coord antiga.
    Reusamos o mesmo limiar do cache para coerência: não vale a pena
    persistir cards "refinados-erradamente" se o cache não vai usar mesmo.
    """
    try:
        from leilao_ia_v2.config.busca_mercado_parametros import get_busca_mercado_parametros

        return int(get_busca_mercado_parametros().min_amostras_cache)
    except Exception:
        return 4


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
    """Conta chamadas REAIS à API Firecrawl (search + scrapes não-cache)."""
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
