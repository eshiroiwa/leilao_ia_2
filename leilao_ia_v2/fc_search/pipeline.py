"""
Orquestração: pesquisa Firecrawl → escolha de URLs → scrape (markdown) → parse → geocode → upsert.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from supabase import Client

from .parser import dedupe_por_url, extrair_anuncios_do_markdown_pagina
from .llm_extractor import extrair_cards_com_llm_markdown, llm_extracao_habilitada
from .query_builder import montar_frase_busca_mercado, montar_frases_busca_mercado_em_camadas
from .search_client import executar_busca_web
from .urls import extrair_urls_do_markdown, extrair_urls_da_busca, selecionar_urls_para_scrape
from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.services import firecrawl_edital
from leilao_ia_v2.services.anuncios_mercado_coleta import persistir_cards_anuncios_mercado
from leilao_ia_v2.services.geocoding import geocodificar_anuncios_batch
from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

logger = logging.getLogger(__name__)


def complementar_anuncios_firecrawl_search(
    client: Client,
    *,
    leilao_imovel_id: str,
    cidade: str,
    estado_raw: str,
    bairro: str,
    tipo_imovel: str,
    area_ref: float,
    ignorar_cache_firecrawl: bool,
    max_chamadas_api: int | None = None,
    frase_busca_override: str | None = None,
) -> tuple[int, str, int]:
    """
    Complemento de ``anuncios_mercado`` via Firecrawl Search + scrape de 3–5 portais.

    Usa os mesmos campos de geolocalização e persistência que o fluxo Viva Real.
    Com ``frase_busca_override``, essa cadeia (após ``strip``) substitui a frase montada a partir do edital.

    Devolve ``(n_gravados, diagnostico_texto, n_chamadas_api_estimadas)`` — ``n_chamadas_api_estimadas``
    soma 1 por ``search`` e 1 por cada ``scrape`` HTTP executado (alinhado ao painel de uso).

    Se ``max_chamadas_api`` for um inteiro > 0, limita o total (search + scrapes); se já consumido pelo
    search não couber nenhum scrape, as URLs de scrape são truncadas. Com ``<= 0``, não executa search.
    ``None`` = sem limite adicional além de ``FC_SEARCH_MAX_SCRAPE_URLS``.
    """
    linhas: list[str] = []

    def _diag() -> str:
        return "\n".join(linhas).strip()

    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        linhas.append("erro: leilao_imovel_id vazio")
        return 0, _diag(), 0

    row = leilao_imoveis_repo.buscar_por_id(lid, client)
    if not isinstance(row, dict):
        logger.warning("Firecrawl Search complemento: leilão %s não encontrado", lid)
        linhas.append(f"erro: leilão não encontrado no Supabase (id={lid})")
        return 0, _diag(), 0

    override = (str(frase_busca_override).strip() if frase_busca_override is not None else "") or None
    if override:
        queries = [override]
        linhas.append("search: frase=override_utilizador")
    else:
        extra = row.get("leilao_extra_json") if isinstance(row.get("leilao_extra_json"), dict) else {}
        bairro_canonico = str((extra or {}).get("bairro_canonico") or "").strip()
        bairro_aliases = list((extra or {}).get("bairro_aliases") or [])
        queries = montar_frases_busca_mercado_em_camadas(
            row,
            tipo_imovel,
            bairro_canonico=bairro_canonico,
            bairro_aliases=bairro_aliases,
        )
    if not queries:
        q1 = montar_frase_busca_mercado(row, tipo_imovel)
        queries = [q1] if q1 else []
    queries = [str(q or "").strip() for q in queries if str(q or "").strip()]
    if not queries:
        logger.warning("Firecrawl Search complemento: sem frases de busca válidas")
        linhas.append("erro: nenhuma frase de busca válida")
        return 0, _diag(), 0

    if max_chamadas_api is not None and int(max_chamadas_api) <= 0:
        linhas.append("orçamento: max_chamadas_api<=0 — complemento Firecrawl Search não executado.")
        return 0, _diag(), 0

    n_chamadas_api = 0
    web: list[dict[str, Any]] = []
    linhas.append(f"search: camadas_planejadas={len(queries)}")
    for i, q in enumerate(queries, start=1):
        if max_chamadas_api is not None and int(n_chamadas_api) >= int(max_chamadas_api):
            linhas.append(
                f"search: orçamento atingido antes da camada {i} "
                f"(max_chamadas_api={int(max_chamadas_api)})."
            )
            break
        if len(q) < 8:
            continue
        try:
            web_i, n_search = executar_busca_web(q)
            n_chamadas_api += int(n_search or 0)
        except Exception:
            logger.exception("Firecrawl Search: falha na pesquisa da camada %s", i)
            linhas.append(f"search[{i}]: exceção na pesquisa (ver log com stack trace)")
            continue
        web_i = list(web_i or [])
        web.extend(web_i)
        linhas.append(f"search[{i}]: frase_chars={len(q)} resultados_web={len(web_i)}")
        if web_i and isinstance(web_i[0], dict):
            linhas.append(f"search[{i}]: chaves_1o_item={sorted(web_i[0].keys())}")

    urls: list[str] = extrair_urls_da_busca(web)
    # URLs embutidas em títulos/descrições (quando a API devolve texto rico)
    for it in web or []:
        if isinstance(it, dict):
            meta = it.get("metadata") if isinstance(it.get("metadata"), dict) else {}
            md = str(it.get("markdown") or "")
            if len(md) > 8000:
                md = md[:8000]
            blob = (
                f"{it.get('title') or ''} {it.get('description') or ''} "
                f"{meta.get('title') or ''} {meta.get('description') or ''} {md}"
            )
            urls.extend(extrair_urls_do_markdown(blob))
    urls = list(dict.fromkeys(urls))

    max_scrape = int(os.getenv("FC_SEARCH_MAX_SCRAPE_URLS", "5") or "5")
    alvo = selecionar_urls_para_scrape(urls, max_urls=max_scrape)
    if max_chamadas_api is not None:
        restante = int(max_chamadas_api) - int(n_chamadas_api)
        if restante < 0:
            restante = 0
        if len(alvo) > restante:
            linhas.append(
                f"orçamento: URLs de scrape reduzidas de {len(alvo)} para {restante} "
                f"(teto max_chamadas_api={int(max_chamadas_api)} após search)."
            )
            alvo = alvo[:restante]
    linhas.append(f"urls: candidatas_portais={len(urls)} para_scrape={len(alvo)}")
    if urls[:12]:
        linhas.append("urls: candidatas (até 12):")
        for u in urls[:12]:
            linhas.append(f"  - {u}")
    if not alvo:
        logger.info("Firecrawl Search complemento: nenhuma URL de portal aceite")
        linhas.append("motivo: nenhuma URL de portal aceite após filtro (hosts permitidos).")
        linhas.append(f"firecrawl_api_calls_estimadas={n_chamadas_api}")
        return 0, _diag(), n_chamadas_api

    uf_sigla = estado_livre_para_sigla_uf(estado_raw) or str(estado_raw or "").strip()[:2].upper()
    estado_parser = str(estado_raw or row.get("estado") or "").strip() or uf_sigla
    cidade_ref = (cidade or str(row.get("cidade") or "")).strip()
    bairro_ref = (bairro or str(row.get("bairro") or "")).strip()
    extra_row = row.get("leilao_extra_json") if isinstance(row.get("leilao_extra_json"), dict) else {}
    bairro_canonico = str((extra_row or {}).get("bairro_canonico") or "").strip()
    lat_ref = row.get("latitude")
    lon_ref = row.get("longitude")

    agregados: list[dict[str, Any]] = []
    llm_fallback_usos = 0
    llm_fallback_limite = max(0, int(os.getenv("FC_SEARCH_LLM_MAX_PAGINAS_FALLBACK", "2") or "2"))
    for u in alvo:
        n_chamadas_api += 1
        try:
            md, meta = firecrawl_edital.scrape_url_markdown(u, ignorar_cache=ignorar_cache_firecrawl)
        except Exception as ex:
            logger.info("Firecrawl Search complemento: scrape falhou url=%s", u[:120], exc_info=True)
            linhas.append(f"scrape: FALHA url={u[:160]} err={type(ex).__name__}: {str(ex)[:200]}")
            continue
        nmd = len(md or "")
        cards = extrair_anuncios_do_markdown_pagina(
            md,
            url_pagina=u,
            cidade_ref=cidade_ref,
            estado_ref=estado_parser,
            bairro_ref=bairro_ref,
        )
        if not cards and llm_extracao_habilitada() and llm_fallback_usos < llm_fallback_limite:
            cards_llm = extrair_cards_com_llm_markdown(
                markdown=md,
                url_pagina=u,
                cidade_ref=cidade_ref,
                estado_ref=estado_parser,
                bairro_ref=bairro_ref,
            )
            if cards_llm:
                cards = cards_llm
                llm_fallback_usos += 1
                linhas.append(
                    f"scrape: fallback_llm acionado url={u[:120]} cards_llm={len(cards_llm)}"
                )
        fonte = str((meta or {}).get("fonte") or "")
        linhas.append(
            f"scrape: ok url={u[:160]} markdown_chars={nmd} cards_extraidos={len(cards)} fonte={fonte}"
        )
        agregados.extend(cards)

    agregados = dedupe_por_url(agregados)
    if not agregados:
        logger.info("Firecrawl Search complemento: nenhum card extraído após %s páginas", len(alvo))
        linhas.append(f"parse: 0 cards após dedupe (páginas_scrapeadas={len(alvo)})")
        linhas.append(f"firecrawl_api_calls_estimadas={n_chamadas_api}")
        return 0, _diag(), n_chamadas_api

    geocodificar_anuncios_batch(
        agregados,
        cidade=cidade_ref,
        estado=uf_sigla,
        bairro_fallback=bairro_ref,
        permitir_fallback_centro_cidade=False,
    )

    query_meta = " | ".join(queries[:3])
    url_listagem_meta = f"firecrawl_search_camadas:{query_meta[:400]}"
    tipo_fb = (tipo_imovel or str(row.get("tipo_imovel") or "apartamento")).strip().lower()
    diag_persist: dict[str, Any] = {}
    salvos = persistir_cards_anuncios_mercado(
        client,
        agregados,
        cidade=cidade_ref,
        estado_raw=str(row.get("estado") or estado_raw or ""),
        bairro=bairro_ref,
        leilao_imovel_id=lid,
        url_listagem=url_listagem_meta,
        tipo_imovel_fallback=tipo_fb,
        origem_metadados="firecrawl_search_complemento",
        leilao_row=row,
        exigir_geolocalizacao=True,
        bairro_canonico=bairro_canonico,
        lat_ref=float(lat_ref) if lat_ref is not None else None,
        lon_ref=float(lon_ref) if lon_ref is not None else None,
        diagnostico_saida=diag_persist,
    )
    desc = dict(diag_persist.get("descartes_por_motivo") or {})
    linhas.append(
        "persistencia: cards_recebidos=%s validos_pre_upsert=%s descartes_total=%s upsert=%s"
        % (
            int(diag_persist.get("cards_recebidos") or 0),
            int(diag_persist.get("cards_validos_pre_upsert") or 0),
            int(diag_persist.get("descartes_total") or 0),
            int(diag_persist.get("upsert_gravados") or salvos or 0),
        )
    )
    if desc:
        itens = ", ".join(f"{k}={int(v)}" for k, v in sorted(desc.items()))
        linhas.append(f"persistencia: descartes_por_motivo: {itens}")
    if salvos:
        logger.info(
            "Firecrawl Search complemento: %s anúncios gravados; frase=%r urls=%s",
            salvos,
            query_meta[:200],
            alvo,
        )
        linhas.append(f"persistencia: anuncios_gravados_upsert={salvos}")
    else:
        linhas.append("persistencia: upsert devolveu 0 (ver regras do repo / linhas rejeitadas)")
    linhas.append(f"firecrawl_api_calls_estimadas={n_chamadas_api}")
    logger.info("Firecrawl Search diagnóstico (resumo):\n%s", _diag())
    return salvos, _diag(), n_chamadas_api
