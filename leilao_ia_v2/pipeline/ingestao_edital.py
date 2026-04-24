"""
Pipeline: URL → Firecrawl (ou cache disco) → LLM → normalização → Supabase.
"""

from __future__ import annotations

import logging
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from supabase import Client
from postgrest.exceptions import APIError

from leilao_ia_v2.config.busca_mercado_parametros import get_busca_mercado_parametros
from leilao_ia_v2.constants import STATUS_PENDENTE
from leilao_ia_v2.exceptions import (
    EscolhaSobreDuplicataNecessaria,
    IngestaoSemConteudoEditalError,
    UrlInvalidaIngestaoError,
)
from leilao_ia_v2.normalizacao import (
    normalizar_conservacao,
    normalizar_data_para_iso,
    normalizar_tipo_casa,
    normalizar_tipo_imovel,
    normalizar_url_leilao,
)
from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM, LeilaoExtraJson
from leilao_ia_v2.services import comparaveis_pos_ingestao, extracao_edital_llm, firecrawl_edital, pii_sanitizer
from leilao_ia_v2.services.cache_media_leilao import formatar_log_pos_cache, resolver_cache_media_pos_ingestao
from leilao_ia_v2.services.markdown_foto_imovel import extrair_url_foto_imovel_markdown
from leilao_ia_v2.services.conteudo_edital_heuristica import validar_markdown_antes_da_extracao
from leilao_ia_v2.services.edital_markdown_limpeza import limpar_edital_markdown_ruido
from leilao_ia_v2.services.geocoding import geocodificar_endereco

logger = logging.getLogger(__name__)


@dataclass
class ResultadoIngestaoEdital:
    """Resultado de uma ingestão bem-sucedida ou ignorada."""

    modo: Literal["inserido", "atualizado", "ignorado_duplicata"]
    id: Optional[str] = None
    url_leilao: str = ""
    log: str = ""
    metricas_llm: dict[str, Any] = field(default_factory=dict)
    pos_comparaveis: dict[str, Any] = field(default_factory=dict)
    pos_cache: dict[str, Any] = field(default_factory=dict)
    firecrawl_chamadas_api_total: int = 0


def _montar_log_linhas(*linhas: str) -> str:
    return "\n".join(l for l in linhas if l)


def _eh_violacao_unique_url_insert(e: APIError) -> bool:
    c = getattr(e, "code", None)
    return c in ("23505", 23505)


def _modalidade_venda_apos_datas(ext: ExtracaoEditalLLM) -> LeilaoExtraJson:
    """
    Se o modelo não preencheu `modalidade_venda`: com 1ª ou 2ª praça → leilao;
    sem ambas → venda_direta (ofertas tipo Superbid “venda direta” sem praças).
    """
    ex = ext.leilao_extra
    cur = ex.modalidade_venda
    if cur in ("leilao", "venda_direta"):
        return ex
    d1, d2 = ext.data_leilao_1_praca, ext.data_leilao_2_praca
    if d1 or d2:
        return ex.model_copy(update={"modalidade_venda": "leilao"})
    return ex.model_copy(update={"modalidade_venda": "venda_direta"})


def _extracao_normalizada(ext: ExtracaoEditalLLM) -> ExtracaoEditalLLM:
    ti = normalizar_tipo_imovel(ext.tipo_imovel)
    co = normalizar_conservacao(ext.conservacao)
    tc = normalizar_tipo_casa(ext.tipo_casa, ti)
    d1 = normalizar_data_para_iso(ext.data_leilao_1_praca)
    d2 = normalizar_data_para_iso(ext.data_leilao_2_praca)
    dl = normalizar_data_para_iso(ext.data_leilao)
    ext1 = ext.model_copy(
        update={
            "tipo_imovel": ti,
            "conservacao": co,
            "tipo_casa": tc,
            "data_leilao_1_praca": d1,
            "data_leilao_2_praca": d2,
            "data_leilao": dl,
        }
    )
    extra = _modalidade_venda_apos_datas(ext1)
    return ext1.model_copy(update={"leilao_extra": extra})


def _buscar_coordenadas_extracao(extn: ExtracaoEditalLLM) -> Optional[tuple[float, float]]:
    """
    Nominatim (mesma lógica do sistema anterior): logradouro + cidade + estado + bairro.
    Só chama se houver ao menos endereço ou cidade.
    """
    logr = (extn.endereco or "").strip()
    bai = (extn.bairro or "").strip()
    cid = (extn.cidade or "").strip()
    uf = (extn.estado or "").strip()
    if not logr and not cid:
        logger.info("Geocodificação omitida: sem endereço nem cidade após extração.")
        return None
    try:
        coords = geocodificar_endereco(
            logradouro=logr,
            bairro=bai,
            cidade=cid,
            estado=uf,
        )
        if coords:
            logger.info("Geocodificação OK: lat=%s lon=%s", coords[0], coords[1])
        else:
            logger.info("Geocodificação sem resultado para logradouro=%r cidade=%r", logr[:80], cid)
        return coords
    except (RuntimeError, ValueError, TypeError):
        logger.exception("Falha na geocodificação (Nominatim)")
        return None


def _leilao_extra_com_pii_redigido(extra: LeilaoExtraJson) -> dict[str, Any]:
    o, r = pii_sanitizer.redigir_pii_extracao_extra(
        extra.observacoes_markdown,
        extra.regras_leilao_markdown,
    )
    ex2 = extra.model_copy(update={"observacoes_markdown": o, "regras_leilao_markdown": r})
    return ex2.model_dump(mode="json", exclude_none=True)


def montar_payload_gravacao(
    ext: ExtracaoEditalLLM,
    *,
    url: str,
    markdown_bruto: str,
    edital_metadados: dict[str, Any],
    metricas_llm: dict[str, Any],
    log_text: str,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> dict[str, Any]:
    extn = _extracao_normalizada(ext)
    extra_json = _leilao_extra_com_pii_redigido(extn.leilao_extra)
    edital_md = pii_sanitizer.redigir_pii_texto(markdown_bruto)
    agora = datetime.now(timezone.utc).isoformat()

    row: dict[str, Any] = {
        "url_leilao": url,
        "status": STATUS_PENDENTE,
        "cache_media_bairro_ids": [],
        "endereco": extn.endereco,
        "cidade": extn.cidade,
        "estado": extn.estado,
        "bairro": extn.bairro,
        "tipo_imovel": extn.tipo_imovel,
        "conservacao": extn.conservacao,
        "tipo_casa": extn.tipo_casa,
        "andar": extn.andar,
        "area_util": extn.area_util,
        "area_total": extn.area_total,
        "quartos": extn.quartos,
        "vagas": extn.vagas,
        "padrao_imovel": extn.padrao_imovel,
        "url_foto_imovel": extn.url_foto_imovel,
        "data_leilao_1_praca": extn.data_leilao_1_praca,
        "valor_lance_1_praca": extn.valor_lance_1_praca,
        "data_leilao_2_praca": extn.data_leilao_2_praca,
        "valor_lance_2_praca": extn.valor_lance_2_praca,
        "valor_arrematacao": extn.valor_arrematacao,
        "valor_avaliacao": extn.valor_avaliacao,
        "data_leilao": extn.data_leilao,
        "leilao_extra_json": extra_json,
        "edital_markdown": edital_md,
        "edital_fonte": str(edital_metadados.get("fonte") or "firecrawl"),
        "edital_coletado_em": agora,
        "edital_metadados_json": edital_metadados,
        "ultima_extracao_llm_em": agora,
        "ultima_extracao_llm_modelo": metricas_llm.get("modelo"),
        "ultima_extracao_tokens_prompt": metricas_llm.get("prompt_tokens"),
        "ultima_extracao_tokens_completion": metricas_llm.get("completion_tokens"),
        "ultima_extracao_custo_usd": metricas_llm.get("custo_usd_estimado"),
        "ultima_ingestao_log_text": log_text,
    }
    if latitude is not None:
        row["latitude"] = latitude
    if longitude is not None:
        row["longitude"] = longitude
    return {k: v for k, v in row.items() if v is not None or k in (
        "leilao_extra_json",
        "edital_metadados_json",
        "cache_media_bairro_ids",
        "edital_markdown",
        "ultima_ingestao_log_text",
        "status",
        "url_leilao",
    )}


def executar_ingestao_edital(
    url: str,
    client: Client,
    *,
    sobrescrever_duplicata: Optional[bool] = None,
    ignorar_cache_firecrawl: bool = False,
) -> ResultadoIngestaoEdital:
    """
    `sobrescrever_duplicata`:
      - None: se existir duplicata, levanta `EscolhaSobreDuplicataNecessaria`.
      - True: atualiza o registro existente.
      - False: não altera o banco (retorna ignorado_duplicata).
    """
    url = normalizar_url_leilao(url)
    log_parts: list[str] = [f"Início ingestão url={url}"]

    existente = leilao_imoveis_repo.buscar_por_url_leilao(url, client)
    if existente and sobrescrever_duplicata is not True:
        log_parts.append(f"Duplicata detectada id={existente.get('id')}")
        if sobrescrever_duplicata is None:
            raise EscolhaSobreDuplicataNecessaria(existente)
        log_parts.append("Usuário optou por não sobrescrever — sem alterações no banco.")
        return ResultadoIngestaoEdital(
            modo="ignorado_duplicata",
            id=str(existente.get("id") or ""),
            url_leilao=url,
            log=_montar_log_linhas(*log_parts),
            pos_comparaveis={},
            pos_cache={},
            firecrawl_chamadas_api_total=0,
        )

    try:
        markdown, fc_meta = firecrawl_edital.scrape_url_markdown(
            url, ignorar_cache=ignorar_cache_firecrawl
        )
    except (RuntimeError, ValueError, TimeoutError) as e:
        logger.exception("Falha no scrape Firecrawl/cache")
        log_parts.append(f"ERRO scrape: {e}")
        log_parts.append(traceback.format_exc())
        raise UrlInvalidaIngestaoError(str(e)) from e
    except Exception as e:
        logger.exception("Falha inesperada no scrape Firecrawl/cache")
        log_parts.append("ERRO scrape inesperado (ver log técnico).")
        log_parts.append(traceback.format_exc())
        raise UrlInvalidaIngestaoError(
            "Falha interna ao obter conteúdo do edital (ver log técnico)."
        ) from e

    log_parts.append(f"Markdown obtido: {len(markdown)} caracteres (fonte={fc_meta.get('fonte')})")

    try:
        diag_md = validar_markdown_antes_da_extracao(markdown)
    except IngestaoSemConteudoEditalError as e:
        logger.info("Markdown rejeitado antes da extração LLM: %s", e.motivo)
        log_parts.append(f"Pré-validação markdown: REJEITADO — {e.motivo}")
        raise
    log_parts.append(
        "Pré-validação markdown: OK "
        f"(indícios_edital={len(diag_md.indicios_encontrados)}: "
        f"{','.join(sorted(diag_md.indicios_encontrados))})"
    )

    try:
        ext, metricas = extracao_edital_llm.extrair_edital_de_markdown(markdown, url)
    except (RuntimeError, ValueError) as e:
        logger.exception("Falha na extração LLM")
        log_parts.append(f"ERRO LLM: {e}")
        raise UrlInvalidaIngestaoError(f"Extração do edital falhou: {e}") from e
    except Exception as e:
        logger.exception("Falha inesperada na extração LLM")
        log_parts.append("ERRO LLM inesperado (ver log técnico).")
        raise UrlInvalidaIngestaoError(
            "Extração do edital falhou por erro interno (ver log técnico)."
        ) from e

    if (ext.url_leilao or "").strip() != url.strip():
        ext = ext.model_copy(update={"url_leilao": url})

    foto_md = extrair_url_foto_imovel_markdown(markdown, url)
    foto_llm = (ext.url_foto_imovel or "").strip()
    if foto_llm.startswith(("http://", "https://")):
        pass
    elif foto_md:
        ext = ext.model_copy(update={"url_foto_imovel": foto_md})

    extn = _extracao_normalizada(ext)
    coords = _buscar_coordenadas_extracao(extn)
    if coords:
        log_parts.append(f"Geocodificação: latitude={coords[0]:.6f} longitude={coords[1]:.6f}")
    else:
        log_parts.append("Geocodificação: coordenadas não obtidas (endereço incompleto ou Nominatim sem hit).")

    res_limpeza = limpar_edital_markdown_ruido(markdown)
    markdown_para_bd = res_limpeza.texto
    if res_limpeza.cortes_aplicados:
        log_parts.append(
            "Limpeza markdown (cross-sell/rodapé): cortes=%s, removidos=%s chars, restantes=%s"
            % (
                ",".join(res_limpeza.cortes_aplicados),
                res_limpeza.removidos_caracteres,
                res_limpeza.caracteres_depois,
            )
        )
    else:
        log_parts.append("Limpeza markdown: nenhum ancoramento de ruído encontrado.")

    fc_meta_gravacao = dict(fc_meta)
    fc_meta_gravacao["markdown_limpeza"] = {
        "cortes": res_limpeza.cortes_aplicados,
        "caracteres_antes": res_limpeza.caracteres_antes,
        "caracteres_depois": res_limpeza.caracteres_depois,
    }

    log_text = _montar_log_linhas(*log_parts, f"Tokens: {metricas}")
    payload = montar_payload_gravacao(
        ext,
        url=url,
        markdown_bruto=markdown_para_bd,
        edital_metadados=fc_meta_gravacao,
        metricas_llm=metricas,
        log_text=log_text,
        latitude=coords[0] if coords else None,
        longitude=coords[1] if coords else None,
    )

    alvo: Optional[dict] = existente
    insercao_row: Optional[dict] = None
    if alvo is None:
        try:
            insercao_row = leilao_imoveis_repo.inserir_leilao_imovel(payload, client)
        except APIError as e:
            if not _eh_violacao_unique_url_insert(e):
                raise
            alvo = leilao_imoveis_repo.buscar_por_url_leilao(url, client)
            if alvo is None:
                raise
            if sobrescrever_duplicata is None:
                raise EscolhaSobreDuplicataNecessaria(alvo) from e
            insercao_row = None
    if insercao_row is not None:
        imovel_id = str(insercao_row.get("id") or "")
        log_parts.append(f"Inserido id={imovel_id}")
        modo_out: Literal["inserido", "atualizado", "ignorado_duplicata"] = "inserido"
    elif alvo is not None:
        iid = str(alvo["id"])
        prev_cache = list(alvo.get("cache_media_bairro_ids") or [])
        if prev_cache:
            payload["cache_media_bairro_ids"] = prev_cache
        leilao_imoveis_repo.atualizar_leilao_imovel(iid, payload, client)
        log_parts.append(f"Registro atualizado id={iid}")
        imovel_id = iid
        modo_out = "atualizado"
    else:
        raise RuntimeError("gravação do leilão: insert não retornou linha e não há registro a atualizar")

    summ: dict[str, Any] = {}
    pos_cache: dict[str, Any] = {}
    n_fc_edital = 0
    if bool(fc_meta.get("consumiu_credito_api")) or str(fc_meta.get("fonte") or "").strip().lower() == "firecrawl":
        n_fc_edital = 1

    final_log = _montar_log_linhas(*log_parts, f"Tokens: {metricas}")
    if imovel_id:
        budget_fc = int(get_busca_mercado_parametros().max_firecrawl_creditos_analise)
        restante_fc = max(0, budget_fc - int(n_fc_edital))
        try:
            summ = comparaveis_pos_ingestao.executar_comparaveis_apos_ingestao_leilao(
                client,
                leilao_imovel_id=imovel_id,
                extn=extn,
                ignorar_cache_firecrawl=ignorar_cache_firecrawl,
                max_chamadas_api_firecrawl=restante_fc,
            )
        except Exception:
            logger.exception("Pós-ingestão comparáveis (Firecrawl Search)")
            summ = {"ok": False, "erro": "excecao_ver_log"}

        log_parts.append(comparaveis_pos_ingestao.formatar_log_pos_ingestao(summ))

        if summ.get("aguarda_confirmacao_frase"):
            try:
                import streamlit as st

                st.session_state["fc_pendente_pos_ingest"] = {
                    "leilao_imovel_id": str(imovel_id),
                    "frase_proposta": str(summ.get("frase_proposta") or ""),
                    "restante_fc_antes_comparaveis": int(restante_fc),
                    "payload_comparaveis": dict(summ.get("payload_comparaveis") or {}),
                    "ignorar_cache_firecrawl": bool(ignorar_cache_firecrawl),
                }
            except Exception:
                pass
            log_parts.append(
                "Cache (automático): adiado — aguarda confirmação da frase de busca (Firecrawl) na aplicação."
            )
        else:
            restante_fc = max(0, restante_fc - int(summ.get("firecrawl_chamadas_api") or 0))
            try:
                cres = resolver_cache_media_pos_ingestao(
                    client,
                    imovel_id,
                    ignorar_cache_firecrawl=ignorar_cache_firecrawl,
                    max_chamadas_api_firecrawl=restante_fc,
                )
                pos_cache = {
                    "ok": cres.ok,
                    "mensagem": cres.mensagem,
                    "reutilizou_existente": cres.reutilizou_existente,
                    "usou_firecrawl_extra": cres.usou_firecrawl_extra,
                    "caches_criados": cres.caches_criados,
                    "firecrawl_chamadas_api": int(getattr(cres, "firecrawl_chamadas_api", 0) or 0),
                }
                log_parts.append(formatar_log_pos_cache(cres))
            except Exception:
                logger.exception("Cache (automático) pós-ingestão")
                pos_cache = {"ok": False, "erro": "excecao_ver_log", "firecrawl_chamadas_api": 0}
                log_parts.append("Cache (automático): falha — exceção (ver log).")

        final_log = _montar_log_linhas(*log_parts, f"Tokens: {metricas}")
        try:
            leilao_imoveis_repo.atualizar_leilao_imovel(
                imovel_id,
                {"ultima_ingestao_log_text": final_log},
                client,
            )
        except Exception:
            logger.exception("Atualização do log pós-ingestão")

    n_fc_total = n_fc_edital + int(summ.get("firecrawl_chamadas_api") or 0) + int(pos_cache.get("firecrawl_chamadas_api") or 0)

    return ResultadoIngestaoEdital(
        modo=modo_out,
        id=imovel_id,
        url_leilao=url,
        log=final_log,
        metricas_llm=metricas,
        pos_comparaveis=summ,
        pos_cache=pos_cache,
        firecrawl_chamadas_api_total=n_fc_total,
    )
