"""
Após gravar um leilão (ingestão do edital): busca comparáveis via Firecrawl Search + scrape
e persiste em ``anuncios_mercado``.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

from supabase import Client

from leilao_ia_v2.config.busca_mercado_parametros import get_busca_mercado_parametros
from leilao_ia_v2.fc_search.query_builder import montar_frase_busca_mercado
from leilao_ia_v2.persistence import anuncios_mercado_repo, leilao_imoveis_repo
from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM
from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

logger = logging.getLogger(__name__)


def _listagem_bd_parece_suficiente_para_comparaveis(
    client: Client,
    *,
    cidade: str,
    estado_raw: str,
    tipo_imovel: str,
) -> bool:
    """
    Evita Firecrawl Search pós-ingestão quando já há volume de anúncios no BD para a mesma cidade/UF/tipo.

    Critério conservador: contagem na listagem SQL (sem raio), ≥ max(min_amostras_cache, 24).
    """
    cid = (cidade or "").strip()
    uf = estado_livre_para_sigla_uf(estado_raw) or str(estado_raw or "").strip()[:2].upper()
    tipo = (tipo_imovel or "apartamento").strip().lower()
    if not cid or len(uf) != 2:
        return False
    min_n = int(get_busca_mercado_parametros().min_amostras_cache)
    limiar = max(min_n, 24)
    rows = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
        client,
        cidade=cid,
        estado_sigla=uf,
        tipos_imovel=[tipo],
    )
    return len(rows) >= limiar


def formatar_log_pos_ingestao(resumo: dict[str, Any]) -> str:
    """Texto curto para anexar a ``ultima_ingestao_log_text``."""
    if resumo.get("aguarda_confirmacao_frase"):
        p = (resumo.get("frase_proposta") or "")[:200]
        return f"Comparáveis (Firecrawl): aguarda confirmação da frase na app — proposta (prévia)={p!r}"
    if resumo.get("omitido"):
        return f"Comparáveis (Firecrawl): omitido — {resumo.get('motivo', '')}"
    if not resumo.get("ok"):
        return f"Comparáveis (Firecrawl): falha — {resumo.get('erro', resumo)}"
    parts = [
        "Comparáveis (Firecrawl Search): OK",
        f"anuncios_salvos={resumo.get('anuncios_salvos', 0)}",
        f"url_listagem={str(resumo.get('url_listagem') or '')[:120]}",
        f"geocodificados={resumo.get('n_geocodificados', 0)}",
    ]
    if resumo.get("markdown_insuficiente"):
        parts.append("nenhum_card_extraido")
    nfc = int(resumo.get("firecrawl_chamadas_api") or 0)
    if nfc > 0:
        parts.append(f"firecrawl_api_calls_estimadas={nfc}")
    base = " | ".join(parts)
    diag = str(resumo.get("diagnostico_firecrawl_search") or "").strip()
    if diag:
        if len(diag) > 8000:
            diag = diag[:8000] + "\n… (diagnóstico truncado)"
        base = f"{base}\n--- Firecrawl Search (resumo) ---\n{diag}"
    return base


def executar_comparaveis_apos_ingestao_leilao(
    client: Client,
    *,
    leilao_imovel_id: str,
    extn: ExtracaoEditalLLM,
    ignorar_cache_firecrawl: bool = False,
    max_chamadas_api_firecrawl: int | None = None,
) -> dict[str, Any]:
    """
    Firecrawl Search (frase a partir do imóvel) + scrape de URLs de portais + upsert em ``anuncios_mercado``.

    ``max_chamadas_api_firecrawl``: teto de chamadas (search + scrapes) para esta etapa; ``None`` usa
    ``get_busca_mercado_parametros().max_firecrawl_creditos_analise``. Na ingestão completa, o pipeline
    passa o saldo restante após o scrape do edital.
    """
    cidade = (extn.cidade or "").strip()
    estado_raw = (extn.estado or "").strip()
    bairro = (extn.bairro or "").strip()

    if not cidade or not estado_raw:
        return {"ok": False, "omitido": True, "motivo": "sem_cidade_ou_estado"}

    if not os.getenv("FIRECRAWL_API_KEY", "").strip():
        return {"ok": False, "omitido": True, "motivo": "FIRECRAWL_API_KEY_ausente"}

    cap_fc = max_chamadas_api_firecrawl
    if cap_fc is None:
        cap_fc = int(get_busca_mercado_parametros().max_firecrawl_creditos_analise)
    if cap_fc <= 0:
        return {
            "ok": True,
            "omitido": True,
            "motivo": "firecrawl_orcamento_analise_esgotado",
            "anuncios_salvos": 0,
            "url_listagem": "",
            "n_geocodificados": 0,
            "markdown_insuficiente": False,
            "firecrawl_chamadas_api": 0,
            "diagnostico_firecrawl_search": (
                "Orçamento de chamadas API Firecrawl esgotado para esta etapa (edital já consumiu o teto da análise)."
            ),
        }

    tipo_imovel = (extn.tipo_imovel or "apartamento").strip().lower()
    if _listagem_bd_parece_suficiente_para_comparaveis(
        client,
        cidade=cidade,
        estado_raw=estado_raw,
        tipo_imovel=tipo_imovel,
    ):
        return {
            "ok": True,
            "omitido": True,
            "motivo": "bd_listagem_ja_com_volume_suficiente",
            "anuncios_salvos": 0,
            "url_listagem": "",
            "n_geocodificados": 0,
            "markdown_insuficiente": False,
            "firecrawl_chamadas_api": 0,
            "diagnostico_firecrawl_search": "",
        }
    area_ref = 0.0
    try:
        if extn.area_util is not None and float(extn.area_util) > 0:
            area_ref = float(extn.area_util)
        elif extn.area_total is not None and float(extn.area_total) > 0:
            area_ref = float(extn.area_total)
    except (TypeError, ValueError):
        area_ref = 0.0

    if get_busca_mercado_parametros().confirmar_frase_firecrawl_search:
        proposta = ""
        row_cf = leilao_imoveis_repo.buscar_por_id(str(leilao_imovel_id), client)
        if isinstance(row_cf, dict):
            try:
                proposta = (montar_frase_busca_mercado(row_cf, tipo_imovel) or "").strip()
            except Exception:
                logger.debug("montar_frase_busca_mercado (confirmação pós-ingestão)", exc_info=True)
        return {
            "ok": True,
            "omitido": True,
            "motivo": "aguarda_confirmacao_frase_firecrawl",
            "aguarda_confirmacao_frase": True,
            "frase_proposta": proposta,
            "payload_comparaveis": {
                "cidade": cidade,
                "estado_raw": estado_raw,
                "bairro": bairro,
                "tipo_imovel": tipo_imovel,
                "area_ref": float(area_ref or 0),
            },
            "anuncios_salvos": 0,
            "url_listagem": "",
            "n_geocodificados": 0,
            "markdown_insuficiente": True,
            "firecrawl_chamadas_api": 0,
            "diagnostico_firecrawl_search": "Confirme ou edite a frase na aplicação para executar o Firecrawl Search.",
        }

    try:
        from leilao_ia_v2.fc_search.pipeline import complementar_anuncios_firecrawl_search

        salvos_fc, diag_fc, n_api = complementar_anuncios_firecrawl_search(
            client,
            leilao_imovel_id=str(leilao_imovel_id),
            cidade=cidade,
            estado_raw=estado_raw,
            bairro=bairro,
            tipo_imovel=tipo_imovel,
            area_ref=float(area_ref or 0),
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
            max_chamadas_api=int(cap_fc),
        )
    except Exception:
        logger.exception("Pós-ingestão: Firecrawl Search (comparáveis)")
        return {
            "ok": False,
            "erro": "firecrawl_search_ver_log",
            "firecrawl_chamadas_api": 0,
        }

    return {
        "ok": True,
        "anuncios_salvos": int(salvos_fc or 0),
        "url_listagem": "firecrawl_search",
        "n_geocodificados": int(salvos_fc or 0),
        "markdown_insuficiente": not salvos_fc,
        "firecrawl_chamadas_api": int(n_api or 0),
        "diagnostico_firecrawl_search": str(diag_fc or "").strip(),
        "falha_por_filtros_persistencia": (
            int(salvos_fc or 0) == 0
            and bool(re.search(r"persistencia:\s*cards_recebidos=\d+", str(diag_fc or ""), re.I))
            and bool(re.search(r"descartes_total=(?!0)\\d+", str(diag_fc or ""), re.I))
        ),
    }
