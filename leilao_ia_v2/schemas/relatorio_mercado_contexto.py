"""Documento persistido em ``leilao_imoveis.relatorio_mercado_contexto_json``."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


RELATORIO_MERCADO_CONTEXTO_VERSAO = 1

# IDs fixos exigidos do modelo (ordem de exibição no relatório).
CARD_IDS_ORDEM: tuple[str, ...] = (
    "populacao",
    "perfil_urbano",
    "centralidade",
    "classe_renda",
    "seguranca",
    "procura_imoveis",
    "bairros_concorrentes",
    "condominios_fechados",
    "volume_anuncios",
    "ajuste_imovel_bairro",
)

CARD_TITULOS_PADRAO: dict[str, str] = {
    "populacao": "População (cidade e bairro, se aplicável)",
    "perfil_urbano": "Perfil do bairro",
    "centralidade": "Localização (central x periférico)",
    "classe_renda": "Classe de renda",
    "seguranca": "Segurança",
    "procura_imoveis": "Procura por imóveis",
    "bairros_concorrentes": "Bairros concorrentes",
    "condominios_fechados": "Condomínios fechados de casas",
    "volume_anuncios": "Volume de anúncios e concorrência",
    "ajuste_imovel_bairro": "Ajuste imóvel × bairro e liquidez",
}


class RelatorioMercadoCard(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    id: str
    titulo: str = ""
    topicos: list[str] = Field(default_factory=list)
    evidencia: str = ""


class RelatorioMercadoSinaisDecisao(BaseModel):
    model_config = ConfigDict(extra="ignore")

    liquidez_bairro: int = Field(50, ge=0, le=100)
    pressao_concorrencia: int = Field(50, ge=0, le=100)
    fit_imovel_bairro: int = Field(50, ge=0, le=100)
    resumo: str = ""


class RelatorioMercadoQualidade(BaseModel):
    model_config = ConfigDict(extra="ignore")

    score_qualidade: int = Field(0, ge=0, le=100)
    n_amostras_cache: int = 0
    n_anuncios_resolvidos: int = 0
    pct_mesmo_bairro: float = 0.0
    pct_geo_valida: float = 0.0
    notas: list[str] = Field(default_factory=list)


class RelatorioMercadoValidade(BaseModel):
    model_config = ConfigDict(extra="ignore")

    ttl_horas: int = 168
    expirado: bool = False
    horas_desde_geracao: float = 0.0
    motivo: str = ""
    cache_principal_id: str = ""
    assinatura_cache_principal: str = ""


class RelatorioMercadoContextoDocumento(BaseModel):
    model_config = ConfigDict(extra="ignore")

    versao: int = RELATORIO_MERCADO_CONTEXTO_VERSAO
    gerado_em_iso: str = ""
    modelo: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    custo_usd_estimado: float = 0.0
    cards: list[RelatorioMercadoCard] = Field(default_factory=list)
    disclaimer: str = Field(
        default="",
        description="Aviso curto sobre natureza aproximada das inferências.",
    )
    sinais_decisao: RelatorioMercadoSinaisDecisao = Field(default_factory=RelatorioMercadoSinaisDecisao)
    qualidade: RelatorioMercadoQualidade = Field(default_factory=RelatorioMercadoQualidade)
    validade: RelatorioMercadoValidade = Field(default_factory=RelatorioMercadoValidade)
    insights_oportunidade: list[str] = Field(default_factory=list)
    insights_risco: list[str] = Field(default_factory=list)
    checklist_diligencia: list[str] = Field(default_factory=list)
    dados_populacao_cidade: list[str] = Field(default_factory=list)
    informacoes_bairro: list[str] = Field(default_factory=list)
    contexto_minimo: list[str] = Field(default_factory=list)
    estrategia_sugerida: str = ""
    tese_acao: str = ""


def normalizar_documento_mercado(raw: Any) -> RelatorioMercadoContextoDocumento:
    """Valida e completa cards faltantes com tópicos vazios."""
    if not isinstance(raw, dict) or not raw:
        return RelatorioMercadoContextoDocumento()
    # Tolerância para respostas de LLM parcialmente fora do schema:
    # se vier card sem `id` (ex.: apenas `title/topicos`), descartamos
    # o item inválido para não abortar toda a análise.
    raw_sanit = dict(raw)
    cards_raw = raw_sanit.get("cards")
    if isinstance(cards_raw, list):
        cards_ok: list[dict[str, Any]] = []
        for c in cards_raw:
            if not isinstance(c, dict):
                continue
            cid = str(c.get("id") or "").strip()
            if not cid:
                continue
            cards_ok.append(c)
        raw_sanit["cards"] = cards_ok
    doc = RelatorioMercadoContextoDocumento.model_validate(raw_sanit)
    por_id = {c.id: c for c in doc.cards}
    out_cards: list[RelatorioMercadoCard] = []
    for cid in CARD_IDS_ORDEM:
        c = por_id.get(cid)
        if c is None:
            tit = CARD_TITULOS_PADRAO.get(cid, cid.replace("_", " ").title())
            out_cards.append(RelatorioMercadoCard(id=cid, titulo=tit, topicos=[], evidencia=""))
        else:
            tit = (c.titulo or "").strip() or CARD_TITULOS_PADRAO.get(cid, cid)
            out_cards.append(
                RelatorioMercadoCard(
                    id=cid,
                    titulo=tit,
                    topicos=list(c.topicos or [])[:14],
                    evidencia=str(getattr(c, "evidencia", "") or "").strip(),
                )
            )
    insights_oportunidade = [str(x).strip() for x in (doc.insights_oportunidade or []) if str(x).strip()][:8]
    insights_risco = [str(x).strip() for x in (doc.insights_risco or []) if str(x).strip()][:8]
    checklist_diligencia = [str(x).strip() for x in (doc.checklist_diligencia or []) if str(x).strip()][:10]
    dados_populacao_cidade = [str(x).strip() for x in (doc.dados_populacao_cidade or []) if str(x).strip()][:6]
    informacoes_bairro = [str(x).strip() for x in (doc.informacoes_bairro or []) if str(x).strip()][:8]
    contexto_minimo = [str(x).strip() for x in (doc.contexto_minimo or []) if str(x).strip()][:6]
    estrategia_sugerida = str(doc.estrategia_sugerida or "").strip()[:220]
    tese_acao = str(doc.tese_acao or "").strip()[:900]
    return doc.model_copy(
        update={
            "cards": out_cards,
            "insights_oportunidade": insights_oportunidade,
            "insights_risco": insights_risco,
            "checklist_diligencia": checklist_diligencia,
            "dados_populacao_cidade": dados_populacao_cidade,
            "informacoes_bairro": informacoes_bairro,
            "contexto_minimo": contexto_minimo,
            "estrategia_sugerida": estrategia_sugerida,
            "tese_acao": tese_acao,
        }
    )


def parse_relatorio_mercado_contexto_json(raw: Any) -> RelatorioMercadoContextoDocumento:
    if not isinstance(raw, dict) or not raw:
        return RelatorioMercadoContextoDocumento()
    try:
        return normalizar_documento_mercado(raw)
    except Exception:
        return RelatorioMercadoContextoDocumento()
