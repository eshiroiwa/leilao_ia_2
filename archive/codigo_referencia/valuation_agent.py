"""
Agente de valuation (Agno): lê imóveis `pendente` no Supabase, estima valor de mercado
via API (DataZap, quando configurada) ou busca web (OLX, ZAP, Viva Real, etc.),
aplica simulação de reforma com custo/m² ajustado pelo INCC-DI (BCB, referência ao SINAPI)
e atualiza o registro.

Schema `leilao_imoveis`: url_leilao, endereco, cidade, estado, bairro, data_leilao, area_util, area_total, quartos, vagas,
valor_arrematacao, valor_mercado_estimado, valor_venda_sugerido, custo_reforma_estimado, roi_projetado, status, created_at.
Status: pendente | analisado | aprovado | descartado_triagem. INCC/reformas não persistem metadados extras no BD.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import httpx
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field
from supabase import Client
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.tools import tool
from agno.tools.duckduckgo import DuckDuckGoTools

from ingestion_agent import (
    SUPABASE_TABLE,
    atualizar_leilao_imovel_campos,
    get_supabase_client,
)
from leilao_constants import (
    STATUS_ANALISADO,
    STATUS_PENDENTE,
    area_efetiva_de_registro,
    segmento_mercado_de_registro,
)
from token_efficiency import (
    buscar_media_bairro_no_cache,
    buscar_cache_media_bairro_supabase,
    salvar_cache_media_bairro_supabase,
)
from anuncios_mercado import (
    buscar_anuncios_similares_supabase,
    estatisticas_comparaveis,
    selecionar_top_comparaveis,
)

load_dotenv()

logger = logging.getLogger(__name__)

# INCC-DI — variação % mês a mês (custo da construção civil), SGS BCB 7462.
INCC_SGS_CODIGO_BCB = 7462
BCB_SGS_URL = (
    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{INCC_SGS_CODIGO_BCB}/dados/ultimos/{{n}}?formato=json"
)

def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        logger.warning("Variável %s inválida (%r). Usando default %s.", name, raw, default)
        return default


# Base de custo de reforma (R$/m²) — parametrizável via .env e reajustada pelo INCC.
CUSTO_M2_PADRAO_BAIXO = _env_float("REFORMA_CUSTO_M2_BAIXO", 500.0)
CUSTO_M2_PADRAO_MEDIO = _env_float("REFORMA_CUSTO_M2_MEDIO", 1000.0)
CUSTO_M2_PADRAO_ALTO = _env_float("REFORMA_CUSTO_M2_ALTO", 1500.0)
REFORMA_MESES_INCC_PADRAO = int(_env_float("REFORMA_MESES_INCC_PADRAO", 12))


class ImovelPendenteSnapshot(BaseModel):
    """Leitura mínima de um registro pendente para avaliação."""

    model_config = ConfigDict(extra="allow")

    id: str
    endereco: Optional[str] = None
    cidade: Optional[str] = None
    estado: Optional[str] = None
    bairro: Optional[str] = None
    data_leilao: Optional[str] = None
    tipo_imovel: Optional[str] = None
    conservacao: Optional[str] = None
    tipo_casa: Optional[str] = None
    andar: Optional[int] = None
    area_util: Optional[float] = None
    area_total: Optional[float] = None
    area_m2: Optional[float] = None  # alias legado em memória
    quartos: Optional[int] = None
    vagas: Optional[int] = None
    padrao_imovel: Optional[str] = Field(
        default="medio",
        description="baixo | medio | alto — define faixa de custo de reforma/m²",
    )
    valor_arrematacao: Optional[float] = None
    valor_lance_atual: Optional[str] = None
    url_leilao: Optional[str] = None
    status: Optional[str] = None


class SimulacaoReformaResultado(BaseModel):
    """Resultado da simulação de reforma com referência ao INCC (proxy de SINAPI/INCC)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    padrao_normalizado: str
    custo_m2_nominal: float = Field(..., description="R$/m² antes do fator INCC")
    area_m2: float = Field(..., gt=0)
    fator_incc_acumulado: float = Field(..., gt=0, description="Produto (1+var_mensal/100) no período")
    meses_incc: int
    custo_reforma_estimado: float = Field(..., ge=0)
    referencia_indices: dict[str, Any] = Field(
        default_factory=dict,
        description="Metadados INCC BCB; SINAPI citado como referência metodológica complementar",
    )


class AtualizacaoAvaliacao(BaseModel):
    """Payload validado; apenas colunas existentes no BD são gravadas."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: str
    valor_mercado_estimado: float = Field(..., gt=0)
    valor_venda_sugerido: Optional[float] = Field(
        default=None,
        gt=0,
        description="Opcional; se omitido, replica valor_mercado_estimado",
    )
    custo_reforma_estimado: Optional[float] = Field(default=None, ge=0)
    fator_incc_reforma: Optional[float] = Field(default=None, gt=0)
    referencia_indices_json: Optional[str] = None
    notas_avaliacao: Optional[str] = Field(default=None, max_length=8000)


def _normalizar_padrao(p: Optional[str]) -> str:
    if not p:
        return "medio"
    x = p.strip().lower()
    if x in ("médio", "medio", "medium", "m"):
        return "medio"
    if x in ("baixo", "low", "b"):
        return "baixo"
    if x in ("alto", "high", "a"):
        return "alto"
    return "medio"


def _custo_m2_por_padrao(padrao: str) -> float:
    p = _normalizar_padrao(padrao)
    if p == "baixo":
        return CUSTO_M2_PADRAO_BAIXO
    if p == "alto":
        return CUSTO_M2_PADRAO_ALTO
    return CUSTO_M2_PADRAO_MEDIO


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=25),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _http_get_json(url: str) -> Any:
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()


def obter_fator_incc_acumulado(meses: int = 12) -> tuple[float, dict[str, Any]]:
    """
    Acumula (1 + var%/100) do INCC-DI (BCB) nos últimos `meses` meses.
    Referência oficial de custo de construção; alinhado conceitualmente ao uso de SINAPI em orçamentos.
    """
    meses = max(1, min(meses, 24))
    url = BCB_SGS_URL.format(n=meses)
    logger.info("Consultando INCC-DI (SGS %s) últimos %s meses", INCC_SGS_CODIGO_BCB, meses)
    raw = _http_get_json(url)
    if not isinstance(raw, list) or not raw:
        raise ValueError("Resposta INCC inválida da API do BCB")

    fator = 1.0
    valores: list[dict[str, Any]] = []
    for item in raw:
        try:
            v = float(str(item["valor"]).replace(",", "."))
        except (KeyError, ValueError):
            continue
        fator *= 1.0 + v / 100.0
        valores.append({"data": item.get("data"), "variacao_pct": v})

    meta = {
        "indice_principal": "INCC-DI (var. % mensal)",
        "fonte": "Banco Central do Brasil — SGS",
        "codigo_serie_sgs": INCC_SGS_CODIGO_BCB,
        "meses_utilizados": len(valores),
        "fator_acumulado": round(fator, 6),
        "referencia_complementar": (
            "SINAPI (Caixa) compõe insumos típicos de reforma; aqui o reajuste usa INCC-DI "
            "como proxy de reposição de custo de obra no período."
        ),
        "amostra_variacoes": valores[-3:],
    }
    return fator, meta


def executar_simulacao_reforma(
    area_m2: float,
    padrao_imovel: str,
    meses_incc: int = REFORMA_MESES_INCC_PADRAO,
) -> SimulacaoReformaResultado:
    """Simulação de reforma: custo/m² conforme padrão, corrigido pelo acumulado do INCC."""
    if area_m2 <= 0:
        raise ValueError("area_m2 deve ser > 0 para simulação de reforma")

    padrao_n = _normalizar_padrao(padrao_imovel)
    custo_m2 = _custo_m2_por_padrao(padrao_n)
    fator, meta = obter_fator_incc_acumulado(meses_incc)
    custo_total = area_m2 * custo_m2 * fator

    return SimulacaoReformaResultado(
        padrao_normalizado=padrao_n,
        custo_m2_nominal=custo_m2,
        area_m2=area_m2,
        fator_incc_acumulado=fator,
        meses_incc=meta.get("meses_utilizados") or meses_incc,
        custo_reforma_estimado=round(custo_total, 2),
        referencia_indices=meta,
    )


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _supabase_select_pendentes(client: Client) -> list[dict[str, Any]]:
    resp = (
        client.table(SUPABASE_TABLE)
        .select("*")
        .eq("status", STATUS_PENDENTE)
        .execute()
    )
    return list(getattr(resp, "data", None) or [])


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _supabase_update_avaliacao(client: Client, imovel_id: str, payload: dict[str, Any]) -> Any:
    return client.table(SUPABASE_TABLE).update(payload).eq("id", imovel_id).execute()


def listar_imoveis_pendentes(client: Optional[Client] = None) -> list[dict[str, Any]]:
    cli = client or get_supabase_client()
    rows = _supabase_select_pendentes(cli)
    logger.info("Imóveis pendentes: %d", len(rows))
    return rows


def persistir_avaliacao_tecnica_no_registro(
    imovel_id: str,
    valor_mercado_estimado: float,
    sim: SimulacaoReformaResultado,
    notas_avaliacao: str,
    valor_maximo_regiao_estimado: Optional[float] = None,
    valor_teto_regiao_agressivo: Optional[float] = None,
    potencial_reposicionamento_pct: Optional[float] = None,
    alerta_precificacao_baixa_amostragem: Optional[bool] = None,
    cache_media_bairro_id: Optional[str] = None,
    client: Optional[Client] = None,
) -> None:
    """Grava valor de mercado, valor de venda sugerido, custo de reforma e status analisado."""
    _ = notas_avaliacao  # sem coluna de observações no BD; útil para logs futuros
    payload: dict[str, Any] = {
        "status": STATUS_ANALISADO,
        "valor_mercado_estimado": valor_mercado_estimado,
        "valor_venda_sugerido": valor_mercado_estimado,
        "custo_reforma_estimado": sim.custo_reforma_estimado,
    }
    if valor_maximo_regiao_estimado is not None:
        payload["valor_maximo_regiao_estimado"] = valor_maximo_regiao_estimado
    if valor_teto_regiao_agressivo is not None:
        payload["valor_teto_regiao_agressivo"] = valor_teto_regiao_agressivo
    if potencial_reposicionamento_pct is not None:
        payload["potencial_reposicionamento_pct"] = potencial_reposicionamento_pct
    if alerta_precificacao_baixa_amostragem is not None:
        payload["alerta_precificacao_baixa_amostragem"] = bool(alerta_precificacao_baixa_amostragem)
    if cache_media_bairro_id:
        x = str(cache_media_bairro_id).strip()
        if x:
            payload["cache_media_bairro_id"] = x
    while True:
        try:
            atualizar_leilao_imovel_campos(imovel_id, payload, client=client)
            return
        except Exception as exc:
            msg = str(exc).lower()
            removido = False
            for col in ("alerta_precificacao_baixa_amostragem", "cache_media_bairro_id"):
                if (
                    col in payload
                    and col in msg
                    and ("does not exist" in msg or "column" in msg)
                ):
                    logger.warning("Coluna %s ausente no Supabase; gravando sem o campo", col)
                    payload.pop(col, None)
                    removido = True
            if not removido:
                raise


def avaliar_imovel_por_cache_media_bairro(
    row: dict[str, Any],
    *,
    fator_prudencia: float = 0.92,
    max_idade_cache_dias: int = 90,
    confianca_minima_cache: float = 55.0,
    client: Optional[Client] = None,
) -> dict[str, Any]:
    """
    Sem LLM: usa `preco_m2_medio` do cache Supabase (cidade+bairro) × área × fator de prudência,
    simula reforma (INCC) e persiste. Retorna skip se faltar dado ou cache.
    """
    iid = row.get("id")
    if not iid:
        return {"status": "skip", "motivo": "sem_id"}
    cidade = (row.get("cidade") or "").strip()
    estado = (row.get("estado") or "").strip()
    bairro = (row.get("bairro") or "").strip()
    if not cidade:
        return {"status": "skip", "motivo": "sem_cidade"}
    if not bairro and not estado:
        return {"status": "skip", "motivo": "sem_bairro_nem_estado_para_cache"}

    area_f = area_efetiva_de_registro(row)
    if area_f <= 0:
        return {"status": "skip", "motivo": "sem_area_util"}

    cache = buscar_media_bairro_no_cache(
        cidade,
        bairro,
        estado=estado,
        registro=row,
        client=client,
        max_idade_dias=max_idade_cache_dias,
    )
    if not cache or cache.get("preco_m2_medio") is None:
        return {"status": "skip", "motivo": "cache_bairro_indisponivel"}

    md: dict[str, Any] = {}
    raw_md = cache.get("metadados_json")
    if isinstance(raw_md, str):
        try:
            md = json.loads(raw_md)
        except json.JSONDecodeError:
            md = {}
    elif isinstance(raw_md, dict):
        md = raw_md
    conf = md.get("confianca_score")
    try:
        conf_v = float(conf) if conf is not None else None
    except (TypeError, ValueError):
        conf_v = None
    amostragem_baixa = bool(md.get("amostragem_baixa") is True)
    if not amostragem_baixa:
        try:
            n_amostras_md = int(float(md.get("n_amostras"))) if md.get("n_amostras") is not None else None
            n_min_md = (
                int(float(md.get("amostragem_minima_recomendada")))
                if md.get("amostragem_minima_recomendada") is not None
                else None
            )
            if n_amostras_md is not None and n_min_md is not None and n_min_md > 0:
                amostragem_baixa = n_amostras_md < n_min_md
        except (TypeError, ValueError):
            pass
    if conf_v is not None and conf_v < float(confianca_minima_cache):
        return {
            "status": "skip",
            "motivo": "cache_baixa_confianca",
            "confianca_score": conf_v,
            "confianca_minima": float(confianca_minima_cache),
        }

    cache_row_id = cache.get("id")
    cache_row_id_str = str(cache_row_id).strip() if cache_row_id is not None else ""

    fb = bool(cache.pop("_cache_fallback_geografico", False))
    gran = cache.pop("_cache_granularidade_match", None)
    pm2 = float(cache["preco_m2_medio"])
    valor_mercado = round(pm2 * area_f * fator_prudencia, 2)
    pm2_p90 = None
    pm2_max = None
    try:
        pm2_p90 = float(md.get("pm2_p90")) if md.get("pm2_p90") is not None else None
    except (TypeError, ValueError):
        pm2_p90 = None
    try:
        pm2_max = float(md.get("pm2_max")) if md.get("pm2_max") is not None else None
    except (TypeError, ValueError):
        pm2_max = None
    if pm2_p90 is None or pm2_max is None:
        # Fallback: se cache legado não tem P90/MAX, recomputa a partir de anúncios similares atuais.
        try:
            seg = segmento_mercado_de_registro(row)
            anuncios = buscar_anuncios_similares_supabase(
                cidade=cidade,
                estado=estado,
                bairro=bairro,
                tipo_imovel_norm=seg["tipo_imovel"],
                client=client or get_supabase_client(),
            )
            comparaveis, _ = selecionar_top_comparaveis(
                anuncios,
                row_referencia=row,
                min_comparaveis=5,
            )
            st = estatisticas_comparaveis(comparaveis)
            if pm2_p90 is None and st.get("pm2_p90") is not None:
                pm2_p90 = float(st["pm2_p90"])
            if pm2_max is None and st.get("pm2_max") is not None:
                pm2_max = float(st["pm2_max"])
        except Exception:
            logger.exception("Não foi possível recalcular teto regional a partir de anúncios para id=%s", iid)
    valor_maximo_regiao_estimado = round(pm2_p90 * area_f, 2) if pm2_p90 and pm2_p90 > 0 else None
    valor_teto_regiao_agressivo = round(pm2_max * area_f, 2) if pm2_max and pm2_max > 0 else None
    potencial_reposicionamento_pct = None
    if valor_maximo_regiao_estimado and valor_mercado > 0:
        potencial_reposicionamento_pct = round(
            ((valor_maximo_regiao_estimado - valor_mercado) / valor_mercado) * 100.0,
            2,
        )
    padrao = row.get("padrao_imovel") or "medio"
    sim = executar_simulacao_reforma(area_f, str(padrao), meses_incc=REFORMA_MESES_INCC_PADRAO)
    notas = (
        f"Avaliação automática (cache segmentado): média R$/m² ref. {pm2:.2f} × {area_f} m² × "
        f"prudência {fator_prudencia}. Fonte: {cache.get('fonte', 'n/d')}."
        + (" [referência: fallback só geografia/bairro — granularidade limitada]" if fb else "")
    )
    persistir_avaliacao_tecnica_no_registro(
        str(iid),
        valor_mercado,
        sim,
        notas,
        valor_maximo_regiao_estimado=valor_maximo_regiao_estimado,
        valor_teto_regiao_agressivo=valor_teto_regiao_agressivo,
        potencial_reposicionamento_pct=potencial_reposicionamento_pct,
        alerta_precificacao_baixa_amostragem=amostragem_baixa,
        cache_media_bairro_id=cache_row_id_str or None,
        client=client,
    )
    return {
        "status": "ok_cache",
        "id": str(iid),
        "valor_mercado_estimado": valor_mercado,
        "confianca_score": conf_v,
        "confianca_nivel": md.get("confianca_nivel"),
        "origem_confianca": "cache",
        "cache_granularidade_match": gran,
        "valor_maximo_regiao_estimado": valor_maximo_regiao_estimado,
        "valor_teto_regiao_agressivo": valor_teto_regiao_agressivo,
        "potencial_reposicionamento_pct": potencial_reposicionamento_pct,
        "alerta_precificacao_baixa_amostragem": amostragem_baixa,
    }


@tool(show_result=True)
def listar_imoveis_pendentes_supabase() -> str:
    """Lista todos os registros da tabela de imóveis com status 'pendente' (campos completos em JSON)."""
    try:
        rows = listar_imoveis_pendentes()
        return json.dumps(rows, ensure_ascii=False, default=str)
    except Exception as e:
        logger.exception("Falha ao listar pendentes")
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


@tool(
    show_result=True,
    instructions=(
        "Chame antes da busca web se DATAZAP_API_URL e DATAZAP_API_KEY estiverem no .env. "
        "Envie JSON com endereco, area_util, quartos, vagas, tipo (ex.: apartamento)."
    ),
)
def consultar_referencia_mercado_datazap(caracteristicas_json: str) -> str:
    """
    Requisição HTTP à API DataZap (quando configurada).
    Variáveis: DATAZAP_API_URL (POST), DATAZAP_API_KEY (Bearer), opcional DATAZAP_TIMEOUT.
    """
    base = os.getenv("DATAZAP_API_URL", "").strip()
    key = os.getenv("DATAZAP_API_KEY", "").strip()
    if not base or not key:
        return json.dumps(
            {
                "api_disponivel": False,
                "mensagem": "Defina DATAZAP_API_URL e DATAZAP_API_KEY para usar a API; caso contrário use busca web.",
            },
            ensure_ascii=False,
        )
    try:
        body = json.loads(caracteristicas_json)
    except json.JSONDecodeError as e:
        return json.dumps({"erro": "json_invalido", "detail": str(e)}, ensure_ascii=False)

    timeout = float(os.getenv("DATAZAP_TIMEOUT", "45"))
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(base, headers=headers, json=body)
            r.raise_for_status()
            out = r.json()
        logger.info("DataZap OK para corpo com chaves: %s", list(body.keys())[:12])
        return json.dumps({"api_disponivel": True, "resposta": out}, ensure_ascii=False, default=str)
    except Exception as e:
        logger.exception("Falha na chamada DataZap")
        return json.dumps(
            {"api_disponivel": True, "erro": str(e)},
            ensure_ascii=False,
        )


@tool(show_result=True)
def simular_reforma_incc(
    area_util: float,
    padrao_imovel: str,
    meses_incc: int = REFORMA_MESES_INCC_PADRAO,
) -> str:
    """
    Calcula custo de reforma estimado usando custo/m² parametrizado no .env por padrão (baixo/médio/alto),
    multiplicado pelo fator acumulado do INCC-DI (BCB) nos últimos meses (referência análoga ao SINAPI).
    """
    try:
        sim = executar_simulacao_reforma(area_util, padrao_imovel, meses_incc=meses_incc)
        return sim.model_dump_json(ensure_ascii=False)
    except Exception as e:
        logger.exception("Simulação de reforma falhou")
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


@tool(
    show_result=True,
    instructions=(
        "Após estimar valor_mercado_estimado (API ou buscas em OLX, ZAP, Viva Real, ImovelWeb), "
        "chame simular_reforma_incc com area_util e padrao_imovel do registro e depois esta tool "
        "com id, valor_mercado_estimado, custo_reforma_estimado e fator vindos da simulação."
    ),
)
def atualizar_avaliacao_imovel_supabase(payload_json: str) -> str:
    """
    Atualiza no BD: valor_mercado_estimado, custo_reforma_estimado, status analisado.
    JSON aceita também fator_incc_reforma / referencia_indices_json / notas (ignorados — sem colunas).
    """
    try:
        data = json.loads(payload_json)
        upd = AtualizacaoAvaliacao.model_validate(data)
    except Exception as e:
        logger.exception("Payload de avaliação inválido")
        return json.dumps({"status": "error", "detail": str(e)}, ensure_ascii=False)

    venda = upd.valor_venda_sugerido if upd.valor_venda_sugerido is not None else upd.valor_mercado_estimado
    row: dict[str, Any] = {
        "status": STATUS_ANALISADO,
        "valor_mercado_estimado": upd.valor_mercado_estimado,
        "valor_venda_sugerido": venda,
    }
    if upd.custo_reforma_estimado is not None:
        row["custo_reforma_estimado"] = upd.custo_reforma_estimado

    try:
        cli = get_supabase_client()
        _supabase_update_avaliacao(cli, upd.id, row)
        logger.info("Avaliação gravada para id=%s", upd.id)
        return json.dumps(
            {"status": "ok", "id": upd.id, "campos_atualizados": list(row.keys())},
            ensure_ascii=False,
        )
    except Exception as e:
        logger.exception("Falha ao atualizar Supabase")
        return json.dumps({"status": "error", "detail": str(e)}, ensure_ascii=False)


def create_valuation_agent(
    *,
    model_id: Optional[str] = None,
    markdown: bool = True,
    max_resultados_busca: int = 8,
) -> Agent:
    """Agente Agno: API DataZap (opcional), busca web DDGS e tools de reforma + persistência."""
    mid = model_id or os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
    busca = DuckDuckGoTools(
        enable_search=True,
        enable_news=False,
        fixed_max_results=max_resultados_busca,
        timeout=25,
        region="br-pt",
    )
    return Agent(
        model=OpenAIChat(id=mid),
        tools=[
            busca,
            listar_imoveis_pendentes_supabase,
            buscar_cache_media_bairro_supabase,
            salvar_cache_media_bairro_supabase,
            consultar_referencia_mercado_datazap,
            simular_reforma_incc,
            atualizar_avaliacao_imovel_supabase,
        ],
        instructions=(
            "Você avalia imóveis de leilão já cadastrados. "
            "Eficiência de tokens: para cada cidade+bairro, chame PRIMEIRO buscar_cache_media_bairro_supabase; "
            "se hit=true, use preco_m2_medio do cache e NÃO chame DataZap de novo para o mesmo bairro. "
            "Só consultar_referencia_mercado_datazap (ou busca web) quando o cache faltar ou estiver obsoleto; "
            "depois salve a média obtida com salvar_cache_media_bairro_supabase para os próximos lotes. "
            "1) Liste pendentes com listar_imoveis_pendentes_supabase. "
            "2) Para cada um: cache → API (se necessário) → web se preciso; estime valor_mercado_estimado (notas). "
            "3) Rode simular_reforma_incc com area_util e padrao_imovel do registro (default médio se ausente). "
            "4) atualizar_avaliacao_imovel_supabase com JSON: id, valor_mercado_estimado, custo_reforma_estimado. "
            "Responda em português, de forma objetiva."
        ),
        markdown=markdown,
    )


__all__ = [
    "AtualizacaoAvaliacao",
    "ImovelPendenteSnapshot",
    "SimulacaoReformaResultado",
    "STATUS_ANALISADO",
    "STATUS_PENDENTE",
    "atualizar_avaliacao_imovel_supabase",
    "consultar_referencia_mercado_datazap",
    "create_valuation_agent",
    "executar_simulacao_reforma",
    "listar_imoveis_pendentes",
    "listar_imoveis_pendentes_supabase",
    "obter_fator_incc_acumulado",
    "simular_reforma_incc",
    "buscar_cache_media_bairro_supabase",
    "salvar_cache_media_bairro_supabase",
    "avaliar_imovel_por_cache_media_bairro",
    "persistir_avaliacao_tecnica_no_registro",
]
