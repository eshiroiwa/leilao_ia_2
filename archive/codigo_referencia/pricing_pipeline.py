"""
Orquestrador de precificação de leilões: integra ingestão, triagem heurística, cache de bairro,
avaliação (cache Python + LLM enxuto opcional), simulação de reforma (INCC), ROI e export Excel.

Desenho para **mínimo de tokens**:
- Playwright e triagem são 100% Python.
- Média de bairro: 1 leitura Supabase por bairro; DataZap/API só se cache miss (preenchido pelo fluxo ou manualmente).
- Anúncios (`anuncios_mercado`): **antes da web**, consulta comparáveis no Supabase; se poucos ou coleta
  com mais de 6 meses (configurável), busca DDGS e persiste só linhas com preço + m² + URL + endereço mínimos.
- Snippets DDGS: **fallback** se não houver anúncios persistidos (uma query por segmento no lote).
- LLM: **uma chamada curta por imóvel** que ainda estiver `pendente` após cache (JSON estrito), com modelo default `gpt-4o-mini`.

Uso programático:
    from pathlib import Path
    from pricing_pipeline import LeilaoPricingPipelineConfig, executar_pipeline_precificacao_leiloes

    rel = executar_pipeline_precificacao_leiloes(
        LeilaoPricingPipelineConfig(caminho_planilha=Path("lotes.xlsx"))
    )

Planilha: obrigatória coluna de URL (`url_leilao` | `url` | `link` | `link_leilao`).
Demais colunas são opcionais; nomes case-insensitive. Ver `ler_entradas_leilao_de_planilha`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Literal, Optional

import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.tools import tool

from financial_agent import (
    ParametrosFinanceirosGlobais,
    exportar_imoveis_roi_minimo_para_excel,
    processar_financeiro_imoveis_por_urls,
)
from leilao_constants import (
    STATUS_ANALISADO,
    STATUS_APROVADO,
    STATUS_DESCARTADO_TRIAGEM,
    STATUS_PENDENTE,
    area_efetiva_de_registro,
    normalizar_data_leilao_para_iso,
    segmento_mercado_de_registro,
)
from ingestion_agent import (
    SUPABASE_TABLE,
    atualizar_leilao_imovel_campos,
    get_supabase_client,
    ingerir_url_leilao,
)
from token_efficiency import (
    CacheMediaBairroSalvar,
    buscar_media_bairro_no_cache,
    merge_segmento_mercado,
    salvar_media_bairro_no_cache,
    triagem_heuristica_de_registro_leilao,
)
from anuncios_mercado import (
    resolver_bairro_para_vivareal,
    resolver_contexto_mercado_anuncios_detalhado,
    sincronizar_amostras_e_atualizar_cache_media_bairro,
)
from valuation_agent import (
    REFORMA_MESES_INCC_PADRAO,
    avaliar_imovel_por_cache_media_bairro,
    executar_simulacao_reforma,
    persistir_avaliacao_tecnica_no_registro,
)

load_dotenv()

logger = logging.getLogger(__name__)

try:
    from ddgs import DDGS
except ImportError:
    DDGS = None

try:
    from openai import BadRequestError, OpenAI
except ImportError:
    BadRequestError = None  # type: ignore[misc, assignment]
    OpenAI = None  # type: ignore[misc, assignment]


STATUSS_VALIDOS_PLANILHA = {
    STATUS_PENDENTE,
    STATUS_ANALISADO,
    STATUS_APROVADO,
    STATUS_DESCARTADO_TRIAGEM,
}


def _normalizar_status_planilha(raw_status: Any) -> Optional[str]:
    """
    O pipeline só avalia/financeiro em registros `pendente`.
    Para entradas de planilha, ignoramos status inesperado para não bloquear o processamento.
    """
    if raw_status is None:
        return None
    s = str(raw_status).strip().lower()
    if not s:
        return None
    if s in STATUSS_VALIDOS_PLANILHA:
        return s
    logger.warning("Status inválido na planilha (%r); usando padrão `pendente`.", raw_status)
    return STATUS_PENDENTE


def openai_chat_completions_create_compat(client: Any, **kwargs: Any) -> Any:
    """
    Compatibilidade entre modelos:
    - `max_completion_tokens` em vez de `max_tokens`;
    - alguns modelos não aceitam `temperature` ≠ padrão (omite o parâmetro);
    - alguns não aceitam `response_format` JSON mode (remove; o parser extrai JSON do texto).
    """
    if OpenAI is None:
        raise RuntimeError("Instale o pacote `openai`")
    if BadRequestError is None:
        return client.chat.completions.create(**kwargs)

    kw = dict(kwargs)
    last_exc: Optional[BaseException] = None
    for _ in range(8):
        try:
            return client.chat.completions.create(**kw)
        except BadRequestError as e:
            last_exc = e
            err = str(e).lower()
            changed = False
            if "max_tokens" in kw and (
                "max_completion_tokens" in err
                or ("unsupported" in err and "max_tokens" in err)
            ):
                mt = kw.pop("max_tokens", None)
                if mt is not None:
                    kw["max_completion_tokens"] = mt
                changed = True
            if "temperature" in kw and "temperature" in err and "unsupported" in err:
                kw.pop("temperature", None)
                changed = True
            if "response_format" in kw and (
                "response_format" in err or "json_object" in err
            ) and "unsupported" in err:
                kw.pop("response_format", None)
                changed = True
            if changed:
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("openai_chat_completions_create_compat: falha inesperada")


class LeilaoPricingPipelineConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    caminho_planilha: Path
    caminho_excel_roi_objetivo: Path = Field(
        default=Path("output/oportunidades_roi_objetivo.xlsx"),
        description="Excel filtrado por ROI mínimo",
    )
    roi_minimo_exportacao_pct: float = Field(default=25.0, ge=-100, le=500)
    fator_prudencia_cache: float = Field(default=0.92, gt=0, le=2.0)
    usar_triagem_preco_m2: bool = True
    usar_avaliacao_llm: bool = True
    limite_imoveis_llm_por_execucao: int = Field(
        default=0,
        ge=0,
        description="0 = ilimitado; use p.ex. 50 para testes",
    )
    delay_entre_scrapes_seg: float = Field(default=0.5, ge=0.0, le=120.0)
    cache_bairro_max_idade_dias: int = Field(default=90, ge=0)
    cache_fallback_geografico: bool = Field(
        default=True,
        description="Se não achar chave_segmento, usa a linha mais recente só por bairro (legado)",
    )
    modelo_llm: str = Field(default_factory=lambda: os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini"))
    usar_anuncios_mercado_supabase: bool = Field(
        default=True,
        description="Consulta anuncios_mercado antes da web; coleta DDGS se poucos ou dados velhos",
    )
    min_anuncios_mercado_similares: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Mínimo de amostras (web + BD) para o contexto; a coleta DDGS tenta persistir pelo menos este número",
    )
    ddgs_max_resultados_por_rodada: int = Field(
        default=28,
        ge=10,
        le=100,
        description="Quantidade de hits DDGS por rodada ao buscar anúncios similares",
    )
    ddgs_rodadas_max: int = Field(
        default=6,
        ge=1,
        le=20,
        description="Rodadas com a query principal antes das buscas alternativas mais amplas",
    )
    max_idade_anuncios_mercado_dias: int = Field(
        default=180,
        ge=1,
        description="Se o anúncio mais recente do conjunto similar for mais velho, dispara nova coleta",
    )
    fallback_snippets_ddgs_se_sem_anuncios: bool = Field(
        default=True,
        description="Se não houver texto de anúncios, usa trechos DDGS como antes",
    )
    sincronizar_amostras_mercado_antes_triagem: bool = Field(
        default=True,
        description=(
            "Garante amostras em anuncios_mercado e recalcula mediana em cache_media_bairro "
            "antes da triagem (independente de já existir cache)"
        ),
    )
    raio_similaridade_inicial_km: float = Field(
        default=3.0,
        ge=0.5,
        le=30.0,
        description="Raio inicial (km) para comparáveis; expande gradualmente se insuficiente",
    )
    raios_similaridade_expansao_km: tuple[float, ...] = Field(
        default=(5.0, 8.0, 12.0),
        description="Raios extras (km) usados em sequência quando 3km não tiver amostra suficiente",
    )
    confianca_minima_comparaveis: float = Field(
        default=55.0,
        ge=0.0,
        le=100.0,
        description="Confiança mínima dos comparáveis para aceitar valuation automático",
    )
    bloquear_llm_baixa_confianca: bool = Field(
        default=True,
        description="Se true, bloqueia avaliação LLM quando comparáveis estiverem com confiança baixa",
    )
    llm_max_tokens_avaliacao: int = Field(
        default_factory=lambda: int(os.getenv("OPENAI_LLM_MAX_TOKENS", "8192")),
        ge=256,
        le=128000,
        description="Limite de saída do modelo na avaliação; modelos com 'raciocínio' precisam de mais (evita finish_reason=length vazio)",
    )
    tempo_limite_execucao_seg: int = Field(
        default=0,
        ge=0,
        description="0 = sem limite; >0 interrompe o pipeline ao exceder o tempo total.",
    )
    abort_checker: Optional[Callable[[], bool]] = Field(
        default=None,
        exclude=True,
        repr=False,
        description="Callback opcional para interrupção externa (ex.: botão de abortar no frontend).",
    )
    progress_callback: Optional[Callable[[dict[str, Any]], None]] = Field(
        default=None,
        exclude=True,
        repr=False,
        description="Callback opcional de progresso para UI (fase/percentual/contadores).",
    )
    modo_entrada: Literal["planilha", "avulso"] = Field(
        default="planilha",
        description="Planilha: segue os demais itens se a listagem VivaReal falhar; avulso: interrompe o pipeline.",
    )


class LLMValorMercadoResposta(BaseModel):
    """Saída estruturada da avaliação LLM (uma chamada por imóvel)."""

    valor_mercado_estimado: float = Field(..., gt=0)
    notas_avaliacao: str = Field(default="", max_length=4000)
    resumo_mercado_regiao: str = Field(default="", max_length=3000)
    cenarios_reforma_estrategica: str = Field(default="", max_length=3000)
    tipo_imovel_inferido: str = Field(default="", max_length=40)
    conservacao_inferida: str = Field(default="", max_length=40)
    tipo_casa_inferida: str = Field(default="", max_length=40)
    faixa_andar_inferida: str = Field(default="", max_length=40)
    andar_inferido: Optional[int] = Field(default=None, ge=0)
    logradouro_chave_inferida: str = Field(default="", max_length=200)

    @field_validator("cenarios_reforma_estrategica", mode="before")
    @classmethod
    def _cenarios_reforma_para_str(cls, v: Any) -> str:
        """Aceita string ou lista (ex.: [{\"descricao\": \"...\"}]) que alguns modelos devolvem."""
        if v is None:
            return ""
        if isinstance(v, str):
            return v.strip()
        if isinstance(v, list):
            partes: list[str] = []
            for item in v:
                if isinstance(item, dict):
                    txt = item.get("descricao")
                    if txt is None:
                        txt = item.get("titulo") or item.get("nome")
                    partes.append(str(txt if txt is not None else item))
                else:
                    partes.append(str(item))
            return "\n".join(partes).strip()
        return str(v).strip()

    @field_validator(
        "notas_avaliacao",
        "resumo_mercado_regiao",
        "tipo_imovel_inferido",
        "conservacao_inferida",
        "tipo_casa_inferida",
        "faixa_andar_inferida",
        "logradouro_chave_inferida",
        mode="before",
    )
    @classmethod
    def _str_json_null_para_vazio(cls, v: Any) -> str:
        if v is None:
            return ""
        return v if isinstance(v, str) else str(v)

    @field_validator("andar_inferido", mode="before")
    @classmethod
    def _andar_opcional(cls, v: Any) -> Optional[int]:
        if v is None or v == "":
            return None
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _extrair_json_objeto_de_texto_llm(text: str) -> str:
    """Remove cercas ```json e isola o primeiro objeto `{...}` na resposta."""
    text = (text or "").strip()
    if not text:
        return ""
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
    if fence:
        text = fence.group(1).strip()
    i = text.find("{")
    if i < 0:
        return text
    depth = 0
    for j in range(i, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return text[i : j + 1]
    return text[i:]


def _conteudo_mensagem_openai(message: Any) -> str:
    """Texto útil da mensagem (content ou fallbacks)."""
    if message is None:
        return ""
    t = getattr(message, "content", None)
    if isinstance(t, str) and t.strip():
        return t
    for attr in ("refusal",):
        v = getattr(message, attr, None)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def _parse_resposta_llm_valor_mercado(content: str) -> LLMValorMercadoResposta:
    blob = _extrair_json_objeto_de_texto_llm(content)
    if not blob.strip():
        raise ValueError(
            "Resposta do modelo vazia ou sem objeto JSON. "
            "Tente outro modelo ou verifique se o prompt não foi bloqueado."
        )
    try:
        data = json.loads(blob)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido na resposta do modelo: {blob[:500]!r}") from e
    if not isinstance(data, dict):
        raise ValueError("Resposta do modelo não é um objeto JSON.")
    if not data or "valor_mercado_estimado" not in data:
        raise ValueError(
            "O modelo devolveu JSON sem 'valor_mercado_estimado' (objeto vazio ou incompleto). "
            f"Trecho: {blob[:600]!r}"
        )
    return LLMValorMercadoResposta.model_validate(data)


def ler_entradas_leilao_de_planilha(caminho: str | Path) -> list[dict[str, Any]]:
    path = Path(caminho).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    suf = path.suffix.lower()
    if suf in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    elif suf == ".csv":
        df = pd.read_csv(path, encoding="utf-8-sig")
    else:
        raise ValueError("Use .csv, .xlsx ou .xls")

    col_map = {str(c).strip().lower(): c for c in df.columns}
    for cand in ("url_leilao", "url", "link", "link_leilao"):
        if cand in col_map:
            url_col = col_map[cand]
            break
    else:
        url_col = df.columns[0]

    out: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        raw_u = row[url_col]
        if pd.isna(raw_u):
            continue
        u = str(raw_u).strip()
        if not u or u.lower() in ("nan", "none"):
            continue
        if u and not u.lower().startswith(("http://", "https://")):
            u = "https://" + u
        meta: dict[str, Any] = {}
        for key in (
            "cidade",
            "estado",
            "bairro",
            "endereco",
            "padrao_imovel",
            "tipo_imovel",
            "conservacao",
            "tipo_casa",
            "andar",
            "data_leilao",
        ):
            if key in col_map and pd.notna(row[col_map[key]]):
                raw_cell = row[col_map[key]]
                if key == "data_leilao":
                    iso = normalizar_data_leilao_para_iso(raw_cell)
                    if iso:
                        meta["data_leilao"] = iso
                else:
                    v = str(raw_cell).strip()
                    if v:
                        meta[key] = v
        if "status" in col_map and pd.notna(row[col_map["status"]]):
            st_norm = _normalizar_status_planilha(row[col_map["status"]])
            if st_norm:
                meta["status"] = st_norm
        for area_key in ("area_util", "area_total"):
            if area_key in col_map and pd.notna(row[col_map[area_key]]):
                try:
                    meta[area_key] = float(row[col_map[area_key]])
                except (TypeError, ValueError):
                    pass
        if "valor_arrematacao" in col_map and pd.notna(row[col_map["valor_arrematacao"]]):
            raw_val = row[col_map["valor_arrematacao"]]
            if isinstance(raw_val, str) and raw_val.strip():
                meta["valor_arrematacao"] = raw_val.strip()
            else:
                try:
                    meta["valor_arrematacao"] = float(raw_val)
                except (TypeError, ValueError):
                    pass
        for key in ("quartos", "vagas"):
            if key in col_map and pd.notna(row[col_map[key]]):
                try:
                    meta[key] = int(float(row[col_map[key]]))
                except (TypeError, ValueError):
                    pass
        for key in ("latitude", "longitude"):
            if key in col_map and pd.notna(row[col_map[key]]):
                try:
                    meta[key] = float(row[col_map[key]])
                except (TypeError, ValueError):
                    pass
        if "andar" in meta:
            try:
                meta["andar"] = int(float(str(meta["andar"]).replace(",", ".")))
            except (TypeError, ValueError):
                meta.pop("andar", None)

        bairro_raw = str(meta.get("bairro") or "").strip()
        estado_raw = str(meta.get("estado") or "").strip()
        cidade_raw = str(meta.get("cidade") or "").strip()
        if bairro_raw and estado_raw and cidade_raw:
            nome_corrigido, slug_vr = resolver_bairro_para_vivareal(bairro_raw, estado_raw, cidade_raw)
            if slug_vr:
                meta["bairro"] = nome_corrigido
                meta["bairro_vivareal_slug"] = slug_vr
                if nome_corrigido != bairro_raw:
                    logger.info(
                        "Bairro corrigido na planilha: '%s' -> '%s' (slug: %s)",
                        bairro_raw, nome_corrigido, slug_vr,
                    )

        out.append({"url_leilao": u, "metadados": meta})
    return out


def _buscar_registros_por_urls(
    urls: set[str],
    client: Optional[Any] = None,
) -> list[dict[str, Any]]:
    cli = client or get_supabase_client()
    rows: list[dict[str, Any]] = []
    for url in urls:
        resp = cli.table(SUPABASE_TABLE).select("*").eq("url_leilao", url).limit(1).execute()
        data = getattr(resp, "data", None) or []
        if data:
            rows.append(data[0])
    return rows


def _geocodificar_registros_sem_coordenadas(
    registros: list[dict[str, Any]],
    *,
    client: Optional[Any] = None,
) -> int:
    """Geocodifica registros de leilão que ainda não têm lat/lon e grava no Supabase."""
    from geocoding import geocodificar_endereco

    cli = client or get_supabase_client()
    atualizados = 0
    for row in registros:
        if row.get("latitude") is not None and row.get("longitude") is not None:
            continue
        iid = row.get("id")
        if not iid:
            continue
        coords = geocodificar_endereco(
            logradouro=str(row.get("endereco") or ""),
            bairro=str(row.get("bairro") or ""),
            cidade=str(row.get("cidade") or ""),
            estado=str(row.get("estado") or ""),
        )
        if coords:
            row["latitude"] = coords[0]
            row["longitude"] = coords[1]
            try:
                atualizar_leilao_imovel_campos(
                    str(iid),
                    {"latitude": coords[0], "longitude": coords[1]},
                    client=cli,
                )
                atualizados += 1
            except Exception:
                logger.debug("Falha ao gravar geocodificação para id=%s", iid, exc_info=True)
    if atualizados:
        logger.info("Geocodificação: %s/%s registros atualizados com coordenadas", atualizados, len(registros))
    return atualizados


def _snippets_comparaveis_por_bairro(
    cidade: str,
    localizacao: str,
    quartos: Optional[int],
    tipo_imovel: str = "apartamento",
    conservacao: str = "",
    tipo_casa: str = "",
    faixa_andar: str = "",
    trecho_rua: str = "",
    max_results: int = 4,
) -> str:
    if DDGS is None:
        return ""
    loc = (localizacao or "").strip() or cidade
    tipo_busca = (tipo_imovel or "imóvel").strip() or "imóvel"
    q = f"{tipo_busca} à venda {loc} {cidade} ZAP OLX Viva Real preço"
    if quartos and quartos > 0:
        q = f"{quartos} quartos " + q
    if conservacao:
        q = f"{conservacao} {q}"
    if tipo_casa and "casa" in tipo_busca.lower():
        q = f"{tipo_casa} {q}"
    if faixa_andar and "apartamento" in tipo_busca.lower():
        q = f"{faixa_andar} andar {q}"
    if trecho_rua:
        q = f"{trecho_rua} {q}"
    try:
        with DDGS(timeout=20) as ddgs:
            hits = list(ddgs.text(q, max_results=max_results, region="br-pt"))
    except Exception:
        logger.exception("Falha DDGS para %s/%s", cidade, loc)
        return ""
    partes = []
    for h in hits:
        title = h.get("title", "")
        body = h.get("body", "")
        href = h.get("href", "")
        partes.append(f"- {title}\n  {body[:400]}\n  {href}")
    return "\n".join(partes) if partes else ""


def _avaliar_imovel_llm_resumo(
    row: dict[str, Any],
    snippets_mercado: str,
    modelo: str,
    *,
    max_tokens: int = 8192,
) -> LLMValorMercadoResposta:
    if OpenAI is None:
        raise RuntimeError("Instale o pacote `openai` para usar avaliação LLM")
    client = OpenAI()
    payload = {
        "endereco": row.get("endereco"),
        "cidade": row.get("cidade"),
        "estado": row.get("estado"),
        "bairro": row.get("bairro"),
        "tipo_imovel": row.get("tipo_imovel"),
        "conservacao": row.get("conservacao"),
        "tipo_casa": row.get("tipo_casa"),
        "andar": row.get("andar"),
        "area_util": row.get("area_util"),
        "area_total": row.get("area_total"),
        "quartos": row.get("quartos"),
        "vagas": row.get("vagas"),
        "valor_arrematacao": row.get("valor_arrematacao"),
        "data_leilao": row.get("data_leilao"),
        "snippets_anuncios_proximos": snippets_mercado[:12000],
    }
    system = (
        "Você é especialista em precificação de imóveis em leilão no Brasil. "
        "Com base nos dados e nos trechos de anúncios (podem ser incompletos), estime o valor de mercado "
        "de venda do imóvel descrito e sugira 1–2 cenários breves de reforma estratégica (custo-benefício). "
        "Classifique também o segmento de mercado para referência futura (cache): "
        "tipo_imovel_inferido: apartamento|casa|desconhecido; conservacao_inferida: novo|usado|desconhecido; "
        "tipo_casa_inferida: terrea|sobrado| vazio se não for casa; "
        "faixa_andar_inferida: terreo|baixo|medio|alto| vazio se casa; andar_inferido: número ou null; "
        "logradouro_chave_inferida: só o nome da rua/avenida sem número, ou vazio. "
        "Responda APENAS um objeto JSON com as chaves: "
        "valor_mercado_estimado (número), notas_avaliacao, resumo_mercado_regiao, "
        "cenarios_reforma_estrategica (string com 1–2 cenários; não use array JSON), "
        "tipo_imovel_inferido, conservacao_inferida, tipo_casa_inferida, faixa_andar_inferida "
        "(use string vazia \"\" quando não aplicável, nunca null), "
        "andar_inferido (número ou null), logradouro_chave_inferida."
    )
    user = json.dumps(payload, ensure_ascii=False, default=str)
    limit = max(256, min(max_tokens, 128000))
    comp = None
    choice0 = None
    raw = ""
    for tentativa in range(2):
        comp = openai_chat_completions_create_compat(
            client,
            model=modelo,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            temperature=0.15,
            max_tokens=limit,
        )
        choice0 = comp.choices[0]
        raw = _conteudo_mensagem_openai(getattr(choice0, "message", None))
        fr = getattr(choice0, "finish_reason", None)
        if (raw or "").strip():
            break
        if fr == "length" and tentativa == 0:
            limit = min(limit * 2, 128000)
            logger.warning(
                "LLM esgotou o limite de tokens (finish_reason=length); repetindo com max_tokens=%s",
                limit,
            )
            continue
        logger.warning(
            "LLM retornou mensagem vazia (finish_reason=%s). Aumente OPENAI_LLM_MAX_TOKENS ou use outro modelo.",
            fr,
        )
        break
    return _parse_resposta_llm_valor_mercado(raw)


def _preencher_cache_bairro_apos_llm_se_miss(
    row_f: dict[str, Any],
    valor_mercado_estimado: float,
    out_llm: LLMValorMercadoResposta,
    *,
    client: Any,
    max_idade_cache_dias: int,
) -> None:
    """
    Grava preco_m2_medio no cache por chave_segmento (tipo, conservação, andar, rua) quando
    ainda não há linha para esse segmento. Inferências do LLM refinam o segmento vs. só o edital.
    """
    cidade = (row_f.get("cidade") or "").strip()
    bairro = (row_f.get("bairro") or "").strip()
    estado = (row_f.get("estado") or "").strip()
    if not cidade or (not bairro and not estado):
        return
    area_f = area_efetiva_de_registro(row_f)
    if area_f <= 0 or valor_mercado_estimado <= 0:
        return
    overrides: dict[str, Optional[str]] = {}
    if (x := (out_llm.tipo_imovel_inferido or "").strip()):
        overrides["tipo_imovel"] = x
    if (x := (out_llm.conservacao_inferida or "").strip()):
        overrides["conservacao"] = x
    if (x := (out_llm.tipo_casa_inferida or "").strip()):
        overrides["tipo_casa"] = x
    if (x := (out_llm.faixa_andar_inferida or "").strip()):
        overrides["faixa_andar"] = x
    if out_llm.andar_inferido is not None:
        overrides["andar"] = str(out_llm.andar_inferido)
    if (x := (out_llm.logradouro_chave_inferida or "").strip()):
        overrides["logradouro_chave"] = x
    try:
        existente = buscar_media_bairro_no_cache(
            cidade,
            bairro,
            estado=estado,
            registro=row_f,
            segmento=overrides if overrides else None,
            fallback_geografico=False,
            client=client,
            max_idade_dias=max_idade_cache_dias,
        )
        if existente:
            iid0 = row_f.get("id")
            cid0 = existente.get("id")
            if iid0 and cid0:
                try:
                    atualizar_leilao_imovel_campos(
                        str(iid0),
                        {"cache_media_bairro_id": str(cid0)},
                        client=client,
                    )
                except Exception:
                    logger.debug("cache_media_bairro_id (cache já existente pós-LLM) não gravado", exc_info=True)
            return
        pm2 = float(valor_mercado_estimado) / float(area_f)
        if pm2 <= 0:
            return
        seg = merge_segmento_mercado(row_f, overrides if overrides else None)
        sv_llm = salvar_media_bairro_no_cache(
            CacheMediaBairroSalvar(
                cidade=cidade,
                bairro=bairro,
                estado=estado,
                tipo_imovel=seg["tipo_imovel"],
                conservacao=seg["conservacao"],
                tipo_casa=seg["tipo_casa"],
                faixa_andar=seg["faixa_andar"],
                faixa_area=seg.get("faixa_area", "-"),
                logradouro_chave=seg["logradouro_chave"],
                preco_m2_medio=round(pm2, 2),
                fonte="llm_pipeline",
                valor_medio_venda=round(float(valor_mercado_estimado), 2),
                maior_valor_venda=round(float(valor_mercado_estimado), 2),
                menor_valor_venda=round(float(valor_mercado_estimado), 2),
                n_amostras=1,
                metadados_json=json.dumps(
                    {
                        "origem": "valor_mercado_estimado_div_area_util",
                        "segmento": seg,
                        "nota": "Proxy por imóvel único + inferências LLM; revisar com amostra maior.",
                    },
                    ensure_ascii=False,
                ),
            ),
            client=client,
        )
        iid1 = row_f.get("id")
        nid = (sv_llm or {}).get("cache_media_bairro_id") if isinstance(sv_llm, dict) else None
        if iid1 and nid:
            try:
                atualizar_leilao_imovel_campos(
                    str(iid1),
                    {"cache_media_bairro_id": str(nid)},
                    client=client,
                )
            except Exception:
                logger.debug("cache_media_bairro_id (novo cache pós-LLM) não gravado", exc_info=True)
        logger.info(
            "Cache segmento preenchido pós-LLM: cidade=%s loc=%s tipo=%s pm2=%.2f",
            cidade,
            bairro or estado,
            seg.get("tipo_imovel"),
            pm2,
        )
    except Exception:
        logger.exception("Não foi possível gravar cache_media_bairro após LLM")


def executar_pipeline_precificacao_leiloes(
    config: LeilaoPricingPipelineConfig,
) -> dict[str, Any]:
    """
    Executa ingestão → triagem → avaliação (cache → LLM opcional) → ROI (só URLs do lote) → Excel ROI mínimo.
    """
    cli = get_supabase_client()
    relatorio: dict[str, Any] = {"fases": []}
    inicio_execucao = time.monotonic()

    def _motivo_interrupcao() -> Optional[str]:
        if config.tempo_limite_execucao_seg > 0:
            if (time.monotonic() - inicio_execucao) >= float(config.tempo_limite_execucao_seg):
                return "tempo_limite_excedido"
        if callable(config.abort_checker):
            try:
                if bool(config.abort_checker()):
                    return "abortado_pelo_usuario"
            except Exception:
                logger.exception("Falha no abort_checker; ignorando callback de abort.")
        return None

    def _interromper_se_necessario(fase: str) -> bool:
        motivo = _motivo_interrupcao()
        if not motivo:
            return False
        relatorio["interrompido"] = True
        relatorio["motivo_interrupcao"] = motivo
        relatorio["fase_interrompida"] = fase
        relatorio["tempo_execucao_seg"] = round(time.monotonic() - inicio_execucao, 2)
        logger.warning("Pipeline interrompido em %s: %s", fase, motivo)
        _emitir_progresso(fase, status="interrompido", mensagem=motivo)
        return True

    ordem_fases = {
        "ingestao": 0.1,
        "amostras_mercado_cache_bairro": 0.25,
        "triagem": 0.4,
        "avaliacao_cache": 0.55,
        "avaliacao_llm": 0.75,
        "financeiro": 0.9,
        "export_excel": 1.0,
    }

    def _emitir_progresso(
        fase: str,
        *,
        status: str = "running",
        mensagem: str = "",
        atual: Optional[int] = None,
        total: Optional[int] = None,
        extras: Optional[dict[str, Any]] = None,
    ) -> None:
        cb = config.progress_callback
        if not callable(cb):
            return
        payload: dict[str, Any] = {
            "fase": fase,
            "status": status,
            "mensagem": mensagem,
            "progress_pct": float(ordem_fases.get(fase, 0.0)),
            "elapsed_seg": round(time.monotonic() - inicio_execucao, 2),
        }
        if atual is not None:
            payload["atual"] = int(atual)
        if total is not None:
            payload["total"] = int(total)
            if total > 0 and atual is not None:
                payload["fase_pct"] = round(min(1.0, max(0.0, float(atual) / float(total))), 3)
        if isinstance(extras, dict):
            payload.update(extras)
        try:
            cb(payload)
        except Exception:
            logger.exception("Falha em progress_callback do pipeline")

    _emitir_progresso("ingestao", mensagem="Iniciando ingestão")
    entradas = ler_entradas_leilao_de_planilha(config.caminho_planilha)
    urls_set = {e["url_leilao"] for e in entradas}
    relatorio["total_urls_planilha"] = len(urls_set)
    meta_por_url: dict[str, dict[str, Any]] = {
        e["url_leilao"]: (e.get("metadados") or {}) for e in entradas
    }

    def _merge_planilha(r: dict[str, Any]) -> dict[str, Any]:
        u = r.get("url_leilao") or ""
        m = meta_por_url.get(u, {})
        return {**r, **m}

    # --- Ingestão (sem LLM) ---
    ing_stats = {"inseridos": 0, "duplicados": 0, "erros": 0}
    total_entradas = len(entradas)
    for idx, e in enumerate(entradas, start=1):
        if _interromper_se_necessario("ingestao"):
            return relatorio
        _emitir_progresso(
            "ingestao",
            mensagem="Ingerindo URLs",
            atual=idx,
            total=total_entradas,
        )
        try:
            r = ingerir_url_leilao(e["url_leilao"], e.get("metadados") or {}, client=cli)
            st = r.get("status", "")
            if st == "inserted":
                ing_stats["inseridos"] += 1
            elif st == "skipped_duplicate":
                ing_stats["duplicados"] += 1
        except Exception:
            ing_stats["erros"] += 1
            logger.exception("Ingestão falhou para %s", e["url_leilao"])
        time.sleep(config.delay_entre_scrapes_seg)
    relatorio["fases"].append({"nome": "ingestao", "detalhe": ing_stats})

    registros = _buscar_registros_por_urls(urls_set, client=cli)

    # --- Geocodificação de registros sem coordenadas ---
    _geocodificar_registros_sem_coordenadas(registros, client=cli)

    # --- Amostras web + atualização cache_media_bairro (antes da triagem; não depende de cache prévio) ---
    if config.usar_anuncios_mercado_supabase and config.sincronizar_amostras_mercado_antes_triagem:
        _emitir_progresso("amostras_mercado_cache_bairro", mensagem="Sincronizando amostras de mercado")
        chaves_sync: set[str] = set()
        sync_stats = {
            "cache_atualizado": 0,
            "skipped_segmento_ja_no_lote": 0,
            "baixa_confianca": 0,
            "erros": 0,
        }
        total_registros_sync = len(registros)
        for idx, row in enumerate(registros, start=1):
            if _interromper_se_necessario("amostras_mercado_cache_bairro"):
                return relatorio
            _emitir_progresso(
                "amostras_mercado_cache_bairro",
                mensagem="Sincronizando comparáveis",
                atual=idx,
                total=total_registros_sync,
                extras={
                    "imovel_id": str(row.get("id") or ""),
                    "url_leilao": str(row.get("url_leilao") or ""),
                    "amostras_cache_atualizado": int(sync_stats["cache_atualizado"]),
                },
            )
            try:
                r = sincronizar_amostras_e_atualizar_cache_media_bairro(
                    _merge_planilha(row),
                    client=cli,
                    min_anuncios=config.min_anuncios_mercado_similares,
                    max_idade_dias=config.max_idade_anuncios_mercado_dias,
                    max_results_ddgs=config.ddgs_max_resultados_por_rodada,
                    ddgs_rodadas_max=config.ddgs_rodadas_max,
                    raio_inicial_km=config.raio_similaridade_inicial_km,
                    raios_expansao_km=config.raios_similaridade_expansao_km,
                    min_confianca_aceitavel=config.confianca_minima_comparaveis,
                    chaves_ja_sincronizadas=chaves_sync,
                    modo_entrada=config.modo_entrada,
                )
                if r.get("interromper_pipeline_avulso"):
                    sync_stats["abort_avulso_vivareal"] = (
                        sync_stats.get("abort_avulso_vivareal", 0) + 1
                    )
                    relatorio["interrompido"] = True
                    relatorio["motivo_interrupcao"] = str(
                        r.get("motivo") or "vivareal_anuncios_insuficientes"
                    )
                    relatorio["fase_interrompida"] = "amostras_mercado_cache_bairro"
                    relatorio["tempo_execucao_seg"] = round(
                        time.monotonic() - inicio_execucao, 2
                    )
                    relatorio["fases"].append(
                        {"nome": "amostras_mercado_cache_bairro", "detalhe": sync_stats}
                    )
                    _emitir_progresso(
                        "amostras_mercado_cache_bairro",
                        status="interrompido",
                        mensagem=relatorio["motivo_interrupcao"],
                    )
                    return relatorio
                if r.get("cache_atualizado"):
                    sync_stats["cache_atualizado"] += 1
                elif r.get("skipped"):
                    sync_stats["skipped_segmento_ja_no_lote"] += 1
                elif r.get("motivo") == "baixa_confianca_comparaveis":
                    sync_stats["baixa_confianca"] += 1
                link_cache = r.get("cache_media_bairro_id")
                if link_cache and row.get("id"):
                    try:
                        atualizar_leilao_imovel_campos(
                            str(row["id"]),
                            {"cache_media_bairro_id": str(link_cache)},
                            client=cli,
                        )
                    except Exception:
                        logger.debug(
                            "Não foi possível gravar cache_media_bairro_id após sincronizar amostras",
                            exc_info=True,
                        )
                _emitir_progresso(
                    "amostras_mercado_cache_bairro",
                    mensagem="Amostras avaliadas",
                    atual=idx,
                    total=total_registros_sync,
                    extras={
                        "imovel_id": str(row.get("id") or ""),
                        "url_leilao": str(row.get("url_leilao") or ""),
                        "n_comparaveis_filtrados": int(r.get("n_comparaveis_filtrados") or 0),
                        "n_precos": int(r.get("n_precos") or 0),
                        "amostragem_baixa": bool(r.get("amostragem_baixa")),
                        "cache_atualizado": bool(r.get("cache_atualizado")),
                        "motivo_sync": str(r.get("motivo") or ""),
                    },
                )
            except Exception:
                sync_stats["erros"] += 1
                logger.exception("sincronizar_amostras_e_atualizar_cache_media_bairro")
        relatorio["fases"].append({"nome": "amostras_mercado_cache_bairro", "detalhe": sync_stats})

    registros = _buscar_registros_por_urls(urls_set, client=cli)

    # --- Triagem ---
    _emitir_progresso("triagem", mensagem="Executando triagem heurística")
    tri_stats = {"descartados": 0, "mantidos": 0}
    if config.usar_triagem_preco_m2:
        total_registros_triagem = len(registros)
        for idx, row in enumerate(registros, start=1):
            if _interromper_se_necessario("triagem"):
                return relatorio
            _emitir_progresso(
                "triagem",
                mensagem="Triando imóveis",
                atual=idx,
                total=total_registros_triagem,
            )
            st = (row.get("status") or "").strip().lower()
            if st == "descartado_triagem":
                continue
            if st and st != "pendente":
                continue
            tr = triagem_heuristica_de_registro_leilao(
                _merge_planilha(row),
                client=cli,
                max_idade_cache_dias=config.cache_bairro_max_idade_dias,
                fallback_geografico=config.cache_fallback_geografico,
            )
            if tr.descartar and row.get("id"):
                atualizar_leilao_imovel_campos(
                    str(row["id"]),
                    {"status": "descartado_triagem"},
                    client=cli,
                )
                tri_stats["descartados"] += 1
            else:
                tri_stats["mantidos"] += 1
    relatorio["fases"].append({"nome": "triagem", "detalhe": tri_stats})

    registros = _buscar_registros_por_urls(urls_set, client=cli)

    # --- Avaliação por cache (sem LLM) ---
    _emitir_progresso("avaliacao_cache", mensagem="Avaliando por cache de bairro")
    confianca_valuation_por_id: dict[str, dict[str, Any]] = {}
    teto_reposicionamento_por_id: dict[str, dict[str, Any]] = {}
    cache_stats = {"ok": 0, "skip": 0}
    total_registros_cache = len(registros)
    for idx, row in enumerate(registros, start=1):
        if _interromper_se_necessario("avaliacao_cache"):
            return relatorio
        _emitir_progresso(
            "avaliacao_cache",
            mensagem="Avaliando imóveis em cache",
            atual=idx,
            total=total_registros_cache,
            extras={
                "imovel_id": str(row.get("id") or ""),
                "url_leilao": str(row.get("url_leilao") or ""),
            },
        )
        if (row.get("status") or "").strip().lower() != "pendente":
            continue
        r = avaliar_imovel_por_cache_media_bairro(
            _merge_planilha(row),
            fator_prudencia=config.fator_prudencia_cache,
            max_idade_cache_dias=config.cache_bairro_max_idade_dias,
            confianca_minima_cache=config.confianca_minima_comparaveis,
            client=cli,
        )
        if r.get("status") == "ok_cache":
            cache_stats["ok"] += 1
            if row.get("id"):
                cid = str(row.get("id"))
                conf = r.get("confianca_score")
                conf_nivel = r.get("confianca_nivel") or ("indefinida" if conf is None else None)
                confianca_valuation_por_id[cid] = {
                    "confianca_score": float(conf) if isinstance(conf, (int, float)) else None,
                    "confianca_nivel": conf_nivel or "indefinida",
                    "origem": str(r.get("origem_confianca") or "cache"),
                }
                teto_reposicionamento_por_id[cid] = {
                    "valor_maximo_regiao_estimado": r.get("valor_maximo_regiao_estimado"),
                    "valor_teto_regiao_agressivo": r.get("valor_teto_regiao_agressivo"),
                    "potencial_reposicionamento_pct": r.get("potencial_reposicionamento_pct"),
                    "origem_teto": "cache_comparaveis",
                }
            _emitir_progresso(
                "avaliacao_cache",
                mensagem="Avaliação por cache concluída",
                atual=idx,
                total=total_registros_cache,
                extras={
                    "imovel_id": str(row.get("id") or ""),
                    "cache_resultado": "ok_cache",
                },
            )
        else:
            cache_stats["skip"] += 1
            _emitir_progresso(
                "avaliacao_cache",
                mensagem="Imóvel não avaliado no cache",
                atual=idx,
                total=total_registros_cache,
                extras={
                    "imovel_id": str(row.get("id") or ""),
                    "cache_resultado": str(r.get("motivo") or "skip"),
                },
            )
    relatorio["fases"].append({"nome": "avaliacao_cache", "detalhe": cache_stats})

    registros = _buscar_registros_por_urls(urls_set, client=cli)
    snippets_cache: dict[tuple[Any, ...], str] = {}
    snippets_ctx_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    llm_stats = {"chamadas": 0, "erros": 0, "skip_baixa_confianca": 0}

    # --- LLM enxuto (só pendentes) ---
    if config.usar_avaliacao_llm:
        _emitir_progresso("avaliacao_llm", mensagem="Avaliando pendentes com LLM")
        total_registros_llm = len(registros)
        for idx, row in enumerate(registros, start=1):
            if _interromper_se_necessario("avaliacao_llm"):
                return relatorio
            _emitir_progresso(
                "avaliacao_llm",
                mensagem="Processando imóveis no LLM",
                atual=idx,
                total=total_registros_llm,
            )
            if (row.get("status") or "").strip().lower() != "pendente":
                continue
            if config.limite_imoveis_llm_por_execucao and llm_stats["chamadas"] >= config.limite_imoveis_llm_por_execucao:
                break
            row_f = _merge_planilha(row)
            cidade = (row_f.get("cidade") or "").strip()
            estado = (row_f.get("estado") or "").strip()
            bairro = (row_f.get("bairro") or "").strip()
            loc = bairro or estado
            if not cidade or not loc:
                logger.warning("LLM skip id=%s: falta cidade e (bairro ou estado)", row.get("id"))
                continue
            seg_snip = segmento_mercado_de_registro(row_f)
            key_snip = (
                cidade.lower(),
                loc.lower(),
                seg_snip["tipo_imovel"],
                seg_snip["conservacao"],
                seg_snip["faixa_andar"],
                seg_snip["logradouro_chave"],
                seg_snip["tipo_casa"],
            )
            ctx: dict[str, Any] = snippets_ctx_cache.get(key_snip, {})
            if key_snip not in snippets_cache:
                q = row_f.get("quartos")
                try:
                    qi = int(q) if q is not None and str(q).strip() != "" else None
                except (TypeError, ValueError):
                    qi = None
                tit = seg_snip["tipo_imovel"]
                tipo_busca = tit if tit != "desconhecido" else "imóvel"
                rua_txt = (
                    seg_snip["logradouro_chave"].replace("-", " ")
                    if seg_snip["logradouro_chave"] not in ("-", "x")
                    else ""
                )
                bloco = ""
                ctx = {}
                if config.usar_anuncios_mercado_supabase:
                    ctx = resolver_contexto_mercado_anuncios_detalhado(
                        row_f,
                        client=cli,
                        min_anuncios=config.min_anuncios_mercado_similares,
                        max_idade_dias=config.max_idade_anuncios_mercado_dias,
                        max_results_ddgs=config.ddgs_max_resultados_por_rodada,
                        ddgs_rodadas_max=config.ddgs_rodadas_max,
                        raio_inicial_km=config.raio_similaridade_inicial_km,
                        raios_expansao_km=config.raios_similaridade_expansao_km,
                        confianca_minima=config.confianca_minima_comparaveis,
                        bloquear_baixa_confianca=config.bloquear_llm_baixa_confianca,
                    )
                    if ctx.get("bloqueado_baixa_confianca"):
                        n_cmp = int(ctx.get("n_comparaveis") or 0)
                        if n_cmp == 0 and config.fallback_snippets_ddgs_se_sem_anuncios:
                            logger.warning(
                                "LLM: sem comparáveis estruturados para id=%s; liberando fallback snippets DDGS",
                                row.get("id"),
                            )
                            bloco = ""
                        else:
                            llm_stats["skip_baixa_confianca"] += 1
                            logger.warning(
                                "LLM skip id=%s: confiança comparáveis baixa (%s)",
                                row.get("id"),
                                (ctx.get("estatisticas_comparaveis") or {}).get("confianca_score"),
                            )
                            snippets_cache[key_snip] = ""
                            snippets_ctx_cache[key_snip] = ctx
                            continue
                    bloco = str(ctx.get("texto") or "")
                if (not bloco or not bloco.strip()) and config.fallback_snippets_ddgs_se_sem_anuncios:
                    bloco = _snippets_comparaveis_por_bairro(
                        cidade,
                        loc,
                        qi,
                        tipo_imovel=tipo_busca,
                        conservacao=seg_snip["conservacao"]
                        if seg_snip["conservacao"] != "desconhecido"
                        else "",
                        tipo_casa=seg_snip["tipo_casa"]
                        if seg_snip["tipo_casa"] not in ("-", "desconhecido")
                        else "",
                        faixa_andar=seg_snip["faixa_andar"]
                        if seg_snip["tipo_imovel"] == "apartamento"
                        and seg_snip["faixa_andar"] not in ("-", "casa")
                        else "",
                        trecho_rua=rua_txt,
                    )
                snippets_cache[key_snip] = bloco
                snippets_ctx_cache[key_snip] = ctx
            snip = snippets_cache[key_snip]
            ctx = snippets_ctx_cache.get(key_snip, {})
            try:
                out = _avaliar_imovel_llm_resumo(
                    row_f,
                    snip,
                    config.modelo_llm,
                    max_tokens=config.llm_max_tokens_avaliacao,
                )
                af = area_efetiva_de_registro(row_f)
                padrao = row_f.get("padrao_imovel") or "medio"
                if af > 0:
                    sim = executar_simulacao_reforma(
                        af,
                        str(padrao),
                        meses_incc=REFORMA_MESES_INCC_PADRAO,
                    )
                else:
                    sim = None
                notas = (
                    f"{out.notas_avaliacao}\n\n--- Mercado da região ---\n{out.resumo_mercado_regiao}\n\n"
                    f"--- Reformas estratégicas ---\n{out.cenarios_reforma_estrategica}\n\n"
                    f"(Avaliação LLM modelo {config.modelo_llm})"
                )
                llm_ok = False
                if sim and row.get("id"):
                    persistir_avaliacao_tecnica_no_registro(
                        str(row["id"]),
                        out.valor_mercado_estimado,
                        sim,
                        notas,
                        client=cli,
                    )
                    llm_ok = True
                elif row.get("id"):
                    atualizar_leilao_imovel_campos(
                        str(row["id"]),
                        {
                            "status": STATUS_ANALISADO,
                            "valor_mercado_estimado": out.valor_mercado_estimado,
                            "valor_venda_sugerido": out.valor_mercado_estimado,
                        },
                        client=cli,
                    )
                    llm_ok = True
                if llm_ok:
                    llm_stats["chamadas"] += 1
                    if row.get("id"):
                        cid = str(row.get("id"))
                        sc = (ctx.get("estatisticas_comparaveis") or {}).get("confianca_score")
                        nv = (ctx.get("estatisticas_comparaveis") or {}).get("confianca_nivel")
                        confianca_valuation_por_id[cid] = {
                            "confianca_score": float(sc) if isinstance(sc, (int, float)) else None,
                            "confianca_nivel": str(nv or "indefinida"),
                            "origem": "llm_comparaveis",
                        }
                        area_ref_llm = area_efetiva_de_registro(row_f)
                        st_cmp = ctx.get("estatisticas_comparaveis") or {}
                        pm2_p90 = st_cmp.get("pm2_p90")
                        pm2_max = st_cmp.get("pm2_max")
                        try:
                            v_max = (
                                round(float(pm2_p90) * float(area_ref_llm), 2)
                                if pm2_p90 is not None and area_ref_llm > 0
                                else None
                            )
                        except (TypeError, ValueError):
                            v_max = None
                        try:
                            v_ag = (
                                round(float(pm2_max) * float(area_ref_llm), 2)
                                if pm2_max is not None and area_ref_llm > 0
                                else None
                            )
                        except (TypeError, ValueError):
                            v_ag = None
                        pot_pct = None
                        try:
                            if v_max is not None and float(out.valor_mercado_estimado) > 0:
                                pot_pct = round(
                                    ((float(v_max) - float(out.valor_mercado_estimado)) / float(out.valor_mercado_estimado))
                                    * 100.0,
                                    2,
                                )
                        except (TypeError, ValueError):
                            pot_pct = None
                        teto_reposicionamento_por_id[cid] = {
                            "valor_maximo_regiao_estimado": v_max,
                            "valor_teto_regiao_agressivo": v_ag,
                            "potencial_reposicionamento_pct": pot_pct,
                            "origem_teto": "llm_comparaveis",
                        }
                        if row.get("id"):
                            atualizar_leilao_imovel_campos(
                                str(row["id"]),
                                {
                                    "valor_maximo_regiao_estimado": v_max,
                                    "valor_teto_regiao_agressivo": v_ag,
                                    "potencial_reposicionamento_pct": pot_pct,
                                },
                                client=cli,
                            )
                    _preencher_cache_bairro_apos_llm_se_miss(
                        row_f,
                        out.valor_mercado_estimado,
                        out,
                        client=cli,
                        max_idade_cache_dias=config.cache_bairro_max_idade_dias,
                    )
            except Exception:
                llm_stats["erros"] += 1
                logger.exception("LLM avaliação falhou id=%s", row.get("id"))

    relatorio["fases"].append({"nome": "avaliacao_llm", "detalhe": llm_stats})

    # --- Financeiro (somente URLs do lote) ---
    fin = processar_financeiro_imoveis_por_urls(
        urls_set,
        globais=ParametrosFinanceirosGlobais(),
        client=cli,
    )
    _emitir_progresso("financeiro", mensagem="Calculando financeiro e ROI")
    fin_stats: dict[str, Any] = {
        "total": len(fin),
        "processados": 0,
        "pulado_descartado_triagem": 0,
        "erros": 0,
        "acima_lance_recomendado": 0,
        "lance_maximo_medio": None,
        "valor_venda_liquido_medio": None,
        "confianca_valuation_final_media": None,
        "confianca_valuation_final_distribuicao": {"alta": 0, "media": 0, "baixa": 0, "indefinida": 0},
        "confianca_valuation_por_imovel": [],
        "valor_maximo_regiao_medio": None,
        "potencial_reposicionamento_medio_pct": None,
        "reposicionamento_por_imovel": [],
        "top_risco_lance_acima_recomendado": [],
    }
    lances_maximos: list[float] = []
    vendas_liquidas: list[float] = []
    confiancas_score: list[float] = []
    riscos_lance: list[dict[str, Any]] = []
    valores_max_regiao: list[float] = []
    potenciais_pct: list[float] = []
    url_por_id: dict[str, str] = {
        str(r.get("id")): str(r.get("url_leilao") or "")
        for r in registros
        if r.get("id")
    }
    total_fin = len(fin)
    for idx, item in enumerate(fin, start=1):
        if _interromper_se_necessario("financeiro"):
            return relatorio
        _emitir_progresso(
            "financeiro",
            mensagem="Persistindo financeiro",
            atual=idx,
            total=total_fin,
        )
        if item.get("erro"):
            fin_stats["erros"] += 1
            continue
        if item.get("pulado"):
            fin_stats["pulado_descartado_triagem"] += 1
            continue
        fin_stats["processados"] += 1
        id_imovel = str(item.get("id") or "")
        conf_item = confianca_valuation_por_id.get(id_imovel, {})
        conf_score = conf_item.get("confianca_score")
        conf_nivel = str(conf_item.get("confianca_nivel") or "indefinida").lower()
        if conf_nivel not in ("alta", "media", "baixa", "indefinida"):
            conf_nivel = "indefinida"
        if isinstance(conf_score, (int, float)):
            confiancas_score.append(float(conf_score))
        fin_stats["confianca_valuation_final_distribuicao"][conf_nivel] += 1
        fin_stats["confianca_valuation_por_imovel"].append(
            {
                "id": id_imovel or None,
                "url_leilao": url_por_id.get(id_imovel, ""),
                "confianca_score": float(conf_score) if isinstance(conf_score, (int, float)) else None,
                "confianca_nivel": conf_nivel,
                "origem": conf_item.get("origem") or "indefinida",
            }
        )
        teto_item = teto_reposicionamento_por_id.get(id_imovel, {})
        v_max_reg = teto_item.get("valor_maximo_regiao_estimado")
        v_teto_ag = teto_item.get("valor_teto_regiao_agressivo")
        pot_teto = teto_item.get("potencial_reposicionamento_pct")
        lm = item.get("lance_maximo_recomendado")
        if isinstance(lm, (int, float)):
            lances_maximos.append(float(lm))
        res = item.get("resultado") if isinstance(item.get("resultado"), dict) else {}
        if isinstance(res, dict):
            vl = res.get("valor_venda_liquido")
            vv = res.get("valor_venda_estimado")
            lance_atual = res.get("valor_lance")
            if isinstance(vl, (int, float)):
                vendas_liquidas.append(float(vl))
            if isinstance(v_max_reg, (int, float)):
                valores_max_regiao.append(float(v_max_reg))
                pot_pct = None
                if isinstance(vv, (int, float)) and float(vv) > 0:
                    pot_pct = ((float(v_max_reg) - float(vv)) / float(vv)) * 100.0
                    potenciais_pct.append(float(pot_pct))
                fin_stats["reposicionamento_por_imovel"].append(
                    {
                        "id": id_imovel or None,
                        "url_leilao": url_por_id.get(id_imovel, ""),
                        "valor_venda_estimado_atual": round(float(vv), 2) if isinstance(vv, (int, float)) else None,
                        "valor_maximo_regiao_estimado": round(float(v_max_reg), 2),
                        "valor_teto_regiao_agressivo": round(float(v_teto_ag), 2)
                        if isinstance(v_teto_ag, (int, float))
                        else None,
                        "potencial_reposicionamento_pct": round(float(pot_pct), 2)
                        if pot_pct is not None
                        else (round(float(pot_teto), 2) if isinstance(pot_teto, (int, float)) else None),
                        "origem_teto": teto_item.get("origem_teto") or "indefinida",
                    }
                )
            if isinstance(lm, (int, float)) and isinstance(lance_atual, (int, float)):
                if float(lance_atual) > float(lm):
                    fin_stats["acima_lance_recomendado"] += 1
                    excesso = float(lance_atual) - float(lm)
                    excesso_pct = (excesso / float(lm)) * 100.0 if float(lm) > 0 else None
                    riscos_lance.append(
                        {
                            "id": id_imovel or None,
                            "url_leilao": url_por_id.get(id_imovel, ""),
                            "valor_lance": round(float(lance_atual), 2),
                            "lance_maximo_recomendado": round(float(lm), 2),
                            "excesso_lance": round(excesso, 2),
                            "excesso_lance_pct": round(excesso_pct, 2) if excesso_pct is not None else None,
                            "roi_liquido_pct": res.get("roi_liquido_pct"),
                        }
                    )
    if lances_maximos:
        fin_stats["lance_maximo_medio"] = round(sum(lances_maximos) / len(lances_maximos), 2)
    if vendas_liquidas:
        fin_stats["valor_venda_liquido_medio"] = round(sum(vendas_liquidas) / len(vendas_liquidas), 2)
    if confiancas_score:
        fin_stats["confianca_valuation_final_media"] = round(sum(confiancas_score) / len(confiancas_score), 2)
    if valores_max_regiao:
        fin_stats["valor_maximo_regiao_medio"] = round(sum(valores_max_regiao) / len(valores_max_regiao), 2)
    if potenciais_pct:
        fin_stats["potencial_reposicionamento_medio_pct"] = round(sum(potenciais_pct) / len(potenciais_pct), 2)
    if riscos_lance:
        riscos_lance.sort(
            key=lambda x: float(x.get("excesso_lance_pct") or 0.0),
            reverse=True,
        )
        fin_stats["top_risco_lance_acima_recomendado"] = riscos_lance[:5]
    fin_stats["confianca_valuation_por_imovel"].sort(
        key=lambda x: float(x.get("confianca_score") or -1.0),
    )
    fin_stats["reposicionamento_por_imovel"].sort(
        key=lambda x: float(x.get("potencial_reposicionamento_pct") or -999.0),
        reverse=True,
    )
    relatorio["fases"].append({"nome": "financeiro", "detalhe": fin_stats})

    # --- Excel ROI objetivo ---
    _emitir_progresso("export_excel", mensagem="Gerando planilha de saída")
    out_path = Path(config.caminho_excel_roi_objetivo)
    exportar_imoveis_roi_minimo_para_excel(
        out_path,
        config.roi_minimo_exportacao_pct,
        client=cli,
    )
    relatorio["fases"].append(
        {"nome": "export_excel", "arquivo": str(out_path.resolve()), "roi_min_pct": config.roi_minimo_exportacao_pct}
    )
    relatorio["tempo_execucao_seg"] = round(time.monotonic() - inicio_execucao, 2)
    _emitir_progresso("export_excel", status="done", mensagem="Execução concluída")

    resumo: dict[str, Any] = {}
    for fase in relatorio.get("fases", []):
        nome = fase.get("nome", "")
        det = fase.get("detalhe") or {}
        if not isinstance(det, dict):
            continue
        if nome == "amostras_mercado_cache_bairro":
            resumo["cache_bairro_atualizado"] = det.get("cache_atualizado", 0)
            resumo["cache_bairro_skipped"] = det.get("skipped_segmento_ja_no_lote", 0)
            resumo["cache_bairro_baixa_confianca"] = det.get("baixa_confianca", 0)
            resumo["cache_bairro_erros"] = det.get("erros", 0)
        elif nome == "avaliacao_cache":
            resumo["cache_avaliados"] = det.get("avaliados", 0)
            resumo["cache_atualizados"] = det.get("atualizados", 0)
        elif nome == "avaliacao_llm":
            resumo["llm_chamadas"] = det.get("chamadas", 0)
            resumo["llm_avaliados"] = det.get("avaliados", 0)
        elif nome == "financeiro":
            resumo["financeiro_processados"] = det.get("processados", 0)
            resumo["financeiro_erros"] = det.get("erros", 0)
    relatorio["resumo"] = resumo

    return relatorio


# --- Agno: orquestrador em linguagem natural (tools chamam o pipeline) -----------------------


@tool(show_result=True)
def tool_executar_pipeline_precificacao(caminho_planilha: str) -> str:
    """
    Roda o pipeline completo sobre uma planilha (.xlsx/.csv) com URLs de leilão.
    Usa variáveis de ambiente para Supabase e opcionalmente OPENAI_CHAT_MODEL.
    ROI mínimo para exportação: defina PRICING_ROI_MIN_EXPORT_PCT (padrão 25) ou edite a planilha de config depois.
    """
    try:
        roi_min = float(os.getenv("PRICING_ROI_MIN_EXPORT_PCT", "25"))
    except ValueError:
        roi_min = 25.0
    out_xlsx = os.getenv("PRICING_EXCEL_SAIDA", "output/oportunidades_roi_objetivo.xlsx")
    cfg = LeilaoPricingPipelineConfig(
        caminho_planilha=Path(caminho_planilha),
        caminho_excel_roi_objetivo=Path(out_xlsx),
        roi_minimo_exportacao_pct=roi_min,
    )
    rel = executar_pipeline_precificacao_leiloes(cfg)
    return json.dumps(rel, ensure_ascii=False, default=str)


@tool(show_result=True)
def tool_exportar_roi_minimo_pipeline(caminho_saida: str, roi_minimo_pct: float) -> str:
    """Exporta Excel com imóveis com roi_liquido_pct >= limiar (após pipeline/financeiro)."""
    try:
        p = exportar_imoveis_roi_minimo_para_excel(caminho_saida, roi_minimo_pct)
        return json.dumps({"ok": True, "arquivo": str(p)}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"ok": False, "erro": str(e)}, ensure_ascii=False)


def create_pricing_orchestrator_agent(
    *,
    model_id: Optional[str] = None,
    markdown: bool = True,
) -> Agent:
    """
    Agente Agno para operar o sistema via chat (dispara pipeline e reexportações).
    O trabalho pesado é feito nas tools (Python); o LLM só orquestra.
    """
    mid = model_id or os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
    return Agent(
        model=OpenAIChat(id=mid),
        tools=[tool_executar_pipeline_precificacao, tool_exportar_roi_minimo_pipeline],
        instructions=(
            "Você opera o sistema de precificação de leilões. "
            "Para processar uma lista de URLs, chame tool_executar_pipeline_precificacao com o caminho absoluto da planilha. "
            "Para gerar nova planilha filtrada por outro ROI, use tool_exportar_roi_minimo_pipeline. "
            "Explique ao usuário as fases retornadas no JSON (ingestão, triagem, cache, LLM, financeiro, export). "
            "Dica: planilhas devem ter colunas url/url_leilao e idealmente cidade e bairro para economizar tokens."
        ),
        markdown=markdown,
    )


__all__ = [
    "LeilaoPricingPipelineConfig",
    "LLMValorMercadoResposta",
    "create_pricing_orchestrator_agent",
    "executar_pipeline_precificacao_leiloes",
    "ler_entradas_leilao_de_planilha",
    "tool_executar_pipeline_precificacao",
    "tool_exportar_roi_minimo_pipeline",
]
