"""
Agente de ingestão (Agno): lê planilhas com links, extrai dados de páginas de leilão
com Playwright+stealth, valida com Pydantic e persiste no Supabase com deduplicação por URL.

Antes de acionar agentes LLM caros na pipeline, use token_efficiency.triagem_heuristica_de_registro_leilao
(com cidade/bairro e cache no Supabase) para descartar lotes com R$/m² acima da média do bairro.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
import gotrue_httpx_compat  # noqa: F401 — antes do supabase (gotrue + httpx: proxy → proxies)
from pydantic import BaseModel, ConfigDict, Field, field_validator
from supabase import Client, create_client
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.tools import tool
from leilao_constants import STATUS_PENDENTE, normalizar_data_leilao_para_iso
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_TABLE = "leilao_imoveis"
_supabase_anon_role_warned = False


def supabase_jwt_role_from_key(key: str) -> Optional[str]:
    """Lê o claim `role` do JWT (anon | service_role) sem validar assinatura."""
    try:
        parts = (key or "").strip().split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        pad = "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(payload_b64 + pad)
        data = json.loads(raw.decode("utf-8"))
        r = data.get("role")
        return str(r) if r is not None else None
    except Exception:
        return None
# Schema Supabase: url_leilao, endereco, cidade, estado, bairro, data_leilao,
# tipo_imovel, conservacao, tipo_casa, andar, padrao_imovel, area_util, area_total, quartos, vagas,
# valor_arrematacao, valor_mercado_estimado, valor_venda_sugerido, custo_reforma_estimado, roi_projetado, status, created_at.
_LINK_COLUMN_CANDIDATES = (
    "url_leilao",
    "url",
    "link",
    "link_leilao",
    "href",
    "url leilao",
)


class LeilaoImovelCreate(BaseModel):
    """Registro validado antes do insert na tabela `leilao_imoveis`."""

    model_config = ConfigDict(str_strip_whitespace=True)

    url_leilao: str = Field(..., description="URL canônica do lote no site do leiloeiro")
    endereco: Optional[str] = None
    cidade: Optional[str] = None
    estado: Optional[str] = None
    bairro: Optional[str] = None
    tipo_imovel: Optional[str] = Field(
        default=None,
        description="apartamento | casa | … (texto livre normalizado na leitura)",
    )
    conservacao: Optional[str] = Field(default=None, description="novo | usado | …")
    tipo_casa: Optional[str] = Field(default=None, description="terrea | sobrado — se casa")
    andar: Optional[int] = Field(default=None, ge=0)
    area_util: Optional[float] = Field(None, ge=0)
    area_total: Optional[float] = Field(None, ge=0, description="Área total do terreno (m²); mais relevante para casas")
    quartos: Optional[int] = Field(None, ge=0)
    vagas: Optional[int] = Field(None, ge=0)
    padrao_imovel: Optional[str] = Field(
        default=None,
        description="Padrão de reforma/custo: baixo | medio | alto (planilha / BD)",
    )
    valor_arrematacao: Optional[float] = Field(None, ge=0)
    data_leilao: Optional[str] = Field(
        default=None,
        description="Data do leilão em YYYY-MM-DD (coluna date no Supabase)",
    )
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    status: Optional[str] = Field(
        default=STATUS_PENDENTE,
        description="pendente | analisado | aprovado | descartado_triagem",
    )

    @field_validator("url_leilao")
    @classmethod
    def normalizar_url(cls, v: str) -> str:
        v = (v or "").strip()
        if not v.lower().startswith(("http://", "https://")):
            raise ValueError("url_leilao deve ser uma URL http(s) válida")
        return v

    @field_validator("valor_arrematacao", mode="before")
    @classmethod
    def _parse_valor_arrematacao(cls, v: Any) -> Any:
        if v is None or v == "":
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        s = re.sub(r"R\$\s*", "", s, flags=re.IGNORECASE).strip()
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        s = re.sub(r"[^\d.\-]", "", s)
        if not s or s == ".":
            return None
        try:
            return float(s)
        except ValueError:
            return None

    @field_validator("data_leilao", mode="before")
    @classmethod
    def _parse_data_leilao(cls, v: Any) -> Any:
        iso = normalizar_data_leilao_para_iso(v)
        return iso


def ler_links_leilao_arquivo(caminho: str | Path) -> list[str]:
    """
    Lê um arquivo Excel (.xlsx, .xls) ou CSV contendo links de leilão.
    Usa a primeira coluna cujo nome (case-insensitive) bate com url/link, senão a primeira coluna.
    """
    path = Path(caminho).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    suffix = path.suffix.lower()
    logger.info("Lendo links de leilão de %s", path)

    if suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    elif suffix == ".csv":
        df = pd.read_csv(path, encoding="utf-8-sig")
    else:
        raise ValueError(f"Formato não suportado: {suffix}. Use .csv, .xlsx ou .xls")

    if df.empty:
        logger.warning("Planilha vazia: %s", path)
        return []

    col_map = {str(c).strip().lower(): c for c in df.columns}
    chosen: Optional[str] = None
    for cand in _LINK_COLUMN_CANDIDATES:
        if cand in col_map:
            chosen = col_map[cand]
            break
    if chosen is None:
        chosen = df.columns[0]
        logger.info("Coluna de links não identificada pelo nome; usando primeira coluna: %s", chosen)

    series = df[chosen].dropna().astype(str).map(str.strip)
    links = [u for u in series.tolist() if u and u.lower() not in ("nan", "none", "")]
    logger.info("Encontrados %d links na coluna %s", len(links), chosen)
    return links


def _parse_numero_br(s: str) -> Optional[float]:
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extrair_campos_do_texto_bruto(texto: str) -> dict[str, Any]:
    """
    Heurísticas para Zuk, Mega Leilões e páginas semelhantes (rótulos em PT-BR).
    Retorna apenas o que for encontrado; campos ausentes ficam None.
    """
    t = re.sub(r"\s+", " ", texto or "")
    out: dict[str, Any] = {
        "endereco": None,
        "area_util": None,
        "area_total": None,
        "quartos": None,
        "vagas": None,
        "valor_arrematacao": None,
        "data_leilao": None,
        "tipo_imovel": None,
        "conservacao": None,
        "tipo_casa": None,
        "andar": None,
    }

    tl = t.lower()
    if re.search(r"\bapto\b|\bapartamento\b|\bflat\b|\bcobertura\b", tl):
        out["tipo_imovel"] = "apartamento"
    elif re.search(r"\bterreno\b|\blote\b|\bgleba\b|\bch[áa]cara\b|\bs[íi]tio\b", tl):
        out["tipo_imovel"] = "terreno"
    elif re.search(r"\bcasa\b|\bsobrado\b|\bresidência\b|\bresidencia\b", tl):
        out["tipo_imovel"] = "casa"
    if re.search(r"\bsobrado\b|\bduplex\b|\btriplex\b", tl):
        out["tipo_casa"] = "sobrado"
    elif re.search(r"\bcasa\s+t[ée]rrea\b|\bt[ée]rrea\b", tl) and out.get("tipo_imovel") == "casa":
        out["tipo_casa"] = "terrea"
    if re.search(r"\blançamento\b|\blancamento\b|\bna planta\b|\bnovo\b", tl):
        out["conservacao"] = "novo"
    elif re.search(r"\busado\b|\brevenda\b", tl):
        out["conservacao"] = "usado"
    m_and = re.search(
        r"(?:andar|pavimento|nível|nivel)\s*[:\s]*(\d{1,2})\b",
        t,
        re.IGNORECASE,
    )
    if m_and:
        try:
            out["andar"] = int(m_and.group(1))
        except ValueError:
            pass
    if out["andar"] is None:
        m_ordinal = re.search(r"\b(\d{1,2})[ºªo]?\s*andar\b", tl)
        if m_ordinal:
            try:
                out["andar"] = int(m_ordinal.group(1))
            except ValueError:
                pass

    # Área (m²)
    m_area = re.search(
        r"(\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+(?:,\d+)?)\s*m\s*[²2]",
        t,
        re.IGNORECASE,
    )
    if m_area:
        out["area_util"] = _parse_numero_br(m_area.group(1))

    # Área total / terreno (m²)
    m_area_total = re.search(
        r"(?:área\s+(?:total|do\s+terreno|terreno)|terreno|lote)\s*[:\s]*(\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+(?:,\d+)?)\s*m\s*[²2]",
        t,
        re.IGNORECASE,
    )
    if m_area_total:
        out["area_total"] = _parse_numero_br(m_area_total.group(1))

    # Quartos / dormitórios
    m_q = re.search(
        r"(\d+)\s*(?:quartos?|dorm(?:itórios?|itorios?)?)\b",
        t,
        re.IGNORECASE,
    )
    if m_q:
        out["quartos"] = int(m_q.group(1))

    # Vagas
    m_v = re.search(r"(\d+)\s*vagas?\b", t, re.IGNORECASE)
    if m_v:
        out["vagas"] = int(m_v.group(1))

    # Valor em R$ (primeira ocorrência que pareça lance/monetário)
    m_val = re.search(
        r"R\$\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?)",
        t,
        re.IGNORECASE,
    )
    if m_val:
        out["valor_arrematacao"] = f"R$ {m_val.group(1)}"

    m_dt = re.search(
        r"(?:data|dia)\s*(?:do\s*)?(?:leil[aã]o|preg[aã]o|sess[aã]o|1[ªa]?\s*pra[cç]a)"
        r"\s*[:\s]+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}|\d{4}-\d{2}-\d{2})",
        t,
        re.IGNORECASE,
    )
    if m_dt:
        out["data_leilao"] = normalizar_data_leilao_para_iso(m_dt.group(1))

    # Endereço: trecho com CEP brasileiro
    m_cep = re.search(
        r"([A-Za-zÀ-ú0-9.,\s\-–]+?\d{5}\s*-\s*\d{3}[A-Za-zÀ-ú0-9.,\s\-–]*)",
        t,
    )
    if m_cep:
        cand = m_cep.group(1).strip(" ,.;")
        if len(cand) > 10:
            out["endereco"] = cand[:500]

    # Terrenos: a área genérica extraída é area_total, não area_util
    if out.get("tipo_imovel") == "terreno":
        if out.get("area_util") and not out.get("area_total"):
            out["area_total"] = out["area_util"]
        out["area_util"] = None

    return out


def _extrair_via_playwright_sync(url_leilao: str, timeout_ms: int = 60_000) -> dict[str, Any]:
    """Navega com Playwright + stealth e monta o payload bruto."""
    resultado: dict[str, Any] = {
        "url_leilao": url_leilao.strip(),
        "endereco": None,
        "area_util": None,
        "area_total": None,
        "quartos": None,
        "vagas": None,
        "valor_arrematacao": None,
        "data_leilao": None,
        "detalhes_brutos": None,
    }

    stealth = Stealth()
    with stealth.use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                locale="pt-BR",
                timezone_id="America/Sao_Paulo",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            logger.info("Playwright: carregando %s", url_leilao)
            page.goto(url_leilao, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1_500)
            texto = page.inner_text("body", timeout=timeout_ms)
            resultado.update(extrair_campos_do_texto_bruto(texto))
            resultado["detalhes_brutos"] = texto[:8000] if texto else None
        finally:
            browser.close()

    return resultado


@tool(
    show_result=True,
    instructions=(
        "Use para obter dados técnicos brutos de uma página de leilão (Zuk, Mega Leilões, etc.). "
        "Passe a URL completa do lote. Depois valide com persistir_leilao_imovel_json."
    ),
)
def extrair_dados_leilao_playwright(url_leilao: str) -> str:
    """
    Abre o link com Playwright e o plugin stealth, lê o HTML renderizado e extrai:
    endereço, m², quartos, vagas e valor do lance atual (quando detectáveis).
    Retorna JSON para o modelo ou para persistir_leilao_imovel_json.
    """
    try:
        dados = _extrair_via_playwright_sync(url_leilao)
        logger.info("Extração concluída para %s", url_leilao)
        return json.dumps(dados, ensure_ascii=False)
    except Exception:
        logger.exception("Falha ao extrair dados de %s", url_leilao)
        return json.dumps(
            {"erro": "falha_extracao", "url_leilao": url_leilao, "mensagem": "ver logs"},
            ensure_ascii=False,
        )


def get_supabase_client() -> Client:
    """
    Ordem: SUPABASE_SERVICE_ROLE_KEY → SUPABASE_KEY → SUPABASE_ANON_KEY.
    Com RLS no Supabase, inserts/updates do pipeline exigem service_role no backend
    (a role `anon` só passa se houver políticas explícitas).
    """
    url = os.getenv("SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
    )
    if not url or not key:
        raise RuntimeError(
            "Defina SUPABASE_URL e uma chave JWT no .env: "
            "SUPABASE_SERVICE_ROLE_KEY (recomendado com RLS) ou SUPABASE_KEY / SUPABASE_ANON_KEY"
        )
    global _supabase_anon_role_warned
    role = supabase_jwt_role_from_key(key)
    if role == "anon" and not _supabase_anon_role_warned:
        _supabase_anon_role_warned = True
        logger.warning(
            "Supabase está com JWT role=anon. Com RLS ativo, INSERT em leilao_imoveis falha (42501). "
            "Crie no .env: SUPABASE_SERVICE_ROLE_KEY=<secret do painel API → service_role> "
            "(não commite; só backend)."
        )
    return create_client(url, key)


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _supabase_url_existe(client: Client, url_leilao: str) -> bool:
    resp = (
        client.table(SUPABASE_TABLE)
        .select("id")
        .eq("url_leilao", url_leilao)
        .limit(1)
        .execute()
    )
    rows = getattr(resp, "data", None) or []
    return len(rows) > 0


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _supabase_inserir(client: Client, payload: dict[str, Any]) -> Any:
    return client.table(SUPABASE_TABLE).insert(payload).execute()


def persistir_leilao_imovel_supabase(
    registro: LeilaoImovelCreate,
    client: Optional[Client] = None,
) -> dict[str, Any]:
    """
    Valida com Pydantic (já feito no modelo), evita duplicata por `url_leilao` e insere.
    """
    cli = client or get_supabase_client()
    url = registro.url_leilao
    if _supabase_url_existe(cli, url):
        logger.info("URL já existente em %s, ignorando insert: %s", SUPABASE_TABLE, url)
        return {"status": "skipped_duplicate", "url_leilao": url}

    row = registro.model_dump(exclude_none=True)
    logger.info("Inserindo em %s: %s", SUPABASE_TABLE, url)
    resp = _supabase_inserir(cli, row)
    data = getattr(resp, "data", None)
    novo_id = None
    if isinstance(data, list) and data:
        novo_id = data[0].get("id")
    elif isinstance(data, dict):
        novo_id = data.get("id")
    return {
        "status": "inserted",
        "url_leilao": url,
        "id": novo_id,
        "response": data,
    }


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _supabase_update_campos(client: Client, imovel_id: str, campos: dict[str, Any]) -> Any:
    return client.table(SUPABASE_TABLE).update(campos).eq("id", imovel_id).execute()


def atualizar_leilao_imovel_campos(
    imovel_id: str,
    campos: dict[str, Any],
    client: Optional[Client] = None,
) -> None:
    """Atualiza campos arbitrários (status, cidade, bairro, notas, etc.)."""
    cli = client or get_supabase_client()
    _supabase_update_campos(cli, imovel_id, campos)
    logger.info("Atualizado id=%s campos=%s", imovel_id, list(campos.keys()))


def ingerir_url_leilao(
    url_leilao: str,
    metadados_planilha: Optional[dict[str, Any]] = None,
    client: Optional[Client] = None,
) -> dict[str, Any]:
    """
    Pipeline sem LLM: Playwright + merge opcional (cidade, bairro da planilha) + insert.
    `metadados_planilha` pode conter: cidade, bairro, quartos, status inicial, etc.
    """
    url_leilao = url_leilao.strip()
    if url_leilao and not url_leilao.lower().startswith(("http://", "https://")):
        url_leilao = "https://" + url_leilao

    cli = client or get_supabase_client()
    if _supabase_url_existe(cli, url_leilao):
        resp = (
            cli.table(SUPABASE_TABLE)
            .select("id,url_leilao,status")
            .eq("url_leilao", url_leilao)
            .limit(1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        row0 = rows[0] if rows else {}
        return {
            "status": "skipped_duplicate",
            "url_leilao": url_leilao,
            "id": row0.get("id"),
        }

    raw = _extrair_via_playwright_sync(url_leilao)
    raw.pop("detalhes_brutos", None)
    if metadados_planilha:
        for k, v in metadados_planilha.items():
            if v is not None and str(v).strip() != "":
                raw[k] = v

    if raw.get("latitude") is None or raw.get("longitude") is None:
        try:
            from geocoding import geocodificar_endereco
            coords = geocodificar_endereco(
                logradouro=str(raw.get("endereco") or ""),
                bairro=str(raw.get("bairro") or ""),
                cidade=str(raw.get("cidade") or ""),
                estado=str(raw.get("estado") or ""),
            )
            if coords:
                raw["latitude"] = coords[0]
                raw["longitude"] = coords[1]
                logger.info("Leilão geocodificado: %.6f, %.6f", coords[0], coords[1])
        except Exception:
            logger.debug("Geocodificação do leilão falhou", exc_info=True)

    registro = LeilaoImovelCreate.model_validate(raw)
    return persistir_leilao_imovel_supabase(registro, client=cli)


@tool(
    show_result=True,
    instructions=(
        "Recebe um JSON com os campos do imóvel (url_leilao obrigatório). "
        "Use após extrair_dados_leilao_playwright; pode remover detalhes_brutos antes de salvar."
    ),
)
def persistir_leilao_imovel_json(payload_json: str) -> str:
    """
    Parseia JSON, valida com Pydantic (LeilaoImovelCreate) e grava na tabela leilao_imoveis
    se url_leilao ainda não existir.
    """
    try:
        raw = json.loads(payload_json)
        if isinstance(raw, dict) and "erro" in raw:
            return json.dumps(
                {"status": "error", "detail": "payload contém erro de extração"},
                ensure_ascii=False,
            )
        raw.pop("detalhes_brutos", None)
        if "area_m2" in raw and raw.get("area_util") is None:
            raw["area_util"] = raw.pop("area_m2")
        if raw.get("valor_arrematacao") is None and raw.get("valor_lance_atual") is not None:
            raw["valor_arrematacao"] = raw.pop("valor_lance_atual")
        elif "valor_lance_atual" in raw and raw.get("valor_arrematacao") is None:
            raw["valor_arrematacao"] = raw.pop("valor_lance_atual")
        registro = LeilaoImovelCreate.model_validate(raw)
        result = persistir_leilao_imovel_supabase(registro)
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        logger.exception("Falha ao persistir imóvel")
        return json.dumps({"status": "error", "detail": str(e)}, ensure_ascii=False)


def create_ingestion_agent(
    *,
    model_id: Optional[str] = None,
    markdown: bool = True,
) -> Agent:
    """Instancia o agente Agno com ferramentas de extração e persistência."""
    mid = model_id or os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
    return Agent(
        model=OpenAIChat(id=mid),
        tools=[extrair_dados_leilao_playwright, persistir_leilao_imovel_json],
        instructions=(
            "Você é um agente de ingestão de imóveis em leilão. "
            "Para cada URL: chame extrair_dados_leilao_playwright, depois persistir_leilao_imovel_json "
            "com o JSON retornado (sem detalhes_brutos). Campos no BD: area_util, area_total, valor_arrematacao, data_leilao, cidade, estado, bairro. "
            "Se a extração retornar erro, registre e siga para o próximo link. "
            "Responda de forma objetiva em português."
        ),
        markdown=markdown,
    )


__all__ = [
    "LeilaoImovelCreate",
    "atualizar_leilao_imovel_campos",
    "create_ingestion_agent",
    "extrair_dados_leilao_playwright",
    "ingerir_url_leilao",
    "ler_links_leilao_arquivo",
    "persistir_leilao_imovel_json",
    "persistir_leilao_imovel_supabase",
    "get_supabase_client",
    "supabase_jwt_role_from_key",
]
