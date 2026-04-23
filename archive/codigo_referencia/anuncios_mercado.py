"""
Anúncios de mercado (Supabase `anuncios_mercado`): comparáveis de venda coletados na web.

Fluxo: antes de depender só de buscas efêmeras, consulta anúncios similares no banco;
se faltar volume ou os dados estiverem velhos, dispara Firecrawl (ou DDGS como fallback),
extrai preço/m² quando possível e persiste só linhas com os campos mínimos exigidos.
"""

from __future__ import annotations

import difflib
import json
import logging
import math
import os
import re
import statistics
import threading
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, NamedTuple, Optional
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from postgrest.exceptions import APIError
from pydantic import BaseModel, ConfigDict, Field, field_validator

from geocoding import (
    _extrair_logradouro_de_url,
    geocodificar_anuncios_batch,
    geocodificar_endereco,
)
from ingestion_agent import atualizar_leilao_imovel_campos, get_supabase_client
from leilao_constants import (
    STATUS_PENDENTE,
    area_efetiva_de_registro,
    faixa_area_de_metragem,
    limites_faixa_area,
    normalizar_tipo_imovel,
    segmento_mercado_de_registro,
)
from token_efficiency import (
    CacheMediaBairroSalvar,
    geo_bucket_de_registro,
    id_cache_media_por_chave_segmento,
    merge_segmento_mercado,
    normalizar_chave_bairro,
    normalizar_chave_segmento,
    salvar_media_bairro_no_cache,
)

load_dotenv()

logger = logging.getLogger(__name__)


class ColetaVivaRealListagemResult(NamedTuple):
    """Resultado de `coletar_vivareal_listagem`: quantidade salva e se a página não trouxe listagem válida."""

    salvos: int
    markdown_insuficiente: bool = False


class ColetaAnunciosMercadoResult(NamedTuple):
    """Resultado de `coletar_e_persistir_via_ddgs`."""

    salvos: int
    vivareal_markdown_insuficiente: bool = False


TABLE_ANUNCIOS_MERCADO = "anuncios_mercado"
TABLE_BAIRROS_VIVAREAL = "bairros_vivareal"

_COLUNAS_TABELA_ANUNCIOS = frozenset({
    "url_anuncio", "portal", "tipo_imovel", "logradouro", "bairro", "cidade",
    "estado", "nome_condominio", "area_construida_m2", "valor_venda",
    "transacao", "titulo", "quartos", "preco_m2", "metadados_json",
    "primeiro_visto_em", "ultima_coleta_em",
    "latitude", "longitude",
})

_missing_table_logged = False

try:
    from firecrawl import Firecrawl
except ImportError:
    Firecrawl = None  # type: ignore[assignment,misc]

try:
    from ddgs import DDGS
except ImportError:
    DDGS = None


def _is_table_missing(exc: BaseException) -> bool:
    if isinstance(exc, APIError):
        if exc.code == "PGRST205":
            return True
        msg = (exc.message or "") + str(exc)
        return TABLE_ANUNCIOS_MERCADO in msg and "could not find the table" in msg.lower()
    return False


def _log_table_missing_once() -> None:
    global _missing_table_logged
    if _missing_table_logged:
        return
    _missing_table_logged = True
    logger.warning(
        "Tabela %r ausente — contexto de anúncios usa só busca web. Crie com supabase_ddls_leilao_ia.sql.",
        TABLE_ANUNCIOS_MERCADO,
    )


def chave_estado(estado: str) -> str:
    """Mesmo texto usado no insert e na busca (evita divergência SP vs nome por extenso)."""
    return _normalizar_estado(estado)


_UF_POR_NOME = {
    "acre": "AC",
    "alagoas": "AL",
    "amapa": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceara": "CE",
    "distrito federal": "DF",
    "espirito santo": "ES",
    "goias": "GO",
    "maranhao": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "para": "PA",
    "paraiba": "PB",
    "parana": "PR",
    "pernambuco": "PE",
    "piaui": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rondonia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "sao paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}


def _slug_texto(s: str) -> str:
    txt = unicodedata.normalize("NFKD", str(s or "").strip()).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"\s+", " ", txt).strip().lower()
    return txt


def _normalizar_estado(estado: Any) -> str:
    s = _slug_texto(str(estado or ""))
    if not s:
        return ""
    if len(s) == 2 and s.isalpha():
        return s.upper()
    uf = _UF_POR_NOME.get(s)
    if uf:
        return uf
    return s.upper()


def _normalizar_cidade(cidade: Any) -> str:
    return _slug_texto(str(cidade or ""))


def _normalizar_bairro(bairro: Any) -> str:
    return _slug_texto(str(bairro or ""))


def _bairro_compativel(alvo: str, candidato: str) -> bool:
    a = _normalizar_bairro(alvo)
    c = _normalizar_bairro(candidato)
    if not a or not c:
        return False
    return a == c or a in c or c in a


def _anuncio_bate_localizacao_referencia(
    *,
    cidade_ref: str,
    estado_ref: str,
    bairro_ref: str,
    cidade_extraida: str,
    estado_extraido: str,
    bairro_extraido: str,
    exigir_bairro_match: bool = True,
    permitir_localizacao_assumida: bool = False,
) -> tuple[bool, str]:
    """
    Garante que o anúncio coletado pertence à mesma localização do imóvel analisado.
    Exige ao menos cidade ou estado extraídos para evitar persistir hits ambíguos.
    """
    cid_ref = _normalizar_cidade(cidade_ref)
    uf_ref = _normalizar_estado(estado_ref)
    bai_ref = _normalizar_bairro(bairro_ref)
    cid_ext = _normalizar_cidade(cidade_extraida)
    uf_ext = _normalizar_estado(estado_extraido)
    bai_ext = _normalizar_bairro(bairro_extraido)

    if not cid_ext and not uf_ext:
        if permitir_localizacao_assumida:
            return True, "localizacao_assumida_por_contexto"
        return False, "sem_cidade_estado_extraidos"
    if cid_ext and cid_ref and cid_ext != cid_ref:
        return False, "cidade_divergente"
    if uf_ext and uf_ref and uf_ext != uf_ref:
        return False, "estado_divergente"
    if exigir_bairro_match and bai_ref and bai_ext and not _bairro_compativel(bai_ref, bai_ext):
        return False, "bairro_divergente"
    return True, ""


def _snippet_sugere_localizacao(
    *,
    title: str,
    body: str,
    cidade_ref: str,
    estado_ref: str,
    bairro_ref: str = "",
) -> bool:
    """
    Pré-filtro barato para reduzir fetch HTTP de hits fora da praça.
    """
    txt = _slug_texto(f"{title} {body}")
    cid = _normalizar_cidade(cidade_ref)
    uf = _normalizar_estado(estado_ref).lower()
    bai = _normalizar_bairro(bairro_ref)
    if bai and bai in txt:
        return True
    if cid and cid in txt:
        return True
    if uf and re.search(rf"(^|[^a-z0-9]){re.escape(uf)}([^a-z0-9]|$)", txt):
        return True
    return False


def _extrair_dados_minimos_de_snippet(title: str, body: str) -> Optional[dict[str, Any]]:
    """
    Fallback rápido: usa o texto do próprio resultado DDGS quando já houver preço e área.
    """
    blob = f"{title or ''} {body or ''}".strip()
    if not blob:
        return None
    valores = extrair_valores_rs_brl(blob)
    area = extrair_area_m2(blob)
    if not valores or area is None:
        return None
    quartos = None
    vagas = None
    if m := _re_qt.search(blob):
        quartos = _parse_int_any(m.group(1))
    if m := _re_vg.search(blob):
        vagas = _parse_int_any(m.group(1))
    return {
        "valor_venda": float(max(valores)),
        "area_m2": float(area),
        "quartos": quartos,
        "vagas": vagas,
        "titulo": (title or "").strip()[:500] or None,
    }


class AnuncioMercadoPersist(BaseModel):
    """Linha válida para insert/update em `anuncios_mercado`."""

    model_config = ConfigDict(str_strip_whitespace=True)

    url_anuncio: str
    portal: str
    tipo_imovel: str
    logradouro: str = ""
    bairro: str
    cidade: str
    estado: str
    nome_condominio: Optional[str] = None
    area_construida_m2: float = Field(..., gt=0)
    valor_venda: float = Field(..., gt=0)
    transacao: str = Field(default="venda")
    titulo: Optional[str] = None
    quartos: Optional[int] = None
    preco_m2: Optional[float] = None
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    metadados_json: dict[str, Any] = Field(default_factory=dict)

    @field_validator("url_anuncio")
    @classmethod
    def _url_http(cls, v: str) -> str:
        u = (v or "").strip()
        if not u.lower().startswith(("http://", "https://")):
            raise ValueError("url_anuncio deve ser http(s)")
        return u

    @field_validator("transacao")
    @classmethod
    def _tx(cls, v: str) -> str:
        x = (v or "venda").strip().lower()
        if x not in ("venda", "aluguel"):
            raise ValueError("transacao deve ser venda ou aluguel")
        return x

    def row_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        a, v = float(d["area_construida_m2"]), float(d["valor_venda"])
        d["preco_m2"] = d.get("preco_m2") or round(v / a, 2)
        return {k: v for k, v in d.items() if k in _COLUNAS_TABELA_ANUNCIOS}


def portal_de_url(url: str) -> str:
    try:
        net = urlparse(url).netloc.lower().replace("www.", "")
        return net[:200] if net else "desconhecido"
    except Exception:
        return "desconhecido"


_SKIP_DOMAINS = (
    "google.",
    "gstatic.",
    "bing.",
    "facebook.",
    "instagram.",
    "youtube.",
    "twitter.",
    "wikipedia.",
)

_PORTAIS_PRIORITARIOS = (
    "vivareal.com.br",
    "zapimoveis.com.br",
)


def _portal_score_confianca(url_ou_portal: str) -> int:
    """Score simples para priorizar resultados de portais confiáveis."""
    p = (url_ou_portal or "").lower().replace("www.", "").strip()
    if not p:
        return 0
    if p.startswith(("http://", "https://")):
        p = portal_de_url(p)
    if any(dom in p for dom in _PORTAIS_PRIORITARIOS):
        return 3
    return 1


def _portal_prioritario(url_ou_portal: str) -> bool:
    return _portal_score_confianca(url_ou_portal) >= 3


def _portal_aceito_pipeline(url_ou_portal: str) -> bool:
    """
    Gate de qualidade do pipeline: só aceita portais confiáveis.
    """
    return _portal_prioritario(url_ou_portal)


def _faixa_pm2_dinamica(pm2_vals: list[float]) -> tuple[Optional[float], Optional[float]]:
    """
    Faixa dinâmica de sanidade para reduzir outliers com base no histórico local.
    """
    vals = [float(v) for v in pm2_vals if v and v > 0]
    if len(vals) < 5:
        return None, None
    p10 = _percentil(vals, 0.10)
    p90 = _percentil(vals, 0.90)
    if p10 is None or p90 is None or p10 <= 0 or p90 <= 0:
        return None, None
    lo = max(250.0, float(p10) * 0.6)
    hi = min(250_000.0, float(p90) * 1.7)
    if lo >= hi:
        return None, None
    return round(lo, 2), round(hi, 2)


def _anuncio_passa_filtro_sanidade(
    *,
    valor_venda: float,
    area_m2: float,
    pm2_min: Optional[float],
    pm2_max: Optional[float],
) -> tuple[bool, str]:
    """
    Filtros duros para evitar comparáveis absurdos.
    """
    if valor_venda <= 0 or area_m2 <= 0:
        return False, "valor_ou_area_invalido"
    if valor_venda < 30_000 or valor_venda > 120_000_000:
        return False, "valor_fora_faixa"
    if area_m2 < 12 or area_m2 > 20_000:
        return False, "area_fora_faixa"
    pm2 = valor_venda / area_m2
    if pm2 < 250 or pm2 > 250_000:
        return False, "pm2_fora_faixa_global"
    if pm2_min is not None and pm2 < pm2_min:
        return False, "pm2_abaixo_faixa_referencia"
    if pm2_max is not None and pm2 > pm2_max:
        return False, "pm2_acima_faixa_referencia"
    return True, ""


def url_parece_anuncio_imoveis(url: str) -> bool:
    u = (url or "").lower()
    if not u.startswith("http"):
        return False
    if any(x in u for x in _SKIP_DOMAINS):
        return False
    return _portal_aceito_pipeline(u)


_re_rs = re.compile(r"R\$\s*([\d]{1,3}(?:\.[\d]{3})*(?:,[\d]+)?|[\d]+(?:,[\d]+)?)", re.IGNORECASE)
_re_m2 = re.compile(r"(\d{1,4}(?:[.,]\d+)?)\s*m\s*[²2]", re.IGNORECASE)
_re_qt = re.compile(r"(\d{1,2})\s*(?:quartos?|dormit[óo]rios?)", re.IGNORECASE)
_re_vg = re.compile(r"(\d{1,2})\s*(?:vagas?)", re.IGNORECASE)
_re_cond = re.compile(
    r"(?:cond(?:om[ií]nio)?|residencial)\s+([A-Za-zÀ-ú0-9\s\-]{3,80}?)(?:\s*[,\-|]|\s*$)",
    re.IGNORECASE,
)
_re_json_ld = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_re_meta_pubdate = re.compile(
    r"<meta[^>]+(?:property|name)=[\"'](?:article:published_time|article:modified_time|datePublished|dateModified)[\"'][^>]+content=[\"']([^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
_re_strip_tags = re.compile(r"<[^>]+>")

_UF_ABREVIADOS_VIVAREAL = {"SP", "RJ"}

_UF_SLUG_VIVAREAL: dict[str, list[str]] = {}
for _nome, _uf in _UF_POR_NOME.items():
    _slug_extenso = _nome.replace(" ", "-")
    _uf_lower = _uf.lower()
    if _uf in _UF_ABREVIADOS_VIVAREAL:
        _UF_SLUG_VIVAREAL[_uf] = [_uf_lower]
    else:
        _UF_SLUG_VIVAREAL[_uf] = [_slug_extenso]

_UF_PARA_ESTADO_EXTENSO = {uf: slugs[0] for uf, slugs in _UF_SLUG_VIVAREAL.items()}

_TIPO_IMOVEL_VIVAREAL_PATH = {
    "apartamento": "apartamento_residencial",
    "casa": "casa_residencial",
    "casa_condominio": "condominio_residencial",
    "terreno": "lote-terreno_residencial",
    "comercial": "comercial",
}

_TIPOS_QUE_INCLUEM_TERRENO = ("casa", "casa_condominio")
_VIVAREAL_TIPOS_COMBINADOS = {
    "casa": "casa_residencial,lote-terreno_residencial",
    "casa_condominio": "condominio_residencial,lote-terreno_residencial",
}

_cache_bairros_vivareal: dict[str, list[str]] = {}
_cache_bairros_lock = threading.Lock()
_re_bairro_slug = re.compile(r"/bairros/([a-z0-9][a-z0-9\-]+[a-z0-9])/")


def _carregar_bairros_do_banco(uf: str, cidade_norm: str, client: Any = None) -> list[str]:
    """Lê bairros já salvos no Supabase para a cidade. Custo: 0 créditos."""
    try:
        cli = client or get_supabase_client()
        resp = (
            cli.table(TABLE_BAIRROS_VIVAREAL)
            .select("slug")
            .eq("estado", uf)
            .eq("cidade", cidade_norm)
            .order("slug")
            .limit(2000)
            .execute()
        )
        return [r["slug"] for r in (resp.data or []) if r.get("slug")]
    except Exception:
        logger.debug("Tabela %s indisponível ou erro ao ler", TABLE_BAIRROS_VIVAREAL, exc_info=True)
        return []


def _salvar_bairros_no_banco(uf: str, cidade_norm: str, slugs: list[str], client: Any = None) -> int:
    """Persiste slugs de bairros no Supabase (upsert). Retorna quantidade salva."""
    if not slugs:
        return 0
    cli = client or get_supabase_client()
    rows = [
        {
            "estado": uf,
            "cidade": cidade_norm,
            "slug": s,
            "nome_humanizado": slug_bairro_para_nome(s),
            "atualizado_em": datetime.now(timezone.utc).isoformat(),
        }
        for s in slugs
    ]
    salvos = 0
    for batch in [rows[i:i + 50] for i in range(0, len(rows), 50)]:
        try:
            cli.table(TABLE_BAIRROS_VIVAREAL).upsert(
                batch, on_conflict="estado,cidade,slug"
            ).execute()
            salvos += len(batch)
        except Exception:
            logger.debug("Falha ao salvar bairros VivaReal no banco", exc_info=True)
    if salvos:
        logger.info("Bairros VivaReal salvos no banco: %s/%s para %s/%s", salvos, len(slugs), uf, cidade_norm)
    return salvos


def _descobrir_bairros_vivareal(estado: str, cidade: str) -> list[str]:
    """Retorna slugs de bairros do VivaReal para a cidade.
    Prioridade: memória → banco de dados → Firecrawl scrape (1 crédito, salva no banco).
    Tenta todas as variantes de slug do estado (ex: rs, rio-grande-do-sul)."""
    uf = _normalizar_estado(estado)
    slugs_estado = _UF_SLUG_VIVAREAL.get(uf, [])
    if not slugs_estado:
        return []
    cidade_slug = _slug_texto(cidade).replace(" ", "-")
    if not cidade_slug:
        return []
    cache_key = f"{slugs_estado[0]}/{cidade_slug}"

    with _cache_bairros_lock:
        if cache_key in _cache_bairros_vivareal:
            return _cache_bairros_vivareal[cache_key]

    db_slugs = _carregar_bairros_do_banco(uf, cidade_slug)
    if db_slugs:
        with _cache_bairros_lock:
            _cache_bairros_vivareal[cache_key] = db_slugs
        logger.info("Bairros VivaReal carregados do banco: %s para %s/%s", len(db_slugs), uf, cidade_slug)
        return db_slugs

    fc = _get_firecrawl_client()
    if fc is None:
        return []

    result = None
    markdown = ""
    for slug_est in slugs_estado:
        url = f"https://www.vivareal.com.br/venda/{slug_est}/{cidade_slug}/"
        try:
            result = fc.scrape(url, formats=["markdown"])
            _contabilizar_creditos(1)
            d = _firecrawl_result_to_dict(result) if result else {}
            markdown = str(d.get("markdown") or "").strip()
            if markdown and _re_bairro_slug.search(markdown):
                logger.info("Bairros VivaReal encontrados via: %s", url[:100])
                break
            logger.info("VivaReal sem bairros via %s, tentando variante...", url[:80])
            markdown = ""
        except Exception as exc:
            _contabilizar_creditos(1)
            if _marcar_creditos_esgotados(exc):
                return []
            logger.info("Firecrawl falhou via %s, tentando variante...", url[:80])
            continue

    if not markdown:
        return []
    slugs: list[str] = []
    seen: set[str] = set()
    for m in _re_bairro_slug.finditer(markdown):
        s = m.group(1)
        if s not in seen:
            seen.add(s)
            slugs.append(s)

    if slugs:
        _salvar_bairros_no_banco(uf, cidade_slug, slugs)

    with _cache_bairros_lock:
        _cache_bairros_vivareal[cache_key] = slugs
    logger.info("VivaReal bairros descobertos via Firecrawl em %s: %s", cache_key, len(slugs))
    return slugs


def _resolver_bairro_vivareal(bairro_digitado: str, bairros_disponiveis: list[str]) -> str:
    """Fuzzy match do bairro digitado contra slugs disponíveis no VivaReal.
    Retorna o slug mais próximo ou string vazia se nenhum match bom."""
    if not bairro_digitado or not bairros_disponiveis:
        return ""
    slug_input = _slug_texto(bairro_digitado).replace(" ", "-")
    if slug_input in bairros_disponiveis:
        return slug_input
    for b in bairros_disponiveis:
        if slug_input in b or b in slug_input:
            return b
    nomes_input = set(slug_input.split("-"))
    nomes_input.discard("")
    if nomes_input:
        best_score = 0.0
        best_slug = ""
        for b in bairros_disponiveis:
            partes_b = set(b.split("-"))
            inter = nomes_input & partes_b
            if inter:
                score = len(inter) / max(len(nomes_input), len(partes_b))
                if score > best_score:
                    best_score = score
                    best_slug = b
        if best_score >= 0.4:
            return best_slug
    matches = difflib.get_close_matches(slug_input, bairros_disponiveis, n=1, cutoff=0.5)
    if matches:
        return matches[0]
    return ""


def _montar_urls_vivareal(
    estado: str,
    cidade: str,
    bairro: str,
    tipo_imovel: str,
    bairro_slug_override: str = "",
    incluir_terrenos: bool = False,
    area_minima: int | None = None,
    area_maxima: int | None = None,
) -> list[str]:
    """Monta URLs de listagem do VivaReal tentando todas as variantes de slug do estado.
    Suporta filtro de área via parâmetros areaMinima/areaMaxima.
    Retorna lista de URLs candidatas (a primeira que funcionar será usada)."""
    uf = _normalizar_estado(estado)
    slugs_estado = _UF_SLUG_VIVAREAL.get(uf, [])
    if not slugs_estado:
        return []
    cidade_slug = _slug_texto(cidade).replace(" ", "-")
    if not cidade_slug:
        return []
    tipo_path = _TIPO_IMOVEL_VIVAREAL_PATH.get(tipo_imovel, "")
    bairro_slug = bairro_slug_override or (_slug_texto(bairro).replace(" ", "-") if bairro else "")

    urls: list[str] = []
    for slug_est in slugs_estado:
        base = f"https://www.vivareal.com.br/venda/{slug_est}/{cidade_slug}/"
        if bairro_slug:
            base += f"bairros/{bairro_slug}/"
        if tipo_path:
            base += f"{tipo_path}/"

        params: list[str] = []
        if incluir_terrenos and tipo_imovel in _TIPOS_QUE_INCLUEM_TERRENO:
            tipos_param = _VIVAREAL_TIPOS_COMBINADOS.get(tipo_imovel)
            if tipos_param:
                params.append(f"tipos={tipos_param}")
        if area_minima is not None:
            params.append(f"areaMinima={area_minima}")
        if area_maxima is not None:
            params.append(f"areaMaxima={area_maxima}")
        if params:
            base += "?" + "&".join(params)
        urls.append(base)
    return urls


_RE_URL_TERRENO = re.compile(r"/(?:lote-terreno|terreno|lote)/", re.IGNORECASE)


def _detectar_tipo_por_card(url: str, titulo: str, block: str) -> str:
    """Detecta se o card é de terreno ou casa com base na URL e conteúdo."""
    if _RE_URL_TERRENO.search(url):
        return "terreno"
    bl = (titulo + " " + block).lower()
    if any(x in bl for x in ("terreno", "lote ", "loteamento")):
        if not any(x in bl for x in ("casa", "sobrado", "apartamento")):
            return "terreno"
    return ""


def _parse_cards_vivareal(markdown: str, cidade_ref: str, estado_ref: str, bairro_ref: str) -> list[dict[str, Any]]:
    """
    Extrai anúncios estruturados do markdown de uma listagem VivaReal.
    Cada card traz: preço, área, quartos, banheiros, vagas, rua, URL do anúncio.
    Detecta automaticamente se o card é de terreno (via URL ou conteúdo).
    """
    anuncios: list[dict[str, Any]] = []
    cards = markdown.split("Contatar](")
    for i, block in enumerate(cards[:-1]):
        try:
            url_part = cards[i + 1].split(")")[0].split('"')[0].strip()
            if not url_part.startswith("http"):
                url_part = "https://www.vivareal.com.br" + url_part

            area_m = re.search(r"Tamanho do im[óo]vel\s*(\d{1,6})\s*m", block, re.IGNORECASE)
            quartos_m = re.search(r"Quantidade de quartos\s*(\d{1,2})", block, re.IGNORECASE)
            banheiros_m = re.search(r"Quantidade de banheiros\s*(\d{1,2})", block, re.IGNORECASE)
            vagas_m = re.search(r"Quantidade de vagas[^\d]*(\d{1,2})", block, re.IGNORECASE)

            preco_m = re.search(r"R\$\s*([\d.]+(?:,\d+)?)\s*\n", block)
            if not preco_m:
                preco_m = re.search(r"R\$\s*([\d.]+(?:,\d+)?)", block)

            if not area_m or not preco_m:
                continue

            preco_raw = preco_m.group(1).strip()
            parts = preco_raw.split(".")
            if all(len(p) == 3 for p in parts[1:]) and len(parts) > 1:
                preco_raw = preco_raw.replace(".", "")
            preco_raw = preco_raw.replace(",", ".")
            preco = float(preco_raw)
            area = float(area_m.group(1))

            if preco < 30_000 or preco > 120_000_000 or area < 12 or area > 50_000:
                continue

            rua_m = re.search(
                r"\n\s*(Rua|Avenida|Av\.|R\.|Alameda|Al\.|Travessa|Tv\.|Estrada|Rod\.|Rodovia|Largo|Praça|Pc\.|Servidão|Beco)[^\n]{3,80}",
                block, re.IGNORECASE,
            )
            logradouro = rua_m.group(0).strip() if rua_m else ""
            if not logradouro:
                addr_m = re.search(r"\n\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,5}),\s*\d", block)
                if addr_m:
                    logradouro = addr_m.group(1).strip()
            if not logradouro:
                logradouro = _extrair_logradouro_de_url(url_part)

            titulo_m = re.search(r"\*\*([^\*]+)\*\*", block)
            titulo = titulo_m.group(1).strip()[:500] if titulo_m else ""

            tipo_card = _detectar_tipo_por_card(url_part, titulo, block)

            anuncios.append({
                "url_anuncio": url_part.split("?")[0],
                "portal": "vivareal.com.br",
                "area_m2": area,
                "valor_venda": preco,
                "quartos": int(quartos_m.group(1)) if quartos_m else None,
                "vagas": int(vagas_m.group(1)) if vagas_m else None,
                "logradouro": logradouro,
                "titulo": titulo,
                "bairro": bairro_ref,
                "cidade": cidade_ref,
                "estado": chave_estado(estado_ref),
                "_tipo_detectado": tipo_card,
            })
        except (ValueError, IndexError, AttributeError):
            continue

    logger.info("Parser VivaReal extraiu %s anúncios da listagem", len(anuncios))
    return anuncios


def coletar_vivareal_listagem(
    *,
    cidade: str,
    estado: str,
    bairro: str,
    tipo_imovel: str,
    client: Any,
    tipo_norm: str = "",
    bairro_vivareal_slug: str = "",
    area_referencia_m2: float = 0,
) -> ColetaVivaRealListagemResult:
    """
    Monta URL de listagem VivaReal, faz scrape e persiste os anúncios extraídos.
    Se bairro_vivareal_slug for fornecido, usa direto (sem fuzzy match — economiza 1 crédito).
    Quando area_referencia_m2 > 0, aplica filtro de faixa de área na URL (?areaMinima=X&areaMaxima=Y).
    Se o markdown vier vazio ou sem o padrão de cards de anúncio, não tenta outra URL:
    retorna ``markdown_insuficiente=True`` e salvos 0.
    """
    if not tipo_norm:
        tipo_norm = normalizar_tipo_imovel(tipo_imovel)

    bairro_slug = ""
    bairros_disponiveis: list[str] = []
    if bairro_vivareal_slug:
        bairro_slug = bairro_vivareal_slug
        logger.info("VivaReal bairro slug fornecido diretamente: '%s'", bairro_slug)
    elif bairro:
        bairros_disponiveis = _descobrir_bairros_vivareal(estado, cidade)
        if bairros_disponiveis:
            bairro_slug = _resolver_bairro_vivareal(bairro, bairros_disponiveis)
            if bairro_slug:
                logger.info(
                    "VivaReal bairro resolvido: '%s' -> '%s' (de %s disponíveis)",
                    bairro, bairro_slug, len(bairros_disponiveis),
                )
            else:
                logger.warning(
                    "VivaReal: bairro '%s' não encontrado entre %s disponíveis em %s/%s. "
                    "Buscando sem filtro de bairro.",
                    bairro, len(bairros_disponiveis), cidade, estado,
                )

    _incluir_terrenos = tipo_norm in _TIPOS_QUE_INCLUEM_TERRENO
    area_min_vr, area_max_vr = None, None
    if area_referencia_m2 > 0:
        faixa = faixa_area_de_metragem(area_referencia_m2)
        area_min_vr, area_max_vr = limites_faixa_area(faixa)
        if area_min_vr or area_max_vr:
            logger.info(
                "VivaReal filtro de área: faixa=%s → areaMinima=%s areaMaxima=%s (ref=%.0f m²)",
                faixa, area_min_vr, area_max_vr, area_referencia_m2,
            )
    urls_candidatas = _montar_urls_vivareal(
        estado, cidade, bairro, tipo_norm,
        bairro_slug_override=bairro_slug,
        incluir_terrenos=_incluir_terrenos,
        area_minima=area_min_vr,
        area_maxima=area_max_vr,
    )
    if not urls_candidatas:
        logger.warning("Não foi possível montar URL VivaReal para %s/%s/%s", cidade, bairro, estado)
        return ColetaVivaRealListagemResult(0, False)

    fc = _get_firecrawl_client()
    if fc is None:
        return ColetaVivaRealListagemResult(0, False)

    result = None
    url = urls_candidatas[0]
    for url_tentativa in urls_candidatas:
        try:
            result = fc.scrape(url_tentativa, formats=["markdown"])
            _contabilizar_creditos(1)
            url = url_tentativa
            d_check = _firecrawl_result_to_dict(result) if result else {}
            md_check = str(d_check.get("markdown") or "").strip()
            if md_check and "Contatar](" in md_check:
                logger.info("VivaReal respondeu com anúncios via: %s", url_tentativa[:100])
                break
            logger.warning(
                "VivaReal: anúncios insuficientes — markdown vazio ou sem padrão de listagem de cards (%s).",
                url_tentativa[:120],
            )
            return ColetaVivaRealListagemResult(0, True)
        except Exception as exc:
            _contabilizar_creditos(1)
            if _marcar_creditos_esgotados(exc):
                return ColetaVivaRealListagemResult(0, False)
            logger.info("VivaReal falhou via %s, tentando variante...", url_tentativa[:80])
            continue

    if not result:
        logger.warning(
            "VivaReal: nenhuma variante de URL retornou scrape válido para %s/%s/%s",
            cidade,
            bairro,
            estado,
        )
        return ColetaVivaRealListagemResult(0, False)

    d = _firecrawl_result_to_dict(result)
    markdown = str(d.get("markdown") or "").strip()
    if not markdown:
        logger.warning(
            "VivaReal: anúncios insuficientes — markdown vazio após resposta (%s).",
            url[:120],
        )
        return ColetaVivaRealListagemResult(0, True)

    if not bairros_disponiveis:
        new_slugs: list[str] = []
        seen: set[str] = set()
        for m_b in _re_bairro_slug.finditer(markdown):
            s = m_b.group(1)
            if s not in seen:
                seen.add(s)
                new_slugs.append(s)
        if new_slugs:
            uf = _normalizar_estado(estado)
            estado_ext = _UF_PARA_ESTADO_EXTENSO.get(uf, "")
            cidade_slug = _slug_texto(cidade).replace(" ", "-")
            cache_key = f"{estado_ext}/{cidade_slug}"
            with _cache_bairros_lock:
                _cache_bairros_vivareal[cache_key] = new_slugs
            _salvar_bairros_no_banco(uf, cidade_slug, new_slugs)
            logger.info("VivaReal bairros descobertos (via listagem): %s em %s", len(new_slugs), cache_key)

    cards = _parse_cards_vivareal(markdown, cidade, estado, bairro)

    geocodificar_anuncios_batch(cards, cidade=cidade, estado=estado, bairro_fallback=bairro)

    cli = client or get_supabase_client()
    salvos = 0
    n_terrenos = 0
    for card in cards:
        try:
            tipo_card = card.pop("_tipo_detectado", "")
            tipo_final = tipo_card if tipo_card == "terreno" else (tipo_norm if tipo_norm != "desconhecido" else "casa_condominio")
            if tipo_card == "terreno":
                n_terrenos += 1

            md: dict[str, Any] = {
                "origem_vivareal_listagem": True,
                "url_listagem": url,
                "data_coleta": datetime.now(timezone.utc).date().isoformat(),
            }
            if card.get("vagas") is not None:
                md["vagas"] = card["vagas"]
            if card.get("latitude") is not None:
                md["lat"] = card["latitude"]
            if card.get("longitude") is not None:
                md["lon"] = card["longitude"]

            row = AnuncioMercadoPersist(
                url_anuncio=card["url_anuncio"],
                portal=card["portal"],
                tipo_imovel=tipo_final,
                logradouro=card.get("logradouro", ""),
                bairro=card["bairro"],
                cidade=card["cidade"],
                estado=card["estado"],
                area_construida_m2=card["area_m2"],
                valor_venda=card["valor_venda"],
                transacao="venda",
                titulo=card.get("titulo"),
                quartos=card.get("quartos") if tipo_final != "terreno" else None,
                latitude=card.get("latitude"),
                longitude=card.get("longitude"),
                metadados_json=md,
            )
            _persistir_uma_linha(cli, row)
            salvos += 1
        except Exception:
            logger.debug("Falha ao persistir anúncio VivaReal: %s", card.get("url_anuncio", "?")[:80], exc_info=True)
            continue

    terreno_info = f" ({n_terrenos} terrenos)" if n_terrenos > 0 else ""
    logger.info("VivaReal listagem %s: %s anúncios salvos de %s extraídos%s (custo 1 crédito)", url[:60], salvos, len(cards), terreno_info)
    return ColetaVivaRealListagemResult(salvos, False)


_DEFAULT_RAIO_KM = 3.0
_RAIOS_PROGRESSIVOS_KM = (3.0, 5.0, 8.0, 12.0)
_TOLERANCIA_AREA_PADRAO = 0.15
_FETCH_HTML_TIMEOUT_PADRAO_SEC = 8.0
_MAX_FETCHES_POR_QUERY = 35
_MAX_FETCHES_TOTAL = 120
_MAX_HITS_POR_QUERY = 80


def extrair_valores_rs_brl(texto: str) -> list[float]:
    out: list[float] = []
    texto = (texto or "").replace("R\\$", "R$")
    for m in _re_rs.finditer(texto):
        raw = m.group(1)
        s = raw.strip().replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "." in s:
            parts = s.split(".")
            if all(len(p) == 3 for p in parts[1:]):
                s = s.replace(".", "")
            # else: mantém como float decimal (ex: 350.5)
        else:
            s = s.replace(",", ".")
        try:
            v = float(s)
            if 30_000 <= v <= 120_000_000:
                out.append(v)
        except ValueError:
            continue
    return out


def extrair_area_m2(texto: str) -> Optional[float]:
    m = _re_m2.search(texto or "")
    if not m:
        return None
    s = m.group(1).replace(",", ".")
    try:
        a = float(s)
        if 10 <= a <= 50_000:
            return a
    except ValueError:
        pass
    return None


def extrair_logradouro_titulo(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return ""
    part = re.split(r"[,|\-|–—]", t, maxsplit=1)[0].strip()
    part = re.sub(r"^\d+\s*", "", part)
    return part[:200] if part else ""


def extrair_nome_condominio(title: str, body: str) -> Optional[str]:
    for blob in (title, body[:600]):
        m = _re_cond.search(blob or "")
        if m:
            nome = m.group(1).strip()
            if len(nome) >= 3:
                return nome[:200]
    return None


def _parse_float_any(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("R$", "").replace("r$", "").strip()
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


def _parse_int_any(v: Any) -> Optional[int]:
    f = _parse_float_any(v)
    if f is None:
        return None
    try:
        return int(round(f))
    except (TypeError, ValueError):
        return None


def _parse_iso_data(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(s)
        return d.date().isoformat()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _strip_html_text(s: str) -> str:
    return re.sub(r"\s+", " ", _re_strip_tags.sub(" ", s or "")).strip()


def _iter_jsonld_nodes(obj: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            nodes.append(x)
            graph = x.get("@graph")
            if isinstance(graph, list):
                for it in graph:
                    walk(it)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(obj)
    return nodes


def _fetch_html(url: str, timeout_sec: float = _FETCH_HTML_TIMEOUT_PADRAO_SEC) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read()
        ctype = str(resp.headers.get("Content-Type") or "").lower()
    enc = "utf-8"
    m = re.search(r"charset=([\w-]+)", ctype)
    if m:
        enc = m.group(1).strip()
    try:
        return raw.decode(enc, errors="replace")
    except LookupError:
        return raw.decode("utf-8", errors="replace")


def _extrair_estruturado_da_pagina(
    url: str,
    fallback_title: str,
    fallback_body: str,
    *,
    timeout_sec: float = _FETCH_HTML_TIMEOUT_PADRAO_SEC,
) -> Optional[dict[str, Any]]:
    try:
        html = _fetch_html(url, timeout_sec=timeout_sec)
    except (URLError, TimeoutError, ValueError, OSError):
        return None
    except Exception:
        logger.exception("Falha no fetch estruturado da página: %s", url[:120])
        return None

    dados: dict[str, Any] = {
        "url_fonte": url,
        "data_coleta": datetime.now(timezone.utc).date().isoformat(),
    }
    blobs = [fallback_title or "", fallback_body or ""]
    addr_parts: dict[str, str] = {}

    for m in _re_json_ld.finditer(html):
        txt = (m.group(1) or "").strip()
        if not txt:
            continue
        try:
            obj = json.loads(txt)
        except json.JSONDecodeError:
            continue
        for node in _iter_jsonld_nodes(obj):
            if not isinstance(node, dict):
                continue
            if "datePublished" in node and "data_anuncio" not in dados:
                if dt := _parse_iso_data(node.get("datePublished")):
                    dados["data_anuncio"] = dt
            if "dateModified" in node and "data_anuncio" not in dados:
                if dt := _parse_iso_data(node.get("dateModified")):
                    dados["data_anuncio"] = dt
            if "name" in node and not dados.get("titulo"):
                dados["titulo"] = str(node.get("name") or "").strip()[:500]
            offers = node.get("offers")
            if isinstance(offers, list):
                offers = offers[0] if offers else None
            if isinstance(offers, dict):
                if "price" in offers and dados.get("valor_venda") is None:
                    if p := _parse_float_any(offers.get("price")):
                        dados["valor_venda"] = p
                for k in ("priceCurrency", "availability", "url"):
                    if offers.get(k):
                        dados[f"offers_{k.lower()}"] = str(offers.get(k))[:200]
            floor = node.get("floorSize")
            if isinstance(floor, dict):
                if v := _parse_float_any(floor.get("value")):
                    dados["area_m2"] = v
            elif dados.get("area_m2") is None:
                if v := _parse_float_any(node.get("floorSize")):
                    dados["area_m2"] = v
            if dados.get("quartos") is None:
                if q := _parse_int_any(node.get("numberOfRooms")):
                    dados["quartos"] = q
            if dados.get("vagas") is None:
                if vg := _parse_int_any(node.get("numberOfParkingSpaces")):
                    dados["vagas"] = vg
            addr = node.get("address")
            if isinstance(addr, dict):
                if x := str(addr.get("streetAddress") or "").strip():
                    addr_parts["logradouro"] = x[:200]
                if x := str(addr.get("addressLocality") or "").strip():
                    addr_parts["cidade"] = x[:120]
                if x := str(addr.get("addressRegion") or "").strip():
                    addr_parts["estado"] = x[:80]
                if x := str(addr.get("addressNeighborhood") or "").strip():
                    addr_parts["bairro"] = x[:120]
            ptype = str(node.get("propertyType") or "").strip().lower()
            if ptype:
                blobs.append(ptype)

    if "data_anuncio" not in dados:
        if m := _re_meta_pubdate.search(html):
            if dt := _parse_iso_data(m.group(1)):
                dados["data_anuncio"] = dt
    # Fallback robusto: usar também texto limpo da página inteira (quando JSON-LD não trouxer preço/área).
    html_text = _strip_html_text(html)
    if html_text:
        blobs.append(html_text[:180000])
    text_hint = "\n".join([_strip_html_text(b) for b in blobs if b]).strip()
    fb = _extrair_dados_basicos_de_texto(text_hint, url, extrair_localizacao=True) or {}
    for k in ("valor_venda", "area_m2", "quartos", "vagas"):
        if dados.get(k) is None and fb.get(k) is not None:
            dados[k] = fb[k]
    if dados.get("logradouro") is None and addr_parts.get("logradouro"):
        dados["logradouro"] = addr_parts["logradouro"]
    for src_key, dst_key in (("bairro", "bairro_extraido"), ("cidade", "cidade_extraida"), ("estado", "estado_extraido")):
        if addr_parts.get(src_key):
            dados[dst_key] = addr_parts[src_key]
        elif fb.get(dst_key):
            dados[dst_key] = fb[dst_key]
    if "data_anuncio" not in dados:
        dados["data_anuncio"] = dados["data_coleta"]
    if dados.get("valor_venda") and dados.get("area_m2"):
        return dados
    return None


def _coords_de_row(row: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    lat = _parse_float_any(row.get("latitude") or row.get("lat"))
    lon = _parse_float_any(row.get("longitude") or row.get("lon"))
    if lat is not None and lon is not None:
        return lat, lon
    md = row.get("metadados_json")
    if isinstance(md, str):
        try:
            md = json.loads(md)
        except json.JSONDecodeError:
            md = {}
    if isinstance(md, dict):
        lat = _parse_float_any(md.get("lat") or md.get("latitude"))
        lon = _parse_float_any(md.get("lon") or md.get("longitude"))
        if lat is not None and lon is not None:
            return lat, lon
    return None, None


def _distancia_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _valor_quartos_vagas(row: dict[str, Any], key: str) -> Optional[int]:
    base = _parse_int_any(row.get(key))
    if base is not None:
        return base
    md = row.get("metadados_json")
    if isinstance(md, str):
        try:
            md = json.loads(md)
        except json.JSONDecodeError:
            md = {}
    if isinstance(md, dict):
        return _parse_int_any(md.get(key))
    return None


def _percentil(vals: list[float], q: float) -> Optional[float]:
    if not vals:
        return None
    if len(vals) == 1:
        return float(vals[0])
    xs = sorted(float(v) for v in vals)
    qn = min(1.0, max(0.0, q))
    pos = (len(xs) - 1) * qn
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(xs[lo])
    frac = pos - lo
    return float(xs[lo] + (xs[hi] - xs[lo]) * frac)


def estatisticas_comparaveis(
    comparaveis: list[dict[str, Any]],
) -> dict[str, Any]:
    """Calcula estatísticas usando TODOS os comparáveis disponíveis."""
    pm2 = _precos_m2_de_anuncios(comparaveis)
    p25 = _percentil(pm2, 0.25)
    p50 = _percentil(pm2, 0.50)
    p75 = _percentil(pm2, 0.75)
    p90 = _percentil(pm2, 0.90)
    pm2_max = max(pm2) if pm2 else None
    spread_pct = None
    if p50 and p50 > 0 and p25 is not None and p75 is not None:
        spread_pct = (p75 - p25) / p50
    scores = [float(x.get("_score_similaridade") or 0.0) for x in comparaveis]
    score_medio = statistics.mean(scores) if scores else 0.0
    comp_amostra = min(1.0, len(pm2) / 8.0) * 30.0
    comp_score = min(1.0, max(0.0, score_medio) / 100.0) * 40.0
    if spread_pct is None:
        comp_estab = 0.0
    elif spread_pct <= 0.15:
        comp_estab = 30.0
    elif spread_pct >= 0.60:
        comp_estab = 0.0
    else:
        comp_estab = 30.0 * (1.0 - ((spread_pct - 0.15) / 0.45))
    conf = max(0.0, min(100.0, comp_amostra + comp_score + comp_estab))
    if conf >= 75:
        nivel = "alta"
    elif conf >= 55:
        nivel = "media"
    else:
        nivel = "baixa"
    return {
        "n_comparaveis": len(comparaveis),
        "n_precos_validos": len(pm2),
        "pm2_p25": round(float(p25), 2) if p25 is not None else None,
        "pm2_p50": round(float(p50), 2) if p50 is not None else None,
        "pm2_p75": round(float(p75), 2) if p75 is not None else None,
        "pm2_p90": round(float(p90), 2) if p90 is not None else None,
        "pm2_max": round(float(pm2_max), 2) if pm2_max is not None else None,
        "spread_iqr_pct": round(float(spread_pct), 4) if spread_pct is not None else None,
        "score_medio_top": round(float(score_medio), 3),
        "confianca_score": round(float(conf), 2),
        "confianca_nivel": nivel,
    }


def selecionar_top_comparaveis(
    anuncios: list[dict[str, Any]],
    *,
    row_referencia: dict[str, Any],
    min_comparaveis: int = 5,
    raios_km: tuple[float, ...] = _RAIOS_PROGRESSIVOS_KM,
    tolerancia_area: float = _TOLERANCIA_AREA_PADRAO,
) -> tuple[list[dict[str, Any]], float]:
    """
    Seleciona comparáveis por score determinístico:
    - tipologia obrigatória;
    - área ±15% (configurável);
    - quartos/vagas com diferença máxima de 1;
    - raio progressivo (3km padrão) quando houver coordenadas.

    Retorna TODOS os candidatos que passaram nos filtros (ordenados por score),
    usando expansão progressiva de raio/tolerância só para atingir o mínimo.
    """
    area_ref = area_efetiva_de_registro(row_referencia)
    if area_ref <= 0:
        return [], raios_km[0] if raios_km else _DEFAULT_RAIO_KM
    tipo_ref = normalizar_tipo_imovel(row_referencia.get("tipo_imovel"))
    _eh_terreno = tipo_ref == "terreno"
    quartos_ref = None if _eh_terreno else _parse_int_any(row_referencia.get("quartos"))
    vagas_ref = None if _eh_terreno else _parse_int_any(row_referencia.get("vagas"))
    lat_ref, lon_ref = _coords_de_row(row_referencia)
    cons_ref = str(row_referencia.get("conservacao") or "").strip().lower()
    raios = tuple(r for r in raios_km if r > 0) or (_DEFAULT_RAIO_KM,)
    tolerancias = tuple(dict.fromkeys((tolerancia_area, 0.25, 0.35)))
    ultima_lista: list[dict[str, Any]] = []
    raio_usado = raios[0]
    for raio in raios:
        for tol in tolerancias:
            lo = area_ref * (1.0 - tol)
            hi = area_ref * (1.0 + tol)
            diff_qv_max = 1 if tol <= 0.2 else 2
            candidatos: list[dict[str, Any]] = []
            for an in anuncios:
                try:
                    a = float(an.get("area_construida_m2") or 0)
                    v = float(an.get("valor_venda") or 0)
                except (TypeError, ValueError):
                    continue
                if a <= 0 or v <= 0:
                    continue
                t = normalizar_tipo_imovel(an.get("tipo_imovel"))
                if tipo_ref != "desconhecido" and t != tipo_ref:
                    continue
                if not (lo <= a <= hi):
                    continue
                q = _valor_quartos_vagas(an, "quartos")
                if quartos_ref is not None and q is not None and abs(q - quartos_ref) > diff_qv_max:
                    continue
                vg = _valor_quartos_vagas(an, "vagas")
                if vagas_ref is not None and vg is not None and abs(vg - vagas_ref) > diff_qv_max:
                    continue
                score = 100.0
                score -= min(35.0, (abs(a - area_ref) / max(area_ref, 1.0)) * 100.0 * 0.35)
                if quartos_ref is not None and q is not None:
                    score -= min(12.0, abs(q - quartos_ref) * 12.0)
                if vagas_ref is not None and vg is not None:
                    score -= min(8.0, abs(vg - vagas_ref) * 8.0)
                cons_an = str(an.get("conservacao") or "").strip().lower()
                if cons_ref and cons_an and cons_ref != cons_an:
                    score -= 10.0
                lat_an, lon_an = _coords_de_row(an)
                dist_km: Optional[float] = None
                if lat_ref is not None and lon_ref is not None and lat_an is not None and lon_an is not None:
                    dist_km = _distancia_km(lat_ref, lon_ref, lat_an, lon_an)
                    if dist_km > raio:
                        continue
                    score -= min(30.0, (dist_km / max(raio, 0.1)) * 30.0)
                an_score = dict(an)
                an_score["_score_similaridade"] = round(score, 3)
                an_score["_distancia_km"] = round(dist_km, 3) if dist_km is not None else None
                an_score["_tolerancia_area_usada"] = round(float(tol), 3)
                an_score["_dif_qv_max_usado"] = diff_qv_max
                candidatos.append(an_score)
            candidatos.sort(key=lambda x: float(x.get("_score_similaridade") or 0), reverse=True)
            raio_usado = raio
            if len(candidatos) >= min_comparaveis:
                return candidatos, raio_usado
            if len(candidatos) > len(ultima_lista):
                ultima_lista = candidatos
    return ultima_lista, raio_usado


def _label_tipo_busca(tipo_imovel: str) -> str:
    """Converte tipo normalizado para texto amigável na query de busca."""
    mapa = {
        "apartamento": "apartamento",
        "casa": "casa",
        "casa_condominio": "casa de condomínio",
        "terreno": "terreno",
        "comercial": "imóvel comercial",
    }
    return mapa.get(tipo_imovel, "imóvel")


def montar_query_ddgs(
    cidade: str,
    localizacao: str,
    quartos: Optional[int],
    tipo_imovel: str,
    conservacao: str,
    tipo_casa: str,
    faixa_andar: str,
    trecho_rua: str,
) -> str:
    loc = (localizacao or "").strip() or cidade
    tipo_busca = _label_tipo_busca(tipo_imovel) if tipo_imovel else "imóvel"
    q = f"site:zapimoveis.com.br {tipo_busca} à venda {loc} {cidade} preço m²"
    if quartos and quartos > 0:
        q = f"{quartos} quartos " + q
    if conservacao:
        q = f"{conservacao} {q}"
    if tipo_casa and tipo_casa not in ("-", "desconhecido") and "casa" in tipo_busca.lower():
        q = f"{tipo_casa} {q}"
    if faixa_andar and "apartamento" in tipo_busca.lower():
        q = f"{faixa_andar} andar {q}"
    if trecho_rua:
        q = f"{trecho_rua} {q}"
    return q


def _ddgs_text_hits(
    q: str,
    max_results: int,
) -> list[dict[str, Any]]:
    if DDGS is None:
        return []
    try:
        with DDGS(timeout=25) as ddgs:
            return list(ddgs.text(q, max_results=max_results, region="br-pt"))
    except Exception:
        logger.exception("DDGS falhou para query=%s", q[:120])
        return []


# ---------------------------------------------------------------------------
# Firecrawl: singleton thread-safe + controle de créditos
# ---------------------------------------------------------------------------

_FIRECRAWL_CREDIT_LIMIT = int(os.getenv("FIRECRAWL_CREDIT_LIMIT", "200"))
_fc_lock = threading.Lock()
_fc_instance: Any | None = None
_firecrawl_credits_exhausted = False
_firecrawl_credits_used = 0


def _get_firecrawl_client() -> Any | None:
    """Singleton thread-safe. Retorna None se esgotado ou indisponível."""
    global _fc_instance
    with _fc_lock:
        if _firecrawl_credits_exhausted:
            return None
        if _firecrawl_credits_used >= _FIRECRAWL_CREDIT_LIMIT:
            logger.info(
                "Firecrawl: limite por execução atingido (%s/%s). Usando DDGS.",
                _firecrawl_credits_used, _FIRECRAWL_CREDIT_LIMIT,
            )
            return None
        if _fc_instance is not None:
            return _fc_instance
        if Firecrawl is None:
            return None
        key = os.getenv("FIRECRAWL_API_KEY", "").strip()
        if not key:
            return None
        try:
            _fc_instance = Firecrawl(api_key=key)
            return _fc_instance
        except Exception:
            logger.exception("Falha ao criar cliente Firecrawl")
            return None


def _contabilizar_creditos(n: int) -> None:
    global _firecrawl_credits_used
    with _fc_lock:
        _firecrawl_credits_used += n
    logger.debug("Firecrawl créditos: +%s (total sessão: %s/%s)", n, _firecrawl_credits_used, _FIRECRAWL_CREDIT_LIMIT)


def _marcar_creditos_esgotados(exc: BaseException) -> bool:
    global _firecrawl_credits_exhausted
    nome = type(exc).__name__
    if "PaymentRequired" in nome or "402" in str(exc):
        with _fc_lock:
            _firecrawl_credits_exhausted = True
        logger.warning("Firecrawl: créditos esgotados — desativando para esta execução. Fallback: DDGS.")
        return True
    return False


def _firecrawl_search_hits(
    q: str,
    max_results: int,
) -> list[dict[str, Any]]:
    """
    Busca via Firecrawl /search SEM scrape (apenas discovery).
    Custo: 2 créditos por cada 10 resultados.
    Retorna lista no mesmo formato que DDGS (chaves: href, title, body).

    Filtra URLs de listagem genérica do ZAP (só interessa anúncios individuais
    que contenham /imovel/ na URL), e expande listagens para extrair sub-URLs.
    """
    fc = _get_firecrawl_client()
    if fc is None:
        return []
    limit = min(max_results, 10)
    try:
        raw = fc.search(
            q,
            limit=limit,
            location="Brazil",
        )
    except Exception as exc:
        if _marcar_creditos_esgotados(exc):
            return []
        logger.exception("Firecrawl search falhou para query=%s", q[:120])
        return []

    _contabilizar_creditos(2)

    web_results: list[dict[str, Any]] = []
    listing_urls: list[str] = []
    items: list[Any] = []
    if hasattr(raw, "web") and isinstance(raw.web, list):
        items = raw.web
    elif isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = raw.get("data", raw.get("web", []))
        if isinstance(items, dict):
            items = items.get("web", [])

    for item in items:
        d = _firecrawl_result_to_dict(item)
        if not d:
            continue

        meta = d.get("metadata") or {}
        if not isinstance(meta, dict):
            meta = {}

        href = str(
            d.get("url")
            or meta.get("sourceURL")
            or meta.get("url")
            or ""
        ).strip()
        if not href:
            continue

        title = str(meta.get("title") or d.get("title") or "").strip()
        description = str(meta.get("description") or d.get("description") or "").strip()

        if "zapimoveis.com.br" in href and "/imovel/" not in href:
            if len(listing_urls) < 2:
                listing_urls.append(href)
            continue

        web_results.append({
            "href": href,
            "title": title,
            "body": description,
            "_firecrawl_markdown": "",
        })

    if listing_urls and len(web_results) < max_results:
        for lurl in listing_urls[:1]:
            sub = _expandir_listagem_zap(lurl)
            for s in sub:
                if len(web_results) >= max_results:
                    break
                web_results.append(s)

    logger.info("Firecrawl search retornou %s resultados (custo ~2 créditos) para query=%s", len(web_results), q[:80])
    return web_results


def _expandir_listagem_zap(url_listagem: str) -> list[dict[str, Any]]:
    """
    Faz UM scrape de uma página de listagem do ZAP e extrai URLs de anúncios
    individuais (/imovel/...) do markdown. Custo: 1 crédito para potencialmente
    20+ anúncios. Muito mais eficiente que scrapear cada URL.
    """
    fc = _get_firecrawl_client()
    if fc is None:
        return []
    try:
        result = fc.scrape(url_listagem, formats=["markdown"])
    except Exception as exc:
        if _marcar_creditos_esgotados(exc):
            return []
        logger.exception("Firecrawl scrape listagem falhou: %s", url_listagem[:100])
        return []

    _contabilizar_creditos(1)

    if not result:
        return []
    d = _firecrawl_result_to_dict(result)
    markdown = str(d.get("markdown") or "").strip()
    if not markdown:
        return []

    imovel_paths = re.findall(r"\(/imovel/([^\)]+)\)", markdown)
    if not imovel_paths:
        imovel_paths = re.findall(r"/imovel/(venda[^\s\"'\)>#]+)", markdown)

    hits: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in imovel_paths:
        clean = path.split("?")[0].rstrip("/")
        if clean in seen:
            continue
        seen.add(clean)
        full_url = f"https://www.zapimoveis.com.br/imovel/{clean}/"
        title_parts = clean.replace("-", " ").split("/")[0] if clean else ""
        hits.append({
            "href": full_url,
            "title": title_parts[:120],
            "body": "",
            "_firecrawl_markdown": "",
        })

    logger.info(
        "Expandiu listagem ZAP %s -> %s anúncios individuais (custo 1 crédito)",
        url_listagem[:60], len(hits),
    )
    return hits


_re_loc_bairro = re.compile(r"(?:bairro|neighborhood)[:\s]+([A-Za-zÀ-ú\s]{3,60})", re.IGNORECASE)
_re_loc_cidade = re.compile(r"(?:cidade|city)[:\s]+([A-Za-zÀ-ú\s]{3,60})", re.IGNORECASE)
_re_loc_estado = re.compile(r"(?:estado|state|uf)[:\s]+([A-Za-zÀ-ú]{2,30})", re.IGNORECASE)


def _extrair_dados_basicos_de_texto(
    texto: str,
    url: str = "",
    fallback_title: str = "",
    extrair_localizacao: bool = False,
) -> dict[str, Any] | None:
    """Extrai preço, área, quartos, vagas de texto plano/markdown.
    Retorna None se faltar valor_venda ou area_m2."""
    if not texto:
        return None
    dados: dict[str, Any] = {
        "url_fonte": url,
        "data_coleta": datetime.now(timezone.utc).date().isoformat(),
    }
    if fallback_title:
        dados["titulo"] = fallback_title[:500]
    valores = extrair_valores_rs_brl(texto)
    if valores:
        dados["valor_venda"] = max(valores)
    area = extrair_area_m2(texto)
    if area is not None:
        dados["area_m2"] = area
    if m := _re_qt.search(texto):
        dados["quartos"] = _parse_int_any(m.group(1))
    if m := _re_vg.search(texto):
        dados["vagas"] = _parse_int_any(m.group(1))
    if extrair_localizacao:
        for pat, key in zip(
            (_re_loc_bairro, _re_loc_cidade, _re_loc_estado),
            ("bairro_extraido", "cidade_extraida", "estado_extraido"),
        ):
            if m := pat.search(texto[:3000]):
                dados[key] = m.group(1).strip()
    dados["data_anuncio"] = dados["data_coleta"]
    if dados.get("valor_venda") and dados.get("area_m2"):
        return dados
    return None


def _firecrawl_result_to_dict(result: Any) -> dict[str, Any]:
    if hasattr(result, "model_dump"):
        return result.model_dump()
    if isinstance(result, dict):
        return result
    return {}


def _firecrawl_scrape_page(url: str) -> dict[str, Any] | None:
    """Scrape via Firecrawl /scrape. Custo: 1 crédito.
    Retorna dict com valor_venda, area_m2 etc ou None."""
    fc = _get_firecrawl_client()
    if fc is None:
        return None
    try:
        result = fc.scrape(url, formats=["markdown"])
    except Exception as exc:
        if _marcar_creditos_esgotados(exc):
            return None
        logger.exception("Firecrawl scrape falhou para url=%s", url[:120])
        return None
    _contabilizar_creditos(1)
    if not result:
        return None
    d = _firecrawl_result_to_dict(result)
    markdown = str(d.get("markdown") or "").strip()
    metadata = d.get("metadata") or {}
    title = str(metadata.get("title") or "").strip() if isinstance(metadata, dict) else ""
    return _extrair_dados_basicos_de_texto(markdown, url, fallback_title=title, extrair_localizacao=True)


def _extrair_dados_de_firecrawl_markdown(
    markdown: str,
    url: str,
    fallback_title: str = "",
) -> dict[str, Any] | None:
    """Extrai dados do markdown já retornado pelo Firecrawl search (sem scrape extra)."""
    return _extrair_dados_basicos_de_texto(markdown, url, fallback_title=fallback_title)


def _buscar_hits_web(
    q: str,
    max_results: int,
) -> tuple[list[dict[str, Any]], str]:
    """
    Tenta Firecrawl primeiro; se indisponível ou sem resultados, cai em DDGS.
    Retorna (hits, fonte) onde fonte é 'firecrawl' ou 'ddgs'.
    """
    fc_hits = _firecrawl_search_hits(q, max_results)
    if fc_hits:
        return fc_hits, "firecrawl"
    ddgs_hits = _ddgs_text_hits(q, max_results)
    return ddgs_hits, "ddgs"


def _tipo_para_busca(row_f: dict[str, Any]) -> str:
    seg = segmento_mercado_de_registro(row_f)
    t = seg["tipo_imovel"]
    if t == "desconhecido":
        return "imóvel"
    return t


def buscar_anuncios_similares_supabase(
    *,
    cidade: str,
    estado: str,
    bairro: str,
    tipo_imovel_norm: str,
    client: Any,
    exigir_bairro_match: bool = True,
) -> list[dict[str, Any]]:
    """Anúncios candidatos na mesma cidade/UF, opcionalmente bairro e tipo.

    Também limita o conjunto a portais confiáveis para evitar contaminação com outliers.
    """
    cli = client or get_supabase_client()
    cid = (cidade or "").strip()
    uf = chave_estado(estado or "")
    bai = (bairro or "").strip()
    try:
        q = cli.table(TABLE_ANUNCIOS_MERCADO).select("*").eq("cidade", cid)
        if tipo_imovel_norm and tipo_imovel_norm != "desconhecido":
            q = q.eq("tipo_imovel", tipo_imovel_norm)
        resp = q.limit(200).execute()
        rows = list(getattr(resp, "data", None) or [])
    except Exception as e:
        if _is_table_missing(e):
            _log_table_missing_once()
            return []
        logger.exception("Falha ao listar %s", TABLE_ANUNCIOS_MERCADO)
        return []

    cid_ref = _normalizar_cidade(cid)
    uf_ref = _normalizar_estado(uf)
    bai_ref = _normalizar_bairro(bai)
    out: list[dict[str, Any]] = []
    for r in rows:
        portal_row = str(r.get("portal") or "").strip().lower()
        url_row = str(r.get("url_anuncio") or "").strip().lower()
        if not _portal_aceito_pipeline(portal_row or url_row):
            continue
        cid_row = _normalizar_cidade(r.get("cidade"))
        uf_row = _normalizar_estado(r.get("estado"))
        if cid_ref and cid_row != cid_ref:
            continue
        if uf_ref and uf_row and uf_row != uf_ref:
            continue
        if exigir_bairro_match and bai_ref:
            bai_row = _normalizar_bairro(r.get("bairro"))
            if bai_row and not _bairro_compativel(bai_ref, bai_row):
                continue
        out.append(r)
    return out


def _parse_ts_ultima_coleta(row: dict[str, Any]) -> Optional[datetime]:
    raw = row.get("ultima_coleta_em")
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def precisa_coletar_mais_anuncios(
    anuncios: list[dict[str, Any]],
    *,
    min_count: int,
    max_idade_dias: int,
) -> bool:
    if len(anuncios) < min_count:
        return True
    datas = [t for r in anuncios if (t := _parse_ts_ultima_coleta(r))]
    if not datas:
        return True
    newest = max(datas)
    limite = datetime.now(timezone.utc) - timedelta(days=max_idade_dias)
    return newest < limite


def coletar_e_persistir_via_ddgs(
    *,
    row_referencia: dict[str, Any],
    cidade: str,
    localizacao: str,
    quartos: Optional[int],
    tipo_imovel_busca: str,
    seg: dict[str, str],
    client: Any,
    min_salvos: int = 5,
    max_results_inicial: int = 28,
    max_rodadas: int = 6,
    timeout_fetch_html_sec: float = _FETCH_HTML_TIMEOUT_PADRAO_SEC,
    max_fetches_por_query: int = _MAX_FETCHES_POR_QUERY,
    max_fetches_total: int = _MAX_FETCHES_TOTAL,
    max_hits_por_query: int = _MAX_HITS_POR_QUERY,
    max_idade_anuncios_dias: int = 180,
) -> ColetaAnunciosMercadoResult:
    """
    Coleta anúncios **exclusivamente do VivaReal** e grava no Supabase.
    Antes de gastar créditos, verifica se já existem comparáveis reais suficientes
    e recentes no banco (mesma tipologia, área compatível, frescor ≤ 180 dias).
    Estratégia:
      0) Banco de dados — se já tem comparáveis suficientes, retorna sem buscar
      1) VivaReal listagem direta (1 crédito FC por tentativa de scrape)
    Se a listagem VivaReal não trouxer markdown com padrão de anúncios, retorna
    ``vivareal_markdown_insuficiente=True`` (sem tentar outra URL nessa etapa).
    Nenhuma busca adicional (DDGS, Firecrawl search) é realizada.
    """
    cli = client or get_supabase_client()
    estado = chave_estado((row_referencia.get("estado") or ""))
    if not (cidade or "").strip() or not estado:
        return ColetaAnunciosMercadoResult(0, False)
    cidade_ref = (cidade or "").strip()
    bairro_ref = (row_referencia.get("bairro") or "").strip()

    tipo_norm = normalizar_tipo_imovel(tipo_imovel_busca)
    if tipo_norm == "desconhecido":
        tipo_norm = normalizar_tipo_imovel(row_referencia.get("tipo_imovel"))

    existentes = buscar_anuncios_similares_supabase(
        cidade=cidade_ref,
        estado=estado,
        bairro=bairro_ref,
        tipo_imovel_norm=tipo_norm,
        client=cli,
    )
    if existentes:
        comparaveis, _ = selecionar_top_comparaveis(
            existentes,
            row_referencia=row_referencia,
            min_comparaveis=min_salvos,
        )
        if not precisa_coletar_mais_anuncios(comparaveis, min_count=min_salvos, max_idade_dias=max_idade_anuncios_dias):
            logger.info(
                "Banco já tem %s comparáveis válidos (tipo=%s, área compatível, "
                "frescor ≤ %s dias) para %s/%s/%s. Pulando coleta web.",
                len(comparaveis), tipo_norm, max_idade_anuncios_dias,
                cidade_ref, bairro_ref, estado,
            )
            return ColetaAnunciosMercadoResult(len(comparaveis), False)
        logger.info(
            "Banco tem %s anúncios mas apenas %s comparáveis válidos (meta=%s). Buscando mais.",
            len(existentes), len(comparaveis), min_salvos,
        )

    vr_slug = str(row_referencia.get("bairro_vivareal_slug") or "").strip()
    area_ref = area_efetiva_de_registro(row_referencia)
    vr_res = coletar_vivareal_listagem(
        cidade=cidade_ref,
        estado=estado,
        bairro=bairro_ref,
        tipo_imovel=tipo_imovel_busca,
        client=cli,
        tipo_norm=tipo_norm,
        bairro_vivareal_slug=vr_slug,
        area_referencia_m2=area_ref,
    )
    if vr_res.markdown_insuficiente:
        return ColetaAnunciosMercadoResult(0, True)
    vr_salvos = vr_res.salvos
    if vr_salvos >= min_salvos:
        logger.info("VivaReal listagem suficiente: %s anúncios (meta %s).", vr_salvos, min_salvos)
        return ColetaAnunciosMercadoResult(vr_salvos, False)

    if vr_salvos > 0:
        logger.warning(
            "VivaReal retornou apenas %s anúncios (meta %s) para %s/%s/%s (tipo=%s). "
            "Amostras insuficientes — nenhuma busca adicional será realizada.",
            vr_salvos, min_salvos, cidade_ref, bairro_ref, estado, tipo_norm,
        )
        return ColetaAnunciosMercadoResult(vr_salvos, False)

    logger.warning(
        "VivaReal não retornou anúncios para %s/%s/%s (tipo=%s). "
        "Amostras não encontradas — nenhuma busca adicional será realizada.",
        cidade_ref, bairro_ref, estado, tipo_norm,
    )
    return ColetaAnunciosMercadoResult(0, False)


def _persistir_uma_linha(client: Any, row: AnuncioMercadoPersist) -> None:
    payload = row.row_dict()
    now = datetime.now(timezone.utc).isoformat()
    payload["ultima_coleta_em"] = now
    payload.setdefault("primeiro_visto_em", now)
    client.table(TABLE_ANUNCIOS_MERCADO).upsert(payload, on_conflict="url_anuncio").execute()


def formatar_anuncios_para_prompt(anuncios: list[dict[str, Any]], max_itens: int = 14) -> str:
    partes: list[str] = []
    for r in anuncios[:max_itens]:
        try:
            vv = float(r.get("valor_venda") or 0)
            aa = float(r.get("area_construida_m2") or 0)
        except (TypeError, ValueError):
            continue
        cond = (r.get("nome_condominio") or "").strip()
        md = r.get("metadados_json")
        if isinstance(md, str):
            try:
                md = json.loads(md)
            except json.JSONDecodeError:
                md = {}
        if not isinstance(md, dict):
            md = {}
        data_anuncio = str(md.get("data_anuncio") or md.get("data_coleta") or "").strip()
        linha = (
            f"- R$ {vv:,.0f} | {aa:.1f} m² constr. | {r.get('tipo_imovel') or '?'} | "
            f"{(r.get('logradouro') or '').strip() or '—'}, {r.get('bairro') or '—'}, "
            f"{r.get('cidade') or '—'}/{r.get('estado') or '—'}"
        )
        if cond:
            linha += f" | cond.: {cond}"
        if data_anuncio:
            linha += f" | data anúncio: {data_anuncio}"
        if r.get("_distancia_km") is not None:
            linha += f" | dist.: {r.get('_distancia_km')} km"
        if r.get("_score_similaridade") is not None:
            linha += f" | score: {r.get('_score_similaridade')}"
        linha += f"\n  {r.get('url_anuncio') or ''}"
        partes.append(linha)
    return "\n".join(partes) if partes else ""


def _precos_m2_de_anuncios(anuncios: list[dict[str, Any]]) -> list[float]:
    out: list[float] = []
    for r in anuncios:
        pm = r.get("preco_m2")
        if pm is not None:
            try:
                out.append(float(pm))
            except (TypeError, ValueError):
                pass
            continue
        try:
            v = float(r.get("valor_venda") or 0)
            a = float(r.get("area_construida_m2") or 0)
            if v > 0 and a > 0:
                out.append(v / a)
        except (TypeError, ValueError):
            continue
    return out


def sincronizar_amostras_e_atualizar_cache_media_bairro(
    row_f: dict[str, Any],
    *,
    client: Any,
    min_anuncios: int,
    max_idade_dias: int,
    max_results_ddgs: int = 28,
    ddgs_rodadas_max: int = 6,
    raio_inicial_km: float = _DEFAULT_RAIO_KM,
    raios_expansao_km: tuple[float, ...] = _RAIOS_PROGRESSIVOS_KM,
    min_confianca_aceitavel: float = 55.0,
    forcar_atualizacao_com_baixa_amostragem: bool = True,
    forcar_atualizacao_com_baixa_confianca: bool = True,
    chaves_ja_sincronizadas: Optional[set[str]] = None,
    modo_entrada: Literal["planilha", "avulso"] = "planilha",
) -> dict[str, Any]:
    """
    Independente de já existir linha em `cache_media_bairro`:
    verifica amostras em `anuncios_mercado` (quantidade + frescor); se necessário coleta na web;
    recalcula mediana R$/m² das amostras e atualiza o cache (upsert por chave_segmento).

    Os comparáveis vêm de `selecionar_top_comparaveis` (área efetiva do imóvel de referência
    ± tolerância progressiva, quartos/vagas, raio), alinhado ao contexto de precificação.
    """
    min_anuncios = max(3, int(min_anuncios))
    cidade = (row_f.get("cidade") or "").strip()
    estado = (row_f.get("estado") or "").strip()
    bairro = (row_f.get("bairro") or "").strip()
    loc = bairro or estado
    if not cidade or not loc:
        return {"ok": False, "motivo": "sem_localizacao"}

    chave_geo = normalizar_chave_bairro(cidade, bairro, estado)
    seg = merge_segmento_mercado(row_f, None)
    chave_seg = normalizar_chave_segmento(chave_geo, seg)
    cli = client or get_supabase_client()

    def _com_id_cache_row(payload: dict[str, Any]) -> dict[str, Any]:
        out = dict(payload)
        if chave_seg and not out.get("cache_media_bairro_id"):
            rid = id_cache_media_por_chave_segmento(chave_seg, client=cli)
            if rid:
                out["cache_media_bairro_id"] = rid
        return out

    if chaves_ja_sincronizadas is not None and chave_seg in chaves_ja_sincronizadas:
        return _com_id_cache_row(
            {
                "ok": True,
                "skipped": True,
                "motivo": "ja_sincronizado_no_lote",
                "chave_segmento": chave_seg,
            }
        )

    tipo_norm = seg["tipo_imovel"]

    anuncios = buscar_anuncios_similares_supabase(
        cidade=cidade,
        estado=estado,
        bairro=bairro,
        tipo_imovel_norm=tipo_norm,
        client=cli,
        exigir_bairro_match=True,
    )
    raios_km = tuple(dict.fromkeys((raio_inicial_km, *raios_expansao_km)))
    comparaveis, raio_usado_km = selecionar_top_comparaveis(
        anuncios,
        row_referencia=row_f,
        min_comparaveis=min_anuncios,
        raios_km=raios_km,
    )

    if precisa_coletar_mais_anuncios(
        comparaveis,
        min_count=min_anuncios,
        max_idade_dias=max_idade_dias,
    ):
        qi = None
        qraw = row_f.get("quartos")
        try:
            if qraw is not None and str(qraw).strip() != "":
                qi = int(float(str(qraw)))
        except (TypeError, ValueError):
            qi = None
        tit = _tipo_para_busca(row_f)
        try:
            cr = coletar_e_persistir_via_ddgs(
                row_referencia=row_f,
                cidade=cidade,
                localizacao=loc,
                quartos=qi,
                tipo_imovel_busca=tit,
                seg=seg,
                client=cli,
                min_salvos=min_anuncios,
                max_results_inicial=max_results_ddgs,
                max_rodadas=ddgs_rodadas_max,
            )
            if cr.vivareal_markdown_insuficiente:
                logger.warning(
                    "Anúncios insuficientes: listagem VivaReal sem markdown válido (vazio ou sem padrão de cards)."
                )
                iid = row_f.get("id")
                if iid:
                    try:
                        atualizar_leilao_imovel_campos(
                            str(iid),
                            {"status": STATUS_PENDENTE},
                            client=cli,
                        )
                    except Exception:
                        logger.debug(
                            "Não foi possível marcar pendente após VR insuficiente (sync)",
                            exc_info=True,
                        )
                if modo_entrada == "avulso":
                    return _com_id_cache_row(
                        {
                            "ok": True,
                            "cache_atualizado": False,
                            "vivareal_markdown_insuficiente": True,
                            "interromper_pipeline_avulso": True,
                            "motivo": "vivareal_anuncios_insuficientes",
                            "chave_segmento": chave_seg,
                        }
                    )
        except Exception:
            logger.exception("Coleta DDGS (sincronizar cache bairro) falhou cidade=%s", cidade)
        anuncios = buscar_anuncios_similares_supabase(
            cidade=cidade,
            estado=estado,
            bairro=bairro,
            tipo_imovel_norm=tipo_norm,
            client=cli,
            exigir_bairro_match=True,
        )
        comparaveis, raio_usado_km = selecionar_top_comparaveis(
            anuncios,
            row_referencia=row_f,
            min_comparaveis=min_anuncios,
            raios_km=raios_km,
        )
        if len(comparaveis) < min_anuncios:
            anuncios = buscar_anuncios_similares_supabase(
                cidade=cidade,
                estado=estado,
                bairro=bairro,
                tipo_imovel_norm=tipo_norm,
                client=cli,
                exigir_bairro_match=False,
            )
            comparaveis, raio_usado_km = selecionar_top_comparaveis(
                anuncios,
                row_referencia=row_f,
                min_comparaveis=min_anuncios,
                raios_km=raios_km,
            )

    stats_cmp = estatisticas_comparaveis(comparaveis)
    precos = _precos_m2_de_anuncios(comparaveis)
    bairro_grava = bairro or estado or "geral"

    if chaves_ja_sincronizadas is not None:
        chaves_ja_sincronizadas.add(chave_seg)

    min_aceitavel_cache = min(5, max(1, min_anuncios))
    n_precos_validos = len(precos)
    if n_precos_validos == 0:
        logger.warning(
            "Sincronização cache bairro: segmento=%s sem preços válidos; cache não atualizado",
            chave_seg,
        )
        return _com_id_cache_row(
            {
                "ok": True,
                "cache_atualizado": False,
                "n_precos": n_precos_validos,
                "chave_segmento": chave_seg,
                "motivo": "sem_precos_validos",
                "estatisticas_comparaveis": stats_cmp,
            }
        )
    baixa_amostragem = n_precos_validos < min_aceitavel_cache
    if baixa_amostragem and not forcar_atualizacao_com_baixa_amostragem:
        logger.warning(
            "Sincronização cache bairro: segmento=%s só %s preço(s)/m² válidos (mínimo aceitável %s; meta %s); cache não atualizado",
            chave_seg,
            n_precos_validos,
            min_aceitavel_cache,
            min_anuncios,
        )
        return _com_id_cache_row(
            {
                "ok": True,
                "cache_atualizado": False,
                "n_precos": n_precos_validos,
                "chave_segmento": chave_seg,
                "motivo": "amostras_insuficientes",
                "estatisticas_comparaveis": stats_cmp,
            }
        )

    confianca = float(stats_cmp.get("confianca_score") or 0.0)
    baixa_confianca = confianca < float(min_confianca_aceitavel)
    if baixa_confianca and not forcar_atualizacao_com_baixa_confianca:
        logger.warning(
            "Sincronização cache bairro: segmento=%s confiança baixa %.2f < %.2f; cache não atualizado",
            chave_seg,
            confianca,
            min_confianca_aceitavel,
        )
        return _com_id_cache_row(
            {
                "ok": True,
                "cache_atualizado": False,
                "n_precos": len(precos),
                "chave_segmento": chave_seg,
                "motivo": "baixa_confianca_comparaveis",
                "estatisticas_comparaveis": stats_cmp,
            }
        )
    if baixa_amostragem:
        logger.warning(
            "Sincronização cache bairro: segmento=%s com baixa amostragem (%s/%s); atualização forçada com cautela",
            chave_seg,
            n_precos_validos,
            min_aceitavel_cache,
        )
    if baixa_confianca:
        logger.warning(
            "Sincronização cache bairro: segmento=%s com confiança baixa %.2f < %.2f; atualização forçada com cautela",
            chave_seg,
            confianca,
            min_confianca_aceitavel,
        )

    pm2_base = float(stats_cmp.get("pm2_p50") or statistics.mean(precos))
    lat_ref, lon_ref = _coords_de_row(row_f)
    geo_bucket = geo_bucket_de_registro(row_f)
    if not geo_bucket and comparaveis:
        for c in comparaveis:
            geo_bucket = geo_bucket_de_registro(c)
            if geo_bucket:
                break
    if (lat_ref is None or lon_ref is None) and comparaveis:
        for c in comparaveis:
            clat, clon = _coords_de_row(c)
            if clat is not None and clon is not None:
                lat_ref, lon_ref = clat, clon
                break

    valores_venda = [float(x["valor_venda"]) for x in comparaveis if x.get("valor_venda")]
    ids_usados = [str(x.get("id") or "") for x in comparaveis if x.get("id")]

    try:
        sv = salvar_media_bairro_no_cache(
            CacheMediaBairroSalvar(
                cidade=cidade,
                bairro=bairro_grava,
                estado=estado,
                tipo_imovel=seg["tipo_imovel"],
                conservacao=seg["conservacao"],
                tipo_casa=seg["tipo_casa"],
                faixa_andar=seg["faixa_andar"],
                faixa_area=seg.get("faixa_area", "-"),
                logradouro_chave=seg["logradouro_chave"],
                geo_bucket=geo_bucket,
                lat_ref=lat_ref,
                lon_ref=lon_ref,
                preco_m2_medio=round(pm2_base, 2),
                fonte="anuncios_mercado_media_todos",
                valor_medio_venda=round(statistics.mean(valores_venda), 2) if valores_venda else None,
                maior_valor_venda=round(max(valores_venda), 2) if valores_venda else None,
                menor_valor_venda=round(min(valores_venda), 2) if valores_venda else None,
                n_amostras=len(comparaveis),
                anuncios_ids=",".join(ids_usados) if ids_usados else None,
                metadados_json=json.dumps(
                    {
                        "n_amostras": len(precos),
                        "amostragem_baixa": baixa_amostragem,
                        "amostragem_minima_recomendada": min_aceitavel_cache,
                        "amostragem_meta_alvo": min_anuncios,
                        "n_comparaveis_filtrados": len(comparaveis),
                        "raio_usado_km": raio_usado_km,
                        "chave_segmento": chave_seg,
                        "origem": "media_todos_comparaveis_similares",
                        "score_medio": stats_cmp.get("score_medio_top"),
                        "pm2_p25": stats_cmp.get("pm2_p25"),
                        "pm2_p50": stats_cmp.get("pm2_p50"),
                        "pm2_p75": stats_cmp.get("pm2_p75"),
                        "pm2_p90": stats_cmp.get("pm2_p90"),
                        "pm2_max": stats_cmp.get("pm2_max"),
                        "spread_iqr_pct": stats_cmp.get("spread_iqr_pct"),
                        "confianca_score": stats_cmp.get("confianca_score"),
                        "confianca_nivel": stats_cmp.get("confianca_nivel"),
                        "confianca_baixa": baixa_confianca,
                        "precificacao_requer_cautela": baixa_amostragem or baixa_confianca,
                        "geo_bucket": geo_bucket or None,
                        "lat_ref": lat_ref,
                        "lon_ref": lon_ref,
                        "urls_amostras": [str(x.get("url_anuncio") or "") for x in comparaveis],
                    },
                    ensure_ascii=False,
                ),
            ),
            client=cli,
        )
    except Exception:
        logger.exception("Não foi possível atualizar cache_media_bairro a partir de anuncios_mercado")
        return _com_id_cache_row(
            {
                "ok": False,
                "cache_atualizado": False,
                "chave_segmento": chave_seg,
                "motivo": "erro_salvar_cache",
            }
        )

    logger.info(
        "cache_media_bairro atualizado: segmento=%s pm2_base=%.2f R$/m² (n=%s, raio=%.1fkm, conf=%.2f)",
        chave_seg,
        pm2_base,
        len(precos),
        raio_usado_km,
        confianca,
    )
    sv_id = (sv or {}).get("cache_media_bairro_id") if isinstance(sv, dict) else None
    return _com_id_cache_row(
        {
            "ok": True,
            "cache_atualizado": True,
            "n_precos": len(precos),
            "n_comparaveis_filtrados": len(comparaveis),
            "amostragem_baixa": baixa_amostragem,
            "confianca_baixa": baixa_confianca,
            "precificacao_requer_cautela": baixa_amostragem or baixa_confianca,
            "raio_usado_km": raio_usado_km,
            "preco_m2_medio": round(pm2_base, 2),
            "estatisticas_comparaveis": stats_cmp,
            "chave_segmento": chave_seg,
            **({"cache_media_bairro_id": sv_id} if sv_id else {}),
        }
    )


def resolver_contexto_mercado_anuncios_detalhado(
    row_f: dict[str, Any],
    *,
    client: Any,
    min_anuncios: int,
    max_idade_dias: int,
    max_results_ddgs: int = 28,
    ddgs_rodadas_max: int = 6,
    raio_inicial_km: float = _DEFAULT_RAIO_KM,
    raios_expansao_km: tuple[float, ...] = _RAIOS_PROGRESSIVOS_KM,
    confianca_minima: float = 55.0,
    bloquear_baixa_confianca: bool = True,
) -> dict[str, Any]:
    """
    Prioriza anúncios similares persistidos; se faltar ou estiver desatualizado, coleta na web e grava.
    Retorna contexto + estatísticas de comparáveis para decisões de bloqueio por baixa confiança.
    """
    min_anuncios = max(3, int(min_anuncios))
    cidade = (row_f.get("cidade") or "").strip()
    estado = (row_f.get("estado") or "").strip()
    bairro = (row_f.get("bairro") or "").strip()
    loc = bairro or estado
    if not cidade or not loc:
        return {
            "texto": "",
            "bloqueado_baixa_confianca": False,
            "motivo": "sem_localizacao",
            "estatisticas_comparaveis": {},
            "n_comparaveis": 0,
        }

    seg = segmento_mercado_de_registro(row_f)
    tipo_norm = seg["tipo_imovel"]
    raios_km = tuple(dict.fromkeys((raio_inicial_km, *raios_expansao_km)))

    anuncios = buscar_anuncios_similares_supabase(
        cidade=cidade,
        estado=estado,
        bairro=bairro,
        tipo_imovel_norm=tipo_norm,
        client=client,
        exigir_bairro_match=True,
    )
    comparaveis, raio_usado = selecionar_top_comparaveis(
        anuncios,
        row_referencia=row_f,
        min_comparaveis=min_anuncios,
        raios_km=raios_km,
    )

    if precisa_coletar_mais_anuncios(
        comparaveis,
        min_count=min_anuncios,
        max_idade_dias=max_idade_dias,
    ):
        qi = None
        qraw = row_f.get("quartos")
        try:
            if qraw is not None and str(qraw).strip() != "":
                qi = int(float(str(qraw)))
        except (TypeError, ValueError):
            qi = None
        tit = _tipo_para_busca(row_f)
        try:
            cr_ctx = coletar_e_persistir_via_ddgs(
                row_referencia=row_f,
                cidade=cidade,
                localizacao=loc,
                quartos=qi,
                tipo_imovel_busca=tit,
                seg=seg,
                client=client,
                min_salvos=min_anuncios,
                max_results_inicial=max_results_ddgs,
                max_rodadas=ddgs_rodadas_max,
            )
            if cr_ctx.vivareal_markdown_insuficiente:
                logger.warning(
                    "Contexto mercado: VivaReal sem listagem válida; seguindo só com anúncios já no banco."
                )
        except Exception:
            logger.exception("Coleta DDGS anúncios falhou (cidade=%s)", cidade)

        anuncios = buscar_anuncios_similares_supabase(
            cidade=cidade,
            estado=estado,
            bairro=bairro,
            tipo_imovel_norm=tipo_norm,
            client=client,
            exigir_bairro_match=True,
        )
        comparaveis, raio_usado = selecionar_top_comparaveis(
            anuncios,
            row_referencia=row_f,
            min_comparaveis=min_anuncios,
            raios_km=raios_km,
        )
        if len(comparaveis) < min_anuncios:
            anuncios = buscar_anuncios_similares_supabase(
                cidade=cidade,
                estado=estado,
                bairro=bairro,
                tipo_imovel_norm=tipo_norm,
                client=client,
                exigir_bairro_match=False,
            )
            comparaveis, raio_usado = selecionar_top_comparaveis(
                anuncios,
                row_referencia=row_f,
                min_comparaveis=min_anuncios,
                raios_km=raios_km,
            )

    stats_cmp = estatisticas_comparaveis(comparaveis)
    confianca = float(stats_cmp.get("confianca_score") or 0.0)
    bloqueado = bool(bloquear_baixa_confianca and confianca < float(confianca_minima))
    cabecalho = (
        f"Comparáveis filtrados: n={stats_cmp.get('n_comparaveis')} | raio={raio_usado:.1f}km | "
        f"confiança={stats_cmp.get('confianca_score')} ({stats_cmp.get('confianca_nivel')}) | "
        f"R$/m² faixa P25/P50/P75/P90 = "
        f"{stats_cmp.get('pm2_p25')}/{stats_cmp.get('pm2_p50')}/{stats_cmp.get('pm2_p75')}/{stats_cmp.get('pm2_p90')} "
        f"| teto bruto pm2_max={stats_cmp.get('pm2_max')}"
    )
    txt = formatar_anuncios_para_prompt(comparaveis, max_itens=min(14, max(5, min_anuncios)))
    return {
        "texto": (f"{cabecalho}\n\n{txt}".strip() if txt else ""),
        "bloqueado_baixa_confianca": bloqueado,
        "motivo": "baixa_confianca_comparaveis" if bloqueado else None,
        "estatisticas_comparaveis": stats_cmp,
        "n_comparaveis": len(comparaveis),
        "raio_usado_km": raio_usado,
    }


def resolver_contexto_mercado_anuncios(
    row_f: dict[str, Any],
    *,
    client: Any,
    min_anuncios: int,
    max_idade_dias: int,
    max_results_ddgs: int = 28,
    ddgs_rodadas_max: int = 6,
    raio_inicial_km: float = _DEFAULT_RAIO_KM,
    raios_expansao_km: tuple[float, ...] = _RAIOS_PROGRESSIVOS_KM,
) -> str:
    d = resolver_contexto_mercado_anuncios_detalhado(
        row_f,
        client=client,
        min_anuncios=min_anuncios,
        max_idade_dias=max_idade_dias,
        max_results_ddgs=max_results_ddgs,
        ddgs_rodadas_max=ddgs_rodadas_max,
        raio_inicial_km=raio_inicial_km,
        raios_expansao_km=raios_expansao_km,
    )
    return str(d.get("texto") or "")


def firecrawl_status() -> dict[str, Any]:
    """Retorna status operacional do Firecrawl para exibição no frontend."""
    disponivel = Firecrawl is not None and bool(os.getenv("FIRECRAWL_API_KEY", "").strip())
    return {
        "disponivel": disponivel,
        "credits_exhausted": _firecrawl_credits_exhausted,
        "credits_used_session": _firecrawl_credits_used,
        "credit_limit_session": _FIRECRAWL_CREDIT_LIMIT,
        "ativo": disponivel and not _firecrawl_credits_exhausted and _firecrawl_credits_used < _FIRECRAWL_CREDIT_LIMIT,
    }


def firecrawl_account_credits() -> dict[str, Any] | None:
    """Consulta saldo de créditos na conta Firecrawl via API REST.
    Retorna None se indisponível."""
    key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    if not key:
        return None
    import urllib.request
    req = urllib.request.Request(
        "https://api.firecrawl.dev/v2/team/credit-usage",
        headers={"Authorization": f"Bearer {key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("success") and isinstance(data.get("data"), dict):
            d = data["data"]
            return {
                "remaining": d.get("remainingCredits"),
                "plan_credits": d.get("planCredits"),
                "billing_start": d.get("billingPeriodStart"),
                "billing_end": d.get("billingPeriodEnd"),
            }
    except Exception:
        logger.debug("Falha ao consultar créditos Firecrawl", exc_info=True)
    return None


def descobrir_bairros_vivareal(estado: str, cidade: str) -> list[str]:
    """Public wrapper: returns list of neighborhood slugs available on VivaReal
    for a given state/city. Costs 1 Firecrawl credit on first call; cached afterwards."""
    return _descobrir_bairros_vivareal(estado, cidade)


def slug_bairro_para_nome(slug: str) -> str:
    """Converts a VivaReal slug like 'jardim-residencial-dr-lessa' to 'Jardim Residencial Dr Lessa'."""
    if not slug:
        return ""
    return slug.replace("-", " ").title()


def resolver_bairro_para_vivareal(
    bairro: str,
    estado: str,
    cidade: str,
) -> tuple[str, str]:
    """Corrige o nome do bairro para o slug exato do VivaReal.
    Retorna (nome_humanizado, slug). Se não encontrar match, retorna (bairro_original, "")."""
    if not bairro or not estado or not cidade:
        return (bairro or "", "")
    slugs = _descobrir_bairros_vivareal(estado, cidade)
    if not slugs:
        return (bairro, "")
    slug = _resolver_bairro_vivareal(bairro, slugs)
    if slug:
        return (slug_bairro_para_nome(slug), slug)
    return (bairro, "")


__all__ = [
    "ColetaAnunciosMercadoResult",
    "ColetaVivaRealListagemResult",
    "TABLE_ANUNCIOS_MERCADO",
    "AnuncioMercadoPersist",
    "buscar_anuncios_similares_supabase",
    "chave_estado",
    "coletar_e_persistir_via_ddgs",
    "coletar_vivareal_listagem",
    "descobrir_bairros_vivareal",
    "resolver_bairro_para_vivareal",
    "estatisticas_comparaveis",
    "firecrawl_account_credits",
    "firecrawl_status",
    "formatar_anuncios_para_prompt",
    "precisa_coletar_mais_anuncios",
    "resolver_contexto_mercado_anuncios",
    "resolver_contexto_mercado_anuncios_detalhado",
    "selecionar_top_comparaveis",
    "sincronizar_amostras_e_atualizar_cache_media_bairro",
    "slug_bairro_para_nome",
]
