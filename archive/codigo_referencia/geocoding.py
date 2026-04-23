"""
Geocodificação de endereços via Nominatim (OpenStreetMap) — 100% gratuito.

Recursos:
  - Queries estruturadas (street/city/state) para evitar que componentes
    desconhecidos do Nominatim (ex: bairros) "envenenem" a busca.
  - Cache em memória (LRU) para evitar chamadas repetidas.
  - Rate-limit de 1 req/seg exigido pelo Nominatim.
  - Fallback progressivo: rua+cidade → cidade.
"""

from __future__ import annotations

import logging
import re
import threading
import time
import unicodedata
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

_nominatim_lock = threading.Lock()
_last_call_ts: float = 0.0
_MIN_INTERVAL_SEC = 1.1

_geocoder = None
_geocoder_lock = threading.Lock()


def _get_geocoder():
    global _geocoder
    if _geocoder is None:
        with _geocoder_lock:
            if _geocoder is None:
                from geopy.geocoders import Nominatim
                _geocoder = Nominatim(
                    user_agent="leilao-ia-geocoder/1.0",
                    timeout=10,
                )
    return _geocoder


def _rate_limit() -> None:
    """Aguarda o intervalo mínimo entre chamadas ao Nominatim."""
    global _last_call_ts
    with _nominatim_lock:
        now = time.monotonic()
        elapsed = now - _last_call_ts
        if elapsed < _MIN_INTERVAL_SEC:
            time.sleep(_MIN_INTERVAL_SEC - elapsed)
        _last_call_ts = time.monotonic()


@lru_cache(maxsize=4096)
def _geocode_cached(query: str) -> Optional[tuple[float, float]]:
    """Chamada real ao Nominatim com query free-text (cacheada)."""
    _rate_limit()
    try:
        loc = _get_geocoder().geocode(query, country_codes="br")
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception:
        logger.debug("Nominatim falhou para query: %s", query, exc_info=True)
    return None


@lru_cache(maxsize=4096)
def _geocode_structured_cached(
    street: str, city: str, state: str,
) -> Optional[tuple[float, float]]:
    """Chamada ao Nominatim com query estruturada (street/city/state separados).
    Evita que bairros inexistentes no OSM 'envenenem' a busca."""
    _rate_limit()
    q: dict[str, str] = {"country": "Brazil"}
    if street:
        q["street"] = street
    if city:
        q["city"] = city
    if state:
        q["state"] = state
    try:
        loc = _get_geocoder().geocode(q, country_codes="br")
        if loc:
            return (loc.latitude, loc.longitude)
    except Exception:
        logger.debug("Nominatim structured falhou: %s", q, exc_info=True)
    return None


_UF_PARA_NOME_ESTADO: dict[str, str] = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal",
    "ES": "Espírito Santo", "GO": "Goiás", "MA": "Maranhão",
    "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais",
    "PA": "Pará", "PB": "Paraíba", "PR": "Paraná", "PE": "Pernambuco",
    "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima",
    "SC": "Santa Catarina", "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins",
}


def _estado_para_nome_completo(uf: str) -> str:
    """Converte UF de 2 letras para nome completo (Nominatim structured funciona
    melhor com nome completo do estado)."""
    s = uf.strip().upper()
    return _UF_PARA_NOME_ESTADO.get(s, s)


def geocodificar_endereco(
    *,
    logradouro: str = "",
    bairro: str = "",
    cidade: str = "",
    estado: str = "",
    permitir_fallback_bairro: bool = True,
) -> Optional[tuple[float, float]]:
    """
    Geocodifica endereço brasileiro em (lat, lon).
    Usa queries estruturadas (Nominatim) para isolamento de componentes:
      1) structured: street=logradouro, city=cidade, state=estado
      2) free-text:  logradouro, cidade, estado, Brasil  (fallback)
      3) free-text:  cidade, estado, Brasil               (se permitir_fallback_bairro)
    """
    logr = logradouro.strip()
    bai = bairro.strip()
    cid = cidade.strip()
    uf = estado.strip()
    estado_nome = _estado_para_nome_completo(uf) if uf else ""

    if logr and cid:
        result = _geocode_structured_cached(logr, cid, estado_nome)
        if result is not None:
            return result

        freetext_parts = [logr, cid]
        if uf:
            freetext_parts.append(uf)
        freetext_parts.append("Brasil")
        result = _geocode_cached(", ".join(freetext_parts))
        if result is not None:
            return result

    if permitir_fallback_bairro:
        if bai and cid:
            result = _geocode_structured_cached("", cid + " " + bai, estado_nome)
            if result is not None:
                return result
        if cid:
            fallback_parts = [cid]
            if uf:
                fallback_parts.append(uf)
            fallback_parts.append("Brasil")
            result = _geocode_cached(", ".join(fallback_parts))
            if result is not None:
                return result

    return None


# ---------------------------------------Selecionar todos marca apenas as linhas visíveis após os filtros do expander acima.------------------------------------
# Extração de logradouro de diferentes fontes
# ---------------------------------------------------------------------------

_PREFIXOS_RUA = (
    "Rua", "Avenida", "Av.", "R.", "Alameda", "Al.", "Travessa", "Tv.",
    "Estrada", "Rod.", "Rodovia", "Largo", "Praça", "Pc.", "Servidão", "Beco",
)

_PREFIXOS_RUA_REGEX = "|".join(re.escape(p) for p in _PREFIXOS_RUA)

_RE_LOGRADOURO_TITULO = re.compile(
    rf"(?:{_PREFIXOS_RUA_REGEX})\s+[^\-,]{{3,60}}",
    re.IGNORECASE,
)

_STOP_WORDS_TITULO = re.compile(
    r"\s+(?:em\s|no\s|na\s|com\s|para\s|de\s+\d|\d+\s*m[²2])",
    re.IGNORECASE,
)

_PREFIXOS_URL = (
    "rua", "avenida", "alameda", "travessa", "estrada",
    "rodovia", "largo", "praca", "beco", "servidao",
)
_PREFIXOS_URL_RE = "|".join(_PREFIXOS_URL)

_RE_LOGRADOURO_URL = re.compile(
    rf"(?:^|/|-)"
    rf"((?:{_PREFIXOS_URL_RE})"
    r"(?:-[a-z]{2,}){1,8})"
    r"(?=-\d|$|/)",
    re.IGNORECASE,
)

_STOP_SLUGS_URL = {
    "residencial", "condominio", "jardim", "parque", "vila",
    "centro", "bairro", "loteamento", "conjunto", "chacara",
    "sitio", "fazenda", "nucleo",
}


def _extrair_logradouro_de_titulo(titulo: str) -> str:
    """Tenta extrair nome de rua/avenida do título do anúncio."""
    m = _RE_LOGRADOURO_TITULO.search(titulo)
    if not m:
        return ""
    raw = m.group(0).strip()
    stop = _STOP_WORDS_TITULO.search(raw)
    if stop:
        raw = raw[:stop.start()].strip()
    return raw if len(raw) >= 5 else ""


def _extrair_logradouro_de_url(url: str) -> str:
    """Tenta extrair nome de rua/avenida da URL do anúncio (ex: vivareal).
    URLs como /imovel/casa-3-quartos-rua-das-flores-bairro-250m2-.../
    Para em palavras que indicam início de bairro (residencial, jardim, etc.)."""
    if not url:
        return ""
    m = _RE_LOGRADOURO_URL.search(url)
    if not m:
        return ""
    raw = m.group(1)
    parts = raw.split("-")

    cleaned: list[str] = []
    for p in parts:
        if p.lower() in _STOP_SLUGS_URL and len(cleaned) >= 2:
            break
        cleaned.append(p)

    if len(cleaned) < 2:
        return ""
    return " ".join(p.capitalize() for p in cleaned)


def _slug_texto(s: str) -> str:
    txt = unicodedata.normalize("NFKD", str(s or "").strip()).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", txt).strip().lower()


def _limpar_logradouro(logr: str, cidade: str = "", bairro: str = "") -> str:
    """Remove caracteres de escape, bairro/cidade do final, e sufixos indesejados."""
    s = logr.strip().rstrip("\\/ ")
    s = s.replace("\\", "")
    stop = _STOP_WORDS_TITULO.search(s)
    if stop:
        s = s[:stop.start()].strip()
    s_lower = _slug_texto(s)
    for ctx in (cidade, bairro):
        ctx_lower = _slug_texto(ctx)
        if ctx_lower and len(ctx_lower) >= 3 and s_lower.endswith(ctx_lower):
            s = s[:len(s) - len(ctx)].strip().rstrip(" -,")
            s_lower = _slug_texto(s)
    return s


# ---------------------------------------------------------------------------
# Batch geocoding
# ---------------------------------------------------------------------------

def geocodificar_anuncios_batch(
    anuncios: list[dict],
    *,
    cidade: str = "",
    estado: str = "",
    bairro_fallback: str = "",
) -> int:
    """
    Enriquece uma lista de anúncios (dicts in-place) com latitude/longitude.
    Usa queries estruturadas Nominatim para máxima precisão.
    Tenta extrair logradouro do card, título ou URL para geocodificação precisa.
    Se não encontrar logradouro, usa fallback bairro+cidade (centroide).
    Retorna quantos anúncios foram geocodificados com sucesso.
    """
    geocoded = 0
    nivel_rua = 0
    nivel_fallback = 0
    for an in anuncios:
        if an.get("latitude") and an.get("longitude"):
            geocoded += 1
            continue

        bairro = str(an.get("bairro") or bairro_fallback or "").strip()
        cid = str(an.get("cidade") or cidade or "").strip()
        uf = str(an.get("estado") or estado or "").strip()

        logradouro = _limpar_logradouro(str(an.get("logradouro") or ""), cid, bairro)
        if not logradouro:
            logradouro = _extrair_logradouro_de_titulo(str(an.get("titulo") or ""))
        if not logradouro:
            raw_url = _extrair_logradouro_de_url(str(an.get("url_anuncio") or ""))
            logradouro = _limpar_logradouro(raw_url, cid, bairro)

        coords = geocodificar_endereco(
            logradouro=logradouro,
            bairro=bairro,
            cidade=cid,
            estado=uf,
        )
        if coords:
            an["latitude"] = coords[0]
            an["longitude"] = coords[1]
            geocoded += 1
            if logradouro:
                nivel_rua += 1
            else:
                nivel_fallback += 1

    if anuncios:
        logger.info(
            "Geocodificação batch: %s/%s com coordenadas (rua=%s, fallback=%s)",
            geocoded, len(anuncios), nivel_rua, nivel_fallback,
        )
    return geocoded
