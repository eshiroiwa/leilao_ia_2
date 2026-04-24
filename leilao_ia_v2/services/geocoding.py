"""
Geocodificação de endereços via Nominatim (OpenStreetMap) — 100% gratuito.

Lógica equivalente ao módulo do projeto de referência (`codigo referencia/geocoding.py`),
mantida aqui para o pacote `leilao_ia_v2` não depender daquela pasta.

Recursos:
  - Queries estruturadas (street/city/state) para evitar que componentes
    desconhecidos do Nominatim (ex: bairros) "envenenem" a busca.
  - Cache em memória (LRU) para evitar chamadas repetidas.
  - Rate-limit de 1 req/seg exigido pelo Nominatim.
  - Com logradouro sem número: bairro+cidade → rua+cidade; correção de “hub” SP vs bairro.
  - Fallback progressivo: rua+bairro (conforme acima) → só cidade.
"""

from __future__ import annotations

import logging
import re
import threading
import time
import unicodedata
from functools import lru_cache
from typing import Optional

from leilao_ia_v2.services.geo_medicao import haversine_km
from geopy.exc import GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable

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
                    user_agent="leilao-ia-v2-geocoder/1.0 (contact: leilao-ia)",
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
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError, ValueError, TypeError):
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
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError, ValueError, TypeError):
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


def _fold_cidade_compacta(cidade: str) -> str:
    t = "".join(
        c for c in unicodedata.normalize("NFD", (cidade or "").lower()) if unicodedata.category(c) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", "", t)


def _cidade_e_municipio_sao_paulo(cidade: str) -> bool:
    return _fold_cidade_compacta(cidade) == "saopaulo"


# Hub onde o Nominatim costuma cair com "rua sem número" + cidade grande (produção).
_SP_HUB_LAT_LON: tuple[float, float] = (-23.435484, -46.5822336)
_HUB_RAIO_KM: float = 2.5


def _logradouro_tem_numero_imovel(logr: str) -> bool:
    """
    Indica se o logradouro traz indício de número de porta / bloco / apto.
    Só a rua (ex.: "Rua Barão … Anhumas") → False → priorizar bairro na geocodificação.
    """
    s = (logr or "").strip()
    if len(s) < 3:
        return False
    s = re.sub(r"(?i)\bcep\s*[:]?\s*[\d.\s-]+", "", s)
    s = re.sub(r"\b\d{5}\s*[-]?\s*\d{3}\b", "", s)
    # Metragem (ex. ", 38 m²" no título) não é número de imóvel — evita falso "com número" e o hub.
    s = re.sub(r"(?i),\s*\d{1,4}\s*m[²2]\b", "", s)
    if re.search(r"(?i),?\s*n[º°.]?\s*\d+", s):
        return True
    if re.search(r"(?i),?\s*apto\.?\s*\d+", s):
        return True
    if re.search(r",\s*\d{1,5}\b(?!\s*m[²2])", s):
        return True
    if re.search(r"(?i)\bbl\.?\s*\d+", s) or re.search(r"(?i)bloco\s+\d+", s):
        return True
    if re.search(r"(?<![\d,])\s+(\d{1,4})\s*$", s.strip()):
        return True
    return False


def _coordenada_parece_hub_sp_central(lat: float, lon: float) -> bool:
    return haversine_km(lat, lon, _SP_HUB_LAT_LON[0], _SP_HUB_LAT_LON[1]) <= _HUB_RAIO_KM


def _geocodificar_tentativas_bairro_cidade(
    bairro: str,
    cidade: str,
    estado_nome: str,
    uf: str,
) -> Optional[tuple[float, float]]:
    b = (bairro or "").strip()
    c = (cidade or "").strip()
    if not b or not c:
        return None
    ufx = (uf or "").strip()
    for q in (
        f"{b}, {c}, {ufx}, Brasil" if ufx else f"{b}, {c}, Brasil",
        f"{b}, bairro de {c}, {estado_nome or ufx}, Brasil"
        if (estado_nome or ufx)
        else f"{b}, bairro de {c}, Brasil",
    ):
        result = _geocode_cached(q)
        if result is not None:
            return result
    return _geocode_structured_cached("", c + " " + b, estado_nome)


def _corrigir_coordenada_hub_sp_se_aplicavel(
    coords: tuple[float, float],
    *,
    logradouro: str,
    bairro: str,
    cidade: str,
    estado_nome: str,
    uf: str,
) -> tuple[float, float]:
    la, lo = coords
    if not _cidade_e_municipio_sao_paulo(cidade):
        return coords
    if not _coordenada_parece_hub_sp_central(la, lo):
        return coords
    if _logradouro_tem_numero_imovel(logradouro):
        return coords
    bai = (bairro or "").strip()
    if not bai:
        return coords
    alt = _geocodificar_tentativas_bairro_cidade(bai, cidade.strip(), estado_nome, uf)
    if alt is None:
        return coords
    a_la, a_lo = alt
    if _coordenada_parece_hub_sp_central(a_la, a_lo):
        return coords
    return alt


def geocodificar_endereco(
    *,
    logradouro: str = "",
    bairro: str = "",
    cidade: str = "",
    estado: str = "",
    permitir_fallback_bairro: bool = True,
    permitir_fallback_centro_cidade: bool = True,
) -> Optional[tuple[float, float]]:
    """
    Geocodifica endereço brasileiro em (lat, lon).
    Se o logradouro **não** tiver número de imóvel mas houver bairro, tenta **primeiro**
    bairro+cidade (evita ponto genérico longe do bairro em cidades grandes).
    Depois: rua estruturada → texto livre → bairro (se ainda não tentado) → só cidade.
    """
    logr = logradouro.strip()
    bai = bairro.strip()
    cid = cidade.strip()
    uf = estado.strip()
    estado_nome = _estado_para_nome_completo(uf) if uf else ""

    rua_sem_numero = bool(logr and cid and not _logradouro_tem_numero_imovel(logr))
    bairro_ja_tentado_no_inicio = False

    if rua_sem_numero and permitir_fallback_bairro and bai and cid:
        result = _geocodificar_tentativas_bairro_cidade(bai, cid, estado_nome, uf)
        bairro_ja_tentado_no_inicio = True
        if result is not None:
            return result

    if logr and cid:
        result = _geocode_structured_cached(logr, cid, estado_nome)
        if result is not None:
            result = _corrigir_coordenada_hub_sp_se_aplicavel(
                result,
                logradouro=logr,
                bairro=bai,
                cidade=cid,
                estado_nome=estado_nome,
                uf=uf,
            )
            return result

        freetext_parts = [logr, cid]
        if uf:
            freetext_parts.append(uf)
        freetext_parts.append("Brasil")
        result = _geocode_cached(", ".join(freetext_parts))
        if result is not None:
            result = _corrigir_coordenada_hub_sp_se_aplicavel(
                result,
                logradouro=logr,
                bairro=bai,
                cidade=cid,
                estado_nome=estado_nome,
                uf=uf,
            )
            return result

    if permitir_fallback_bairro:
        if bai and cid and not bairro_ja_tentado_no_inicio:
            result = _geocodificar_tentativas_bairro_cidade(bai, cid, estado_nome, uf)
            if result is not None:
                return result
        if cid and permitir_fallback_centro_cidade:
            fallback_parts = [cid]
            if uf:
                fallback_parts.append(uf)
            fallback_parts.append("Brasil")
            result = _geocode_cached(", ".join(fallback_parts))
            if result is not None:
                return result

    return None


def geocodificar_texto_livre(endereco: str) -> Optional[tuple[float, float]]:
    """
    Geocodifica uma linha de endereço em texto livre (Nominatim, Brasil).
    Uso: correção manual do endereço completo quando os campos do registo estão errados.
    Reutiliza a cache LRU interna (mesma query = mesma resposta, sem novas requisições).
    """
    q = (endereco or "").strip()
    if len(q) < 4:
        return None
    low = q.lower()
    if "brasil" not in low and "brazil" not in low:
        q = f"{q}, Brasil"
    return _geocode_cached(q)


# ---------------------------------------------------------------------------
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


def extrair_logradouro_de_url(url: str) -> str:
    """
    Extrai nome de rua/avenida a partir da slug da URL (ex.: Viva Real).
    Para em palavras que indicam início de bairro (residencial, jardim, etc.).
    """
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


def sanear_logradouro_markdown_card(s: str, *, max_len: int = 240) -> str:
    """Remove barras invertidas de escape Markdown e lixo no fim (listagens Firecrawl / cards)."""
    t = (s or "").strip()
    while t.endswith("\\"):
        t = t[:-1].strip()
    t = re.sub(r"\\+\s*$", "", t).strip()
    t = re.sub(r"(?m)\\\s*$", "", t).strip()
    t = re.sub(r"\s+", " ", t).strip()
    return t[:max_len]


_RE_LOGRADOURO_LINHA_MD = re.compile(
    rf"(?:^|\n)\s*((?:{_PREFIXOS_RUA_REGEX})[\s\u00a0]+[^\n]{{4,140}})",
    re.I,
)
# Chaves na Mão / Zap: blocos com rótulo antes do texto (nem sempre começa por "Rua")
_RE_ENDERECO_ROTULO_MD = re.compile(
    r"(?:^|\n)\s*(?:Endere[cç]o|Localiza[cç][aã]o|Onde\s+fica|"
    r"Endere[cç]o\s+do\s+im[oó]vel)\s*:?\s*([^\n]{6,180})",
    re.I,
)


def melhor_logradouro_janela_proximo_url(bloco: str, url_index: int, *, max_len: int = 240) -> str:
    """
    Linha de endereço no markdown mais próxima da URL: prefixo Rua/Av.… ou rótulos tipo
    ``Endereço:`` / ``Localização:`` (comum em Chaves na Mão e outros portais no Firecrawl).
    """
    if not (bloco or "").strip():
        return ""
    candidatos: list[tuple[int, str]] = []
    for m in _RE_LOGRADOURO_LINHA_MD.finditer(bloco):
        seg = sanear_logradouro_markdown_card(m.group(1).strip())
        if len(seg) >= 6:
            candidatos.append((m.start(1), seg))
    for m in _RE_ENDERECO_ROTULO_MD.finditer(bloco):
        seg = sanear_logradouro_markdown_card(m.group(1).strip())
        if len(seg) < 6:
            continue
        if re.match(r"^https?://", seg, re.I):
            continue
        if re.fullmatch(r"[\s,.-]+", seg):
            continue
        if re.fullmatch(
            r"(?i)indispon[ií]vel|n/?a|n[aã]o\s+informad[oa]|a\s+combinar",
            seg.strip(),
        ):
            continue
        candidatos.append((m.start(1), seg))
    best = ""
    best_d = 10**9
    for start_idx, seg in candidatos:
        d = abs(start_idx - max(0, url_index))
        if d < best_d:
            best_d, best = d, seg
    return best[:max_len] if best else ""


def extrair_logradouro_do_titulo_imovel(titulo: str) -> str:
    """API pública: tenta isolar rua/avenida a partir do título do anúncio."""
    return _extrair_logradouro_de_titulo(titulo)


def _slug_texto(s: str) -> str:
    txt = unicodedata.normalize("NFKD", str(s or "").strip()).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", txt).strip().lower()


def _limpar_logradouro(logr: str, cidade: str = "", bairro: str = "") -> str:
    """Remove caracteres de escape, bairro/cidade do final, e sufixos indesejados."""
    s = sanear_logradouro_markdown_card(logr).strip().rstrip("\\/ ")
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
    permitir_fallback_centro_cidade: bool = False,
) -> int:
    """
    Enriquece uma lista de anúncios (dicts in-place) com latitude/longitude.
    Usa queries estruturadas Nominatim para máxima precisão.
    Tenta extrair logradouro do card, título ou URL para geocodificação precisa.
    Sem logradouro, tenta bairro+cidade; por defeito **não** usa só o centro da cidade
    (``permitir_fallback_centro_cidade=False``), alinhado a comparáveis no Viva Real.
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
            raw_url = extrair_logradouro_de_url(str(an.get("url_anuncio") or ""))
            logradouro = _limpar_logradouro(raw_url, cid, bairro)

        coords = geocodificar_endereco(
            logradouro=logradouro,
            bairro=bairro,
            cidade=cid,
            estado=uf,
            permitir_fallback_centro_cidade=permitir_fallback_centro_cidade,
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
