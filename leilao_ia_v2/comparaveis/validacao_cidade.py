"""
Validação dura de município por geocode + reverse geocode.

Esta é a peça que impede o bug "anúncio de São Bernardo aparecer como
Pindamonhangaba" descrito pelo usuário. A lógica é simples e brutal:

1. Geocodifica o endereço do card **sem fornecer cidade** (usa apenas o que o
   próprio anúncio expõe: rua, bairro, eventualmente o título).
2. Reverse-geocodifica as coordenadas obtidas para descobrir o município real.
3. Compara o município real com o município alvo (slug normalizado: lowercase,
   sem acentos, alfanumérico).
4. Se ≠, **descarta o card** com motivo registado.

Provider primário é Google Maps (decidido pelo utilizador na fase de design,
opção 3-A: "mais preciso, ~$0.005/chamada"). Quando ``GOOGLE_MAPS_API_KEY``
não está disponível, cai para Nominatim como fallback determinístico.

Este módulo NÃO depende do pacote `services/geocoding.py` antigo para evitar
acoplamento circular (e porque aquele módulo aceita `cidade` como hint, o que
é exactamente o que estamos a tentar evitar). As chamadas HTTP são pequenas
e isoladas para que os testes possam mockar facilmente.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import unicodedata
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


_GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
_NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
_NOMINATIM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse"
_NOMINATIM_USER_AGENT = "leilao-ia-v2-comparaveis/1.0 (contact: leilao-ia)"
_NOMINATIM_MIN_INTERVAL = 1.1

_nominatim_lock = threading.Lock()
_nominatim_last_call_ts = 0.0


def _slug(s: str) -> str:
    """Slug determinístico para comparar nomes de cidade.

    >>> _slug("São Paulo")
    'saopaulo'
    >>> _slug(" pindamonhangaba ")
    'pindamonhangaba'
    >>> _slug("Mogi das Cruzes")
    'mogidascruzes'
    """
    if not s:
        return ""
    base = "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", "", base)


def _provider_geocode() -> str:
    """Retorna 'google' se houver chave configurada, senão 'nominatim'."""
    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    return "google" if key else "nominatim"


def _rate_limit_nominatim() -> None:
    """Garante 1 req/s ao Nominatim (política de uso justo do OSM)."""
    global _nominatim_last_call_ts
    with _nominatim_lock:
        agora = time.monotonic()
        delta = agora - _nominatim_last_call_ts
        if delta < _NOMINATIM_MIN_INTERVAL:
            time.sleep(_NOMINATIM_MIN_INTERVAL - delta)
        _nominatim_last_call_ts = time.monotonic()


def _http_get_json(url: str, *, headers: Optional[dict[str, str]] = None, timeout: float = 12.0) -> Optional[dict]:
    """Wrapper minimalista sobre urllib para JSON. Retorna None em qualquer falha.

    Mantemo-lo aqui (e não num módulo de utils) para que os testes mockem
    apenas este símbolo via ``unittest.mock.patch``.
    """
    try:
        req = urllib.request.Request(url, headers=headers or {"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)
    except Exception:
        logger.debug("HTTP GET falhou: %s", url[:200], exc_info=True)
        return None


# -----------------------------------------------------------------------------
# Geocode "forward" SEM cidade: textos curtos só com rua + bairro + UF + país.
# -----------------------------------------------------------------------------

def _construir_query_sem_cidade(*, logradouro: str, bairro: str, estado_uf: str) -> str:
    """Monta query free-text que **omite deliberadamente** o nome da cidade.

    Nunca incluímos cidade aqui — ela é o que queremos *descobrir*. UF entra
    porque ajuda a desambiguar bairros homónimos entre estados (ex.: "Centro").

    Exige pelo menos `logradouro` OU `bairro` — sem nenhum dos dois, a query
    degeneraria em "SP, Brasil" e geocodificaria para o centroide do estado,
    o que faria com que qualquer cidade-alvo passasse na validação.
    """
    logr = (logradouro or "").strip()
    bai = (bairro or "").strip()
    if not logr and not bai:
        return ""
    partes = [
        logr,
        bai,
        (estado_uf or "").strip().upper(),
        "Brasil",
    ]
    partes = [p for p in partes if p]
    return ", ".join(partes)


def _geocode_google_sem_cidade(query: str) -> Optional[tuple[float, float]]:
    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key or not query:
        return None
    params = urllib.parse.urlencode(
        {
            "address": query,
            "components": "country:BR",
            "key": key,
            "language": "pt-BR",
            "region": "br",
        }
    )
    data = _http_get_json(f"{_GOOGLE_GEOCODE_URL}?{params}")
    if not data:
        return None
    if str(data.get("status") or "").upper() != "OK":
        return None
    rs = data.get("results") or []
    if not rs:
        return None
    loc = ((rs[0].get("geometry") or {}).get("location") or {})
    lat, lng = loc.get("lat"), loc.get("lng")
    if lat is None or lng is None:
        return None
    return (float(lat), float(lng))


def _geocode_nominatim_sem_cidade(query: str) -> Optional[tuple[float, float]]:
    if not query:
        return None
    _rate_limit_nominatim()
    params = urllib.parse.urlencode(
        {
            "q": query,
            "format": "json",
            "limit": "1",
            "countrycodes": "br",
            "addressdetails": "0",
        }
    )
    data = _http_get_json(
        f"{_NOMINATIM_SEARCH_URL}?{params}",
        headers={"User-Agent": _NOMINATIM_USER_AGENT, "Accept": "application/json"},
    )
    if not data or not isinstance(data, list) or not data:
        return None
    item = data[0]
    try:
        return (float(item["lat"]), float(item["lon"]))
    except (KeyError, ValueError, TypeError):
        return None


def geocode_sem_cidade(
    *, logradouro: str, bairro: str, estado_uf: str
) -> Optional[tuple[float, float]]:
    """Geocodifica um endereço **sem fornecer cidade**.

    Usa Google quando ``GOOGLE_MAPS_API_KEY`` está definida; Nominatim caso
    contrário. Retorna ``None`` se nada for encontrado (caller deve descartar
    o card nesse caso).
    """
    query = _construir_query_sem_cidade(
        logradouro=logradouro, bairro=bairro, estado_uf=estado_uf
    )
    if not query:
        return None
    if _provider_geocode() == "google":
        coords = _geocode_google_sem_cidade(query)
        if coords is not None:
            return coords
        return _geocode_nominatim_sem_cidade(query)
    return _geocode_nominatim_sem_cidade(query)


# -----------------------------------------------------------------------------
# Reverse geocode → município real
# -----------------------------------------------------------------------------

_GOOGLE_TIPOS_MUNICIPIO = (
    "locality",
    "administrative_area_level_2",
    "administrative_area_level_3",
)


def _reverse_google_municipio(lat: float, lon: float) -> Optional[str]:
    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        return None
    params = urllib.parse.urlencode(
        {
            "latlng": f"{lat:.7f},{lon:.7f}",
            "key": key,
            "language": "pt-BR",
            "result_type": "|".join(_GOOGLE_TIPOS_MUNICIPIO),
        }
    )
    data = _http_get_json(f"{_GOOGLE_GEOCODE_URL}?{params}")
    if not data or str(data.get("status") or "").upper() != "OK":
        return None
    for r in data.get("results") or []:
        for comp in r.get("address_components") or []:
            tipos = {str(t).lower() for t in (comp.get("types") or [])}
            if tipos & set(_GOOGLE_TIPOS_MUNICIPIO):
                nome = str(comp.get("long_name") or comp.get("short_name") or "").strip()
                if nome:
                    return nome
    return None


def _reverse_nominatim_municipio(lat: float, lon: float) -> Optional[str]:
    _rate_limit_nominatim()
    params = urllib.parse.urlencode(
        {
            "lat": f"{lat:.7f}",
            "lon": f"{lon:.7f}",
            "format": "json",
            "zoom": "10",
            "addressdetails": "1",
            "accept-language": "pt-BR",
        }
    )
    data = _http_get_json(
        f"{_NOMINATIM_REVERSE_URL}?{params}",
        headers={"User-Agent": _NOMINATIM_USER_AGENT, "Accept": "application/json"},
    )
    if not data or not isinstance(data, dict):
        return None
    addr = data.get("address") or {}
    if not isinstance(addr, dict):
        return None
    for k in ("city", "town", "municipality", "village", "county"):
        nome = str(addr.get(k) or "").strip()
        if nome:
            return nome
    return None


def reverse_municipio(lat: float, lon: float) -> Optional[str]:
    """Reverse geocode → nome do município (Google primário, Nominatim fallback)."""
    if _provider_geocode() == "google":
        nome = _reverse_google_municipio(lat, lon)
        if nome:
            return nome
        return _reverse_nominatim_municipio(lat, lon)
    return _reverse_nominatim_municipio(lat, lon)


# -----------------------------------------------------------------------------
# API pública: validar_municipio_card
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ResultadoValidacaoMunicipio:
    """Resultado imutável da validação. Use em logs e na decisão de descartar."""

    valido: bool
    motivo: str
    municipio_real: Optional[str] = None
    coordenadas: Optional[tuple[float, float]] = None
    municipio_alvo_slug: str = ""
    municipio_real_slug: str = ""

    @property
    def deve_descartar(self) -> bool:
        return not self.valido


def validar_municipio_card(
    *,
    logradouro: str,
    bairro: str,
    estado_uf: str,
    cidade_alvo: str,
) -> ResultadoValidacaoMunicipio:
    """Valida que o endereço do card pertence à cidade-alvo do leilão.

    Fluxo:

    1. Constrói query SEM cidade (rua + bairro + UF + Brasil).
    2. Geocodifica → coordenadas.
    3. Reverse-geocodifica → município real.
    4. Compara slug(cidade_alvo) == slug(municipio_real).

    Args:
        logradouro: rua do anúncio (extraída do markdown ou do título).
        bairro: bairro do anúncio (extraído do anúncio, NÃO do leilão).
        estado_uf: UF de 2 letras (vem do leilão; é o único hint geográfico
            permitido porque o anúncio raramente vai para outro estado).
        cidade_alvo: município do leilão (será comparado por slug).

    Returns:
        :class:`ResultadoValidacaoMunicipio` com bandeira ``valido``.
    """
    alvo_slug = _slug(cidade_alvo)
    if not alvo_slug:
        return ResultadoValidacaoMunicipio(
            valido=False,
            motivo="cidade_alvo_vazia",
            municipio_alvo_slug="",
        )

    coords = geocode_sem_cidade(
        logradouro=logradouro, bairro=bairro, estado_uf=estado_uf
    )
    if coords is None:
        return ResultadoValidacaoMunicipio(
            valido=False,
            motivo="geocode_falhou",
            municipio_alvo_slug=alvo_slug,
        )

    municipio = reverse_municipio(coords[0], coords[1])
    if not municipio:
        return ResultadoValidacaoMunicipio(
            valido=False,
            motivo="reverse_falhou",
            coordenadas=coords,
            municipio_alvo_slug=alvo_slug,
        )

    real_slug = _slug(municipio)
    if real_slug != alvo_slug:
        return ResultadoValidacaoMunicipio(
            valido=False,
            motivo="municipio_diferente",
            municipio_real=municipio,
            coordenadas=coords,
            municipio_alvo_slug=alvo_slug,
            municipio_real_slug=real_slug,
        )

    return ResultadoValidacaoMunicipio(
        valido=True,
        motivo="ok",
        municipio_real=municipio,
        coordenadas=coords,
        municipio_alvo_slug=alvo_slug,
        municipio_real_slug=real_slug,
    )
