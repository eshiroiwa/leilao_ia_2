"""
Validação de município por evidência **em camadas** + precisão de geocode.

Para cidades pequenas (Pindamonhangaba, Cruzeiro, Atibaia, …) o geocode "sem
cidade" frequentemente falha ou desambigua para o município errado quando o
anúncio só expõe um bairro genérico ("Centro", "Santana"). Para reduzir o
descarte de cards legítimos sem perder a defesa contra o bug
*"Pindamonhangaba → São Bernardo"*, a validação tem **três camadas de
evidência** com pesos progressivamente mais altos:

1. **Texto local**: o nome da cidade-alvo aparece no markdown da janela do
   card (campo ``cidade_no_markdown`` produzido pelo extrator). Match
   determinístico, sem chamada de rede.
2. **Geocode + reverse** (camada original): geocodifica rua/bairro/UF SEM
   incluir cidade, depois reverse-geocodifica para descobrir o município real
   e compara com o alvo.
3. **Página confirmada**: a página inteira foi marcada como ``CONFIRMADA``
   pelo :mod:`comparaveis.pagina_filtro` (cidade-alvo em H1/título/breadcrumb).
   Se o geocode da camada 2 falhou ou divergiu, ainda assim aceitamos o card.

Quando a validação passa por (1) ou (3) — ou seja, **sem** geocode válido —,
chamamos :func:`obter_coordenadas_com_cidade` para obter lat/lng precisos
**incluindo** o nome da cidade-alvo na query (só é seguro porque já temos
evidência textual independente).

**Precisão do geocode** (decisão A do plano A+B):
:func:`obter_coordenadas_com_cidade` e :func:`geocode_sem_cidade` devolvem,
para além de ``(lat, lon)``, uma classificação textual da precisão:

- ``"rooftop"``  — número exacto (Google: ``ROOFTOP`` / ``RANGE_INTERPOLATED``).
- ``"rua"``       — centróide de uma rua (sem número).
- ``"bairro"``    — centróide de bairro/sublocality.
- ``"cidade"``    — centróide de município/locality.
- ``"desconhecido"`` — fallback inseguro (raro).

A precisão é exposta em :class:`ResultadoValidacaoMunicipio.precisao_geo` para
o caller (persistência, refino) decidir como usar a coordenada.

Provider primário é Google Maps (decisão 3-A: ~$0.005/chamada). Quando
``GOOGLE_MAPS_API_KEY`` não está disponível, cai para Nominatim. As chamadas
HTTP são isoladas para mock fácil em testes.
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


# Classificações canónicas de precisão (ordem da maior para a menor).
PRECISAO_ROOFTOP = "rooftop"
PRECISAO_RUA = "rua"
PRECISAO_BAIRRO = "bairro"
PRECISAO_CIDADE = "cidade"
PRECISAO_DESCONHECIDA = "desconhecido"

_PRECISOES_VALIDAS: tuple[str, ...] = (
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_DESCONHECIDA,
)


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
# Classificação de precisão (Google + Nominatim)
# -----------------------------------------------------------------------------

# Tipos no Google address_components / result.types que indicam o nível
# de granularidade do geocode.
_GOOGLE_TIPOS_RUA: frozenset[str] = frozenset({"route", "street_address"})
_GOOGLE_TIPOS_ROOFTOP: frozenset[str] = frozenset({"premise", "subpremise", "street_address"})
_GOOGLE_TIPOS_BAIRRO: frozenset[str] = frozenset(
    {
        "sublocality",
        "sublocality_level_1",
        "sublocality_level_2",
        "neighborhood",
        "postal_code",
    }
)
_GOOGLE_TIPOS_CIDADE_LOCALIDADE: frozenset[str] = frozenset(
    {
        "locality",
        "postal_town",
        "administrative_area_level_2",
        "administrative_area_level_3",
        "administrative_area_level_4",
    }
)


def _classificar_precisao_google(result: dict) -> str:
    """Classifica a precisão de UM resultado de Google Geocoding.

    Combina ``geometry.location_type`` com ``types`` do próprio resultado:

    - ``ROOFTOP`` / ``RANGE_INTERPOLATED``                 → rooftop
    - ``GEOMETRIC_CENTER`` ou ``APPROXIMATE`` + types:
        - ``route``/``street_address``                       → rua
        - ``sublocality*`` / ``neighborhood`` / ``postal_code`` → bairro
        - ``locality`` / ``administrative_area_level_*``     → cidade
    - tudo o resto                                          → desconhecido
    """
    if not isinstance(result, dict):
        return PRECISAO_DESCONHECIDA
    geom = result.get("geometry") or {}
    loc_type = str(geom.get("location_type") or "").upper()
    if loc_type in ("ROOFTOP", "RANGE_INTERPOLATED"):
        return PRECISAO_ROOFTOP

    types = {str(t).lower() for t in (result.get("types") or [])}
    if types & _GOOGLE_TIPOS_RUA:
        return PRECISAO_RUA
    if types & _GOOGLE_TIPOS_BAIRRO:
        return PRECISAO_BAIRRO
    if types & _GOOGLE_TIPOS_CIDADE_LOCALIDADE:
        return PRECISAO_CIDADE
    if loc_type == "GEOMETRIC_CENTER":
        return PRECISAO_RUA
    if loc_type == "APPROXIMATE":
        return PRECISAO_CIDADE
    return PRECISAO_DESCONHECIDA


# Combinações class/type do Nominatim que indicam o nível de granularidade.
_NOMINATIM_TIPOS_ROOFTOP: frozenset[str] = frozenset(
    {"building", "house", "residential", "apartments", "house_number"}
)
_NOMINATIM_TIPOS_RUA: frozenset[str] = frozenset(
    {"residential", "primary", "secondary", "tertiary", "unclassified", "service", "road"}
)
_NOMINATIM_TIPOS_BAIRRO: frozenset[str] = frozenset(
    {"suburb", "neighbourhood", "quarter", "city_block", "borough"}
)
_NOMINATIM_TIPOS_CIDADE: frozenset[str] = frozenset(
    {"city", "town", "village", "municipality", "hamlet", "administrative"}
)


def _classificar_precisao_nominatim(item: dict) -> str:
    """Classifica a precisão de UM resultado Nominatim via class/type/addresstype.

    Nominatim devolve ``class`` (categoria geral, ex.: ``highway``) e
    ``type`` (subtipo, ex.: ``residential``). ``addresstype`` (quando
    presente) é o melhor indicador agregado.

    Ordem de prioridade: rua (class=highway/addresstype=road) > rooftop
    (building/house/apartments) > bairro > cidade. ``class=highway`` força
    rua mesmo quando ``type=residential`` (porque "residential" como tipo de
    via designa rua residencial, não imóvel residencial).
    """
    if not isinstance(item, dict):
        return PRECISAO_DESCONHECIDA
    addresstype = str(item.get("addresstype") or "").lower()
    klass = str(item.get("class") or "").lower()
    tipo = str(item.get("type") or "").lower()

    if klass == "highway" or addresstype == "road":
        return PRECISAO_RUA
    if (
        addresstype in _NOMINATIM_TIPOS_ROOFTOP
        or tipo in _NOMINATIM_TIPOS_ROOFTOP
        or klass == "building"
    ):
        return PRECISAO_ROOFTOP
    if tipo in _NOMINATIM_TIPOS_RUA:
        return PRECISAO_RUA
    if addresstype in _NOMINATIM_TIPOS_BAIRRO or tipo in _NOMINATIM_TIPOS_BAIRRO:
        return PRECISAO_BAIRRO
    if addresstype in _NOMINATIM_TIPOS_CIDADE or tipo in _NOMINATIM_TIPOS_CIDADE:
        return PRECISAO_CIDADE
    return PRECISAO_DESCONHECIDA


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


def _geocode_google(query: str) -> Optional[tuple[float, float, str]]:
    """Invoca Google Geocoding e devolve (lat, lon, precisao).

    ``query`` já contém (ou omite, conforme o caller) o nome da cidade.
    """
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
    primeiro = rs[0]
    loc = ((primeiro.get("geometry") or {}).get("location") or {})
    lat, lng = loc.get("lat"), loc.get("lng")
    if lat is None or lng is None:
        return None
    return (float(lat), float(lng), _classificar_precisao_google(primeiro))


def _geocode_nominatim(query: str) -> Optional[tuple[float, float, str]]:
    """Invoca Nominatim e devolve (lat, lon, precisao)."""
    if not query:
        return None
    _rate_limit_nominatim()
    params = urllib.parse.urlencode(
        {
            "q": query,
            "format": "json",
            "limit": "1",
            "countrycodes": "br",
            "addressdetails": "1",
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
        lat = float(item["lat"])
        lon = float(item["lon"])
    except (KeyError, ValueError, TypeError):
        return None
    return (lat, lon, _classificar_precisao_nominatim(item))


def geocode_sem_cidade(
    *, logradouro: str, bairro: str, estado_uf: str
) -> Optional[tuple[float, float, str]]:
    """Geocodifica um endereço **sem fornecer cidade**.

    Devolve ``(lat, lon, precisao)`` onde ``precisao`` é uma das constantes
    ``PRECISAO_*``. Usa Google quando ``GOOGLE_MAPS_API_KEY`` está definida;
    Nominatim caso contrário. Retorna ``None`` se nada for encontrado.
    """
    query = _construir_query_sem_cidade(
        logradouro=logradouro, bairro=bairro, estado_uf=estado_uf
    )
    if not query:
        return None
    if _provider_geocode() == "google":
        coords = _geocode_google(query)
        if coords is not None:
            return coords
        return _geocode_nominatim(query)
    return _geocode_nominatim(query)


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
# Geocode COM cidade (apenas para coords finais, depois de validada por outras
# evidências independentes — texto local ou página confirmada)
# -----------------------------------------------------------------------------

def _construir_query_com_cidade(
    *,
    logradouro: str,
    bairro: str,
    cidade: str,
    estado_uf: str,
) -> str:
    """Monta query free-text incluindo a cidade.

    Só deve ser usada quando já existe evidência **independente** de que o card
    pertence à cidade — caso contrário, viola a separação que protege contra o
    bug Pindamonhangaba → São Bernardo (a cidade do leilão enviesaria o
    geocoding).
    """
    cid = (cidade or "").strip()
    uf = (estado_uf or "").strip().upper()
    if not cid:
        return ""
    partes = [
        (logradouro or "").strip(),
        (bairro or "").strip(),
        cid,
        uf,
        "Brasil",
    ]
    return ", ".join(p for p in partes if p)


def obter_coordenadas_com_cidade(
    *,
    logradouro: str,
    bairro: str,
    cidade: str,
    estado_uf: str,
) -> Optional[tuple[float, float, str]]:
    """Geocode lat/lng usando o nome da cidade como hint (mais preciso).

    Devolve ``(lat, lon, precisao)`` onde precisao ∈ ``PRECISAO_*``.

    .. warning::

       **Só** chame esta função para um card cuja pertença à ``cidade`` já tenha
       sido confirmada por evidência independente (``cidade_no_markdown`` ou
       ``pagina_confirmada``). Se chamar antes da validação, a query enviesa
       o geocode e o controlo contra o bug histórico fica derrotado.

    Usa Google quando há ``GOOGLE_MAPS_API_KEY``; cai para Nominatim como
    fallback. Retorna ``None`` se ambos falharem (caller pode persistir sem
    coordenadas).
    """
    query = _construir_query_com_cidade(
        logradouro=logradouro, bairro=bairro, cidade=cidade, estado_uf=estado_uf
    )
    if not query:
        return None
    if _provider_geocode() == "google":
        coords = _geocode_google(query)
        if coords is not None:
            return coords
        return _geocode_nominatim(query)
    return _geocode_nominatim(query)


# -----------------------------------------------------------------------------
# API pública: validar_municipio_card
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ResultadoValidacaoMunicipio:
    """Resultado imutável da validação. Use em logs e na decisão de descartar.

    O campo :attr:`precisao_geo` reporta a precisão do geocode usado na coord
    final (uma das constantes ``PRECISAO_*``). Quando a validação reprova
    (``valido=False``) ou as coordenadas são ``None``, o valor fica vazio.
    """

    valido: bool
    motivo: str
    municipio_real: Optional[str] = None
    coordenadas: Optional[tuple[float, float]] = None
    municipio_alvo_slug: str = ""
    municipio_real_slug: str = ""
    precisao_geo: str = ""

    @property
    def deve_descartar(self) -> bool:
        return not self.valido


def _coords_xy(t: Optional[tuple[float, float, str]]) -> Optional[tuple[float, float]]:
    """Extrai (lat, lon) de uma tupla com precisão; devolve None se falhar."""
    if t is None:
        return None
    return (t[0], t[1])


def _coords_precisao(t: Optional[tuple[float, float, str]]) -> str:
    """Extrai a string de precisão de uma tupla; devolve "" se faltar."""
    if t is None or len(t) < 3:
        return ""
    return str(t[2] or "")


def validar_municipio_card(
    *,
    logradouro: str,
    bairro: str,
    estado_uf: str,
    cidade_alvo: str,
    cidade_no_markdown: str = "",
    pagina_confirmada: bool = False,
) -> ResultadoValidacaoMunicipio:
    """Valida que o endereço do card pertence à cidade-alvo do leilão.

    Hierarquia de evidência (camadas em ordem de preferência):

    1. **Texto local** — ``cidade_no_markdown`` (preenchido pelo extrator
       quando o nome da cidade-alvo está na janela do card).
       *Sem chamada de rede.* Se confere, motivo ``ok_texto_local``.
    2. **Geocode + reverse** (camada original) — geocodifica rua+bairro+UF
       sem cidade, depois reverse para descobrir o município real.
       Se confere, motivo ``ok``.
    3. **Página confirmada** — ``pagina_confirmada=True`` significa que o
       :mod:`pagina_filtro` viu a cidade-alvo em posição privilegiada (H1,
       título, breadcrumb). Se a camada 2 falhou ou divergiu, ainda assim
       aceitamos o card. Motivo: ``ok_pagina_confirmada``.

    Quando passamos por (1) ou (3), as coordenadas finais são obtidas via
    :func:`obter_coordenadas_com_cidade` (geocode COM cidade, mais preciso),
    pois nessa altura já temos garantia independente de pertencimento.

    O campo ``precisao_geo`` no resultado expõe a precisão da coord final
    para o caller (persistência) decidir se grava lat/lon, aplica jitter
    ou apenas marca o anúncio como aproximado.

    Args:
        logradouro: rua do anúncio (extraída do markdown ou do título).
        bairro: bairro do anúncio (extraído do anúncio, NÃO do leilão).
        estado_uf: UF de 2 letras (vem do leilão).
        cidade_alvo: município do leilão (será comparado por slug).
        cidade_no_markdown: nome da cidade-alvo se o extrator a encontrou na
            janela do card (sinal forte e gratuito).
        pagina_confirmada: True se :mod:`pagina_filtro` confirmou a página
            inteira como sendo da cidade-alvo. Permite rescue da camada 3.

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

    if cidade_no_markdown and _slug(cidade_no_markdown) == alvo_slug:
        coords_local = obter_coordenadas_com_cidade(
            logradouro=logradouro,
            bairro=bairro,
            cidade=cidade_alvo,
            estado_uf=estado_uf,
        )
        return ResultadoValidacaoMunicipio(
            valido=True,
            motivo="ok_texto_local",
            municipio_real=cidade_alvo,
            coordenadas=_coords_xy(coords_local),
            municipio_alvo_slug=alvo_slug,
            municipio_real_slug=alvo_slug,
            precisao_geo=_coords_precisao(coords_local),
        )

    coords = geocode_sem_cidade(
        logradouro=logradouro, bairro=bairro, estado_uf=estado_uf
    )
    municipio_real: Optional[str] = None
    real_slug = ""
    motivo_fallback_geocode = ""

    if coords is None:
        motivo_fallback_geocode = "geocode_falhou"
    else:
        municipio_real = reverse_municipio(coords[0], coords[1])
        if not municipio_real:
            motivo_fallback_geocode = "reverse_falhou"
        else:
            real_slug = _slug(municipio_real)
            if real_slug == alvo_slug:
                return ResultadoValidacaoMunicipio(
                    valido=True,
                    motivo="ok",
                    municipio_real=municipio_real,
                    coordenadas=_coords_xy(coords),
                    municipio_alvo_slug=alvo_slug,
                    municipio_real_slug=real_slug,
                    precisao_geo=_coords_precisao(coords),
                )
            motivo_fallback_geocode = "municipio_diferente"

    if pagina_confirmada:
        coords_pag = obter_coordenadas_com_cidade(
            logradouro=logradouro,
            bairro=bairro,
            cidade=cidade_alvo,
            estado_uf=estado_uf,
        )
        return ResultadoValidacaoMunicipio(
            valido=True,
            motivo="ok_pagina_confirmada",
            municipio_real=cidade_alvo,
            coordenadas=_coords_xy(coords_pag),
            municipio_alvo_slug=alvo_slug,
            municipio_real_slug=alvo_slug,
            precisao_geo=_coords_precisao(coords_pag),
        )

    return ResultadoValidacaoMunicipio(
        valido=False,
        motivo=motivo_fallback_geocode,
        municipio_real=municipio_real,
        coordenadas=_coords_xy(coords),
        municipio_alvo_slug=alvo_slug,
        municipio_real_slug=real_slug,
        precisao_geo=_coords_precisao(coords),
    )
