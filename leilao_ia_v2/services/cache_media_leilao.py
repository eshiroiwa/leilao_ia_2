"""
Criação de ``cache_media_bairro`` a partir do leilão + ``anuncios_mercado`` (raio em km, tipo, faixa de área).

As amostras filtradas vêm **ordenadas** por proximidade geográfica e similaridade de área (``_filtrar_amostras``).
O **cache principal** (simulação) usa no máximo ``cache_max_amostras_principal`` (ajustes de busca / padrão
10) anúncios; o restante é gravado em **caches de referência** em lotes de ``cache_max_amostras_lote``. É possível gravar
com **1** amostra; com menos de ``CACHE_VOLUME_BAIXO_LIMITE`` amostras em um segmento, metadados marcam
``volume_amostras_baixo`` para alerta na UI.

**Terrenos** gravam ``modo_cache=terrenos`` (referência); com muitas amostras, o mesmo fatiamento aplica-se.

Raio, faixa de metragem e ``min_amostras_cache`` seguem ``get_busca_mercado_parametros()`` (ajustáveis na app
Streamlit): controlam **quando** tentar geocode/Firecrawl; ao final, se existir **≥ 1** amostra válida, o
cache é montado (com advertência de volume quando aplicável).

Fluxo económico: (1) candidatos no banco → (2) geocodificar só anúncios sem coordenadas
se ainda faltar amostra → (3) **no máximo uma** complementação via **Firecrawl Search** (web + scrape)
→ (4) se não houver nenhuma amostra válida, aborta com mensagem clara.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import statistics
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from supabase import Client

from leilao_ia_v2.config.busca_mercado_parametros import (
    get_busca_mercado_parametros,
    mensagem_com_dica_ajuste_busca,
)
from leilao_ia_v2.normalizacao import (
    normalizar_conservacao,
    normalizar_tipo_casa,
    normalizar_tipo_imovel,
)
from leilao_ia_v2.persistence import anuncios_mercado_repo, cache_media_bairro_repo, leilao_imoveis_repo
from leilao_ia_v2.services.exclusao_cache_listagem_leilao import filtrar_anuncios_mantendo_apenas_mercado_comparavel
from leilao_ia_v2.services.geocoding import (
    geocodificar_anuncios_batch,
    geocodificar_endereco,
    reverse_geocodificar_bairro,
)
from leilao_ia_v2.services.geo_medicao import coords_de_anuncio, geo_bucket_de_coords, haversine_km
from leilao_ia_v2.vivareal.slug import slug_vivareal
from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

logger = logging.getLogger(__name__)

# Valor legado usado quando ``raio_km=None`` em APIs públicas (substituído em runtime por parâmetros da sessão).
RAIO_KM_PADRAO = 10.0
MAX_ANUNCIOS_GEOCODE = 50

_TIPOS_CASA_SOBRADO: frozenset[str] = frozenset({"casa", "sobrado", "casa_condominio"})
_TIPOS_TERRENO_BUSCA: tuple[str, ...] = ("terreno", "lote")
_TIPOS_RESIDENCIAIS_POOL: tuple[str, ...] = ("casa", "sobrado", "casa_condominio")
_UF_SLUGS: tuple[str, ...] = (
    "ac",
    "al",
    "am",
    "ap",
    "ba",
    "ce",
    "df",
    "es",
    "go",
    "ma",
    "mg",
    "ms",
    "mt",
    "pa",
    "pb",
    "pe",
    "pi",
    "pr",
    "rj",
    "rn",
    "ro",
    "rr",
    "rs",
    "sc",
    "se",
    "sp",
    "to",
)

# Política de composição dos caches gravados (UI lê ``volume_amostras_baixo`` em ``metadados_json``).
CACHE_MONTE_MIN_EXIGIDO = 1
CACHE_VOLUME_BAIXO_LIMITE = 5
CACHE_APOIO_ESCALA_MIN_AMOSTRAS = 3
CACHE_APOIO_ESCALA_MENOR_MIN = 0.60
CACHE_APOIO_ESCALA_MENOR_MAX = 0.95
CACHE_APOIO_ESCALA_MAIOR_MIN = 1.05
CACHE_APOIO_ESCALA_MAIOR_MAX = 1.60
RAIO_EXPANSAO_GEO_FIRST_MEDIO_KM = 10.0
RAIO_EXPANSAO_GEO_FIRST_MAX_KM = 15.0
_CIDADES_PILOTO_GEO_FIRST: frozenset[str] = frozenset({"taubate", "aparecida"})
# Distribuição de metragem para liquidez (aviso de outlier da região).
LIQ_METRAGEM_MIN_AMOSTRAS = 20
LIQ_METRAGEM_MAX_FIRECRAWL_CREDITOS = 4
# Padrões quando não há contexto Streamlit (testes, scripts). A UI usa ``get_busca_mercado_parametros()``.
CACHE_AMOSTRAS_PRINCIPAL_MAX = 10
CACHE_AMOSTRAS_LOTE_REFERENCIA = 10


def _float_positivo(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x > 0 else None


def _env_flag_bool(nome: str, default: bool) -> bool:
    raw = str(os.getenv(nome, "") or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "y", "on", "sim"}:
        return True
    if raw in {"0", "false", "no", "n", "off", "nao", "não"}:
        return False
    return bool(default)


def _cidade_rollout_slug(cidade: str) -> str:
    return _slug_fold(cidade).replace("-", "")


def _flag_por_cidade(nome: str, cidade: str, *, default: bool) -> bool:
    raw = str(os.getenv(nome, "") or "").strip().lower()
    if raw in {"pilot", "piloto"}:
        return _cidade_rollout_slug(cidade) in _CIDADES_PILOTO_GEO_FIRST
    return _env_flag_bool(nome, default)


def _cache_geo_first_enabled(cidade: str) -> bool:
    return _flag_por_cidade("CACHE_GEO_FIRST_ENABLED", cidade, default=True)


def _cache_radius_expansion_enabled(cidade: str) -> bool:
    return _cache_geo_first_enabled(cidade) and _flag_por_cidade(
        "CACHE_RADIUS_EXPANSION_ENABLED", cidade, default=True
    )


def _bairro_canonico_enabled(cidade: str) -> bool:
    return _flag_por_cidade("BAIRRO_CANONICO_ENABLED", cidade, default=False)


def _host_url(u: str) -> str:
    s = str(u or "").strip().lower()
    if not s:
        return ""
    s = s.replace("https://", "").replace("http://", "")
    return s.split("/", 1)[0].strip()


def _slug_fold(s: Any) -> str:
    raw = str(s or "").strip().lower()
    if not raw:
        return ""
    txt = slug_vivareal(raw)
    return "" if txt in ("", "-") else txt


def _cidade_inferida_da_url(url: str) -> str:
    u = str(url or "").strip().lower()
    if not u:
        return ""
    uf_pat = "|".join(_UF_SLUGS)
    m_uf = re.search(
        rf"-(?:{uf_pat})-(?P<cidade>[a-z0-9]+(?:-[a-z0-9]+){{0,3}})-(?=[a-z0-9-]*?(?:\d+m2|rs\d|id-|venda|aluguel))",
        u,
        flags=re.IGNORECASE,
    )
    if m_uf:
        return _slug_fold(m_uf.group("cidade"))
    candidatos = list(
        re.finditer(
            r"-(?P<cidade>[a-z0-9]+(?:-[a-z0-9]+){0,4})-(?:com-[a-z0-9-]+-)?\d+m2-(?:venda|aluguel)-",
            u,
            flags=re.IGNORECASE,
        )
    )
    if candidatos:
        return _slug_fold(candidatos[-1].group("cidade"))
    return ""


def _url_indica_cidade_diferente_do_alvo(url: str, cidade_alvo: str) -> bool:
    alvo = _slug_fold(cidade_alvo)
    if not alvo:
        return False
    inferida = _cidade_inferida_da_url(url)
    if not inferida:
        return False
    if inferida == alvo:
        return False
    if inferida.endswith(f"-{alvo}") or alvo.endswith(f"-{inferida}"):
        return False
    return True


def _filtrar_candidatos_por_coerencia_url_cidade(
    candidatos: list[dict[str, Any]],
    cidade_alvo: str,
    *,
    linhas: list[str] | None = None,
    etapa: str = "",
) -> list[dict[str, Any]]:
    if not candidatos:
        return []
    out = [
        c
        for c in candidatos
        if not _url_indica_cidade_diferente_do_alvo(str(c.get("url_anuncio") or ""), cidade_alvo)
    ]
    if linhas is not None and len(out) != len(candidatos):
        lbl = etapa or "coerencia_url_cidade"
        linhas.append(
            f"{lbl}: descartados_por_cidade_url={len(candidatos) - len(out)} (alvo={cidade_alvo or '-'})"
        )
    return out


def _url_canonica(u: str) -> str:
    s = str(u or "").strip()
    if not s:
        return ""
    s = re.sub(r"#.*$", "", s)
    s = re.sub(r"\?.*$", "", s)
    return s.rstrip("/").lower()


def _parse_brl_num(txt: str) -> float | None:
    s = str(txt or "").strip()
    if not s:
        return None
    s = s.replace("R$", "").replace(" ", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(".", "")
    try:
        v = float(s)
    except Exception:
        return None
    return v if v > 0 else None


def _extrair_preco_titulo_aprox(titulo: str) -> float | None:
    t = str(titulo or "")
    if not t:
        return None
    m = re.search(r"R\$\s*([\d\.\,]{4,})", t, flags=re.IGNORECASE)
    if not m:
        return None
    return _parse_brl_num(m.group(1))


def _extrair_preco_url_aprox(url: str) -> float | None:
    u = str(url or "")
    if not u:
        return None
    m = re.search(r"[-_/]RS(\d{5,10})(?:[-_/]|$)", u, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        v = float(m.group(1))
    except Exception:
        return None
    return v if v > 0 else None


def _preco_anuncio_inconsistente(a: dict[str, Any]) -> bool:
    """
    Detecta incoerência forte de preço entre ``valor_venda`` persistido e sinais do card (título/URL).
    """
    valor = _float_positivo(a.get("valor_venda"))
    if valor is None:
        return True
    refs: list[float] = []
    t = _extrair_preco_titulo_aprox(str(a.get("titulo") or ""))
    if t is not None:
        refs.append(t)
    u = _extrair_preco_url_aprox(str(a.get("url_anuncio") or ""))
    if u is not None:
        refs.append(u)
    if not refs:
        return False
    ref = float(statistics.median(refs))
    if ref <= 0:
        return False
    ratio = valor / ref
    diff = abs(valor - ref)
    return diff >= 200_000 and (ratio < 0.70 or ratio > 1.30)


def _deduplicar_amostras_similares(amostras: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    """
    Remove duplicados exatos por URL e duplicados muito parecidos (mesma origem + buckets de preço/área/geo).
    """
    out: list[dict[str, Any]] = []
    seen_url: set[str] = set()
    seen_fuzzy: set[tuple[Any, ...]] = set()
    n_dup_url = 0
    n_dup_fuzzy = 0
    for a in amostras:
        url0 = _url_canonica(str(a.get("url_anuncio") or ""))
        if url0 and url0 in seen_url:
            n_dup_url += 1
            continue
        if url0:
            seen_url.add(url0)
        area = _float_positivo(a.get("area_construida_m2")) or 0.0
        valor = _float_positivo(a.get("valor_venda")) or 0.0
        la, lo = coords_de_anuncio(a)
        geo_b = (round(float(la), 3), round(float(lo), 3)) if (la is not None and lo is not None) else ("", "")
        fuzzy = (
            _host_url(str(a.get("url_anuncio") or "")),
            str(normalizar_tipo_imovel(a.get("tipo_imovel")) or "").lower(),
            _bairro_normalizado_para_match(a.get("bairro")),
            int(area / 15.0) if area > 0 else -1,
            int(valor / 100_000.0) if valor > 0 else -1,
            geo_b,
        )
        if fuzzy in seen_fuzzy:
            n_dup_fuzzy += 1
            continue
        seen_fuzzy.add(fuzzy)
        out.append(a)
    return out, n_dup_url, n_dup_fuzzy


def _indices_outliers_mad_log(valores: list[float], *, z_limite: float = 3.5) -> set[int]:
    """
    Detecta outliers usando z-score robusto (MAD) no domínio logarítmico.
    """
    if len(valores) < 6:
        return set()
    logs = [math.log(float(v)) for v in valores if v > 0]
    if len(logs) != len(valores):
        return set()
    med = float(statistics.median(logs))
    desvios = [abs(x - med) for x in logs]
    mad = float(statistics.median(desvios))
    if mad <= 0:
        return set()
    out: set[int] = set()
    for i, x in enumerate(logs):
        z = 0.6745 * (x - med) / mad
        if abs(z) > float(z_limite):
            out.add(i)
    return out


def _remover_extremos_estatisticos(
    amostras: list[dict[str, Any]],
    linhas: list[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """
    Remove extremos fortes de preço absoluto e preço/m² para reduzir distorção do cache.
    Só aplica quando há volume suficiente e mantém no mínimo 4 amostras para não matar cobertura.
    """
    if len(amostras) < 6:
        return amostras, 0
    idx_validos: list[int] = []
    valores: list[float] = []
    pm2s: list[float] = []
    for i, a in enumerate(amostras):
        v = _float_positivo(a.get("valor_venda"))
        ar = _float_positivo(a.get("area_construida_m2"))
        if v is None or ar is None:
            continue
        idx_validos.append(i)
        valores.append(float(v))
        pm2s.append(float(v) / float(ar))
    if len(idx_validos) < 6:
        return amostras, 0

    out_val = _indices_outliers_mad_log(valores)
    out_pm2 = _indices_outliers_mad_log(pm2s)

    med_val = float(statistics.median(valores))
    med_pm2 = float(statistics.median(pm2s))
    out_ratio_val: set[int] = set()
    out_ratio_pm2: set[int] = set()
    if med_val > 0:
        for j, v in enumerate(valores):
            ratio = v / med_val
            if abs(v - med_val) >= 200_000 and (ratio < 0.45 or ratio > 1.90):
                out_ratio_val.add(j)
    if med_pm2 > 0:
        for j, vpm2 in enumerate(pm2s):
            ratio_pm2 = vpm2 / med_pm2
            if ratio_pm2 < 0.50 or ratio_pm2 > 2.00:
                out_ratio_pm2.add(j)

    out_local = out_val | out_pm2 | out_ratio_val | out_ratio_pm2
    if not out_local:
        return amostras, 0

    idx_drop = {idx_validos[j] for j in out_local}
    n_drop = len(idx_drop)
    n_keep = len(amostras) - n_drop
    if n_keep < 4:
        if linhas is not None:
            linhas.append(
                "  saneamento_extremos: detectados outliers, mas removidos=0 para preservar volume mínimo (4)."
            )
        return amostras, 0

    filtradas = [a for i, a in enumerate(amostras) if i not in idx_drop]
    if linhas is not None:
        linhas.append(
            "  saneamento_extremos: "
            f"outliers_valor_mad={len(out_val)} | outliers_preco_m2_mad={len(out_pm2)} | "
            f"outliers_valor_ratio={len(out_ratio_val)} | outliers_preco_m2_ratio={len(out_ratio_pm2)} | "
            f"removidos={n_drop} | "
            f"restantes={len(filtradas)}"
        )
    return filtradas, n_drop


def _sanear_amostras_para_cache(
    amostras: list[dict[str, Any]],
    linhas: list[str] | None = None,
    *,
    deduplicar_similares: bool = True,
) -> list[dict[str, Any]]:
    n0 = len(amostras)
    if n0 == 0:
        return amostras
    sem_incons = [a for a in amostras if not _preco_anuncio_inconsistente(a)]
    n_inc = n0 - len(sem_incons)
    if deduplicar_similares:
        dedup, n_dup_url, n_dup_fuzzy = _deduplicar_amostras_similares(sem_incons)
    else:
        dedup, n_dup_url, n_dup_fuzzy = sem_incons, 0, 0
    sem_extremos, n_out = _remover_extremos_estatisticos(dedup, linhas)
    if linhas is not None and (n_inc > 0 or n_dup_url > 0 or n_dup_fuzzy > 0 or n_out > 0):
        linhas.append(
            "  saneamento_amostras: "
            f"incons_preco={n_inc} | dup_url={n_dup_url} | dup_similar={n_dup_fuzzy} | "
            f"outliers={n_out} | restantes={len(sem_extremos)}"
        )
    return sem_extremos


def _filtrar_amostras_so_terreno(amostras: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for a in amostras:
        ti = str(a.get("tipo_imovel") or "").strip().lower()
        if ti in ("terreno", "lote"):
            out.append(a)
    return out


def _parse_extra(leilao: dict[str, Any]) -> dict[str, Any]:
    raw = leilao.get("leilao_extra_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


def _percentil_linear(xs: list[float], p: float) -> float:
    """Percentil com interpolação linear (0..1)."""
    if not xs:
        return 0.0
    if len(xs) == 1:
        return float(xs[0])
    ys = sorted(float(v) for v in xs)
    q = max(0.0, min(1.0, float(p)))
    idx = q * (len(ys) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ys[lo]
    frac = idx - lo
    return ys[lo] + (ys[hi] - ys[lo]) * frac


def _analise_liquidez_metragem(
    area_imovel_m2: float,
    areas_amostra: list[float],
    *,
    min_amostras: int = LIQ_METRAGEM_MIN_AMOSTRAS,
) -> dict[str, Any]:
    vals = [float(v) for v in areas_amostra if float(v) > 0]
    n = len(vals)
    base: dict[str, Any] = {
        "n_amostras_area": n,
        "min_amostras_recomendado": int(min_amostras),
        "area_imovel_m2": float(area_imovel_m2 or 0.0),
        "fit_metragem_score": 0,
        "alerta_outlier_metragem": False,
        "mensagem_alerta": "",
        "faixa_tipica_m2": None,
        "status": "sem_dados",
    }
    if area_imovel_m2 <= 0:
        base["status"] = "sem_area_imovel"
        return base
    if n < 5:
        base["status"] = "amostra_insuficiente"
        base["mensagem_alerta"] = "Amostra insuficiente para avaliar outlier de metragem."
        return base
    p10 = _percentil_linear(vals, 0.10)
    p25 = _percentil_linear(vals, 0.25)
    p50 = _percentil_linear(vals, 0.50)
    p75 = _percentil_linear(vals, 0.75)
    p90 = _percentil_linear(vals, 0.90)
    iqr = max(0.0, p75 - p25)
    lim_inf = max(0.0, p25 - 1.5 * iqr)
    lim_sup = p75 + 1.5 * iqr
    area = float(area_imovel_m2)
    # Percentil aproximado (rank médio).
    menores = sum(1 for v in vals if v < area)
    iguais = sum(1 for v in vals if v == area)
    pct = ((menores + 0.5 * iguais) / n) * 100.0 if n > 0 else 0.0
    fit = max(0, min(100, int(round(100.0 - abs(pct - 50.0) * 2.0))))
    out_moderado = area < p10 or area > p90
    out_forte = area < lim_inf or area > lim_sup
    alerta = bool(out_moderado or out_forte)
    msg = ""
    if alerta:
        cls = "forte" if out_forte else "moderado"
        msg = (
            f"Metragem {cls} fora do padrão local (imóvel={area:.1f}m²; "
            f"faixa típica P10-P90={p10:.1f}-{p90:.1f}m²). Liquidez pode ser menor."
        )
    base.update(
        {
            "status": "ok",
            "p10_area_m2": round(p10, 2),
            "p25_area_m2": round(p25, 2),
            "p50_area_m2": round(p50, 2),
            "p75_area_m2": round(p75, 2),
            "p90_area_m2": round(p90, 2),
            "limite_iqr_inf_m2": round(lim_inf, 2),
            "limite_iqr_sup_m2": round(lim_sup, 2),
            "percentil_area_imovel": round(pct, 2),
            "fit_metragem_score": int(fit),
            "outlier_moderado_p10_p90": bool(out_moderado),
            "outlier_forte_iqr": bool(out_forte),
            "alerta_outlier_metragem": bool(alerta),
            "mensagem_alerta": msg,
            "faixa_tipica_m2": f"{round(p10, 1)}-{round(p90, 1)}",
            "amostra_fraca": bool(n < int(min_amostras)),
        }
    )
    return base


def _coletar_metricas_para_analise_liquidez(
    client: Client,
    *,
    cache_ids: list[str],
    lat0: float,
    lon0: float,
    raio_km: float,
    tipo_l: str,
    cidade: str,
    estado_sigla: str,
    bairro: str,
    leilao_id: str,
    estado_raw: str,
    ignorar_cache_firecrawl: bool,
    max_chamadas_api_firecrawl: int | None = None,
) -> tuple[list[dict[str, float]], int, str]:
    """
    Coleta métricas para análise de liquidez (área/pm2/distância/tipo).
    Prioriza anúncios dos caches já gravados; se amostra fraca, faz até 1 rodada Firecrawl sem faixa de área.
    """
    usados_fc = 0
    origem = "caches_vinculados"
    ids_limpos = [str(x).strip() for x in (cache_ids or []) if str(x).strip()]
    metricas: list[dict[str, float]] = []
    tipos = list(_TIPOS_RESIDENCIAIS_POOL) if tipo_l in _TIPOS_CASA_SOBRADO else [tipo_l]
    tset = {str(t).strip().lower() for t in tipos if str(t).strip()}

    if ids_limpos:
        rows_c = cache_media_bairro_repo.buscar_por_ids(client, ids_limpos)
        ads_ids: list[str] = []
        seen_ads: set[str] = set()
        for c in rows_c:
            md = _metadados_cache_como_dict(c.get("metadados_json"))
            if str(md.get("modo_cache") or "").strip().lower() == "terrenos":
                continue
            ids_c = _parse_csv_anuncio_ids(c.get("anuncios_ids"))
            for aid in ids_c:
                if aid not in seen_ads:
                    seen_ads.add(aid)
                    ads_ids.append(aid)
        if ads_ids:
            ads = anuncios_mercado_repo.buscar_por_ids(client, ads_ids)
            if tset:
                ads = [a for a in ads if str(a.get("tipo_imovel") or "").strip().lower() in tset]
            ads = _filtrar_amostras(
                ads,
                lat0,
                lon0,
                area_ref=0.0,
                raio_km=raio_km,
                aplicar_faixa_area_edital=False,
            )
            ads = _apos_filtro_geo_excluir_listagem_sinc_lance(ads, None, None)
            ads = [a for a in ads if not _preco_anuncio_inconsistente(a)]
            ads, _, _ = _deduplicar_amostras_similares(ads)
            for a in ads:
                ar = _float_positivo(a.get("area_construida_m2"))
                vv = _float_positivo(a.get("valor_venda"))
                if ar is None or vv is None:
                    continue
                la, lo = coords_de_anuncio(a)
                dist = float(haversine_km(lat0, lon0, float(la), float(lo))) if (la is not None and lo is not None) else 1e9
                metricas.append(
                    {
                        "area_m2": float(ar),
                        "preco_m2": float(vv) / float(ar),
                        "dist_km": float(dist),
                        "tipo_eq": 1.0 if str(a.get("tipo_imovel") or "").strip().lower() == str(tipo_l).strip().lower() else 0.0,
                    }
                )

    if len(metricas) < LIQ_METRAGEM_MIN_AMOSTRAS:
        orc = max(0, int(max_chamadas_api_firecrawl or LIQ_METRAGEM_MAX_FIRECRAWL_CREDITOS))
        teto = min(orc, LIQ_METRAGEM_MAX_FIRECRAWL_CREDITOS)
        pode_fc = teto > 0
        if pode_fc:
            n_fc, n_api = _uma_coleta_firecrawl_search(
                client,
                cidade=cidade,
                estado_raw=estado_raw,
                bairro=bairro,
                tipo_imovel=tipo_l,
                area_ref=0.0,
                leilao_id=leilao_id,
                ignorar_cache_firecrawl=ignorar_cache_firecrawl,
                max_chamadas_api=teto,
                frase_busca_override=None,
            )
            usados_fc += int(n_api or 0)
            if int(n_fc or 0) > 0:
                origem = "caches_vinculados+firecrawl_extra"
                cand = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
                    client,
                    cidade=cidade,
                    estado_sigla=estado_sigla,
                    tipos_imovel=tipos,
                )
                cand = _filtrar_amostras(
                    cand,
                    lat0,
                    lon0,
                    area_ref=0.0,
                    raio_km=raio_km,
                    aplicar_faixa_area_edital=False,
                )
                cand = [a for a in cand if not _preco_anuncio_inconsistente(a)]
                cand, _, _ = _deduplicar_amostras_similares(cand)
                metricas = []
                for a in cand:
                    ar = _float_positivo(a.get("area_construida_m2"))
                    vv = _float_positivo(a.get("valor_venda"))
                    if ar is None or vv is None:
                        continue
                    la, lo = coords_de_anuncio(a)
                    dist = float(haversine_km(lat0, lon0, float(la), float(lo))) if (la is not None and lo is not None) else 1e9
                    metricas.append(
                        {
                            "area_m2": float(ar),
                            "preco_m2": float(vv) / float(ar),
                            "dist_km": float(dist),
                            "tipo_eq": 1.0 if str(a.get("tipo_imovel") or "").strip().lower() == str(tipo_l).strip().lower() else 0.0,
                        }
                    )

    return metricas, usados_fc, origem


def _pm2_imovel_referencia(leilao: dict[str, Any], area_imovel_m2: float) -> float | None:
    if area_imovel_m2 <= 0:
        return None
    for k in ("valor_avaliacao", "valor_mercado_estimado", "valor_arrematacao", "valor_lance_1_praca", "valor_lance_2_praca"):
        v = _float_positivo(leilao.get(k))
        if v is not None and v > 0:
            return float(v) / float(area_imovel_m2)
    return None


def _analise_fit_multidimensional(
    area_imovel_m2: float,
    pm2_imovel: float | None,
    metricas_amostra: list[dict[str, float]],
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "fit_multidimensional_score": 0,
        "alerta_outlier_multidimensional": False,
        "mensagem_alerta_multidimensional": "",
        "status_multidimensional": "sem_dados",
    }
    if area_imovel_m2 <= 0:
        out["status_multidimensional"] = "sem_area_imovel"
        return out
    areas = [float(m.get("area_m2") or 0.0) for m in metricas_amostra if float(m.get("area_m2") or 0.0) > 0]
    pm2s = [float(m.get("preco_m2") or 0.0) for m in metricas_amostra if float(m.get("preco_m2") or 0.0) > 0]
    dists = [float(m.get("dist_km") or 0.0) for m in metricas_amostra if float(m.get("dist_km") or 0.0) < 1e8]
    tipos = [float(m.get("tipo_eq") or 0.0) for m in metricas_amostra]
    if len(areas) < 6 or len(pm2s) < 6:
        out["status_multidimensional"] = "amostra_insuficiente"
        return out

    # Fit por área (percentil centralidade).
    menores_a = sum(1 for v in areas if v < float(area_imovel_m2))
    iguais_a = sum(1 for v in areas if v == float(area_imovel_m2))
    pct_a = ((menores_a + 0.5 * iguais_a) / len(areas)) * 100.0
    fit_a = max(0.0, min(100.0, 100.0 - abs(pct_a - 50.0) * 2.0))
    p25_a = _percentil_linear(areas, 0.25)
    p75_a = _percentil_linear(areas, 0.75)
    iqr_a = max(0.0, p75_a - p25_a)
    outlier_area_forte = float(area_imovel_m2) < max(0.0, p25_a - 1.5 * iqr_a) or float(area_imovel_m2) > (p75_a + 1.5 * iqr_a)

    fit_pm2 = 50.0
    outlier_pm2_forte = False
    if pm2_imovel is not None and pm2_imovel > 0:
        menores_p = sum(1 for v in pm2s if v < float(pm2_imovel))
        iguais_p = sum(1 for v in pm2s if v == float(pm2_imovel))
        pct_p = ((menores_p + 0.5 * iguais_p) / len(pm2s)) * 100.0
        fit_pm2 = max(0.0, min(100.0, 100.0 - abs(pct_p - 50.0) * 2.0))
        p25_p = _percentil_linear(pm2s, 0.25)
        p75_p = _percentil_linear(pm2s, 0.75)
        iqr_p = max(0.0, p75_p - p25_p)
        outlier_pm2_forte = float(pm2_imovel) < max(0.0, p25_p - 1.5 * iqr_p) or float(pm2_imovel) > (p75_p + 1.5 * iqr_p)

    # Quanto menor a mediana de distância das amostras, melhor representatividade local.
    med_dist = float(statistics.median(dists)) if dists else 6.0
    fit_dist = max(0.0, min(100.0, 100.0 - (med_dist / 6.0) * 100.0))
    fit_tipo = 100.0 * (sum(tipos) / len(tipos)) if tipos else 50.0

    fit_multi = (
        0.45 * float(fit_a)
        + 0.35 * float(fit_pm2)
        + 0.15 * float(fit_dist)
        + 0.05 * float(fit_tipo)
    )
    fit_multi = max(0.0, min(100.0, fit_multi))
    alerta = bool(fit_multi < 45.0 or outlier_area_forte or outlier_pm2_forte)
    msg = ""
    if alerta:
        msg = (
            "Imóvel com fit multidimensional baixo no micro-mercado "
            f"(score {fit_multi:.0f}/100). Liquidez potencialmente menor."
        )
    out.update(
        {
            "status_multidimensional": "ok",
            "fit_multidimensional_score": int(round(fit_multi)),
            "fit_area_score": int(round(fit_a)),
            "fit_preco_m2_score": int(round(fit_pm2)),
            "fit_distancia_local_score": int(round(fit_dist)),
            "fit_tipo_score": int(round(fit_tipo)),
            "outlier_area_forte": bool(outlier_area_forte),
            "outlier_preco_m2_forte": bool(outlier_pm2_forte),
            "alerta_outlier_multidimensional": bool(alerta),
            "mensagem_alerta_multidimensional": msg,
        }
    )
    return out


def _persistir_analise_liquidez_metragem_leilao(
    client: Client,
    *,
    leilao: dict[str, Any],
    cache_ids: list[str],
    lat0: float,
    lon0: float,
    raio_km: float,
    tipo_l: str,
    estado_sigla: str,
    ignorar_cache_firecrawl: bool,
    max_chamadas_api_firecrawl: int | None = None,
) -> tuple[int, dict[str, Any]]:
    cidade = str(leilao.get("cidade") or "").strip()
    bairro = str(leilao.get("bairro") or "").strip()
    estado_raw = str(leilao.get("estado") or "").strip()
    lid = str(leilao.get("id") or "").strip()
    area_imovel = _area_referencia_m2(leilao)
    metricas, n_api, origem = _coletar_metricas_para_analise_liquidez(
        client,
        cache_ids=cache_ids,
        lat0=float(lat0),
        lon0=float(lon0),
        raio_km=float(raio_km),
        tipo_l=tipo_l,
        cidade=cidade,
        estado_sigla=estado_sigla,
        bairro=bairro,
        leilao_id=lid,
        estado_raw=estado_raw,
        ignorar_cache_firecrawl=ignorar_cache_firecrawl,
        max_chamadas_api_firecrawl=max_chamadas_api_firecrawl,
    )
    analise = _analise_liquidez_metragem(area_imovel, [float(m.get("area_m2") or 0.0) for m in metricas])
    pm2_ref = _pm2_imovel_referencia(leilao, area_imovel)
    analise_multi = _analise_fit_multidimensional(area_imovel, pm2_ref, metricas)
    for k, v in analise_multi.items():
        analise[k] = v
    analise["pm2_imovel_referencia"] = round(float(pm2_ref), 2) if pm2_ref is not None else None
    analise["origem_amostras"] = origem
    analise["raio_km"] = float(raio_km)
    extra = _parse_extra(leilao)
    extra["analise_liquidez_metragem"] = analise
    leilao_imoveis_repo.atualizar_leilao_imovel(lid, {"leilao_extra_json": extra}, client)
    return n_api, analise


def _area_referencia_m2(leilao: dict[str, Any]) -> float:
    for k in ("area_util", "area_total"):
        v = leilao.get(k)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _coords_leilao(leilao: dict[str, Any]) -> Optional[tuple[float, float]]:
    try:
        la = leilao.get("latitude")
        lo = leilao.get("longitude")
        if la is not None and lo is not None:
            return float(la), float(lo)
    except (TypeError, ValueError):
        pass
    logr = str(leilao.get("endereco") or "").strip()
    bai = str(leilao.get("bairro") or "").strip()
    cid = str(leilao.get("cidade") or "").strip()
    uf = str(leilao.get("estado") or "").strip()
    if not logr and not cid:
        return None
    try:
        c = geocodificar_endereco(logradouro=logr, bairro=bai, cidade=cid, estado=uf)
        return c if c else None
    except Exception:
        logger.exception("Geocodificação do leilão para cache")
        return None


def _normalizar_lista_alias_bairro(v: Any) -> list[str]:
    out: list[str] = []
    if isinstance(v, list):
        vals = v
    elif isinstance(v, str) and v.strip():
        vals = [x.strip() for x in v.split(",")]
    else:
        vals = []
    seen: set[str] = set()
    for x in vals:
        sx = str(x or "").strip()
        if not sx:
            continue
        key = _bairro_normalizado_para_match(sx)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(sx)
    return out


def _normalizar_nome_empreendimento(v: Any) -> str:
    s = " ".join(str(v or "").strip().split())
    if not s:
        return ""
    s = re.sub(r"(?i)^(condom[ií]nio|edif[ií]cio|pr[eé]dio)\s+", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip(" -,:;.")
    return s[:160]


def _texto_boilerplate_condominio(s: str) -> bool:
    t = str(s or "").lower()
    if not t:
        return False
    termos = (
        "regras para pagamento",
        "despesas",
        "sob responsabilidade do comprador",
        "a caixa realizará o pagamento",
        "limite de 10%",
        "valor de avaliação",
        "tributos",
    )
    return any(k in t for k in termos)


def _nome_empreendimento_valido(v: Any) -> str:
    s = _normalizar_nome_empreendimento(v)
    if not s or len(s) < 4:
        return ""
    if _texto_boilerplate_condominio(s):
        return ""
    return s


def _nome_empreendimento_leilao(leilao: dict[str, Any]) -> str:
    extra = _parse_extra(leilao)
    for k in (
        "nome_condominio",
        "condominio",
        "nome_predio",
        "predio",
        "nome_edificio",
        "edificio",
        "nome_empreendimento",
        "empreendimento",
    ):
        v = _nome_empreendimento_valido(extra.get(k) or leilao.get(k))
        if v:
            return v
    textos = [
        str(extra.get("observacoes_markdown") or "").strip(),
        str(leilao.get("endereco") or "").strip(),
        str(leilao.get("descricao") or "").strip(),
    ]
    for obs in textos:
        if not obs:
            continue
        for ln in obs.splitlines():
            s = " ".join(str(ln or "").strip().split())
            if len(s) < 8:
                continue
            if re.search(r"(?i)\b(condom[ií]nio|edif[ií]cio|pr[eé]dio)\b", s):
                m = re.search(r"(?i)\b(condom[ií]nio|edif[ií]cio|pr[eé]dio)\b\s*[:\-]?\s*(.+)$", s)
                if m:
                    c = _nome_empreendimento_valido(m.group(2) or s)
                    if c:
                        return c
                c2 = _nome_empreendimento_valido(s)
                if c2:
                    return c2
    return ""


def _leilao_indica_condominio(leilao: dict[str, Any]) -> bool:
    if _nome_empreendimento_leilao(leilao):
        return True
    extra = _parse_extra(leilao)
    blob = " ".join(
        str(x or "")
        for x in (
            leilao.get("endereco"),
            leilao.get("descricao"),
            extra.get("observacoes_markdown"),
            extra.get("edital_resumo"),
            leilao.get("edital_markdown"),
        )
    )
    if _texto_boilerplate_condominio(blob):
        return False
    b = blob.lower()
    # Evita falso positivo de texto jurídico "Condomínio: sob responsabilidade..."
    if re.search(r"(?i)\bcondom[ií]nio\s*:", b):
        return False
    return bool(
        re.search(
            r"(?i)\b(condom[ií]nio\s+(residencial|fechado|[a-z0-9]))\b",
            b,
        )
        or re.search(r"(?i)\bcasa\s+em\s+condom[ií]nio\b", b)
    )


def _texto_norm_match(v: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _slug_fold(v))


def _anuncio_match_empreendimento(anuncio: dict[str, Any], nome_empreendimento_ref: str) -> bool:
    ref = _normalizar_nome_empreendimento(nome_empreendimento_ref)
    if not ref:
        return False
    ref_n = _texto_norm_match(ref)
    if len(ref_n) < 6:
        return False
    md = _metadados_anuncio_como_dict(anuncio)
    blobs = [
        anuncio.get("titulo"),
        anuncio.get("url_anuncio"),
        anuncio.get("logradouro"),
        anuncio.get("bairro"),
        md.get("nome_empreendimento"),
        md.get("condominio"),
        md.get("nome_condominio"),
    ]
    txt = _texto_norm_match(" ".join(str(x or "") for x in blobs))
    if not txt:
        return False
    if ref_n in txt:
        return True
    toks = [
        _texto_norm_match(t)
        for t in re.split(r"\s+", ref)
        if _texto_norm_match(t)
        and _texto_norm_match(t)
        not in {"condominio", "residencial", "predio", "edificio", "torre", "bloco", "vila"}
    ]
    if not toks:
        return False
    hit = sum(1 for t in toks if t and t in txt)
    return hit >= min(2, len(toks))


def _garantir_bairro_canonico_leilao(
    client: Client,
    leilao: dict[str, Any],
    *,
    lat0: float,
    lon0: float,
) -> tuple[str, str]:
    """
    Mantém trilha de bairro informado/canônico em ``leilao_extra_json``.
    Retorna (bairro_informado, bairro_referencia_cache), sem sobrescrever silenciosamente
    o campo textual principal do leilão.
    """
    bairro_informado = str(leilao.get("bairro") or "").strip()
    cidade = str(leilao.get("cidade") or "").strip()
    if not _bairro_canonico_enabled(cidade):
        return bairro_informado, bairro_informado
    extra = _parse_extra(leilao)
    mudou = False
    if str(extra.get("bairro_informado") or "").strip() != bairro_informado:
        extra["bairro_informado"] = bairro_informado
        mudou = True
    aliases = _normalizar_lista_alias_bairro(extra.get("bairro_aliases"))
    nome_rev, fonte_rev = reverse_geocodificar_bairro(lat0, lon0)
    bairro_canonico = str(extra.get("bairro_canonico") or "").strip()
    if nome_rev:
        if nome_rev != bairro_canonico:
            extra["bairro_canonico"] = nome_rev
            bairro_canonico = nome_rev
            mudou = True
        if fonte_rev:
            if str(extra.get("fonte_canonica_bairro") or "").strip() != fonte_rev:
                extra["fonte_canonica_bairro"] = fonte_rev
                mudou = True
    alias_norm = {_bairro_normalizado_para_match(x) for x in aliases}
    for v in (bairro_informado, bairro_canonico):
        sv = str(v or "").strip()
        if not sv:
            continue
        k = _bairro_normalizado_para_match(sv)
        if not k:
            continue
        if k not in alias_norm:
            aliases.append(sv)
            alias_norm.add(k)
            mudou = True
    if aliases != _normalizar_lista_alias_bairro(extra.get("bairro_aliases")):
        extra["bairro_aliases"] = aliases
        mudou = True
    if bairro_informado and bairro_canonico:
        diverge = _bairro_normalizado_para_match(bairro_informado) != _bairro_normalizado_para_match(bairro_canonico)
        if bool(extra.get("bairro_canonico_divergente")) != bool(diverge):
            extra["bairro_canonico_divergente"] = bool(diverge)
            mudou = True
    if mudou:
        try:
            leilao_imoveis_repo.atualizar_leilao_imovel(str(leilao.get("id") or ""), {"leilao_extra_json": extra}, client)
            leilao["leilao_extra_json"] = extra
        except Exception:
            logger.exception("Falha ao persistir bairro canônico do leilão")
    bairro_ref = bairro_canonico or bairro_informado
    return bairro_informado, bairro_ref


def _bairro_referencia_cache_leilao(leilao: dict[str, Any]) -> str:
    extra = _parse_extra(leilao)
    b_can = str(extra.get("bairro_canonico") or "").strip()
    b_inf = str(extra.get("bairro_informado") or leilao.get("bairro") or "").strip()
    return b_can or b_inf


def _tipos_somente_terreno_ou_lote(tipos: list[str]) -> bool:
    ts = {str(t).strip().lower() for t in tipos if str(t).strip()}
    return bool(ts) and ts <= {"terreno", "lote"}


def _filtrar_amostras(
    candidatos: list[dict[str, Any]],
    lat0: float,
    lon0: float,
    area_ref: float,
    *,
    raio_km: float,
    aplicar_faixa_area_edital: bool = True,
    empreendimento_referencia: str = "",
) -> list[dict[str, Any]]:
    bp = get_busca_mercado_parametros()
    scored: list[tuple[dict[str, Any], float, float, float]] = []
    for r in candidatos:
        emp_pen = 0.0 if _anuncio_match_empreendimento(r, empreendimento_referencia) else 1.0
        la, lo = coords_de_anuncio(r)
        if la is None or lo is None:
            if emp_pen > 0.0:
                continue
            d = 0.0
        else:
            d = haversine_km(lat0, lon0, float(la), float(lo))
            if d > float(raio_km):
                continue
        try:
            am = float(r.get("area_construida_m2") or 0)
        except (TypeError, ValueError):
            continue
        if am <= 0:
            continue
        if aplicar_faixa_area_edital and area_ref > 0 and emp_pen > 0.0:
            lo_a, hi_a = bp.area_fator_min * area_ref, bp.area_fator_max * area_ref
            if not (lo_a <= am <= hi_a):
                continue
        delta_a = abs(am - area_ref) if (area_ref > 0 and aplicar_faixa_area_edital and emp_pen > 0.0) else 0.0
        scored.append((r, emp_pen, d, delta_a))
    scored.sort(key=lambda x: (x[1], x[2], x[3]))
    return [t[0] for t in scored]


def _apos_filtro_geo_excluir_listagem_sinc_lance(
    amostras: list[dict[str, Any]],
    leilao: dict[str, Any] | None,
    linhas: list[str] | None,
) -> list[dict[str, Any]]:
    if not leilao:
        return amostras
    n0 = len(amostras)
    out = filtrar_anuncios_mantendo_apenas_mercado_comparavel(amostras, leilao)
    if linhas is not None and n0 > len(out):
        linhas.append(
            f"  listagem_sinc_lance: excluídos {n0 - len(out)} anúncio(s) (preço ~lance 1ª/2ª praça + sinais)."
        )
    return out


def _caps_amostras_cache_mercado() -> tuple[int, int]:
    """Limites de anúncios por cache a partir de ``get_busca_mercado_parametros()`` (sidebar ``bm_cache_*``)."""
    bp = get_busca_mercado_parametros()
    cap_p = max(1, int(getattr(bp, "cache_max_amostras_principal", CACHE_AMOSTRAS_PRINCIPAL_MAX)))
    cap_l = max(1, int(getattr(bp, "cache_max_amostras_lote", CACHE_AMOSTRAS_LOTE_REFERENCIA)))
    return cap_p, cap_l


def _fatias_amostras_cache(
    amostras: list[dict[str, Any]],
    cap_principal: int,
    cap_lote: int,
) -> tuple[list[dict[str, Any]], list[list[dict[str, Any]]]]:
    """Primeira fatia = cache principal (simulação); demais fatias = referência, em lotes de ``cap_lote`` itens."""
    if not amostras:
        return [], []
    c0 = max(1, int(cap_principal))
    step = max(1, int(cap_lote))
    pri = amostras[:c0]
    rest = amostras[c0:]
    sec: list[list[dict[str, Any]]] = []
    for i in range(0, len(rest), step):
        chunk = rest[i : i + step]
        if chunk:
            sec.append(chunk)
    return pri, sec


def _meta_volume_e_papel(
    n: int,
    papel: str,
    *,
    lote_referencia_indice: int | None = None,
    cap_principal: int | None = None,
    cap_lote: int | None = None,
) -> dict[str, Any]:
    c_p, c_l = _caps_amostras_cache_mercado()
    if cap_principal is not None:
        c_p = max(1, int(cap_principal))
    if cap_lote is not None:
        c_l = max(1, int(cap_lote))
    m: dict[str, Any] = {
        "volume_amostras_baixo": n < CACHE_VOLUME_BAIXO_LIMITE,
        "volume_amostras_alvo_conforto": CACHE_VOLUME_BAIXO_LIMITE,
        "cache_papel": papel,
        "cache_amostras_cap_principal": c_p,
        "cache_amostras_cap_lote_referencia": c_l,
    }
    if lote_referencia_indice is not None:
        m["cache_lote_referencia_indice"] = int(lote_referencia_indice)
    return m


def _diagnostico_filtro_amostras(
    candidatos: list[dict[str, Any]],
    lat0: float,
    lon0: float,
    area_ref: float,
    *,
    raio_km: float,
    etiqueta: str,
    aplicar_faixa_area_edital: bool = True,
    bairro_informado: str = "",
    bairro_canonico: str = "",
    empreendimento_referencia: str = "",
) -> str:
    """Contagens alinhadas à ordem de exclusão de ``_filtrar_amostras`` (para log em ``ultima_ingestao_log_text``)."""
    bp = get_busca_mercado_parametros()
    n_total = len(candidatos)
    n_sem_coord = 0
    n_fora_raio = 0
    n_area_parse_err = 0
    n_area_zero = 0
    n_fora_faixa_area = 0
    n_mesmo_empreendimento = 0
    ids_fora_raio: list[str] = []
    ids_fora_faixa: list[str] = []
    ids_area_zero: list[str] = []
    ids_area_parse_err: list[str] = []
    ids_sem_coord: list[str] = []
    dists: list[float] = []
    dists_fora_raio: list[float] = []
    lo_a = hi_a = None
    if area_ref > 0:
        lo_a, hi_a = bp.area_fator_min * area_ref, bp.area_fator_max * area_ref

    for r in candidatos:
        la, lo = coords_de_anuncio(r)
        rid = str(r.get("id") or r.get("url_anuncio") or "-")
        eh_mesmo_emp = _anuncio_match_empreendimento(r, empreendimento_referencia)
        if eh_mesmo_emp:
            n_mesmo_empreendimento += 1
        if la is None or lo is None:
            if not eh_mesmo_emp:
                n_sem_coord += 1
                if len(ids_sem_coord) < 5:
                    ids_sem_coord.append(rid)
                continue
            d = 0.0
        else:
            d = haversine_km(lat0, lon0, float(la), float(lo))
        dists.append(d)
        if d > float(raio_km):
            n_fora_raio += 1
            dists_fora_raio.append(d)
            if len(ids_fora_raio) < 5:
                ids_fora_raio.append(rid)
            continue
        try:
            am = float(r.get("area_construida_m2") or 0)
        except (TypeError, ValueError):
            n_area_parse_err += 1
            if len(ids_area_parse_err) < 5:
                ids_area_parse_err.append(rid)
            continue
        if am <= 0:
            n_area_zero += 1
            if len(ids_area_zero) < 5:
                ids_area_zero.append(rid)
            continue
        if (
            aplicar_faixa_area_edital
            and area_ref > 0
            and lo_a is not None
            and hi_a is not None
            and not eh_mesmo_emp
        ):
            if not (lo_a <= am <= hi_a):
                n_fora_faixa_area += 1
                if len(ids_fora_faixa) < 5:
                    ids_fora_faixa.append(rid)
                continue

    n_pass = len(
        _filtrar_amostras(
            candidatos,
            lat0,
            lon0,
            area_ref,
            raio_km=raio_km,
            aplicar_faixa_area_edital=aplicar_faixa_area_edital,
            empreendimento_referencia=empreendimento_referencia,
        )
    )
    d_min = min(dists) if dists else None
    d_max = max(dists) if dists else None
    dr_min = min(dists_fora_raio) if dists_fora_raio else None
    dr_max = max(dists_fora_raio) if dists_fora_raio else None

    def _pct(x: int) -> float:
        return (100.0 * float(x) / float(n_total)) if n_total > 0 else 0.0

    b_inf = _bairro_normalizado_para_match(bairro_informado)
    b_can = _bairro_normalizado_para_match(bairro_canonico)
    n_mesmo_b_inf = 0
    n_mesmo_b_can = 0
    for r in candidatos:
        b = _bairro_normalizado_para_match(r.get("bairro"))
        if b_inf and b == b_inf:
            n_mesmo_b_inf += 1
        if b_can and b == b_can:
            n_mesmo_b_can += 1

    lines = [
        f"{etiqueta}: total_bd={n_total} | passam_filtro={n_pass}",
        (
            f"  excluídos: sem_coord={n_sem_coord} | distância>{raio_km}km={n_fora_raio} | "
            f"área_parse_erro={n_area_parse_err} | área<=0={n_area_zero} | fora_faixa_m²={n_fora_faixa_area}"
        ),
        (
            f"  taxas: sem_coord={_pct(n_sem_coord):.1f}% | dentro_raio={_pct(max(0, n_total - n_fora_raio - n_sem_coord)):.1f}% | "
            f"mesmo_bairro_informado={_pct(n_mesmo_b_inf):.1f}%"
            + (f" | mesmo_bairro_canonico={_pct(n_mesmo_b_can):.1f}%" if b_can else "")
            + (f" | mesmo_empreendimento={_pct(n_mesmo_empreendimento):.1f}%" if empreendimento_referencia else "")
        ),
    ]
    if not aplicar_faixa_area_edital:
        lines.append("  faixa_m²: desativada (segmento terreno/lote — só raio e área>0)")
    elif area_ref > 0 and lo_a is not None and hi_a is not None:
        lines.append(
            f"  faixa_m²_ref={area_ref:.1f} aceita [{lo_a:.1f}, {hi_a:.1f}] "
            f"(fatores {bp.area_fator_min}..{bp.area_fator_max})"
        )
        if empreendimento_referencia:
            lines.append(
                "  regra_empreendimento: anúncios com match de condomínio/prédio não são excluídos por faixa de m²."
            )
    else:
        lines.append("  faixa_m²: sem filtro por área (ref<=0 ou inválida)")
    if dists:
        lines.append(f"  dist_km (anúncios com coord): min={d_min:.2f} max={d_max:.2f}")
    if dists_fora_raio:
        lines.append(f"  dist_km (só excluídos por raio): min={dr_min:.2f} max={dr_max:.2f}")
    if ids_fora_raio or ids_fora_faixa or ids_area_parse_err or ids_area_zero or ids_sem_coord:
        lines.append(
            "  motivos_por_anuncio(amostra): "
            f"fora_raio={ids_fora_raio or []} | "
            f"fora_faixa_area={ids_fora_faixa or []} | "
            f"area_parse_erro={ids_area_parse_err or []} | "
            f"area_zero={ids_area_zero or []} | "
            f"sem_coord={ids_sem_coord or []}"
        )
    lines.append(f"  ref_geo: lat={lat0:.6f} lon={lon0:.6f} raio_km={raio_km}")
    return "\n".join(lines)


def _contexto_log_cache(
    leilao_id: str,
    cidade: str,
    estado_sigla: str,
    tipo_leilao: str,
    area_ref_m2: float,
    min_amostras: int,
    raio_km: float,
) -> str:
    bp = get_busca_mercado_parametros()
    return (
        f"Contexto: leilao_id={leilao_id} cidade={cidade} uf={estado_sigla} tipo={tipo_leilao}\n"
        f"Parâmetros: min_amostras_cache={min_amostras} raio_km={raio_km} "
        f"area_ref_m2={area_ref_m2:.4g} area_fatores={bp.area_fator_min}..{bp.area_fator_max}"
    )


def _tentar_geocodificar_sem_coordenadas(
    client: Client,
    candidatos: list[dict[str, Any]],
    *,
    cidade: str,
    estado_sigla: str,
    bairro_fb: str,
    bairro_alvo: str = "",
) -> int:
    def _rank_bairro(a: dict[str, Any]) -> tuple[int, int]:
        """Prioriza anúncios sem geo do mesmo bairro-alvo."""
        b_alvo = _bairro_normalizado_para_match(bairro_alvo or bairro_fb)
        b_an = _bairro_normalizado_para_match(a.get("bairro"))
        if b_alvo and b_an and b_alvo == b_an:
            return (0, 0)
        if b_an:
            return (1, 0)
        return (2, 0)

    def _centroide_bairro(cidade_v: str, uf_v: str, bairro_v: str) -> Optional[tuple[float, float]]:
        bq = str(bairro_v or "").strip()
        if not bq:
            return None
        try:
            resp = (
                client.table("anuncios_mercado")
                .select("latitude,longitude")
                .eq("estado", str(uf_v or "").strip()[:2].upper())
                .ilike("cidade", f"%{str(cidade_v or '').strip()}%")
                .ilike("bairro", f"%{bq}%")
                .limit(400)
                .execute()
            )
        except Exception:
            logger.debug("fallback centroide bairro falhou na query", exc_info=True)
            return None
        rows = list(getattr(resp, "data", None) or [])
        lats: list[float] = []
        lons: list[float] = []
        for r in rows:
            try:
                la = float(r.get("latitude"))
                lo = float(r.get("longitude"))
            except (TypeError, ValueError):
                continue
            lats.append(la)
            lons.append(lo)
        if not lats:
            return None
        return (sum(lats) / len(lats), sum(lons) / len(lons))

    sem: list[dict[str, Any]] = []
    for r in candidatos:
        la, lo = coords_de_anuncio(r)
        if la is None or lo is None:
            sem.append(r)
    if not sem:
        return 0
    sem.sort(key=_rank_bairro)
    batch: list[dict[str, Any]] = []
    for r in sem[:MAX_ANUNCIOS_GEOCODE]:
        batch.append(
            {
                "url_anuncio": r.get("url_anuncio"),
                "titulo": r.get("titulo"),
                "bairro": r.get("bairro") or bairro_fb,
                "cidade": r.get("cidade") or cidade,
                "estado": r.get("estado") or estado_sigla,
                "logradouro": r.get("logradouro"),
            }
        )
    geocodificar_anuncios_batch(
        batch,
        cidade=cidade,
        estado=estado_sigla,
        bairro_fallback=bairro_fb,
        permitir_fallback_centro_cidade=False,
    )
    atualizados = 0
    for i, r in enumerate(sem[:MAX_ANUNCIOS_GEOCODE]):
        if i >= len(batch):
            break
        b = batch[i]
        lat, lon = b.get("latitude"), b.get("longitude")
        aid = r.get("id")
        if aid and lat is not None and lon is not None:
            try:
                anuncios_mercado_repo.atualizar_geolocalizacao(
                    client, str(aid), float(lat), float(lon)
                )
                atualizados += 1
            except Exception:
                logger.debug("Update geo anúncio %s falhou", aid, exc_info=True)
        elif aid:
            # Último recurso: centroide do bairro (menos preciso que rua, mas melhor que sem coordenadas).
            c_b = _centroide_bairro(
                str(r.get("cidade") or cidade),
                str(r.get("estado") or estado_sigla),
                str(r.get("bairro") or bairro_fb),
            )
            if c_b is not None:
                try:
                    anuncios_mercado_repo.atualizar_geolocalizacao(
                        client,
                        str(aid),
                        float(c_b[0]),
                        float(c_b[1]),
                    )
                    atualizados += 1
                except Exception:
                    logger.debug("Update geo anúncio por centroide bairro %s falhou", aid, exc_info=True)
    return atualizados


def _uma_coleta_firecrawl_search(
    client: Client,
    *,
    cidade: str,
    estado_raw: str,
    bairro: str,
    tipo_imovel: str,
    area_ref: float,
    leilao_id: str,  # noqa: ARG001 - mantido por compat; v2 não usa (anúncios não são por leilão)
    ignorar_cache_firecrawl: bool,  # noqa: ARG001 - cache de scrape sempre reusado (gratuito)
    max_chamadas_api: int | None = None,
    frase_busca_override: str | None = None,  # noqa: ARG001 - v2 usa frase determinística
) -> tuple[int, int]:
    """Devolve ``(n_anuncios_gravados, n_chamadas_api_estimadas)``.

    Implementação v2: delega ao :mod:`leilao_ia_v2.comparaveis.integracao`.
    Os parâmetros ``leilao_id``, ``ignorar_cache_firecrawl`` e
    ``frase_busca_override`` são mantidos para compatibilidade da assinatura
    com chamadores antigos, mas ignorados pelo pipeline v2 (a frase é
    determinística por design e o cache em disco é sempre reusado).
    """
    from leilao_ia_v2.comparaveis.integracao import executar_comparaveis_para_cache

    return executar_comparaveis_para_cache(
        client,
        cidade=cidade,
        estado_raw=estado_raw,
        bairro=bairro,
        tipo_imovel=tipo_imovel,
        area_ref=float(area_ref or 0.0),
        max_chamadas_api=max_chamadas_api,
    )


def _montar_payload_cache(
    leilao: dict[str, Any],
    amostras: list[dict[str, Any]],
    *,
    lat0: float,
    lon0: float,
    geo_bucket: str,
    tipo_segmento: str,
    modo: Literal["principal", "terrenos"],
    raio_km: float,
    uso_simulacao: bool = True,
    apenas_referencia: bool = False,
    tipo_casa_segmento_meta: Optional[str] = None,
    tipo_imovel_cache: Optional[str] = None,
    tipo_casa_coluna: Optional[str] = None,
    nome_suffix: str | None = None,
    metadados_extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cidade = str(leilao.get("cidade") or "").strip()
    bairro = str(leilao.get("bairro") or "").strip()
    estado_sigla = estado_livre_para_sigla_uf(str(leilao.get("estado") or "")) or str(leilao.get("estado") or "")[:2]
    tipo_l = str(normalizar_tipo_imovel(leilao.get("tipo_imovel")) or "desconhecido")
    if tipo_l == "casa" and _leilao_indica_condominio(leilao):
        tipo_l = "casa_condominio"
    tipo_row = str(tipo_imovel_cache or tipo_l) if modo != "terrenos" else "terreno"
    cons = str(normalizar_conservacao(leilao.get("conservacao")) or "desconhecido")
    tc_raw = normalizar_tipo_casa(leilao.get("tipo_casa"), tipo_l)
    tc = str(tipo_casa_coluna) if tipo_casa_coluna is not None else (str(tc_raw) if tc_raw is not None else "-")
    andar = leilao.get("andar")
    faixa_andar = str(andar) if andar is not None else "-"
    logr_ch = slug_vivareal(str(leilao.get("endereco") or ""))[:80] or "-"

    vals: list[float] = []
    pm2s: list[float] = []
    descartadas_metricas = 0
    for a in amostras:
        v = _float_positivo(a.get("valor_venda"))
        ar = _float_positivo(a.get("area_construida_m2"))
        if v is None or ar is None:
            descartadas_metricas += 1
            continue
        vals.append(v)
        pm2s.append(v / ar)
    if descartadas_metricas:
        logger.warning(
            "Cache métricas: %s amostra(s) inválida(s) ignoradas (valor/área <=0 ou não numéricos)",
            descartadas_metricas,
        )
    preco_m2_media = round(sum(pm2s) / len(pm2s), 4) if pm2s else 0.0
    valor_media = round(sum(vals) / len(vals), 2) if vals else 0.0
    preco_m2_mediana = round(float(statistics.median(pm2s)), 4) if pm2s else 0.0
    valor_mediana = round(float(statistics.median(vals)), 2) if vals else 0.0

    cidade_ref = str(leilao.get("cidade") or "").strip()
    bairro_ref_cache = _bairro_referencia_cache_leilao(leilao)
    dists: list[float] = []
    q_geo_pts: list[float] = []
    n_url_ok = 0
    for a in amostras:
        la, lo = coords_de_anuncio(a)
        if la is not None and lo is not None:
            d = float(haversine_km(float(lat0), float(lon0), float(la), float(lo)))
            dists.append(d)
            q = _penalidade_qualidade_geo_anuncio(a)
            # Pontuação por nível: rooftop=100, rua=80, bairro=55, cidade=30, sem coords/desconhecido=20.
            if q == 0:
                pt = 100.0
            elif q == 1:
                pt = 80.0
            elif q == 2:
                pt = 55.0
            elif q == 3:
                pt = 30.0
            else:
                pt = 20.0
            q_geo_pts.append(pt)
        if not _url_indica_cidade_diferente_do_alvo(str(a.get("url_anuncio") or ""), cidade_ref):
            n_url_ok += 1
    n_am = max(1, len(amostras))
    geo_cobertura_raio_pct = max(0.0, min(100.0, round(100.0 * (len(dists) / n_am), 2)))
    geo_precisao_geocode_score = round((sum(q_geo_pts) / len(q_geo_pts)), 2) if q_geo_pts else 0.0
    geo_volume_util_score = max(
        0.0,
        min(100.0, round(100.0 * (len(amostras) / max(1, CACHE_VOLUME_BAIXO_LIMITE)), 2)),
    )
    geo_consistencia_cidade_url_pct = max(0.0, min(100.0, round(100.0 * (n_url_ok / n_am), 2)))
    geo_cobertura_bairro_pct = max(
        0.0,
        min(100.0, round(100.0 * (_contar_amostras_mesmo_bairro(amostras, bairro_ref_cache) / n_am), 2)),
    )
    geo_confianca_score = round(
        (0.30 * geo_cobertura_raio_pct)
        + (0.25 * geo_precisao_geocode_score)
        + (0.20 * geo_volume_util_score)
        + (0.20 * geo_consistencia_cidade_url_pct)
        + (0.05 * geo_cobertura_bairro_pct),
        2,
    )
    if geo_confianca_score >= 75:
        geo_confianca_classe = "alta"
    elif geo_confianca_score >= 55:
        geo_confianca_classe = "media"
    else:
        geo_confianca_classe = "baixa"
    dist_msg = f" em até {max(dists):.1f} km" if dists else ""
    geo_confianca_msg = (
        f"{geo_confianca_classe.title()} confiança: {len(amostras)} comparáveis diretos{dist_msg}. "
        f"Cobertura bairro {geo_cobertura_bairro_pct:.0f}%."
    )

    ref_flag = 1 if apenas_referencia else 0
    sim_flag = 1 if uso_simulacao else 0
    sufixo = f"tipo={tipo_segmento}|mod={modo}|sim={sim_flag}|ref={ref_flag}|r={int(raio_km)}"
    chave_segmento = f"{geo_bucket}|{sufixo}|{uuid.uuid4().hex[:12]}"
    chave_bairro = f"{slug_vivareal(cidade)}_{slug_vivareal(bairro)}_{geo_bucket}"[:190]
    nome = f"Mercado {int(raio_km)}km | {tipo_segmento} | {bairro or cidade} | n={len(amostras)}"
    if modo == "terrenos":
        nome = f"{nome} (terrenos — referência)"
    elif apenas_referencia:
        nome = f"{nome} (ref. {tipo_segmento})"
    if nome_suffix:
        nome = f"{nome} {nome_suffix.strip()}"

    extra = _parse_extra(leilao)
    meta: dict[str, Any] = {
        "leilao_imovel_id": str(leilao.get("id") or ""),
        "raio_km": raio_km,
        "modo_cache": modo,
        "tipo_segmento": tipo_segmento,
        "area_referencia_m2": _area_referencia_m2(leilao),
        "observacao_extra": (extra.get("observacoes_markdown") or "")[:500] if isinstance(extra, dict) else None,
        "uso_simulacao": bool(uso_simulacao),
        "apenas_referencia": bool(apenas_referencia),
        "tipo_casa_segmento": tipo_casa_segmento_meta or tipo_segmento,
        # Estatística robusta (mediana) para reduzir efeito de outliers no mercado.
        "preco_m2_mediana_amostra": preco_m2_mediana,
        "valor_mediana_venda_amostra": valor_mediana,
        "preco_m2_media_amostra": preco_m2_media,
        "valor_media_venda_amostra": valor_media,
        "geo_confianca_score": geo_confianca_score,
        "geo_confianca_classe": geo_confianca_classe,
        "geo_confianca_msg": geo_confianca_msg,
        "geo_cobertura_raio_pct": geo_cobertura_raio_pct,
        "geo_precisao_geocode_score": geo_precisao_geocode_score,
        "geo_volume_util_score": geo_volume_util_score,
        "geo_consistencia_cidade_url_pct": geo_consistencia_cidade_url_pct,
        "geo_cobertura_bairro_pct": geo_cobertura_bairro_pct,
    }
    if metadados_extras:
        for k, v in metadados_extras.items():
            if v is not None:
                meta[k] = v

    return {
        "chave_bairro": chave_bairro,
        "cidade": cidade,
        "bairro": bairro or "-",
        "estado": estado_sigla,
        "tipo_imovel": tipo_row,
        "conservacao": cons,
        "tipo_casa": tc,
        "faixa_andar": faixa_andar,
        "logradouro_chave": logr_ch,
        "geo_bucket": geo_bucket,
        "lat_ref": lat0,
        "lon_ref": lon0,
        "chave_segmento": chave_segmento,
        # Mantém o nome das colunas legado, mas com agregado robusto.
        "preco_m2_medio": preco_m2_mediana if pm2s else 0.0,
        "valor_medio_venda": valor_mediana if vals else 0.0,
        "maior_valor_venda": max(vals) if vals else None,
        "menor_valor_venda": min(vals) if vals else None,
        "n_amostras": len(amostras),
        "anuncios_ids": ",".join(str(a.get("id")) for a in amostras if a.get("id")),
        "nome_cache": nome[:240],
        "faixa_area": "-",
        "fonte": "cache_media_leilao_v1",
        "metadados_json": meta,
    }


def _centroide_coords_anuncios(anuncios: list[dict[str, Any]]) -> Optional[tuple[float, float]]:
    lats: list[float] = []
    lons: list[float] = []
    for a in anuncios:
        la, lo = coords_de_anuncio(a)
        if la is not None and lo is not None:
            lats.append(float(la))
            lons.append(float(lo))
    if not lats:
        return None
    return sum(lats) / len(lats), sum(lons) / len(lons)


def _tipo_imovel_predominante_anuncios(anuncios: list[dict[str, Any]]) -> str:
    c = Counter(
        str(normalizar_tipo_imovel(a.get("tipo_imovel")) or "desconhecido").lower() for a in anuncios
    )
    return c.most_common(1)[0][0] if c else "desconhecido"


def criar_cache_manual_de_anuncios(
    client: Client,
    anuncios: list[dict[str, Any]],
    nome_cache: str,
) -> tuple[bool, str, Optional[str]]:
    """
    Cria um registo em ``cache_media_bairro`` a partir de anúncios escolhidos na UI.
    O referencial geográfico (``lat_ref``/``lon_ref``/``geo_bucket``) é o **centróide** dos anúncios com coordenadas.
    Medianas/valores são recalculados a partir da amostra (mesma lógica de ``_montar_payload_cache``).
    """
    nome = (nome_cache or "").strip()
    if not nome:
        return False, "Indique um nome para o cache.", None
    if not anuncios:
        return False, "Selecione pelo menos um anúncio na tabela.", None

    valid: list[dict[str, Any]] = []
    for a in anuncios:
        if not a.get("id"):
            continue
        try:
            ar = float(a.get("area_construida_m2") or 0)
            v = float(a.get("valor_venda") or 0)
        except (TypeError, ValueError):
            continue
        if ar <= 0 or v <= 0:
            continue
        valid.append(a)
    if not valid:
        return False, "Nenhum anúncio válido: é necessário id, área e valor de venda > 0.", None

    cc = _centroide_coords_anuncios(valid)
    if cc is None:
        return (
            False,
            "Nenhum anúncio selecionado tem latitude e longitude. Re-geocodifique os anúncios e tente de novo.",
            None,
        )
    lat0, lon0 = cc
    geo_bucket = geo_bucket_de_coords(lat0, lon0)
    tipo_pred = _tipo_imovel_predominante_anuncios(valid)
    a0 = valid[0]
    cidade = str(a0.get("cidade") or "").strip() or "—"
    bairro = str(a0.get("bairro") or "").strip()
    uf_raw = str(a0.get("estado") or "").strip()
    estado_sigla = estado_livre_para_sigla_uf(uf_raw) or (uf_raw[:2].upper() if len(uf_raw) >= 2 else uf_raw)

    areas = [float(x.get("area_construida_m2") or 0) for x in valid if float(x.get("area_construida_m2") or 0) > 0]
    faixa_a = f"{min(areas):.0f}-{max(areas):.0f}" if areas else "-"

    synthetic: dict[str, Any] = {
        "id": "",
        "cidade": cidade,
        "bairro": bairro,
        "estado": estado_sigla,
        "tipo_imovel": tipo_pred,
        "conservacao": "desconhecido",
        "tipo_casa": "desconhecido",
        "andar": None,
        "endereco": str(a0.get("logradouro") or "").strip(),
        "leilao_extra_json": {},
        "area_util": max(0.0, float(a0.get("area_construida_m2") or 0)),
    }

    metadados_extras: dict[str, Any] = {
        "origem": "criacao_manual_ui",
        "nome_definido_por_usuario": nome[:240],
        "volume_amostras_baixo": len(valid) < CACHE_VOLUME_BAIXO_LIMITE,
        "volume_amostras_alvo_conforto": CACHE_VOLUME_BAIXO_LIMITE,
    }

    row = _montar_payload_cache(
        synthetic,
        valid,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_segmento=tipo_pred,
        modo="principal",
        raio_km=float(RAIO_KM_PADRAO),
        uso_simulacao=True,
        apenas_referencia=False,
        metadados_extras=metadados_extras,
    )
    row["nome_cache"] = nome[:240]
    row["faixa_area"] = (faixa_a or "-")[:80]
    row["fonte"] = "cache_manual_ui_v1"

    new_id = cache_media_bairro_repo.inserir(client, row)
    if not new_id:
        return (
            False,
            "Falha ao inserir em cache_media_bairro (ver credenciais, rede ou conflito de chave).",
            None,
        )
    return (
        True,
        f"Cache gravado: {len(valid)} amostra(s), preço/m² e valores agregados na linha.",
        new_id,
    )


@dataclass
class ResultadoCriacaoCacheLeilao:
    ok: bool
    mensagem: str
    caches_criados: list[dict[str, Any]] = field(default_factory=list)
    usou_firecrawl_extra: bool = False
    reutilizou_existente: bool = False
    firecrawl_chamadas_api: int = 0
    log_diagnostico: str = ""


def formatar_log_pos_cache(res: ResultadoCriacaoCacheLeilao) -> str:
    """Resumo + diagnóstico opcional para ``ultima_ingestao_log_text`` após o passo automático de cache."""
    if not res.ok:
        base = f"Cache (automático): falha — {res.mensagem}"
    else:
        suf = ""
        if res.reutilizou_existente:
            suf = " (reutilizou cache existente)"
        if res.usou_firecrawl_extra:
            suf += " [Firecrawl Search na composição do cache]"
        base = f"Cache (automático): OK{suf} — {res.mensagem}"
    extra = (res.log_diagnostico or "").strip()
    if extra:
        return f"{base}\n--- Diagnóstico cache ---\n{extra}"
    return base


def _parse_csv_anuncio_ids(raw: Any) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _metadados_cache_como_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


def _cache_row_segmento_terreno(row: dict[str, Any]) -> bool:
    meta = _metadados_cache_como_dict(row.get("metadados_json"))
    if meta.get("modo_cache") == "terrenos":
        return True
    if str(meta.get("tipo_segmento") or "").strip().lower() == "terreno":
        return True
    nome = str(row.get("nome_cache") or "").lower()
    return "(terrenos)" in nome


def _cache_row_principal_para_tipo(row: dict[str, Any], tipo_l: str) -> bool:
    if _cache_row_segmento_terreno(row):
        return False
    meta = _metadados_cache_como_dict(row.get("metadados_json"))
    if meta.get("apenas_referencia") is True:
        return False
    seg = str(meta.get("tipo_segmento") or "").strip().lower()
    if seg:
        return seg == tipo_l.strip().lower()
    return str(row.get("tipo_imovel") or "").strip().lower() == tipo_l.strip().lower()


def _bairro_normalizado_para_match(v: Any) -> str:
    s = slug_vivareal(str(v or "").strip())
    return "" if s in ("", "-") else s


def _contar_amostras_mesmo_bairro(amostras: list[dict[str, Any]], bairro_referencia: str) -> int:
    b_ref = _bairro_normalizado_para_match(bairro_referencia)
    if not b_ref:
        return 0
    n = 0
    for a in amostras:
        if _bairro_normalizado_para_match(a.get("bairro")) == b_ref:
            n += 1
    return n


def _ordenar_amostras_priorizando_mesmo_bairro(
    amostras: list[dict[str, Any]],
    bairro_referencia: str,
) -> list[dict[str, Any]]:
    b_ref = _bairro_normalizado_para_match(bairro_referencia)
    if not b_ref:
        return list(amostras)
    mesmos: list[dict[str, Any]] = []
    outros: list[dict[str, Any]] = []
    for a in amostras:
        if _bairro_normalizado_para_match(a.get("bairro")) == b_ref:
            mesmos.append(a)
        else:
            outros.append(a)
    return mesmos + outros


def _metadados_anuncio_como_dict(a: dict[str, Any]) -> dict[str, Any]:
    raw = a.get("metadados_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            j = json.loads(raw)
            if isinstance(j, dict):
                return j
        except json.JSONDecodeError:
            return {}
    return {}


def _logradouro_tem_numero(logradouro: Any) -> bool:
    s = str(logradouro or "").strip()
    return bool(re.search(r"\d", s))


def _penalidade_qualidade_geo_anuncio(a: dict[str, Any]) -> int:
    """
    Menor é melhor:
    0 = rooftop (número exacto) — geocode preciso ou logradouro com número;
    1 = rua (centróide de rua, sem número);
    2 = bairro (centróide de bairro, com jitter ±80 m aplicado na persistência);
    3 = cidade (centróide do município) — só usado se faltarem alternativas;
    4 = sem coordenadas / desconhecido.

    Lê primeiro o marcador explícito ``metadados_json.precisao_geo`` produzido
    pelo :mod:`comparaveis.persistencia` (valores possíveis: ``rooftop``,
    ``rua``, ``bairro_centroide``, ``cidade_centroide``, ``desconhecido``).
    Se o marcador não existir (anúncios antigos), faz fallback para a
    heurística antiga baseada em texto livre nos metadados + ``logradouro``.
    """
    la, lo = coords_de_anuncio(a)
    if la is None or lo is None:
        return 4

    md = _metadados_anuncio_como_dict(a)
    marcador = str(md.get("precisao_geo") or "").strip().lower()
    if marcador:
        if marcador == "rooftop":
            return 0
        if marcador == "rua":
            return 1
        if marcador == "bairro_centroide":
            return 2
        if marcador == "cidade_centroide":
            return 3
        if marcador == "desconhecido":
            return 4

    txt_md = " ".join(f"{k}:{v}" for k, v in md.items()).lower()
    if "centroide" in txt_md or "fallback" in txt_md:
        return 2
    if _logradouro_tem_numero(a.get("logradouro")):
        return 0
    return 1


def _ordenar_amostras_para_cache_principal(
    amostras: list[dict[str, Any]],
    *,
    bairro_referencia: str,
    lat0: float,
    lon0: float,
    area_ref: float,
    geo_first_enabled: bool = True,
) -> list[dict[str, Any]]:
    """
    Ordenação estável para o cache principal:
    Geo-first:
    1) menor distância real ao imóvel
    2) melhor qualidade de geolocalização
    3) menor delta de área quando há área de referência
    4) mesmo bairro (sinal auxiliar)
    Legacy (sem geo-first): mesmo bairro continua primeiro.
    """
    b_ref = _bairro_normalizado_para_match(bairro_referencia)

    def _key(a: dict[str, Any]) -> tuple[float, float, float, float]:
        b = _bairro_normalizado_para_match(a.get("bairro"))
        bairro_pen = 0.0 if (b_ref and b and b == b_ref) else 1.0
        la, lo = coords_de_anuncio(a)
        if la is None or lo is None:
            dist = 1e9
        else:
            dist = float(haversine_km(lat0, lon0, float(la), float(lo)))
        q_geo = float(_penalidade_qualidade_geo_anuncio(a))
        try:
            ar = float(a.get("area_construida_m2") or 0.0)
        except (TypeError, ValueError):
            ar = 0.0
        delta_a = abs(ar - float(area_ref)) if (area_ref > 0 and ar > 0) else 0.0
        if geo_first_enabled:
            return (dist, q_geo, delta_a, bairro_pen)
        return (bairro_pen, dist, q_geo, delta_a)

    return sorted(amostras, key=_key)


def _ordenar_candidatos_priorizando_mesmo_bairro(
    candidatos: list[dict[str, Any]],
    bairro_leilao: str,
) -> list[dict[str, Any]]:
    """Mantém candidatos do mesmo bairro à frente; restante segue ordem original."""
    b_ref = _bairro_normalizado_para_match(bairro_leilao)
    if not b_ref:
        return list(candidatos)
    mesmos: list[dict[str, Any]] = []
    outros: list[dict[str, Any]] = []
    for c in candidatos:
        b_c = _bairro_normalizado_para_match(c.get("bairro"))
        if b_c and b_c == b_ref:
            mesmos.append(c)
        else:
            outros.append(c)
    return mesmos + outros


def _diagnostico_reuso_bairro(cache_row: dict[str, Any], bairro_leilao: str) -> str:
    """Classifica se o reuso foi no mesmo bairro ou fallback em bairro distinto."""
    b_le = str(bairro_leilao or "").strip()
    b_ca = str(cache_row.get("bairro") or "").strip()
    n_le = _bairro_normalizado_para_match(b_le)
    n_ca = _bairro_normalizado_para_match(b_ca)
    if n_le and n_ca:
        if n_le == n_ca:
            return "mesmo_bairro"
        return f"fallback_outro_bairro (leilao='{b_le or '-'}' cache='{b_ca or '-'}')"
    if not n_le:
        return "bairro_leilao_ausente"
    if not n_ca:
        return "bairro_cache_ausente"
    return "bairro_indeterminado"


def _raios_expansao_geo_first(raio_base_km: float) -> list[float]:
    rb = max(0.5, float(raio_base_km or RAIO_KM_PADRAO))
    vals = [rb, max(rb, RAIO_EXPANSAO_GEO_FIRST_MEDIO_KM), max(rb, RAIO_EXPANSAO_GEO_FIRST_MAX_KM)]
    out: list[float] = []
    for v in vals:
        rv = round(float(v), 4)
        if rv not in out:
            out.append(rv)
    return out


def _atingiu_cobertura_bairro_geo_first(
    *,
    n_amostras: int,
    n_mesmo_bairro: int,
    min_amostras: int,
    min_mesmo_bairro: int,
    bairro_referencia: str,
    geo_first_enabled: bool = True,
) -> bool:
    if int(n_amostras) < int(min_amostras):
        return False
    if not _bairro_normalizado_para_match(bairro_referencia):
        return True
    if int(min_mesmo_bairro) <= 0:
        return True
    if int(n_mesmo_bairro) >= int(min_mesmo_bairro):
        return True
    if not geo_first_enabled:
        return False
    # Geo-first: aceita cobertura parcial do bairro alvo quando o volume total é suficiente.
    min_relax = max(1, int(min_mesmo_bairro // 2))
    return int(n_mesmo_bairro) >= int(min_relax)


def _amostras_reuso_validas(
    client: Client,
    cache_row: dict[str, Any],
    lat0: float,
    lon0: float,
    area_ref: float,
    tipos_anuncio: list[str],
    *,
    raio_km: float,
    aplicar_faixa_area_edital: bool = True,
    leilao: dict[str, Any] | None = None,
    bairro_referencia: str = "",
    min_amostras_mesmo_bairro: int = 0,
    geo_first_enabled: bool = True,
    empreendimento_referencia: str = "",
) -> Optional[list[dict[str, Any]]]:
    ids = _parse_csv_anuncio_ids(cache_row.get("anuncios_ids"))
    if not ids:
        return None
    ads = anuncios_mercado_repo.buscar_por_ids(client, ids)
    if not ads:
        return None
    cidade_ref = str((leilao or {}).get("cidade") or cache_row.get("cidade") or "").strip()
    ads = _filtrar_candidatos_por_coerencia_url_cidade(ads, cidade_ref)
    if not ads:
        return None
    tset = {t.strip().lower() for t in tipos_anuncio if str(t).strip()}
    if tset:
        ads_f = [a for a in ads if str(a.get("tipo_imovel") or "").strip().lower() in tset]
        # Reuso só é permitido quando o segmento do cache está consistente.
        # Se existir qualquer anúncio fora dos tipos esperados, força reconstrução
        # para evitar perpetuar cache "contaminado" por classificação antiga.
        if len(ads_f) != len(ads):
            return None
    else:
        ads_f = ads
    amostras = _filtrar_amostras(
        ads_f,
        lat0,
        lon0,
        area_ref,
        raio_km=raio_km,
        aplicar_faixa_area_edital=aplicar_faixa_area_edital,
        empreendimento_referencia=empreendimento_referencia,
    )
    amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, None)
    amostras = _sanear_amostras_para_cache(amostras, None)
    if int(min_amostras_mesmo_bairro or 0) > 0:
        n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro_referencia)
        if not _atingiu_cobertura_bairro_geo_first(
            n_amostras=len(amostras),
            n_mesmo_bairro=n_mesmo_bairro,
            min_amostras=max(CACHE_MONTE_MIN_EXIGIDO, int(min_amostras_mesmo_bairro or 0)),
            min_mesmo_bairro=int(min_amostras_mesmo_bairro),
            bairro_referencia=bairro_referencia,
            geo_first_enabled=geo_first_enabled,
        ):
            return None
    if len(amostras) >= CACHE_MONTE_MIN_EXIGIDO:
        return amostras
    return None


def _montar_amostras_para_tipos(
    client: Client,
    lat0: float,
    lon0: float,
    area_ref: float,
    tipos: list[str],
    estado_sigla: str,
    cidade: str,
    bairro: str,
    *,
    raio_km: float,
    pode_geocode: bool,
    pode_firecrawl_search: bool,
    tipo_imovel_coleta: str,
    leilao_id: str,
    estado_raw: str,
    ignorar_cache_firecrawl: bool,
    max_chamadas_api_firecrawl: int | None = None,
    leilao: dict[str, Any] | None = None,
    frase_busca_firecrawl_override: str | None = None,
) -> tuple[list[dict[str, Any]], bool, str, str, int]:
    """
    Devolve (amostras, usou_firecrawl_listagem, mensagem_erro, log_diagnostico, n_chamadas_api_fc).
    ``usou_firecrawl_listagem`` indica se Firecrawl Search foi usado com sucesso (amostras suficientes após a rodada).
    ``n_chamadas_api_fc`` é a estimativa de chamadas à API (só >0 quando ``_uma_coleta_firecrawl_search`` corre).
    ``frase_busca_firecrawl_override`` substitui a frase automática nessa ida à pesquisa web (confirmação do utilizador).
    ``max_chamadas_api_firecrawl``: teto de chamadas (search + scrapes) para esta rodada; ``None`` = sem teto extra.
    Ordem: (1) lista no BD por cidade/UF/tipos → filtro raio/área; (2) geocode + re-lista;
    (3) só se ainda faltar amostra, uma Firecrawl Search → re-lista.
    Com ``leilao``, remove anúncios de listagem do próprio lance (1ª/2ª praça + sinais).
    """
    min_n = get_busca_mercado_parametros().min_amostras_cache
    min_mesmo_bairro = max(0, int(min_n))
    min_mesmo_bairro_relax = max(0, int(min_n // 2))
    geo_first_enabled = _cache_geo_first_enabled(cidade)
    radius_exp_enabled = _cache_radius_expansion_enabled(cidade)
    raios_busca = _raios_expansao_geo_first(raio_km) if radius_exp_enabled else [max(0.5, float(raio_km or RAIO_KM_PADRAO))]
    aplicar_faixa = not _tipos_somente_terreno_ou_lote(tipos)
    msg = ""
    extra_leilao = _parse_extra(leilao or {})
    bairro_canonico = str(extra_leilao.get("bairro_canonico") or "").strip()
    empreendimento_ref = _nome_empreendimento_leilao(leilao or {})
    tipos_txt = ",".join(str(t).strip() for t in tipos if str(t).strip()) or tipo_imovel_coleta
    linhas: list[str] = [
        f"Montagem amostras: tipos_busca=[{tipos_txt}] tipo_coleta_fc={tipo_imovel_coleta} "
        f"(mínimo exigido={min_n}; BD antes de Firecrawl)"
    ]
    linhas.append(
        f"Geo-first={'ativo' if geo_first_enabled else 'desligado'}; "
        f"bairro canônico={'ativo' if _bairro_canonico_enabled(cidade) else 'desligado'}; "
        f"expansão_raio={'ativa' if radius_exp_enabled else 'desligada'}."
    )
    if empreendimento_ref:
        linhas.append(f"Empreendimento alvo detectado: {empreendimento_ref}")
    if geo_first_enabled:
        linhas.append(
            "Geo-first ativo: prioriza distância real e aceita cobertura parcial do bairro alvo "
            f"(mínimo relaxado={min_mesmo_bairro_relax})."
        )
    if radius_exp_enabled and len(raios_busca) > 1:
        linhas.append(
            "Expansão progressiva de raio habilitada: "
            + " -> ".join(f"{r:g}km" for r in raios_busca)
        )
    candidatos = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
        client,
        cidade=cidade,
        estado_sigla=estado_sigla,
        tipos_imovel=tipos,
    )
    candidatos = _filtrar_candidatos_por_coerencia_url_cidade(
        candidatos, cidade, linhas=linhas, etapa="query_bd_coerencia_cidade_url"
    )
    linhas.append(
        _diagnostico_filtro_amostras(
            candidatos,
            lat0,
            lon0,
            area_ref,
            raio_km=raio_km,
            etiqueta="Após query BD (antes de geocode/Firecrawl)",
            aplicar_faixa_area_edital=aplicar_faixa,
            bairro_informado=bairro,
            bairro_canonico=bairro_canonico,
            empreendimento_referencia=empreendimento_ref,
        )
    )
    amostras = _filtrar_amostras(
        candidatos,
        lat0,
        lon0,
        area_ref,
        raio_km=raio_km,
        aplicar_faixa_area_edital=aplicar_faixa,
        empreendimento_referencia=empreendimento_ref,
    )
    amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
    amostras = _sanear_amostras_para_cache(amostras, linhas)
    n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
    linhas.append(
        f"bairro_alvo: '{bairro or '-'}' | amostras_mesmo_bairro={n_mesmo_bairro} "
        f"(mínimo estrito={min_mesmo_bairro}; relaxado={min_mesmo_bairro_relax})"
    )

    if _atingiu_cobertura_bairro_geo_first(
        n_amostras=len(amostras),
        n_mesmo_bairro=n_mesmo_bairro,
        min_amostras=min_n,
        min_mesmo_bairro=min_mesmo_bairro,
        bairro_referencia=bairro,
        geo_first_enabled=geo_first_enabled,
    ):
        linhas.append("Resultado: amostras suficientes só com dados já no BD (sem geocode nem Firecrawl).")
        return amostras, False, msg, "\n".join(linhas), 0
    for raio_exp in raios_busca[1:]:
        linhas.append(
            _diagnostico_filtro_amostras(
                candidatos,
                lat0,
                lon0,
                area_ref,
                raio_km=raio_exp,
                etiqueta=f"Expansão de raio (somente BD) @{raio_exp:g}km",
                aplicar_faixa_area_edital=aplicar_faixa,
                bairro_informado=bairro,
                bairro_canonico=bairro_canonico,
                empreendimento_referencia=empreendimento_ref,
            )
        )
        amostras_exp = _filtrar_amostras(
            candidatos,
            lat0,
            lon0,
            area_ref,
            raio_km=raio_exp,
            aplicar_faixa_area_edital=aplicar_faixa,
            empreendimento_referencia=empreendimento_ref,
        )
        amostras_exp = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras_exp, leilao, linhas)
        amostras_exp = _sanear_amostras_para_cache(amostras_exp, linhas)
        n_mesmo_exp = _contar_amostras_mesmo_bairro(amostras_exp, bairro)
        linhas.append(
            f"expansao_bd@{raio_exp:g}km: amostras_mesmo_bairro={n_mesmo_exp} "
            f"(mínimo estrito={min_mesmo_bairro}; relaxado={min_mesmo_bairro_relax})"
        )
        if _atingiu_cobertura_bairro_geo_first(
            n_amostras=len(amostras_exp),
            n_mesmo_bairro=n_mesmo_exp,
            min_amostras=min_n,
            min_mesmo_bairro=min_mesmo_bairro,
            bairro_referencia=bairro,
            geo_first_enabled=geo_first_enabled,
        ):
            linhas.append(f"Resultado: amostras suficientes com expansão de raio para {raio_exp:g}km (sem geocode/Firecrawl).")
            return amostras_exp, False, msg, "\n".join(linhas), 0
        amostras = amostras_exp

    if pode_geocode:
        _tentar_geocodificar_sem_coordenadas(
            client,
            candidatos,
            cidade=cidade,
            estado_sigla=estado_sigla,
            bairro_fb=bairro,
            bairro_alvo=bairro,
        )
        candidatos = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
            client,
            cidade=cidade,
            estado_sigla=estado_sigla,
            tipos_imovel=tipos,
        )
        candidatos = _filtrar_candidatos_por_coerencia_url_cidade(
            candidatos, cidade, linhas=linhas, etapa="apos_geocode_coerencia_cidade_url"
        )
        linhas.append(
            _diagnostico_filtro_amostras(
                candidatos,
                lat0,
                lon0,
                area_ref,
                raio_km=raio_km,
                etiqueta="Após geocodificar anúncios sem coord e re-query BD",
                aplicar_faixa_area_edital=aplicar_faixa,
                bairro_informado=bairro,
                bairro_canonico=bairro_canonico,
                empreendimento_referencia=empreendimento_ref,
            )
        )
        amostras = _filtrar_amostras(
            candidatos,
            lat0,
            lon0,
            area_ref,
            raio_km=raio_km,
            aplicar_faixa_area_edital=aplicar_faixa,
            empreendimento_referencia=empreendimento_ref,
        )
        amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
        amostras = _sanear_amostras_para_cache(amostras, linhas)
        n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
        linhas.append(
            f"após_geocode: amostras_mesmo_bairro={n_mesmo_bairro} "
            f"(mínimo estrito={min_mesmo_bairro}; relaxado={min_mesmo_bairro_relax})"
        )
        if _atingiu_cobertura_bairro_geo_first(
            n_amostras=len(amostras),
            n_mesmo_bairro=n_mesmo_bairro,
            min_amostras=min_n,
            min_mesmo_bairro=min_mesmo_bairro,
            bairro_referencia=bairro,
            geo_first_enabled=geo_first_enabled,
        ):
            linhas.append("Resultado: amostras suficientes após geocode (sem Firecrawl).")
            return amostras, False, msg, "\n".join(linhas), 0
        for raio_exp in raios_busca[1:]:
            linhas.append(
                _diagnostico_filtro_amostras(
                    candidatos,
                    lat0,
                    lon0,
                    area_ref,
                    raio_km=raio_exp,
                    etiqueta=f"Expansão de raio (pós-geocode) @{raio_exp:g}km",
                    aplicar_faixa_area_edital=aplicar_faixa,
                    bairro_informado=bairro,
                    bairro_canonico=bairro_canonico,
                    empreendimento_referencia=empreendimento_ref,
                )
            )
            amostras_exp = _filtrar_amostras(
                candidatos,
                lat0,
                lon0,
                area_ref,
                raio_km=raio_exp,
                aplicar_faixa_area_edital=aplicar_faixa,
                empreendimento_referencia=empreendimento_ref,
            )
            amostras_exp = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras_exp, leilao, linhas)
            amostras_exp = _sanear_amostras_para_cache(amostras_exp, linhas)
            n_mesmo_exp = _contar_amostras_mesmo_bairro(amostras_exp, bairro)
            linhas.append(
                f"expansao_pos_geocode@{raio_exp:g}km: amostras_mesmo_bairro={n_mesmo_exp} "
                f"(mínimo estrito={min_mesmo_bairro}; relaxado={min_mesmo_bairro_relax})"
            )
            if _atingiu_cobertura_bairro_geo_first(
                n_amostras=len(amostras_exp),
                n_mesmo_bairro=n_mesmo_exp,
                min_amostras=min_n,
                min_mesmo_bairro=min_mesmo_bairro,
                bairro_referencia=bairro,
                geo_first_enabled=geo_first_enabled,
            ):
                linhas.append(f"Resultado: amostras suficientes após geocode com expansão para {raio_exp:g}km.")
                return amostras_exp, False, msg, "\n".join(linhas), 0
            amostras = amostras_exp

    n_api_fc = 0
    pode_fc = pode_firecrawl_search and (max_chamadas_api_firecrawl is None or int(max_chamadas_api_firecrawl) > 0)
    if pode_fc:
        n, n_api_fc = _uma_coleta_firecrawl_search(
            client,
            cidade=cidade,
            estado_raw=estado_raw,
            bairro=bairro,
            tipo_imovel=tipo_imovel_coleta,
            area_ref=area_ref,
            leilao_id=leilao_id,
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
            max_chamadas_api=max_chamadas_api_firecrawl,
            frase_busca_override=frase_busca_firecrawl_override,
        )
        logger.info(
            "Complemento Firecrawl Search para cache: %s anúncios gravados (api_estimada=%s)",
            n,
            n_api_fc,
        )
        linhas.append(
            f"Firecrawl Search: anúncios gravados nesta rodada={n} chamadas_api_estimadas={n_api_fc}"
        )
        candidatos = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
            client,
            cidade=cidade,
            estado_sigla=estado_sigla,
            tipos_imovel=tipos,
        )
        candidatos = _filtrar_candidatos_por_coerencia_url_cidade(
            candidatos, cidade, linhas=linhas, etapa="apos_firecrawl_coerencia_cidade_url"
        )
        linhas.append(
            _diagnostico_filtro_amostras(
                candidatos,
                lat0,
                lon0,
                area_ref,
                raio_km=raio_km,
                etiqueta="Após Firecrawl Search e re-query BD",
                aplicar_faixa_area_edital=aplicar_faixa,
                bairro_informado=bairro,
                bairro_canonico=bairro_canonico,
                empreendimento_referencia=empreendimento_ref,
            )
        )
        amostras = _filtrar_amostras(
            candidatos,
            lat0,
            lon0,
            area_ref,
            raio_km=raio_km,
            aplicar_faixa_area_edital=aplicar_faixa,
            empreendimento_referencia=empreendimento_ref,
        )
        amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
        amostras = _sanear_amostras_para_cache(amostras, linhas)
        n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
        linhas.append(
            f"após_firecrawl: amostras_mesmo_bairro={n_mesmo_bairro} "
            f"(mínimo estrito={min_mesmo_bairro}; relaxado={min_mesmo_bairro_relax})"
        )
        if _atingiu_cobertura_bairro_geo_first(
            n_amostras=len(amostras),
            n_mesmo_bairro=n_mesmo_bairro,
            min_amostras=min_n,
            min_mesmo_bairro=min_mesmo_bairro,
            bairro_referencia=bairro,
            geo_first_enabled=geo_first_enabled,
        ):
            linhas.append("Resultado: amostras suficientes após Firecrawl Search.")
            return amostras, True, msg, "\n".join(linhas), n_api_fc
        for raio_exp in raios_busca[1:]:
            linhas.append(
                _diagnostico_filtro_amostras(
                    candidatos,
                    lat0,
                    lon0,
                    area_ref,
                    raio_km=raio_exp,
                    etiqueta=f"Expansão de raio (pós-Firecrawl) @{raio_exp:g}km",
                    aplicar_faixa_area_edital=aplicar_faixa,
                    bairro_informado=bairro,
                    bairro_canonico=bairro_canonico,
                    empreendimento_referencia=empreendimento_ref,
                )
            )
            amostras_exp = _filtrar_amostras(
                candidatos,
                lat0,
                lon0,
                area_ref,
                raio_km=raio_exp,
                aplicar_faixa_area_edital=aplicar_faixa,
                empreendimento_referencia=empreendimento_ref,
            )
            amostras_exp = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras_exp, leilao, linhas)
            amostras_exp = _sanear_amostras_para_cache(amostras_exp, linhas)
            n_mesmo_exp = _contar_amostras_mesmo_bairro(amostras_exp, bairro)
            linhas.append(
                f"expansao_pos_firecrawl@{raio_exp:g}km: amostras_mesmo_bairro={n_mesmo_exp} "
                f"(mínimo estrito={min_mesmo_bairro}; relaxado={min_mesmo_bairro_relax})"
            )
            if _atingiu_cobertura_bairro_geo_first(
                n_amostras=len(amostras_exp),
                n_mesmo_bairro=n_mesmo_exp,
                min_amostras=min_n,
                min_mesmo_bairro=min_mesmo_bairro,
                bairro_referencia=bairro,
                geo_first_enabled=geo_first_enabled,
            ):
                linhas.append(f"Resultado: amostras suficientes após Firecrawl com expansão para {raio_exp:g}km.")
                return amostras_exp, True, msg, "\n".join(linhas), n_api_fc
            amostras = amostras_exp

    elif pode_firecrawl_search and max_chamadas_api_firecrawl is not None and int(max_chamadas_api_firecrawl) <= 0:
        linhas.append("Firecrawl Search: omitido (orçamento de chamadas API esgotado para esta rodada).")

    amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
    amostras = _sanear_amostras_para_cache(amostras, linhas)
    n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
    if len(amostras) >= CACHE_MONTE_MIN_EXIGIDO:
        linhas.append(
            f"Resultado: {len(amostras)} amostra(s) após todas as etapas (mínimo configurado p/ coleta={min_n}; "
            "montagem de cache permitida com volume reduzido se <5 amostras no segmento)."
        )
        limite_bairro_log = min_mesmo_bairro_relax if geo_first_enabled else min_mesmo_bairro
        if n_mesmo_bairro < limite_bairro_log:
            linhas.append(
                "Observação: sem amostras suficientes do bairro alvo; cache montado com fallback de bairros próximos."
            )
        return amostras, False, "", "\n".join(linhas), n_api_fc

    msg = mensagem_com_dica_ajuste_busca(
        "Nenhum anúncio válido após geocodificar e complementar via Firecrawl Search (sem coordenadas ou fora dos filtros)."
    )
    linhas.append("Resultado: 0 amostras após todas as etapas.")
    return amostras, False, msg, "\n".join(linhas), n_api_fc


def _inserir_caches_residenciais_fatiados(
    client: Client,
    leilao: dict[str, Any],
    amostras: list[dict[str, Any]],
    *,
    lat0: float,
    lon0: float,
    geo_bucket: str,
    tipo_l: str,
    raio: float,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    """
    Grava cache principal (melhores amostras até o teto de **Ajustes de busca**) + caches de referência
    em lotes. Devolve ``(entradas_resumo, mensagem_erro)``; mensagem só se falhar insert.
    """
    if not amostras:
        return [], "Lista de amostras vazia."
    cap_p, cap_l = _caps_amostras_cache_mercado()
    amostras_ordenadas = _ordenar_amostras_para_cache_principal(
        amostras,
        bairro_referencia=_bairro_referencia_cache_leilao(leilao),
        lat0=float(lat0),
        lon0=float(lon0),
        area_ref=_area_referencia_m2(leilao),
        geo_first_enabled=_cache_geo_first_enabled(str(leilao.get("cidade") or "")),
    )
    tc_o = normalizar_tipo_casa(leilao.get("tipo_casa"), tipo_l)
    tipo_casa_prim = str(tc_o) if tc_o else "-"
    pri, secs = _fatias_amostras_cache(amostras_ordenadas, cap_p, cap_l)
    out: list[dict[str, Any]] = []

    row_p = _montar_payload_cache(
        leilao,
        pri,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_segmento=tipo_l,
        modo="principal",
        raio_km=raio,
        uso_simulacao=True,
        apenas_referencia=False,
        tipo_casa_segmento_meta=tipo_l,
        tipo_imovel_cache=tipo_l,
        tipo_casa_coluna=tipo_casa_prim,
        metadados_extras=_meta_volume_e_papel(
            len(pri), "principal_simulacao", cap_principal=cap_p, cap_lote=cap_l
        ),
    )
    cid_p = cache_media_bairro_repo.inserir(client, row_p)
    if not cid_p:
        return [], "Falha ao inserir cache_media_bairro (principal)."
    out.append(
        {
            "id": cid_p,
            "nome_cache": row_p.get("nome_cache"),
            "n_amostras": len(pri),
            "modo": "principal",
        }
    )

    for j, chunk in enumerate(secs, start=1):
        row_s = _montar_payload_cache(
            leilao,
            chunk,
            lat0=lat0,
            lon0=lon0,
            geo_bucket=geo_bucket,
            tipo_segmento=tipo_l,
            modo="principal",
            raio_km=raio,
            uso_simulacao=False,
            apenas_referencia=True,
            tipo_casa_segmento_meta=f"referencia_lote_{j}",
            tipo_imovel_cache=tipo_l,
            tipo_casa_coluna="-",
            nome_suffix=f"(referência extra {j})",
            metadados_extras=_meta_volume_e_papel(
                len(chunk),
                "referencia_extra",
                lote_referencia_indice=j,
                cap_principal=cap_p,
                cap_lote=cap_l,
            ),
        )
        cid_s = cache_media_bairro_repo.inserir(client, row_s)
        if not cid_s:
            return [], f"Falha ao inserir cache_media_bairro (referência extra {j})."
        out.append(
            {
                "id": cid_s,
                "nome_cache": row_s.get("nome_cache"),
                "n_amostras": len(chunk),
                "modo": "principal_ref",
            }
        )
    return out, None


def _inserir_caches_terrenos_fatiados(
    client: Client,
    leilao: dict[str, Any],
    amostras_t: list[dict[str, Any]],
    *,
    lat0: float,
    lon0: float,
    geo_bucket: str,
    raio: float,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    if not amostras_t:
        return [], None
    cap_p, cap_l = _caps_amostras_cache_mercado()
    pri, secs = _fatias_amostras_cache(amostras_t, cap_p, cap_l)
    chunks: list[list[dict[str, Any]]] = []
    if pri:
        chunks.append(pri)
    chunks.extend(secs)
    out: list[dict[str, Any]] = []
    nchunks = len(chunks)
    for j, chunk in enumerate(chunks, start=1):
        suffix = None
        if nchunks > 1:
            suffix = f"· parte {j}/{nchunks}"
        row_t = _montar_payload_cache(
            leilao,
            chunk,
            lat0=lat0,
            lon0=lon0,
            geo_bucket=geo_bucket,
            tipo_segmento="terreno",
            modo="terrenos",
            raio_km=raio,
            uso_simulacao=False,
            apenas_referencia=True,
            tipo_casa_segmento_meta="terreno",
            nome_suffix=suffix,
            metadados_extras=_meta_volume_e_papel(
                len(chunk),
                "terrenos_referencia",
                lote_referencia_indice=j,
                cap_principal=cap_p,
                cap_lote=cap_l,
            ),
        )
        cid_t = cache_media_bairro_repo.inserir(client, row_t)
        if not cid_t:
            return [], f"Falha ao inserir cache_media_bairro (terrenos parte {j})."
        out.append(
            {
                "id": cid_t,
                "nome_cache": row_t.get("nome_cache"),
                "n_amostras": len(chunk),
                "modo": "terrenos",
            }
        )
    return out, None


def _selecionar_amostras_apoio_escala(
    candidatos: list[dict[str, Any]],
    *,
    area_ref: float,
    fator_min: float,
    fator_max: float,
    ids_excluir: set[str] | None = None,
    min_amostras: int = CACHE_APOIO_ESCALA_MIN_AMOSTRAS,
    limite: int = CACHE_AMOSTRAS_LOTE_REFERENCIA,
) -> list[dict[str, Any]]:
    if area_ref <= 0:
        return []
    lo = float(fator_min) * float(area_ref)
    hi = float(fator_max) * float(area_ref)
    excl = ids_excluir or set()
    out: list[dict[str, Any]] = []
    for a in candidatos:
        aid = str(a.get("id") or "").strip()
        if aid and aid in excl:
            continue
        ar = _float_positivo(a.get("area_construida_m2"))
        if ar is None:
            continue
        if not (lo <= float(ar) <= hi):
            continue
        out.append(a)
        if len(out) >= int(max(1, limite)):
            break
    if len(out) < int(max(1, min_amostras)):
        return []
    return out


def _inserir_cache_apoio_escala(
    client: Client,
    leilao: dict[str, Any],
    amostras: list[dict[str, Any]],
    *,
    lat0: float,
    lon0: float,
    geo_bucket: str,
    tipo_l: str,
    raio: float,
    etiqueta: str,
    fator_min: float,
    fator_max: float,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    if not amostras:
        return None, None
    row = _montar_payload_cache(
        leilao,
        amostras,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_segmento=tipo_l,
        modo="principal",
        raio_km=raio,
        uso_simulacao=False,
        apenas_referencia=True,
        tipo_casa_segmento_meta=f"apoio_escala_{etiqueta}",
        tipo_imovel_cache=tipo_l,
        tipo_casa_coluna="-",
        nome_suffix=f"(apoio escala {etiqueta})",
        metadados_extras={
            **_meta_volume_e_papel(len(amostras), f"apoio_escala_{etiqueta}"),
            "apoio_escala_fator_min": float(fator_min),
            "apoio_escala_fator_max": float(fator_max),
            "apoio_escala_area_ref_m2": float(_area_referencia_m2(leilao)),
        },
    )
    cid = cache_media_bairro_repo.inserir(client, row)
    if not cid:
        return None, f"Falha ao inserir cache_media_bairro (apoio escala {etiqueta})."
    return (
        {
            "id": cid,
            "nome_cache": row.get("nome_cache"),
            "n_amostras": len(amostras),
            "modo": f"apoio_escala_{etiqueta}",
        },
        None,
    )


def _criar_caches_apoio_escala(
    client: Client,
    leilao: dict[str, Any],
    *,
    lat0: float,
    lon0: float,
    geo_bucket: str,
    tipo_l: str,
    estado_sigla: str,
    cidade: str,
    raio: float,
    ids_excluir: set[str] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    if tipo_l in _TIPOS_TERRENO_BUSCA:
        return [], "Apoio escala: omitido para terreno/lote."
    area_ref = _area_referencia_m2(leilao)
    if area_ref <= 0:
        return [], "Apoio escala: omitido (área de referência ausente)."

    tipos_principal = list(_TIPOS_RESIDENCIAIS_POOL) if tipo_l in _TIPOS_CASA_SOBRADO else [tipo_l]
    candidatos = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
        client,
        cidade=cidade,
        estado_sigla=estado_sigla,
        tipos_imovel=tipos_principal,
    )
    candidatos = _filtrar_candidatos_por_coerencia_url_cidade(candidatos, cidade)
    raios_busca = [float(raio)]
    for r in (10.0, 15.0):
        if r > float(raio) and r not in raios_busca:
            raios_busca.append(r)
    cand_por_id: dict[str, dict[str, Any]] = {}
    for r in raios_busca:
        lote = _filtrar_amostras(
            candidatos,
            lat0,
            lon0,
            area_ref=0.0,
            raio_km=float(r),
            aplicar_faixa_area_edital=False,
        )
        for a in lote:
            aid = str(a.get("id") or "").strip()
            if aid:
                cand_por_id.setdefault(aid, a)
    candidatos = list(cand_por_id.values())
    candidatos = _apos_filtro_geo_excluir_listagem_sinc_lance(candidatos, leilao, None)
    # Para caches auxiliares de escala, preserva diversidade/volume de faixa de área;
    # dedupe agressivo de "similares" costuma eliminar amostras úteis.
    candidatos = _sanear_amostras_para_cache(candidatos, None, deduplicar_similares=False)
    cap_p, cap_l = _caps_amostras_cache_mercado()
    min_apoio = max(1, int(CACHE_APOIO_ESCALA_MIN_AMOSTRAS))
    lim_apoio = max(min_apoio, min(cap_l, cap_p))

    out: list[dict[str, Any]] = []
    logs: list[str] = []
    base_ids = set(ids_excluir or set())
    men = _selecionar_amostras_apoio_escala(
        candidatos,
        area_ref=area_ref,
        fator_min=CACHE_APOIO_ESCALA_MENOR_MIN,
        fator_max=CACHE_APOIO_ESCALA_MENOR_MAX,
        ids_excluir=base_ids,
        limite=lim_apoio,
        min_amostras=min_apoio,
    )
    if not men:
        men = _selecionar_amostras_apoio_escala(
            candidatos,
            area_ref=area_ref,
            fator_min=max(0.30, CACHE_APOIO_ESCALA_MENOR_MIN - 0.20),
            fator_max=min(1.00, CACHE_APOIO_ESCALA_MENOR_MAX + 0.15),
            ids_excluir=base_ids,
            limite=lim_apoio,
            min_amostras=min_apoio,
        )
    c_men, err_men = _inserir_cache_apoio_escala(
        client,
        leilao,
        men,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_l=tipo_l,
        raio=raio,
        etiqueta="menor",
        fator_min=CACHE_APOIO_ESCALA_MENOR_MIN,
        fator_max=CACHE_APOIO_ESCALA_MENOR_MAX,
    )
    if err_men:
        return [], err_men
    if c_men:
        out.append(c_men)
        logs.append(f"Apoio escala menor: n={len(men)} faixa={CACHE_APOIO_ESCALA_MENOR_MIN:.2f}-{CACHE_APOIO_ESCALA_MENOR_MAX:.2f}x")
        for a in men:
            aid = str(a.get("id") or "").strip()
            if aid:
                base_ids.add(aid)

    mai = _selecionar_amostras_apoio_escala(
        candidatos,
        area_ref=area_ref,
        fator_min=CACHE_APOIO_ESCALA_MAIOR_MIN,
        fator_max=CACHE_APOIO_ESCALA_MAIOR_MAX,
        ids_excluir=base_ids,
        limite=lim_apoio,
        min_amostras=min_apoio,
    )
    if not mai:
        mai = _selecionar_amostras_apoio_escala(
            candidatos,
            area_ref=area_ref,
            fator_min=max(1.00, CACHE_APOIO_ESCALA_MAIOR_MIN - 0.05),
            fator_max=min(3.20, CACHE_APOIO_ESCALA_MAIOR_MAX + 0.90),
            ids_excluir=base_ids,
            limite=lim_apoio,
            min_amostras=min_apoio,
        )
    c_mai, err_mai = _inserir_cache_apoio_escala(
        client,
        leilao,
        mai,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_l=tipo_l,
        raio=raio,
        etiqueta="maior",
        fator_min=CACHE_APOIO_ESCALA_MAIOR_MIN,
        fator_max=CACHE_APOIO_ESCALA_MAIOR_MAX,
    )
    if err_mai:
        return [], err_mai
    if c_mai:
        out.append(c_mai)
        logs.append(f"Apoio escala maior: n={len(mai)} faixa={CACHE_APOIO_ESCALA_MAIOR_MIN:.2f}-{CACHE_APOIO_ESCALA_MAIOR_MAX:.2f}x")

    if not logs:
        logs.append("Apoio escala: sem amostra suficiente para criar cache auxiliar.")
    return out, " | ".join(logs)


def resolver_cache_media_pos_ingestao(
    client: Client,
    leilao_imovel_id: str,
    *,
    ignorar_cache_firecrawl: bool = False,
    raio_km: float | None = None,
    max_chamadas_api_firecrawl: int | None = None,
    frase_busca_firecrawl_override: str | None = None,
) -> ResultadoCriacaoCacheLeilao:
    """
    Fluxo automático pós-ingestão: tenta **reutilizar** linhas em ``cache_media_bairro`` no mesmo
    micro-geobucket + UF + cidade, validando amostras (raio / faixa de área) sobre os anúncios
    ainda presentes no BD; só depois cria segmentos em falta (com a mesma política económica de
    ``_montar_amostras_para_tipos``).

    ``max_chamadas_api_firecrawl``: saldo de chamadas (search + scrapes) para esta etapa; ``None``
    usa ``get_busca_mercado_parametros().max_firecrawl_creditos_analise``. Entre montagem principal
    e terrenos o saldo é decrementado.

    Em sucesso, **substitui** ``cache_media_bairro_ids`` do imóvel pela lista resolvida.
    """
    bp = get_busca_mercado_parametros()
    orcamento_fc = (
        int(bp.max_firecrawl_creditos_analise)
        if max_chamadas_api_firecrawl is None
        else max(0, int(max_chamadas_api_firecrawl))
    )
    raio = float(raio_km) if raio_km is not None and raio_km > 0 else float(bp.raio_km)
    min_n = bp.min_amostras_cache
    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        return ResultadoCriacaoCacheLeilao(False, "id do leilão inválido.", log_diagnostico="id do leilão vazio.")

    leilao = leilao_imoveis_repo.buscar_por_id(lid, client)
    if not leilao:
        return ResultadoCriacaoCacheLeilao(False, "Leilão não encontrado.", log_diagnostico=f"leilao_id={lid!r} não encontrado.")

    cidade = str(leilao.get("cidade") or "").strip()
    estado_raw = str(leilao.get("estado") or "").strip()
    bairro = str(leilao.get("bairro") or "").strip()
    if not cidade or not estado_raw:
        return ResultadoCriacaoCacheLeilao(
            False,
            "Leilão sem cidade ou estado — cache não criado.",
            log_diagnostico="cidade ou estado ausente no registro do leilão.",
        )

    coords = _coords_leilao(leilao)
    if not coords:
        return ResultadoCriacaoCacheLeilao(
            False,
            "Leilão sem latitude/longitude e geocodificação do endereço falhou — defina coordenadas ou endereço completo.",
            log_diagnostico="Sem coords no leilão e geocodificação do endereço falhou.",
        )
    lat0, lon0 = coords
    bairro_informado, bairro_ref = _garantir_bairro_canonico_leilao(
        client,
        leilao,
        lat0=float(lat0),
        lon0=float(lon0),
    )
    geo_bucket = geo_bucket_de_coords(lat0, lon0)
    area_ref = _area_referencia_m2(leilao)
    empreendimento_ref = _nome_empreendimento_leilao(leilao)
    tipo_l = str(normalizar_tipo_imovel(leilao.get("tipo_imovel")) or "desconhecido")
    if tipo_l == "casa" and (_leilao_indica_condominio(leilao) or empreendimento_ref):
        # Se o edital traz nome de condomínio/empreendimento, tratamos como casa_condominio.
        tipo_l = "casa_condominio"
    if tipo_l == "desconhecido":
        tipo_l = "apartamento"
    geo_first_enabled = _cache_geo_first_enabled(cidade)
    min_mesmo_bairro_reuso = max(0, int(min_n // 2)) if geo_first_enabled else max(0, int(min_n))

    estado_sigla = estado_livre_para_sigla_uf(estado_raw) or estado_raw[:2].upper()
    ctx_base = _contexto_log_cache(lid, cidade, estado_sigla, tipo_l, area_ref, min_n, raio)
    ctx_base += (
        f"\nbairro_informado={bairro_informado or '-'} | "
        f"bairro_referencia_cache={bairro_ref or '-'} | geo_first={'on' if geo_first_enabled else 'off'}"
    )

    candidatos = cache_media_bairro_repo.listar_candidatos_reuso(
        client,
        geo_bucket=geo_bucket,
        estado_sigla=estado_sigla,
        cidade=cidade,
    )
    candidatos_ordenados = _ordenar_candidatos_priorizando_mesmo_bairro(candidatos, bairro_ref)

    principal_id: Optional[str] = None
    principal_row: Optional[dict[str, Any]] = None
    for c in candidatos_ordenados:
        if not _cache_row_principal_para_tipo(c, tipo_l):
            continue
        if (
            _amostras_reuso_validas(
                client,
                c,
                lat0,
                lon0,
                area_ref,
                [tipo_l],
                raio_km=raio,
                leilao=leilao,
                bairro_referencia=bairro_ref,
                min_amostras_mesmo_bairro=min_mesmo_bairro_reuso,
                geo_first_enabled=geo_first_enabled,
                empreendimento_referencia=empreendimento_ref,
            )
            is not None
        ):
            principal_id = str(c.get("id") or "").strip() or None
            principal_row = c
            break

    terreno_id: Optional[str] = None
    terreno_row: Optional[dict[str, Any]] = None
    if tipo_l in _TIPOS_CASA_SOBRADO:
        for c in candidatos_ordenados:
            if not _cache_row_segmento_terreno(c):
                continue
            if (
                _amostras_reuso_validas(
                    client,
                    c,
                    lat0,
                    lon0,
                    area_ref,
                    list(_TIPOS_TERRENO_BUSCA),
                    raio_km=raio,
                    aplicar_faixa_area_edital=False,
                    leilao=leilao,
                    bairro_referencia=bairro_ref,
                    min_amostras_mesmo_bairro=min_mesmo_bairro_reuso,
                    geo_first_enabled=geo_first_enabled,
                    empreendimento_referencia=empreendimento_ref,
                )
                is not None
            ):
                terreno_id = str(c.get("id") or "").strip() or None
                terreno_row = c
                break

    reutil_principal = principal_id is not None
    reutil_terreno = terreno_id is not None
    diag_reuso_principal = (
        _diagnostico_reuso_bairro(principal_row, bairro) if isinstance(principal_row, dict) else ""
    )
    diag_reuso_terreno = (
        _diagnostico_reuso_bairro(terreno_row, bairro) if isinstance(terreno_row, dict) else ""
    )

    caches: list[dict[str, Any]] = []
    usou_fc = False
    n_fc_cache = 0
    diag_principal = ""
    diag_terrenos = ""
    diag_apoio_escala = ""
    ids_base_principal: set[str] = set()

    if principal_id and principal_row is not None:
        try:
            n_p = int(principal_row.get("n_amostras") or 0)
        except (TypeError, ValueError):
            n_p = 0
        caches.append(
            {
                "id": principal_id,
                "nome_cache": principal_row.get("nome_cache"),
                "n_amostras": n_p,
                "modo": "principal",
            }
        )
        for aid in _parse_csv_anuncio_ids(principal_row.get("anuncios_ids")):
            if aid:
                ids_base_principal.add(aid)
    else:
        tipos_principal = list(_TIPOS_RESIDENCIAIS_POOL) if tipo_l in _TIPOS_CASA_SOBRADO else [tipo_l]
        amostras, usou_vr, err, diag_principal, n_fc_pri = _montar_amostras_para_tipos(
            client,
            lat0,
            lon0,
            area_ref,
            tipos_principal,
            estado_sigla,
            cidade,
            bairro_ref,
            raio_km=raio,
            pode_geocode=True,
            pode_firecrawl_search=True,
            tipo_imovel_coleta=tipo_l,
            leilao_id=lid,
            estado_raw=estado_raw,
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
            max_chamadas_api_firecrawl=orcamento_fc,
            leilao=leilao,
            frase_busca_firecrawl_override=frase_busca_firecrawl_override,
        )
        if usou_vr:
            usou_fc = True
        n_fc_cache += int(n_fc_pri or 0)
        orcamento_fc = max(0, orcamento_fc - int(n_fc_pri or 0))
        if len(amostras) < CACHE_MONTE_MIN_EXIGIDO:
            return ResultadoCriacaoCacheLeilao(
                False,
                mensagem_com_dica_ajuste_busca(err or "Nenhuma amostra válida para montar cache (principal)."),
                usou_firecrawl_extra=usou_fc,
                firecrawl_chamadas_api=n_fc_cache,
                log_diagnostico=f"{ctx_base}\n--- Principal ---\n{diag_principal}",
            )
        criados_r, err_ins = _inserir_caches_residenciais_fatiados(
            client,
            leilao,
            amostras,
            lat0=lat0,
            lon0=lon0,
            geo_bucket=geo_bucket,
            tipo_l=tipo_l,
            raio=raio,
        )
        if err_ins:
            return ResultadoCriacaoCacheLeilao(
                False,
                mensagem_com_dica_ajuste_busca(err_ins),
                usou_firecrawl_extra=usou_fc,
                firecrawl_chamadas_api=n_fc_cache,
                log_diagnostico=f"{ctx_base}\n--- Principal ---\n{diag_principal}",
            )
        caches.extend(criados_r)
        for a in amostras:
            aid = str(a.get("id") or "").strip()
            if aid:
                ids_base_principal.add(aid)

    apoio_caches, diag_apoio = _criar_caches_apoio_escala(
        client,
        leilao,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_l=tipo_l,
        estado_sigla=estado_sigla,
        cidade=cidade,
        raio=raio,
        ids_excluir=ids_base_principal,
    )
    if apoio_caches:
        caches.extend(apoio_caches)
    diag_apoio_escala = f"--- Apoio escala ---\n{diag_apoio or 'sem detalhes'}"

    if tipo_l in _TIPOS_CASA_SOBRADO:
        if terreno_id and terreno_row is not None:
            try:
                n_t = int(terreno_row.get("n_amostras") or 0)
            except (TypeError, ValueError):
                n_t = 0
            caches.append(
                {
                    "id": terreno_id,
                    "nome_cache": terreno_row.get("nome_cache"),
                    "n_amostras": n_t,
                    "modo": "terrenos",
                }
            )
        else:
            amostras_t, usou_vr2, err_t, diag_terrenos, n_fc_ter = _montar_amostras_para_tipos(
                client,
                lat0,
                lon0,
                area_ref,
                list(_TIPOS_TERRENO_BUSCA),
                estado_sigla,
                cidade,
                bairro_ref,
                raio_km=raio,
                pode_geocode=True,
                pode_firecrawl_search=True,
                tipo_imovel_coleta="terreno",
                leilao_id=lid,
                estado_raw=estado_raw,
                ignorar_cache_firecrawl=ignorar_cache_firecrawl,
                max_chamadas_api_firecrawl=orcamento_fc,
                leilao=leilao,
                frase_busca_firecrawl_override=frase_busca_firecrawl_override,
            )
            if usou_vr2:
                usou_fc = True
            n_fc_cache += int(n_fc_ter or 0)
            amostras_t = _filtrar_amostras_so_terreno(amostras_t)
            if len(amostras_t) >= CACHE_MONTE_MIN_EXIGIDO:
                criados_t, err_t_ins = _inserir_caches_terrenos_fatiados(
                    client,
                    leilao,
                    amostras_t,
                    lat0=lat0,
                    lon0=lon0,
                    geo_bucket=geo_bucket,
                    raio=raio,
                )
                if err_t_ins:
                    return ResultadoCriacaoCacheLeilao(
                        False,
                        mensagem_com_dica_ajuste_busca(err_t_ins),
                        usou_firecrawl_extra=usou_fc,
                        firecrawl_chamadas_api=n_fc_cache,
                        log_diagnostico=f"{ctx_base}\n--- Terrenos ---\n{diag_terrenos}",
                    )
                caches.extend(criados_t)
            elif err_t:
                logger.info("Cache terrenos omitido (pós-ingestão): %s", err_t)

    novos = [str(c["id"]) for c in caches if c.get("id")]
    if not novos:
        return ResultadoCriacaoCacheLeilao(
            False,
            mensagem_com_dica_ajuste_busca("Nenhum segmento de cache aplicável ao imóvel."),
            usou_firecrawl_extra=usou_fc,
            reutilizou_existente=reutil_principal or reutil_terreno,
            firecrawl_chamadas_api=n_fc_cache,
            log_diagnostico=ctx_base,
        )

    leilao_imoveis_repo.definir_cache_media_bairro_ids(lid, novos, client)
    try:
        n_api_liq, analise_liq = _persistir_analise_liquidez_metragem_leilao(
            client,
            leilao=leilao,
            cache_ids=novos,
            lat0=lat0,
            lon0=lon0,
            raio_km=raio,
            tipo_l=tipo_l,
            estado_sigla=estado_sigla,
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
            max_chamadas_api_firecrawl=orcamento_fc,
        )
        n_fc_cache += int(n_api_liq or 0)
        log_ok_extra = (
            "Liquidez metragem: "
            f"fit={analise_liq.get('fit_metragem_score', 0)} "
            f"fit_multi={analise_liq.get('fit_multidimensional_score', 0)} "
            f"amostras={analise_liq.get('n_amostras_area', 0)} "
            f"alerta={bool(analise_liq.get('alerta_outlier_multidimensional', False) or analise_liq.get('alerta_outlier_metragem', False))}"
        )
    except Exception:
        logger.exception("Falha ao persistir análise de liquidez por metragem (pos_ingestao)")
        log_ok_extra = "Liquidez metragem: falha ao calcular/persistir."

    nseg = len(caches)
    msg = f"Aplicado(s) {nseg} segmento(s) de cache ao imóvel."
    log_ok: list[str] = [ctx_base]
    if reutil_principal:
        log_ok.append(
            f"Principal: reutilizado cache id={principal_id} | reuso_bairro={diag_reuso_principal or 'indefinido'}"
        )
    elif diag_principal:
        log_ok.append("--- Principal (montado) ---")
        log_ok.append(diag_principal)
    if tipo_l in _TIPOS_CASA_SOBRADO:
        if reutil_terreno:
            log_ok.append(
                f"Terrenos: reutilizado cache id={terreno_id} | reuso_bairro={diag_reuso_terreno or 'indefinido'}"
            )
        elif diag_terrenos.strip():
            log_ok.append("--- Terrenos ---")
            log_ok.append(diag_terrenos)
    if diag_apoio_escala.strip():
        log_ok.append(diag_apoio_escala)
    log_ok.append(log_ok_extra)
    log_diag_final = "\n".join(log_ok)
    _tentar_gravar_roi_pos_cache(client, lid)
    _tentar_gravar_precificacao_v2(client, lid)
    return ResultadoCriacaoCacheLeilao(
        True,
        msg,
        caches_criados=caches,
        usou_firecrawl_extra=usou_fc,
        reutilizou_existente=reutil_principal or reutil_terreno,
        firecrawl_chamadas_api=n_fc_cache,
        log_diagnostico=log_diag_final,
    )


def criar_caches_media_para_leilao(
    client: Client,
    leilao_imovel_id: str,
    *,
    ignorar_cache_firecrawl: bool = False,
    raio_km: float | None = None,
    max_chamadas_api_firecrawl: int | None = None,
    frase_busca_firecrawl_override: str | None = None,
) -> ResultadoCriacaoCacheLeilao:
    """
    Cria um ou dois caches (principal + opcional terrenos só a partir do já existente no BD).

    Até **duas** rodadas de complemento Firecrawl (principal e terrenos), cada uma no máximo uma
    pesquisa+scrapes conforme ``complementar_anuncios_firecrawl_search``. O teto global por chamada
    a esta função vem de ``max_chamadas_api_firecrawl`` (``None`` = parâmetro de busca) e é repartido
    entre as duas rodadas.
    """
    bp = get_busca_mercado_parametros()
    orcamento_fc = (
        int(bp.max_firecrawl_creditos_analise)
        if max_chamadas_api_firecrawl is None
        else max(0, int(max_chamadas_api_firecrawl))
    )
    raio = float(raio_km) if raio_km is not None and raio_km > 0 else float(bp.raio_km)
    min_n = bp.min_amostras_cache
    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        return ResultadoCriacaoCacheLeilao(False, "id do leilão inválido.", log_diagnostico="id do leilão vazio.")

    leilao = leilao_imoveis_repo.buscar_por_id(lid, client)
    if not leilao:
        return ResultadoCriacaoCacheLeilao(False, "Leilão não encontrado.", log_diagnostico=f"leilao_id={lid!r} não encontrado.")

    cidade = str(leilao.get("cidade") or "").strip()
    estado_raw = str(leilao.get("estado") or "").strip()
    bairro = str(leilao.get("bairro") or "").strip()
    if not cidade or not estado_raw:
        return ResultadoCriacaoCacheLeilao(
            False,
            "Leilão sem cidade ou estado — cache não criado.",
            log_diagnostico="cidade ou estado ausente no registro do leilão.",
        )

    coords = _coords_leilao(leilao)
    if not coords:
        return ResultadoCriacaoCacheLeilao(
            False,
            "Leilão sem latitude/longitude e geocodificação do endereço falhou — defina coordenadas ou endereço completo.",
            log_diagnostico="Sem coords no leilão e geocodificação do endereço falhou.",
        )
    lat0, lon0 = coords
    bairro_informado, bairro_ref = _garantir_bairro_canonico_leilao(
        client,
        leilao,
        lat0=float(lat0),
        lon0=float(lon0),
    )
    geo_bucket = geo_bucket_de_coords(lat0, lon0)
    area_ref = _area_referencia_m2(leilao)
    tipo_l = str(normalizar_tipo_imovel(leilao.get("tipo_imovel")) or "desconhecido")
    if tipo_l == "casa" and _leilao_indica_condominio(leilao):
        tipo_l = "casa_condominio"
    if tipo_l == "desconhecido":
        tipo_l = "apartamento"

    estado_sigla = estado_livre_para_sigla_uf(estado_raw) or estado_raw[:2].upper()
    ctx_base = _contexto_log_cache(lid, cidade, estado_sigla, tipo_l, area_ref, min_n, raio)
    ctx_base += (
        f"\nbairro_informado={bairro_informado or '-'} | "
        f"bairro_referencia_cache={bairro_ref or '-'} | "
        f"geo_first={'on' if _cache_geo_first_enabled(cidade) else 'off'}"
    )

    caches: list[dict[str, Any]] = []
    usou_fc = False
    n_fc_cache = 0
    diag_terrenos = ""
    diag_apoio_escala = ""
    ids_base_principal: set[str] = set()

    tipos_principal = list(_TIPOS_RESIDENCIAIS_POOL) if tipo_l in _TIPOS_CASA_SOBRADO else [tipo_l]
    amostras, usou_vr, err, diag_principal, n_fc_pri = _montar_amostras_para_tipos(
        client,
        lat0,
        lon0,
        area_ref,
        tipos_principal,
        estado_sigla,
        cidade,
        bairro_ref,
        raio_km=raio,
        pode_geocode=True,
        pode_firecrawl_search=True,
        tipo_imovel_coleta=tipo_l,
        leilao_id=lid,
        estado_raw=estado_raw,
        ignorar_cache_firecrawl=ignorar_cache_firecrawl,
        max_chamadas_api_firecrawl=orcamento_fc,
        leilao=leilao,
        frase_busca_firecrawl_override=frase_busca_firecrawl_override,
    )
    if usou_vr:
        usou_fc = True
    n_fc_cache += int(n_fc_pri or 0)
    orcamento_fc = max(0, orcamento_fc - int(n_fc_pri or 0))
    if len(amostras) < CACHE_MONTE_MIN_EXIGIDO:
        return ResultadoCriacaoCacheLeilao(
            False,
            mensagem_com_dica_ajuste_busca(err or "Nenhuma amostra válida para montar cache."),
            usou_firecrawl_extra=usou_fc,
            firecrawl_chamadas_api=n_fc_cache,
            log_diagnostico=f"{ctx_base}\n--- Principal ---\n{diag_principal}",
        )

    criados_r, err_ins = _inserir_caches_residenciais_fatiados(
        client,
        leilao,
        amostras,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_l=tipo_l,
        raio=raio,
    )
    if err_ins:
        return ResultadoCriacaoCacheLeilao(
            False,
            mensagem_com_dica_ajuste_busca(err_ins),
            usou_firecrawl_extra=usou_fc,
            firecrawl_chamadas_api=n_fc_cache,
            log_diagnostico=f"{ctx_base}\n--- Principal ---\n{diag_principal}",
        )
    caches.extend(criados_r)
    for a in amostras:
        aid = str(a.get("id") or "").strip()
        if aid:
            ids_base_principal.add(aid)

    apoio_caches, diag_apoio = _criar_caches_apoio_escala(
        client,
        leilao,
        lat0=lat0,
        lon0=lon0,
        geo_bucket=geo_bucket,
        tipo_l=tipo_l,
        estado_sigla=estado_sigla,
        cidade=cidade,
        raio=raio,
        ids_excluir=ids_base_principal,
    )
    if apoio_caches:
        caches.extend(apoio_caches)
    diag_apoio_escala = diag_apoio

    if tipo_l in _TIPOS_CASA_SOBRADO:
        amostras_t, usou_vr2, err_t, diag_terrenos, n_fc_ter = _montar_amostras_para_tipos(
            client,
            lat0,
            lon0,
            area_ref,
            list(_TIPOS_TERRENO_BUSCA),
            estado_sigla,
            cidade,
            bairro_ref,
            raio_km=raio,
            pode_geocode=True,
            pode_firecrawl_search=True,
            tipo_imovel_coleta="terreno",
            leilao_id=lid,
            estado_raw=estado_raw,
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
            max_chamadas_api_firecrawl=orcamento_fc,
            leilao=leilao,
            frase_busca_firecrawl_override=frase_busca_firecrawl_override,
        )
        if usou_vr2:
            usou_fc = True
        n_fc_cache += int(n_fc_ter or 0)
        amostras_t = _filtrar_amostras_so_terreno(amostras_t)
        if len(amostras_t) >= CACHE_MONTE_MIN_EXIGIDO:
            criados_t, err_t_ins = _inserir_caches_terrenos_fatiados(
                client,
                leilao,
                amostras_t,
                lat0=lat0,
                lon0=lon0,
                geo_bucket=geo_bucket,
                raio=raio,
            )
            if err_t_ins:
                return ResultadoCriacaoCacheLeilao(
                    False,
                    mensagem_com_dica_ajuste_busca(err_t_ins),
                    usou_firecrawl_extra=usou_fc,
                    firecrawl_chamadas_api=n_fc_cache,
                    log_diagnostico=f"{ctx_base}\n--- Terrenos ---\n{diag_terrenos}",
                )
            caches.extend(criados_t)
        elif err_t:
            logger.info("Cache terrenos omitido: %s", err_t)

    novos = [c["id"] for c in caches]
    leilao_imoveis_repo.anexar_cache_media_bairro_ids(lid, novos, client)
    try:
        n_api_liq, analise_liq = _persistir_analise_liquidez_metragem_leilao(
            client,
            leilao=leilao,
            cache_ids=novos,
            lat0=lat0,
            lon0=lon0,
            raio_km=raio,
            tipo_l=tipo_l,
            estado_sigla=estado_sigla,
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
            max_chamadas_api_firecrawl=orcamento_fc,
        )
        n_fc_cache += int(n_api_liq or 0)
        liq_line = (
            "Liquidez metragem: "
            f"fit={analise_liq.get('fit_metragem_score', 0)} "
            f"fit_multi={analise_liq.get('fit_multidimensional_score', 0)} "
            f"amostras={analise_liq.get('n_amostras_area', 0)} "
            f"alerta={bool(analise_liq.get('alerta_outlier_multidimensional', False) or analise_liq.get('alerta_outlier_metragem', False))}"
        )
    except Exception:
        logger.exception("Falha ao persistir análise de liquidez por metragem")
        liq_line = "Liquidez metragem: falha ao calcular/persistir."

    log_ok: list[str] = [ctx_base, "--- Principal ---", diag_principal]
    if diag_apoio_escala.strip():
        log_ok.append("--- Apoio escala ---")
        log_ok.append(diag_apoio_escala)
    if tipo_l in _TIPOS_CASA_SOBRADO and diag_terrenos.strip():
        log_ok.append("--- Terrenos ---")
        log_ok.append(diag_terrenos)
    log_ok.append(liq_line)

    _tentar_gravar_roi_pos_cache(client, lid)
    _tentar_gravar_precificacao_v2(client, lid)
    return ResultadoCriacaoCacheLeilao(
        True,
        f"Criado(s) {len(caches)} cache(s).",
        caches_criados=caches,
        usou_firecrawl_extra=usou_fc,
        firecrawl_chamadas_api=n_fc_cache,
        log_diagnostico="\n".join(log_ok),
    )


def _tentar_gravar_roi_pos_cache(client: Client, leilao_imovel_id: str) -> None:
    try:
        from leilao_ia_v2.services.roi_pos_cache_leilao import estimar_e_gravar_roi_pos_cache

        r = estimar_e_gravar_roi_pos_cache(client, leilao_imovel_id)
        if r.ok:
            logger.info("ROI pós-cache: %s leilao=%s", r.motivo, str(leilao_imovel_id)[:12])
        else:
            logger.info("ROI pós-cache: ignorado (%s) leilao=%s", r.motivo, str(leilao_imovel_id)[:12])
    except Exception:
        logger.exception("ROI pós-cache falhou (leilao_id=%s)", str(leilao_imovel_id)[:12])


def _tentar_gravar_precificacao_v2(client: Client, leilao_imovel_id: str) -> None:
    """Hook do motor de precificação v2 (Sprint 2/3).

    Roda em paralelo ao ``_tentar_gravar_roi_pos_cache`` legado: o motor
    novo lê amostras de ``anuncios_mercado`` (não do cache agregado) e
    grava o snapshot em ``leilao_extra_json.precificacao_v2``. Isolado em
    try/except — uma falha aqui **não** interrompe a ingestão.
    """
    try:
        from leilao_ia_v2.precificacao.integracao import precificar_leilao

        r = precificar_leilao(client, leilao_imovel_id)
        if r.ok:
            logger.info("Precificação v2: %s leilao=%s", r.motivo, str(leilao_imovel_id)[:12])
        else:
            logger.info(
                "Precificação v2: ignorada (%s) leilao=%s",
                r.motivo, str(leilao_imovel_id)[:12],
            )
    except Exception:
        logger.exception("Precificação v2 falhou (leilao_id=%s)", str(leilao_imovel_id)[:12])


def recalcular_caches_mercado_para_leilao(
    client: Client,
    leilao_imovel_id: str,
    *,
    apagar_caches_sem_outro_vinculo: bool = True,
    ignorar_cache_firecrawl: bool = False,
    raio_km: float | None = None,
    max_chamadas_api_firecrawl: int | None = None,
    frase_busca_firecrawl_override: str | None = None,
) -> ResultadoCriacaoCacheLeilao:
    """
    Recálculo: remove todos os UUIDs de ``cache_media_bairro`` do imóvel, depois cria novos caches
    (a partir de ``anuncios_mercado`` + Firecrawl conforme parâmetros) e vincula ao leilão.

    **Partilha de cache:** o mesmo registo de ``cache_media_bairro`` pode constar no array de
    **vários** leilões. Com ``apagar_caches_sem_outro_vinculo=True`` (padrão), após desvincular
    o imóvel as linhas de cache deixam de ser apagadas se ainda forem usadas por outro leilão.
    Só se removem linhas a que nenhum leilão volte a referenciar.
    """
    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        return ResultadoCriacaoCacheLeilao(
            False,
            "id do leilão inválido.",
            log_diagnostico="id do leilão vazio.",
        )
    row = leilao_imoveis_repo.buscar_por_id(lid, client)
    if not row:
        return ResultadoCriacaoCacheLeilao(
            False,
            "Leilão não encontrado.",
            log_diagnostico=f"leilao_id={lid!r} não encontrado.",
        )
    anteriores: list[str] = []
    seen: set[str] = set()
    for x in row.get("cache_media_bairro_ids") or []:
        s = str(x).strip()
        if s and s not in seen:
            anteriores.append(s)
            seen.add(s)
    leilao_imoveis_repo.definir_cache_media_bairro_ids(lid, [], client)

    orfas_apagadas: list[str] = []
    if apagar_caches_sem_outro_vinculo and anteriores:
        for oid in anteriores:
            refs = leilao_imoveis_repo.listar_ids_leilao_que_incluem_cache_id(oid, client)
            if not refs:
                try:
                    cache_media_bairro_repo.apagar_por_id(client, oid)
                    orfas_apagadas.append(oid)
                except Exception as exc:
                    logger.warning("Não foi possível apagar cache orfã id=%s: %s", oid, exc)
    res = criar_caches_media_para_leilao(
        client,
        lid,
        ignorar_cache_firecrawl=ignorar_cache_firecrawl,
        raio_km=raio_km,
        max_chamadas_api_firecrawl=max_chamadas_api_firecrawl,
        frase_busca_firecrawl_override=frase_busca_firecrawl_override,
    )
    if not anteriores and not orfas_apagadas:
        return res
    bloco: list[str] = [
        f"--- Recálculo (antes: {len(anteriores)} id(s) desvinculado(s) deste imóvel) ---",
    ]
    if orfas_apagadas:
        bloco.append(
            f"Linhas removidas de cache_media_bairro (sem outro leilão a referenciar): {len(orfas_apagadas)}"
        )
        for j, oid in enumerate(orfas_apagadas[:12], start=1):
            bloco.append(f"  {j}. {oid}")
        if len(orfas_apagadas) > 12:
            bloco.append(f"  … (+{len(orfas_apagadas) - 12} id(s))")
    elif anteriores:
        bloco.append(
            "Nenhuma linha de cache apagada: um ou mais IDs ainda vinculados a outro(s) leilão(ões)."
        )
    extra = "\n" + "\n".join(bloco)
    if orfas_apagadas:
        msg_final = f"Removidas {len(orfas_apagadas)} linha(s) de cache sem outro leilão a referenciar. {res.mensagem}"
    else:
        msg_final = res.mensagem

    return ResultadoCriacaoCacheLeilao(
        res.ok,
        msg_final,
        caches_criados=res.caches_criados,
        usou_firecrawl_extra=res.usou_firecrawl_extra,
        reutilizou_existente=res.reutilizou_existente,
        firecrawl_chamadas_api=res.firecrawl_chamadas_api,
        log_diagnostico=(res.log_diagnostico or "") + extra,
    )
