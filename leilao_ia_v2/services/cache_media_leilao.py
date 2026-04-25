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
import os
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
from leilao_ia_v2.services.geocoding import geocodificar_anuncios_batch, geocodificar_endereco
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

# Política de composição dos caches gravados (UI lê ``volume_amostras_baixo`` em ``metadados_json``).
CACHE_MONTE_MIN_EXIGIDO = 1
CACHE_VOLUME_BAIXO_LIMITE = 5
# Padrões quando não há contexto Streamlit (testes, scripts). A UI usa ``get_busca_mercado_parametros()``.
CACHE_AMOSTRAS_PRINCIPAL_MAX = 10
CACHE_AMOSTRAS_LOTE_REFERENCIA = 10


def _float_positivo(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x > 0 else None


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
) -> list[dict[str, Any]]:
    bp = get_busca_mercado_parametros()
    scored: list[tuple[dict[str, Any], float, float]] = []
    for r in candidatos:
        la, lo = coords_de_anuncio(r)
        if la is None or lo is None:
            continue
        d = haversine_km(lat0, lon0, float(la), float(lo))
        if d > float(raio_km):
            continue
        try:
            am = float(r.get("area_construida_m2") or 0)
        except (TypeError, ValueError):
            continue
        if am <= 0:
            continue
        if aplicar_faixa_area_edital and area_ref > 0:
            lo_a, hi_a = bp.area_fator_min * area_ref, bp.area_fator_max * area_ref
            if not (lo_a <= am <= hi_a):
                continue
        delta_a = abs(am - area_ref) if area_ref > 0 and aplicar_faixa_area_edital else 0.0
        scored.append((r, d, delta_a))
    scored.sort(key=lambda x: (x[1], x[2]))
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
) -> str:
    """Contagens alinhadas à ordem de exclusão de ``_filtrar_amostras`` (para log em ``ultima_ingestao_log_text``)."""
    bp = get_busca_mercado_parametros()
    n_total = len(candidatos)
    n_sem_coord = 0
    n_fora_raio = 0
    n_area_parse_err = 0
    n_area_zero = 0
    n_fora_faixa_area = 0
    dists: list[float] = []
    dists_fora_raio: list[float] = []
    lo_a = hi_a = None
    if area_ref > 0:
        lo_a, hi_a = bp.area_fator_min * area_ref, bp.area_fator_max * area_ref

    for r in candidatos:
        la, lo = coords_de_anuncio(r)
        if la is None or lo is None:
            n_sem_coord += 1
            continue
        d = haversine_km(lat0, lon0, float(la), float(lo))
        dists.append(d)
        if d > float(raio_km):
            n_fora_raio += 1
            dists_fora_raio.append(d)
            continue
        try:
            am = float(r.get("area_construida_m2") or 0)
        except (TypeError, ValueError):
            n_area_parse_err += 1
            continue
        if am <= 0:
            n_area_zero += 1
            continue
        if aplicar_faixa_area_edital and area_ref > 0 and lo_a is not None and hi_a is not None:
            if not (lo_a <= am <= hi_a):
                n_fora_faixa_area += 1
                continue

    n_pass = len(
        _filtrar_amostras(
            candidatos,
            lat0,
            lon0,
            area_ref,
            raio_km=raio_km,
            aplicar_faixa_area_edital=aplicar_faixa_area_edital,
        )
    )
    d_min = min(dists) if dists else None
    d_max = max(dists) if dists else None
    dr_min = min(dists_fora_raio) if dists_fora_raio else None
    dr_max = max(dists_fora_raio) if dists_fora_raio else None

    lines = [
        f"{etiqueta}: total_bd={n_total} | passam_filtro={n_pass}",
        (
            f"  excluídos: sem_coord={n_sem_coord} | distância>{raio_km}km={n_fora_raio} | "
            f"área_parse_erro={n_area_parse_err} | área<=0={n_area_zero} | fora_faixa_m²={n_fora_faixa_area}"
        ),
    ]
    if not aplicar_faixa_area_edital:
        lines.append("  faixa_m²: desativada (segmento terreno/lote — só raio e área>0)")
    elif area_ref > 0 and lo_a is not None and hi_a is not None:
        lines.append(
            f"  faixa_m²_ref={area_ref:.1f} aceita [{lo_a:.1f}, {hi_a:.1f}] "
            f"(fatores {bp.area_fator_min}..{bp.area_fator_max})"
        )
    else:
        lines.append("  faixa_m²: sem filtro por área (ref<=0 ou inválida)")
    if dists:
        lines.append(f"  dist_km (anúncios com coord): min={d_min:.2f} max={d_max:.2f}")
    if dists_fora_raio:
        lines.append(f"  dist_km (só excluídos por raio): min={dr_min:.2f} max={dr_max:.2f}")
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
    leilao_id: str,
    ignorar_cache_firecrawl: bool,
    max_chamadas_api: int | None = None,
    frase_busca_override: str | None = None,
) -> tuple[int, int]:
    """Devolve ``(n_anuncios_gravados, n_chamadas_api_estimadas)``."""
    if not os.getenv("FIRECRAWL_API_KEY", "").strip():
        return 0, 0
    try:
        from leilao_ia_v2.fc_search.pipeline import complementar_anuncios_firecrawl_search

        n_fc, _diag, n_api = complementar_anuncios_firecrawl_search(
            client,
            leilao_imovel_id=str(leilao_id),
            cidade=cidade,
            estado_raw=estado_raw,
            bairro=bairro,
            tipo_imovel=tipo_imovel,
            area_ref=float(area_ref or 0),
            ignorar_cache_firecrawl=ignorar_cache_firecrawl,
            max_chamadas_api=max_chamadas_api,
            frase_busca_override=frase_busca_override,
        )
        return int(n_fc or 0), int(n_api or 0)
    except Exception:
        logger.exception("Complemento anúncios via Firecrawl Search (cache de média)")
        return 0, 0


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
) -> Optional[list[dict[str, Any]]]:
    ids = _parse_csv_anuncio_ids(cache_row.get("anuncios_ids"))
    if not ids:
        return None
    ads = anuncios_mercado_repo.buscar_por_ids(client, ids)
    if not ads:
        return None
    tset = {t.strip().lower() for t in tipos_anuncio if str(t).strip()}
    if tset:
        ads_f = [a for a in ads if str(a.get("tipo_imovel") or "").strip().lower() in tset]
        if not ads_f:
            ads_f = ads
    else:
        ads_f = ads
    amostras = _filtrar_amostras(
        ads_f,
        lat0,
        lon0,
        area_ref,
        raio_km=raio_km,
        aplicar_faixa_area_edital=aplicar_faixa_area_edital,
    )
    amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, None)
    if int(min_amostras_mesmo_bairro or 0) > 0:
        n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro_referencia)
        if n_mesmo_bairro < int(min_amostras_mesmo_bairro):
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
    aplicar_faixa = not _tipos_somente_terreno_ou_lote(tipos)
    msg = ""
    tipos_txt = ",".join(str(t).strip() for t in tipos if str(t).strip()) or tipo_imovel_coleta
    linhas: list[str] = [
        f"Montagem amostras: tipos_busca=[{tipos_txt}] tipo_coleta_fc={tipo_imovel_coleta} "
        f"(mínimo exigido={min_n}; BD antes de Firecrawl)"
    ]
    candidatos = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
        client,
        cidade=cidade,
        estado_sigla=estado_sigla,
        tipos_imovel=tipos,
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
        )
    )
    amostras = _filtrar_amostras(
        candidatos, lat0, lon0, area_ref, raio_km=raio_km, aplicar_faixa_area_edital=aplicar_faixa
    )
    amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
    n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
    linhas.append(f"bairro_alvo: '{bairro or '-'}' | amostras_mesmo_bairro={n_mesmo_bairro} (mínimo={min_mesmo_bairro})")

    if len(amostras) >= min_n and n_mesmo_bairro >= min_mesmo_bairro:
        linhas.append("Resultado: amostras suficientes só com dados já no BD (sem geocode nem Firecrawl).")
        return amostras, False, msg, "\n".join(linhas), 0

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
        linhas.append(
            _diagnostico_filtro_amostras(
                candidatos,
                lat0,
                lon0,
                area_ref,
                raio_km=raio_km,
                etiqueta="Após geocodificar anúncios sem coord e re-query BD",
                aplicar_faixa_area_edital=aplicar_faixa,
            )
        )
        amostras = _filtrar_amostras(
            candidatos, lat0, lon0, area_ref, raio_km=raio_km, aplicar_faixa_area_edital=aplicar_faixa
        )
        amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
        n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
        linhas.append(
            f"após_geocode: amostras_mesmo_bairro={n_mesmo_bairro} (mínimo={min_mesmo_bairro})"
        )
        if len(amostras) >= min_n and n_mesmo_bairro >= min_mesmo_bairro:
            linhas.append("Resultado: amostras suficientes após geocode (sem Firecrawl).")
            return amostras, False, msg, "\n".join(linhas), 0

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
        linhas.append(
            _diagnostico_filtro_amostras(
                candidatos,
                lat0,
                lon0,
                area_ref,
                raio_km=raio_km,
                etiqueta="Após Firecrawl Search e re-query BD",
                aplicar_faixa_area_edital=aplicar_faixa,
            )
        )
        amostras = _filtrar_amostras(
            candidatos, lat0, lon0, area_ref, raio_km=raio_km, aplicar_faixa_area_edital=aplicar_faixa
        )
        amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
        n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
        linhas.append(
            f"após_firecrawl: amostras_mesmo_bairro={n_mesmo_bairro} (mínimo={min_mesmo_bairro})"
        )
        if len(amostras) >= min_n and n_mesmo_bairro >= min_mesmo_bairro:
            linhas.append("Resultado: amostras suficientes após Firecrawl Search.")
            return amostras, True, msg, "\n".join(linhas), n_api_fc

    elif pode_firecrawl_search and max_chamadas_api_firecrawl is not None and int(max_chamadas_api_firecrawl) <= 0:
        linhas.append("Firecrawl Search: omitido (orçamento de chamadas API esgotado para esta rodada).")

    amostras = _apos_filtro_geo_excluir_listagem_sinc_lance(amostras, leilao, linhas)
    n_mesmo_bairro = _contar_amostras_mesmo_bairro(amostras, bairro)
    if len(amostras) >= CACHE_MONTE_MIN_EXIGIDO:
        linhas.append(
            f"Resultado: {len(amostras)} amostra(s) após todas as etapas (mínimo configurado p/ coleta={min_n}; "
            "montagem de cache permitida com volume reduzido se <5 amostras no segmento)."
        )
        if n_mesmo_bairro < min_mesmo_bairro:
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
    amostras_ordenadas = _ordenar_amostras_priorizando_mesmo_bairro(
        amostras,
        str(leilao.get("bairro") or ""),
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
    geo_bucket = geo_bucket_de_coords(lat0, lon0)
    area_ref = _area_referencia_m2(leilao)
    tipo_l = str(normalizar_tipo_imovel(leilao.get("tipo_imovel")) or "desconhecido")
    if tipo_l == "desconhecido":
        tipo_l = "apartamento"

    estado_sigla = estado_livre_para_sigla_uf(estado_raw) or estado_raw[:2].upper()
    ctx_base = _contexto_log_cache(lid, cidade, estado_sigla, tipo_l, area_ref, min_n, raio)

    candidatos = cache_media_bairro_repo.listar_candidatos_reuso(
        client,
        geo_bucket=geo_bucket,
        estado_sigla=estado_sigla,
        cidade=cidade,
    )
    candidatos_ordenados = _ordenar_candidatos_priorizando_mesmo_bairro(candidatos, bairro)

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
                bairro_referencia=bairro,
                min_amostras_mesmo_bairro=min_n,
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
                    bairro_referencia=bairro,
                    min_amostras_mesmo_bairro=min_n,
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
            bairro,
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
                bairro,
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
    log_diag_final = "\n".join(log_ok)
    _tentar_gravar_roi_pos_cache(client, lid)
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
    geo_bucket = geo_bucket_de_coords(lat0, lon0)
    area_ref = _area_referencia_m2(leilao)
    tipo_l = str(normalizar_tipo_imovel(leilao.get("tipo_imovel")) or "desconhecido")
    if tipo_l == "desconhecido":
        tipo_l = "apartamento"

    estado_sigla = estado_livre_para_sigla_uf(estado_raw) or estado_raw[:2].upper()
    ctx_base = _contexto_log_cache(lid, cidade, estado_sigla, tipo_l, area_ref, min_n, raio)

    caches: list[dict[str, Any]] = []
    usou_fc = False
    n_fc_cache = 0
    diag_terrenos = ""

    tipos_principal = list(_TIPOS_RESIDENCIAIS_POOL) if tipo_l in _TIPOS_CASA_SOBRADO else [tipo_l]
    amostras, usou_vr, err, diag_principal, n_fc_pri = _montar_amostras_para_tipos(
        client,
        lat0,
        lon0,
        area_ref,
        tipos_principal,
        estado_sigla,
        cidade,
        bairro,
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

    if tipo_l in _TIPOS_CASA_SOBRADO:
        amostras_t, usou_vr2, err_t, diag_terrenos, n_fc_ter = _montar_amostras_para_tipos(
            client,
            lat0,
            lon0,
            area_ref,
            list(_TIPOS_TERRENO_BUSCA),
            estado_sigla,
            cidade,
            bairro,
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

    log_ok: list[str] = [ctx_base, "--- Principal ---", diag_principal]
    if tipo_l in _TIPOS_CASA_SOBRADO and diag_terrenos.strip():
        log_ok.append("--- Terrenos ---")
        log_ok.append(diag_terrenos)

    _tentar_gravar_roi_pos_cache(client, lid)
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
