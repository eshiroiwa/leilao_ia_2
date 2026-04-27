"""Persistência de cards extraídos de portais (markdown) em ``anuncios_mercado``."""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from typing import Any
from urllib.parse import urlparse

from supabase import Client

from leilao_ia_v2.normalizacao import normalizar_tipo_casa
from leilao_ia_v2.persistence import anuncios_mercado_repo
from leilao_ia_v2.services.geo_medicao import haversine_km
from leilao_ia_v2.services.exclusao_cache_listagem_leilao import (
    anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance,
)


def _titulo_sugere_terreno_ou_lote(titulo: Any) -> bool:
    s = str(titulo or "").strip().lower()
    if not s:
        return False
    chaves = (
        "terreno",
        "terrenos",
        " lote ",
        " lote,",
        "lote ",
        "gleba",
        "área de ",
        "area de ",
        "loteamento",
        "lotes ",
    )
    return any(k in s for k in chaves)


def _tipo_detectado_por_titulo(titulo: Any) -> str:
    s = str(titulo or "").strip().lower()
    if not s:
        return ""
    if _titulo_sugere_terreno_ou_lote(s):
        return "terreno"
    if "apartamento" in s or "apto" in s:
        return "apartamento"
    if "sobrado" in s:
        return "sobrado"
    if "casa de condominio" in s or "casa em condominio" in s or "condom" in s:
        return "casa_condominio"
    if "casa" in s:
        return "casa"
    return ""


def _leilao_indica_condominio(leilao_row: dict[str, Any] | None) -> bool:
    if not isinstance(leilao_row, dict):
        return False
    extra = leilao_row.get("leilao_extra_json")
    if not isinstance(extra, dict):
        extra = {}
    txt = " ".join(
        str(x or "")
        for x in (
            leilao_row.get("descricao"),
            extra.get("observacoes_markdown"),
            extra.get("nome_condominio"),
            extra.get("condominio"),
            extra.get("nome_empreendimento"),
            extra.get("empreendimento"),
        )
    )
    t = txt.lower()
    if any(
        k in t
        for k in (
            "regras para pagamento",
            "sob responsabilidade do comprador",
            "a caixa realizará o pagamento",
            "limite de 10%",
            "tributos",
        )
    ):
        return False
    if re.search(r"(?i)\bcondom[ií]nio\s*:", t):
        return False
    if extra.get("nome_condominio") or extra.get("condominio") or extra.get("nome_empreendimento"):
        return True
    return bool(
        re.search(r"(?i)\bcasa\s+em\s+condom[ií]nio\b", t)
        or re.search(r"(?i)\bcondom[ií]nio\s+(residencial|fechado|[a-z0-9])\b", t)
    )


def _segmento_mercado_residencial(card: dict[str, Any], tipo_final: str) -> str:
    tit = str(card.get("titulo") or "").lower()
    if tipo_final in ("terreno", "lote"):
        return tipo_final
    if tipo_final == "casa_condominio":
        return "casa_condominio"
    if tipo_final == "sobrado":
        return "sobrado"
    if tipo_final == "casa":
        if "condom" in tit or "condomínio" in tit or "condominio" in tit:
            return "casa_condominio"
        tc = normalizar_tipo_casa(card.get("titulo"), "casa")
        if tc == "terrea":
            return "terrea"
        if tc == "sobrado":
            return "sobrado"
        return "casa_generica"
    return "outros"


def _titulo_card_invalido(titulo: Any) -> bool:
    s = str(titulo or "").strip()
    if not s:
        return True
    s_low = s.lower()
    if s_low.startswith("mensagem]("):
        return True
    if s_low in {"mensagem", "contatar", "ver telefone"}:
        return True
    return False


def _titulo_fallback_por_url(url: str) -> str:
    try:
        seg = (urlparse(str(url or "")).path or "").strip("/").split("/")[-1]
    except Exception:
        return ""
    s = seg.replace("-", " ").strip()
    s = re.sub(r"\b(id|m2|rs)\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) < 10:
        return ""
    return s[:220]


def _bairro_inferido_da_url(url: str, cidade_alvo: str) -> str:
    u = str(url or "").strip().lower()
    if not u:
        return ""
    cidade = _slug_fold(cidade_alvo)
    if not cidade:
        return ""
    try:
        path = (urlparse(u).path or "").lower()
    except Exception:
        path = u
    toks = [t for t in re.split(r"[^a-z0-9]+", path) if t]
    if not toks:
        return ""
    stop = {
        "casa",
        "apartamento",
        "sobrado",
        "imovel",
        "imoveis",
        "venda",
        "aluguel",
        "quartos",
        "quarto",
        "com",
        "sem",
        "suite",
        "suites",
        "banheiros",
        "banheiro",
        "vaga",
        "vagas",
        "id",
        "m2",
        "sp",
        "academia",
        "piscina",
        "churrasqueira",
        "mobiliado",
        "condicionado",
        "ar",
        "vista",
        "quintal",
        "suite",
    }
    bairro_hint = {"jardim", "jd", "vila", "parque", "centro", "chacara", "bosque", "residencial", "condominio"}
    # bairro imediatamente antes da cidade no slug.
    out: list[str] = []
    for i, t in enumerate(toks):
        if t != cidade:
            continue
        j = i - 1
        cand: list[str] = []
        while j >= 0 and len(cand) < 5:
            tj = toks[j]
            if tj in stop or any(ch.isdigit() for ch in tj):
                break
            cand.append(tj)
            j -= 1
        cand.reverse()
        if len(cand) >= 1:
            out = cand
            break
    if not out:
        return ""
    b = " ".join(out).strip()
    if len(b) < 4:
        return ""
    if not any(tok in bairro_hint for tok in out):
        return ""
    return b.title()[:90]


def _sanear_bairro_anuncio(v: Any) -> str:
    s = " ".join(str(v or "").strip().split())
    if not s:
        return ""
    s = re.sub(r"(?i)\b\d{2,5}\s*m2\b", "", s).strip()
    s = re.sub(r"(?i)\b(id|rs)\s*\d+\b", "", s).strip(" -_,;")
    s = re.sub(r"\s{2,}", " ", s).strip()
    if not s:
        return ""
    bad = {"academia", "piscina", "churrasqueira", "mobiliado", "condicionado", "escritorio"}
    toks = {_slug_fold(x) for x in s.split()}
    if toks & bad:
        return ""
    return s[:90]


def _bairro_inferido_do_titulo(titulo: Any, cidade_alvo: str) -> str:
    t = _slug_fold(titulo)
    c = _slug_fold(cidade_alvo)
    if not t or not c:
        return ""
    # Ex.: "... parque-sao-cristovao-taubate ..."
    m = re.search(
        rf"((?:jardim|jd|vila|parque|centro|chacara|bosque|residencial|condominio)-[a-z0-9-]{{2,60}})-{c}\b",
        t,
    )
    if not m:
        return ""
    b = str(m.group(1) or "").strip("-")
    if not b:
        return ""
    return b.replace("-", " ").title()[:90]


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


def _slug_fold(s: Any) -> str:
    raw = str(s or "").strip().lower()
    if not raw:
        return ""
    txt = "".join(
        c for c in unicodedata.normalize("NFD", raw) if unicodedata.category(c) != "Mn"
    )
    txt = re.sub(r"[^a-z0-9]+", "-", txt).strip("-")
    txt = re.sub(r"-{2,}", "-", txt)
    return txt


def _cidade_inferida_da_url(url: str) -> str:
    """
    Heurística conservadora:
    - padrão com UF na URL (ex.: ``...-sp-franca-...``);
    - padrão de ficha do Viva Real próximo ao trecho ``m2-venda/aluguel``.
    """
    u = str(url or "").strip().lower()
    if not u:
        return ""

    uf_pat = "|".join(_UF_SLUGS)
    stop_tokens = {"m2", "id", "ids", "rs", "venda", "aluguel", "comprar", "alugar"}

    def _token_cidade(v: str) -> str:
        s = _slug_fold(v)
        if not s or any(ch.isdigit() for ch in s):
            return ""
        if s in stop_tokens:
            return ""
        return s

    # Padrão comum Chaves/Zap: ``...-sp-taubate-estiva-88m2-...``.
    m_uf = re.search(
        rf"-(?:{uf_pat})-(?P<cauda>[a-z0-9-]{{3,120}}?)-(?:(?:\d{{2,5}}m2)|(?:id-)|(?:rs\d))",
        u,
        flags=re.IGNORECASE,
    )
    if m_uf:
        toks = [t for t in str(m_uf.group("cauda") or "").split("-") if t]
        if toks:
            t0 = _token_cidade(toks[0])
            if t0:
                return t0

    # Padrão Viva/Zap: ``...-bairro-taubate-sp-88m2-id-...``.
    m_antes_uf = re.search(
        rf"-(?P<cidade>[a-z0-9]{{3,40}})-(?:{uf_pat})-(?:\d{{2,5}}m2)",
        u,
        flags=re.IGNORECASE,
    )
    if m_antes_uf:
        t1 = _token_cidade(m_antes_uf.group("cidade") or "")
        if t1:
            return t1
    return ""


def _url_indica_cidade_diferente(url: str, cidade_alvo: str) -> bool:
    alvo = _slug_fold(cidade_alvo)
    if not alvo:
        return False
    inferida = _cidade_inferida_da_url(url)
    if not inferida:
        return False
    if len(inferida) < 3:
        return False
    if inferida == alvo:
        return False
    # URLs podem trazer "bairro-cidade". Se terminar na cidade-alvo, aceitamos.
    if inferida.endswith(f"-{alvo}") or alvo.endswith(f"-{inferida}"):
        return False
    if inferida in alvo or alvo in inferida:
        return False
    return True


def _score_geo_card(
    *,
    card: dict[str, Any],
    cidade_alvo: str,
    bairro_informado: str,
    bairro_canonico: str,
    lat_ref: float | None,
    lon_ref: float | None,
) -> float:
    """
    Score geográfico 0..100 para auditoria/ranking:
    - distância real (peso 60%)
    - aderência de bairro informado/canônico (peso 25%)
    - consistência cidade/URL (peso 15%)
    """
    dist_pts = 55.0
    lat = card.get("latitude")
    lon = card.get("longitude")
    try:
        if lat_ref is not None and lon_ref is not None and lat is not None and lon is not None:
            d = float(haversine_km(float(lat_ref), float(lon_ref), float(lat), float(lon)))
            if d <= 2.0:
                dist_pts = 100.0
            elif d <= 5.0:
                dist_pts = 85.0
            elif d <= 10.0:
                dist_pts = 70.0
            elif d <= 15.0:
                dist_pts = 55.0
            else:
                dist_pts = 35.0
    except Exception:
        dist_pts = 55.0

    b_card = _slug_fold(card.get("bairro"))
    b_inf = _slug_fold(bairro_informado)
    b_can = _slug_fold(bairro_canonico)
    bairro_pts = 55.0
    if b_card and (b_inf or b_can):
        if b_card == b_inf or b_card == b_can:
            bairro_pts = 100.0
        elif b_can and b_card.endswith(f"-{b_can}"):
            bairro_pts = 80.0
        else:
            bairro_pts = 45.0

    cidade_url_pts = 100.0 if not _url_indica_cidade_diferente(str(card.get("url_anuncio") or ""), cidade_alvo) else 0.0

    score = (0.60 * dist_pts) + (0.25 * bairro_pts) + (0.15 * cidade_url_pts)
    return max(0.0, min(100.0, round(float(score), 2)))


def persistir_cards_anuncios_mercado(
    client: Client,
    cards: list[dict[str, Any]],
    *,
    cidade: str,
    estado_raw: str,
    bairro: str,
    leilao_imovel_id: str,
    url_listagem: str,
    tipo_imovel_fallback: str,
    origem_metadados: str = "firecrawl_mercado",
    amostras_sem_filtro_area_edital: bool = False,
    leilao_row: dict[str, Any] | None = None,
    exigir_geolocalizacao: bool = False,
    bairro_canonico: str = "",
    lat_ref: float | None = None,
    lon_ref: float | None = None,
    diagnostico_saida: dict[str, Any] | None = None,
) -> int:
    """Converte cards (listagem ou ficha) em linhas ``anuncios_mercado`` e faz upsert."""
    from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

    uf_an = estado_livre_para_sigla_uf(estado_raw) or str(estado_raw or "").strip()[:2].upper()
    linhas: list[dict[str, Any]] = []
    motivos = Counter()
    for card in cards:
        c = dict(card)
        url_anuncio = str(c.get("url_anuncio") or "").strip()
        if _url_indica_cidade_diferente(url_anuncio, cidade):
            motivos["cidade_url"] += 1
            continue
        tipo_det = str(c.pop("_tipo_detectado", "") or "").strip().lower()
        if not tipo_det:
            tipo_det = _tipo_detectado_por_titulo(c.get("titulo"))
        tipo_base = str(tipo_imovel_fallback or "apartamento").strip().lower()
        leilao_condominio = _leilao_indica_condominio(leilao_row)
        if tipo_base == "casa" and leilao_condominio:
            tipo_base = "casa_condominio"
        if tipo_base in ("terreno", "lote"):
            if tipo_det != "terreno" and not _titulo_sugere_terreno_ou_lote(c.get("titulo")):
                continue
            tipo_final = "lote" if tipo_base == "lote" else "terreno"
        elif tipo_det in {"terreno", "lote", "apartamento", "casa", "sobrado", "casa_condominio"}:
            tipo_final = "terreno" if tipo_det == "lote" else tipo_det
        else:
            tipo_final = tipo_base
        if leilao_condominio and tipo_final == "casa":
            tipo_final = "casa_condominio"
        if tipo_final == "desconhecido":
            tipo_final = "apartamento"
        try:
            area_m2 = float(c["area_m2"])
            valor = float(c["valor_venda"])
        except (KeyError, TypeError, ValueError):
            motivos["area_valor_parse"] += 1
            continue
        if area_m2 <= 0 or valor <= 0:
            motivos["area_valor_invalido"] += 1
            continue
        if _titulo_card_invalido(c.get("titulo")):
            tit_fb = _titulo_fallback_por_url(url_anuncio)
            if tit_fb:
                c["titulo"] = tit_fb
        if _titulo_card_invalido(c.get("titulo")):
            motivos["titulo_invalido"] += 1
            continue
        if exigir_geolocalizacao:
            try:
                lat = float(c.get("latitude"))
                lon = float(c.get("longitude"))
                if lat == 0.0 and lon == 0.0:
                    motivos["coord_zero"] += 1
                    continue
            except (TypeError, ValueError):
                motivos["sem_coord"] += 1
                continue
        seg_m = _segmento_mercado_residencial(c, tipo_final)
        meta: dict[str, Any] = {
            "leilao_imovel_id": str(leilao_imovel_id or ""),
            "origem": origem_metadados,
            "url_listagem": url_listagem,
            "segmento_mercado_residencial": seg_m,
        }
        score_geo = _score_geo_card(
            card=c,
            cidade_alvo=str(cidade or ""),
            bairro_informado=str(bairro or ""),
            bairro_canonico=str(bairro_canonico or ""),
            lat_ref=lat_ref,
            lon_ref=lon_ref,
        )
        meta["score_geo"] = score_geo
        if amostras_sem_filtro_area_edital:
            meta["amostras_sem_filtro_area_edital"] = True
            meta["confiabilidade_comparavel_reduzida"] = True
        bairro_card = _sanear_bairro_anuncio(c.get("bairro"))
        if not bairro_card:
            bairro_card = _sanear_bairro_anuncio(_bairro_inferido_da_url(url_anuncio, str(cidade or "")))
        if not bairro_card:
            bairro_card = _sanear_bairro_anuncio(_bairro_inferido_do_titulo(c.get("titulo"), str(cidade or "")))
        if bairro_card and _slug_fold(bairro_card) == _slug_fold(bairro):
            # Se o bairro veio igual ao do leilão, tenta extrair do anúncio; se não achar, deixa vazio.
            b_alt = _sanear_bairro_anuncio(_bairro_inferido_da_url(url_anuncio, str(cidade or "")))
            if not b_alt:
                b_alt = _sanear_bairro_anuncio(_bairro_inferido_do_titulo(c.get("titulo"), str(cidade or "")))
            bairro_card = b_alt or ""

        if leilao_row:
            pre_check = {
                "titulo": c.get("titulo"),
                "url_anuncio": str(c.get("url_anuncio") or "").strip(),
                "valor_venda": valor,
                "area_construida_m2": area_m2,
                "bairro": bairro_card,
                "metadados_json": meta,
            }
            if anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(pre_check, leilao_row):
                meta["incluir_em_cache"] = False
                meta["exclusao_motivo"] = "listagem_sinc_lance_mercado"
        linhas.append(
            {
                "url_anuncio": url_anuncio,
                "portal": str(c.get("portal") or "desconhecido").strip(),
                "tipo_imovel": tipo_final,
                "logradouro": str(c.get("logradouro") or "").strip(),
                "bairro": bairro_card,
                "cidade": str(c.get("cidade") or cidade).strip(),
                "estado": str(c.get("estado") or uf_an).strip()[:2],
                "area_construida_m2": area_m2,
                "valor_venda": valor,
                "transacao": "venda",
                "titulo": c.get("titulo"),
                "quartos": None if tipo_final == "terreno" else c.get("quartos"),
                "latitude": c.get("latitude"),
                "longitude": c.get("longitude"),
                "metadados_json": meta,
            }
        )
    if diagnostico_saida is not None:
        diagnostico_saida.clear()
        diagnostico_saida.update(
            {
                "cards_recebidos": int(len(cards or [])),
                "cards_validos_pre_upsert": int(len(linhas)),
                "descartes_total": int(max(0, len(cards or []) - len(linhas))),
                "descartes_por_motivo": dict(motivos),
            }
        )
    if not linhas:
        return 0
    n_up = anuncios_mercado_repo.upsert_lote(client, linhas)
    if diagnostico_saida is not None:
        diagnostico_saida["upsert_gravados"] = int(n_up or 0)
    return n_up
