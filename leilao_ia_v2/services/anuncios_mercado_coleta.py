"""Persistência de cards extraídos de portais (markdown) em ``anuncios_mercado``."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from supabase import Client

from leilao_ia_v2.normalizacao import normalizar_tipo_casa
from leilao_ia_v2.persistence import anuncios_mercado_repo
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

    # Ex.: chavesnamão/zap etc. ``...-sp-franca-vila-x-220m2-...``
    uf_pat = "|".join(_UF_SLUGS)
    m_uf = re.search(
        rf"-(?:{uf_pat})-(?P<cidade>[a-z0-9]+(?:-[a-z0-9]+){{0,3}})-(?=[a-z0-9-]*?(?:\d+m2|rs\d|id-|venda|aluguel))",
        u,
        flags=re.IGNORECASE,
    )
    if m_uf:
        return _slug_fold(m_uf.group("cidade"))

    # VivaReal: captura o grupo imediatamente antes de ``m2-venda/aluguel``.
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


def _url_indica_cidade_diferente(url: str, cidade_alvo: str) -> bool:
    alvo = _slug_fold(cidade_alvo)
    if not alvo:
        return False
    inferida = _cidade_inferida_da_url(url)
    if not inferida:
        return False
    if inferida == alvo:
        return False
    # URLs podem trazer "bairro-cidade". Se terminar na cidade-alvo, aceitamos.
    if inferida.endswith(f"-{alvo}") or alvo.endswith(f"-{inferida}"):
        return False
    return True


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
) -> int:
    """Converte cards (listagem ou ficha) em linhas ``anuncios_mercado`` e faz upsert."""
    from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

    uf_an = estado_livre_para_sigla_uf(estado_raw) or str(estado_raw or "").strip()[:2].upper()
    linhas: list[dict[str, Any]] = []
    for card in cards:
        c = dict(card)
        url_anuncio = str(c.get("url_anuncio") or "").strip()
        if _url_indica_cidade_diferente(url_anuncio, cidade):
            continue
        tipo_det = str(c.pop("_tipo_detectado", "") or "").strip().lower()
        tipo_base = str(tipo_imovel_fallback or "apartamento").strip().lower()
        if tipo_base in ("terreno", "lote"):
            if tipo_det != "terreno" and not _titulo_sugere_terreno_ou_lote(c.get("titulo")):
                continue
            tipo_final = "lote" if tipo_base == "lote" else "terreno"
        elif tipo_det == "terreno":
            tipo_final = "terreno"
        else:
            tipo_final = tipo_base
        if tipo_final == "desconhecido":
            tipo_final = "apartamento"
        try:
            area_m2 = float(c["area_m2"])
            valor = float(c["valor_venda"])
        except (KeyError, TypeError, ValueError):
            continue
        if area_m2 <= 0 or valor <= 0:
            continue
        if _titulo_card_invalido(c.get("titulo")):
            continue
        if exigir_geolocalizacao:
            try:
                lat = float(c.get("latitude"))
                lon = float(c.get("longitude"))
                if lat == 0.0 and lon == 0.0:
                    continue
            except (TypeError, ValueError):
                continue
        seg_m = _segmento_mercado_residencial(c, tipo_final)
        meta: dict[str, Any] = {
            "leilao_imovel_id": str(leilao_imovel_id or ""),
            "origem": origem_metadados,
            "url_listagem": url_listagem,
            "segmento_mercado_residencial": seg_m,
        }
        if amostras_sem_filtro_area_edital:
            meta["amostras_sem_filtro_area_edital"] = True
            meta["confiabilidade_comparavel_reduzida"] = True
        if leilao_row:
            pre_check = {
                "titulo": c.get("titulo"),
                "url_anuncio": str(c.get("url_anuncio") or "").strip(),
                "valor_venda": valor,
                "area_construida_m2": area_m2,
                "bairro": str(c.get("bairro") or bairro or "").strip(),
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
                "bairro": str(c.get("bairro") or bairro or "").strip(),
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
    if not linhas:
        return 0
    return anuncios_mercado_repo.upsert_lote(client, linhas)
