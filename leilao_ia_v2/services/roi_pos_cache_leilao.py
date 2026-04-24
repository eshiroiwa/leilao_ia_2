"""
Após a montagem do cache de mercado, estima ROI bruto, lucro bruto/líquido, ROI líquido
e lance máximo (50% de ROI bruto) a partir de ``valor_medio_venda`` / min / max do
``cache_media_bairro`` principal. Receita líquida: venda menos 6% de corretagem; na compra
``L * (1 + r) + C_fix`` com 5%+3%+2% (e reforma, **zero** se tipo for terreno/lote) — alinhado a ``simulacao_operacao``.
Lucro líquido: 15% de IR (PF) sobre o lucro bruto positivo.

Persiste em colunas de ``leilao_imoveis`` (ver ``012``/``013`` em ``leilao_ia_v2/sql/``).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Optional

from supabase import Client

from leilao_ia_v2.normalizacao import normalizar_tipo_imovel
from leilao_ia_v2.persistence import cache_media_bairro_repo, leilao_imoveis_repo

logger = logging.getLogger(__name__)

# Premissas do agente (corretagem 6% s/ venda; IR 15% s/ lucro bruto; registro 2% no pós-cache)
LIMIAR_VALOR_POPULAR = 200_000.0
LIMIAR_VALOR_ALTO_PADRAO = 1_500_000.0
LIMIAR_M2_POPULAR_MIN = 50.0
REFORMA_POPULAR_MIN = 10_000.0
REFORMA_POPULAR_RS_POR_M2 = 200.0
REFORMA_MEDIO_MIN = 15_000.0
REFORMA_MEDIO_RS_POR_M2 = 500.0
REFORMA_ALTO_MIN = 30_000.0
REFORMA_ALTO_RS_POR_M2 = 1_000.0
REFORMA_FALLBACK_SEM_AREA_POPULAR = 10_000.0
REFORMA_FALLBACK_SEM_AREA_MEDIO = 30_000.0
REFORMA_FALLBACK_SEM_AREA_ALTO = 80_000.0
PCT_LEILOEIRO = 0.05
PCT_ITBI = 0.03
PCT_REGISTRO = 0.02
PCT_COMISSAO_IMOBILIARIA = 0.06
PCT_IR_SOBRE_LUCRO_BRUTO = 0.15
RATE_TAXA_TOTAL_COM_LEI = PCT_LEILOEIRO + PCT_ITBI + PCT_REGISTRO
RATE_TAXA_SEM_LEILOEIRO = PCT_ITBI + PCT_REGISTRO
DESOCUPACAO_ACIMA_100M2 = 10_000.0
LIMIAR_M2_DESOC = 100.0


def _area_m2_imovel(row: dict[str, Any]) -> float:
    for k in ("area_util", "area_total"):
        try:
            v = float(row.get(k) or 0.0)
            if v > 0:
                return v
        except (TypeError, ValueError):
            continue
    return 0.0


def _parse_extra_leilao(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("leilao_extra_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            o = json.loads(raw)
            return o if isinstance(o, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _blob_texto_sinais_leiloeiro(row: dict[str, Any]) -> str:
    """URL + `leilao_extra_json` + início do edital (onde costuma surgir a modalidade)."""
    ex = _parse_extra_leilao(row)
    partes: list[str] = [str(row.get("url_leilao") or "")]
    for k in (
        "observacoes_markdown",
        "regras_leilao_markdown",
        "modalidade_venda",
        "tipo_pagamento_resumo",
    ):
        v = ex.get(k)
        if v is not None and v != "":
            partes.append(str(v))
    em = row.get("edital_markdown")
    if em:
        partes.append(str(em)[:20000])
    return "\n".join(partes)


def _possui_exige_comissao_leiloeiro_licitacao_ou_sfi(txt: str) -> bool:
    """
    Licitação aberta (presencial) e leilão SFI: em geral há comissão de leiloeiro.
    """
    t = txt
    if re.search(
        r"licita[ç]ão\s+aberta|licitacao\s+aberta|licita[ç]ão[-\s]aberta",
        t,
        re.I,
    ):
        return True
    if re.search(r"leil[aã]o\s+sfi|leilao\s+sfi|leil[aã]o[-\s]sfi|leilao[-\s]sfi", t, re.I):
        return True
    return False


def _caixa_tem_sinal_comissao_leiloeiro(txt: str) -> bool:
    """
    Sinais de que a Caixa cobra 5% de leiloeiro: pessoa, leilão único, licitação aberta, SFI, etc.
    (texto de edital / observações; normalmente em português.)
    """
    t = txt
    if re.search(
        r"leiloeiro\s*\(a\)|leiloeir[oa]\s*\(a\)|\bleiloeir[oa]s?\b|leiloeiros?",
        t,
        re.I,
    ):
        return True
    if re.search(
        r"leil[aã]o\s+[uú]nico|leilao\s+unico|leil[aã]o[-\s]+[uú]nico|leilao[-\s]unico",
        t,
        re.I,
    ):
        return True
    if _possui_exige_comissao_leiloeiro_licitacao_ou_sfi(txt):
        return True
    return False


def _caixa_tem_sinal_isencao_comissao_leiloeiro(txt: str) -> bool:
    """
    Sem 5% de leiloeiro na Caixa: *venda online*, *compra direta* ou *venda direta online*
    (o edital de compra/venda directa no portal, não confundir com leilão com leiloeiro nomeado).
    """
    t = " ".join(str(txt).split())
    t = t.lower()
    if re.search(r"\bcompra\s+direta\b", t, re.I):
        return True
    if re.search(r"\bvenda\s+on[-\s]?line\b", t, re.I):
        return True
    if re.search(r"venda\s+direta\s+on[-\s]?line", t, re.I):
        return True
    return False


def _caixa_aplica_regra_comissao_leiloeiro(txt: str) -> bool:
    """
    1) Qualquer sinal de comissão (leiloeiro, leilão único, licitação aberta, SFI) → cobra 5%.
    2) Senão, sinal de isenção (venda online, compra direta, venda direta online) → não cobra.
    3) Senão, assume-se leilão com comissão (regra conservadora no portal Caixa).
    """
    if _caixa_tem_sinal_comissao_leiloeiro(txt):
        return True
    if _caixa_tem_sinal_isencao_comissao_leiloeiro(txt):
        return False
    return True


def aplica_comissao_leiloeiro(row: dict[str, Any]) -> bool:
    """
    Retorna se incidem os 5% de comissão de leiloeiro sobre a arrematação (o resto do ``r`` continua 3+2%).

    **Caixa (``caixa.gov``):** regra própria em ``_caixa_aplica_regra_comissao_leiloeiro``
    (leiloeiro, leilão único, licitação aberta, etc. → cobra; venda online, compra direta → não cobra).

    **Fora da Caixa:** considera-se sempre comissão de leiloeiro (5%).
    """
    url = str(row.get("url_leilao") or "").lower()
    if "caixa.gov" in url:
        return _caixa_aplica_regra_comissao_leiloeiro(_blob_texto_sinais_leiloeiro(row))
    return True


def imovel_sem_reforma_pos_cache(row: dict[str, Any]) -> bool:
    """
    Terreno ou lote: no pós-cache a reforma estimada (tabela R$/m²) não se aplica — custo 0.
    Alinhado ao segmento terrenos no cache de mercado.
    """
    t = normalizar_tipo_imovel(row.get("tipo_imovel"))
    if not t or t == "desconhecido":
        return False
    return t in ("terreno", "lote")


def _lance_referencia_brl(row: dict[str, Any]) -> float:
    try:
        l1 = float(row.get("valor_lance_1_praca") or 0.0)
    except (TypeError, ValueError):
        l1 = 0.0
    try:
        l2 = float(row.get("valor_lance_2_praca") or 0.0)
    except (TypeError, ValueError):
        l2 = 0.0
    if l1 > 0 and l2 > 0:
        return l2
    if l2 > 0:
        return l2
    if l1 > 0:
        return l1
    try:
        va = float(row.get("valor_arrematacao") or 0.0)
        if va > 0:
            return va
    except (TypeError, ValueError):
        pass
    return 0.0


def _taxa_total(*, aplica_5_leiloeiro: bool) -> float:
    return RATE_TAXA_TOTAL_COM_LEI if aplica_5_leiloeiro else RATE_TAXA_SEM_LEILOEIRO


def _valor_mercado_de_cache(
    c: dict[str, Any], area_m2: float
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """(valor_mercado, max_regiao, min_regiao) a partir de uma linha cache_media_bairro."""
    try:
        vm = c.get("valor_medio_venda")
        vmed = float(vm) if vm is not None and float(vm) > 0 else None
    except (TypeError, ValueError):
        vmed = None
    if vmed is None and area_m2 > 0:
        try:
            pm2 = c.get("preco_m2_medio")
            if pm2 is not None:
                p = float(pm2)
                if p > 0:
                    vmed = round(p * area_m2, 2)
        except (TypeError, ValueError):
            pass
    try:
        mx = c.get("maior_valor_venda")
        vmax = float(mx) if mx is not None and float(mx) > 0 else None
    except (TypeError, ValueError):
        vmax = None
    try:
        mn = c.get("menor_valor_venda")
        vmin = float(mn) if mn is not None and float(mn) > 0 else None
    except (TypeError, ValueError):
        vmin = None
    return vmed, vmax, vmin


def _cache_row_elegivel_para_simulacao(c: dict[str, Any]) -> bool:
    raw = c.get("metadados_json")
    md: dict[str, Any] = {}
    if isinstance(raw, dict):
        md = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            j = json.loads(raw)
            if isinstance(j, dict):
                md = j
        except json.JSONDecodeError:
            md = {}
    if str(md.get("modo_cache") or "").strip().lower() == "terrenos":
        return False
    if md.get("apenas_referencia") is True:
        return False
    if md.get("uso_simulacao") is False:
        return False
    return True


def _escolher_cache_pos_cache(
    cache_rows_ordenadas: list[dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], str]:
    for c in cache_rows_ordenadas:
        if _cache_row_elegivel_para_simulacao(c):
            return c, "principal_simulacao"
    if cache_rows_ordenadas:
        return cache_rows_ordenadas[0], "fallback_primeiro_cache"
    return None, "sem_cache_carregado"


@dataclass
class RoiPosCacheResultado:
    ok: bool
    motivo: str
    payload: dict[str, Any]


def _segmento_reforma_por_valor_venda(valor_venda: float) -> str:
    try:
        v = float(valor_venda)
    except (TypeError, ValueError):
        v = 0.0
    if v > 0 and v <= LIMIAR_VALOR_POPULAR:
        return "popular"
    if v > LIMIAR_VALOR_ALTO_PADRAO:
        return "alto"
    return "medio"


def _custo_reforma_pos_cache(
    area_m2: float,
    valor_venda: float,
    *,
    sem_reforma: bool = False,
) -> float:
    """
    Só a reforma (R$) para o agente pós-cache, sem desocupação.
    - ``sem_reforma`` (terreno/lote): 0.
    - popular (<=200k): mínimo 10k; até 50 m² => 10k; acima => max(10k, 200*m²).
    - médio (>200k até 1,5M): max(15k, 500*m²).
    - alto (>1,5M): max(30k, 1.000*m²).
    - sem área útil/total (>0): fallback por segmento (popular 10k, médio 30k, alto 80k).
    """
    if sem_reforma:
        return 0.0
    seg = _segmento_reforma_por_valor_venda(valor_venda)
    a = float(area_m2 or 0.0)
    if a <= 0.0:
        if seg == "popular":
            return REFORMA_FALLBACK_SEM_AREA_POPULAR
        if seg == "alto":
            return REFORMA_FALLBACK_SEM_AREA_ALTO
        return REFORMA_FALLBACK_SEM_AREA_MEDIO
    if seg == "popular":
        if a <= LIMIAR_M2_POPULAR_MIN:
            return REFORMA_POPULAR_MIN
        return max(REFORMA_POPULAR_MIN, REFORMA_POPULAR_RS_POR_M2 * a)
    if seg == "alto":
        return max(REFORMA_ALTO_MIN, REFORMA_ALTO_RS_POR_M2 * a)
    return max(REFORMA_MEDIO_MIN, REFORMA_MEDIO_RS_POR_M2 * a)


def _custo_fixos(area_m2: float, valor_venda: float, *, sem_reforma: bool = False) -> float:
    c = _custo_reforma_pos_cache(area_m2, valor_venda, sem_reforma=sem_reforma)
    a = float(area_m2 or 0.0)
    if (not sem_reforma) and a > LIMIAR_M2_DESOC:
        c += DESOCUPACAO_ACIMA_100M2
    return c


def _venda_liquida_projetada(valor_venda: float) -> float:
    """
    Receita líquida no pós-cache: **só** 6% de comissão imobiliária s/ V.
    Comissão de leiloeiro, ITBI e registro entram no investimento
    ``L * (1 + r) + C_fix``, não em V.
    """
    v = float(valor_venda)
    return v - PCT_COMISSAO_IMOBILIARIA * v


def calcular_roi_e_lance_max(
    valor_venda: float,
    lance: float,
    area_m2: float,
    *,
    aplica_5: bool,
    sem_reforma: bool = False,
) -> tuple[Optional[float], Optional[float]]:
    """
    Retorna (roi_projetado_pct, lance_max_50).

    - v_liq_venda = V - 6% V (comissão imob.); 5%+3%+2% só no investimento (L*(1+r)+C_fix)
    - Lucro = v_liq_venda - (L * (1 + r) + C_fix)
    - r = _taxa_total (comissões/tributos **sobre a arrematação**)
    """
    m = metricas_lucro_roi_pos_cache(
        valor_venda, lance, area_m2, aplica_5_leiloeiro=aplica_5, sem_reforma=sem_reforma
    )
    return m.get("roi_projetado"), m.get("lance_maximo_recomendado")


def metricas_lucro_roi_pos_cache(
    valor_venda: float,
    lance: float,
    area_m2: float,
    *,
    aplica_5_leiloeiro: bool,
    sem_reforma: bool = False,
) -> dict[str, Any]:
    """
    Uma fonte de verdade para o agente pós-cache: lucros, ROIs, lance máximo (50% ROI bruto),
    com 6% corretagem s/ venda e IR 15% s/ lucro bruto (PF, sobre lucro positivo).
    Chaves nulas se não houver lance/valor.
    """
    C_fix = _custo_fixos(area_m2, valor_venda, sem_reforma=sem_reforma)
    r = _taxa_total(aplica_5_leiloeiro=aplica_5_leiloeiro)
    out: dict[str, Any] = {
        "roi_projetado": None,
        "lance_maximo_recomendado": None,
        "lucro_bruto_projetado": None,
        "lucro_liquido_projetado": None,
        "roi_liquido_projetado": None,
    }
    if valor_venda <= 0 or lance <= 0:
        return out
    v_liq = _venda_liquida_projetada(valor_venda)
    invest = lance * (1.0 + r) + C_fix
    if invest <= 0:
        return out
    lucro = v_liq - invest
    lb = round(lucro, 2)
    roi_b = round(lucro / invest, 6)
    ir = PCT_IR_SOBRE_LUCRO_BRUTO * max(0.0, lucro)
    ll = round(lucro - ir, 2)
    roi_l = round((lucro - ir) / invest, 6) if invest > 0 else None
    num = v_liq - 1.5 * C_fix
    den = 1.5 * (1.0 + r)
    lance_max: Optional[float] = None
    if num > 0 and den > 0:
        lance_max = round(num / den, 2)
    return {
        "roi_projetado": roi_b,
        "lance_maximo_recomendado": lance_max,
        "lucro_bruto_projetado": lb,
        "lucro_liquido_projetado": ll,
        "roi_liquido_projetado": roi_l,
    }


def metricas_pos_cache_de_leilao_row(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Reconstroi as métricas a partir de ``valor_mercado_estimado`` + lance/área do leilão
    (ex.: leitores da UI sem colunas de lucro ainda preenchidas).
    """
    v_raw = row.get("valor_mercado_estimado")
    if v_raw is None:
        return None
    try:
        v = float(v_raw)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    l = _lance_referencia_brl(row)
    a = _area_m2_imovel(row)
    if l <= 0:
        return None
    return metricas_lucro_roi_pos_cache(
        v,
        l,
        a,
        aplica_5_leiloeiro=aplica_comissao_leiloeiro(row),
        sem_reforma=imovel_sem_reforma_pos_cache(row),
    )


def estimar_e_gravar_roi_pos_cache(
    client: Client,
    leilao_imovel_id: str,
) -> RoiPosCacheResultado:
    """
    Lê o 1.º cache em ``cache_media_bairro_ids``; se houver amostra, grava
    `valor_mercado_estimado` (e métricas heurísticas) a partir do cache e do lance.

    Se o leilão já tiver ``operacao_simulacao_json`` com ``outputs`` (simulação gravada),
    **não** sobrescreve lucro, ROI, valor, reforma e lance máximo — passam a vir só de
    ``calcular_simulacao`` + ``atualizar_operacao_e_modalidades`` (evita pós-cache a
    anular o alinhamento com o documento).
    """
    lid = str(leilao_imovel_id or "").strip()
    if not lid:
        return RoiPosCacheResultado(False, "id vazio", {})

    row = leilao_imoveis_repo.buscar_por_id(lid, client)
    if not row:
        return RoiPosCacheResultado(False, "leilão não encontrado", {})

    ordem: list[str] = []
    for x in row.get("cache_media_bairro_ids") or []:
        s = str(x).strip()
        if s and s not in set(ordem):
            ordem.append(s)
    if not ordem:
        return RoiPosCacheResultado(False, "sem cache vinculado", {})

    cache_rows = cache_media_bairro_repo.buscar_por_ids(client, ordem)
    by_id = {str(c.get("id") or "").strip(): c for c in cache_rows if c.get("id")}
    ordenadas = [by_id[i] for i in ordem if i in by_id]
    c, origem_cache = _escolher_cache_pos_cache(ordenadas)
    if not c:
        return RoiPosCacheResultado(False, "cache id não carregou", {"cache_ids": ordem[:5]})

    area = _area_m2_imovel(row)
    v_merc, v_max, v_min = _valor_mercado_de_cache(c, area)
    if v_merc is None or v_merc <= 0:
        return RoiPosCacheResultado(
            False,
            "cache sem valor_medio_venda (nem preco_m2×área)",
            {"cache_id": c.get("id")},
        )

    aplica5 = aplica_comissao_leiloeiro(row)
    sem_reforma = imovel_sem_reforma_pos_cache(row)
    lance = _lance_referencia_brl(row)
    custo_reforma = round(_custo_reforma_pos_cache(area, v_merc, sem_reforma=sem_reforma), 2)

    m_comp: dict[str, Any] = {}
    if lance > 0:
        m_comp = metricas_lucro_roi_pos_cache(
            v_merc, lance, area, aplica_5_leiloeiro=aplica5, sem_reforma=sem_reforma
        )
    else:
        logger.info(
            "ROI pós-cache: lance de referência zero (edital) id=%s — ROI/lance max não calculados",
            lid[:12],
        )

    payload: dict[str, Any] = {
        "valor_mercado_estimado": v_merc,
        "custo_reforma_estimado": custo_reforma,
        "valor_maximo_regiao_estimado": v_max,
        "valor_minimo_regiao_estimado": v_min,
        "roi_projetado": m_comp.get("roi_projetado"),
        "lance_maximo_recomendado": m_comp.get("lance_maximo_recomendado"),
        "lucro_bruto_projetado": m_comp.get("lucro_bruto_projetado"),
        "lucro_liquido_projetado": m_comp.get("lucro_liquido_projetado"),
        "roi_liquido_projetado": m_comp.get("roi_liquido_projetado"),
    }
    if origem_cache != "principal_simulacao":
        logger.info(
            "ROI pós-cache: sem cache principal elegível; usando fallback `%s` (cache=%s) leilao=%s",
            origem_cache,
            str(c.get("id") or "")[:12],
            lid[:12],
        )
    if leilao_imoveis_repo.leilao_tem_indicadores_simulacao_gravados(row.get("operacao_simulacao_json")):
        for k in leilao_imoveis_repo.COLUNAS_INDICADORES_SOMENTE_SIMULACAO:
            payload.pop(k, None)
        logger.info(
            "ROI pós-cache: não sobrescreve indicadores financeiros (já há `operacao_simulacao_json.outputs`); "
            "mantém-se max/min de região. leilao=%s",
            lid[:12],
        )
    to_write = {k: v for k, v in payload.items() if v is not None}
    leilao_imoveis_repo.atualizar_leilao_imovel(lid, to_write, client)
    return RoiPosCacheResultado(True, "gravado", payload)
