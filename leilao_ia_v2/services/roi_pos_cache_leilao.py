"""
Após a montagem do cache de mercado, estima ROI bruto, lucro bruto/líquido, ROI líquido
e lance máximo (50% de ROI bruto) a partir de ``valor_medio_venda`` / min / max do
``cache_media_bairro`` principal. Inclui 6% de comissão imobiliária s/ venda; lucro
líquido projeta 15% de IR (PF) sobre o lucro bruto positivo.

Persiste em colunas de ``leilao_imoveis`` (ver ``012``/``013`` em ``leilao_ia_v2/sql/``).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Optional

from supabase import Client

from leilao_ia_v2.persistence import cache_media_bairro_repo, leilao_imoveis_repo

logger = logging.getLogger(__name__)

# Premissas do agente (corretagem 6% s/ venda; IR 15% s/ lucro bruto; registro 2% no pós-cache)
REFORMA_RS_POR_M2 = 500.0
# Reforma pós-cache: ≤50m² 10k; >50 e ≤70m² 15k; >70m² 500 R$/m² (mais desocupação se >100m²; ver _custo_fixos)
LIMIAR_M2_REFORMA_TETO_1 = 50.0
LIMIAR_M2_REFORMA_TETO_2 = 70.0
REFORMA_ATE_50M2 = 10_000.0
REFORMA_ATE_70M2 = 15_000.0
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


def aplica_comissao_leiloeiro(row: dict[str, Any]) -> bool:
    """
    Em Caixa / venda direta online / venda online, não há 5% de comissão de leiloeiro
    (só impostos % sobre a base alvo).
    """
    url = str(row.get("url_leilao") or "").lower()
    ex = _parse_extra_leilao(row)
    ex_txt = json.dumps(ex, ensure_ascii=False).lower()
    if "caixa.gov" in url or "venda online" in ex_txt or "venda direta" in ex_txt:
        return False
    if re.search(
        r"\b(caixa|venda\s+direta|venda\s+online|leilao\s*online|online\s*sem\s*leiloeiro)\b",
        ex_txt,
    ):
        return False
    if re.search(
        r"\b(caixa|venda\s+direta|venda\s+online)\b",
        url,
    ):
        return False
    return True


def _lance_referencia_brl(row: dict[str, Any]) -> float:
    for k in ("valor_lance_1_praca", "valor_lance_2_praca", "valor_arrematacao"):
        try:
            v = float(row.get(k) or 0.0)
            if v > 0:
                return v
        except (TypeError, ValueError):
            continue
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


@dataclass
class RoiPosCacheResultado:
    ok: bool
    motivo: str
    payload: dict[str, Any]


def _custo_reforma_pos_cache(area_m2: float) -> float:
    """
    Só a reforma (R$) para o agente pós-cache, sem desocupação.
    - sem área (>0) não aplicável: 0; até 50 m²: 10.000; até 70 m²: 15.000; acima: 500 × m².
    """
    a = float(area_m2)
    if a <= 0.0:
        return 0.0
    if a <= LIMIAR_M2_REFORMA_TETO_1:
        return REFORMA_ATE_50M2
    if a <= LIMIAR_M2_REFORMA_TETO_2:
        return REFORMA_ATE_70M2
    return REFORMA_RS_POR_M2 * a


def _custo_fixos(area_m2: float) -> float:
    c = _custo_reforma_pos_cache(area_m2)
    if area_m2 > LIMIAR_M2_DESOC:
        c += DESOCUPACAO_ACIMA_100M2
    return c


def _venda_liquida_projetada(valor_venda: float, r: float) -> float:
    """
    Receita líquida do lado da venda (simplificação alinhada à simulação v2):
    tributos/encargos modelados com taxa `r` sobre V e, em separado, 6% de corretagem s/ V.
    """
    return valor_venda * (1.0 - r) - PCT_COMISSAO_IMOBILIARIA * valor_venda


def calcular_roi_e_lance_max(
    valor_venda: float,
    lance: float,
    area_m2: float,
    *,
    aplica_5: bool,
) -> tuple[Optional[float], Optional[float]]:
    """
    Retorna (roi_projetado_pct, lance_max_50).

    - v_liq_venda = V * (1 - r) - 6% V (comissão imobiliária s/ venda)
    - Lucro = v_liq_venda - L * (1 + r) - C_fix
    - Investimento = L * (1 + r) + C_fix
    - r = _taxa_total (compra; mesmo r na aquisição)
    """
    m = metricas_lucro_roi_pos_cache(
        valor_venda, lance, area_m2, aplica_5_leiloeiro=aplica_5
    )
    return m.get("roi_projetado"), m.get("lance_maximo_recomendado")


def metricas_lucro_roi_pos_cache(
    valor_venda: float,
    lance: float,
    area_m2: float,
    *,
    aplica_5_leiloeiro: bool,
) -> dict[str, Any]:
    """
    Uma fonte de verdade para o agente pós-cache: lucros, ROIs, lance máximo (50% ROI bruto),
    com 6% corretagem s/ venda e IR 15% s/ lucro bruto (PF, sobre lucro positivo).
    Chaves nulas se não houver lance/valor.
    """
    C_fix = _custo_fixos(area_m2)
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
    v_liq = _venda_liquida_projetada(valor_venda, r)
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
        v, l, a, aplica_5_leiloeiro=aplica_comissao_leiloeiro(row)
    )


def estimar_e_gravar_roi_pos_cache(
    client: Client,
    leilao_imovel_id: str,
) -> RoiPosCacheResultado:
    """
    Lê o 1.º cache em ``cache_media_bairro_ids``; se houver amostra, grava
    `valor_mercado_estimado`, reforma, min/max, `roi_projetado`, `lance_maximo_recomendado`.
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

    pid = ordem[0]
    c = cache_media_bairro_repo.buscar_por_id(pid, client)
    if not c:
        return RoiPosCacheResultado(False, "cache id não carregou", {"cache_id": pid})

    area = _area_m2_imovel(row)
    v_merc, v_max, v_min = _valor_mercado_de_cache(c, area)
    if v_merc is None or v_merc <= 0:
        return RoiPosCacheResultado(
            False,
            "cache sem valor_medio_venda (nem preco_m2×área)",
            {"cache_id": pid},
        )

    aplica5 = aplica_comissao_leiloeiro(row)
    lance = _lance_referencia_brl(row)
    custo_reforma = round(_custo_reforma_pos_cache(area), 2)

    m_comp: dict[str, Any] = {}
    if lance > 0:
        m_comp = metricas_lucro_roi_pos_cache(
            v_merc, lance, area, aplica_5_leiloeiro=aplica5
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
    leilao_imoveis_repo.atualizar_leilao_imovel(lid, {k: v for k, v in payload.items() if v is not None}, client)
    return RoiPosCacheResultado(True, "gravado", payload)
