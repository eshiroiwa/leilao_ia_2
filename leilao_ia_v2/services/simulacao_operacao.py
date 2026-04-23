"""
Cálculo determinístico da simulação de operação (leilão → venda).

- Comissão leiloeiro e ITBI: **% sobre o lance nominal (arrematação)**. Registro: idem. Opcional: **desconto à vista** reduz o caixa pago do lance; comissão do leiloeiro (e % ITBI/registro) seguem o **lance cheio**.
- Reforma: manual ou R$/m² (básica, média, completa, alto padrão).
- Lance máximo para ROI desejado: bissecção sobre o lance (ROI bruto ou líquido).
"""

from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from leilao_ia_v2.schemas.operacao_simulacao import (
    ModoPagamentoSimulacao,
    ModoReforma,
    ModoRoiDesejado,
    ModoValorVenda,
    OperacaoSimulacaoDocumento,
    SimulacaoOperacaoInputs,
    SimulacaoOperacaoOutputs,
)
from leilao_ia_v2.services.simulacao_pagamento_prazo_fin import (
    juros_acumulados_price_ate_t,
    pmt_price,
    primeira_prestacao_sac,
    saldo_devedor_price_apos_t_parcelas,
    saldo_devedor_sac_apos_t,
    soma_juros_sac_ate_t,
    soma_prestacoes_sac_ate_t,
    taxa_mensal_de_anual,
    total_parcelas_price_acumuladas,
)

REFORMA_RS_M2: dict[str, float] = {
    ModoReforma.BASICA.value: 500.0,
    ModoReforma.MEDIA.value: 1000.0,
    ModoReforma.COMPLETA.value: 1500.0,
    ModoReforma.ALTO_PADRAO.value: 2500.0,
}


def _area_m2_leilao(row: dict[str, Any]) -> float:
    for k in ("area_util", "area_total"):
        try:
            v = float(row.get(k) or 0)
        except (TypeError, ValueError):
            continue
        if v > 0:
            return v
    return 0.0


def _parse_csv_uuids_anuncios(raw: Any) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _metadados_cache_row(cache_row: dict[str, Any]) -> dict[str, Any]:
    raw = cache_row.get("metadados_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


def _cache_elegivel_para_simulacao(cache_row: dict[str, Any]) -> bool:
    """Terrenos e caches só de referência não entram no cálculo de valor de venda."""
    md = _metadados_cache_row(cache_row)
    if str(md.get("modo_cache") or "").strip().lower() == "terrenos":
        return False
    if md.get("apenas_referencia") is True:
        return False
    if md.get("uso_simulacao") is False:
        return False
    return True


def _escolher_cache_row(
    caches_ordenados: list[dict[str, Any]],
    cache_id_preferido: Optional[str],
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    if not caches_ordenados:
        return None, None
    elegiveis = [c for c in caches_ordenados if _cache_elegivel_para_simulacao(c)]
    base = elegiveis if elegiveis else []
    pref = (cache_id_preferido or "").strip()
    if pref:
        for c in base:
            if str(c.get("id") or "") == pref:
                return c, pref
        for c in caches_ordenados:
            if str(c.get("id") or "") == pref and not _cache_elegivel_para_simulacao(c):
                return None, None
    if base:
        c0 = base[0]
        return c0, str(c0.get("id") or "") or None
    return None, None


def _anuncios_do_cache(
    cache_row: dict[str, Any],
    ads_por_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for aid in _parse_csv_uuids_anuncios(cache_row.get("anuncios_ids")):
        a = ads_por_id.get(aid)
        if isinstance(a, dict):
            out.append(a)
    return out


def _media_preco_m2_anuncios(ads: list[dict[str, Any]]) -> Optional[float]:
    vals: list[float] = []
    for a in ads:
        try:
            pm = float(a.get("preco_m2") or 0)
            if pm > 0:
                vals.append(pm)
                continue
            ar = float(a.get("area_construida_m2") or 0)
            vv = float(a.get("valor_venda") or 0)
            if ar > 0 and vv > 0:
                vals.append(vv / ar)
        except (TypeError, ValueError):
            continue
    if not vals:
        return None
    return float(statistics.mean(vals))


def _media_valor_venda_anuncios(ads: list[dict[str, Any]]) -> Optional[float]:
    vals: list[float] = []
    for a in ads:
        try:
            v = float(a.get("valor_venda") or 0)
        except (TypeError, ValueError):
            continue
        if v > 0:
            vals.append(v)
    if not vals:
        return None
    return float(statistics.mean(vals))


def _menor_valor_venda_anuncios(ads: list[dict[str, Any]]) -> Optional[float]:
    vals: list[float] = []
    for a in ads:
        try:
            v = float(a.get("valor_venda") or 0)
        except (TypeError, ValueError):
            continue
        if v > 0:
            vals.append(v)
    if not vals:
        return None
    return float(min(vals))


def resolver_valor_venda_estimado(
    *,
    row_leilao: dict[str, Any],
    inp: SimulacaoOperacaoInputs,
    caches_ordenados: list[dict[str, Any]],
    ads_por_id: dict[str, dict[str, Any]],
) -> tuple[float, SimulacaoOperacaoOutputs]:
    notas: list[str] = []
    area = _area_m2_leilao(row_leilao)
    modo = inp.modo_valor_venda
    tem_cache_elegivel = bool(caches_ordenados) and any(
        _cache_elegivel_para_simulacao(c) for c in caches_ordenados
    )
    cache_row, cache_usado = _escolher_cache_row(caches_ordenados, inp.cache_media_bairro_id)
    ads = _anuncios_do_cache(cache_row, ads_por_id) if cache_row else []
    if caches_ordenados and not tem_cache_elegivel and modo != ModoValorVenda.MANUAL:
        pref = (inp.cache_media_bairro_id or "").strip()
        if pref:
            notas.append(
                "O cache indicado é só de referência (terreno/outro segmento) — não usado na simulação."
            )
        else:
            notas.append(
                "Todos os caches vinculados são só de referência (ex.: terrenos); defina um cache de simulação ou modo manual."
            )

    venda = 0.0
    modo_resolvido = str(modo.value)

    if modo == ModoValorVenda.MANUAL:
        venda = float(inp.valor_venda_manual or 0)
        if venda <= 0:
            notas.append("Modo manual: informe valor_venda_manual > 0.")
    elif modo == ModoValorVenda.CACHE_PRECO_M2_X_AREA:
        if not cache_row:
            if not caches_ordenados or tem_cache_elegivel:
                notas.append("Sem cache vinculado para usar preço médio R$/m².")
        elif area <= 0:
            notas.append("Área útil/total do leilão ausente ou zero — não dá para aplicar R$/m².")
        else:
            try:
                pm2 = float(cache_row.get("preco_m2_medio") or 0)
            except (TypeError, ValueError):
                pm2 = 0.0
            if pm2 > 0:
                venda = pm2 * area
            else:
                notas.append("Cache sem preco_m2_medio válido.")
    elif modo == ModoValorVenda.CACHE_VALOR_MEDIO_VENDA:
        if not cache_row:
            if not caches_ordenados or tem_cache_elegivel:
                notas.append("Sem cache vinculado para valor_medio_venda.")
        else:
            try:
                vm = float(cache_row.get("valor_medio_venda") or 0)
            except (TypeError, ValueError):
                vm = 0.0
            if vm > 0:
                venda = vm
            else:
                notas.append("Cache sem valor_medio_venda válido.")
    elif modo == ModoValorVenda.CACHE_MENOR_VALOR_VENDA:
        if not cache_row:
            if not caches_ordenados or tem_cache_elegivel:
                notas.append("Sem cache vinculado para menor_valor_venda.")
        else:
            try:
                vmin = float(cache_row.get("menor_valor_venda") or 0)
            except (TypeError, ValueError):
                vmin = 0.0
            if vmin > 0:
                venda = vmin
            else:
                notas.append("Cache sem menor_valor_venda válido.")
    elif modo == ModoValorVenda.ANUNCIOS_VALOR_MEDIO:
        m = _media_valor_venda_anuncios(ads)
        if m is not None:
            venda = m
        else:
            notas.append("Sem anúncios com valor_venda para média.")
    elif modo == ModoValorVenda.ANUNCIOS_MENOR_VALOR:
        m = _menor_valor_venda_anuncios(ads)
        if m is not None:
            venda = m
        else:
            notas.append("Sem anúncios com valor_venda para menor valor.")
    elif modo == ModoValorVenda.ANUNCIOS_PRECO_M2_X_AREA:
        if area <= 0:
            notas.append("Área útil/total ausente — não dá para aplicar m² dos anúncios.")
        else:
            m = _media_preco_m2_anuncios(ads)
            if m is not None:
                venda = m * area
            else:
                notas.append("Sem anúncios com preço/m² derivável.")

    parcial = SimulacaoOperacaoOutputs(
        valor_venda_estimado=venda,
        modo_valor_venda_resolvido=modo_resolvido,
        cache_media_bairro_id_usado=cache_usado,
        area_m2_usada=area if area > 0 else None,
        notas=list(notas),
    )
    return venda, parcial


def _comissao_imobiliaria_resolvida(venda: float, inp: SimulacaoOperacaoInputs) -> float:
    if inp.comissao_imobiliaria_brl and inp.comissao_imobiliaria_brl > 0:
        return float(inp.comissao_imobiliaria_brl)
    p = float(inp.comissao_imobiliaria_pct_sobre_venda or 0)
    if p > 0 and venda > 0:
        return round(venda * (p / 100.0), 2)
    return 0.0


def _reforma_valor_brl(inp: SimulacaoOperacaoInputs, area: float) -> tuple[float, str]:
    modo = inp.reforma_modo
    m = str(modo.value if hasattr(modo, "value") else modo)
    if m == ModoReforma.MANUAL.value:
        return float(inp.reforma_brl or 0), m
    r = REFORMA_RS_M2.get(m, 0.0)
    if area <= 0:
        return 0.0, m
    return round(area * r, 2), m


def _comissao_leiloeiro_brl(lance: float, inp: SimulacaoOperacaoInputs) -> tuple[float, float]:
    if inp.comissao_leiloeiro_brl and inp.comissao_leiloeiro_brl > 0:
        return float(inp.comissao_leiloeiro_brl), 0.0
    p = float(inp.comissao_leiloeiro_pct_sobre_arrematacao or 0)
    if lance > 0 and p > 0:
        return round(lance * (p / 100.0), 2), p
    return 0.0, p


def _itbi_brl(lance: float, inp: SimulacaoOperacaoInputs) -> tuple[float, float]:
    if inp.itbi_brl and inp.itbi_brl > 0:
        return float(inp.itbi_brl), 0.0
    p = float(inp.itbi_pct_sobre_arrematacao or 0)
    if lance > 0 and p > 0:
        return round(lance * (p / 100.0), 2), p
    return 0.0, p


def _registro_brl(lance: float, inp: SimulacaoOperacaoInputs) -> tuple[float, float]:
    if inp.registro_brl and inp.registro_brl > 0:
        return float(inp.registro_brl), 0.0
    p = float(inp.registro_pct_sobre_arrematacao or 0)
    if lance > 0 and p > 0:
        return round(lance * (p / 100.0), 2), p
    return 0.0, p


def _lance_pago_e_desconto_avista(lance_nominal: float, inp: SimulacaoOperacaoInputs) -> tuple[float, float, float, bool]:
    """(lance_pago, desconto_em_r, pct_aplicada, ativo). Comissão/ITBI/registro usam só ``lance_nominal``."""
    if not bool(getattr(inp, "desconto_pagamento_avista", False)):
        return lance_nominal, 0.0, 0.0, False
    pct = max(0.0, min(99.0, float(getattr(inp, "desconto_pagamento_avista_pct", 0) or 0.0)))
    pago = round(float(lance_nominal) * (1.0 - pct / 100.0), 2)
    dv = max(0.0, round(float(lance_nominal) - pago, 2))
    return pago, dv, pct, True


@dataclass
class MetricasLance:
    lance: float
    lance_pago: float
    desconto_avista_brl: float
    desconto_avista_pct: float
    desconto_avista_ativo: bool
    clei: float
    clei_pct: float
    itbi: float
    itbi_pct: float
    registro: float
    registro_pct: float
    reforma: float
    reforma_modo: str
    subtotal: float
    comissao_imob: float
    lucro_bruto: float
    roi_bruto: Optional[float]
    ir_val: float
    base_ir: float
    ir_manual: bool
    lucro_liquido: float
    roi_liquido: Optional[float]


def _metricas_para_lance(
    lance: float,
    *,
    venda: float,
    inp: SimulacaoOperacaoInputs,
    area: float,
) -> MetricasLance:
    lance_pago, d_brl, d_pct, d_ativo = _lance_pago_e_desconto_avista(lance, inp)
    clei, pclei = _comissao_leiloeiro_brl(lance, inp)
    itbi, pitbi = _itbi_brl(lance, inp)
    reg, preg = _registro_brl(lance, inp)
    reforma, rmod = _reforma_valor_brl(inp, area)
    cond = float(inp.condominio_atrasado_brl or 0)
    iptu = float(inp.iptu_atrasado_brl or 0)
    desoc = float(inp.desocupacao_brl or 0)
    outros = float(inp.outros_custos_brl or 0)

    subtotal = lance_pago + clei + itbi + reg + cond + iptu + reforma + desoc + outros
    comissao_imob = _comissao_imobiliaria_resolvida(venda, inp)
    lucro_bruto = venda - subtotal - comissao_imob
    roi_bruto: Optional[float] = lucro_bruto / subtotal if subtotal > 0 else None

    ir_manual = inp.ir_valor_manual_brl
    ir_usou_manual = ir_manual is not None and ir_manual >= 0
    if ir_usou_manual:
        ir_val = float(ir_manual or 0)
        base_ir = 0.0
    elif inp.tipo_pessoa == "PF":
        base_lucro_imob = max(0.0, lucro_bruto)
        base_ir = base_lucro_imob
        ir_val = max(0.0, float(inp.ir_aliquota_pf_pct or 0) / 100.0) * base_lucro_imob
    else:
        venda_liq = max(0.0, venda - comissao_imob)
        base_ir = venda_liq
        ir_val = max(0.0, float(inp.ir_aliquota_pj_pct or 0) / 100.0) * venda_liq

    lucro_liquido = lucro_bruto - ir_val
    roi_liquido: Optional[float] = lucro_liquido / subtotal if subtotal > 0 else None

    return MetricasLance(
        lance=lance,
        lance_pago=lance_pago,
        desconto_avista_brl=d_brl,
        desconto_avista_pct=d_pct,
        desconto_avista_ativo=d_ativo,
        clei=clei,
        clei_pct=pclei,
        itbi=itbi,
        itbi_pct=pitbi,
        registro=reg,
        registro_pct=preg,
        reforma=reforma,
        reforma_modo=rmod,
        subtotal=subtotal,
        comissao_imob=comissao_imob,
        lucro_bruto=lucro_bruto,
        roi_bruto=roi_bruto,
        ir_val=ir_val,
        base_ir=base_ir,
        ir_manual=ir_usou_manual,
        lucro_liquido=lucro_liquido,
        roi_liquido=roi_liquido,
    )


def _roi_escolhido(m: MetricasLance, modo: ModoRoiDesejado) -> Optional[float]:
    if modo == ModoRoiDesejado.LIQUIDO:
        return m.roi_liquido
    return m.roi_bruto


def _lance_maximo_para_roi(
    *,
    venda: float,
    inp: SimulacaoOperacaoInputs,
    area: float,
    roi_desejado_pct: float,
    modo_roi: ModoRoiDesejado,
) -> tuple[Optional[float], list[str]]:
    notas: list[str] = []
    R = float(roi_desejado_pct) / 100.0
    if R < 0 or (venda <= 0 and inp.comissao_imobiliaria_pct_sobre_venda <= 0):
        notas.append("Informe venda estimada ou comissão para base de cálculo.")
        return None, notas

    m0 = _metricas_para_lance(0.0, venda=venda, inp=inp, area=area)
    r0 = _roi_escolhido(m0, modo_roi)
    if r0 is None or m0.subtotal <= 0:
        notas.append("Subtotal de custos com lance zero é zero — não é possível calcular ROI.")
        return None, notas
    if r0 < R:
        notas.append(
            "Com lance zero o ROI já fica abaixo do desejado — não existe lance máximo que atinja o alvo."
        )
        return None, notas

    lo, hi = 0.0, max(venda, 1.0)
    m_hi = _metricas_para_lance(hi, venda=venda, inp=inp, area=area)
    r_hi = _roi_escolhido(m_hi, modo_roi)
    esc = 0
    while r_hi is not None and r_hi > R and hi < venda * 15 + 1e7 and esc < 40:
        hi *= 1.35
        m_hi = _metricas_para_lance(hi, venda=venda, inp=inp, area=area)
        r_hi = _roi_escolhido(m_hi, modo_roi)
        esc += 1
    if r_hi is None or (r_hi is not None and r_hi > R):
        notas.append("Não foi possível bracketar o lance (aumente limites ou revise custos).")
        return None, notas

    for _ in range(80):
        mid = (lo + hi) / 2.0
        rm = _roi_escolhido(_metricas_para_lance(mid, venda=venda, inp=inp, area=area), modo_roi)
        if rm is None:
            hi = mid
            continue
        if rm >= R:
            lo = mid
        else:
            hi = mid
        if hi - lo < max(1.0, abs(hi) * 1e-7):
            break

    return round(lo, 2), notas


def _ir_sobre_cenario_lucro(
    lucro_bruto: float, venda: float, cimob: float, inp: SimulacaoOperacaoInputs
) -> tuple[float, float, bool]:
    ir_manual = inp.ir_valor_manual_brl
    if ir_manual is not None and float(ir_manual) >= 0:
        return float(ir_manual or 0), 0.0, True
    if inp.tipo_pessoa == "PF":
        base = max(0.0, lucro_bruto)
        ir_val = max(0.0, float(inp.ir_aliquota_pf_pct or 0) / 100.0) * base
        return ir_val, base, False
    venda_liq = max(0.0, venda - cimob)
    ir_val = max(0.0, float(inp.ir_aliquota_pj_pct or 0) / 100.0) * venda_liq
    return ir_val, venda_liq, False


def _resolver_modo_pagamento(inp: SimulacaoOperacaoInputs) -> ModoPagamentoSimulacao:
    m = getattr(inp, "modo_pagamento", None)
    if isinstance(m, ModoPagamentoSimulacao):
        return m
    if isinstance(m, str):
        s = m.strip().lower()
        for x in ModoPagamentoSimulacao:
            if x.value == s:
                return x
    return ModoPagamentoSimulacao.VISTA


def _custo_comum_fora_lance_m(m: MetricasLance, inp: SimulacaoOperacaoInputs) -> float:
    return (
        m.clei
        + m.itbi
        + m.registro
        + m.reforma
        + float(inp.condominio_atrasado_brl or 0)
        + float(inp.iptu_atrasado_brl or 0)
        + float(inp.desocupacao_brl or 0)
        + float(inp.outros_custos_brl or 0)
    )


def _roi_anual_de_periodo(roi: Optional[float], meses: float) -> Optional[float]:
    if roi is None or meses <= 0:
        return None
    try:
        return (1.0 + float(roi)) ** (12.0 / float(meses)) - 1.0
    except (ArithmeticError, ValueError, OverflowError):
        return None


def _resultado_por_modo_e_tempo(
    *,
    modo: ModoPagamentoSimulacao,
    lance: float,
    venda: float,
    m: MetricasLance,
    inp: SimulacaoOperacaoInputs,
) -> dict[str, Any]:
    T = max(0.25, float(getattr(inp, "tempo_estimado_venda_meses", 12.0) or 12.0))
    cimob = m.comissao_imob
    v_liq = venda - cimob
    c_comum = _custo_comum_fora_lance_m(m, inp)
    pmt = 0.0
    saldo_q = 0.0
    juros_ate = 0.0
    k = int(math.floor(T))
    n_eff = 0
    L = max(0.0, float(lance) or 0.0)
    explic: list[str] = []
    if modo == ModoPagamentoSimulacao.VISTA:
        n_eff = 0
        c_inv = m.subtotal
        lucro_b = v_liq - c_inv
        ir_val, base_ir, ir_usou = _ir_sobre_cenario_lucro(lucro_b, venda, cimob, inp)
        lucro_l = lucro_b - ir_val
        inv = c_inv
        pmt = 0.0
        saldo_q = 0.0
        juros_ate = 0.0
        explic.append(
            f"Modalidade **à vista**: lucro/ROI com custo do subtotal até a venda. "
            f"ROI anualizado com tempo estimado T = {T:.1f} meses."
        )
    elif modo == ModoPagamentoSimulacao.PRAZO:
        e_pct = max(0.0, min(95.0, float(getattr(inp, "prazo_entrada_pct", 30.0) or 0.0)))
        n = max(1, min(60, int(getattr(inp, "prazo_num_parcelas", 30) or 30)))
        i_m = max(0.0, float(getattr(inp, "prazo_juros_mensal_pct", 1.0) or 0.0)) / 100.0
        n_eff = n
        E = round(L * (e_pct / 100.0), 2)
        P0 = max(0.0, L - E)
        kpay = int(min(k, n))
        pmt = pmt_price(P0, i_m, n)
        tot_prest = total_parcelas_price_acumuladas(P0, i_m, n, kpay)
        saldo_q = round(saldo_devedor_price_apos_t_parcelas(P0, i_m, n, kpay), 2)
        juros_ate = juros_acumulados_price_ate_t(P0, i_m, n, kpay)
        c_inv = round(E + c_comum + tot_prest, 2)
        lucro_b = v_liq - saldo_q - c_inv
        ir_val, base_ir, ir_usou = _ir_sobre_cenario_lucro(lucro_b, venda, cimob, inp)
        inv = c_inv
        lucro_l = lucro_b - ir_val
        explic.append(
            "Modalidade **parcelada (judicial)**: entrada + parcelas (tabela **Price** homogênea) + custos. "
            "Quitação na venda = saldo devedor do parcelamento. Ajuste ao edital (CPC/art. 895; referências: ~25% entrada, até 30x, juros/índices locais)."
        )
    else:
        e_pct = max(5.0, min(50.0, float(getattr(inp, "fin_entrada_pct", 20.0) or 0.0)))
        n = max(12, min(480, int(getattr(inp, "fin_prazo_meses", 360) or 360)))
        n_eff = n
        i_ano = float(getattr(inp, "fin_taxa_juros_anual_pct", 14.0) or 0.0)
        i_m = taxa_mensal_de_anual(i_ano)
        sistema = str(getattr(inp, "fin_sistema", "SAC") or "SAC").upper()
        E = round(L * (e_pct / 100.0), 2)
        P0 = max(0.0, L - E)
        kpay = int(min(k, n))
        if sistema == "SAC":
            tot_prest = soma_prestacoes_sac_ate_t(P0, i_m, n, kpay)
            juros_ate = soma_juros_sac_ate_t(P0, i_m, n, kpay)
            # 1.ª prestação SAC (P/n + juros s/ saldo) — evita confundir com média no T ou com só P/n
            pmt = primeira_prestacao_sac(P0, i_m, n)
            saldo_q = round(saldo_devedor_sac_apos_t(P0, n, kpay), 2)
        else:
            pmt = pmt_price(P0, i_m, n)
            tot_prest = total_parcelas_price_acumuladas(P0, i_m, n, kpay)
            juros_ate = juros_acumulados_price_ate_t(P0, i_m, n, kpay)
            saldo_q = round(saldo_devedor_price_apos_t_parcelas(P0, i_m, n, kpay), 2)
        c_inv = round(E + c_comum + tot_prest, 2)
        lucro_b = v_liq - saldo_q - c_inv
        ir_val, base_ir, ir_usou = _ir_sobre_cenario_lucro(lucro_b, venda, cimob, inp)
        inv = c_inv
        lucro_l = lucro_b - ir_val
        explic.append(
            "Financiamento: entrada mín. típica 5–20% (banco/edital) + juros. "
            f"Sistema {sistema} com taxa ~{i_ano:.2f}% a.a. (→ i mensal composto). "
            f"{'1.ª prestação SAC' if sistema == 'SAC' else 'PMT tabela Price'} mostrada abaixo; prazo {n} meses. "
            "No SAC as parcelas caem ao longo do tempo (só a 1.ª prestação é referência de pico de caixa)."
        )
    roi_b = (lucro_b / inv) if inv > 0 else None
    roi_l = (lucro_l / inv) if inv > 0 else None
    return {
        "T": T,
        "lucro_bruto": lucro_b,
        "lucro_liquido": lucro_l,
        "ir_val": ir_val,
        "base_ir": base_ir,
        "ir_usou_manual": ir_usou,
        "roi_bruto": roi_b,
        "roi_liquido": roi_l,
        "investimento_cash": inv,
        "saldo_quit": saldo_q,
        "juros_ate": juros_ate,
        "pmt": pmt,
        "k": k,
        "n": n_eff,
        "explic": explic,
    }


def calcular_simulacao(
    *,
    row_leilao: dict[str, Any],
    inp: SimulacaoOperacaoInputs,
    caches_ordenados: list[dict[str, Any]],
    ads_por_id: dict[str, dict[str, Any]],
) -> OperacaoSimulacaoDocumento:
    venda, base_out = resolver_valor_venda_estimado(
        row_leilao=row_leilao,
        inp=inp,
        caches_ordenados=caches_ordenados,
        ads_por_id=ads_por_id,
    )
    notas = list(base_out.notas)
    area = float(base_out.area_m2_usada or _area_m2_leilao(row_leilao) or 0.0)

    lance = float(inp.lance_brl or 0)
    m = _metricas_para_lance(lance, venda=venda, inp=inp, area=area)
    if m.ir_manual:
        notas.append("IR definido manualmente (ignora alíquota PF/PJ).")
    if m.desconto_avista_ativo and m.desconto_avista_brl and m.desconto_avista_brl > 0:
        notas.append(
            "Desconto à vista aplicado no caixa do lance; comissão do leiloeiro, ITBI e registro (%) seguem o lance nominal."
        )

    lance_max: Optional[float] = None
    lance_max_notas: list[str] = []
    roi_pct_inf = inp.roi_desejado_pct
    modo_pag = _resolver_modo_pagamento(inp)
    if roi_pct_inf is not None and float(roi_pct_inf) > 0:
        # Teto de lance por meta de ROI: sempre cenário **à vista** (ROI s/ caixa em cima do
        # subtotal de custos). Não depende de T nem da modalidade mostrada no painel principal.
        inp_vista = inp.model_copy(update={"modo_pagamento": ModoPagamentoSimulacao.VISTA})
        lance_max, lance_max_notas = _lance_maximo_para_roi(
            venda=venda,
            inp=inp_vista,
            area=area,
            roi_desejado_pct=float(roi_pct_inf),
            modo_roi=inp.roi_desejado_modo,
        )
        if modo_pag != ModoPagamentoSimulacao.VISTA:
            lance_max_notas = list(lance_max_notas)
            lance_max_notas.append(
                "Referência no cenário **à vista** (independente do T e da modalidade simulada acima)."
            )

    agora = datetime.now(timezone.utc).isoformat()
    modo_roi_doc: Optional[str] = None
    if roi_pct_inf is not None and float(roi_pct_inf) > 0:
        modo_roi_doc = str(inp.roi_desejado_modo.value)

    rt = _resultado_por_modo_e_tempo(
        modo=modo_pag, lance=lance, venda=venda, m=m, inp=inp
    )
    for ex in rt.get("explic") or []:
        if ex and ex not in notas:
            notas.append(ex)
    Tc = float(rt.get("T") or 0.0)
    inv_c = float(rt.get("investimento_cash") or 0.0)
    saldo_quit = float(rt.get("saldo_quit") or 0.0)
    # Soma econômica (caixa pago + saldo a quitar no repasse) alinha o “subtotal” com o
    # lucro: v_liq - inv - saldo = venda - cimob - (inv+saldo). Só o caixa (inv) estaria
    # subcontabilizado e confundiria a comparação com à vista.
    subtotal_economico = round(inv_c + max(0.0, saldo_quit), 2)
    roi_b_a = _roi_anual_de_periodo(rt.get("roi_bruto"), Tc)
    roi_l_a = _roi_anual_de_periodo(rt.get("roi_liquido"), Tc)

    out = SimulacaoOperacaoOutputs(
        valor_venda_estimado=round(venda, 2),
        modo_valor_venda_resolvido=base_out.modo_valor_venda_resolvido,
        cache_media_bairro_id_usado=base_out.cache_media_bairro_id_usado,
        area_m2_usada=base_out.area_m2_usada,
        lance_brl=round(m.lance, 2),
        lance_pago_apos_desconto_brl=round(m.lance_pago, 2),
        desconto_pagamento_avista_ativo=m.desconto_avista_ativo,
        desconto_pagamento_avista_pct_efetivo=round(m.desconto_avista_pct, 4),
        desconto_pagamento_avista_valor_brl=round(m.desconto_avista_brl, 2),
        comissao_leiloeiro_brl=round(m.clei, 2),
        comissao_leiloeiro_pct_efetivo=m.clei_pct,
        itbi_brl=round(m.itbi, 2),
        itbi_pct_efetivo=m.itbi_pct,
        registro_brl=round(m.registro, 2),
        registro_pct_efetivo=m.registro_pct,
        condominio_atrasado_brl=round(float(inp.condominio_atrasado_brl or 0), 2),
        iptu_atrasado_brl=round(float(inp.iptu_atrasado_brl or 0), 2),
        reforma_brl=round(m.reforma, 2),
        reforma_modo_resolvido=m.reforma_modo,
        desocupacao_brl=round(float(inp.desocupacao_brl or 0), 2),
        outros_custos_brl=round(float(inp.outros_custos_brl or 0), 2),
        subtotal_custos_operacao=subtotal_economico,
        comissao_imobiliaria_brl=round(m.comissao_imob, 2),
        custo_total_com_corretagem=round(subtotal_economico + m.comissao_imob, 2),
        lucro_bruto=round(float(rt.get("lucro_bruto") or 0.0), 2),
        roi_bruto=round(float(rt.get("roi_bruto") or 0.0), 6)
        if rt.get("roi_bruto") is not None
        else None,
        base_ir=round(float(rt.get("base_ir") or 0.0), 2),
        ir_calculado_brl=round(float(rt.get("ir_val") or 0.0), 2),
        ir_usou_manual=bool(rt.get("ir_usou_manual", False) or m.ir_manual),
        lucro_liquido=round(float(rt.get("lucro_liquido") or 0.0), 2),
        roi_liquido=round(float(rt.get("roi_liquido") or 0.0), 6)
        if rt.get("roi_liquido") is not None
        else None,
        roi_desejado_pct_informado=float(roi_pct_inf) if roi_pct_inf is not None else None,
        roi_desejado_modo_informado=modo_roi_doc,
        lance_maximo_para_roi_desejado=lance_max,
        lance_maximo_roi_notas=lance_max_notas,
        calculado_em_iso=agora,
        notas=notas,
        modo_pagamento_resolvido=modo_pag.value,
        tempo_estimado_venda_meses_resolvido=round(Tc, 2),
        investimento_cash_ate_momento_venda=round(inv_c, 2),
        saldo_divida_quitacao_na_venda=round(float(rt.get("saldo_quit") or 0.0), 2),
        total_juros_ate_momento_venda=round(float(rt.get("juros_ate") or 0.0), 2),
        pmt_mensal_resolvido=round(float(rt.get("pmt") or 0.0), 2),
        roi_bruto_anualizado=round(roi_b_a, 6) if roi_b_a is not None else None,
        roi_liquido_anualizado=round(roi_l_a, 6) if roi_l_a is not None else None,
    )

    return OperacaoSimulacaoDocumento(inputs=inp, outputs=out)
