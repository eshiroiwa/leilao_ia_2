"""
Exporta JSON com parcelas da simulação (painel) vs agente pós-cache e controle algebricamente.
Útil para auditar diferenças (ex.: lance, desconto à vista, T e modalidade).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from leilao_ia_v2.schemas.operacao_simulacao import (
    ModoPagamentoSimulacao,
    OperacaoSimulacaoDocumento,
    SimulacaoOperacaoInputs,
    SimulacaoOperacaoOutputs,
)
from leilao_ia_v2.services.roi_pos_cache_leilao import (
    PCT_COMISSAO_IMOBILIARIA,
    _taxa_total,
    aplica_comissao_leiloeiro,
    imovel_sem_reforma_pos_cache,
    metricas_lucro_roi_pos_cache,
    metricas_pos_cache_de_leilao_row,
)


def _soma_subtotal_pecas_vista(o: SimulacaoOperacaoOutputs) -> float:
    return float(
        (o.lance_pago_apos_desconto_brl or 0.0)
        + (o.comissao_leiloeiro_brl or 0.0)
        + (o.itbi_brl or 0.0)
        + (o.registro_brl or 0.0)
        + (o.condominio_atrasado_brl or 0.0)
        + (o.iptu_atrasado_brl or 0.0)
        + (o.reforma_brl or 0.0)
        + (o.desocupacao_brl or 0.0)
        + (o.outros_custos_brl or 0.0)
    )


def _lucro_bruto_manual_vista(v: float, cim: float, sub: float) -> float:
    """Igual a ``_resultado_por_modo`` à vista: (v - cim) - sub (subtotal sem corretagem)."""
    return v - cim - sub


def build_simulacao_debug_payload(
    row: dict[str, Any],
    inp: SimulacaoOperacaoInputs,
    doc: OperacaoSimulacaoDocumento,
) -> dict[str, Any]:
    o = doc.outputs
    if o is None:
        return {"erro": "OperacaoSimulacaoDocumento sem outputs", "inputs": _safe_dump(inp)}

    v = float(o.valor_venda_estimado or 0.0)
    cim = float(o.comissao_imobiliaria_brl or 0.0)
    v_liq = v - cim
    sub_econ = float(o.subtotal_custos_operacao or 0.0)
    sub_soma = _soma_subtotal_pecas_vista(o)
    soma_drift = round(sub_soma - sub_econ, 2)

    modo = str(getattr(o, "modo_pagamento_resolvido", "") or "")
    controle_vista = None
    if modo == str(ModoPagamentoSimulacao.VISTA.value) or modo in ("VISTA", "vista", ""):
        lb_chk = _lucro_bruto_manual_vista(v, cim, sub_soma)
        lb_chk2 = _lucro_bruto_manual_vista(v, cim, sub_econ)
        controle_vista = {
            "venda": round(v, 2),
            "menos_comissao_imob_6_pct": round(cim, 2),
            "receita_liquida_venda_menos_corretagem": round(v_liq, 2),
            "soma_pecas_subtotal": round(sub_soma, 2),
            "subtotal_custos_operacao_no_documento": round(sub_econ, 2),
            "diferenca_peca_a_peca_vs_subtotal_doc": soma_drift,
            "lucro_se_usar_soma_pecas": round(lb_chk, 2),
            "lucro_se_usar_subtotal_doc": round(lb_chk2, 2),
            "lucro_bruto_no_documento": round(float(o.lucro_bruto or 0.0), 2),
        }

    a5 = aplica_comissao_leiloeiro(row)
    lance = float(o.lance_brl or 0.0)
    area = float(o.area_m2_usada or 0.0)
    pos_mesma_base: Optional[dict[str, Any]] = None
    if v > 0 and lance > 0 and area > 0:
        m = metricas_lucro_roi_pos_cache(
            v,
            lance,
            area,
            aplica_5_leiloeiro=a5,
            sem_reforma=imovel_sem_reforma_pos_cache(row),
        )
        r_tax = _taxa_total(aplica_5_leiloeiro=a5)
        pos_mesma_base = {
            "entrada": "mesmo V, L e área do documento de simulação (não depende de valor_mercado_estimado da linha)",
            "aplica_5_pct_leiloeiro_na_aposta_preco": a5,
            "r_tributos_sobre_lance": r_tax,
            "metricas": m,
        }

    pos_row: Optional[dict[str, Any]] = None
    try:
        pr = metricas_pos_cache_de_leilao_row(row)
        if pr is not None:
            pos_row = {k: pr.get(k) for k in sorted(pr.keys())}
    except Exception as e:
        pos_row = {"erro": str(e)}

    vm_row = row.get("valor_mercado_estimado")
    nota_vm = None
    try:
        if vm_row is not None and v > 0 and abs(float(vm_row) - v) > 0.5:
            nota_vm = (
                f"valor_mercado_estimado na linha do leilão ({vm_row!r}) difere de "
                f"valor_venda_estimado no painel ({v}). A coluna pós-cache pode refletir outra base."
            )
    except (TypeError, ValueError):
        pass

    return {
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "leilao_imovel_id": str(row.get("id") or "") or None,
        "nota_valor_mercado_vs_painel": nota_vm,
        "notas_painel": list(o.notas or []),
        "inputs": _safe_dump(inp),
        "outputs": _safe_dump(o),
        "controle_algebrico": {
            "modo_pagamento_resolvido": modo,
            "vista_ou_equivalente": controle_vista,
        },
        "pos_cache_mesma_base_que_painel": pos_mesma_base,
        "pos_cache_a_partir_da_linha_leilao_valor_mercado_estimado": pos_row,
        "rubricas_ajuda": {
            "PCT_COMISSAO_IMOBILIARIA": PCT_COMISSAO_IMOBILIARIA,
            "subtotal_pecas": "lance_pago + leiloeiro + ITBI + registro + cond + iptu + reforma + desoc + outros",
            "lucro_bruto_à_vista_padrão": "venda - comissão imob. - subtotal (sem juros/parcelas/T)",
        },
    }


def _safe_dump(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    return str(obj)


def default_debug_json_path(iid: str) -> Path:
    root = Path(__file__).resolve().parent.parent / "_debug"
    root.mkdir(parents=True, exist_ok=True)
    safe = str(iid or "sem_id").replace("/", "_")[:40]
    return root / f"ultima_simulacao_{safe}.json"


def export_simulacao_debug_json(
    row: dict[str, Any],
    inp: SimulacaoOperacaoInputs,
    doc: OperacaoSimulacaoDocumento,
    *,
    path: Optional[Path | str] = None,
) -> tuple[str, Path]:
    """
    Retorna ``(string_json, caminho_gravado)``. Se ``path`` for None, usa
    ``leilao_ia_v2/_debug/ultima_simulacao_{iid}.json``.
    """
    payload = build_simulacao_debug_payload(row, inp, doc)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    p = Path(path) if path is not None else default_debug_json_path(str(row.get("id") or "sem_id"))
    p.write_text(text, encoding="utf-8")
    return text, p


__all__ = [
    "build_simulacao_debug_payload",
    "default_debug_json_path",
    "export_simulacao_debug_json",
]
