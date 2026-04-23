"""
Chaves de sessão por modalidade (vista / prazo / financiado) e hidratação a partir de ``SimulacaoOperacaoInputs``.

**Campos comuns** (lance, tributos, reforma, venda, IR, corretagem, etc.) vivem sempre em ``simop_key(iid, "vista", …)`` —
um único conjunto de widgets evita divergência entre modalidades. ``derramar_inputs_no_session(..., preencher_somente_omisso=True)``
preenche só chaves ainda inexistente(s). Quando a aba do Simulador deixa de ser executada, o Streamlit pode apagar
essas chaves: ``simop_gravar_snapshot_draft`` / ``simop_restaurar_draft_de_snapshot`` guardam e repõem o rascunho
por leilão (``iid``).
Parâmetros específicos de **parcelado** (``pr_ent``, ``pr_n``, ``pr_jm``) permanecem em chaves ``prazo``; de
**financiamento** (``fin_*``) em ``financiado``.

O cálculo ativo (qual ``modo_pagamento``) vem de ``simop_key_mpag``; ``construir_inputs_de_sessao`` lê o comum em ``vista``
e o específico nas tags de prazo / financiado.
"""

from __future__ import annotations

import copy
from typing import Any, Literal

import streamlit as st

from leilao_ia_v2.schemas.operacao_simulacao import (
    ModoPagamentoSimulacao,
    ModoReforma,
    ModoRoiDesejado,
    ModoValorVenda,
    SimulacaoOperacaoInputs,
)

SimopTag = Literal["vista", "prazo", "financiado"]

TAGS: tuple[SimopTag, ...] = ("vista", "prazo", "financiado")


def simop_m_lab_to_tag(m_lab: str) -> SimopTag:
    t = (m_lab or "").strip()
    if "Parcelado" in t:
        return "prazo"
    if "Financiado" in t:
        return "financiado"
    return "vista"


def tag_to_modo_pagamento(tag: SimopTag) -> ModoPagamentoSimulacao:
    if tag == "prazo":
        return ModoPagamentoSimulacao.PRAZO
    if tag == "financiado":
        return ModoPagamentoSimulacao.FINANCIADO
    return ModoPagamentoSimulacao.VISTA


def _suf(iid: str) -> str:
    return str(iid).replace("-", "")[:20]


def simop_key(iid: str, tag: str, campo: str) -> str:
    return f"simop_{tag}_{campo}_{_suf(iid)}"


def simop_key_mpag(iid: str) -> str:
    """Legado: ex-seletor 3 vias. Mantido para compat.; o painel principal passou a ser sempre à vista."""
    return f"simop_global_mpag_seg_{_suf(iid)}"


def simop_key_cmp_painel(iid: str) -> str:
    """Comparação ao lado do painel: ``nenhum`` | ``prazo`` | ``financiado`` (só um painel extra)."""
    return f"simop_cmp_painel_{_suf(iid)}"


def simop_key_ui_nicho_prazo_fin(iid: str) -> str:
    """
    On/off: mostrar os blocos de parâmetros de **parcelado judicial** e **financiamento bancário**
    (entrada %, parcelas, juros, etc.). O formulário comum fica sempre visível; este só controla a expansão.
    """
    return f"simop_ui_nicho_prazo_fin_{_suf(iid)}"


def simop_key_tempo_venda_global(iid: str) -> str:
    """
    **Único** tempo estimado até a venda (meses) para vista, prazo e financiado.
    (Antes existia `t_venda` por modalidade; agora o mesmo valor aplica a todas as simulações.)
    """
    return f"simop_global_t_venda_{_suf(iid)}"


def simop_ensure_tempo_venda_global(iid: str) -> None:
    """Garante o valor em ``simop_key_tempo_venda_global``; migra chave antiga por-tag se faltar."""
    g = simop_key_tempo_venda_global(iid)
    if g in st.session_state:
        return
    for tag in TAGS:
        leg = st.session_state.get(simop_key(iid, tag, "t_venda"))
        if leg is not None:
            st.session_state[g] = float(leg)
            return
    st.session_state[g] = 12.0


def simop_hidratou_chave(iid: str) -> str:
    return f"simop_hidratou_{_suf(iid)}"


SIMOP_DRAFT_SNAPSHOT_BAG = "simop_draft_key_snapshots_v1"


def _simop_chave_candidata_draft(iid: str, k: str) -> bool:
    """Inclui chaves ``simop_*`` deste leilão (sufixo), exceto a bandeira de hidratação e botões.

    ``st.button`` (ex.: Gravar) não podem ser escritos em ``st.session_state`` — ver
    ``StreamlitValueAssignmentNotAllowedError``.
    """
    suf = _suf(iid)
    ks = str(k)
    if not ks.startswith("simop_") or not ks.endswith(suf):
        return False
    if ks.startswith("simop_hidratou_"):
        return False
    if ks == simop_key(iid, "vista", "save"):
        return False
    if ks.startswith("simop_save3_"):
        return False
    return True


def simop_listar_chaves_draft_por_iid(iid: str) -> list[str]:
    return [k for k in st.session_state.keys() if _simop_chave_candidata_draft(iid, k)]


def simop_gravar_snapshot_draft(iid: str) -> None:
    """
    Grava o rascunho atual deste leilão (todos os ``simop_*`` com sufixo do ``iid``).
    Necessário porque, ao trocar de aba, o Streamlit pode não reexecutar os widgets
    e as chaves somem; na volta aplicamos o snapshot (ver ``simop_restaurar_draft_de_snapshot``).
    """
    iid = str(iid or "").strip()
    if not iid:
        return
    _raw_bag = st.session_state.get(SIMOP_DRAFT_SNAPSHOT_BAG)
    bag: dict[str, Any] = _raw_bag if isinstance(_raw_bag, dict) else {}
    snap: dict[str, Any] = {}
    for k in simop_listar_chaves_draft_por_iid(iid):
        try:
            snap[k] = copy.deepcopy(st.session_state.get(k))
        except Exception:
            snap[k] = st.session_state.get(k)
    bag[iid] = snap
    st.session_state[SIMOP_DRAFT_SNAPSHOT_BAG] = bag


def simop_restaurar_draft_de_snapshot(iid: str) -> None:
    """
    Repõe chaves **em falta** a partir do último snapshot (fim do run anterior).

    Deve correr **antes** da hidratação a partir do banco (``_simop_hidratar_modalidades``): assim, ao voltar
    de outra aba o rascunho repõe o que o Streamlit apagou; no mesmo run em que o utilizador
    editou, o valor já vem em ``st.session_state`` e **não** é sobrescrito (evita voltar
    ao valor por defeito). A hidratação a partir do banco preenche só o que continuar em falta.
    """
    iid = str(iid or "").strip()
    if not iid:
        return
    bag = st.session_state.get(SIMOP_DRAFT_SNAPSHOT_BAG)
    if not isinstance(bag, dict):
        return
    snap = bag.get(iid)
    if not isinstance(snap, dict) or not snap:
        return
    for k, v in snap.items():
        if not _simop_chave_candidata_draft(iid, k):
            continue
        if k in st.session_state:
            continue
        st.session_state[k] = v


def simop_limpar_snapshot_draft(iid: str) -> None:
    """Após persistir a simulação no banco, evita reaproximar um rascunho antigo à sessão."""
    iid = str(iid or "").strip()
    if not iid:
        return
    bag = st.session_state.get(SIMOP_DRAFT_SNAPSHOT_BAG)
    if isinstance(bag, dict) and iid in bag:
        del bag[iid]
        st.session_state[SIMOP_DRAFT_SNAPSHOT_BAG] = bag


def _key_para_ref_label(rmod: str) -> str:
    m = {
        "none": "Sem reforma",
        "basica": "500/m²",
        "media": "1k/m²",
        "completa": "1,5k/m²",
        "alto": "2,5k/m²",
        "alto_padrao": "2,5k/m²",
        "manual": "R$ livre",
    }
    r = (rmod or "basica").strip().lower()
    if "none" in r or r == "none":
        return m["none"]
    return m.get(r, "500/m²")


def derramar_inputs_no_session(
    iid: str,
    tag: SimopTag,
    inp: SimulacaoOperacaoInputs,
    *,
    preencher_somente_omisso: bool = False,
) -> None:
    """
    Volca ``inputs`` no ``session_state`` com chaves da modalidade.
    Com ``preencher_somente_omisso=True``, só cria a chave se ainda não existir
    (rascunho segue ao trocar de aba; troca de leilão = nova hidratação completa).
    """
    d = inp.model_dump(mode="python", exclude_unset=False)
    ss = st.session_state
    k = lambda c: simop_key(iid, tag, c)  # noqa: E731
    kv = lambda c: simop_key(iid, "vista", c)  # noqa: E731

    def _w(key: str, val: object) -> None:
        if preencher_somente_omisso and key in ss:
            return
        ss[key] = val  # type: ignore[assignment]

    _w(k("lance"), float(d.get("lance_brl") or 0))
    _w(k("lance_2a"), bool(d.get("usar_lance_segunda_praca") or False))
    _w(kv("descav"), bool(d.get("desconto_pagamento_avista") or False))
    _w(kv("descav_pct"), float(d.get("desconto_pagamento_avista_pct") or 0.0))
    # t_venda é global: ver simop_key_tempo_venda_global + hidratação.
    _w(k("pr_ent"), float(d.get("prazo_entrada_pct") or 30.0))
    _w(k("pr_n"), int(d.get("prazo_num_parcelas") or 30))
    _w(k("pr_jm"), float(d.get("prazo_juros_mensal_pct") or 0.0))
    _w(k("fin_ent"), float(d.get("fin_entrada_pct") or 20.0))
    _w(k("fin_n"), int(d.get("fin_prazo_meses") or 360))
    _w(k("fin_tx"), float(d.get("fin_taxa_juros_anual_pct") or 0.0))
    fs = str(d.get("fin_sistema") or "SAC").upper()
    _w(k("fin_sys"), fs if fs in ("SAC", "PRICE") else "SAC")
    mvv = d.get("modo_valor_venda")
    if isinstance(mvv, ModoValorVenda):
        mv_str = mvv.value
    elif isinstance(mvv, str):
        mv_str = mvv
    else:
        mv_str = str(getattr(mvv, "value", mvv) or ModoValorVenda.CACHE_VALOR_MEDIO_VENDA.value)
    _w(k("modo_val"), mv_str)
    _w(k("vmanual"), float(d.get("valor_venda_manual") or 0))
    _w(k("tipo"), "PJ" if d.get("tipo_pessoa") == "PJ" else "PF")
    _w(k("ir_pf"), float(d.get("ir_aliquota_pf_pct") or 0))
    _w(k("ir_pj"), float(d.get("ir_aliquota_pj_pct") or 0))
    _w(k("cleipct"), float(d.get("comissao_leiloeiro_pct_sobre_arrematacao") or 0))
    _w(k("itbipct"), float(d.get("itbi_pct_sobre_arrematacao") or 0))
    if float(d.get("registro_brl") or 0) > 0:
        _w(k("regfix"), float(d.get("registro_brl") or 0))
    else:
        _w(k("regpct"), float(d.get("registro_pct_sobre_arrematacao") or 0.0))
    _w(k("cond"), float(d.get("condominio_atrasado_brl") or 0))
    _w(k("iptu"), float(d.get("iptu_atrasado_brl") or 0))
    _w(k("des"), float(d.get("desocupacao_brl") or 0))
    _w(k("out"), float(d.get("outros_custos_brl") or 0))
    rm = d.get("reforma_modo")
    rmod = str(rm) if isinstance(rm, str) else getattr(rm, "value", str(rm or "basica"))
    _w(k("refui_lbl"), _key_para_ref_label(rmod))
    _w(k("refmanual"), float(d.get("reforma_brl") or 0))
    _w(k("cimob"), float(d.get("comissao_imobiliaria_brl") or 0))
    _w(k("cimobpct"), float(d.get("comissao_imobiliaria_pct_sobre_venda") or 0))
    rd = d.get("roi_desejado_pct")
    _w(k("roi_w"), float(rd) if rd is not None else 50.0)
    rdm = d.get("roi_desejado_modo")
    rms = rdm if isinstance(rdm, str) else getattr(rdm, "value", "bruto")
    _w(k("roi_seg"), "Líquido" if "liqu" in str(rms).lower() else "Bruto")


def construir_inputs_de_sessao(
    *,
    iid: str,
    tag: SimopTag,
    inp0: SimulacaoOperacaoInputs,
    modo_valor: ModoValorVenda,
    v_manual_st: float,
    def_lance: float,
    ref_mod: ModoReforma,
    reforma_brl_inp: float,
    cache_sel: str | None,
) -> SimulacaoOperacaoInputs:
    k = lambda c: st.session_state.get(simop_key(iid, "vista", c))  # noqa: E731
    kv = k
    k_pr = lambda c: st.session_state.get(simop_key(iid, "prazo", c))  # noqa: E731
    k_fn = lambda c: st.session_state.get(simop_key(iid, "financiado", c))  # noqa: E731

    mpag_e = tag_to_modo_pagamento(tag)
    lance = float(k("lance") if k("lance") is not None else def_lance)
    usar_2 = bool(
        k("lance_2a")
        if k("lance_2a") is not None
        else (inp0.usar_lance_segunda_praca or False)
    )
    desconto_vista_on = tag == "vista" and bool(kv("descav") if kv("descav") is not None else False)
    dsk_pct = float(kv("descav_pct") if kv("descav_pct") is not None else 10.0)

    tipo = str(k("tipo") or "PF")
    ir_pf = float(k("ir_pf") if k("ir_pf") is not None else inp0.ir_aliquota_pf_pct)
    ir_pj = float(k("ir_pj") if k("ir_pj") is not None else inp0.ir_aliquota_pj_pct)
    v_man = float(k("vmanual") if k("vmanual") is not None else (inp0.valor_venda_manual or 0))
    v_man = v_manual_st if v_manual_st else v_man

    legacy_reg = float(inp0.registro_brl or 0) > 0
    clei_pct = float(
        k("cleipct") if k("cleipct") is not None else inp0.comissao_leiloeiro_pct_sobre_arrematacao
    )
    itbi_pct = float(k("itbipct") if k("itbipct") is not None else inp0.itbi_pct_sobre_arrematacao)
    if legacy_reg and k("regfix") is not None:
        reg_brl_inp = float(k("regfix"))
        reg_pct = float(inp0.registro_pct_sobre_arrematacao or 0)
    else:
        reg_brl_inp = 0.0
        reg_pct = float(
            k("regpct") if k("regpct") is not None else (inp0.registro_pct_sobre_arrematacao or 2.0)
        )

    cond = float(k("cond") if k("cond") is not None else inp0.condominio_atrasado_brl)
    iptu = float(k("iptu") if k("iptu") is not None else inp0.iptu_atrasado_brl)
    desoc = float(k("des") if k("des") is not None else inp0.desocupacao_brl)
    outros = float(k("out") if k("out") is not None else inp0.outros_custos_brl)
    cimob_brl = float(k("cimob") if k("cimob") is not None else inp0.comissao_imobiliaria_brl)
    cimob_pct = float(
        k("cimobpct") if k("cimobpct") is not None else inp0.comissao_imobiliaria_pct_sobre_venda
    )
    roi_desej = float(k("roi_w") if k("roi_w") is not None else 0.0)
    rs = str(k("roi_seg") or "Bruto")
    roi_modo = (
        ModoRoiDesejado.LIQUIDO
        if "íquido" in rs or rs.strip().lower() in ("líquido", "liquido")
        else ModoRoiDesejado.BRUTO
    )

    fin_sis = str(k_fn("fin_sys") or "SAC").upper()
    if fin_sis not in ("SAC", "PRICE"):
        fin_sis = "SAC"

    tgv = st.session_state.get(simop_key_tempo_venda_global(iid))
    if tgv is None:
        tgv = k("t_venda")
    tempo_meses = float(
        tgv if tgv is not None else (inp0.tempo_estimado_venda_meses or 12.0)
    )

    return SimulacaoOperacaoInputs(
        tipo_pessoa="PJ" if tipo == "PJ" else "PF",
        modo_valor_venda=modo_valor,
        valor_venda_manual=(v_man if modo_valor == ModoValorVenda.MANUAL else None),
        cache_media_bairro_id=cache_sel,
        usar_lance_segunda_praca=usar_2,
        lance_brl=lance,
        modo_pagamento=mpag_e,
        tempo_estimado_venda_meses=tempo_meses,
        prazo_entrada_pct=float(
            k_pr("pr_ent") if k_pr("pr_ent") is not None else 30.0
        ),
        prazo_num_parcelas=int(k_pr("pr_n") or 30),
        prazo_juros_mensal_pct=float(
            k_pr("pr_jm")
            if k_pr("pr_jm") is not None
            else (inp0.prazo_juros_mensal_pct or 0.0)
        ),
        fin_entrada_pct=float(
            k_fn("fin_ent") if k_fn("fin_ent") is not None else 20.0
        ),
        fin_prazo_meses=int(k_fn("fin_n") or 360),
        fin_taxa_juros_anual_pct=float(
            k_fn("fin_tx") if k_fn("fin_tx") is not None else 14.0
        ),
        fin_sistema=fin_sis,  # type: ignore[arg-type]
        desconto_pagamento_avista=desconto_vista_on,
        desconto_pagamento_avista_pct=dsk_pct,
        comissao_leiloeiro_pct_sobre_arrematacao=clei_pct,
        comissao_leiloeiro_brl=0.0,
        itbi_pct_sobre_arrematacao=itbi_pct,
        itbi_brl=0.0,
        registro_pct_sobre_arrematacao=reg_pct,
        registro_brl=reg_brl_inp,
        condominio_atrasado_brl=cond,
        iptu_atrasado_brl=iptu,
        reforma_modo=ref_mod,
        reforma_brl=reforma_brl_inp,
        desocupacao_brl=desoc,
        outros_custos_brl=outros,
        comissao_imobiliaria_brl=cimob_brl,
        comissao_imobiliaria_pct_sobre_venda=cimob_pct,
        ir_aliquota_pf_pct=ir_pf,
        ir_aliquota_pj_pct=ir_pj,
        ir_valor_manual_brl=None,
        roi_desejado_pct=(float(roi_desej) if roi_desej and float(roi_desej) > 0 else None),
        roi_desejado_modo=roi_modo,
    )
