"""
Integração com ``streamlit-shadcn-ui`` na aba Simulação: controlos minimalistas
(slider, switch, input, radio) com sincronização em ``st.session_state``.

Se o pacote não estiver instalado, as funções devolvem ``False`` e a app
deve desenhar os equivalentes nativos (``st.number_input``, etc.).
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_shadcn_ok: bool | None = None


def shadcn_ui_disponivel() -> bool:
    global _shadcn_ok
    if _shadcn_ok is not None:
        return _shadcn_ok
    try:
        import streamlit_shadcn_ui  # noqa: F401

        _shadcn_ok = True
    except Exception as e:  # noqa: BLE001
        logger.debug("streamlit-shadcn-ui indisponível: %s", e)
        _shadcn_ok = False
    return bool(_shadcn_ok)


def _f(x: Any, *, default: float = 0.0) -> float:
    if x is None:
        return default
    try:
        s = str(x).strip().replace(",", ".")
        if s == "":
            return default
        return float(s)
    except (TypeError, ValueError):
        return default


def _shadcn_slider_scalar(raw: Any, *, fallback: float) -> float:
    """
    O componente React do ``streamlit-shadcn-ui`` usa o Slider de intervalo:
    ``defaultValue`` e o valor devolvido são ``number[]`` (um elemento), não um escalar.
    Passar um float quebra o front-end (ex.: ``C.map is not a function``).
    """
    if isinstance(raw, (list, tuple)) and len(raw) > 0:
        return _f(raw[0], default=fallback)
    return _f(raw, default=fallback)


def _radio_option_rows(labels_values: list[tuple[str, str]]) -> list[dict[str, str]]:
    """Formato exigido pelo front-end: ``{ label, value, id }`` por opção."""
    out: list[dict[str, str]] = []
    for lab, val in labels_values:
        sid = re.sub(r"[^a-zA-Z0-9_-]+", "-", f"opt-{val}").strip("-").lower() or f"id-{len(out)}"
        out.append({"label": lab, "value": val, "id": sid})
    return out


def render_topo_prazo_e_nicho(
    iid: str,
    tvk: str,
    k_nicho: str,
    *,
    default_prazo: float,
) -> bool:
    """
    Prazo (slider 0,5–360) + interruptor nicho. Escreve em ``tvk`` e ``k_nicho``.
    """
    if not shadcn_ui_disponivel():
        return False
    try:
        import streamlit as st
        from streamlit_shadcn_ui import slider as sh_slider, switch as sh_switch

        c1, c2 = st.columns([1.15, 1.0], gap="small")
        with c1:
            cur = float(
                st.session_state.get(tvk, default_prazo)
                or default_prazo
            )
            cur = max(0.5, min(360.0, cur))
            v = sh_slider(
                default_value=[cur],
                min_value=0.5,
                max_value=360.0,
                step=0.5,
                label="Prazo até a venda (meses)",
                key=f"shadcn_tv_{iid}",
            )
            st.session_state[tvk] = _shadcn_slider_scalar(v, fallback=cur)
        with c2:
            checked = sh_switch(
                default_checked=bool(st.session_state.get(k_nicho, True)),
                label="Exibir: parcelado judicial e financ. bancário",
                key=f"shadcn_nicho_{iid}",
            )
            st.session_state[k_nicho] = bool(checked)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("shadcn topo: %s", e)
        return False


def render_aliquota_ir(
    iid: str,
    sk_ir_pf: str,
    sk_ir_pj: str,
    tipo: str,
    *,
    default_pf: float,
    default_pj: float,
) -> bool:
    """Uma caixa numérica (shadcn ``input``) para a alíquota PF ou PJ ativa."""
    if not shadcn_ui_disponivel():
        return False
    try:
        import streamlit as st
        from streamlit_shadcn_ui import input as sh_input

        if tipo == "PF":
            sk = sk_ir_pf
            d0 = default_pf
        else:
            sk = sk_ir_pj
            d0 = default_pj
        base = float(st.session_state.get(sk, d0) or d0)
        raw = sh_input(
            default_value=f"{base:g}",
            type="number",
            placeholder="Alíquota IR (%)",
            key=f"shadcn_ir_{tipo}_{iid}",
        )
        st.session_state[sk] = _f(raw, default=base)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("shadcn IR: %s", e)
        return False


def render_card_roi_sensibilidade(
    iid: str,
    sk_roi_w: str,
    sk_roi_seg: str,
    *,
    default_roi: float,
    default_seg: str,
) -> bool:
    """Slider ROI 0–200 % + rádio Bruto / Líquido (shadcn)."""
    if not shadcn_ui_disponivel():
        return False
    try:
        import streamlit as st
        from streamlit_shadcn_ui import radio_group as sh_radio, slider as sh_slider

        ro = float(st.session_state.get(sk_roi_w, default_roi) or default_roi)
        ro = max(0.0, min(200.0, ro))
        vroi = sh_slider(
            default_value=[ro],
            min_value=0.0,
            max_value=200.0,
            step=1.0,
            label="ROI desej. % (0 = desliga lance máx.)",
            key=f"shadcn_roiw_{iid}",
        )
        st.session_state[sk_roi_w] = _shadcn_slider_scalar(vroi, fallback=ro)

        prev = str(st.session_state.get(sk_roi_seg, default_seg) or default_seg)
        if prev not in ("Bruto", "Líquido"):
            prev = "Bruto"
        ch = sh_radio(
            options=_radio_option_rows(
                [("Bruto", "Bruto"), ("Líquido", "Líquido")]
            ),
            default_value=prev,
            key=f"shadcn_rois_{iid}",
        )
        st.session_state[sk_roi_seg] = str(ch) if ch in ("Bruto", "Líquido") else prev
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("shadcn ROI: %s", e)
        return False
