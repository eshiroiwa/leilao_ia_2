"""
UI da Precificação v2 (Sprint 4).

Renderiza um card no painel de detalhes do leilão com:

- valor estimado + faixa P20–P80;
- badge de **veredito** (FORTE/OPORTUNIDADE/NEUTRA/RISCO/EVITAR/...);
- badge de **confiança** (ALTA/MEDIA/BAIXA/INSUFICIENTE);
- **alerta de liquidez** quando o alvo está fora do padrão;
- meta-info da expansão progressiva (raio final, área relax, tipo);
- tabela colapsável com as amostras usadas;
- botão **Recalcular** que chama
  :func:`leilao_ia_v2.precificacao.integracao.precificar_leilao` sob demanda.

Helpers de formatação/extração são **puros** (sem `streamlit`) para que
possam ser testados isoladamente — ver
``leilao_ia_v2/tests/precificacao/integracao/test_ui_precificacao_v2.py``.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import streamlit as st

from leilao_ia_v2.precificacao.integracao.persistencia import METADADO_KEY

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Helpers puros (sem streamlit) — testáveis
# -----------------------------------------------------------------------------

# Mapeamento de cores por veredito (paleta consistente com `dc-badge`).
_CORES_VEREDITO: dict[str, tuple[str, str]] = {
    "FORTE":         ("#0f5132", "#d1e7dd"),  # verde escuro / verde claro
    "OPORTUNIDADE":  ("#0a3622", "#a3cfbb"),
    "NEUTRA":        ("#41464b", "#e2e3e5"),  # cinza
    "RISCO":         ("#664d03", "#ffecb5"),  # amarelo/âmbar
    "EVITAR":        ("#58151c", "#f1aeb5"),  # vermelho
    "INSUFICIENTE":  ("#41464b", "#e2e3e5"),
    "SEM_LANCE":     ("#41464b", "#e2e3e5"),
}

_CORES_CONFIANCA: dict[str, tuple[str, str]] = {
    "ALTA":         ("#0f5132", "#d1e7dd"),
    "MEDIA":        ("#664d03", "#ffecb5"),
    "BAIXA":        ("#58151c", "#f1aeb5"),
    "INSUFICIENTE": ("#41464b", "#e2e3e5"),
}

_CORES_LIQUIDEZ: dict[str, tuple[str, str]] = {
    "ok":    ("#0f5132", "#d1e7dd"),
    "media": ("#664d03", "#ffecb5"),
    "alta":  ("#58151c", "#f1aeb5"),
}


def extrair_precificacao_v2(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extrai o snapshot ``precificacao_v2`` de uma row de ``leilao_imoveis``.

    Devolve ``None`` quando:
    - row é ``None`` ou não-dict;
    - ``leilao_extra_json`` ausente/inválido;
    - chave ``precificacao_v2`` ausente.

    Não levanta — é usado em código de UI que precisa degradar com graça.
    """
    if not isinstance(row, dict):
        return None
    extra = row.get("leilao_extra_json")
    if not isinstance(extra, dict):
        return None
    pv2 = extra.get(METADADO_KEY)
    if not isinstance(pv2, dict):
        return None
    return pv2


def formatar_brl(v: Any, *, sufixo: str = "") -> str:
    """Formata um valor numérico como BRL (``R$ 1.234.567,89``).

    ``None``/strings vazias/``NaN`` → ``"—"``.
    Aceita ``sufixo`` opcional (ex.: ``"/m²"``).
    """
    if v is None or v == "":
        return "—"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if x != x:  # NaN
        return "—"
    inteiro = int(abs(round(x * 100))) // 100
    cent = int(abs(round(x * 100))) % 100
    neg = "-" if x < 0 else ""
    corpo = f"{inteiro:,}".replace(",", ".")
    return f"{neg}R$ {corpo},{cent:02d}{sufixo}"


def formatar_pct(v: Any, *, casas: int = 1) -> str:
    """Formata fração ou inteiro como percentagem (``"43.0%"``)."""
    if v is None or v == "":
        return "—"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if x != x:
        return "—"
    return f"{x:.{int(casas)}f}%"


def cor_veredito(nivel: str) -> tuple[str, str]:
    """Devolve (cor_texto, cor_fundo) para um nível de veredito.

    Cinza neutro para níveis desconhecidos.
    """
    return _CORES_VEREDITO.get((nivel or "").strip().upper(), ("#41464b", "#e2e3e5"))


def cor_confianca(nivel: str) -> tuple[str, str]:
    return _CORES_CONFIANCA.get((nivel or "").strip().upper(), ("#41464b", "#e2e3e5"))


def cor_liquidez(severidade: str) -> tuple[str, str]:
    return _CORES_LIQUIDEZ.get((severidade or "").strip().lower(), ("#41464b", "#e2e3e5"))


def _badge_html(label: str, *, cor_txt: str, cor_bg: str) -> str:
    """HTML inline de uma badge — estilo consistente com `dc-badge`."""
    return (
        f'<span style="display:inline-block; padding:0.2rem 0.55rem; '
        f'border-radius:0.4rem; font-size:0.72rem; font-weight:700; '
        f'letter-spacing:0.06em; color:{cor_txt}; background:{cor_bg}; '
        f'text-transform:uppercase;">{label}</span>'
    )


def montar_html_resumo(precificacao: dict[str, Any]) -> str:
    """Monta o HTML do resumo principal (valor + faixa + badges).

    Função pura — montagem puramente textual, testável sem streamlit.
    """
    val_est = precificacao.get("valor_estimado")
    p20 = precificacao.get("p20_total")
    p80 = precificacao.get("p80_total")

    veredito = (precificacao.get("veredito") or {}).get("nivel", "—")
    veredito_desc = (precificacao.get("veredito") or {}).get("descricao", "")
    confianca = (precificacao.get("confianca") or {}).get("nivel", "—")
    conf_motivo = (precificacao.get("confianca") or {}).get("motivo", "")

    cv_txt, cv_bg = cor_veredito(veredito)
    cf_txt, cf_bg = cor_confianca(confianca)

    badge_v = _badge_html(veredito.replace("_", " "), cor_txt=cv_txt, cor_bg=cv_bg)
    badge_c = _badge_html(f"Confiança {confianca}", cor_txt=cf_txt, cor_bg=cf_bg)

    valor_str = formatar_brl(val_est)
    faixa_str = (
        f"{formatar_brl(p20)} — {formatar_brl(p80)}"
        if (p20 is not None and p80 is not None)
        else "—"
    )

    return (
        f'<div style="margin-bottom:0.4rem;">{badge_v} &nbsp; {badge_c}</div>'
        f'<div style="font-size:1.45rem; font-weight:700; line-height:1.1;">{valor_str}</div>'
        f'<div style="font-size:0.82rem; color:#64748b; margin-top:0.15rem;">'
        f'Faixa P20–P80: <b>{faixa_str}</b></div>'
        f'<div style="font-size:0.82rem; color:#475569; margin-top:0.45rem;">{veredito_desc}</div>'
        f'<div style="font-size:0.74rem; color:#94a3b8; margin-top:0.1rem;">{conf_motivo}</div>'
    )


def montar_html_alerta_liquidez(precificacao: dict[str, Any]) -> Optional[str]:
    """Monta HTML do alerta de liquidez quando severidade > ok.

    Devolve ``None`` quando severidade é ``ok`` (não exibe nada).
    """
    al = precificacao.get("alerta_liquidez") or {}
    severidade = (al.get("severidade") or "").strip().lower()
    if severidade not in {"media", "alta"}:
        return None
    msg = al.get("mensagem", "")
    razao = al.get("razao_area")
    fator = al.get("fator_aplicado")
    cor_txt, cor_bg = cor_liquidez(severidade)
    badge = _badge_html(f"Liquidez {severidade}", cor_txt=cor_txt, cor_bg=cor_bg)
    razao_str = (
        f"razão alvo/amostras = {razao:.2f}" if isinstance(razao, (int, float)) else ""
    )
    fator_str = (
        f"fator aplicado = {fator:.2f}×" if isinstance(fator, (int, float)) else ""
    )
    sufixo = " · ".join([s for s in (razao_str, fator_str) if s])
    return (
        f'<div style="margin-top:0.6rem; padding:0.55rem 0.7rem; '
        f'background:{cor_bg}; border-radius:0.4rem;">'
        f'{badge}'
        f'<div style="margin-top:0.3rem; font-size:0.83rem; color:{cor_txt};">{msg}</div>'
        + (f'<div style="font-size:0.72rem; color:{cor_txt}; opacity:0.85;">{sufixo}</div>' if sufixo else "")
        + '</div>'
    )


def montar_linhas_meta(precificacao: dict[str, Any]) -> list[str]:
    """Devolve linhas de meta-info para mostrar abaixo do resumo principal."""
    estat = precificacao.get("estatistica") or {}
    expansao = precificacao.get("expansao") or {}
    out: list[str] = []
    n_uteis = estat.get("n_uteis")
    n_outlier = estat.get("n_descartados_outlier")
    if n_uteis is not None:
        n_outlier_str = f" ({n_outlier} outlier{'s' if (n_outlier or 0) != 1 else ''} descartados)" if n_outlier else ""
        out.append(f"**Amostras úteis:** {n_uteis}{n_outlier_str}")
    cv = estat.get("cv_pct")
    if cv is not None:
        out.append(f"**Dispersão (CV):** {formatar_pct(cv)}")
    mediana = estat.get("mediana_r_m2")
    if mediana is not None:
        out.append(f"**Mediana R$/m²:** {formatar_brl(mediana, sufixo='/m²')}")
    raio = expansao.get("raio_final_m")
    niveis = expansao.get("niveis_expansao_aplicados")
    if raio is not None:
        sufixo_exp = ""
        if niveis:
            sufixo_exp = f" (após {niveis} expansão{'ões' if niveis > 1 else ''})"
        out.append(f"**Raio final:** {raio} m{sufixo_exp}")
    if expansao.get("tipo_relax_aplicado"):
        out.append("**Tipos próximos:** ativo (ex.: casa↔sobrado)")
    return out


# -----------------------------------------------------------------------------
# Render principal (Streamlit)
# -----------------------------------------------------------------------------


def _render_tabela_amostras(precificacao: dict[str, Any]) -> None:
    """Renderiza a tabela colapsável com as amostras usadas."""
    amostras = precificacao.get("amostras") or []
    if not amostras:
        st.caption("Nenhuma amostra serializada (precificação insuficiente).")
        return

    import pandas as pd  # local import — Streamlit já o tem

    df = pd.DataFrame([
        {
            "URL": a.get("url", ""),
            "Tipo": a.get("tipo", ""),
            "Área (m²)": a.get("area_m2"),
            "Valor anúncio": a.get("valor"),
            "R$/m² bruto": a.get("preco_m2_bruto"),
            "R$/m² ajustado": a.get("preco_m2_ajustado"),
            "Distância (km)": a.get("distancia_km"),
            "Precisão geo": a.get("precisao_geo"),
            "Raio origem (m)": a.get("raio_origem_m"),
        }
        for a in amostras
    ])
    st.dataframe(
        df,
        width="stretch",
        height=min(420, max(180, 56 + len(amostras) * 32)),
        hide_index=True,
        column_config={
            "URL": st.column_config.LinkColumn("URL", display_text="abrir"),
            "Valor anúncio": st.column_config.NumberColumn(format="R$ %.2f"),
            "R$/m² bruto": st.column_config.NumberColumn(format="R$ %.2f"),
            "R$/m² ajustado": st.column_config.NumberColumn(format="R$ %.2f"),
            "Distância (km)": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def _refrescar_session_state_apos_recalculo(client: Any, leilao_id: str) -> None:
    """Recarrega ``leilao_imoveis`` e atualiza ``ultimo_extracao`` na sessão.

    O snapshot ``precificacao_v2`` é gravado em ``leilao_extra_json``; sem
    isto, o ``st.rerun`` re-renderiza com a row em cache (ainda sem o JSON
    novo), dando a falsa impressão de que nada mudou.
    """
    try:
        from leilao_ia_v2.persistence import leilao_imoveis_repo
    except Exception:
        return
    try:
        fresh = leilao_imoveis_repo.buscar_por_id(leilao_id, client)
    except Exception:
        logger.exception("ui.precificacao_v2: refresh row falhou (lid=%s)", leilao_id[:12])
        return
    if isinstance(fresh, dict):
        st.session_state["ultimo_extracao"] = fresh


def _executar_recalculo(client: Any, leilao_id: str) -> None:
    """Chama :func:`precificar_leilao` em modo persistente.

    Isolado em uma função para facilitar mock em testes futuros.
    Devolve via toast/info; recarrega a página ao fim.
    """
    try:
        from leilao_ia_v2.precificacao.integracao import precificar_leilao
    except Exception:
        st.error("Módulo de precificação não disponível.")
        logger.exception("ui.precificacao_v2: import falhou")
        return
    try:
        r = precificar_leilao(client, leilao_id)
    except Exception:
        logger.exception("ui.precificacao_v2: recálculo falhou (lid=%s)", leilao_id[:12])
        st.error("Falha inesperada no recálculo.")
        return
    if r.ok and r.persistido:
        _refrescar_session_state_apos_recalculo(client, leilao_id)
        st.success(f"Precificação atualizada — {r.motivo}")
    elif r.ok:
        st.warning(f"Calculado mas não persistido — {r.motivo}")
    else:
        st.error(f"Não foi possível recalcular: {r.motivo}")


def render_card(
    row: dict[str, Any],
    *,
    client: Any = None,
    on_recalcular: Any = None,
    key_prefix: str = "precv2",
) -> None:
    """Renderiza o card "Precificação v2" para um leilão.

    Args:
        row: row de ``leilao_imoveis`` (precisa ter ``id`` e
            ``leilao_extra_json``; tudo opcional fora isso).
        client: Supabase client — necessário se quiser oferecer recálculo.
        on_recalcular: callback opcional ``(client, leilao_id) -> None``;
            usa :func:`_executar_recalculo` se não for passado.
        key_prefix: prefixo para chaves de widgets Streamlit (evita colisão
            quando o card aparece em múltiplos contextos na mesma página).
    """
    with st.container(border=True):
        st.markdown(
            '<div class="sim-card-head">Precificação v2 — comparáveis</div>',
            unsafe_allow_html=True,
        )

        leilao_id = str((row or {}).get("id") or "").strip()
        precif = extrair_precificacao_v2(row)

        col_resumo, col_botao = st.columns([4, 1], gap="small")

        with col_resumo:
            if precif is None:
                st.caption(
                    "Ainda sem precificação v2 para este leilão. Use o botão "
                    "**Recalcular** ao lado para gerar agora a partir dos "
                    "anúncios já em `anuncios_mercado`."
                )
            else:
                st.markdown(montar_html_resumo(precif), unsafe_allow_html=True)
                alerta_html = montar_html_alerta_liquidez(precif)
                if alerta_html:
                    st.markdown(alerta_html, unsafe_allow_html=True)

        with col_botao:
            disabled = not (leilao_id and client is not None)
            tooltip = (
                "Recalcula a precificação consultando `anuncios_mercado` "
                "agora. Não consome créditos do Firecrawl."
            ) if not disabled else "Selecione um leilão e tenha o Supabase conectado."
            if st.button(
                "↻ Recalcular",
                key=f"{key_prefix}_btn_recalc_{leilao_id or 'none'}",
                use_container_width=True,
                disabled=disabled,
                help=tooltip,
            ):
                cb = on_recalcular or _executar_recalculo
                cb(client, leilao_id)
                st.rerun()

        if precif is not None:
            linhas_meta = montar_linhas_meta(precif)
            if linhas_meta:
                st.caption(" · ".join(linhas_meta))

            with st.expander(
                f"Amostras usadas ({len(precif.get('amostras') or [])})",
                expanded=False,
            ):
                _render_tabela_amostras(precif)
