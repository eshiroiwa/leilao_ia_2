"""
Parâmetros de busca de comparáveis (Firecrawl Search + cache de média).

Na aplicação Streamlit, os valores vêm de ``st.session_state["busca_mercado"]``.
Fora do Streamlit (testes, agente, scripts), usam-se os padrões deste módulo.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

# Texto curto para anexar a mensagens de “poucas amostras”.
DICA_AJUSTES_BUSCA_SIDEBAR = (
    " Se faltar amostras, alargue **metragens**, **raio** ou o **mínimo de amostras** em "
    "**⚙️ Ajustes de busca** na barra lateral. O **máximo de créditos Firecrawl por análise** limita "
    "search + scrapes no edital, comparáveis e cache na mesma ingestão."
)


@dataclass(frozen=True)
class BuscaMercadoParametros:
    """Fatores em relação à área de referência do edital (área útil ou total)."""

    area_fator_min: float = 0.75
    area_fator_max: float = 1.30
    raio_km: float = 6.0
    min_amostras_cache: int = 4
    #: Teto de chamadas Firecrawl (search + scrapes) por **análise/ingestão** e por invocação de cache isolada.
    #: Inclui: 1 search (2cr) + scrape de listagens + refino top-N (até 8 scrapes individuais).
    max_firecrawl_creditos_analise: int = 20
    #: Máximo de anúncios no **cache principal** (simulação); o resto vai a caches de referência em lotes.
    cache_max_amostras_principal: int = 8
    #: Tamanho de cada **lote** de anúncios nos caches de referência (e terrenos em partes).
    cache_max_amostras_lote: int = 8


def _clamp_int(v: Any, lo: int, hi: int, default: int) -> int:
    try:
        x = int(v)
    except (TypeError, ValueError):
        x = default
    return max(lo, min(hi, x))


def _clamp_float(v: Any, lo: float, hi: float, default: float) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        x = default
    return max(lo, min(hi, x))


def parametros_de_session_state(sess: Mapping[str, Any]) -> BuscaMercadoParametros:
    """
    Constrói parâmetros a partir do estado da sessão Streamlit.

    Usa chaves planas ``bm_*`` (definidas na sidebar). Se existir ``sess["busca_mercado"]`` (dict)
    legado, também é aceite.
    """
    raw = sess.get("busca_mercado")
    d: dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}
    amin_src = sess.get("bm_area_pct_min", d.get("area_pct_min", 75))
    amax_src = sess.get("bm_area_pct_max", d.get("area_pct_max", 130))
    raio_src = sess.get("bm_raio_km", d.get("raio_km", 6.0))
    min_cache_src = sess.get("bm_min_amostras_cache", d.get("min_amostras_cache", 4))
    max_fc_src = sess.get("bm_max_firecrawl_creditos", d.get("max_firecrawl_creditos_analise", 20))
    cap_pri_src = sess.get("bm_cache_max_principal", d.get("cache_max_amostras_principal", 8))
    cap_lote_src = sess.get("bm_cache_max_lote", d.get("cache_max_amostras_lote", 8))

    amin_p = _clamp_int(amin_src, 30, 120, 75)
    amax_p = _clamp_int(amax_src, 80, 350, 130)
    if amin_p >= amax_p:
        amax_p = min(350, amin_p + 5)

    raio = _clamp_float(raio_src, 0.5, 80.0, 6.0)
    min_cache = _clamp_int(min_cache_src, 1, 25, 4)
    max_fc = _clamp_int(max_fc_src, 1, 50, 20)
    cap_pri = _clamp_int(cap_pri_src, 1, 50, 8)
    cap_lote = _clamp_int(cap_lote_src, 1, 50, 8)
    if cap_lote < 1:
        cap_lote = 1
    if cap_pri < 1:
        cap_pri = 1

    return BuscaMercadoParametros(
        area_fator_min=amin_p / 100.0,
        area_fator_max=amax_p / 100.0,
        raio_km=raio,
        min_amostras_cache=min_cache,
        max_firecrawl_creditos_analise=max_fc,
        cache_max_amostras_principal=cap_pri,
        cache_max_amostras_lote=cap_lote,
    )


def _streamlit_session_state() -> Mapping[str, Any] | None:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        if get_script_run_ctx() is None:
            return None
        import streamlit as st

        return st.session_state
    except Exception:
        return None


def get_busca_mercado_parametros() -> BuscaMercadoParametros:
    """
    Parâmetros ativos: sessão Streamlit quando há contexto de execução; caso contrário, padrão.
    """
    sess = _streamlit_session_state()
    if sess is not None:
        return parametros_de_session_state(sess)
    return BuscaMercadoParametros()


def mensagem_com_dica_ajuste_busca(texto: str) -> str:
    t = (texto or "").strip()
    if not t:
        return DICA_AJUSTES_BUSCA_SIDEBAR.strip()
    if "Ajustes de busca" in t:
        return t
    return f"{t}{DICA_AJUSTES_BUSCA_SIDEBAR}"


def defaults_chaves_busca_mercado_session() -> dict[str, Any]:
    """Valores iniciais (``setdefault``) para chaves ``bm_*`` na sessão Streamlit."""
    return {
        "bm_area_pct_min": 75,
        "bm_area_pct_max": 130,
        "bm_raio_km": 6.0,
        "bm_min_amostras_cache": 4,
        "bm_max_firecrawl_creditos": 20,
        "bm_cache_max_principal": 8,
        "bm_cache_max_lote": 8,
    }
