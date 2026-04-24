"""
Interface Streamlit — ingestão de edital (URL avulsa ou planilha .xlsx/.csv só com URLs).

Execute na raiz do repositório:
  streamlit run leilao_ia_v2/app_ingestao.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st

from leilao_ia_v2.exceptions import (
    EscolhaSobreDuplicataNecessaria,
    IngestaoSemConteudoEditalError,
    UrlInvalidaIngestaoError,
)
from leilao_ia_v2.services.conteudo_edital_heuristica import MENSAGEM_ACOES_USUARIO
from leilao_ia_v2.pipeline.ingestao_edital import executar_ingestao_edital
from leilao_ia_v2.planilha_urls import ler_urls_de_planilha
from leilao_ia_v2.normalizacao import normalizar_url_leilao
from leilao_ia_v2.supabase_client import get_supabase_client
from leilao_ia_v2.ui.app_theme import STREAMLIT_PAGE_CSS

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Leilão IA v2 — Ingestão", layout="wide")
st.html(STREAMLIT_PAGE_CSS)

if "pending_duplicate_url" not in st.session_state:
    st.session_state.pending_duplicate_url = None
if "pending_duplicate_registro" not in st.session_state:
    st.session_state.pending_duplicate_registro = None

st.title("Leilão IA v2 — Ingestão de edital")
st.caption("Firecrawl + LLM + Supabase. Duplicata: confirme se deseja sobrescrever.")

ignorar_cache = st.sidebar.checkbox("Ignorar cache em disco do Firecrawl", value=False)

if st.session_state.pending_duplicate_registro and st.session_state.pending_duplicate_url:
    reg = st.session_state.pending_duplicate_registro
    st.warning(
        f"**URL já cadastrada** — {reg.get('cidade') or '—'}/{reg.get('estado') or '—'}. "
        "Informe outra URL na aba abaixo ou toque em *Dispensar*."
    )
    if st.button("Dispensar aviso", type="secondary", key="ingestao_dup_dismiss"):
        st.session_state.pending_duplicate_url = None
        st.session_state.pending_duplicate_registro = None
        st.rerun()
    with st.expander("Atualizar registro (avançado)"):
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Sobrescrever registro existente", type="primary", key="ingestao_dup_over"):
                cli = get_supabase_client()
                r = executar_ingestao_edital(
                    st.session_state.pending_duplicate_url,
                    cli,
                    sobrescrever_duplicata=True,
                    ignorar_cache_firecrawl=ignorar_cache,
                )
                st.success(f"Atualizado: {r.modo} id={r.id}")
                st.text(r.log)
                st.session_state.pending_duplicate_url = None
                st.session_state.pending_duplicate_registro = None
                st.rerun()
        with c2:
            if st.button("Manter (não alterar)", key="ingestao_dup_keep"):
                cli = get_supabase_client()
                r = executar_ingestao_edital(
                    st.session_state.pending_duplicate_url,
                    cli,
                    sobrescrever_duplicata=False,
                    ignorar_cache_firecrawl=ignorar_cache,
                )
                st.info(r.log)
                st.session_state.pending_duplicate_url = None
                st.session_state.pending_duplicate_registro = None
                st.rerun()

tab1, tab2 = st.tabs(["URL avulsa", "Planilha (URLs)"])

with tab1:
    url_avulsa = st.text_input("URL do leilão", placeholder="https://...")
    if st.button("Ingerir URL", type="primary"):
        if not (url_avulsa or "").strip():
            st.error("Informe uma URL.")
        else:
            cli = get_supabase_client()
            try:
                r = executar_ingestao_edital(
                    url_avulsa.strip(),
                    cli,
                    sobrescrever_duplicata=None,
                    ignorar_cache_firecrawl=ignorar_cache,
                )
                st.success(f"Concluído: {r.modo} id={r.id}")
                st.text(r.log)
                st.json(r.metricas_llm)
            except EscolhaSobreDuplicataNecessaria as dup:
                st.session_state.pending_duplicate_url = normalizar_url_leilao(url_avulsa.strip())
                st.session_state.pending_duplicate_registro = dup.registro_existente
                st.warning("URL já cadastrada. Use os botões acima para sobrescrever ou manter.")
                st.rerun()
            except IngestaoSemConteudoEditalError as e:
                st.warning("Conteúdo insuficiente para edital — **nada foi gravado** no banco.")
                st.markdown(f"**Motivo:** {e.motivo}")
                st.info(MENSAGEM_ACOES_USUARIO)
            except UrlInvalidaIngestaoError as e:
                st.error(f"URL inválida ou conteúdo indisponível — nada foi gravado: {e}")

with tab2:
    arq = st.file_uploader("Planilha (.xlsx ou .csv)", type=["xlsx", "csv", "xls"])
    if arq and st.button("Processar planilha"):
        suf = Path(arq.name).suffix.lower()
        with tempfile.NamedTemporaryFile(delete=False, suffix=suf) as tmp:
            tmp.write(arq.getvalue())
            tmp_path = Path(tmp.name)
        try:
            urls = ler_urls_de_planilha(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        cli = get_supabase_client()
        progress = st.progress(0.0, text="Iniciando…")
        for i, u in enumerate(urls):
            progress.progress((i + 1) / max(len(urls), 1), text=f"URL {i + 1}/{len(urls)}")
            st.markdown(f"**{u}**")
            try:
                r = executar_ingestao_edital(
                    u,
                    cli,
                    sobrescrever_duplicata=None,
                    ignorar_cache_firecrawl=ignorar_cache,
                )
                st.success(f"  → {r.modo} id={r.id}")
            except EscolhaSobreDuplicataNecessaria:
                st.warning(
                    "  → Duplicata encontrada. O processamento em lote não altera duplicatas automaticamente. "
                    "Use a aba URL avulsa para essa URL ou reenvie a planilha após resolver no banco."
                )
                st.stop()
            except IngestaoSemConteudoEditalError as e:
                st.warning(f"  → Sem edital na página (nada gravado): {e.motivo}")
            except UrlInvalidaIngestaoError as e:
                st.error(f"  → Falhou (nada gravado): {e}")
        progress.progress(1.0, text="Concluído")
