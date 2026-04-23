"""
CSS global do Streamlit — mesma paleta e tipografia do dashboard de comparação
(DM Sans, fundo em gradiente escuro, acentos verde / índigo / âmbar).
"""
from __future__ import annotations

STREAMLIT_PAGE_CSS = """
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,600;0,9..40,700;1,9..40,400&family=IBM+Plex+Mono:wght@400&display=swap" rel="stylesheet" />
<style>
:root {
  /* Tokens alinhados ao dashboard de comparação (dc-root) */
  --dc-a: 161 94% 30%;
  --dc-b: 250 60% 58%;
  --dc-c: 32 90% 55%;
  --dc-text: 210 40% 98%;
  --dc-muted: 215 20% 70%;
  --bg0: hsl(222 47% 6%);
  --bg1: hsl(217 33% 11%);
  --card: hsl(220 30% 12% / 0.88);
  --card-border: hsl(160 40% 48% / 0.22);
  --accent: hsl(160 55% 48%);
  --accent-dim: hsl(161 50% 22%);
  --accent-indigo: hsl(250 55% 72%);
  --text: hsl(var(--dc-text));
  --muted: hsl(215 16% 65%);
  --warn: #fbbf24;
  --err: #fca5a5;
  --ok: #6ee7b7;
}
html, body, [class*="css"]  {
  font-family: "DM Sans", system-ui, sans-serif !important;
  color: var(--text);
}
.stApp {
  background: linear-gradient(165deg, hsl(222 45% 8%) 0%, hsl(230 40% 6%) 45%, hsl(222 50% 5%) 100%) fixed;
}
section[data-testid="stSidebar"] {
  background: linear-gradient(180deg, rgba(12, 18, 32, 0.97) 0%, rgba(15, 23, 42, 0.95) 40%, rgba(8, 12, 24, 0.98) 100%);
  border-right: 1px solid var(--card-border);
  width: 402px !important;
  min-width: 402px !important;
  max-width: 402px !important;
  height: 100dvh !important;
  max-height: 100dvh !important;
  overflow: hidden !important;
  position: relative;
}
section[data-testid="stSidebar"] > div {
  width: 100% !important;
  height: 100dvh !important;
  max-height: 100dvh !important;
  display: flex !important;
  flex-direction: column !important;
  overflow: hidden !important;
  min-height: 0 !important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
  flex: 1 1 auto !important;
  min-height: 0 !important;
  max-height: 100% !important;
  height: 100% !important;
  overflow-y: auto !important;
  overflow-x: hidden !important;
  -webkit-overflow-scrolling: touch;
  scrollbar-gutter: stable;
  scrollbar-width: thin;
  scrollbar-color: rgba(110, 231, 183, 0.45) hsl(222 40% 8% / 0.6);
}
section[data-testid="stSidebar"] [data-testid="stSidebarContent"]::-webkit-scrollbar {
  width: 8px;
}
section[data-testid="stSidebar"] [data-testid="stSidebarContent"]::-webkit-scrollbar-thumb {
  background: rgba(110, 231, 183, 0.32);
  border-radius: 8px;
}
section[data-testid="stSidebar"] [data-testid="stSidebarContent"]::-webkit-scrollbar-track {
  background: rgba(0, 0, 0, 0.2);
}
section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
  flex: 1 1 auto !important;
  min-height: 0 !important;
  max-height: none !important;
  height: auto !important;
  overflow-y: visible !important;
  overflow-x: hidden !important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > .block-container {
  padding-top: 0.6rem;
  padding-bottom: calc(0.5rem + env(safe-area-inset-bottom, 0px));
}
section[data-testid="stSidebar"] h3 {
  font-size: 0.95rem !important;
  margin: 0 0 0.2rem 0 !important;
  line-height: 1.25 !important;
}
.lnav-brand-wrap { margin: 0 0 0.25rem 0; }
.lnav-brand-t {
  font-size: 1.08rem; font-weight: 750; letter-spacing: -0.02em; margin: 0 0 0.1rem 0; line-height: 1.2;
  background: linear-gradient(120deg, hsl(160 55% 58%), hsl(250 55% 72%));
  -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
}
.lnav-brand-s { font-size: 0.72rem; color: hsl(215 16% 62%); margin: 0; line-height: 1.4; }
.lnav-sep {
  height: 1px; margin: 0.55rem 0 0.45rem 0;
  background: linear-gradient(90deg, transparent, rgba(255,255,255,.1), transparent);
}
p.lnav-sec {
  font-size: 0.68rem;
  text-transform: uppercase;
  letter-spacing: 0.11em;
  color: hsl(215 14% 58%);
  margin: 0.35rem 0 0.4rem 0;
  font-weight: 600;
}
section[data-testid="stSidebar"] [data-testid="stButton"] > button {
  min-height: 2.45rem;
  height: auto !important;
  text-align: left !important;
  justify-content: flex-start !important;
  white-space: normal !important;
  padding: 0.45rem 0.7rem !important;
  line-height: 1.3 !important;
  border-radius: 14px !important;
  background: linear-gradient(155deg, hsl(220 30% 15% / 0.95) 0%, hsl(230 32% 10% / 0.99) 100%) !important;
  border: 1px solid rgba(255,255,255,.1) !important;
  box-shadow: 0 6px 22px -12px rgba(0,0,0,.55) !important;
  transition: border-color 0.2s ease, box-shadow 0.2s ease, transform 0.15s ease;
}
section[data-testid="stSidebar"] [data-testid="stButton"] > button:hover {
  border-color: rgba(110, 231, 183, 0.28) !important;
  transform: translateY(-1px);
}
section[data-testid="stSidebar"] [data-testid="stButton"] > button[kind="primary"] {
  border-color: rgba(45, 212, 191, 0.42) !important;
  box-shadow: 0 0 0 1px rgba(45, 212, 191, 0.12), 0 10px 28px -12px rgba(0,0,0,.6) !important;
}
.leilao-sidebar-assistente-hint {
  margin: 0 0 0.35rem 0;
  font-size: 0.72rem;
  line-height: 1.35;
  color: #94a3b8;
}
.leilao-wrap { max-width: 1680px; margin: 0 auto; padding: 0 0.5rem; }
.leilao-hero {
  text-align: left;
  padding: 0.25rem 0 1rem 0;
}
.leilao-hero h1 {
  font-weight: 700;
  font-size: 2rem;
  letter-spacing: -0.03em;
  margin: 0;
  background: linear-gradient(120deg, hsl(160 55% 58%), hsl(250 55% 72%));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
.leilao-hero p { color: var(--muted); margin-top: 0.35rem; font-size: 1rem; }
.leilao-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 1rem;
  margin-bottom: 1.5rem;
}
.leilao-card {
  background: var(--card);
  border: 1px solid var(--card-border);
  border-radius: 16px;
  padding: 1rem 1.15rem;
  backdrop-filter: blur(12px);
  box-shadow: 0 8px 32px rgba(0,0,0,0.35);
  transition: transform 0.2s ease, border-color 0.2s ease;
}
.leilao-card:hover {
  transform: translateY(-2px);
  border-color: rgba(110, 231, 183, 0.38);
}
.leilao-card-label {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: var(--muted);
  margin-bottom: 0.35rem;
}
.leilao-card-value {
  font-size: 1.05rem;
  font-weight: 600;
  word-break: break-word;
  font-family: 'IBM Plex Mono', ui-monospace, monospace;
}
.leilao-card-value.ok { color: var(--ok); }
.leilao-card-value.warn { color: var(--warn); }
.leilao-card-value.err { color: var(--err); }
.leilao-extracao-foto-wrap {
  margin-bottom: 0.5rem;
  border-radius: 12px;
  overflow: hidden;
  border: 1px solid rgba(110, 231, 183, 0.15);
  max-height: 300px;
  background: rgba(0, 0, 0, 0.22);
  display: flex;
  align-items: center;
  justify-content: center;
}
.leilao-extracao-foto-wrap img {
  width: 100%;
  max-height: 300px;
  object-fit: contain;
  vertical-align: middle;
  display: block;
}
.leilao-extracao-cards-stack {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  margin-bottom: 0.65rem;
}
/* Aba Leilão: alinhado ao painel financeiro (sim-res / sim-card) */
.leilao-extracao-panel-host { margin-top: 0.05rem; }
.leilao-foto-edital-cap {
  font-size: 0.58rem;
  color: rgba(148, 163, 184, 0.9);
  margin: 0.2rem 0 0.45rem 0;
  line-height: 1.2;
}
.leilao-extra-pre {
  margin: 0;
  font-size: 0.75rem;
  line-height: 1.5;
  color: #e8edf5;
  white-space: pre-wrap;
  word-break: break-word;
  font-family: 'IBM Plex Mono', ui-monospace, monospace;
  max-height: min(50vh, 28rem);
  overflow: auto;
}
.leilao-cache-mercado-in { margin-top: 0.05rem; }
/* KPIs do cache: mesmo bloco sp-sim-line que o detalhamento financeiro */
.leilao-cache-painel-fin {
  margin-bottom: 0.55rem;
}
.dc-root.sp-sim-financeiro .sp-sim-line-val a.leilao-ext-detalhe-a {
  color: #6ee7b7;
  font-weight: 600;
  text-decoration: none;
}
.dc-root.sp-sim-financeiro .sp-sim-line-val a.leilao-ext-detalhe-a:hover {
  text-decoration: underline;
}
.leilao-grid-mini {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(158px, 1fr));
  gap: 0.5rem;
}
.leilao-grid-mini.leilao-grid-address-only {
  grid-template-columns: 1fr;
}
.leilao-grid-mini.leilao-grid-address-only .leilao-card-mini {
  min-height: 0;
}
.leilao-grid-mini.leilao-grid-address-only .leilao-card-value {
  font-size: 0.92rem;
  line-height: 1.35;
  white-space: pre-wrap;
}
.leilao-card-mini {
  background: rgba(24, 32, 48, 0.55);
  border: 1px solid rgba(110, 231, 183, 0.12);
  border-radius: 10px;
  padding: 0.45rem 0.65rem;
  box-shadow: none;
}
.leilao-card-mini .leilao-card-label {
  font-size: 0.62rem;
  letter-spacing: 0.08em;
  margin-bottom: 0.2rem;
  opacity: 0.85;
}
.leilao-card-mini .leilao-card-value {
  font-size: 0.88rem;
  font-weight: 500;
}
.leilao-result-window {
  background: rgba(15, 23, 42, 0.72);
  border: 1px solid var(--card-border);
  border-radius: 20px;
  padding: 1.25rem 1.35rem 1.5rem;
  margin: 1rem 0 1.25rem 0;
  box-shadow: 0 12px 40px rgba(0,0,0,0.4);
}
.leilao-result-window h2 {
  margin: 0 0 1rem 0;
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--text);
}
.leilao-sidebar-metrics-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.28rem;
  margin-bottom: 0.35rem;
}
.leilao-sidebar-metric-card {
  background: rgba(24, 32, 48, 0.65);
  border: 1px solid rgba(110, 231, 183, 0.1);
  border-radius: 7px;
  padding: 0.28rem 0.38rem;
}
.leilao-sidebar-metric-card .lbl {
  font-size: 0.52rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #94a3b8;
  line-height: 1.2;
}
.leilao-sidebar-metric-card .val {
  font-size: 0.76rem;
  font-weight: 600;
  color: #e8edf5;
  margin-top: 0.1rem;
  word-break: break-word;
  line-height: 1.22;
}
.leilao-sidebar-metric-card.leilao-span2 {
  grid-column: 1 / -1;
}
.leilao-sidebar-chat-scroll {
  max-height: min(42dvh, 14rem);
  min-height: 4.5rem;
  overflow-y: auto;
  overflow-x: hidden;
  margin: 0.3rem 0 0;
  padding: 0.38rem 0.42rem 0.42rem 0.42rem;
  box-sizing: border-box;
  border: 1px solid rgba(110, 231, 183, 0.1);
  border-radius: 10px;
  background: rgba(0, 0, 0, 0.18);
}
.leilao-sidebar-msg {
  margin-bottom: 0.5rem;
  font-size: 0.82rem;
  line-height: 1.42;
}
.leilao-sidebar-msg .role {
  font-size: 0.62rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #6ee7b7;
  display: block;
  margin-bottom: 0.12rem;
}
.leilao-sidebar-msg.user .body { color: #cbd5e1; }
.leilao-sidebar-msg.asst .body { color: #e8edf5; }
.leilao-sidebar-msg:last-child {
  margin-bottom: 0.15rem;
}
section[data-testid="stSidebar"] div[data-testid="stChatInput"] {
  position: sticky !important;
  left: 0 !important;
  right: 0 !important;
  bottom: calc(0.5rem + env(safe-area-inset-bottom, 0px)) !important;
  z-index: 80;
  margin: 0.35rem 0.55rem 0.5rem 0.55rem !important;
  border-radius: 14px !important;
  pointer-events: auto;
  flex-shrink: 0 !important;
  align-self: stretch !important;
  background: rgba(15, 23, 42, 0.98) !important;
}
section[data-testid="stSidebar"] div[data-testid="stChatInput"] > div {
  box-shadow: 0 -10px 40px rgba(0, 0, 0, 0.55) !important;
  border-radius: 14px !important;
}
section[data-testid="stSidebar"] textarea[data-testid="stChatInputTextArea"] {
  max-height: min(46dvh, 15rem) !important;
}
/* Diálogos Streamlit: botões com mesma largura nas colunas */
div[data-testid="stDialog"] [data-testid="column"] button,
div[data-testid="stModal"] [data-testid="column"] button {
  width: 100% !important;
  min-height: 3rem !important;
  align-self: stretch !important;
  white-space: normal !important;
  line-height: 1.25 !important;
}
.leilao-cache-col-title {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
  margin: 0 0 0.55rem 0;
  font-weight: 600;
}
.leilao-cache-segment {
  margin-bottom: 1rem;
  padding-bottom: 0.85rem;
  border-bottom: 1px solid rgba(110, 231, 183, 0.1);
}
.leilao-cache-segment:last-child {
  border-bottom: none;
  margin-bottom: 0;
  padding-bottom: 0;
}
.leilao-cache-kpi-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
  gap: 0.45rem;
  margin-bottom: 0.6rem;
}
.leilao-cache-kpi {
  background: rgba(24, 32, 48, 0.55);
  border: 1px solid rgba(110, 231, 183, 0.12);
  border-radius: 10px;
  padding: 0.42rem 0.55rem;
}
.leilao-cache-kpi .lbl {
  font-size: 0.58rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--muted);
  margin-bottom: 0.18rem;
}
.leilao-cache-kpi .val {
  font-size: 0.82rem;
  font-weight: 600;
  color: var(--text);
  font-family: 'IBM Plex Mono', ui-monospace, monospace;
}
.leilao-cache-table-wrap {
  max-height: 280px;
  overflow: auto;
  border: 1px solid rgba(110, 231, 183, 0.12);
  border-radius: 10px;
  background: rgba(0, 0, 0, 0.15);
}
.leilao-cache-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.76rem;
}
.leilao-cache-table th,
.leilao-cache-table td {
  padding: 0.38rem 0.48rem;
  text-align: left;
  border-bottom: 1px solid rgba(148, 163, 184, 0.12);
  vertical-align: top;
}
.leilao-cache-table th {
  color: var(--muted);
  font-weight: 600;
  font-size: 0.62rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  position: sticky;
  top: 0;
  background: rgba(15, 23, 42, 0.96);
  z-index: 1;
}
.leilao-cache-table a {
  color: #6ee7b7;
  word-break: break-all;
}
.leilao-cache-empty {
  color: var(--muted);
  font-size: 0.88rem;
  padding: 0.75rem 0.25rem;
  line-height: 1.45;
}
/* st.container(border=True): cartão nativo com campos dentro (colunas da simulação) */
div[data-testid="stColumn"] [data-testid="stVerticalBlockBorderWrapper"] {
  background: linear-gradient(160deg, rgba(24, 32, 48, 0.58) 0%, rgba(15, 23, 42, 0.45) 100%) !important;
  border-radius: 12px !important;
  border: 1px solid rgba(110, 231, 183, 0.16) !important;
  box-shadow: 0 3px 14px rgba(0, 0, 0, 0.24) !important;
  margin-bottom: 0.36rem !important;
  padding: 0.06rem 0.2rem 0.28rem !important;
}
div[data-testid="stColumn"] [data-testid="stVerticalBlockBorderWrapper"] label {
  font-size: 0.5rem !important;
}
div[data-testid="stColumn"] [data-testid="stVerticalBlockBorderWrapper"] input {
  font-size: 0.6rem !important;
}
div[data-testid="stColumn"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stWidgetLabel"] {
  margin-bottom: 0.04rem !important;
}
div[data-testid="stColumn"] [data-testid="stVerticalBlockBorderWrapper"] .stNumberInput input,
div[data-testid="stColumn"] [data-testid="stVerticalBlockBorderWrapper"] .stTextInput input {
  min-height: 1.72rem !important;
  padding-top: 0.12rem !important;
  padding-bottom: 0.12rem !important;
}
.sim-card-head {
  font-size: 0.52rem;
  font-weight: 650;
  letter-spacing: 0.09em;
  text-transform: uppercase;
  color: #94a3b8;
  padding: 0.26rem 0.12rem 0.22rem;
  border-bottom: 1px solid rgba(148, 163, 184, 0.14);
  margin: 0 0 0.1rem 0;
}
.sim-card-html {
  margin-top: 0.1rem;
}
.sim-kpi-strip {
  font-size: 0.58rem;
  color: #94a3b8;
  line-height: 1.35;
  padding: 0.12rem 0 0;
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 0.2rem 0.4rem;
}
.sim-kpi-strip strong {
  color: #6ee7b7;
  font-weight: 600;
}
.sim-kpi-dot { color: rgba(148, 163, 184, 0.4); }
.sim-kpi-muted { color: rgba(148, 163, 184, 0.78); font-size: 0.6rem; }
.sim-praca-ref {
  font-size: 0.68rem;
  color: rgba(148, 163, 184, 0.92);
  line-height: 1.35;
}
.sim-praca-ref strong { color: #6ee7b7; font-weight: 600; }
.sim-cmp-painel-tit {
  font-size: 0.72rem;
  font-weight: 700;
  color: #6ee7b7;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  margin: 0.35rem 0 0.12rem 0;
  padding: 0 0.1rem;
}
.sim-op-h {
  font-size: 0.58rem;
  font-weight: 600;
  color: var(--accent);
  margin: 0.14rem 0 0.06rem 0;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
div[data-testid="stExpander"] details summary + div .sim-op-tight label {
  font-size: 0.5rem !important;
  margin-bottom: 0.02rem !important;
}
div[data-testid="stExpander"] details summary + div .sim-op-tight input {
  padding: 0.04rem 0.12rem !important;
  min-height: 1.22rem !important;
  font-size: 0.6rem !important;
}
div[data-testid="stExpander"] details summary + div .sim-op-tight [data-testid="stWidgetLabel"] {
  margin-bottom: 0.06rem !important;
}
.sim-res-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(168px, 1fr));
  gap: 0.58rem;
  margin: 0.48rem 0 0.58rem 0;
}
.sim-res-card {
  background: linear-gradient(165deg, rgba(24, 32, 48, 0.95) 0%, rgba(15, 23, 42, 0.88) 100%);
  border: 1px solid rgba(110, 231, 183, 0.2);
  border-radius: 12px;
  padding: 0.68rem 0.62rem 0.58rem;
  text-align: center;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.28);
}
.sim-res-card--accent {
  border-color: rgba(110, 231, 183, 0.45);
  background: linear-gradient(165deg, rgba(17, 94, 89, 0.35) 0%, rgba(24, 32, 48, 0.92) 100%);
}
.sim-res-card .sim-res-lbl {
  font-size: 0.64rem;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-weight: 600;
  line-height: 1.25;
}
.sim-res-card .sim-res-val {
  font-size: 1.08rem;
  font-weight: 700;
  color: #6ee7b7;
  margin-top: 0.3rem;
  line-height: 1.18;
  font-variant-numeric: tabular-nums;
}
.sim-res-card .sim-res-val.muted { color: var(--muted); font-weight: 500; font-size: 0.95rem; }
.sim-res-card .sim-res-val.ok { color: #6ee7b7; }
.sim-res-card .sim-res-val.warn { color: #fbbf24; }
.sim-res-card .sim-res-val.err { color: #f87171; }
.sim-res-card .sim-res-sub {
  font-size: 0.62rem;
  color: rgba(148, 163, 184, 0.85);
  margin-top: 0.28rem;
}
.sim-fin-sec {
  margin: 0.32rem 0 0.1rem 0;
}
.sim-fin-h {
  font-size: 0.58rem;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-weight: 600;
  margin: 0 0 0.18rem 0;
}
.sim-panel-lista {
  border: 1px solid rgba(110, 231, 183, 0.2);
  border-radius: 14px;
  padding: 0.75rem 0.85rem 0.85rem;
  background: rgba(15, 23, 42, 0.55);
}
.sim-res-col-scroll {
  /* Painel financeiro: altura livre, sem rolagem interna (o fluxo da página rola se precisar). */
  max-height: none;
  min-height: 0;
  overflow: visible;
  padding-right: 0.35rem;
  margin-top: 0.05rem;
}
.sim-mercado-ctx-wrap { margin-top: 0.08rem; }
.sim-mercado-ctx-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 0.55rem;
  margin-top: 0.32rem;
}
.sim-mercado-ctx-card {
  border: 1px solid rgba(110, 231, 183, 0.18);
  border-radius: 12px;
  padding: 0.55rem 0.62rem 0.52rem;
  background: rgba(15, 23, 42, 0.65);
  text-align: left;
}
.sim-mercado-ctx-card .sim-mercado-ctx-tit {
  font-size: 0.62rem;
  font-weight: 650;
  color: #6ee7b7;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  margin-bottom: 0.35rem;
  line-height: 1.25;
}
.sim-mercado-ctx-card ul {
  margin: 0;
  padding-left: 1.05em;
  font-size: 0.72rem;
  color: #e8edf5;
  line-height: 1.42;
}
.sim-mercado-ctx-card li { margin-bottom: 0.22rem; }
.sim-lista-rodape {
  margin-top: 1.25rem;
  padding-top: 0.35rem;
}
/* Filtros da tabela de leilões (topo): mesmo peso visual dos cartões da simulação */
.lista-leiloes-filtros [data-testid="stWidgetLabel"] p {
  font-size: 0.52rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.06em !important;
  text-transform: uppercase !important;
  color: #94a3b8 !important;
}
.lista-leiloes-filtros [data-baseweb="select"] > div {
  font-size: 0.78rem !important;
}
.lista-leiloes-filtros [data-testid="stTextInput"] input {
  font-size: 0.78rem !important;
}
.leilao-alerta-amostras {
  border: 1px solid rgba(251, 191, 36, 0.48);
  border-radius: 14px;
  padding: 0.85rem 1rem 0.75rem 1rem;
  margin: 0.45rem 0 0.85rem 0;
  background: linear-gradient(165deg, rgba(120, 53, 15, 0.5) 0%, rgba(30, 20, 12, 0.55) 100%);
  box-shadow: 0 4px 18px rgba(0, 0, 0, 0.28);
}
.leilao-alerta-amostras-title {
  color: #fde68a;
  font-weight: 650;
  font-size: 0.92rem;
  margin: 0 0 0.4rem 0;
  letter-spacing: 0.02em;
}
.leilao-alerta-amostras-body {
  color: #e8edf5;
  font-size: 0.84rem;
  line-height: 1.48;
  margin: 0 0 0.45rem 0;
}
.leilao-alerta-amostras-list {
  margin: 0.2rem 0 0 1.1rem;
  padding: 0;
  color: #fde68a;
  font-size: 0.82rem;
  line-height: 1.5;
}
.leilao-alerta-amostras-list li { margin-bottom: 0.2rem; }

/* --- Controles nativos Streamlit (Base Web) — mesma paleta --- */
div[data-baseweb="tab-list"] {
  background: transparent !important;
  gap: 0.1rem 0.35rem !important;
  border-bottom: 1px solid hsl(220 16% 22% / 0.5) !important;
}
div[data-baseweb="tab"] {
  color: hsl(var(--dc-muted)) !important;
  font-weight: 500 !important;
  border: none !important;
}
div[data-baseweb="tab"][aria-selected="true"] {
  color: #6ee7b7 !important;
  font-weight: 600 !important;
  border-bottom: 2px solid hsl(160 48% 48% / 0.85) !important;
  border-radius: 0 !important;
}
.stButton > button {
  font-family: "DM Sans", system-ui, sans-serif !important;
  border-radius: 10px !important;
  transition: box-shadow 0.15s ease, filter 0.15s ease !important;
}
.stButton > button[kind="primary"] {
  background: linear-gradient(160deg, hsl(160 40% 36%) 0%, hsl(168 32% 30%) 100%) !important;
  border: 1px solid hsl(160 38% 45% / 0.4) !important;
  color: #f1f5f9 !important;
  font-weight: 600 !important;
  box-shadow: 0 4px 18px -6px rgba(0,0,0,0.45) !important;
}
.stButton > button[kind="primary"]:hover { filter: brightness(1.05); }
.stButton > button[kind="secondary"] {
  background: hsl(220 24% 16% / 0.9) !important;
  color: #e2e8f0 !important;
  border: 1px solid hsl(220 18% 30% / 0.5) !important;
}
div[data-baseweb="select"] > div, div[data-baseweb="input"] input, textarea {
  font-family: "DM Sans", system-ui, sans-serif !important;
}
div[data-baseweb="slider"] { filter: hue-rotate(-8deg) saturate(0.9); }
div[data-testid="stMetricValue"] { color: #6ee7b7 !important; }
div[data-testid="stMetricLabel"] { color: hsl(var(--dc-muted)) !important; }
</style>
"""
