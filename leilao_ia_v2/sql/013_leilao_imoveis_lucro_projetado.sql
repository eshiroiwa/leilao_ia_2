-- =============================================================================
-- leilao_ia_v2 — Lucro bruto / líquido e ROI líquido projetados (pós-cache, sem simulação gravada).
-- Aplique no SQL Editor do Supabase (public).
-- =============================================================================

alter table public.leilao_imoveis
  add column if not exists lucro_bruto_projetado double precision;
alter table public.leilao_imoveis
  add column if not exists lucro_liquido_projetado double precision;
alter table public.leilao_imoveis
  add column if not exists roi_liquido_projetado double precision;

comment on column public.leilao_imoveis.lucro_bruto_projetado is
  'Projeção pós-cache (R$): venda líq. − investimento, com 6% corretagem s/ venda e alíquotas do agente.';

comment on column public.leilao_imoveis.lucro_liquido_projetado is
  'Projeção pós-cache (R$): lucro bruto − 15% IR (PF) sobre o lucro bruto positivo.';

comment on column public.leilao_imoveis.roi_liquido_projetado is
  'Projeção pós-cache (fração 0–1): lucro líquido / investimento (mesma base do ROI bruto).';

comment on column public.leilao_imoveis.roi_projetado is
  'ROI bruto projetado (fração): lucro bruto pós 6% corretagem s/ venda, sobre investimento.';

comment on column public.leilao_imoveis.lance_maximo_recomendado is
  'Lance (R$) alvo com ROI bruto 50% nas premissas do agente (incl. 6% corretagem na saída).';
