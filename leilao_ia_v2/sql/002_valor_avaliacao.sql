-- =============================================================================
-- leilao_ia_v2 — Valor de avaliação (perícia / venal), distinto de lance.
-- Aplique manualmente no SQL Editor do Supabase (schema public).
-- =============================================================================

alter table public.leilao_imoveis
  add column if not exists valor_avaliacao double precision;

comment on column public.leilao_imoveis.valor_avaliacao is
  'Valor de avaliação ou perícia do imóvel no edital (float). Não é lance mínimo nem arrematação.';

-- =============================================================================
-- Fim
-- =============================================================================
