-- =============================================================================
-- leilao_ia_v2 — URL da foto do imóvel (quando identificável no markdown).
-- Aplique manualmente no SQL Editor do Supabase (schema public).
-- =============================================================================

alter table public.leilao_imoveis
  add column if not exists url_foto_imovel text;

comment on column public.leilao_imoveis.url_foto_imovel is
  'URL https da imagem principal do imóvel no edital (extraída do markdown quando disponível).';

-- =============================================================================
-- Fim
-- =============================================================================
