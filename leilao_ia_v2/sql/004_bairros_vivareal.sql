-- =============================================================================
-- leilao_ia_v2 — Bairros canónicos do Viva Real por cidade (cache para URLs).
-- Aplique no SQL Editor do Supabase (public).
--
-- Se já existir `bairros_vivareal` com colunas antigas (estado, cidade, slug),
-- NÃO rode só este ficheiro — use `005_bairros_vivareal_migracao_schema_antigo.sql`.
-- =============================================================================

create table if not exists public.bairros_vivareal (
  id uuid primary key default gen_random_uuid(),
  uf_segmento text not null,
  cidade_slug text not null,
  zona_slug text not null default '',
  bairro_slug text not null,
  nome_exibicao text,
  fonte text not null default 'firecrawl_parse',
  payload_raw jsonb not null default '{}'::jsonb,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

comment on table public.bairros_vivareal is
  'Bairros (slugs) como o Viva Real usa nas URLs, por cidade/UF; evita nova descoberta a cada leilão.';

comment on column public.bairros_vivareal.uf_segmento is
  'Segmento de estado na URL (ex.: sp, rj, minas-gerais).';

comment on column public.bairros_vivareal.cidade_slug is
  'Cidade em minúsculas e hífens, sem acento (ex.: sao-paulo, rio-de-janeiro).';

comment on column public.bairros_vivareal.zona_slug is
  'Opcional: para RJ capital (ex.: zona-sul, zona-norte, zona-oeste, centro).';

comment on column public.bairros_vivareal.bairro_slug is
  'Bairro na URL (minúsculas, hífens, sem acento).';

create unique index if not exists bairros_vivareal_unico
  on public.bairros_vivareal (uf_segmento, cidade_slug, zona_slug, bairro_slug);

create index if not exists bairros_vivareal_cidade_idx
  on public.bairros_vivareal (uf_segmento, cidade_slug);
