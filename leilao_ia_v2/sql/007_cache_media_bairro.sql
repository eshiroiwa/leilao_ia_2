-- =============================================================================
-- leilao_ia_v2 — Tabela cache_media_bairro (amostras / comparáveis agregados).
-- Para bases novas. Se já existir tabela herdada do legado, o CREATE IF NOT EXISTS
-- não altera colunas; alinhe com o DDL completo do legado se necessário.
-- =============================================================================

create table if not exists public.cache_media_bairro (
  id uuid primary key default gen_random_uuid(),
  chave_bairro text not null,
  cidade text not null,
  bairro text not null,
  preco_m2_medio double precision not null default 0,
  fonte text,
  metadados_json jsonb not null default '{}'::jsonb,
  atualizado_em timestamptz not null default now(),
  estado text,
  tipo_imovel text,
  conservacao text,
  tipo_casa text,
  faixa_andar text,
  logradouro_chave text,
  geo_bucket text,
  lat_ref double precision,
  lon_ref double precision,
  chave_segmento text not null,
  valor_medio_venda double precision,
  maior_valor_venda double precision,
  menor_valor_venda double precision,
  n_amostras integer,
  anuncios_ids text,
  nome_cache text,
  faixa_area text not null default '-'
);

create unique index if not exists cache_media_bairro_chave_segmento_key
  on public.cache_media_bairro (chave_segmento);

create index if not exists cache_media_bairro_chave_bairro_idx
  on public.cache_media_bairro (chave_bairro);

create index if not exists cache_media_bairro_cidade_estado_idx
  on public.cache_media_bairro (cidade, estado);

comment on table public.cache_media_bairro is
  'Cache de mediana/média de mercado por micro-região e segmento; anuncios_ids lista os UUIDs da amostra.';
