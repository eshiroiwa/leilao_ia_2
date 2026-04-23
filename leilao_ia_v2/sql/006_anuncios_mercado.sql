-- =============================================================================
-- leilao_ia_v2 — Anúncios de mercado (Viva Real e outros) para comparáveis.
-- Aplique no SQL Editor do Supabase se a tabela ainda não existir.
-- Compatível com o DDL legado do projeto (upsert por url_anuncio).
-- =============================================================================

create table if not exists public.anuncios_mercado (
  id uuid primary key default gen_random_uuid(),
  url_anuncio text not null,
  portal text not null,
  tipo_imovel text not null,
  logradouro text not null default '',
  bairro text not null,
  cidade text not null,
  estado text not null,
  nome_condominio text,
  area_construida_m2 double precision not null,
  valor_venda double precision not null,
  transacao text not null default 'venda',
  titulo text,
  quartos integer,
  preco_m2 double precision,
  metadados_json jsonb not null default '{}'::jsonb,
  primeiro_visto_em timestamptz not null default now(),
  ultima_coleta_em timestamptz not null default now(),
  created_at timestamptz not null default now(),
  constraint anuncios_mercado_url_anuncio_key unique (url_anuncio),
  constraint anuncios_mercado_transacao_check check (transacao in ('venda', 'aluguel')),
  constraint anuncios_mercado_area_chk check (area_construida_m2 > 0),
  constraint anuncios_mercado_valor_chk check (valor_venda > 0)
);

create index if not exists anuncios_mercado_geo_tipo_idx
  on public.anuncios_mercado (cidade, bairro, estado, tipo_imovel);

create index if not exists anuncios_mercado_ultima_coleta_idx
  on public.anuncios_mercado (ultima_coleta_em desc);

create index if not exists anuncios_mercado_metadados_gin
  on public.anuncios_mercado using gin (metadados_json);

alter table public.anuncios_mercado add column if not exists latitude double precision;
alter table public.anuncios_mercado add column if not exists longitude double precision;

comment on table public.anuncios_mercado is
  'Comparáveis de venda coletados na web (ex.: Viva Real), ligados ao leilão via metadados_json.leilao_imovel_id';
