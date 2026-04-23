-- =============================================================================
-- leilao_ia_v2 — Migração: schema antigo de bairros_vivareal → schema v2
--
-- Causa do erro 42703: já existia `bairros_vivareal` com colunas
--   estado, cidade, slug, nome_humanizado (ex.: codigo referencia/supabase_ddls).
-- O 004 usa `CREATE TABLE IF NOT EXISTS`, que não recria a tabela.
--
-- Uso no SQL Editor do Supabase:
--   1) Rode este ficheiro UMA vez.
--   2) Se ainda não tiver a tabela v2 nunca criada, pode também rodar 004
--      (este 005 já inclui o CREATE completo após renomear o legado).
-- =============================================================================

-- Renomeia só se ainda for o layout legado (tem `estado`, não tem `uf_segmento`).
do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'bairros_vivareal'
      and column_name = 'estado'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'bairros_vivareal'
      and column_name = 'uf_segmento'
  ) then
    execute 'alter table public.bairros_vivareal rename to bairros_vivareal_v1_backup';
  end if;
end $$;

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
  'Opcional na prática: para RJ capital (ex.: zona-sul, zona-norte, zona-oeste, centro). Vazio = string vazia.';

comment on column public.bairros_vivareal.bairro_slug is
  'Bairro na URL (minúsculas, hífens, sem acento).';

create unique index if not exists bairros_vivareal_unico
  on public.bairros_vivareal (uf_segmento, cidade_slug, zona_slug, bairro_slug);

create index if not exists bairros_vivareal_cidade_idx
  on public.bairros_vivareal (uf_segmento, cidade_slug);

-- Copia dados do backup só se a tabela de backup existir (evita erro de relação inexistente).
do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public'
      and table_name = 'bairros_vivareal_v1_backup'
  ) then
    execute $mig$
      insert into public.bairros_vivareal (
        uf_segmento,
        cidade_slug,
        zona_slug,
        bairro_slug,
        nome_exibicao,
        fonte,
        payload_raw,
        criado_em,
        atualizado_em
      )
      select
        lower(trim(estado)),
        lower(trim(cidade)),
        '',
        lower(trim(slug)),
        nullif(trim(nome_humanizado), ''),
        'migrated_from_v1',
        '{}'::jsonb,
        coalesce(atualizado_em, now()),
        coalesce(atualizado_em, now())
      from public.bairros_vivareal_v1_backup
      on conflict (uf_segmento, cidade_slug, zona_slug, bairro_slug) do nothing
    $mig$;
  end if;
end $$;

-- Pode apagar manualmente o backup após validar: drop table if exists public.bairros_vivareal_v1_backup;
