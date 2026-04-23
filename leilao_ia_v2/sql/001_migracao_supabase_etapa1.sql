-- =============================================================================
-- leilao_ia_v2 — Etapa 1: edital (Firecrawl + extração), múltiplos caches,
-- métricas de LLM e contexto jsonb.
--
-- Aplique manualmente no SQL Editor do Supabase (schema public).
-- Ajuste nomes de constraints se o Postgres gerar nomes diferentes.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- cache_media_bairro_id (uuid FK) → cache_media_bairro_ids (uuid[])
-- Remove vínculo único; múltiplos segmentos/caches por imóvel.
-- ---------------------------------------------------------------------------
alter table public.leilao_imoveis
  drop constraint if exists leilao_imoveis_cache_media_bairro_id_fkey;

alter table public.leilao_imoveis
  drop constraint if exists leilao_imoveis_cache_media_bairro_id_fkey1;

-- Se a FK tiver nome automático, liste com:
-- select conname from pg_constraint where conrelid = 'public.leilao_imoveis'::regclass;

alter table public.leilao_imoveis
  drop column if exists cache_media_bairro_id;

alter table public.leilao_imoveis
  add column if not exists cache_media_bairro_ids uuid[]
  not null default '{}'::uuid[];

comment on column public.leilao_imoveis.cache_media_bairro_ids is
  'Lista de IDs em cache_media_bairro usados na análise (vários segmentos ou revisões). Sem FK para evitar arrays referenciados.';

create index if not exists leilao_imoveis_cache_media_bairro_ids_gin
  on public.leilao_imoveis using gin (cache_media_bairro_ids);


-- ---------------------------------------------------------------------------
-- 1ª e 2ª praça / leilões
-- ---------------------------------------------------------------------------
alter table public.leilao_imoveis add column if not exists data_leilao_1_praca date;
alter table public.leilao_imoveis add column if not exists valor_lance_1_praca double precision;
alter table public.leilao_imoveis add column if not exists data_leilao_2_praca date;
alter table public.leilao_imoveis add column if not exists valor_lance_2_praca double precision;

comment on column public.leilao_imoveis.data_leilao_1_praca is 'Data da 1ª praça / 1º leilão (edital)';
comment on column public.leilao_imoveis.valor_lance_1_praca is 'Lance mínimo ou 1ª avaliação (1ª praça)';
comment on column public.leilao_imoveis.data_leilao_2_praca is 'Data da 2ª praça quando houver';
comment on column public.leilao_imoveis.valor_lance_2_praca is 'Lance mínimo ou 2ª avaliação (2ª praça)';


-- ---------------------------------------------------------------------------
-- Edital bruto + contexto semiestruturado (JSON Schema validado na aplicação)
-- ---------------------------------------------------------------------------
alter table public.leilao_imoveis add column if not exists leilao_extra_json jsonb not null default '{}'::jsonb;

alter table public.leilao_imoveis add column if not exists edital_markdown text;

alter table public.leilao_imoveis add column if not exists edital_fonte text;

alter table public.leilao_imoveis add column if not exists edital_coletado_em timestamptz;

alter table public.leilao_imoveis add column if not exists edital_metadados_json jsonb not null default '{}'::jsonb;

comment on column public.leilao_imoveis.leilao_extra_json is
  'Dados extras para agentes: pagamento, processo, regras (ver schema v2).';
comment on column public.leilao_imoveis.edital_markdown is 'Markdown integral retornado pelo Firecrawl (pode ser grande).';
comment on column public.leilao_imoveis.edital_fonte is 'Origem do scrape, ex.: firecrawl';
comment on column public.leilao_imoveis.edital_metadados_json is 'Metadados da API Firecrawl / job / URL canônica';

create index if not exists leilao_imoveis_leilao_extra_json_gin
  on public.leilao_imoveis using gin (leilao_extra_json);


-- ---------------------------------------------------------------------------
-- Métricas da última extração LLM (tokens / custo estimado)
-- ---------------------------------------------------------------------------
alter table public.leilao_imoveis add column if not exists ultima_extracao_llm_em timestamptz;

alter table public.leilao_imoveis add column if not exists ultima_extracao_llm_modelo text;

alter table public.leilao_imoveis add column if not exists ultima_extracao_tokens_prompt integer;

alter table public.leilao_imoveis add column if not exists ultima_extracao_tokens_completion integer;

alter table public.leilao_imoveis add column if not exists ultima_extracao_custo_usd double precision;

comment on column public.leilao_imoveis.ultima_extracao_custo_usd is
  'Estimativa em USD com base em preços por 1M tokens (env OPENAI_PRICE_*).';


-- ---------------------------------------------------------------------------
-- Log textual resumido (última execução do agente de ingestão)
-- ---------------------------------------------------------------------------
alter table public.leilao_imoveis add column if not exists ultima_ingestao_log_text text;

comment on column public.leilao_imoveis.ultima_ingestao_log_text is
  'Resumo textual de etapas/avisos da última ingestão (debug).';


-- ---------------------------------------------------------------------------
-- Fim — recarregue o schema da API PostgREST se necessário.
-- =============================================================================
