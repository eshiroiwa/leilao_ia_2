-- =============================================================================
-- leilao-ia — DDL Supabase (leilao_imoveis + cache_media_bairro)
-- Schema: public (ajuste se usar outro)
-- =============================================================================
--
-- Use UMA das opções abaixo:
--   A) OPÇÃO 1 — só adiciona/ajusta colunas (mantém dados)
--   B) OPÇÃO 2 — apaga e recria tabelas (PERDE todos os dados)
--
-- Depois: RLS / políticas / grants conforme seu projeto; recarregue o schema da API.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- OPÇÃO 1A — leilao_imoveis: novos campos de segmento + bairro (idempotente)
-- ---------------------------------------------------------------------------
alter table public.leilao_imoveis add column if not exists bairro text;
alter table public.leilao_imoveis add column if not exists tipo_imovel text;
alter table public.leilao_imoveis add column if not exists conservacao text;
alter table public.leilao_imoveis add column if not exists tipo_casa text;
alter table public.leilao_imoveis add column if not exists andar integer;
-- opcional: padrão de reforma vindo da planilha (baixo | medio | alto)
alter table public.leilao_imoveis add column if not exists padrao_imovel text;
-- data do leilão (sessão / 1ª praça etc.)
alter table public.leilao_imoveis add column if not exists data_leilao date;
-- valor de venda sugerido pelo pipeline (cache/LLM); alinhado a valor_mercado_estimado na gravação
alter table public.leilao_imoveis add column if not exists valor_venda_sugerido double precision;
-- financeiro com margem de segurança de liquidez
alter table public.leilao_imoveis add column if not exists valor_venda_liquido double precision;
alter table public.leilao_imoveis add column if not exists lance_maximo_recomendado double precision;
alter table public.leilao_imoveis add column if not exists fator_liquidez_venda double precision;
-- teto de reposicionamento por região (comparáveis)
alter table public.leilao_imoveis add column if not exists valor_maximo_regiao_estimado double precision;
alter table public.leilao_imoveis add column if not exists valor_teto_regiao_agressivo double precision;
alter table public.leilao_imoveis add column if not exists potencial_reposicionamento_pct double precision;
alter table public.leilao_imoveis add column if not exists alerta_precificacao_baixa_amostragem boolean default false;
alter table public.leilao_imoveis add column if not exists area_total double precision;
alter table public.leilao_imoveis add column if not exists valor_arrematado_final double precision;
-- vínculo explícito ao registro de cache_media_bairro usado na precificação deste leilão
alter table public.leilao_imoveis add column if not exists cache_media_bairro_id uuid
  references public.cache_media_bairro (id) on delete set null;

comment on column public.leilao_imoveis.cache_media_bairro_id is
  'ID (UUID) da linha em cache_media_bairro usada quando o imóvel foi analisado; o frontend prioriza este cache na aba Leilões';

comment on column public.leilao_imoveis.area_total is
  'Área total do terreno em m² (mais relevante para casas; para apartamentos pode ficar null)';
comment on column public.leilao_imoveis.valor_arrematado_final is
  'Valor real pelo qual o imóvel foi arrematado no leilão (preenchido manualmente após o leilão)';
comment on column public.leilao_imoveis.valor_venda_sugerido is
  'Valor de venda sugerido calculado pelo agente (mesma base que valor_mercado_estimado)';
comment on column public.leilao_imoveis.valor_venda_liquido is
  'Valor de venda estimado após aplicar fator de liquidez';
comment on column public.leilao_imoveis.lance_maximo_recomendado is
  'Lance máximo para atingir o ROI alvo com custos e fator de liquidez';
comment on column public.leilao_imoveis.fator_liquidez_venda is
  'Fator prudencial aplicado sobre o valor de venda estimado (ex.: 0.92)';
comment on column public.leilao_imoveis.valor_maximo_regiao_estimado is
  'Valor máximo de referência da região com base em P90 de R$/m² dos comparáveis';
comment on column public.leilao_imoveis.valor_teto_regiao_agressivo is
  'Teto agressivo da região com base no maior R$/m² observado nos comparáveis';
comment on column public.leilao_imoveis.potencial_reposicionamento_pct is
  'Potencial percentual de reposicionamento: (valor_maximo_regiao_estimado - valor_mercado_estimado) / valor_mercado_estimado';
comment on column public.leilao_imoveis.alerta_precificacao_baixa_amostragem is
  'True quando a precificação foi calculada com amostras abaixo do mínimo recomendado; usar com cautela';


-- ---------------------------------------------------------------------------
-- OPÇÃO 1B — cache_media_bairro: criar tabela mínima se ainda não existir
-- ---------------------------------------------------------------------------
create table if not exists public.cache_media_bairro (
  id uuid primary key default gen_random_uuid(),
  chave_bairro text not null,
  cidade text not null,
  bairro text not null,
  preco_m2_medio double precision not null,
  fonte text,
  metadados_json text,
  atualizado_em timestamptz not null default now(),
  constraint cache_media_bairro_chave_bairro_key unique (chave_bairro)
);

-- Tabela já existente no seu projeto: o CREATE IF NOT EXISTS é ignorado; o DROP CONSTRAINT abaixo remove o UNIQUE antigo quando for o caso.

-- ---------------------------------------------------------------------------
-- OPÇÃO 1C — cache_media_bairro: colunas de segmento + chave única por segmento
-- ---------------------------------------------------------------------------
alter table public.cache_media_bairro add column if not exists estado text;
alter table public.cache_media_bairro add column if not exists tipo_imovel text;
alter table public.cache_media_bairro add column if not exists conservacao text;
alter table public.cache_media_bairro add column if not exists tipo_casa text;
alter table public.cache_media_bairro add column if not exists faixa_andar text;
alter table public.cache_media_bairro add column if not exists logradouro_chave text;
alter table public.cache_media_bairro add column if not exists geo_bucket text;
alter table public.cache_media_bairro add column if not exists lat_ref double precision;
alter table public.cache_media_bairro add column if not exists lon_ref double precision;
alter table public.cache_media_bairro add column if not exists chave_segmento text;

-- Preencher chave_segmento nas linhas antigas
update public.cache_media_bairro
set
  tipo_imovel = coalesce(nullif(trim(tipo_imovel), ''), 'desconhecido'),
  conservacao = coalesce(nullif(trim(conservacao), ''), 'desconhecido'),
  tipo_casa = coalesce(nullif(trim(tipo_casa), ''), '-'),
  faixa_andar = coalesce(nullif(trim(faixa_andar), ''), '-'),
  logradouro_chave = coalesce(nullif(trim(logradouro_chave), ''), '-'),
  chave_segmento = coalesce(
    nullif(trim(chave_segmento), ''),
    chave_bairro || '|tipo=desconhecido|cons=desconhecido|cas=-|and=-|rua=-'
  )
where chave_segmento is null or trim(chave_segmento) = '';

-- Garantir NOT NULL em chave_segmento após backfill (falha se sobrar NULL)
alter table public.cache_media_bairro
  alter column chave_segmento set not null;

-- Permite vários segmentos por mesmo bairro geográfico
alter table public.cache_media_bairro drop constraint if exists cache_media_bairro_chave_bairro_key;

drop index if exists public.cache_media_bairro_chave_segmento_key;
create unique index cache_media_bairro_chave_segmento_key
  on public.cache_media_bairro (chave_segmento);

create index if not exists cache_media_bairro_chave_bairro_idx
  on public.cache_media_bairro (chave_bairro);

comment on column public.cache_media_bairro.chave_segmento is
  'Única por combinação: área + tipo_imovel + conservacao + tipo_casa + faixa_andar + logradouro_chave';
comment on column public.cache_media_bairro.geo_bucket is
  'Bucket geográfico da micro-região (grid lat/lon), usado para cache mais específico em bairros grandes';
comment on column public.cache_media_bairro.lat_ref is
  'Latitude de referência da amostra usada para compor o cache';
comment on column public.cache_media_bairro.lon_ref is
  'Longitude de referência da amostra usada para compor o cache';

-- ---------------------------------------------------------------------------
-- OPÇÃO 1E — cache_media_bairro: estatísticas de valor de venda
-- ---------------------------------------------------------------------------
alter table public.cache_media_bairro add column if not exists valor_medio_venda double precision;
alter table public.cache_media_bairro add column if not exists maior_valor_venda double precision;
alter table public.cache_media_bairro add column if not exists menor_valor_venda double precision;
alter table public.cache_media_bairro add column if not exists n_amostras integer;
alter table public.cache_media_bairro add column if not exists anuncios_ids text;
alter table public.cache_media_bairro add column if not exists nome_cache text;

comment on column public.cache_media_bairro.nome_cache is
  'Nome amigável do cache (definido pelo utilizador no frontend ou gerado automaticamente na sincronização)';
comment on column public.cache_media_bairro.valor_medio_venda is 'Média dos valores de venda dos anúncios usados para compor o cache';
comment on column public.cache_media_bairro.maior_valor_venda is 'Maior valor de venda entre os anúncios usados para compor o cache';
comment on column public.cache_media_bairro.menor_valor_venda is 'Menor valor de venda entre os anúncios usados para compor o cache';
comment on column public.cache_media_bairro.n_amostras is 'Quantidade de anúncios/amostras usados para compor o cache';
comment on column public.cache_media_bairro.anuncios_ids is 'IDs (UUIDs) dos anúncios usados para compor o cache, separados por vírgula';


-- ---------------------------------------------------------------------------
-- OPÇÃO 1D — anuncios_mercado: ofertas coletadas na web (comparáveis de venda)
-- Campos obrigatórios na aplicação ao gravar: url, tipo, endereço (componentes),
--   area_construida_m2, valor_venda. (logradouro/nome_condominio podem ser '' / null.)
-- Se já existir versão antiga desta tabela, faça backup e: drop table public.anuncios_mercado cascade;
-- Depois: RLS / políticas (service_role no backend ignora RLS)
-- ---------------------------------------------------------------------------
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

alter table public.anuncios_mercado add column if not exists arquivado_em timestamptz;
alter table public.anuncios_mercado add column if not exists arquivado_motivo text;

alter table public.anuncios_mercado add column if not exists latitude double precision;
alter table public.anuncios_mercado add column if not exists longitude double precision;

create index if not exists anuncios_mercado_arquivado_em_idx
  on public.anuncios_mercado (arquivado_em desc);

comment on table public.anuncios_mercado is
  'Anúncios de venda/aluguel coletados na web; comparáveis para precificação (não confundir com leilao_imoveis)';
comment on column public.anuncios_mercado.logradouro is 'Rua/avenida sem número; vazio se não extraído';
comment on column public.anuncios_mercado.nome_condominio is 'Opcional; preenchido quando identificável no anúncio';
comment on column public.anuncios_mercado.arquivado_em is
  'Soft delete: timestamp de arquivamento; null = ativo';
comment on column public.anuncios_mercado.arquivado_motivo is
  'Motivo textual do arquivamento manual/automático';

alter table public.anuncios_mercado enable row level security;

-- Se o cliente Python usar anon e as operações falharem após o RLS, ou use
-- SUPABASE_SERVICE_ROLE_KEY no backend, ou crie políticas (ex.: só service_role / usuários autenticados).


-- ---------------------------------------------------------------------------
-- OPÇÃO 1F — bairros_vivareal: cache persistente de slugs de bairros do VivaReal
-- ---------------------------------------------------------------------------
create table if not exists public.bairros_vivareal (
  id uuid primary key default gen_random_uuid(),
  estado text not null,
  cidade text not null,
  slug text not null,
  nome_humanizado text not null,
  atualizado_em timestamptz not null default now(),
  constraint bairros_vivareal_slug_cidade_estado_key unique (estado, cidade, slug)
);

create index if not exists bairros_vivareal_estado_cidade_idx
  on public.bairros_vivareal (estado, cidade);

-- Coluna faixa_area no cache (segmentação por metragem)
alter table public.cache_media_bairro
add column if not exists faixa_area text not null default '-';

comment on table public.bairros_vivareal is
  'Cache persistente de bairros disponíveis no VivaReal por cidade/estado. Evita gastar créditos Firecrawl em cidades já pesquisadas.';
comment on column public.bairros_vivareal.slug is
  'Slug do bairro no VivaReal (ex: residencial-e-comercial-portal-dos-eucaliptos)';
comment on column public.bairros_vivareal.nome_humanizado is
  'Nome legível gerado a partir do slug (ex: Residencial E Comercial Portal Dos Eucaliptos)';

alter table public.bairros_vivareal enable row level security;

-- Colunas de geolocalização em leilao_imoveis (geocodificação Nominatim)
alter table public.leilao_imoveis
add column if not exists latitude double precision;

alter table public.leilao_imoveis
add column if not exists longitude double precision;


-- =============================================================================
-- OPÇÃO 2 — DROP + CREATE (apaga dados; recrie políticas RLS depois)
-- =============================================================================
/*
begin;

drop table if exists public.cache_media_bairro cascade;
drop table if exists public.leilao_imoveis cascade;

create table public.leilao_imoveis (
  id uuid primary key default gen_random_uuid(),
  url_leilao text not null,
  endereco text,
  cidade text,
  estado text,
  bairro text,
  tipo_imovel text,
  conservacao text,
  tipo_casa text,
  andar integer,
  area_util double precision,
  area_total double precision,
  quartos integer,
  vagas integer,
  latitude double precision,
  longitude double precision,
  padrao_imovel text,
  data_leilao date,
  valor_arrematacao double precision,
  valor_mercado_estimado double precision,
  valor_venda_sugerido double precision,
  valor_venda_liquido double precision,
  lance_maximo_recomendado double precision,
  fator_liquidez_venda double precision,
  valor_maximo_regiao_estimado double precision,
  valor_teto_regiao_agressivo double precision,
  potencial_reposicionamento_pct double precision,
  alerta_precificacao_baixa_amostragem boolean default false,
  custo_reforma_estimado double precision,
  roi_projetado double precision,
  status text not null default 'pendente',
  created_at timestamptz not null default now(),
  constraint leilao_imoveis_url_leilao_key unique (url_leilao)
);

create table public.cache_media_bairro (
  id uuid primary key default gen_random_uuid(),
  chave_bairro text not null,
  chave_segmento text not null,
  cidade text not null,
  bairro text not null,
  estado text,
  tipo_imovel text not null default 'desconhecido',
  conservacao text not null default 'desconhecido',
  tipo_casa text not null default '-',
  faixa_andar text not null default '-',
  faixa_area text not null default '-',
  logradouro_chave text not null default '-',
  geo_bucket text,
  lat_ref double precision,
  lon_ref double precision,
  preco_m2_medio double precision not null,
  valor_medio_venda double precision,
  maior_valor_venda double precision,
  menor_valor_venda double precision,
  n_amostras integer,
  anuncios_ids text,
  fonte text,
  metadados_json text,
  atualizado_em timestamptz not null default now(),
  constraint cache_media_bairro_chave_segmento_key unique (chave_segmento)
);

create index cache_media_bairro_chave_bairro_idx
  on public.cache_media_bairro (chave_bairro);

comment on table public.leilao_imoveis is 'Lotes de leilão — ingestão, valuation, financeiro';
comment on table public.cache_media_bairro is 'Média R$/m² por segmento de mercado (cache)';

commit;
*/
