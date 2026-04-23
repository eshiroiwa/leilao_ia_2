-- Três simulações (à vista, parcelado judicial, financiado) para comparação e persistência.
-- Aplicar no Supabase (SQL) após 008.

alter table public.leilao_imoveis
  add column if not exists simulacoes_modalidades_json jsonb not null default '{}'::jsonb;

comment on column public.leilao_imoveis.simulacoes_modalidades_json is
  'Bundle versionado: vista, prazo, financiado (OperacaoSimulacaoDocumento por modalidade).';

create index if not exists leilao_imoveis_simulacoes_modalidades_json_gin
  on public.leilao_imoveis using gin (simulacoes_modalidades_json);
