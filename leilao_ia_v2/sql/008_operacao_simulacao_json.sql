-- =============================================================================
-- Simulação de operação (custos, venda estimada, IR, ROI) — um único JSONB.
-- =============================================================================

alter table public.leilao_imoveis
  add column if not exists operacao_simulacao_json jsonb not null default '{}'::jsonb;

comment on column public.leilao_imoveis.operacao_simulacao_json is
  'Última simulação gravada: inputs, outputs, modo de venda, referência de cache (versão interna em JSON).';

create index if not exists leilao_imoveis_operacao_simulacao_json_gin
  on public.leilao_imoveis using gin (operacao_simulacao_json);
