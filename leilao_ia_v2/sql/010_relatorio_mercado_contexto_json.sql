-- Contexto de mercado gerado por LLM para o relatório HTML (reutilização sem nova chamada).
-- Aplique no SQL Editor do Supabase.

alter table public.leilao_imoveis
  add column if not exists relatorio_mercado_contexto_json jsonb not null default '{}'::jsonb;

comment on column public.leilao_imoveis.relatorio_mercado_contexto_json is
  'Análise de mercado/bairro para relatório (cards + métricas LLM). Versão em campo "versao" dentro do JSON.';

create index if not exists leilao_imoveis_relatorio_mercado_ctx_gin
  on public.leilao_imoveis using gin (relatorio_mercado_contexto_json);
