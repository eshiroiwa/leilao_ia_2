-- =============================================================================
-- leilao_ia_v2 — ROI bruto pós-cache: colunas alinhadas ao agente; remoção de legado.
-- Aplique no SQL Editor do Supabase (public).
-- =============================================================================

-- Novo: teto mínimo da amostra (cache) — usado com maior_valor (maior = máximo da região)
alter table public.leilao_imoveis
  add column if not exists valor_minimo_regiao_estimado double precision;

comment on column public.leilao_imoveis.valor_minimo_regiao_estimado is
  'Menor valor de venda da amostra (cache de média) no momento do cálculo do agente.';

-- Colunas de negócio usadas pelo agente (garanta que existam em bases antigas)
alter table public.leilao_imoveis add column if not exists valor_mercado_estimado double precision;
alter table public.leilao_imoveis add column if not exists custo_reforma_estimado double precision;
alter table public.leilao_imoveis add column if not exists roi_projetado double precision;
alter table public.leilao_imoveis add column if not exists lance_maximo_recomendado double precision;
alter table public.leilao_imoveis add column if not exists valor_maximo_regiao_estimado double precision;
alter table public.leilao_imoveis add column if not exists valor_arrematado_final double precision;

-- Remoção de campos não usados no leilao_ia_v2 (legado de outros pipelines)
alter table public.leilao_imoveis drop column if exists valor_venda_sugerido;
alter table public.leilao_imoveis drop column if exists valor_venda_liquido;
alter table public.leilao_imoveis drop column if exists fator_liquidez_venda;
alter table public.leilao_imoveis drop column if exists valor_teto_regiao_agressivo;
alter table public.leilao_imoveis drop column if exists potencial_reposicionamento_pct;
alter table public.leilao_imoveis drop column if exists cache_granularidade_utilizada;

comment on column public.leilao_imoveis.valor_mercado_estimado is
  'Preço de venda de referência (média do cache principal) para o cálculo do ROI bruto.';

comment on column public.leilao_imoveis.custo_reforma_estimado is
  'Reforma estimada pós-cache: até 50m² 10k; >50 e ≤70m² 15k; acima 500 R$/m² (agente v2).';

comment on column public.leilao_imoveis.roi_projetado is
  'ROI bruto (fração, ex.: 0,45 = 45 %) pós-cache: lucro / investimento, com alíquotas e custos do agente.';

comment on column public.leilao_imoveis.lance_maximo_recomendado is
  'Lance (R$) alvo com ROI bruto 50% nas mesmas premissas do agente.';

comment on column public.leilao_imoveis.valor_maximo_regiao_estimado is
  'Maior valor de venda no cache (amostra) no momento do cálculo.';

comment on column public.leilao_imoveis.valor_arrematado_final is
  'Reservado para o valor de arrematação conhecida após o leilão.';
