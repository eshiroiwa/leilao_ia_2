-- Opcional: remove a tabela de cache de bairros do Viva Real (não usada pelo app v2 com Firecrawl Search).
-- Confirme que não há dependências externas antes de executar em produção.

drop table if exists public.bairros_vivareal cascade;
