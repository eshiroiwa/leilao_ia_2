# Auditoria Supabase (revisão manual)

Este documento apoia **limpeza** de colunas/tabelas. **Não apague nada no banco** sem backup e sem confirmar com `SELECT` / uso real em produção.

## Tabelas usadas ativamente (v2)

| Tabela | Uso no código |
|--------|----------------|
| `leilao_imoveis` | Ingestão, painel, simulação, relatório, mapas (`leilao_imoveis_repo`, pipelines) |
| `cache_media_bairro` | Caches de média por segmento; vínculo via `cache_media_bairro_ids` em leilões |
| `anuncios_mercado` | Comparáveis (Viva Real / Firecrawl); upsert em `anuncios_mercado_repo` |

## Tabela provavelmente obsoleta

- **`bairros_vivareal`** — o projeto v2 usa ficheiros em `docs/vivareal/` e lógica em `leilao_ia_v2/vivareal/`; existem SQL `009_drop_bairros_vivareal.sql` e `005_bairros_vivareal_migracao_schema_antigo.sql`. Se no seu projeto a tabela ainda existir e estiver vazia ou não lida por nenhum job, pode ser removida **após** confirmar que nada externo a usa.

## Colunas `leilao_imoveis` (referência)

O pipeline de ingestão grava um subconjunto (ver `leilao_ia_v2/pipeline/ingestao_edital.py`).  
Colunas de negócio comuns: `url_leilao`, dados do imóvel, `leilao_extra_json`, `edital_markdown`, `edital_coletado_em`, `data_leilao_*_praca`, `valor_lance_*_praca`, `operacao_simulacao_json`, `simulacoes_modalidades_json`, `relatorio_mercado_contexto_json`, `cache_media_bairro_ids`, métricas `ultima_extracao_*`, etc.

### Candidatas a “revisar antes de apagar”

- Colunas de **precificação legada** descritas em `archive/codigo_referencia/supabase_ddls_leilao_ia.sql` (ex.: `valor_venda_sugerido`, `valor_maximo_regiao_estimado`, `potencial_reposicionamento_pct`, `alerta_precificacao_baixa_amostragem`, …) **não** aparecem no `insert`/`update` atuais do v2. Se ainda existirem na sua base e estiverem sempre `NULL`, são fortes candidatas a remoção **depois** de validar com dados reais.
- `cache_media_bairro_id` (singular) foi substituída por `cache_media_bairro_ids` (array) em `001_migracao_supabase_etapa1.sql`; a coluna antiga deve ter sido removida na migração. Se ainda existir, pode eliminar-se após confirmação.
- `valor_arrematado_final` e similares: só passam a ser usados se a UI/rotinas os preencherem; o v2 atual foca `operacao_simulacao_json` e campos de edital.

## Como decidir o que apagar

1. No Supabase, `SELECT` com contagem de não nulos por coluna.
2. Procurar no repositório: `grep -r "nome_da_coluna" leilao_ia_v2 --include='*.py'`.
3. Se zero uso **e** confirmação de negócio, gerar `ALTER TABLE ... DROP COLUMN` num script versionado (ex.: `leilao_ia_v2/sql/012_*.sql`).

## JSONB

- `operacao_simulacao_json` / `simulacoes_modalidades_json` / `relatorio_mercado_contexto_json` — **não** apagar: núcleo da simulação e relatórios.
