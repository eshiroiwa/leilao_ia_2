"""
Pacote `comparaveis` — pipeline novo de busca e validação de imóveis comparáveis
de mercado a partir de um leilão.

Construído **paralelo** ao pacote antigo `leilao_ia_v2/fc_search/`. Não é importado
em produção até que a flag `LEILAO_IA_COMPARAVEIS_NOVO=1` esteja ligada e o
pipeline de ingestão (`leilao_ia_v2/pipeline/ingestao_edital.py`) seja atualizado
para delegar a este módulo (PR 5 do plano).

Princípios desta reescrita:

1. UMA pesquisa Firecrawl por leilão (vs. 4 layered no módulo antigo).
2. Orçamento real de créditos (search = 2 / 10 results, scrape = 1) com cap duro
   por ingestão (default 15 créditos).
3. Validação dura de cidade: cada card é geocodificado **sem cidade** e o
   município real é confirmado por reverse-geocode. Cards com município ≠ alvo
   são descartados antes de qualquer persistência.
4. Pré-filtro de páginas: scrape só é gasto em URLs cujo título/breadcrumb
   indicam a cidade-alvo.
5. Persistência **sem fallback de cidade**: jamais grava um anúncio com a
   cidade do leilão por omissão.

Os módulos deste pacote são organizados como leaves testáveis isoladamente:

- `orcamento` — contador determinístico de créditos Firecrawl.
- `frase`     — montagem de UMA frase de busca focada.
- `validacao_cidade` — geocode + reverse para confirmar município.
- `pagina_filtro` (PR 2) — heurística "página menciona cidade-alvo?".
- `extrator`  (PR 2) — markdown → cards (sem inventar cidade).
- `busca`     (PR 3) — wrapper Firecrawl Search com orçamento.
- `scrape`    (PR 3) — wrapper Firecrawl Scrape com orçamento.
- `persistencia` (PR 4) — upsert em `anuncios_mercado` com regras estritas.
- `pipeline`  (PR 4) — orquestrador end-to-end.
"""

from __future__ import annotations
