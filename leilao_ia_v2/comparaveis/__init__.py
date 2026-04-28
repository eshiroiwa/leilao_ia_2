"""
Pacote `comparaveis` — pipeline de busca e validação de imóveis comparáveis
de mercado a partir de um leilão.

Princípios:

1. UMA pesquisa Firecrawl por leilão.
2. Orçamento real de créditos (search = 2 / 10 results, scrape = 1) com cap duro
   por ingestão (default 15 créditos).
3. Validação dura de cidade: cada card é geocodificado **sem cidade** e o
   município real é confirmado por reverse-geocode. Cards com município ≠ alvo
   são descartados antes de qualquer persistência.
4. Pré-filtro de páginas: scrape só é gasto em URLs cujo título/breadcrumb
   indicam a cidade-alvo.
5. Persistência **sem fallback de cidade**: jamais grava um anúncio com a
   cidade do leilão por omissão.

Módulos do pacote (leaves testáveis isoladamente):

- ``orcamento``        — contador determinístico de créditos Firecrawl.
- ``frase``            — montagem de UMA frase de busca focada.
- ``validacao_cidade`` — geocode + reverse para confirmar município.
- ``pagina_filtro``    — heurística "página menciona cidade-alvo?".
- ``extrator``         — markdown → cards (sem inventar cidade).
- ``busca``            — wrapper Firecrawl Search com orçamento.
- ``scrape``           — wrapper Firecrawl Scrape com orçamento (cache em disco gratuito).
- ``persistencia``     — upsert em ``anuncios_mercado`` com regras estritas.
- ``pipeline``         — orquestrador end-to-end.
- ``integracao``       — adaptador para o resto da aplicação (única porta de entrada).
"""

from __future__ import annotations
