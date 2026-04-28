"""Harness de confiabilidade ("golden snapshots") para o pipeline de comparáveis.

Cada arquivo em ``casos/`` descreve um leilão real (ou anonimizado) com:

- contexto do imóvel (cidade, UF, tipo, área, bairro, descrição);
- markdowns simulados que o Firecrawl retornaria para cada URL de search;
- mocks de geocode (logradouro/bairro → município real);
- expectativas explícitas sobre o resultado: anúncios persistidos, cidades,
  bairros, tipos, salvaguardas (boilerplate, sufixos URL, contaminação).

O harness executa o **pipeline real** de :mod:`leilao_ia_v2.comparaveis.pipeline`
com hooks deterministas — sem rede, sem Firecrawl, sem geocoder. O objectivo
não é cobrir todos os caminhos (testes unitários fazem isso) mas sim travar
regressões em casos concretos que já provocaram bugs em produção.

Como adicionar caso novo: ver ``casos/README.md``.
"""
