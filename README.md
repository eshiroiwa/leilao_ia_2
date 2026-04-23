# Leilão IA v2

Aplicação em Python para ingestão de editais, análise de leilão, simulação e comparáveis (Supabase, Firecrawl, Streamlit).

## Instalação (deploy / CI)

```bash
pip install -r leilao_ia_v2/requirements.txt
pip install -e ".[dev]"
```

O `pyproject.toml` regista o pacote `leilao_ia_v2` (modo editável); as dependências de runtime estão em `requirements.txt` para evitar conflitos de resolução entre ambientes.

Variáveis: copie `.env` a partir do exemplo do projeto; veja `docs/DEPLOY.md`.

## Executar a UI

```bash
streamlit run leilao_ia_v2/app_assistente_ingestao.py
```

## Estrutura

- `leilao_ia_v2/` — código de produção (inclui `fc_search` para Firecrawl Search).
- `leilao_ia_v2/sql/` — scripts SQL (aplicar manualmente no Supabase).
- `docs/` — deploy, auditoria de schema, referências.
- `archive/` — código de referência legado (não usado no runtime v2).
