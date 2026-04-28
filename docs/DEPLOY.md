# Deploy e ambiente

## Requisitos

- Python 3.11+
- Conta Supabase (URL + chave) e tabelas conforme `leilao_ia_v2/sql/` (migrations na ordem numérica)
- `OPENAI_API_KEY` (extração de edital e relatórios)
- `FIRECRAWL_API_KEY` (edital, busca e scrape de comparáveis)

## Instalação

Na raiz do repositório:

```bash
pip install -r leilao_ia_v2/requirements.txt
pip install -e ".[dev]"
```

O pacote instalável contém **apenas** `leilao_ia_v2` (incluindo `leilao_ia_v2.comparaveis`, o pipeline de busca de imóveis comparáveis). As dependências de aplicação vêm do `requirements.txt`.

## Configuração

1. Crie `.env` na **raiz do repositório** (o Streamlit em `app_assistente_ingestao.py` carrega `../.env` em relação ao ficheiro).
2. Variáveis comuns: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` (ou a chave usada pelo client), `OPENAI_API_KEY`, `FIRECRAWL_API_KEY`.
3. Opcional: `GOOGLE_MAPS_API_KEY` para validação dura de município por reverse-geocode (caso ausente, cai para Nominatim).

## Testes

```bash
pytest
```

## Apps Streamlit

- Painel principal: `streamlit run leilao_ia_v2/app_assistente_ingestao.py`
- Ingestão avulsa: `streamlit run leilao_ia_v2/app_ingestao.py`

## Notas

- Código em `archive/codigo_referencia` é legado; não faz parte do pacote.
- Não comitar `.env` com segredos.
