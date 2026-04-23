# Busca de comparáveis via Firecrawl Search

Fluxo **principal** do projeto: monta uma frase de busca em português a partir do imóvel do leilão, chama **`Firecrawl.search`**, escolhe **3 a 5** URLs de portais (Viva Real, ZAP, ImovelWeb, OLX, etc.), faz **`scrape`** de cada página (reutiliza `leilao_ia_v2.services.firecrawl_edital.scrape_url_markdown` e cache em disco), extrai anúncios do markdown, **geocodifica** com o mesmo Nominatim do projeto principal e grava em **`anuncios_mercado`** via `leilao_ia_v2.services.anuncios_mercado_coleta.persistir_cards_anuncios_mercado`.

## Requisitos

- Mesmo `.env` na raiz do repositório: `FIRECRAWL_API_KEY`, Supabase (`SUPABASE_URL` + chave).
- Dependências do pacote principal (`firecrawl-py`, etc.), ver `leilao_ia_v2/requirements.txt`.

## Integração

- **Pós-ingestão**: `leilao_ia_v2.services.comparaveis_pos_ingestao.executar_comparaveis_apos_ingestao_leilao`.
- **Cache de média** (complemento de amostras): `leilao_ia_v2.services.cache_media_leilao._uma_coleta_firecrawl_search`.

### Variáveis opcionais

| Variável | Significado | Omissão |
|----------|-------------|---------|
| `FC_SEARCH_LIMIT` | Resultados web pedidos ao `search` | `12` |
| `FC_SEARCH_MAX_SCRAPE_URLS` | Máximo de páginas de portais a fazer `scrape` | `5` |

## CLI

Na raiz do repositório:

```bash
# Só mostra frase + URLs (consome 1 search Firecrawl)
python -m leilao_ia_v2.fc_search --leilao-id <UUID> --dry-run

# Executa search + scrapes + Supabase
python -m leilao_ia_v2.fc_search --leilao-id <UUID>
```

## Limitações

- O **parser genérico** depende de markdown com links `[texto](url)` e, perto do link, valores **R$** e **m²**; páginas muito diferentes podem devolver poucos ou zero cards.
- Listagens **Viva Real** com o padrão clássico `Contatar]` continuam a usar o parser dedicado em `leilao_ia_v2.vivareal.parser_cards_listagem` quando a URL devolvida for do VR.
- Créditos Firecrawl: **search** + **um scrape por URL** (ver documentação e painel de saldo).

## Estrutura

- `query_builder.py` — frase de busca.
- `search_client.py` — `Firecrawl.search`.
- `urls.py` — filtro de hosts e selecção de URLs.
- `parser.py` — extracção de cards.
- `pipeline.py` — orquestração e persistência com `origem_metadados=firecrawl_search_complemento`.
