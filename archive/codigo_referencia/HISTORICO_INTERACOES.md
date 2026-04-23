# Histórico de Interações — Leilão IA

**Última atualização:** 17/04/2026 — Simulador: desconto à vista (%), cálculo no backend e layout em duas linhas (Desc à vista % | Lance; Base venda | Venda)

---

## 1. Visão Geral do Projeto

Sistema de precificação inteligente de imóveis em leilão.

**Stack:** Python 3.12+, Agno (Phidata), Supabase, Streamlit, Plotly, Pydantic, Firecrawl.

**Arquivos principais:**
- `frontend_streamlit.py` — Frontend completo (Streamlit)
- `token_efficiency.py` — Lógica de negócio, cache, normalização
- `supabase_ddls_leilao_ia.sql` — DDLs do banco de dados
- `.cursorrules` — Regras do projeto para o agente

**Tabelas Supabase:**
- `leilao_imoveis` — Imóveis de leilão
- `anuncios_mercado` — Anúncios de mercado (comparáveis)
- `cache_media_bairro` — Cache de preço por m² por bairro

---

## 2. Todas as Alterações Realizadas (em ordem cronológica)

### 2.1 Reescrita completa do Frontend
- Substituição de navegação por abas (`st.tabs`) por navegação via sidebar com `st.radio`
- Sidebar dinâmica — conteúdo (filtros, ações, opções) muda conforme a aba ativa
- Filtros cascata (estado → cidade → bairro) com dados do IBGE e ViaCEP
- CSS customizado injetado via `st.markdown` para visual moderno

### 2.2 Aba Resumo
- KPIs do pipeline em cards HTML estilizados com gradiente
- KPIs do Firecrawl (Motor, Créditos, Saldo) em cards
- Empty state elegante quando não há relatório
- Expanders para detalhes das fases e JSON bruto

### 2.3 Aba Leilões
- Layout em dois painéis: lista de leilões + detalhe do leilão selecionado
- Remoção dos campos `created_at` e `id` da visualização
- Campo `data_leilao` adicionado e utilizável como filtro
- Filtro por ROI projetado mínimo
- Ordenação por data, ROI ou lance
- Toggle para mostrar/ocultar leilões encerrados
- Campo `valor_arrematado_final` com botão de salvar
- Gráficos Plotly (histograma de ROI, scatter lance×ROI) em expander colapsável
- Header card estilizado com badges de ROI e status
- Info grid com métricas do imóvel
- 3 abas internas no painel de detalhe:
  - **Cache do bairro** — mostra R$/m², venda média, maior/menor valor do cache (busca fuzzy)
  - **Recalcular** — recalcula análise com cache existente ou R$/m² manual
  - **Resultado** — registra valor arrematado final
- Filtro de status (selectbox)
- KPI cards com cores semânticas (verde positivo, vermelho negativo, amarelo destaque)

### 2.4 Aba Anúncios (unificada com antiga aba Gestão)
- Todas funcionalidades de gestão mantidas (excluir, arquivar, restaurar, recalcular cache)
- KPIs em cards estilizados (Total, Média R$/m², Mediana, Faixa)
- Toggle "Selecionar todos"
- Importação via JSON ou avulso
  - Parser `_parse_preco_br` corrigido para formato brasileiro (R$ 550.000)
  - Validação de campos obrigatórios
  - KPIs de importação com cores indicativas
- Seção de restaurar arquivados em expander
- Formatação de `ultima_coleta_em` para exibir apenas data (dd/mm/yyyy)
- Links clicáveis (`LinkColumn`) nos anúncios

### 2.5 Aba Cache (nova)
- Dois painéis: entradas de cache | anúncios de mercado
- Filtros por estado, cidade, bairro, tipo de imóvel
- Toggle "Selecionar todos" em ambas as tabelas
- Ações: excluir, mesclar, criar cache com anúncios, adicionar ao cache existente
- Criação manual de cache com campos: cidade, bairro, estado, tipo, R$/m², fonte, valor médio, maior, menor
- Permite múltiplas entradas para o mesmo bairro (chave com UUID)
- KPIs em cards modernos
- Campos formatados: R$/m², valor médio venda, maior valor, menor valor, n_amostras

### 2.6 Aba Simulador
- Header card estilizado com localização, tipo, área e link do leilão
- Desconto à vista (%) no lance nominal, comissão/ITBI s/ nominal — detalhe em **§2.25**
- Seleção de base da venda: Manual, Média R$/m², Venda média, Maior valor, Menor valor (do cache)
- Campos de reforma com dropdown pré-definido (Leve R$500, Médio R$1.000, Alto R$1.500, Premium R$2.500)
- Registro com cálculo percentual configurável
- ITBI com toggle sobre lance ou sobre venda
- Resultados em KPI cards com cores semânticas (ROI verde/vermelho)
- Tabela de decomposição do investimento
- Toggle para gravar simulação no banco
- Painel de comparáveis com métricas em cards modernos

### 2.8 Campo `area_total` (área do terreno)
- Novo campo `area_total` (double precision) adicionado em todo o sistema
- Mais relevante para casas; para apartamentos é opcional/não utilizado
- **leilao_constants.py**: `ALIASES_AREA_TOTAL`, `area_total_de_registro()` (helper análogo a `area_util_de_registro`)
- **ingestion_agent.py**: `LeilaoImovelCreate` com campo `area_total`, extração via regex em `extrair_campos_do_texto_bruto` (busca padrões como "área total", "terreno", "lote" + m²), payload Playwright inclui `area_total`
- **valuation_agent.py**: `ImovelPendenteSnapshot` com campo `area_total`
- **pricing_pipeline.py**: leitura da planilha reconhece coluna `area_total`, payload LLM inclui `area_total`
- **financial_agent.py**: `area_total` nas colunas preferidas de exportação Excel
- **frontend_streamlit.py**:
  - Formulário avulso: campo "Área total m²" com help "Área do terreno (casas)"
  - `_build_temp_xlsx`: inclui `area_total` no XLSX temporário
  - `_coletar_web_avulso`: inclui `area_total` no `row_ref`
  - Lista de leilões: coluna "Á. total" na tabela
  - Header do detalhe: mostra "Terreno X m²" quando `area_total > 0`
  - Simulador: badge "Terreno X m²" no header quando disponível
- **supabase_ddls_leilao_ia.sql**: ALTER TABLE + CREATE TABLE atualizados

### 2.7 Regra de área para terrenos
- **Regra de negócio:** para imóveis tipo "terreno", a área relevante é `area_total` (área do lote); `area_util` é ignorada/anulada.
- **leilao_constants.py:**
  - `normalizar_tipo_imovel` agora reconhece "terreno", "lote", "gleba", "chácara", "sítio" → `"terreno"`
  - Nova função `area_efetiva_de_registro(row)`: retorna `area_total` para terrenos, `area_util` para os demais (com fallback cruzado)
- **Módulos de cálculo** (`pricing_pipeline.py`, `valuation_agent.py`, `token_efficiency.py`, `anuncios_mercado.py`):
  - Substituído `area_util_de_registro` por `area_efetiva_de_registro` em todos os pontos de cálculo de preço/m² e triagem
- **ingestion_agent.py:**
  - Detecção de tipo "terreno" no `extrair_campos_do_texto_bruto`
  - Para terrenos: se só `area_util` foi extraída, reclassifica como `area_total`; `area_util` fica `None`
- **anuncios_mercado.py:**
  - Na seleção de comparáveis (`selecionar_comparaveis_deterministico`), quartos e vagas são ignorados quando tipo = terreno
- **frontend_streamlit.py:**
  - Formulário avulso de leilão: quando tipo=terreno, mostra apenas "Área do terreno m²" (sem campo de área útil)
  - Formulário avulso de anúncio: label muda para "Área do terreno (m²) *" e quartos é ocultado para terrenos
  - Header de detalhe do leilão: terrenos mostram apenas "Terreno X m²" (sem área útil)
  - Simulador: badge adaptado para terrenos; `area_imovel` usa `area_efetiva_de_registro`
  - Recalcular: usa `area_efetiva_de_registro` em vez de `area_util` direto

### 2.8 Busca combinada VivaReal (casas + terrenos)
- **Regra de negócio:** ao analisar imóveis não-apartamento (casa, casa_condominio), a busca no VivaReal agora inclui **terrenos** na mesma consulta, sem custo extra de créditos Firecrawl.
- **anuncios_mercado.py:**
  - Corrigido `_TIPO_IMOVEL_VIVAREAL_PATH` para terreno: `terrenos_lotes` → `lote-terreno_residencial` (path real do VivaReal)
  - Novos mapas `_TIPOS_QUE_INCLUEM_TERRENO` e `_VIVAREAL_TIPOS_COMBINADOS` para gerar URLs combinadas
  - `_montar_url_vivareal`: novo parâmetro `incluir_terrenos`; quando True, adiciona `?tipos=casa_residencial,lote-terreno_residencial`
  - `_parse_cards_vivareal`: nova função `_detectar_tipo_por_card` que identifica terrenos pela URL do anúncio ou pelo conteúdo do card
  - Cada card agora carrega `_tipo_detectado` para classificação correta na persistência
  - `coletar_vivareal_listagem`: ativa `incluir_terrenos` automaticamente para casa/casa_condominio; classifica tipo_imovel corretamente ao persistir; quartos=None para terrenos; log informa quantos terrenos foram encontrados
  - Limite máximo de área aumentado de 20.000 para 50.000 m² (terrenos rurais/chácaras)
- **Custo:** 0 créditos adicionais — mesma consulta única do VivaReal retorna casas + terrenos

### 2.9 Seletor de bairros via VivaReal (slug exato)
- **Problema resolvido:** fuzzy match de bairros falhava (ex: "Portal dos Eucaliptos" vs slug real "residencial-e-comercial-portal-dos-eucaliptos"), causando fallback para DDGS/Firecrawl (39 créditos para 5 anúncios vs 1 crédito via VivaReal)
- **frontend_streamlit.py:**
  - Nova função `_bairros_vivareal_cached(uf, cidade)` — retorna lista de `(slug, nome_humanizado)` do VivaReal, cacheada por 2h (`st.cache_data`), custo 1 crédito Firecrawl na primeira chamada
  - Seletor de bairro no formulário avulso agora usa bairros do VivaReal como fonte primária (selectbox com nomes humanizados)
  - O slug original é preservado em `bairro_vivareal_slug` e propagado para o pipeline
  - Fallback para bairros do banco de dados + digitação livre caso VivaReal não esteja disponível
- **anuncios_mercado.py:**
  - `coletar_vivareal_listagem`: novo parâmetro `bairro_vivareal_slug` — quando fornecido, usa slug direto (sem fuzzy match, sem crédito extra de descoberta)
  - `coletar_e_persistir_via_ddgs`: extrai `bairro_vivareal_slug` do `row_referencia` e repassa
- **Impacto:** economia de ~38 créditos Firecrawl por busca com bairro impreciso; URL do VivaReal sempre correta
- **Bug fix:** `_UF_PARA_ESTADO_EXTENSO` gerava `sao-paulo` no path da URL, mas VivaReal usa UF lowercase (`sp`). Corrigido para `{v: v.lower() for v in _UF_POR_NOME.values()}` — era a causa raiz dos bairros não carregarem

### 2.10 Cache persistente de bairros VivaReal + auto-correção de bairros na planilha
- **Nova tabela Supabase:** `bairros_vivareal` (DDL em `supabase_ddls_leilao_ia.sql`)
  - Colunas: `estado`, `cidade`, `slug`, `nome_humanizado`, `atualizado_em`
  - Constraint unique em `(estado, cidade, slug)`
- **anuncios_mercado.py:**
  - `_descobrir_bairros_vivareal` agora segue prioridade: memória → banco → Firecrawl (1 crédito, salva no banco)
  - Novas funções `_carregar_bairros_do_banco` e `_salvar_bairros_no_banco`
  - Bairros descobertos via listagem também são salvos no banco
  - Nova função pública `resolver_bairro_para_vivareal(bairro, estado, cidade)` → `(nome_corrigido, slug)`
- **pricing_pipeline.py:**
  - `ler_entradas_leilao_de_planilha` agora auto-corrige nomes de bairros: se "Portal dos Eucaliptos" existe como "Residencial E Comercial Portal Dos Eucaliptos" no VivaReal, o nome é corrigido e o slug é propagado automaticamente
  - Log informa cada correção: `"Bairro corrigido na planilha: 'X' -> 'Y' (slug: Z)"`
- **Impacto:** 0 créditos para cidades já pesquisadas; bairros da planilha sempre corretos

### 2.11 Verificação robusta de comparáveis antes de scraping
- **anuncios_mercado.py:**
  - `coletar_e_persistir_via_ddgs` agora usa `selecionar_top_comparaveis` para verificar se o banco já tem comparáveis **reais** (mesma tipologia, área ±15–35%, quartos ±1–2) em vez de apenas contar anúncios brutos
  - Frescor padrão aumentado de 30 para **180 dias** (preços de imóveis não variam tanto em 6 meses)
  - Se os comparáveis existentes são suficientes (≥ `min_salvos`) e recentes, coleta web é totalmente pulada
  - Logs detalhados informam: "Banco tem X anúncios mas apenas Y comparáveis válidos (meta=Z)"
- **Impacto:** Economia de créditos significativa; scraping só acontece quando os dados existentes não são comparáveis reais ao imóvel de referência

### 2.12 Segmentação de cache por faixa de área (m²)
- **leilao_constants.py:**
  - Nova função `faixa_area_de_metragem(area)` → faixas: `ate-60`, `61-100`, `101-150`, `151-250`, `251-500`, `acima-500`
  - `segmento_mercado_de_registro` agora inclui `faixa_area` nas dimensões do segmento
- **token_efficiency.py:**
  - `normalizar_chave_segmento` inclui `|area=...` na chave
  - `merge_segmento_mercado` suporta override de `faixa_area`
  - `CacheMediaBairroSalvar` com novo campo `faixa_area`
  - `_supabase_select_cache_hierarquico`: níveis progressivos mantêm `faixa_area` nos mais específicos, relaxam nos genéricos
  - `salvar_media_bairro_no_cache` grava `faixa_area` no banco
- **anuncios_mercado.py:** `sincronizar_amostras_e_atualizar_cache_media_bairro` propaga `faixa_area` ao salvar cache
- **pricing_pipeline.py:** `_preencher_cache_bairro_apos_llm_se_miss` propaga `faixa_area` ao salvar cache pós-LLM
- **supabase_ddls_leilao_ia.sql:** `ALTER TABLE cache_media_bairro ADD COLUMN faixa_area text NOT NULL DEFAULT '-'`
- **Impacto:** Uma casa de 80m² e uma de 300m² no mesmo bairro agora geram entradas de cache separadas (R$/m² diferente por faixa)

### 2.13 Geocodificação automática de anúncios e leilões (Nominatim/geopy)
- **Novo módulo `geocoding.py`:**
  - `geocodificar_endereco(logradouro, bairro, cidade, estado)` → `(lat, lon)` ou `None`
  - `geocodificar_anuncios_batch(anuncios)` — enriquece lista de dicts in-place
  - API Nominatim (OpenStreetMap): 100% gratuita, sem chave de API
  - Cache LRU em memória (4096 entradas) + rate-limit 1 req/seg
  - Fallback progressivo: rua+bairro+cidade → bairro+cidade → cidade
- **anuncios_mercado.py:**
  - `AnuncioMercadoPersist`: novos campos `latitude`/`longitude` (colunas diretas na tabela, não só JSON)
  - `_COLUNAS_TABELA_ANUNCIOS`: inclui `latitude`/`longitude`
  - `coletar_vivareal_listagem`: geocodifica batch de cards; lat/lon salvos tanto nas colunas quanto em `metadados_json`
  - Coleta DDGS/Firecrawl: geocodifica individualmente cada anúncio; lat/lon nas colunas diretas
  - `geo_bucket_de_registro` lê lat/lon das colunas diretas ou do `metadados_json` → `geo_bucket` preenchido automaticamente
- **ingestion_agent.py:**
  - `LeilaoImovelCreate`: novos campos `latitude` e `longitude`
  - `ingerir_url_leilao`: geocodifica endereço do leilão e salva coordenadas no registro
- **pricing_pipeline.py:**
  - Nova função `_geocodificar_registros_sem_coordenadas`: geocodifica imóveis já existentes no banco que ainda não têm lat/lon e grava via `atualizar_leilao_imovel_campos`
  - Chamada automaticamente no pipeline logo após carregar registros, antes da sincronização de amostras
  - `ler_entradas_leilao_de_planilha`: lê colunas `latitude`/`longitude` da planilha
- **frontend_streamlit.py:**
  - Pipeline avulso: geocodifica endereço ao montar o formulário (antes de criar a planilha temporária)
  - `_build_temp_xlsx`: inclui colunas `latitude`/`longitude`
- **supabase_ddls_leilao_ia.sql:**
  - `ALTER TABLE leilao_imoveis ADD COLUMN latitude/longitude double precision`
- **requirements.txt:** adicionado `geopy>=2.4`
- **Impacto:** Cache agora diferencia micro-regiões dentro do mesmo bairro via `geo_bucket` (~550m de grid); casas perto de avenida comercial vs zona residencial terão caches separados. Todos os imóveis (novos e existentes) são geocodificados automaticamente.

### 2.14 URLs VivaReal com slug correto por estado + busca exclusiva VivaReal
- **Problema:** VivaReal usa abreviação apenas para SP e RJ (`/sp/`, `/rj/`); todos os outros estados usam nome extenso (`/rio-grande-do-sul/`, `/minas-gerais/`, etc.)
- **anuncios_mercado.py:**
  - `_UF_ABREVIADOS_VIVAREAL = {"SP", "RJ"}` — define quais estados usam abreviação
  - `_UF_SLUG_VIVAREAL`: dict UF → lista com slug correto (SP→`["sp"]`, RS→`["rio-grande-do-sul"]`, MG→`["minas-gerais"]`)
  - `_montar_urls_vivareal` (renomeado de `_montar_url_vivareal`): retorna lista de URLs com variantes de slug do estado
  - `coletar_vivareal_listagem`: tenta cada URL candidata até encontrar anúncios
  - `_descobrir_bairros_vivareal`: mesma lógica de tentativa para descoberta de bairros
  - `coletar_e_persistir_via_ddgs`: **DDGS e Firecrawl search desabilitados** — busca exclusivamente no VivaReal
  - Se amostras insuficientes: retorna com log de aviso "Amostras não encontradas" (sem gastar créditos extras)
  - Código morto do DDGS removido (~300 linhas)
- **Impacto:** 1 requisição por estado (sem tentativa dupla); zero créditos desperdiçados

### 2.15 Cache: valores de venda + IDs dos anúncios usados
- **Problema:** `valor_medio_venda`, `maior_valor_venda`, `menor_valor_venda` e `n_amostras` existiam como colunas na tabela `cache_media_bairro` mas nunca eram populados no salvamento do cache
- **token_efficiency.py:**
  - `CacheMediaBairroSalvar`: adicionados campos `valor_medio_venda`, `maior_valor_venda`, `menor_valor_venda`, `n_amostras`, `anuncios_ids`
  - `salvar_media_bairro_no_cache`: grava todos os novos campos no upsert
- **anuncios_mercado.py:**
  - `sincronizar_amostras_e_atualizar_cache_media_bairro`: calcula média/max/min dos valores de venda dos comparáveis selecionados e extrai seus IDs antes de salvar o cache (evolução posterior: ver §2.22 — passou a usar todos os comparáveis retornados pelo filtro de área exata, não só 5)
- **pricing_pipeline.py:**
  - `_preencher_cache_bairro_apos_llm_se_miss`: popula `valor_medio_venda`, `n_amostras=1` com o valor estimado pelo LLM
- **DDL:** `ALTER TABLE public.cache_media_bairro ADD COLUMN IF NOT EXISTS anuncios_ids text;`
- **Impacto:** Agora cada entrada de cache mostra valores de venda reais e quais anúncios a compõem

### 2.16 Geocodificação: fallback por título do anúncio
- **Problema:** em Gravataí/RS todos os anúncios vinham com a mesma lat/lon porque os cards do VivaReal não traziam nome de rua no formato capturado pelo regex; todos caíam no fallback "bairro+cidade" (mesma query → mesmo cache → mesmo ponto)
- **geocoding.py:**
  - `_extrair_logradouro_de_titulo`: novo regex que extrai rua/avenida do título do anúncio
  - `geocodificar_anuncios_batch`: se `logradouro` está vazio, usa título como fallback → queries distintas → coordenadas diferentes
- **anuncios_mercado.py:**
  - `_parse_cards_vivareal`: regex de logradouro ampliado (Estrada, Rodovia, Largo, Praça, Servidão, Beco) + segundo regex para padrões "Nome Sobrenome, número"
- **Impacto:** Anúncios em bairros onde o VivaReal não traz rua explícita agora geocodificam com coordenadas variadas

### 2.17 Frontend: filtro de data de leilão + toggle para descartados
- **Problema 1:** imóvel com `data_leilao` = hoje não aparecia — comparação timezone-aware vs timezone-naive falhava silenciosamente
- **Problema 2:** imóveis com `status=descartado_triagem` não apareciam (ROI=None → `fillna(-9999) >= -100` = False)
- **frontend_streamlit.py:**
  - `hoje` agora usa `pd.Timestamp(datetime.now().date())` (local, naive)
  - `data_leilao` convertido com `.dt.tz_localize(None)` para remover timezone
  - Novo toggle **"Mostrar descartados"** na sidebar — por padrão oculta registros com status `descartado*`
  - Filtro de ROI: `fillna(-9999)` → `fillna(roi_min)` para não excluir registros sem ROI calculado

### 2.18 Frontend: CSS para evitar corte de conteúdo no topo
- **Problema:** Header fixo do Streamlit 1.56 sobrepunha o conteúdo das abas
- **frontend_streamlit.py:**
  - CSS: `header[data-testid="stHeader"]`, `.stAppHeader`, `[data-testid="stToolbar"]` → `position: relative`
  - `.block-container`, `[data-testid="stAppViewBlockContainer"]`, `.stMainBlockContainer` → `padding-top: 2rem`
  - `[data-testid="stDecoration"]` → `display: none`

### 2.19 Filtro de metragem na busca VivaReal
- **Problema:** a busca no VivaReal trazia anúncios de todas as metragens, misturando imóveis de 60m² com de 400m²; o cache por faixa de área ficava com poucos comparáveis na faixa correta
- **leilao_constants.py:**
  - `_FAIXA_AREA_LIMITES`: dict faixa → (area_min, area_max) com limites exatos por faixa
  - `limites_faixa_area(faixa)`: retorna `(area_min, area_max)` para montar parâmetros de URL
- **anuncios_mercado.py:**
  - `_montar_urls_vivareal`: novos parâmetros `area_minima`/`area_maxima` → gera `?areaMinima=X&areaMaxima=Y` na URL
  - `coletar_vivareal_listagem`: novo parâmetro `area_referencia_m2`; calcula a faixa e aplica filtro na URL
  - `coletar_e_persistir_via_ddgs`: extrai `area_efetiva_de_registro` e propaga para `coletar_vivareal_listagem`
- **Parâmetros VivaReal descobertos:** `areaMinima` e `areaMaxima` (confirmado via teste manual)
- **Impacto:** busca já retorna anúncios na faixa de metragem correta → cache mais preciso, menos "ruído" de imóveis fora da faixa

### 2.21 Geocodificação Nominatim estruturada e extração de rua pela URL
- **Problema:** queries free-text com bairro que não existe no OpenStreetMap (ex.: "Residencial Parque das Palmeiras") faziam o Nominatim falhar na rua+cidade+bairro; o fallback ia direto ao centroide da cidade → mesma lat/lon para todos os anúncios.
- **geocoding.py:**
  - `_geocode_structured_cached(street, city, state)` — parâmetros separados para o Nominatim (evita que o bairro "envenene" a busca da rua).
  - Mapeamento UF → nome completo do estado (`_UF_PARA_NOME_ESTADO`) para o campo `state` estruturado.
  - `geocodificar_endereco`: tenta estruturada (rua+cidade+estado) antes do free-text; fallback cidade quando permitido.
  - `_extrair_logradouro_de_url`: regex que para em slugs de bairro (`residencial`, `jardim`, `parque`, etc.); `_limpar_logradouro` remove sufixos de cidade/bairro e caracteres estranhos.
  - `_extrair_logradouro_de_titulo`: corta em stop words (" em ", " com ", metragem).
- **anuncios_mercado.py:** `_parse_cards_vivareal` chama `_extrair_logradouro_de_url` quando o markdown do card não traz logradouro.
- **Impacto:** mais anúncios com coordenadas distintas quando a rua aparece na URL ou no título.

### 2.22 Cache: usar todas as amostras após o filtro de área exata (não só 5)
- **Problema:** `selecionar_top_comparaveis` truncava a `min_comparaveis` (ex.: 5); `estatisticas_comparaveis` usava `top_k=5`; o salvamento do cache cortava em `[:5]` — mesmo com 15+ comparáveis válidos, o cache usava no máximo 5.
- **anuncios_mercado.py:**
  - `selecionar_top_comparaveis`: ao atingir `min_comparaveis`, retorna **todos** os candidatos daquele par (raio × tolerância), ordenados por score (não mais `candidatos[:min_comparaveis]`).
  - `estatisticas_comparaveis`: removido `top_k`; estatísticas sobre a lista inteira recebida.
  - `sincronizar_amostras_e_atualizar_cache_media_bairro`: `precos`, média/min/max de venda, `n_amostras`, `anuncios_ids` e metadados (`urls_amostras`, etc.) sobre **todos** os `comparaveis`; `fonte` passou a `anuncios_mercado_media_todos`; `origem` em metadados: `media_todos_comparaveis_similares`.
- **valuation_agent.py:** chamada a `estatisticas_comparaveis(comparaveis)` sem `top_k`.
- **Nota:** o conjunto ainda é limitado pelo filtro de **área efetiva do imóvel de referência** ± tolerância, quartos/vagas e raio (§2.23 mantém isso).

### 2.23 Filtro do cache apenas por faixa de área (61–100 m²) — revertido
- **Tentativa:** `_filtrar_anuncios_por_faixa_area` + uso dos limites de `faixa_area` em `sincronizar_amostras_e_atualizar_cache_media_bairro` para incluir todos os anúncios da faixa (ex.: 22/22), independentemente da metragem exata do leilão.
- **Motivo do revert (pedido do usuário):** manter o alinhamento com **área exata do imóvel de referência** (`area_efetiva_de_registro` + tolerância progressiva + raio), considerado essencial para o cache.
- **Estado atual:** `sincronizar_amostras_e_atualizar_cache_media_bairro` voltou a usar apenas `selecionar_top_comparaveis` + `raios_km`; função `_filtrar_anuncios_por_faixa_area` removida.
- **Observação operacional:** se no banco há muitos anúncios na faixa VivaReal mas poucos passam no filtro fino (ex.: leilão ~67 m² com ±35% e raio 3 km), o número de amostras no cache pode ser menor que o total de linhas em `anuncios_mercado` — comportamento esperado com área exata.

### 2.24 Aba Cache: seleção estável (pool + entradas)
- **Problema relatado:** ao marcar linhas no **pool de mercado** ou em **entradas do cache**, a interface piscava, havia atraso e muitas vezes era preciso clicar duas vezes; a **composição do cache** (já com `st.dataframe` + `multi-row`) funcionava bem.
- **Causa:** `st.data_editor` com coluna booleana (`selecionar` / `usar`) reconstruída a cada rerun a partir do estado persistido, somado a reconciliação frágil do widget com o DataFrame de entrada.
- **Solução (frontend_streamlit.py):**
  - Substituído `data_editor` por **`st.dataframe`** com `on_select="rerun"`, `selection_mode="multi-row"` e clique na linha (mesmo padrão da composição).
  - Helpers novos: `_ca_widget_sel_state`, `_ca_df_select_widget_key` (fingerprint dos IDs visíveis), `_ca_maybe_seed_df_selection` (primeira montagem reaplica `ca_persist_selected_cache_row_ids` / `ca_persist_selected_pool_row_ids`).
  - Mantidos `reset_index(drop=True)` nos DataFrames filtrados, colunas numéricas com `NumberColumn` onde aplicável, textos de ajuda atualizados (“selecionados no pool” em vez de “marcar usar”).
  - Removida a função `_ca_flags_from_persist` (só servia ao fluxo antigo com checkbox no editor).
- **Nota:** cada clique de seleção ainda dispara rerun completo do app (comportamento do Streamlit com `on_select="rerun"`); a melhoria é a ausência de conflito com o `data_editor`.

### 2.25 Simulador: desconto à vista (%) no lance
- **Regra de negócio:** o valor informado em **Lance (R$)** é o **lance nominal**. O campo **Desc à vista %** (0–99, padrão 0) reduz apenas o **caixa pago pelo lance** na simulação: `nominal × (1 − %/100)`. A **comissão do leiloeiro** e o **ITBI s/ lance** continuam calculados sobre o **nominal**. O **lance máximo** para o ROI alvo resolve o **nominal máximo** com o mesmo desconto.
- **financial_agent.py:**
  - `RoiCalculoEntrada`: novo campo `desconto_avista_pct` (default 0); descrição de `valor_lance` como nominal para percentuais.
  - `calcular_roi_liquido`: investimento = lance efetivo + comissão(nominal) + ITBI s/ lance(nominal) + registro + reforma.
  - `RoiCalculoResultado`: `valor_lance_efetivo`, `desconto_avista_pct_aplicado`.
  - `calcular_lance_maximo_para_roi`: parâmetro `desconto_avista_pct`; coeficiente `(1−d) + com% + ITBI%` sobre o nominal.
  - `montar_entrada_roi_de_registro`: lê `desconto_avista_pct` do registo se existir (opcional).
  - `aplicar_financeiro_a_registro`, `_enriquecer_colunas_financeiras_derivadas`, `calcular_lance_maximo_json`: repasse do desconto onde aplicável.
- **frontend_streamlit.py:**
  - `_sim_roi_leilao_snapshot`: parâmetro `desconto_avista_pct`; repasse a `RoiCalculoEntrada` e `calcular_lance_maximo_para_roi`.
  - Parâmetros: **1ª linha** — **Desc à vista %** (esquerda) e **Lance (R$)** (direita), `st.columns(2)`; **2ª linha** — **Base venda** e **Venda (R$)** na linha de baixo (mesma simetria).
  - Tabela de detalhes: linhas para lance nominal, desconto %, lance pago à vista, comissão “s/ lance nominal”, ITBI s/ lance nominal quando aplicável; `inv_check` usa `valor_lance_efetivo`.
  - `_sim_purge_keys_para_imovel`: inclui `sda_{iid}` para “Nova simulação” limpar o desconto.
- **Iterações de UI nesta sessão:** rótulo curto “Desc” → reorganização (base venda e venda abaixo de lance/desconto) → rótulo **Desc à vista %** → inversão da ordem (desconto à esquerda, lance à direita).

### 2.20 Alterações Transversais
- **Formatação numérica brasileira** em todo o sistema (ponto como separador de milhar)
  - Funções `_fmt_n` e `_fmt_brl` aplicadas em todos os `st.metric`, `st.caption`, `st.markdown`
  - `st.column_config.TextColumn` com valores pré-formatados nos `st.data_editor`
- **Busca fuzzy de cache** (`_buscar_cache_para_imovel`) com tags [exato], [similar], [região]
- **Helper `_safe_str`** para converter NaN/None/nan para string limpa
- **`_inserir_cache_novo`** resiliente a schema incompleto (fallback se `chave_segmento` não existir)
- **CSS global** com classes: `.kpi-card`, `.lei-header`, `.badge`, `.info-grid`, `.section-title`, `.empty-state`
- **Helpers HTML**: `_kpi_card`, `_info_item`, `_badge`, `_status_badge`, `_roi_badge`
- Substituição de `use_container_width=True` por `width="stretch"` (deprecation fix)

---

## 3. DDLs Pendentes (executar no Supabase se ainda não feito)

```sql
-- Adicionar coluna area_total (área do terreno)
ALTER TABLE public.leilao_imoveis
ADD COLUMN IF NOT EXISTS area_total double precision;

COMMENT ON COLUMN public.leilao_imoveis.area_total IS
  'Área total do terreno em m² (mais relevante para casas; para apartamentos pode ficar null)';

-- Adicionar coluna valor_arrematado_final
ALTER TABLE public.leilao_imoveis
ADD COLUMN IF NOT EXISTS valor_arrematado_final double precision;

-- Adicionar colunas ao cache
ALTER TABLE public.cache_media_bairro
ADD COLUMN IF NOT EXISTS chave_segmento text UNIQUE;

ALTER TABLE public.cache_media_bairro
ADD COLUMN IF NOT EXISTS valor_medio_venda double precision;

ALTER TABLE public.cache_media_bairro
ADD COLUMN IF NOT EXISTS maior_valor_venda double precision;

ALTER TABLE public.cache_media_bairro
ADD COLUMN IF NOT EXISTS menor_valor_venda double precision;

ALTER TABLE public.cache_media_bairro
ADD COLUMN IF NOT EXISTS n_amostras integer;

-- Tabela de cache de bairros do VivaReal
CREATE TABLE IF NOT EXISTS public.bairros_vivareal (
  id uuid primary key default gen_random_uuid(),
  estado text not null,
  cidade text not null,
  slug text not null,
  nome_humanizado text not null,
  atualizado_em timestamptz not null default now(),
  constraint bairros_vivareal_slug_cidade_estado_key unique (estado, cidade, slug)
);

CREATE INDEX IF NOT EXISTS bairros_vivareal_estado_cidade_idx
  ON public.bairros_vivareal (estado, cidade);

-- Faixa de área no cache (segmentação por metragem)
ALTER TABLE public.cache_media_bairro
ADD COLUMN IF NOT EXISTS faixa_area text NOT NULL DEFAULT '-';

-- Geolocalização em leilao_imoveis
ALTER TABLE public.leilao_imoveis
ADD COLUMN IF NOT EXISTS latitude double precision;

ALTER TABLE public.leilao_imoveis
ADD COLUMN IF NOT EXISTS longitude double precision;

-- Geolocalização em anuncios_mercado
ALTER TABLE public.anuncios_mercado
ADD COLUMN IF NOT EXISTS latitude double precision;

ALTER TABLE public.anuncios_mercado
ADD COLUMN IF NOT EXISTS longitude double precision;

-- IDs dos anúncios usados no cache
ALTER TABLE public.cache_media_bairro
ADD COLUMN IF NOT EXISTS anuncios_ids text;
```

---

## 4. Estado Atual (17/04/2026 — revisado, Simulador + Cache)

### Concluído
- [x] Reescrita completa do frontend
- [x] Navegação dinâmica por sidebar
- [x] Filtros cascata com IBGE/ViaCEP
- [x] Aba Leilões com dois painéis e visual moderno
- [x] Aba Anúncios unificada com Gestão
- [x] Aba Cache para manipulação de cache
- [x] Aba Simulador com cálculos dinâmicos
- [x] Formatação numérica brasileira global
- [x] Busca fuzzy de cache
- [x] Visual moderno em TODAS as abas (KPI cards, info-grids, badges, empty states)
- [x] Campo `area_total` (área do terreno) em todo o sistema
- [x] Regra de área para terrenos: `area_total` como área efetiva, `area_util` ignorada
- [x] Busca combinada VivaReal: casas + terrenos na mesma consulta (0 créditos extras)
- [x] Seletor de bairros via VivaReal: slug exato, sem fuzzy match falho
- [x] Cache persistente de bairros VivaReal no banco de dados (0 créditos para cidades já pesquisadas)
- [x] Auto-correção de nomes de bairros na importação de planilhas
- [x] Verificação robusta de comparáveis no banco antes de scraping (tipologia, área, quartos, frescor ≤ 180 dias)
- [x] Segmentação de cache por faixa de área (m²): entradas separadas por tipo + metragem
- [x] Geocodificação automática via Nominatim/geopy (anúncios + leilões → geo_bucket por micro-região)
- [x] URLs VivaReal com slug correto por estado (SP/RJ abreviados, demais por extenso); busca exclusiva VivaReal (DDGS desabilitado)
- [x] Geocodificação com fallback por título do anúncio (evita coordenadas iguais em bairros sem rua explícita)
- [x] Frontend: correção de filtro de data (timezone-aware vs naive), toggle "Mostrar descartados", correção filtro ROI para registros sem ROI
- [x] Frontend: CSS para evitar corte de conteúdo no topo (Streamlit 1.56)
- [x] Cache: `valor_medio_venda`, `maior_valor_venda`, `menor_valor_venda`, `n_amostras` agora populados no salvamento
- [x] Cache: nova coluna `anuncios_ids` com UUIDs dos anúncios usados no cálculo
- [x] Geocodificação: queries estruturadas Nominatim + extração de logradouro pela URL e limpeza contextual
- [x] Cache: cálculo de média/P50/min/max/IDs usa **todos** os comparáveis retornados por `selecionar_top_comparaveis` (não truncar em 5)
- [x] Cache: mantido filtro por **área exata** do imóvel de referência (revert do filtro só por faixa 61–100 m²)
- [x] Cache (frontend): seleção no **pool** e **entradas do cache** via `st.dataframe` (multi-row), alinhado à composição — sem piscar/duplo clique do `data_editor`
- [x] Simulador: **Desc à vista %** (desconto no caixa do lance; comissão e ITBI s/ lance sobre nominal); layout em duas linhas simétricas; integração `financial_agent` + tabela de detalhes + purge de widget

### Sem Tarefas Pendentes
Todas as solicitações documentadas neste histórico foram implementadas (salvo decisões explícitas de revert, indicadas nas seções). O sistema está funcional e com visual moderno unificado.

---

## 5. Arquitetura do Frontend (referência rápida)

```
frontend_streamlit.py
├── CSS global (st.markdown <style>)
├── Helpers: _kpi_card, _info_item, _badge, _status_badge, _roi_badge, _safe_str
├── Helpers numéricos: _fmt_n, _fmt_brl, _fmt_seg, _parse_preco_br, _input_num_br
├── Helpers de dados: _query_table, _query_anuncios, _query_cache_bairro_all
├── Helpers de cache: _buscar_cache_para_imovel, _inserir_cache_novo, _recalcular_cache
├── Helpers de filtro: _cascading_filters, _apply_filters
├── Sidebar dinâmica (st.radio + condicional por página)
├── Pipeline executor (threading)
├── Status banner (st.fragment)
└── Páginas:
    ├── 📊 Resumo — KPIs pipeline + Firecrawl
    ├── 🏠 Leilões — Lista + Detalhe (Cache | Recalcular | Resultado)
    ├── 📋 Anúncios — Tabela editável + Importar (JSON/avulso)
    ├── 🗄️ Cache — Entradas + pool: `st.dataframe` (seleção multi-linha); composição com membros do cache
    └── 🧮 Simulador — Parâmetros (lance nominal, desc. à vista %, base/venda, etc.) | Comparáveis
```

---

## 6. Referência de Chats Anteriores

- [Redesign completo do frontend](0d33897c-a964-4400-ae92-af3b6ca6815a) — Chat principal com todas as iterações de frontend
- [Campo area_total e regra terrenos](bf81b126-fb13-4766-ab1c-7dfd59d21f35) — Adição do campo area_total, regra de área para terrenos, VivaReal, cache, geocodificação, frontend (Leilões/descartados), etc.
- **17/04/2026 (documentado em §2.24):** aba Cache — problema de piscar/atraso ao selecionar pool e entradas; solução com `st.dataframe` + seleção multi-linha e helpers de fingerprint/seed.
- **17/04/2026 (documentado em §2.25):** Simulador — desconto à vista (%), regras no `financial_agent`, layout (Desc à vista % | Lance na 1ª linha; Base venda | Venda na 2ª), tabela e lance máximo; ajustes de rótulo e ordem dos campos; atualização deste `HISTORICO_INTERACOES.md` a pedido do usuário.
