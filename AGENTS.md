# Agentes e assistente (Leilão IA v2)

## Objetivo do assistente no frontend

O chat do Streamlit (`leilao_ia_v2/app_assistente_ingestao.py` e evoluções da UI) deve funcionar como **assistente global** da aplicação: não só ingestão por URL.

### Comportamento esperado

1. **Ferramentas e agentes**  
   À medida que novos agentes forem criados (filtros, relatórios, consultas ao banco, etc.), o assistente exposto ao utilizador deve **ter acesso às mesmas capacidades** — idealmente um único agente “orquestrador” com **todas** as tools registadas, ou um router que delega sem perder contexto. Evitar silos em que o chat só sabe ingestão.

2. **Pedidos operacionais**  
   Quando o utilizador pedir algo concreto (aplicar um filtro, buscar um registo, listar dados com critérios, etc.), o assistente deve **executar via tools** (consultas parametrizadas, repositórios, APIs internas) e devolver **resultado acionável** ou confirmação do que foi feito — não apenas texto genérico.

3. **Explicação e onboarding**  
   O assistente deve **explicar como o sistema funciona** (pipeline, duplicatas, Firecrawl para edital e para comparáveis via search, Supabase, mapa, cards) e **orientar o uso da app** (onde ver resultados, o que fazer em caso de erro, duplicata, etc.), alinhado às mensagens já definidas no código (ex.: `MENSAGEM_ACOES_USUARIO`).

### Segurança e desenho (para implementação futura)

- Consultas a base de dados: **funções ou repositórios** com SQL controlado (parâmetros, limites), não SQL livre gerado pelo modelo, salvo política explícita de leitura e revisão.
- Respeitar **RLS** e credenciais já usadas pelo app (Supabase).
- Manter respostas ao utilizador **curtas** quando o JSON técnico for interno; detalhe pesado pode ir para a UI (cards, tabelas) em vez do chat.

### Estado atual

- Agente: `leilao_ia_v2/agents/agente_ingestao_edital.py` — tool `tool_ingestir_leilao_por_url` (pode ser reutilizado por orquestradores futuros).
- A UI principal (`app_assistente_ingestao.py`) usa ingestão por URL na **barra lateral**; o chat global, se voltar, deve seguir o ponto 1 acima.
