# Suite Golden — Harness de Confiabilidade

Cada arquivo em `casos/*.json` é um **snapshot real** (ou anonimizado) de um
leilão que provocou — ou poderia provocar — um bug no pipeline de comparáveis
(`leilao_ia_v2/comparaveis/`). O caso descreve:

- contexto do imóvel (cidade, UF, tipo, área, bairro, descrição opcional);
- markdowns que o Firecrawl retornaria para cada URL;
- resultados de geocode (logradouro → município);
- expectativas explícitas sobre o que o pipeline DEVE / NÃO DEVE produzir.

O **pipeline real** roda contra hooks deterministas — sem rede, sem Firecrawl,
sem geocoder. Isso garante:

- detecção de regressões em segundos (a suite inteira roda em < 2 s);
- documentação executável dos requisitos de negócio;
- proteção contra bugs históricos que voltem a aparecer com refactors.

## Estrutura

```
leilao_ia_v2/tests/golden/
├── __init__.py
├── README.md              ← você está aqui
├── harness.py             ← núcleo: executar_caso, comparar, formatar_resultado
├── validar_golden.py      ← CLI standalone (executa, sai 0/1)
├── test_golden.py         ← runner pytest (1 teste por caso, parametrizado)
└── casos/
    ├── 01_pindamonhangaba_descarta_sao_bernardo.json
    ├── 02_aparecida_descarta_franca.json
    └── ...
```

## Como rodar

Via pytest (parte da suite normal):

```bash
pytest leilao_ia_v2/tests/golden/
```

Via CLI standalone (útil para debugar com saída humana):

```bash
python -m leilao_ia_v2.tests.golden.validar_golden --verbose
python -m leilao_ia_v2.tests.golden.validar_golden --filtro pinda
```

## Formato do JSON

```json
{
  "descricao": "Texto curto explicando o que o caso valida.",
  "pendente": "OPCIONAL: motivo. Se presente, o caso vira xfail (bug conhecido).",
  "leilao": {
    "cidade": "Pindamonhangaba",
    "estado_uf": "SP",
    "tipo_imovel": "apartamento",
    "bairro": "Centro",
    "area_m2": 65,
    "descricao_imovel": "OPCIONAL: usado para detectar boilerplate, condomínio, etc.",
    "leilao_extra_json": {
      "nome_empreendimento": "OPCIONAL: força promoção casa→casa_condominio."
    }
  },
  "cidades_conhecidas": ["OPCIONAL: lista para o filtro de página detectar concorrentes"],
  "busca": {
    "urls": ["URLs que a Firecrawl Search 'devolveria' para a frase de busca."]
  },
  "scrapes": [
    {
      "url": "URL idêntica a uma das de busca.urls",
      "markdown": "Markdown bruto que a Firecrawl Scrape 'devolveria'."
    }
  ],
  "geocodes": [
    {
      "chaves": ["rua das flores, 200", "rua das flores 200"],
      "municipio": "Pindamonhangaba",
      "lat": -22.9234,
      "lon": -45.4621,
      "precisao": "rua"
    }
  ],
  "esperado": {
    "persistidos": 2,
    "min_persistidos": 1,
    "max_persistidos": 5,
    "cidades_anuncios": ["Pindamonhangaba"],
    "cidades_proibidas": ["São Bernardo do Campo"],
    "tipos": ["apartamento"],
    "tipos_proibidos": ["casa_condominio"],
    "bairros_contem": ["centro"],
    "bairros_proibidos": ["88m2", "id-"],
    "tipo_promocao": {
      "https://.../casa-1/": {
        "tipo_final": "casa_condominio",
        "promovido": true
      }
    },
    "bairro_promocao": {
      "https://.../casa-1/": {
        "bairro": "Vila Esplanada",
        "origem": "card"
      }
    },
    "metadados_marcadores": ["rua", "rooftop", "bairro_centroide"]
  }
}
```

Todas as chaves de `esperado` são opcionais. Use só as que realmente
importam para o caso. Comparações são case-insensitive em strings de
bairro; cidades comparam por slug (acentos / capitalização ignorados).

### Como o `geocodes.chaves` é casado

O harness procura por:
1. `<logradouro_lower> | <bairro_lower>` (ex.: `"rua tal, 100 | centro"`)
2. `<logradouro_lower>` apenas (ex.: `"rua tal, 100"`)

Se nenhuma chave casa, a validação devolve `valido=False` (cidade descartada),
EXCETO quando a página já foi confirmada como sendo da cidade-alvo E o card
trouxe `cidade_no_markdown` — nesse caso aprova como evidência textual
(mesmo comportamento do pipeline real).

### Marca `pendente` (xfail)

Quando você descobre um bug mas ainda não vai corrigir, marque o caso:

```json
{
  "pendente": "BUG-XYZ: descrição curta + onde está o problema (arquivo:linha).",
  ...
}
```

A suite trata como `xfail`:
- Se o caso falhar → ok (esperado).
- Se o caso passar → **erro**: alguém corrigiu o bug, remova a marca.

## Como adicionar caso novo

1. Reproduza o cenário em produção (qual leilão? quais URLs? qual saída
   que NÃO esperava?).
2. Salve os markdowns relevantes — só o que o pipeline realmente
   processaria (cabeçalho do Firecrawl scrape, listagem com cards).
3. Anote os geocodes que importam para a validação geográfica.
4. Crie `casos/NN_descricao_curta.json` (numere para ordenação estável).
5. Defina `esperado` com o que o pipeline DEVERIA produzir.
6. Rode `python -m leilao_ia_v2.tests.golden.validar_golden --filtro NN`
   para iterar até passar (ou marcar como `pendente`).
7. Rode `pytest leilao_ia_v2/tests/golden/` para garantir integração.

## Princípios

- **Pipeline real, hooks fakes**: validamos código de produção, não o
  harness. Apenas search/scrape/validar_municipio são mockados — o
  resto (extracção, filtro, normalização, montar_linha) é o real.
- **Esperado mínimo, falhas claras**: cada caso valida UMA coisa
  específica. Múltiplas asserções no mesmo JSON tornam diff confuso.
- **Anonimização**: dados públicos (URLs públicas, endereços de
  testemunhas) podem entrar; PII privada não.
- **Idempotência**: rodar 10 vezes deve dar o mesmo resultado. Sem
  random, sem timestamp, sem dependência de ordem de teste.
