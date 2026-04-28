"""Teste de regressão do bug: ingestão de imóvel em Pindamonhangaba persistia
**zero** anúncios no Supabase, apesar de páginas de listagem (Viva Real, Zap,
Chaves na Mão) trazerem dezenas de anúncios reais.

Causas-raiz que este teste cobre simultaneamente:

- (M1) URLs de listagem (``/apartamentos-a-venda/...``) eram descartadas pelo
  filtro de aproveitabilidade *antes* do scrape — agora são aceitas.
- (M2) Frase deterministicamente *"apartamento 65 m² Santana Pindamonhangaba SP"*
  não casava com o SEO dos portais (singular + área restritiva). A nova frase
  é *"apartamentos à venda em Santana Pindamonhangaba SP"*.
- (M3) Geocode "sem cidade" de bairros genéricos (Centro, Santana, Vila X)
  desambigua para cidades populares (São Paulo, Santana de Parnaíba), e o
  card era descartado por *município_diferente* — agora há duas camadas de
  rescue (``cidade_no_markdown`` e ``pagina_confirmada``).

Toda a rede é mockada (HTTP do geocoder + função de scrape do Firecrawl),
mas usamos as implementações reais de ``frase``, ``extrator``,
``pagina_filtro``, ``validacao_cidade`` e ``pipeline``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from leilao_ia_v2.comparaveis import validacao_cidade as vc
from leilao_ia_v2.comparaveis.busca import ResultadoBusca
from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.pipeline import LeilaoAlvo, executar_pipeline
from leilao_ia_v2.comparaveis.scrape import ResultadoScrape


# -----------------------------------------------------------------------------
# Markdown sintético inspirado em chavesnamao.com.br — H1 com cidade,
# breadcrumbs com "Santana, Pindamonhangaba", e múltiplos cards.
# -----------------------------------------------------------------------------

_MD_LISTAGEM_PINDA = """\
# Apartamentos à venda em Santana, Pindamonhangaba - SP

Início > SP > Pindamonhangaba > Santana > Apartamentos à venda

Encontramos 84 apartamentos à venda em Santana, Pindamonhangaba.

---

![Apto frente Santana](https://cdn.cnm.example/1.jpg)

**Apartamento 2 dormitórios na Rua Cônego João Antonio**

[Ver detalhes](https://chavesnamao.com.br/imovel/sp-pindamonhangaba-santana-apto-001/)

R$ 320.000 · 65 m²

Bairro Santana, Pindamonhangaba/SP

---

![Apto Vila Princesa](https://cdn.cnm.example/2.jpg)

**Apartamento 3 quartos próximo à Princesa Cecília**

[Ver detalhes](https://chavesnamao.com.br/imovel/sp-pindamonhangaba-santana-apto-002/)

R$ 410.000 · 78 m²

Em Santana, Pindamonhangaba.

---

![Apto Jardim Resende](https://cdn.cnm.example/3.jpg)

**Apartamento todo reformado em Pindamonhangaba**

[Ver detalhes](https://chavesnamao.com.br/imovel/sp-pindamonhangaba-jdresende-apto-003/)

R$ 285.000 · 55 m²

Jardim Resende, Pindamonhangaba SP

---
"""


def _scrape_real_md(url, *, orcamento, cliente=None):
    orcamento.consumir_scrape(url=url)
    return ResultadoScrape(
        url=url,
        markdown=_MD_LISTAGEM_PINDA,
        executado=True,
        custo_creditos=1,
        fonte="firecrawl",
    )


def _busca_listagem(url):
    return ResultadoBusca(
        urls_aceites=(url,),
        urls_descartadas=(),
        custo_creditos=2,
        executada=True,
    )


def _http_geocode(url, headers=None, timeout=12.0):
    """Mock do geocoder Google. Sem cidade → SBC; com cidade → coords corretas
    de Pinda. Reverse de SBC → São Bernardo."""
    if "address=" in url and "Pindamonhangaba" in url:
        return {
            "status": "OK",
            "results": [{"geometry": {"location": {"lat": -22.93, "lng": -45.47}}}],
        }
    if "address=" in url:
        return {
            "status": "OK",
            "results": [{"geometry": {"location": {"lat": -23.69, "lng": -46.56}}}],
        }
    if "latlng=" in url:
        return {
            "status": "OK",
            "results": [
                {
                    "address_components": [
                        {"long_name": "São Bernardo do Campo", "types": ["locality"]}
                    ]
                }
            ],
        }
    return None


# -----------------------------------------------------------------------------
# Cenário-bug original: SEM nenhuma das três camadas
# -----------------------------------------------------------------------------

def test_bug_original_sem_camadas_persistia_zero(monkeypatch):
    """Reproduz o bug: rejeita listagem antes de scrapear → 0 cards.

    Com a nova lógica isso já não é possível (listagens são sempre aceitas
    pelo filtro), mas o teste documenta o cenário-base e prova que as
    camadas adicionais resolvem o problema.

    Aqui simulamos *apenas* o filtro: nenhuma listagem chegaria a scrapear.
    Validamos pela observação: ``url_eh_anuncio_aproveitavel`` aceita a URL.
    """
    from leilao_ia_v2.comparaveis.extrator import url_eh_anuncio_aproveitavel
    url_listagem = (
        "https://chavesnamao.com.br/apartamentos-a-venda/sp-pindamonhangaba/santana/"
    )
    # Antes do PR: rejeitada. Agora: aceita → scrape vai acontecer.
    assert url_eh_anuncio_aproveitavel(url_listagem)


# -----------------------------------------------------------------------------
# Cenário com TODAS as camadas activas — pipeline persiste cards Pindamonhangaba
# -----------------------------------------------------------------------------

def test_pipeline_pinda_persiste_cards_via_texto_local_e_pagina(monkeypatch):
    """End-to-end: frase → busca → scrape → filtro → extract → validar → persistir.

    Resultado esperado: TODOS os 3 cards do markdown são persistidos, mesmo
    com geocode "sem cidade" mandando para SBC. As camadas que salvam:

    - Camada 1 (texto local): "Pindamonhangaba" aparece no markdown da janela
      de cada card → ``cidade_no_markdown="Pindamonhangaba"`` no extrator.
    - Camada 3 (página confirmada): o H1 da página tem "Pindamonhangaba",
      então o ``pagina_filtro`` retorna ``StatusPagina.CONFIRMADA``.
    """
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
    leilao = LeilaoAlvo(
        cidade="Pindamonhangaba",
        estado_uf="SP",
        tipo_imovel="apartamento",
        bairro="Santana",
        area_m2=65.0,
    )
    orc = OrcamentoFirecrawl(cap=20)
    url_listagem = (
        "https://chavesnamao.com.br/apartamentos-a-venda/sp-pindamonhangaba/santana/"
    )

    persistidas: list = []

    def fake_persistir(client, linhas):
        persistidas.extend(linhas)
        return len(linhas)

    with patch.object(vc, "_http_get_json", side_effect=_http_geocode):
        r = executar_pipeline(
            leilao,
            orcamento=orc,
            supabase_client=object(),
            fn_search=MagicMock(return_value=_busca_listagem(url_listagem)),
            fn_scrape=_scrape_real_md,
            fn_persistir=fake_persistir,
        )

    s = r.estatisticas
    assert s.abortado is False
    assert s.cards_extraidos == 3, f"esperava 3 cards, vieram {s.cards_extraidos}"
    assert s.cards_aprovados_validacao == 3, (
        f"esperava 3 aprovações via camadas, vieram {s.cards_aprovados_validacao}; "
        f"motivos_descarte={s.motivos_descarte_validacao}"
    )
    assert s.persistidos == 3
    assert {l.cidade for l in persistidas} == {"Pindamonhangaba"}
    assert {l.estado for l in persistidas} == {"SP"}
    assert {l.tipo_imovel for l in persistidas} == {"apartamento"}
    # Frase usada deve ser a nova (plural + à venda em)
    assert s.frase_busca == "apartamentos à venda em Santana Pindamonhangaba SP"


def test_pipeline_pinda_descarta_quando_nada_confirma(monkeypatch):
    """Se removermos as evidências (cidade no markdown e H1 com a cidade),
    o pipeline volta a descartar — ou seja, as camadas são realmente as que
    salvam, não um efeito acidental do geocode."""
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key")
    leilao = LeilaoAlvo(
        cidade="Pindamonhangaba",
        estado_uf="SP",
        tipo_imovel="apartamento",
        bairro="Santana",
        area_m2=65.0,
    )
    orc = OrcamentoFirecrawl(cap=20)
    url_listagem = (
        "https://chavesnamao.com.br/apartamentos-a-venda/sp-pindamonhangaba/santana/"
    )
    # Markdown SEM cidade-alvo (nem no H1, nem na janela dos cards).
    md_sem_pinda = """\
# Apartamentos à venda

Encontramos algumas opções.

---

![A](https://cdn.cnm.example/1.jpg)

**Apartamento 2 dormitórios**

[Ver detalhes](https://chavesnamao.com.br/imovel/x-001/)

R$ 320.000 · 65 m²

---

![B](https://cdn.cnm.example/2.jpg)

**Apartamento 3 quartos**

[Ver detalhes](https://chavesnamao.com.br/imovel/x-002/)

R$ 410.000 · 78 m²

---
"""

    def scrape(url, *, orcamento, cliente=None):
        orcamento.consumir_scrape(url=url)
        return ResultadoScrape(
            url=url,
            markdown=md_sem_pinda,
            executado=True,
            custo_creditos=1,
            fonte="firecrawl",
        )

    persistidas: list = []

    def fake_persistir(client, linhas):
        persistidas.extend(linhas)
        return len(linhas)

    with patch.object(vc, "_http_get_json", side_effect=_http_geocode):
        r = executar_pipeline(
            leilao,
            orcamento=orc,
            supabase_client=object(),
            fn_search=MagicMock(return_value=_busca_listagem(url_listagem)),
            fn_scrape=scrape,
            fn_persistir=fake_persistir,
        )

    # Página é REJEITADA pelo filtro (cidade-alvo não aparece no markdown),
    # então não chega a tentar extrair cards.
    s = r.estatisticas
    assert s.paginas_filtro_rejeitado == 1
    assert s.cards_aprovados_validacao == 0
    assert s.persistidos == 0
    assert persistidas == []
