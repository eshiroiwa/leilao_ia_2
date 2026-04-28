"""Testes do módulo central de normalização de anúncios.

Cobre os comportamentos críticos que o pipeline depende:

- saneamento de bairro (sufixos lixo, features mascaradas como bairro);
- inferência de bairro a partir de URL/título;
- regra de não-herança do bairro do leilão;
- detecção de empreendimento/condomínio com salvaguardas para boilerplate;
- promoção `casa → casa_condominio` apenas com evidência real;
- inferência de cidade da URL (defesa anti-contaminação cross-cidade).
"""

from __future__ import annotations

import json

import pytest

from leilao_ia_v2.services import normalizacao_anuncio as na


# ---------------------------------------------------------------------------
# sanear_bairro
# ---------------------------------------------------------------------------

class TestSanearBairro:
    def test_remove_sufixo_m2(self) -> None:
        assert na.sanear_bairro("Vila Esplanada 333m2") == "Vila Esplanada"

    def test_remove_id_e_preco(self) -> None:
        assert na.sanear_bairro("Centro id-12345 RS180000") == "Centro"

    def test_remove_acoes_de_url(self) -> None:
        assert na.sanear_bairro("Centro venda") == "Centro"

    def test_string_curta_apos_limpeza_vira_vazio(self) -> None:
        assert na.sanear_bairro("88m2") == ""

    def test_feature_isolada_vira_vazio(self) -> None:
        assert na.sanear_bairro("Piscina") == ""
        assert na.sanear_bairro("Academia") == ""

    def test_nome_real_passa_intacto(self) -> None:
        assert na.sanear_bairro("Jardim das Flores") == "Jardim das Flores"

    def test_none_e_vazio(self) -> None:
        assert na.sanear_bairro(None) == ""
        assert na.sanear_bairro("   ") == ""


# ---------------------------------------------------------------------------
# Inferência de bairro de URL e título
# ---------------------------------------------------------------------------

class TestBairroInferidoDaUrl:
    def test_padrao_vivareal_com_cidade(self) -> None:
        url = "https://www.vivareal.com.br/imovel/casa-3-quartos-vila-esplanada-bairros-sao-jose-do-rio-preto-com-150m2-venda-RS500000-id-1234567/"
        # Aceita "vila-esplanada" mas não "casa" nem "3-quartos".
        out = na._bairro_inferido_da_url(url, cidade_alvo="São José do Rio Preto")
        assert "Esplanada" in out

    def test_padrao_barra_bairros(self) -> None:
        url = "https://www.kenlo.com.br/imoveis/taubate/bairros/parque-olimpico"
        out = na._bairro_inferido_da_url(url, cidade_alvo="Taubaté")
        assert out == "Parque Olimpico"

    def test_url_vazia(self) -> None:
        assert na._bairro_inferido_da_url("", "Taubaté") == ""

    def test_descarta_stopword(self) -> None:
        url = "https://www.vivareal.com.br/imovel/venda-aparecida-sp-RS200000-id-1/"
        # Aqui o único candidato seria "venda" ou "aluguel" — deve ficar vazio.
        out = na._bairro_inferido_da_url(url, cidade_alvo="Aparecida")
        assert out == ""


class TestBairroInferidoDoTitulo:
    def test_label_bairro(self) -> None:
        assert na._bairro_inferido_do_titulo("Casa no Bairro Centro") == "Centro"

    def test_no_bairro(self) -> None:
        assert na._bairro_inferido_do_titulo("Excelente apartamento no Centro") == "Centro"

    def test_titulo_sem_bairro(self) -> None:
        assert na._bairro_inferido_do_titulo("Casa 3 quartos para venda") == ""


# ---------------------------------------------------------------------------
# inferir_bairro_anuncio (decisão final + safeguard)
# ---------------------------------------------------------------------------

class TestInferirBairroAnuncio:
    def test_card_confiavel_e_diferente_do_leilao(self) -> None:
        b, origem = na.inferir_bairro_anuncio(
            bairro_card="Vila Esplanada",
            bairro_leilao="Centro",
        )
        assert b == "Vila Esplanada"
        assert origem == "card"

    def test_card_igual_leilao_sem_evidencia_independente_vira_vazio(self) -> None:
        b, origem = na.inferir_bairro_anuncio(
            bairro_card="Centro",
            bairro_leilao="Centro",
        )
        assert b == ""
        assert origem == "vazio_para_evitar_heranca"

    def test_card_igual_leilao_com_url_concordando_eh_aceito(self) -> None:
        url = "https://www.kenlo.com.br/imoveis/taubate/bairros/centro"
        b, origem = na.inferir_bairro_anuncio(
            bairro_card="Centro",
            bairro_leilao="Centro",
            url=url,
            cidade_leilao="Taubaté",
        )
        assert b == "Centro"
        assert origem == "card"

    def test_url_quando_card_vazio(self) -> None:
        url = "https://www.kenlo.com.br/imoveis/taubate/bairros/parque-olimpico"
        b, origem = na.inferir_bairro_anuncio(
            bairro_card="",
            url=url,
            cidade_leilao="Taubaté",
        )
        assert b == "Parque Olimpico"
        assert origem == "url"

    def test_titulo_quando_url_vazia(self) -> None:
        b, origem = na.inferir_bairro_anuncio(
            bairro_card="",
            titulo="Casa no Bairro Jardim Aurora",
        )
        assert b == "Jardim Aurora"
        assert origem == "titulo"

    def test_tudo_vazio(self) -> None:
        b, origem = na.inferir_bairro_anuncio()
        assert b == ""
        assert origem == "vazio"

    def test_card_com_lixo_eh_saneado(self) -> None:
        b, _ = na.inferir_bairro_anuncio(bairro_card="Centro 88m2 id-1")
        assert b == "Centro"

    def test_card_features_isoladas_vira_vazio(self) -> None:
        b, origem = na.inferir_bairro_anuncio(bairro_card="Piscina")
        assert b == ""
        assert origem == "vazio"


# ---------------------------------------------------------------------------
# Detecção de boilerplate de condomínio
# ---------------------------------------------------------------------------

class TestBoilerplateCondominio:
    def test_texto_caixa_responsabilidade_eh_boilerplate(self) -> None:
        texto = (
            "REGRAS PARA PAGAMENTO DAS DESPESAS (caso existam): "
            "Condomínio: Sob responsabilidade do comprador, até o limite de 10% "
            "em relação ao valor de avaliação do imóvel."
        )
        assert na.texto_eh_boilerplate_condominio(texto) is True

    def test_texto_normal_nao_eh_boilerplate(self) -> None:
        assert na.texto_eh_boilerplate_condominio("Casa em condomínio fechado") is False

    def test_vazio_nao_eh_boilerplate(self) -> None:
        assert na.texto_eh_boilerplate_condominio("") is False
        assert na.texto_eh_boilerplate_condominio(None) is False


# ---------------------------------------------------------------------------
# Nome empreendimento + leilao_indica_condominio
# ---------------------------------------------------------------------------

class TestNomeEmpreendimentoLeilao:
    def test_via_chave_estruturada_extra(self) -> None:
        leilao = {
            "leilao_extra_json": {"nome_condominio": "Residencial Villagio di Italia"},
        }
        assert na.nome_empreendimento_leilao(leilao) == "Residencial Villagio di Italia"

    def test_via_chave_top_level(self) -> None:
        leilao = {"condominio": "Condomínio Solar das Águas"}
        out = na.nome_empreendimento_leilao(leilao)
        # O prefixo "Condomínio " é removido pela normalização.
        assert "Solar das" in out

    def test_via_observacoes_markdown(self) -> None:
        leilao = {
            "leilao_extra_json": {
                "observacoes_markdown": "Imóvel localizado no Condomínio: Vila Verde Plaza"
            }
        }
        # NOTA: "Condomínio:" é tratado como key/value e bloqueia detecção
        # quando não há outro indicador positivo.
        # O caso interessante é com texto solto:
        leilao2 = {
            "leilao_extra_json": {
                "observacoes_markdown": "Casa no Condomínio Residencial Villagio di Italia"
            }
        }
        assert "Villagio di Italia" in na.nome_empreendimento_leilao(leilao2)

    def test_ignora_boilerplate_caixa(self) -> None:
        leilao = {
            "descricao": (
                "REGRAS PARA PAGAMENTO. Condomínio: Sob responsabilidade do comprador, "
                "até o limite de 10% em relação ao valor de avaliação."
            )
        }
        assert na.nome_empreendimento_leilao(leilao) == ""

    def test_extra_como_string_json(self) -> None:
        leilao = {
            "leilao_extra_json": json.dumps(
                {"nome_condominio": "Residencial Cosmos"}
            )
        }
        assert na.nome_empreendimento_leilao(leilao) == "Residencial Cosmos"

    def test_leilao_nao_dict(self) -> None:
        assert na.nome_empreendimento_leilao(None) == ""  # type: ignore[arg-type]


class TestLeilaoIndicaCondominio:
    def test_com_nome_de_empreendimento(self) -> None:
        leilao = {"leilao_extra_json": {"nome_condominio": "Residencial Vila Verde"}}
        assert na.leilao_indica_condominio(leilao) is True

    def test_boilerplate_caixa_falsifica(self) -> None:
        leilao = {
            "descricao": (
                "REGRAS PARA PAGAMENTO DAS DESPESAS: Condomínio: Sob responsabilidade "
                "do comprador, até o limite de 10% em relação ao valor de avaliação."
            )
        }
        assert na.leilao_indica_condominio(leilao) is False

    def test_indicador_positivo_no_texto(self) -> None:
        leilao = {"descricao": "Casa em condomínio fechado, com 3 quartos"}
        assert na.leilao_indica_condominio(leilao) is True

    def test_apenas_palavra_condominio_nao_basta(self) -> None:
        leilao = {"descricao": "Valor inclui condomínio, IPTU pago"}
        assert na.leilao_indica_condominio(leilao) is False

    def test_leilao_sem_nada(self) -> None:
        assert na.leilao_indica_condominio({}) is False

    def test_leilao_nao_dict(self) -> None:
        assert na.leilao_indica_condominio(None) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Decisão de tipo do anúncio
# ---------------------------------------------------------------------------

class TestDecidirTipoImovelAnuncio:
    def test_apartamento_passa_intacto(self) -> None:
        assert na.decidir_tipo_imovel_anuncio(tipo_leilao="apartamento") == "apartamento"

    def test_casa_sem_evidencia_continua_casa(self) -> None:
        assert na.decidir_tipo_imovel_anuncio(tipo_leilao="casa") == "casa"

    def test_casa_promove_quando_leilao_indica(self) -> None:
        out = na.decidir_tipo_imovel_anuncio(
            tipo_leilao="casa",
            leilao_indica_condominio_flag=True,
        )
        assert out == "casa_condominio"

    def test_casa_promove_quando_anuncio_indica(self) -> None:
        out = na.decidir_tipo_imovel_anuncio(
            tipo_leilao="casa",
            titulo="Casa em condomínio fechado",
        )
        assert out == "casa_condominio"

    def test_casa_promove_quando_url_indica(self) -> None:
        out = na.decidir_tipo_imovel_anuncio(
            tipo_leilao="casa",
            url="https://x/casa-em-condominio-residencial-villagio/",
        )
        assert out == "casa_condominio"

    def test_casa_condominio_nao_rebaixa(self) -> None:
        # Mesmo sem evidência adicional, mantém o tipo do leilão.
        out = na.decidir_tipo_imovel_anuncio(tipo_leilao="casa_condominio")
        assert out == "casa_condominio"

    def test_apenas_palavra_condominio_nao_promove(self) -> None:
        # "taxa de condomínio" é genérico demais.
        out = na.decidir_tipo_imovel_anuncio(
            tipo_leilao="casa",
            titulo="Casa com taxa de condomínio incluída",
        )
        assert out == "casa"

    def test_tipo_vazio_vira_desconhecido(self) -> None:
        assert na.decidir_tipo_imovel_anuncio(tipo_leilao="") == "desconhecido"


# ---------------------------------------------------------------------------
# Cidade inferida da URL
# ---------------------------------------------------------------------------

class TestCidadeInferidaDaUrl:
    def test_padrao_vivareal(self) -> None:
        url = "https://www.vivareal.com.br/imovel/casa-3-quartos-centro-sp-aparecida-com-150m2-venda-RS500000-id-1/"
        out = na.cidade_inferida_da_url(url)
        assert out == "aparecida"

    def test_url_invalida_devolve_vazio(self) -> None:
        assert na.cidade_inferida_da_url("https://example.com/foo") == ""

    def test_vazio(self) -> None:
        assert na.cidade_inferida_da_url("") == ""


class TestUrlIndicaCidadeDiferente:
    def test_mesma_cidade_nao_eh_diferente(self) -> None:
        url = "https://www.vivareal.com.br/imovel/casa-centro-sp-aparecida-150m2-venda-RS1-id-1/"
        assert na.url_indica_cidade_diferente(url, "Aparecida") is False

    def test_cidade_diferente_eh_detectada(self) -> None:
        url = "https://www.vivareal.com.br/imovel/casa-centro-sp-franca-150m2-venda-RS1-id-1/"
        assert na.url_indica_cidade_diferente(url, "Aparecida") is True

    def test_url_sem_inferencia_devolve_falso(self) -> None:
        assert na.url_indica_cidade_diferente("https://example.com/foo", "Aparecida") is False

    def test_cidade_alvo_vazia(self) -> None:
        assert na.url_indica_cidade_diferente("https://www.vivareal.com.br/x/", "") is False


# ---------------------------------------------------------------------------
# anuncio_match_empreendimento
# ---------------------------------------------------------------------------

class TestAnuncioMatchEmpreendimento:
    def test_inclusao_direta(self) -> None:
        anuncio = {"titulo": "Casa no Condomínio Residencial Villagio di Italia"}
        assert na.anuncio_match_empreendimento(anuncio, "Residencial Villagio di Italia") is True

    def test_match_por_tokens(self) -> None:
        anuncio = {"titulo": "Casa Villagio Italia 3 quartos"}
        # "Villagio" + "Italia" ≥ 2 tokens não-genéricos → match.
        assert na.anuncio_match_empreendimento(anuncio, "Residencial Villagio di Italia") is True

    def test_apenas_token_generico_nao_combina(self) -> None:
        anuncio = {"titulo": "Casa em condomínio residencial"}
        # "condominio" e "residencial" são genéricos — não deve combinar.
        assert na.anuncio_match_empreendimento(anuncio, "Residencial Villagio di Italia") is False

    def test_metadados_dict(self) -> None:
        anuncio = {
            "titulo": "Casa em condomínio",
            "metadados_json": {"nome_condominio": "Vila Verde Plaza"},
        }
        assert na.anuncio_match_empreendimento(anuncio, "Vila Verde Plaza") is True

    def test_metadados_string_json(self) -> None:
        anuncio = {
            "titulo": "Casa",
            "metadados_json": json.dumps({"condominio": "Solar das Águas"}),
        }
        assert na.anuncio_match_empreendimento(anuncio, "Solar das Águas") is True

    def test_ref_curta_nao_combina(self) -> None:
        anuncio = {"titulo": "Vila"}
        assert na.anuncio_match_empreendimento(anuncio, "Vila") is False  # < 6 chars normalizados

    def test_anuncio_nao_dict(self) -> None:
        assert na.anuncio_match_empreendimento(None, "X") is False  # type: ignore[arg-type]
