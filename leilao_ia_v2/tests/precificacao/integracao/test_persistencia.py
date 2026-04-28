"""
Testes da serialização e persistência do ResultadoPrecificacao.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from leilao_ia_v2.precificacao import precificar
from leilao_ia_v2.precificacao.dominio import (
    Amostra,
    ImovelAlvo,
    PRECISAO_RUA,
)
from leilao_ia_v2.precificacao.integracao.persistencia import (
    METADADO_KEY,
    SCHEMA_VERSAO,
    MAX_AMOSTRAS_PERSISTIDAS,
    gravar_resultado,
    resultado_para_payload,
)


def _amostra(*, valor=400_000, area=80, dist=0.5):
    return Amostra(
        url=f"https://x.com/{valor}-{area}-{dist}",
        valor_anuncio=valor, area_m2=area, tipo_imovel="apartamento",
        distancia_km=dist, precisao_geo=PRECISAO_RUA, raio_origem_m=500,
    )


def _alvo():
    return ImovelAlvo(
        cidade="Pinda", estado_uf="SP", bairro="X", tipo_imovel="apartamento",
        area_m2=80, latitude=-22.9, longitude=-45.4, lance_minimo=300_000,
    )


def _rodar_motor(amostras):
    def buscar(**_kw):
        return list(amostras)
    return precificar(alvo=_alvo(), fn_buscar_amostras=buscar)


class TestResultadoParaPayload:
    def test_payload_tem_todos_os_campos_esperados(self):
        amostras = [_amostra(dist=i * 0.1) for i in range(8)]
        r = _rodar_motor(amostras)
        p = resultado_para_payload(r)

        assert p["schema_versao"] == SCHEMA_VERSAO
        assert "calculado_em" in p
        assert p["alvo"]["cidade"] == "Pinda"
        assert p["alvo"]["lance_minimo"] == 300_000.0
        assert p["valor_estimado"] is not None
        assert p["p20_total"] is not None
        assert p["p80_total"] is not None
        assert p["estatistica"]["n_uteis"] >= 3
        assert p["confianca"]["nivel"] in {"ALTA", "MEDIA", "BAIXA"}
        assert p["veredito"]["nivel"] in {
            "FORTE", "OPORTUNIDADE", "NEUTRA", "RISCO", "EVITAR",
        }
        assert "alerta_liquidez" in p
        assert "expansao" in p
        assert isinstance(p["amostras"], list)

    def test_payload_quando_insuficiente_tem_estatistica_none(self):
        # Apenas 2 amostras → INSUFICIENTE.
        r = _rodar_motor([_amostra(), _amostra()])
        p = resultado_para_payload(r)
        assert p["estatistica"] is None
        assert p["valor_estimado"] is None
        assert p["confianca"]["nivel"] == "INSUFICIENTE"
        assert p["veredito"]["nivel"] == "INSUFICIENTE"

    def test_amostras_capadas_em_max(self):
        # 50 amostras válidas — payload deve capar em 30.
        amostras = [_amostra(dist=i * 0.01) for i in range(50)]
        r = _rodar_motor(amostras)
        p = resultado_para_payload(r)
        assert len(p["amostras"]) == MAX_AMOSTRAS_PERSISTIDAS

    def test_amostras_ordenadas_por_distancia_asc(self):
        amostras = [
            _amostra(dist=0.5),
            _amostra(dist=0.1),
            _amostra(dist=0.3),
        ] + [_amostra(dist=0.4) for _ in range(5)]
        r = _rodar_motor(amostras)
        p = resultado_para_payload(r)
        distancias = [a["distancia_km"] for a in p["amostras"]]
        assert distancias == sorted(distancias)

    def test_amostra_serializada_tem_campos_chave(self):
        r = _rodar_motor([_amostra() for _ in range(6)])
        p = resultado_para_payload(r)
        a = p["amostras"][0]
        for chave in (
            "url", "valor", "area_m2", "tipo", "distancia_km",
            "precisao_geo", "raio_origem_m",
            "preco_m2_bruto", "preco_m2_ajustado",
            "fator_oferta", "fator_area",
        ):
            assert chave in a, f"campo ausente: {chave}"


class TestGravarResultado:
    def test_grava_sob_chave_correta_preservando_outras(self):
        r = _rodar_motor([_amostra() for _ in range(6)])
        client = MagicMock()
        # leilao_extra_json atual já tem outras chaves — devem ser preservadas.
        atual = {"chave_legada": {"x": 1}, "outra": "valor"}

        with patch(
            "leilao_ia_v2.precificacao.integracao.persistencia.leilao_imoveis_repo.atualizar_leilao_imovel"
        ) as up:
            novo = gravar_resultado(client, "abc-uuid", r, leilao_extra_json_atual=atual)

        assert "chave_legada" in novo
        assert "outra" in novo
        assert METADADO_KEY in novo
        # Foi chamada com exatamente esse novo dict?
        up.assert_called_once_with("abc-uuid", {"leilao_extra_json": novo}, client)

    def test_busca_no_banco_quando_atual_nao_passado(self):
        r = _rodar_motor([_amostra() for _ in range(6)])
        client = MagicMock()
        with patch(
            "leilao_ia_v2.precificacao.integracao.persistencia.leilao_imoveis_repo.buscar_por_id",
            return_value={"leilao_extra_json": {"k": "v"}},
        ) as buscar, patch(
            "leilao_ia_v2.precificacao.integracao.persistencia.leilao_imoveis_repo.atualizar_leilao_imovel"
        ):
            novo = gravar_resultado(client, "lid", r)
        buscar.assert_called_once_with("lid", client)
        assert novo["k"] == "v"
        assert METADADO_KEY in novo

    def test_id_vazio_levanta_value_error(self):
        r = _rodar_motor([_amostra() for _ in range(6)])
        client = MagicMock()
        with pytest.raises(ValueError):
            gravar_resultado(client, "", r)

    def test_atual_invalido_eh_tratado_como_dict_vazio(self):
        r = _rodar_motor([_amostra() for _ in range(6)])
        client = MagicMock()
        with patch(
            "leilao_ia_v2.precificacao.integracao.persistencia.leilao_imoveis_repo.atualizar_leilao_imovel"
        ):
            novo = gravar_resultado(client, "lid", r, leilao_extra_json_atual="string-invalida")
        assert list(novo.keys()) == [METADADO_KEY]
