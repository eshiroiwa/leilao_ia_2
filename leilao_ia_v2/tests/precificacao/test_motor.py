"""
Testes de integração do motor :func:`precificar` — usa todos os módulos
do pacote em um pipeline ponta-a-ponta com buscador mockado.
"""

from __future__ import annotations

from leilao_ia_v2.precificacao import (
    Amostra,
    ImovelAlvo,
    PoliticaExpansao,
    PRECISAO_BAIRRO,
    PRECISAO_CIDADE,
    PRECISAO_ROOFTOP,
    PRECISAO_RUA,
    VEREDITO_EVITAR,
    VEREDITO_FORTE,
    VEREDITO_INSUFICIENTE,
    VEREDITO_NEUTRA,
    VEREDITO_OPORTUNIDADE,
    VEREDITO_RISCO,
    VEREDITO_SEM_LANCE,
    precificar,
)


def _alvo(area=80.0, lance=320_000.0):
    return ImovelAlvo(
        cidade="Pindamonhangaba",
        estado_uf="SP",
        bairro="Araretama",
        tipo_imovel="apartamento",
        area_m2=area,
        latitude=-22.9,
        longitude=-45.4,
        lance_minimo=lance,
    )


def _amostra(*, valor=400_000.0, area=80.0, precisao=PRECISAO_RUA, raio=500):
    return Amostra(
        url=f"https://x.com/{valor}-{area}",
        valor_anuncio=float(valor),
        area_m2=float(area),
        tipo_imovel="apartamento",
        distancia_km=raio / 1000.0,
        precisao_geo=precisao,
        raio_origem_m=raio,
    )


def _buscador_fixo(amostras):
    """Devolve sempre as mesmas amostras independentemente dos kwargs."""

    def _f(**_kwargs):
        return list(amostras)

    return _f


def _amostras_homogeneas(n: int = 10, *, valor=400_000, area=80, precisao=PRECISAO_RUA):
    return [_amostra(valor=valor, area=area, precisao=precisao) for _ in range(n)]


class TestCenarioBemSucedido:
    def test_pipeline_completo_com_amostras_uniformes(self):
        # 14 amostras iguais de 5000 R$/m². Alvo: 80m², lance 300k.
        # Após oferta 0.90: R$/m² ajustado = 4500. Mediana = 4500.
        # Valor estimado = 4500 × 80 = 360k. Lance 300k < 0.85·P20=306k → FORTE.
        amostras = _amostras_homogeneas(14, valor=400_000, area=80)
        r = precificar(alvo=_alvo(area=80, lance=300_000), fn_buscar_amostras=_buscador_fixo(amostras))

        assert r.estatistica is not None
        assert r.estatistica.n_uteis == 14
        assert r.estatistica.mediana_r_m2 == 4500.0
        assert r.valor_estimado == 360_000.0
        # 14 amostras, CV=0, frac=1.0 (todas RUA) → ALTA.
        assert r.confianca.nivel == "ALTA"
        assert r.veredito.nivel == VEREDITO_FORTE
        assert r.alerta_liquidez.severidade == "ok"

    def test_pipeline_devolve_neutro_com_amostras_dispersas(self):
        # Mistura: 5 amostras a 4500 R$/m² e 5 a 5500 → mediana 5000.
        # Após oferta 0.90: 4050, 4950 → mediana 4500.
        # Val_est = 4500 × 80 = 360k. P20 ≈ 4050 × 80 = 324k. P80 ≈ 4950 × 80 = 396k.
        # 0.85·P20 ≈ 275k. Lance 350k > 324k (P20) e ≤ 360k (val_est) → NEUTRA.
        amostras = (
            [_amostra(valor=360_000, area=80) for _ in range(5)]
            + [_amostra(valor=440_000, area=80) for _ in range(5)]
        )
        r = precificar(alvo=_alvo(area=80, lance=350_000), fn_buscar_amostras=_buscador_fixo(amostras))
        assert r.veredito.nivel == VEREDITO_NEUTRA

    def test_pipeline_devolve_evitar_quando_lance_muito_alto(self):
        amostras = _amostras_homogeneas(10, valor=400_000, area=80)
        # P80 com amostras idênticas = mediana = 4500. P80 total = 360k.
        # Lance 500k > 360k → EVITAR.
        r = precificar(alvo=_alvo(area=80, lance=500_000), fn_buscar_amostras=_buscador_fixo(amostras))
        assert r.veredito.nivel == VEREDITO_EVITAR


class TestCenarioInsuficiente:
    def test_zero_amostras_veredito_insuficiente(self):
        r = precificar(alvo=_alvo(), fn_buscar_amostras=_buscador_fixo([]))
        assert r.veredito.nivel == VEREDITO_INSUFICIENTE
        assert r.valor_estimado is None
        assert r.confianca.nivel == "INSUFICIENTE"

    def test_duas_amostras_veredito_insuficiente(self):
        amostras = _amostras_homogeneas(2)
        r = precificar(alvo=_alvo(), fn_buscar_amostras=_buscador_fixo(amostras))
        assert r.veredito.nivel == VEREDITO_INSUFICIENTE


class TestCenarioComLiquidez:
    def test_alvo_muito_maior_aplica_fator_e_rebaixa_veredito(self):
        # Mediana area amostras = 60m². Alvo = 150m² → razão = 2.5 → alta liquidez.
        # Fator 0.85 reduz valor estimado; rebaixa veredito 2 níveis.
        amostras = _amostras_homogeneas(10, valor=240_000, area=60)
        # 240k/60 = 4000 R$/m². Ajustado oferta = 3600. Heineck (60→150):
        # F = (60/150)^0.125 ≈ 0.891. Final ≈ 3209. × 150 ≈ 481k.
        # × fator liquidez 0.85 ≈ 408k.
        # Lance hipotético 300k → muito abaixo de P20 → FORTE bruto;
        # liquidez alta rebaixa 2 → NEUTRA.
        r = precificar(
            alvo=_alvo(area=150, lance=300_000),
            fn_buscar_amostras=_buscador_fixo(amostras),
        )
        assert r.alerta_liquidez.severidade == "alta"
        assert r.alerta_liquidez.fator_aplicado == 0.85
        assert r.veredito.rebaixado is True
        # Bruto seria FORTE; rebaixado 2 = NEUTRA. Mas confiança continua ALTA aqui.
        assert r.veredito.nivel == VEREDITO_NEUTRA

    def test_fator_liquidez_aplicado_no_valor_estimado(self):
        # Mesmas amostras de 60m² @ 4000/m². Alvo grande 150m²: liquidez alta.
        # Comparar valor com e sem aplicar liquidez.
        amostras = _amostras_homogeneas(10, valor=240_000, area=60)
        r = precificar(alvo=_alvo(area=150), fn_buscar_amostras=_buscador_fixo(amostras))
        assert r.valor_estimado is not None
        # Razão entre valor estimado e (mediana × area) deve aproximar 0.85.
        razao = r.valor_estimado / (r.estatistica.mediana_r_m2 * 150)
        assert abs(razao - 0.85) < 1e-6


class TestCenarioConfianca:
    def test_dispersao_alta_resulta_em_confianca_baixa_e_rebaixa_veredito(self):
        # Amostras com R$/m² muito diferentes.
        amostras = [
            _amostra(valor=200_000, area=80),  # 2500
            _amostra(valor=600_000, area=80),  # 7500
            _amostra(valor=240_000, area=80),  # 3000
            _amostra(valor=560_000, area=80),  # 7000
            _amostra(valor=320_000, area=80),  # 4000
            _amostra(valor=480_000, area=80),  # 6000
        ]
        r = precificar(alvo=_alvo(area=80, lance=200_000), fn_buscar_amostras=_buscador_fixo(amostras))
        # CV alto → BAIXA confiança → rebaixa veredito.
        assert r.confianca.nivel == "BAIXA"
        # Lance abaixo de P20 → seria OPORTUNIDADE; rebaixado 1 → NEUTRA.
        assert r.veredito.rebaixado is True

    def test_pouca_precisao_alta_resulta_em_confianca_baixa(self):
        # 10 amostras uniformes mas todas com precisão de cidade.
        amostras = [
            _amostra(valor=400_000, area=80, precisao=PRECISAO_CIDADE)
            for _ in range(10)
        ]
        r = precificar(alvo=_alvo(area=80), fn_buscar_amostras=_buscador_fixo(amostras))
        assert r.confianca.nivel == "BAIXA"


class TestCenarioSemLance:
    def test_sem_lance_devolve_estimativa_mas_veredito_sem_lance(self):
        amostras = _amostras_homogeneas(10)
        alvo = ImovelAlvo(
            cidade="X", estado_uf="SP", bairro="Y", tipo_imovel="apartamento",
            area_m2=80, lance_minimo=None,
        )
        r = precificar(alvo=alvo, fn_buscar_amostras=_buscador_fixo(amostras))
        assert r.valor_estimado is not None
        assert r.veredito.nivel == VEREDITO_SEM_LANCE


class TestCenarioComExpansao:
    def test_expansao_rebaixa_score_da_confianca(self):
        # Buscador devolve só 2 amostras nos primeiros 4 degraus, 10 no último.
        chamadas = {"n": 0}

        def buscar(*, raio_m, area_relax_pct, permitir_tipo_proximo):
            chamadas["n"] += 1
            if chamadas["n"] < 5:
                return [_amostra() for _ in range(2)]
            return _amostras_homogeneas(10)

        pol = PoliticaExpansao(n_minimo_alvo=10)
        r = precificar(alvo=_alvo(), fn_buscar_amostras=buscar, politica=pol)

        assert r.expansao.niveis_expansao_aplicados >= 3
        # Score deve ter sido rebaixado em pelo menos 0.05 × 3 = 0.15.
        assert r.confianca.score < 0.95
