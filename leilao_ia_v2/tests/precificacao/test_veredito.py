"""
Testes do veredito automático (níveis brutos + rebaixamento por liquidez/confiança).
"""

from __future__ import annotations

from leilao_ia_v2.precificacao.dominio import (
    AlertaLiquidez,
    CONFIANCA_ALTA,
    CONFIANCA_BAIXA,
    CONFIANCA_INSUFICIENTE,
    CONFIANCA_MEDIA,
    Confianca,
    VEREDITO_EVITAR,
    VEREDITO_FORTE,
    VEREDITO_INSUFICIENTE,
    VEREDITO_NEUTRA,
    VEREDITO_OPORTUNIDADE,
    VEREDITO_RISCO,
    VEREDITO_SEM_LANCE,
)
from leilao_ia_v2.precificacao.veredito import computar_veredito


def _conf(nivel=CONFIANCA_ALTA, score=0.95):
    return Confianca(nivel=nivel, motivo=nivel, score=score)


def _liq(rebaixa=0, severidade="ok", fator=1.0):
    return AlertaLiquidez(
        razao_area=1.0,
        severidade=severidade,
        mensagem="",
        fator_aplicado=fator,
        rebaixa_niveis=rebaixa,
    )


# Cenário base: P20=400k, valor_estimado=500k, P80=600k.
P20 = 400_000.0
VAL = 500_000.0
P80 = 600_000.0


class TestNiveisBrutos:
    def test_forte_quando_lance_muito_abaixo_de_p20(self):
        v = computar_veredito(
            lance_minimo=320_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_FORTE
        assert v.rebaixado is False
        assert v.desconto_vs_p20_pct is not None
        assert v.desconto_vs_p20_pct > 0

    def test_oportunidade_quando_lance_proximo_de_p20(self):
        v = computar_veredito(
            lance_minimo=380_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_OPORTUNIDADE

    def test_neutra_quando_lance_em_torno_da_estimativa(self):
        v = computar_veredito(
            lance_minimo=480_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_NEUTRA

    def test_risco_quando_lance_acima_da_estimativa_mas_dentro_de_p80(self):
        v = computar_veredito(
            lance_minimo=580_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_RISCO

    def test_evitar_quando_lance_acima_de_p80(self):
        v = computar_veredito(
            lance_minimo=700_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_EVITAR


class TestRebaixamentos:
    def test_confianca_baixa_rebaixa_um(self):
        # FORTE - 1 = OPORTUNIDADE
        v = computar_veredito(
            lance_minimo=320_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(CONFIANCA_BAIXA, 0.35), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_OPORTUNIDADE
        assert v.rebaixado is True

    def test_liquidez_media_rebaixa_um(self):
        # OPORTUNIDADE - 1 = NEUTRA
        v = computar_veredito(
            lance_minimo=380_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(rebaixa=1, severidade="media", fator=0.92),
        )
        assert v.nivel == VEREDITO_NEUTRA
        assert v.rebaixado is True

    def test_liquidez_alta_rebaixa_dois(self):
        # FORTE - 2 = NEUTRA
        v = computar_veredito(
            lance_minimo=320_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(rebaixa=2, severidade="alta", fator=0.85),
        )
        assert v.nivel == VEREDITO_NEUTRA
        assert v.rebaixado is True

    def test_combinado_baixa_alta_rebaixa_tres(self):
        # FORTE - 3 = RISCO
        v = computar_veredito(
            lance_minimo=320_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(CONFIANCA_BAIXA, 0.3),
            alerta_liquidez=_liq(rebaixa=2, severidade="alta", fator=0.85),
        )
        assert v.nivel == VEREDITO_RISCO

    def test_rebaixamento_nao_passa_do_evitar(self):
        # já em EVITAR; rebaixar mais não muda.
        v = computar_veredito(
            lance_minimo=700_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(CONFIANCA_BAIXA, 0.3),
            alerta_liquidez=_liq(rebaixa=2, severidade="alta", fator=0.85),
        )
        assert v.nivel == VEREDITO_EVITAR


class TestCasosEspeciais:
    def test_sem_lance(self):
        v = computar_veredito(
            lance_minimo=None, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_SEM_LANCE

    def test_lance_zero_eh_sem_lance(self):
        v = computar_veredito(
            lance_minimo=0, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_SEM_LANCE

    def test_confianca_insuficiente_devolve_insuficiente(self):
        v = computar_veredito(
            lance_minimo=300_000, valor_estimado=VAL, p20_total=P20, p80_total=P80,
            confianca=_conf(CONFIANCA_INSUFICIENTE, 0.0), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_INSUFICIENTE

    def test_estimativa_invalida_devolve_insuficiente(self):
        v = computar_veredito(
            lance_minimo=300_000, valor_estimado=None, p20_total=None, p80_total=None,
            confianca=_conf(), alerta_liquidez=_liq(),
        )
        assert v.nivel == VEREDITO_INSUFICIENTE
