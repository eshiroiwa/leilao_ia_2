"""
Testes da avaliação de confiança (níveis baseados em N + CV + fração precisão).
"""

from __future__ import annotations

from leilao_ia_v2.precificacao.confianca import avaliar_confianca
from leilao_ia_v2.precificacao.dominio import (
    CONFIANCA_ALTA,
    CONFIANCA_BAIXA,
    CONFIANCA_INSUFICIENTE,
    CONFIANCA_MEDIA,
)


class TestAvaliarConfianca:
    def test_n_pequeno_eh_insuficiente(self):
        c = avaliar_confianca(n_uteis=2, cv_pct=10, fracao_precisao_alta=1.0)
        assert c.nivel == CONFIANCA_INSUFICIENTE
        assert c.score == 0.0

    def test_alta_quando_todos_criterios_batem(self):
        c = avaliar_confianca(n_uteis=15, cv_pct=18, fracao_precisao_alta=0.6)
        assert c.nivel == CONFIANCA_ALTA
        assert c.score >= 0.9

    def test_media_quando_n_intermediario(self):
        c = avaliar_confianca(n_uteis=8, cv_pct=30, fracao_precisao_alta=0.4)
        assert c.nivel == CONFIANCA_MEDIA
        assert 0.4 < c.score < 0.9

    def test_baixa_quando_cv_alto_mesmo_com_n_grande(self):
        c = avaliar_confianca(n_uteis=15, cv_pct=60, fracao_precisao_alta=0.6)
        assert c.nivel == CONFIANCA_BAIXA

    def test_baixa_quando_pouca_precisao_alta(self):
        # N e CV ok mas fracao_precisao_alta abaixo do mínimo de MEDIA (0.30) → BAIXA.
        c = avaliar_confianca(n_uteis=10, cv_pct=25, fracao_precisao_alta=0.10)
        assert c.nivel == CONFIANCA_BAIXA

    def test_baixa_quando_n_minimo_mas_demais_falham(self):
        c = avaliar_confianca(n_uteis=4, cv_pct=80, fracao_precisao_alta=0.0)
        assert c.nivel == CONFIANCA_BAIXA

    def test_clampa_fracao_fora_do_intervalo(self):
        # fração negativa não deve quebrar; tratada como 0.0.
        c = avaliar_confianca(n_uteis=15, cv_pct=10, fracao_precisao_alta=-0.5)
        assert c.nivel in {CONFIANCA_BAIXA, CONFIANCA_MEDIA}
