from __future__ import annotations

from leilao_ia_v2.services import pii_sanitizer


def test_redige_cpf():
    s = "Titular CPF 123.456.789-00 fim"
    assert "CPF REMOVIDO" in pii_sanitizer.redigir_pii_texto(s)


def test_redige_email():
    s = "Contato fulano@dominio.com.br ok"
    assert "E-MAIL REMOVIDO" in pii_sanitizer.redigir_pii_texto(s)


def test_redige_extracao_extra():
    o, r = pii_sanitizer.redigir_pii_extracao_extra(
        "email a@b.com",
        "cpf 000.000.000-00",
    )
    assert "E-MAIL" in (o or "")
    assert "CPF" in (r or "")
