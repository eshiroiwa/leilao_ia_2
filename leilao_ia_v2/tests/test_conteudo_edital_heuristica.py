from __future__ import annotations

import pytest

from leilao_ia_v2.exceptions import IngestaoSemConteudoEditalError
from leilao_ia_v2.services.conteudo_edital_heuristica import (
    diagnosticar_markdown_edital,
    validar_markdown_antes_da_extracao,
)


def _markdown_edital_fake() -> str:
    return (
        "Edital de leilão judicial. Imóvel apartamento na primeira praça. "
        "Lance mínimo conforme edital. Matrícula 99.999 do CRI. " * 25
    )


def test_diagnostico_encontra_indicios():
    md = _markdown_edital_fake()
    d = diagnosticar_markdown_edital(md)
    assert d.caracteres >= 450
    assert len(d.indicios_encontrados) >= 2


def test_validar_aceita_markdown_rico():
    validar_markdown_antes_da_extracao(_markdown_edital_fake())


def test_validar_rejeita_curto():
    with pytest.raises(IngestaoSemConteudoEditalError):
        validar_markdown_antes_da_extracao("leilão edital")  # curto


def test_validar_rejeita_marketing_sem_leilao():
    pestana_like = (
        "Bem-vindo ao hotel Pestana. Reserve seu quarto com vista para o mar. "
        "Spa, restaurante e estacionamento. Melhor preço garantido. " * 40
    )
    assert len(pestana_like) >= 450
    with pytest.raises(IngestaoSemConteudoEditalError) as ei:
        validar_markdown_antes_da_extracao(pestana_like)
    assert "indícios" in ei.value.motivo.lower() or "leilão" in ei.value.motivo.lower()
