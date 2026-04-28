"""Runner pytest da suite golden.

Cada arquivo em ``casos/`` vira um teste separado (parametrizado por
caminho). Casos marcados com ``"pendente": "<motivo>"`` são esperados
falhar (``xfail``) — quando passarem, o pytest irá reportar
``XPASS``, sinal que o bug foi corrigido e o JSON deve ser atualizado.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from leilao_ia_v2.tests.golden.harness import (
    CasoGolden,
    executar_caso,
    formatar_resultado,
    listar_casos,
)


def _ids(caminhos: list[Path]) -> list[str]:
    return [c.stem for c in caminhos]


@pytest.mark.parametrize("caminho_caso", listar_casos(), ids=_ids(listar_casos()))
def test_caso_golden(caminho_caso: Path):
    caso = CasoGolden.carregar(caminho_caso)
    resultado = executar_caso(caso)

    if caso.pendente:
        # Bug conhecido — esperamos que o caso ainda falhe. Se passar,
        # o pytest reporta XPASS (com strict=True) e força o autor a
        # remover o campo "pendente" do JSON.
        if resultado.passou:
            pytest.fail(
                f"Caso '{caso.nome}' está marcado como PENDENTE mas PASSOU. "
                f"Motivo registado: {caso.pendente}\n"
                f"Provavelmente o bug foi corrigido — remova o campo 'pendente' "
                f"do JSON para que ele seja validado normalmente."
            )
        pytest.xfail(reason=f"Pendente: {caso.pendente}")
    else:
        assert resultado.passou, "\n" + formatar_resultado(resultado)
