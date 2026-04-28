"""
Testes do serviço top-level :func:`precificar_leilao`.

Mocka:
- ``leilao_imoveis_repo.buscar_por_id`` (devolve a row do leilão);
- ``construir_buscador`` (devolve um callback que devolve amostras prontas);
- ``gravar_resultado`` (verifica payload final).

Resultado: o serviço orquestra as 4 etapas e devolve um
:class:`ResultadoServico` informativo.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from leilao_ia_v2.precificacao.dominio import Amostra, PRECISAO_RUA
from leilao_ia_v2.precificacao.integracao.servico import (
    ResultadoServico,
    precificar_leilao,
)


def _amostra(*, valor=400_000, area=80, dist=0.5):
    return Amostra(
        url=f"https://x.com/{valor}-{dist}",
        valor_anuncio=valor, area_m2=area, tipo_imovel="apartamento",
        distancia_km=dist, precisao_geo=PRECISAO_RUA, raio_origem_m=500,
    )


def _row(area=80, lance=300_000):
    return {
        "id": "uuid-leilao",
        "cidade": "Pinda",
        "estado": "SP",
        "bairro": "X",
        "tipo_imovel": "apartamento",
        "area_util": area,
        "valor_lance_2_praca": lance,
        "latitude": -22.9,
        "longitude": -45.4,
        "leilao_extra_json": {"existing": True},
    }


# Helper: monta os patches em ordem que o serviço espera.
def _patch_dependencias(rows_amostras):
    """Devolve um context manager combinado para mockar todas as dependências externas."""
    fake_buscar = MagicMock(return_value=list(rows_amostras))

    return patch.multiple(
        "leilao_ia_v2.precificacao.integracao.servico",
        leilao_imoveis_repo=MagicMock(),
        construir_buscador=MagicMock(return_value=fake_buscar),
        gravar_resultado=MagicMock(),
    )


class TestSucesso:
    def test_pipeline_completo_com_persistencia(self):
        amostras = [_amostra() for _ in range(14)]
        with patch(
            "leilao_ia_v2.precificacao.integracao.servico.leilao_imoveis_repo"
        ) as repo, patch(
            "leilao_ia_v2.precificacao.integracao.servico.construir_buscador",
            return_value=lambda **_kw: list(amostras),
        ), patch(
            "leilao_ia_v2.precificacao.integracao.servico.gravar_resultado"
        ) as grv:
            repo.buscar_por_id.return_value = _row()
            cli = MagicMock()
            r = precificar_leilao(cli, "uuid-leilao")

        assert isinstance(r, ResultadoServico)
        assert r.ok is True
        assert r.persistido is True
        assert r.resultado is not None
        assert r.resultado.veredito.nivel == "FORTE"
        # Persistência foi chamada com o resultado correto.
        grv.assert_called_once()
        kwargs = grv.call_args.kwargs
        assert kwargs.get("leilao_extra_json_atual") == {"existing": True}

    def test_persistir_false_devolve_resultado_sem_gravar(self):
        amostras = [_amostra() for _ in range(14)]
        with patch(
            "leilao_ia_v2.precificacao.integracao.servico.leilao_imoveis_repo"
        ) as repo, patch(
            "leilao_ia_v2.precificacao.integracao.servico.construir_buscador",
            return_value=lambda **_kw: list(amostras),
        ), patch(
            "leilao_ia_v2.precificacao.integracao.servico.gravar_resultado"
        ) as grv:
            repo.buscar_por_id.return_value = _row()
            r = precificar_leilao(MagicMock(), "uuid", persistir=False)

        assert r.ok is True
        assert r.persistido is False
        assert r.resultado is not None
        grv.assert_not_called()


class TestCenariosNegativos:
    def test_id_vazio(self):
        r = precificar_leilao(MagicMock(), "")
        assert r.ok is False
        assert "vazio" in r.motivo

    def test_leilao_nao_encontrado(self):
        with patch(
            "leilao_ia_v2.precificacao.integracao.servico.leilao_imoveis_repo"
        ) as repo:
            repo.buscar_por_id.return_value = None
            r = precificar_leilao(MagicMock(), "uuid")
        assert r.ok is False
        assert "não encontrado" in r.motivo

    def test_area_zero_eh_rejeitado(self):
        with patch(
            "leilao_ia_v2.precificacao.integracao.servico.leilao_imoveis_repo"
        ) as repo:
            repo.buscar_por_id.return_value = _row(area=0)
            r = precificar_leilao(MagicMock(), "uuid")
        assert r.ok is False
        assert "área" in r.motivo.lower() or "area" in r.motivo.lower()

    def test_motor_explode_e_servico_devolve_falha_controlada(self):
        with patch(
            "leilao_ia_v2.precificacao.integracao.servico.leilao_imoveis_repo"
        ) as repo, patch(
            "leilao_ia_v2.precificacao.integracao.servico.construir_buscador",
            side_effect=RuntimeError("boom"),
        ):
            repo.buscar_por_id.return_value = _row()
            r = precificar_leilao(MagicMock(), "uuid")
        assert r.ok is False
        assert "motor" in r.motivo.lower()

    def test_falha_ao_persistir_marca_ok_mas_persistido_false(self):
        amostras = [_amostra() for _ in range(14)]
        with patch(
            "leilao_ia_v2.precificacao.integracao.servico.leilao_imoveis_repo"
        ) as repo, patch(
            "leilao_ia_v2.precificacao.integracao.servico.construir_buscador",
            return_value=lambda **_kw: list(amostras),
        ), patch(
            "leilao_ia_v2.precificacao.integracao.servico.gravar_resultado",
            side_effect=RuntimeError("DB indisponível"),
        ):
            repo.buscar_por_id.return_value = _row()
            r = precificar_leilao(MagicMock(), "uuid")
        assert r.ok is True
        assert r.persistido is False
        assert r.resultado is not None
        assert "não persistido" in r.motivo
