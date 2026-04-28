"""
Subpacote ``precificacao.integracao`` — adapters que ligam o motor puro
(:mod:`leilao_ia_v2.precificacao`) ao Supabase e ao pipeline de ingestão.

Camadas:

- :mod:`.conversores`     row Supabase ↔ tipos de domínio.
- :mod:`.buscador_supabase` consulta ``anuncios_mercado`` com filtros
  (raio Haversine, área±tol, tipo).
- :mod:`.persistencia`     serializa :class:`ResultadoPrecificacao` em
  ``leilao_imoveis.metadados_json.precificacao_v2``.
- :mod:`.servico`          ponto único de entrada
  :func:`precificar_leilao` — recebe ``leilao_imovel_id`` + ``client``,
  monta o callback de busca, executa o motor e persiste.

A ingestão de cache (em ``services/cache_media_leilao.py``) chama
:func:`servico.precificar_leilao` num try/except, em paralelo ao
``_tentar_gravar_roi_pos_cache`` legado — falhas aqui **não** interrompem
o pipeline.
"""

from leilao_ia_v2.precificacao.integracao.buscador_supabase import (
    BuscaSupabaseConfig,
    construir_buscador,
)
from leilao_ia_v2.precificacao.integracao.conversores import (
    leilao_row_para_alvo,
    anuncio_row_para_amostra,
)
from leilao_ia_v2.precificacao.integracao.persistencia import (
    METADADO_KEY,
    resultado_para_payload,
    gravar_resultado,
)
from leilao_ia_v2.precificacao.integracao.servico import precificar_leilao

__all__ = [
    "BuscaSupabaseConfig",
    "METADADO_KEY",
    "anuncio_row_para_amostra",
    "construir_buscador",
    "gravar_resultado",
    "leilao_row_para_alvo",
    "precificar_leilao",
    "resultado_para_payload",
]
