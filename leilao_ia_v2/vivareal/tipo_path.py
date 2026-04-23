"""
Segmento de tipo de imóvel no path do Viva Real (ex.: apartamento_residencial).

Documentação interna: `busca viva real.odt` (correção: usar apartamento_residencial, não 'apartamento' sozinho).

A junção completa da URL (venda/uf/cidade/.../segmento/...) fica para etapa final.
"""

from __future__ import annotations

from typing import Optional

# Tipos canónicos do pipeline que, por defeito, tratamos como comerciais no Viva Real.
_TIPOS_COMERCIAL_PADRAO = frozenset(
    {
        "consultorio",
        "galpao",
        "deposito",
        "armazem",
        "imovel_comercial",
        "ponto_comercial",
        "loja",
        "box",
        "sala",
        "conjunto",
    }
)

_RESIDENCIAL: dict[str, str] = {
    "apartamento": "apartamento_residencial",
    "casa": "casa_residencial",
    "kitnet": "kitnet_residencial",
    "casa_condominio": "condominio_residencial",
    "chacara": "chacara_residencial",
    "cobertura": "cobertura_residencial",
    "duplex": "cobertura_residencial",
    "flat": "flat_residencial",
    "lote": "lote-terreno_residencial",
    "terreno": "lote-terreno_residencial",
    "sobrado": "sobrado_residencial",
    "predio": "edificio-residencial_comercial",
    "edificio": "edificio-residencial_comercial",
    "fazenda": "granja_comercial",
    "sitio": "granja_comercial",
}

_COMERCIAL: dict[str, str] = {
    "consultorio": "consultorio_comercial",
    "galpao": "galpao_comercial",
    "deposito": "galpao_comercial",
    "armazem": "galpao_comercial",
    "imovel_comercial": "imovel-comercial_comercial",
    "lote": "lote-terreno_comercial",
    "terreno": "lote-terreno_comercial",
    "ponto_comercial": "ponto-comercial_comercial",
    "loja": "ponto-comercial_comercial",
    "box": "ponto-comercial_comercial",
    "sala": "sala_comercial",
    "conjunto": "sala_comercial",
    "predio": "predio_comercial",
    "edificio": "predio_comercial",
}

# Valores usados no path (ex.: apartamento_residencial) — útil para filtrar links em parsers.
SEGMENTOS_TIPO_PATH_VIVAREAL: frozenset[str] = frozenset(
    set(_RESIDENCIAL.values()) | set(_COMERCIAL.values())
)


def tipo_imovel_para_segmento_vivareal(
    tipo_imovel: Optional[str],
    *,
    uso_comercial: Optional[bool] = None,
) -> Optional[str]:
    """
    Retorna o segmento de path do tipo (ex.: apartamento_residencial) ou None.

    `uso_comercial`: se None, infere a partir do tipo (lista comercial padrão).
    """
    t = str(tipo_imovel or "").strip().lower()
    if not t or t == "desconhecido":
        return None
    comercial = uso_comercial if uso_comercial is not None else (t in _TIPOS_COMERCIAL_PADRAO)
    d = _COMERCIAL if comercial else _RESIDENCIAL
    if t in d:
        return d[t]
    # tipos só residenciais (ex.: kitnet) não aparecem em COMERCIAL
    if not comercial and t in _RESIDENCIAL:
        return _RESIDENCIAL[t]
    return None
