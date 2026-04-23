"""Segmento de estado na URL do Viva Real (sp, rj ou nome com hífens)."""

from __future__ import annotations

import re
import unicodedata
from typing import Optional

# Sigla UF (2 letras) → segmento exato da URL (regra Viva Real: só sp e rj em sigla).
_UF_PARA_SEGMENTO: dict[str, str] = {
    "AC": "acre",
    "AL": "alagoas",
    "AP": "amapa",
    "AM": "amazonas",
    "BA": "bahia",
    "CE": "ceara",
    "DF": "distrito-federal",
    "ES": "espirito-santo",
    "GO": "goias",
    "MA": "maranhao",
    "MT": "mato-grosso",
    "MS": "mato-grosso-do-sul",
    "MG": "minas-gerais",
    "PA": "para",
    "PB": "paraiba",
    "PR": "parana",
    "PE": "pernambuco",
    "PI": "piaui",
    "RJ": "rj",
    "RN": "rio-grande-do-norte",
    "RS": "rio-grande-do-sul",
    "RO": "rondonia",
    "RR": "roraima",
    "SC": "santa-catarina",
    "SP": "sp",
    "SE": "sergipe",
    "TO": "tocantins",
}


def _fold_compact(s: str) -> str:
    t = "".join(
        c for c in unicodedata.normalize("NFD", s.lower()) if unicodedata.category(c) != "Mn"
    )
    return re.sub(r"\s+", "", t)


# Nome do estado (sem espaços, sem acento) → mesmo segmento que pela sigla.
_NOME_ESTADO_COMPACTO_PARA_SEGMENTO: dict[str, str] = {
    _fold_compact("Acre"): _UF_PARA_SEGMENTO["AC"],
    _fold_compact("Alagoas"): _UF_PARA_SEGMENTO["AL"],
    _fold_compact("Amapá"): _UF_PARA_SEGMENTO["AP"],
    _fold_compact("Amapa"): _UF_PARA_SEGMENTO["AP"],
    _fold_compact("Amazonas"): _UF_PARA_SEGMENTO["AM"],
    _fold_compact("Bahia"): _UF_PARA_SEGMENTO["BA"],
    _fold_compact("Ceará"): _UF_PARA_SEGMENTO["CE"],
    _fold_compact("Ceara"): _UF_PARA_SEGMENTO["CE"],
    _fold_compact("Distrito Federal"): _UF_PARA_SEGMENTO["DF"],
    _fold_compact("Espírito Santo"): _UF_PARA_SEGMENTO["ES"],
    _fold_compact("Espirito Santo"): _UF_PARA_SEGMENTO["ES"],
    _fold_compact("Goiás"): _UF_PARA_SEGMENTO["GO"],
    _fold_compact("Goias"): _UF_PARA_SEGMENTO["GO"],
    _fold_compact("Maranhão"): _UF_PARA_SEGMENTO["MA"],
    _fold_compact("Maranhao"): _UF_PARA_SEGMENTO["MA"],
    _fold_compact("Mato Grosso"): _UF_PARA_SEGMENTO["MT"],
    _fold_compact("Mato Grosso do Sul"): _UF_PARA_SEGMENTO["MS"],
    _fold_compact("Minas Gerais"): _UF_PARA_SEGMENTO["MG"],
    _fold_compact("Pará"): _UF_PARA_SEGMENTO["PA"],
    _fold_compact("Para"): _UF_PARA_SEGMENTO["PA"],
    _fold_compact("Paraíba"): _UF_PARA_SEGMENTO["PB"],
    _fold_compact("Paraiba"): _UF_PARA_SEGMENTO["PB"],
    _fold_compact("Paraná"): _UF_PARA_SEGMENTO["PR"],
    _fold_compact("Parana"): _UF_PARA_SEGMENTO["PR"],
    _fold_compact("Pernambuco"): _UF_PARA_SEGMENTO["PE"],
    _fold_compact("Piauí"): _UF_PARA_SEGMENTO["PI"],
    _fold_compact("Piaui"): _UF_PARA_SEGMENTO["PI"],
    _fold_compact("Rio de Janeiro"): _UF_PARA_SEGMENTO["RJ"],
    _fold_compact("Rio Grande do Norte"): _UF_PARA_SEGMENTO["RN"],
    _fold_compact("Rio Grande do Sul"): _UF_PARA_SEGMENTO["RS"],
    _fold_compact("Rondônia"): _UF_PARA_SEGMENTO["RO"],
    _fold_compact("Rondonia"): _UF_PARA_SEGMENTO["RO"],
    _fold_compact("Roraima"): _UF_PARA_SEGMENTO["RR"],
    _fold_compact("Santa Catarina"): _UF_PARA_SEGMENTO["SC"],
    _fold_compact("São Paulo"): _UF_PARA_SEGMENTO["SP"],
    _fold_compact("Sao Paulo"): _UF_PARA_SEGMENTO["SP"],
    _fold_compact("Sergipe"): _UF_PARA_SEGMENTO["SE"],
    _fold_compact("Tocantins"): _UF_PARA_SEGMENTO["TO"],
}


def estado_para_uf_segmento_vivareal(estado: Optional[str]) -> str:
    """
    Converte sigla (RJ, SP) ou nome do estado para o segmento da URL do Viva Real.
    """
    raw = str(estado or "").strip()
    if not raw:
        return ""
    if len(raw) == 2 and raw.isalpha():
        return _UF_PARA_SEGMENTO.get(raw.upper(), "")
    comp = _fold_compact(raw)
    if comp in _NOME_ESTADO_COMPACTO_PARA_SEGMENTO:
        return _NOME_ESTADO_COMPACTO_PARA_SEGMENTO[comp]
    from leilao_ia_v2.vivareal.slug import slug_vivareal

    return slug_vivareal(raw)


def estado_livre_para_sigla_uf(estado: Optional[str]) -> str:
    """Nome do estado, sigla ou segmento de URL → sigla de duas letras (ex.: ``MG``)."""
    raw = str(estado or "").strip()
    if not raw:
        return ""
    if len(raw) == 2 and raw.isalpha():
        return raw.upper()
    seg = estado_para_uf_segmento_vivareal(raw)
    if not seg:
        return ""
    for uf, s in _UF_PARA_SEGMENTO.items():
        if s.lower() == seg.lower():
            return uf
    return ""


def segmentos_uf_urls_listagem_vivareal(uf_segmento: str) -> list[str]:
    """
    Segmentos de UF a tentar na URL de listagem (ex.: RS → ``rio-grande-do-sul`` e ``rs``).

    O Viva Real usa ``sp``/``rj`` em sigla; outros estados costumam usar o slug extenso.
    """
    s = (uf_segmento or "").strip().lower()
    if not s:
        return []
    if s in ("rs", "rio-grande-do-sul"):
        return ["rio-grande-do-sul", "rs"]
    return [s]
