"""
Normalização de campos alinhada ao vocabulário do legado (reimplementação local).
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Any, Optional

from leilao_ia_v2.constants import (
    CONSERVACAO_VALIDAS,
    TIPO_CASA_VALIDOS,
    TIPOS_IMOVEL_VALIDOS,
)


def _fold_lower(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s.lower()) if unicodedata.category(c) != "Mn"
    )


# Ordem: do mais específico ao mais genérico (primeira correspondência vence).
_TIPO_IMOVEL_SUBSTR: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("casa_condominio", ("casa de condominio", "casa_condominio", "casa em condominio", "condominio fechado")),
    ("ponto_comercial", ("ponto comercial", "ponto_comercial")),
    ("imovel_comercial", ("imovel comercial", "imovel_comercial")),
    ("consultorio", ("consultorio", "consultorios")),
    ("kitnet", ("kitnet", "kit net", "quitinete", "kitchenette")),
    ("cobertura", ("cobertura", "coberturas")),
    ("flat", ("flat",)),
    ("duplex", ("duplex", "triplex")),
    ("sobrado", ("sobrado", "sobrados")),
    ("chacara", ("chacara", "chacaras")),
    ("sitio", ("sitio", "sitios")),
    ("fazenda", ("fazenda", "fazendas")),
    ("deposito", ("deposito", "depositos")),
    ("galpao", ("galpao", "galpoes", "galpao logistico", "pavilhao", "warehouse", "centro de distribuicao")),
    ("armazem", ("armazem", "armazens")),
    ("lote", ("loteamento", "lote", "lotes")),
    ("terreno", ("terreno", "gleba", "baldio")),
    ("predio", ("predio", "predios")),
    ("edificio", ("edificio", "edificios")),
    ("loja", ("loja", "lojas")),
    ("box", ("box", "box garagem", "box de garagem")),
    ("conjunto", ("conjunto comercial", "conjunto_comercial", "conjunto de salas", "conjunto de lojas")),
    ("sala", ("sala comercial", "sala_comercial", "salas comerciais", "andar corrido")),
    ("apartamento", ("apartamento", "apartamentos", "apto", "loft")),
)


def normalizar_tipo_imovel(val: Any) -> Optional[str]:
    raw = str(val or "").strip()
    if not raw:
        return None
    slug = (
        "".join(
            c for c in unicodedata.normalize("NFD", raw.lower()) if unicodedata.category(c) != "Mn"
        )
        .replace(" ", "_")
        .replace("-", "_")
    )
    if slug in TIPOS_IMOVEL_VALIDOS and slug != "desconhecido":
        return slug

    s = _fold_lower(val)
    if "flatron" in s:
        s = s.replace("flatron", "")
    for out, keys in _TIPO_IMOVEL_SUBSTR:
        if any(k in s for k in keys):
            return out if out in TIPOS_IMOVEL_VALIDOS else "desconhecido"
    if "geminada" in s or re.search(r"\b(casa|casas)\b", s) or "terrea" in s or "terreo" in s:
        return "casa"
    return "desconhecido"


def normalizar_conservacao(val: Any) -> Optional[str]:
    s = str(val or "").strip().lower()
    if not s:
        return None
    if any(x in s for x in ("novo", "lançamento", "lancamento", "na planta", "naplanta", "construtora")):
        out = "novo"
    elif any(x in s for x in ("usado", "revenda", "antigo")):
        out = "usado"
    else:
        out = "desconhecido"
    return out if out in CONSERVACAO_VALIDAS else "desconhecido"


def normalizar_tipo_casa(val: Any, tipo_imovel: Optional[str]) -> Optional[str]:
    ti = str(tipo_imovel or "").strip().lower()
    if ti == "sobrado":
        return "sobrado"
    if ti not in ("casa", "casa_condominio"):
        return None
    s = str(val or "").strip().lower()
    if any(x in s for x in ("sobrado", "duplex", "triplex")):
        out = "sobrado"
    elif any(x in s for x in ("terrea", "térrea", "terreo", "térreo", "pavimento único")):
        out = "terrea"
    else:
        out = "desconhecido" if s else "desconhecido"
    return out if out in TIPO_CASA_VALIDOS else "desconhecido"


def normalizar_data_para_iso(val: Any) -> Optional[str]:
    """Retorna YYYY-MM-DD ou None."""
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, date):
        return val.isoformat()
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    m = re.match(r"^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})$", s)
    if m:
        d, mo, ys = int(m.group(1)), int(m.group(2)), m.group(3)
        y = int(ys)
        if len(ys) == 2:
            y += 2000 if y < 70 else 1900
        try:
            return date(y, mo, d).isoformat()
        except ValueError:
            return None
    return None


def normalizar_url_leilao(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u
    return u
