"""
Status e convenções alinhados ao schema Supabase `leilao_imoveis` do projeto.

Colunas esperadas: url_leilao, endereco, cidade, estado, bairro, data_leilao (date ISO),
tipo_imovel, conservacao, tipo_casa, andar, area_util, area_total, quartos, vagas,
valor_arrematacao, valor_mercado_estimado, valor_venda_sugerido, custo_reforma_estimado, roi_projetado, status, created_at.

`segmento_mercado_de_registro` consolida dimensões para cache/triagem (apto vs casa, andar, etc.).
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Any, Optional

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None  # type: ignore[misc, assignment]

STATUS_PENDENTE = "pendente"
STATUS_ANALISADO = "analisado"
STATUS_APROVADO = "aprovado"
# Fluxo interno (coluna `status` é text; valor extra permitido)
STATUS_DESCARTADO_TRIAGEM = "descartado_triagem"

# Aliases legados (leitura de dicts em memória / planilhas antigas)
ALIASES_AREA = ("area_util", "area_m2")
ALIASES_AREA_TOTAL = ("area_total", "area_terreno", "area_lote")
ALIASES_LANCE = ("valor_arrematacao", "valor_lance", "valor_lance_atual")
ALIASES_ANDAR = ("andar", "pavimento", "nivel")


def normalizar_data_leilao_para_iso(val: Any) -> Optional[str]:
    """
    Converte data de planilha/célula/texto para 'YYYY-MM-DD' (compatível com coluna date no Postgres).
    Aceita ISO, dd/mm/aaaa, datetime/date, pandas.Timestamp.
    """
    if val is None or val == "":
        return None
    if pd is not None:
        if isinstance(val, pd.Timestamp):
            if pd.isna(val):
                return None
            return val.date().isoformat()
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, date):
        return val.isoformat()
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
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


def _slug_ascii(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").strip()).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "-"


def andar_de_registro(row: dict[str, Any]) -> Optional[int]:
    for k in ALIASES_ANDAR:
        v = row.get(k)
        if v is None or v == "":
            continue
        try:
            n = int(float(str(v).strip().replace(",", ".")))
            return n
        except (TypeError, ValueError):
            continue
    return None


def faixa_andar_de_numero(andar: Optional[int]) -> str:
    """Faixa para cache: térreo / baixo / médio / alto (aptos). Casas usam `casa`."""
    if andar is None:
        return "-"
    if andar <= 0:
        return "terreo"
    if andar <= 3:
        return "baixo"
    if andar <= 10:
        return "medio"
    return "alto"


def logradouro_chave_de_endereco(endereco: Any, max_partes: int = 4) -> str:
    """
    Primeiros tokens do endereço (sem número), para granularidade por rua quando houver dado.
    """
    if not endereco:
        return "-"
    s = re.sub(r"\s+", " ", str(endereco).strip())
    if not s:
        return "-"
    s = re.split(r",|;|\(|cep", s, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    tokens = re.findall(r"[A-Za-zÀ-ú]{3,}", s, flags=re.IGNORECASE)
    if not tokens:
        return "-"
    frag = " ".join(tokens[:max_partes])
    return _slug_ascii(frag)


def normalizar_tipo_imovel(val: Any) -> str:
    s = str(val or "").strip().lower()
    if not s:
        return "desconhecido"
    if any(x in s for x in ("apto", "apart", "flat", "cobertura", "loft")):
        return "apartamento"
    if any(x in s for x in (
        "casa de condominio", "casa de condomínio", "casa_condominio",
        "condominio fechado", "condomínio fechado", "casa em condominio",
        "casa em condomínio",
    )):
        return "casa_condominio"
    if any(x in s for x in ("casa", "sobrado", "terrea", "térrea", "geminada")):
        return "casa"
    if any(x in s for x in ("terreno", "lote", "gleba", "chacara", "chácara", "sitio", "sítio")):
        return "terreno"
    return "desconhecido"


def normalizar_conservacao(val: Any) -> str:
    s = str(val or "").strip().lower()
    if not s:
        return "desconhecido"
    if any(x in s for x in ("novo", "lançamento", "lancamento", "na planta", "naplanta", "construtora")):
        return "novo"
    if any(x in s for x in ("usado", "revenda", "antigo")):
        return "usado"
    return "desconhecido"


def normalizar_tipo_casa(val: Any, tipo_imovel: str) -> str:
    if tipo_imovel not in ("casa", "casa_condominio"):
        return "-"
    s = str(val or "").strip().lower()
    if any(x in s for x in ("sobrado", "duplex", "triplex")):
        return "sobrado"
    if any(x in s for x in ("terrea", "térrea", "terreo", "térreo", "pavimento único")):
        return "terrea"
    return "desconhecido" if s else "desconhecido"


def segmento_mercado_de_registro(row: dict[str, Any]) -> dict[str, str]:
    """
    Dimensões para chave de cache de referência R$/m².
    Valores estáveis (slug-like) alinhados a normalizar_chave_segmento em token_efficiency.
    """
    tipo = normalizar_tipo_imovel(row.get("tipo_imovel"))
    cons = normalizar_conservacao(row.get("conservacao"))
    tc = normalizar_tipo_casa(row.get("tipo_casa"), tipo)
    ad = andar_de_registro(row)
    if tipo in ("casa", "casa_condominio"):
        faixa = "casa"
    else:
        faixa = faixa_andar_de_numero(ad)
    rua = logradouro_chave_de_endereco(row.get("endereco"))
    area = area_efetiva_de_registro(row)
    return {
        "tipo_imovel": tipo,
        "conservacao": cons,
        "tipo_casa": tc if tipo in ("casa", "casa_condominio") else "-",
        "faixa_andar": faixa,
        "logradouro_chave": rua,
        "faixa_area": faixa_area_de_metragem(area),
    }


def area_util_de_registro(row: dict[str, Any]) -> float:
    for k in ALIASES_AREA:
        v = row.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def area_total_de_registro(row: dict[str, Any]) -> float:
    for k in ALIASES_AREA_TOTAL:
        v = row.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _tipo_eh_terreno(row: dict[str, Any]) -> bool:
    tipo = normalizar_tipo_imovel(row.get("tipo_imovel"))
    return tipo == "terreno"


def area_efetiva_de_registro(row: dict[str, Any]) -> float:
    """Retorna a área relevante para cálculos de preço/m²:
    - Terrenos usam area_total (área do lote/terreno).
    - Demais tipos usam area_util (área construída).
    Fallback: se o campo primário estiver vazio, tenta o alternativo."""
    if _tipo_eh_terreno(row):
        a = area_total_de_registro(row)
        return a if a > 0 else area_util_de_registro(row)
    a = area_util_de_registro(row)
    return a if a > 0 else area_total_de_registro(row)


_FAIXAS_AREA: list[tuple[float, str]] = [
    (60, "ate-60"),
    (100, "61-100"),
    (150, "101-150"),
    (250, "151-250"),
    (500, "251-500"),
]


def faixa_area_de_metragem(area: float) -> str:
    """Classifica a área em faixa para segmentação de cache R$/m²."""
    if area <= 0:
        return "-"
    for teto, label in _FAIXAS_AREA:
        if area <= teto:
            return label
    return "acima-500"


_FAIXA_AREA_LIMITES: dict[str, tuple[int | None, int | None]] = {
    "ate-60": (None, 60),
    "61-100": (61, 100),
    "101-150": (101, 150),
    "151-250": (151, 250),
    "251-500": (251, 500),
    "acima-500": (501, None),
}


def limites_faixa_area(faixa: str) -> tuple[int | None, int | None]:
    """Retorna (area_min, area_max) para uma faixa de área.
    None indica sem limite naquela direção."""
    return _FAIXA_AREA_LIMITES.get(faixa, (None, None))


def _parse_moeda_simples(valor: Any) -> Optional[float]:
    if valor is None:
        return None
    if isinstance(valor, bool):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    s = re.sub(r"R\$\s*", "", s, flags=re.IGNORECASE).strip()
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    if not s or s == ".":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def valor_arrematacao_de_registro(row: dict[str, Any]) -> Optional[float]:
    for k in ALIASES_LANCE:
        v = row.get(k)
        if v is None:
            continue
        p = _parse_moeda_simples(v)
        if p is not None:
            return p
    return None


__all__ = [
    "ALIASES_ANDAR",
    "ALIASES_AREA",
    "ALIASES_AREA_TOTAL",
    "ALIASES_LANCE",
    "andar_de_registro",
    "normalizar_data_leilao_para_iso",
    "STATUS_ANALISADO",
    "STATUS_APROVADO",
    "STATUS_DESCARTADO_TRIAGEM",
    "STATUS_PENDENTE",
    "area_efetiva_de_registro",
    "area_total_de_registro",
    "faixa_area_de_metragem",
    "limites_faixa_area",
    "area_util_de_registro",
    "faixa_andar_de_numero",
    "logradouro_chave_de_endereco",
    "normalizar_conservacao",
    "normalizar_tipo_casa",
    "normalizar_tipo_imovel",
    "segmento_mercado_de_registro",
    "valor_arrematacao_de_registro",
]
