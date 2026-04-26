"""Monta frase de busca em linguagem natural a partir do registro do leilão."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf


def _area_referencia_m2(leilao: dict[str, Any]) -> float:
    for k in ("area_util", "area_total"):
        v = leilao.get(k)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _fold_alfa(s: str) -> str:
    t = "".join(
        c for c in unicodedata.normalize("NFD", (s or "").lower()) if unicodedata.category(c) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", "", t)


def _segmento_eh_complemento_ou_cep(part: str) -> bool:
    """
    Números de casa/apto/bloco, CEP, ou somente numérico (ex.: 150, 1000-A).
    """
    p = (part or "").strip()
    if not p:
        return True
    if re.search(r"(?i)\bcep\b", p):
        return True
    if re.search(r"\b\d{5}\s*[-]?\s*\d{3}\b", p):
        return True
    if re.match(r"(?i)N[º°.]?\s*\d", p) or re.match(r"(?i)N\.\s*\d", p):
        return True
    if re.search(
        r"(?i)\b(?:apto|ap\.?|ap\.)\s*\.?\s*\d+",
        f" {p} ",
    ):
        return True
    if re.search(r"(?i)\b(?:bl|bl\.|bloco)\s*\.?\s*\d+\b", p):
        return True
    if re.search(r"(?i)\b(?:casa|cs)\.?\s*\d+\b", p):
        return True
    if re.search(r"(?i)\b(?:casa|cs)\.?\s*[a-z]\b", p) and re.search(r"\d", p):
        return True
    if re.search(r"(?i)\b(?:lote|lt)\.?\s*\d+\b", p):
        return True
    if re.match(r"^[\d\s./-]+[a-zA-Z]?$", p) and re.search(r"\d", p):
        return True
    if p.replace(".", "").isdigit() or (len(p) <= 6 and re.match(r"^\d+[\s-]?[A-Za-z]?$", p)):
        return True
    if re.search(r"(?i)\b(?:apto|ap\.?|n\.?|n[º°]|bl\.?|bloco)\b", p) and re.search(
        r"\d", p
    ):
        return True
    return False


def _logradouro_sem_numeros_complementos(endereco: str) -> str:
    """
    Mantém só o nome de logradouro(ões): remove CEP, tudo a partir de Nº/apto/bloco/ número puro, etc.
    """
    s = (endereco or "").strip()
    if not s:
        return ""
    s = s.split("–", 1)[0] if "–" in s else s
    s = re.split(r"(?i)\s*[-–]\s*CEP\s*:", s, maxsplit=1)[0]
    s = re.split(r"(?i)\bCEP\s*:", s, maxsplit=1)[0]
    s = re.sub(r"(?i)\bCEP\s*[\d.\s-]+", "", s)
    s = s.strip(" ,-–/")
    if not s:
        return ""
    partes = [p.strip() for p in s.split(",") if p.strip()]
    boas: list[str] = []
    for p in partes:
        if _segmento_eh_complemento_ou_cep(p):
            break
        boas.append(p)
    return ", ".join(boas).strip(" ,-–/")


def _normalizar_uf_frase(uf: str) -> str:
    sig = estado_livre_para_sigla_uf(uf)
    if sig:
        return sig.lower()
    t = (uf or "").strip()
    if len(t) == 2 and t.isalpha():
        return t.lower()
    return ""


def montar_frase_busca_mercado(leilao: dict[str, Any], tipo_mercado: str | None) -> str:
    """
    Frase de busca alinhada ao segmento de mercado (terreno/lote vs casa etc.).

    O complemento Firecrawl usa isto para não repetir a query do edital quando o alvo são terrenos.
    """
    tm = (tipo_mercado or "").strip().lower()
    if not tm:
        return montar_frase_busca(leilao)
    row = dict(leilao)
    if tm in ("terreno", "lote"):
        row["tipo_imovel"] = "lote" if tm == "lote" else "terreno"
    else:
        row["tipo_imovel"] = tm
    return montar_frase_busca(row)


def montar_frase_busca(leilao: dict[str, Any]) -> str:
    """
    Ex.: ``apartamento, de 39m², à venda, na rua barao carlos de sousa anhumas, jardim recanto verde, sp``.

    Não inclui CEP, número de imóvel, apartamento, bloco etc.; só logradouro, bairro (se houver) e UF.
    Campos: ``tipo_imovel``, área, ``endereco``, ``bairro``, ``cidade`` (só se sem bairro), ``estado``.
    """
    tipo = (leilao.get("tipo_imovel") or "imóvel").strip().lower()
    if tipo in ("", "desconhecido"):
        tipo = "imóvel"

    area = _area_referencia_m2(leilao)
    rua = _logradouro_sem_numeros_complementos((leilao.get("endereco") or "").strip())
    bai = (leilao.get("bairro") or "").strip()
    cid = (leilao.get("cidade") or "").strip()
    uf_f = _normalizar_uf_frase((leilao.get("estado") or "").strip())

    if bai and rua:
        fb = _fold_alfa(bai)
        segs = [p.strip() for p in rua.split(",") if p.strip()]
        segs = [p for p in segs if _fold_alfa(p) != fb]
        rua = ", ".join(segs).strip(" ,-–/")

    partes: list[str] = [tipo]
    if area > 0:
        partes.append(f"de {int(round(area))}m²")
    partes.append("à venda")
    if rua:
        partes.append(f"na {rua}")
    if bai:
        partes.append(bai)
    if cid:
        # Mantém cidade explícita para reduzir ambiguidade em bairros homônimos
        # e evitar resultados de municípios diferentes na busca web.
        partes.append(cid)
    if uf_f:
        partes.append(uf_f)

    out = ", ".join(partes).strip()
    return out.lower()[:900]
