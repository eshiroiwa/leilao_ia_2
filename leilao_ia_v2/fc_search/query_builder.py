"""Monta frase de busca em linguagem natural a partir do registro do leilão."""

from __future__ import annotations

import json
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


def _parse_extra_leilao(leilao: dict[str, Any]) -> dict[str, Any]:
    raw = leilao.get("leilao_extra_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            j = json.loads(raw)
            if isinstance(j, dict):
                return j
        except Exception:
            return {}
    return {}


def _normalizar_nome_empreendimento(nome: str) -> str:
    s = " ".join(str(nome or "").strip().split())
    s = re.sub(r"(?i)^(condom[ií]nio|edif[ií]cio|pr[eé]dio)\s+", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip(" -,:;.")
    return s[:140]


def _nome_empreendimento_leilao(leilao: dict[str, Any]) -> str:
    extra = _parse_extra_leilao(leilao)
    chaves = (
        "nome_condominio",
        "condominio",
        "nome_predio",
        "predio",
        "nome_edificio",
        "edificio",
        "nome_empreendimento",
        "empreendimento",
    )
    for k in chaves:
        v = _normalizar_nome_empreendimento(str(extra.get(k) or leilao.get(k) or ""))
        if v:
            return v

    obs = str(extra.get("observacoes_markdown") or "")
    if obs:
        m = re.search(
            r"(?im)\b(?:condom[ií]nio|edif[ií]cio|pr[eé]dio)\s*[:\-]\s*([^\n,.;]{3,120})",
            obs,
        )
        if m:
            v = _normalizar_nome_empreendimento(m.group(1))
            if v:
                return v
        for ln in obs.splitlines():
            s = " ".join(str(ln or "").strip().split())
            if len(s) < 8:
                continue
            if re.search(r"(?i)\b(condom[ií]nio|edif[ií]cio|pr[eé]dio)\b", s):
                v2 = _normalizar_nome_empreendimento(s)
                if v2:
                    return v2
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


def _frase_camadas_base(
    *,
    tipo: str,
    area: float,
    rua: str,
    bairro: str,
    cidade: str,
    uf_f: str,
    incluir_rua: bool,
    incluir_bairro: bool,
) -> str:
    partes: list[str] = [tipo]
    if area > 0:
        partes.append(f"de {int(round(area))}m²")
    partes.append("à venda")
    if incluir_rua and rua:
        partes.append(f"na {rua}")
    if incluir_bairro and bairro:
        partes.append(bairro)
    if cidade:
        partes.append(cidade)
    if uf_f:
        partes.append(uf_f)
    return ", ".join(partes).strip().lower()[:900]


def _frase_empreendimento(
    *,
    tipo: str,
    empreendimento: str,
    cidade: str,
    uf_f: str,
) -> str:
    ep = _normalizar_nome_empreendimento(empreendimento)
    if not ep:
        return ""
    if tipo in {"casa", "sobrado", "casa_condominio"}:
        base = f"casas à venda no condomínio {ep}"
    elif tipo == "apartamento":
        base = f"apartamentos à venda no condomínio ou prédio {ep}"
    else:
        base = f"imóveis à venda no empreendimento {ep}"
    partes = [base]
    if cidade:
        partes.append(cidade)
    if uf_f:
        partes.append(uf_f)
    return ", ".join(partes).strip().lower()[:900]


def montar_frases_busca_mercado_em_camadas(
    leilao: dict[str, Any],
    tipo_mercado: str | None,
    *,
    bairro_canonico: str = "",
    bairro_aliases: list[str] | None = None,
) -> list[str]:
    """
    Gera frases Q1/Q2/Q3 para descoberta + validação geográfica:
    - Q1: rua + cidade + UF + tipo
    - Q2: cidade + UF + tipo (sem bairro)
    - Q3: bairro canônico (+ aliases) + cidade + UF + tipo
    """
    tm = (tipo_mercado or "").strip().lower()
    row = dict(leilao)
    if tm:
        if tm in ("terreno", "lote"):
            row["tipo_imovel"] = "lote" if tm == "lote" else "terreno"
        else:
            row["tipo_imovel"] = tm

    tipo = (row.get("tipo_imovel") or "imóvel").strip().lower()
    if tipo in ("", "desconhecido"):
        tipo = "imóvel"
    area = _area_referencia_m2(row)
    rua = _logradouro_sem_numeros_complementos((row.get("endereco") or "").strip())
    bai_info = (row.get("bairro") or "").strip()
    cidade = (row.get("cidade") or "").strip()
    uf_f = _normalizar_uf_frase((row.get("estado") or "").strip())
    bai_canon = (bairro_canonico or "").strip()
    aliases = list(bairro_aliases or [])
    empreendimento = _nome_empreendimento_leilao(row)

    out: list[str] = []
    # Q0: foco por empreendimento/condomínio/prédio.
    q_emp = _frase_empreendimento(
        tipo=tipo,
        empreendimento=empreendimento,
        cidade=cidade,
        uf_f=uf_f,
    )
    if q_emp:
        out.append(q_emp)
    # Q1
    if rua:
        out.append(
            _frase_camadas_base(
                tipo=tipo,
                area=area,
                rua=rua,
                bairro=bai_info,
                cidade=cidade,
                uf_f=uf_f,
                incluir_rua=True,
                incluir_bairro=False,
            )
        )
    # Q2
    out.append(
        _frase_camadas_base(
            tipo=tipo,
            area=area,
            rua=rua,
            bairro=bai_info,
            cidade=cidade,
            uf_f=uf_f,
            incluir_rua=False,
            incluir_bairro=False,
        )
    )
    # Q3
    bairros_q3: list[str] = []
    if bai_canon:
        bairros_q3.append(bai_canon)
    for a in aliases:
        s = str(a or "").strip()
        if not s:
            continue
        if _fold_alfa(s) in {_fold_alfa(x) for x in bairros_q3}:
            continue
        bairros_q3.append(s)
        if len(bairros_q3) >= 3:
            break
    if not bairros_q3 and bai_info:
        bairros_q3.append(bai_info)
    for bq in bairros_q3:
        out.append(
            _frase_camadas_base(
                tipo=tipo,
                area=area,
                rua=rua,
                bairro=bq,
                cidade=cidade,
                uf_f=uf_f,
                incluir_rua=False,
                incluir_bairro=True,
            )
        )
    # dedupe mantendo ordem
    uniq: list[str] = []
    seen: set[str] = set()
    for q in out:
        qn = q.strip().lower()
        if not qn or qn in seen:
            continue
        seen.add(qn)
        uniq.append(qn)
    return uniq
