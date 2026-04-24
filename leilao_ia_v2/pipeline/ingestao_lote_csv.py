"""
Ingestao em lote por CSV com colunas variaveis.

Foco da fase 1:
- usar os dados do arquivo (sem scrape de edital);
- persistir/atualizar leilao com URL + campos disponiveis;
- montar cache de mercado (inclui busca de anuncios quando necessario);
- calcular ROI pos-cache (disparado pelo fluxo de cache);
- nunca interromper o lote por falha de um item.
"""

from __future__ import annotations

import csv
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from supabase import Client

from leilao_ia_v2.constants import STATUS_PENDENTE
from leilao_ia_v2.normalizacao import normalizar_tipo_imovel, normalizar_url_leilao
from leilao_ia_v2.persistence import leilao_imoveis_repo
from leilao_ia_v2.services.cache_media_leilao import resolver_cache_media_pos_ingestao
from leilao_ia_v2.services.geocoding import geocodificar_endereco

logger = logging.getLogger(__name__)

_URL_CANDS = (
    "url_leilao",
    "url",
    "link",
    "link de acesso",
    "link_de_acesso",
    "link acesso",
    "href",
)


@dataclass
class LinhaLoteResultado:
    linha: int
    url: str
    status: str
    leilao_id: str = ""
    mensagem: str = ""
    firecrawl_chamadas_api: int = 0


@dataclass
class ResultadoLoteCsv:
    arquivo: str
    total_linhas_csv: int
    total_urls_validas: int
    processados: int
    ok: int
    erro: int
    ignorados: int
    cancelado: bool = False
    resultados: list[LinhaLoteResultado] = field(default_factory=list)


@dataclass
class ResumoCsvLeiloes:
    total_linhas_csv: int
    total_urls_validas: int
    total_sem_url: int
    preview: list[dict[str, str]] = field(default_factory=list)


def resultado_lote_csv_para_dict(r: ResultadoLoteCsv) -> dict[str, Any]:
    return {
        "arquivo": r.arquivo,
        "total_linhas_csv": r.total_linhas_csv,
        "total_urls_validas": r.total_urls_validas,
        "processados": r.processados,
        "ok_itens": r.ok,
        "erro_itens": r.erro,
        "ignorados": r.ignorados,
        "cancelado": bool(r.cancelado),
        "resultados": [
            {
                "linha": x.linha,
                "url": x.url,
                "status": x.status,
                "leilao_id": x.leilao_id,
                "mensagem": x.mensagem,
                "firecrawl_chamadas_api": x.firecrawl_chamadas_api,
            }
            for x in r.resultados
        ],
    }


def resumir_csv_leiloes(
    caminho_csv: str | Path,
    *,
    preview_limite: int = 3,
) -> ResumoCsvLeiloes:
    """Resumo rápido para validação do arquivo antes da ingestão."""
    regs = ler_registros_csv_leiloes(caminho_csv)
    total = len(regs)
    validas = 0
    preview: list[dict[str, str]] = []
    for i, reg in enumerate(regs, start=1):
        url = _col_url(reg)
        if url:
            validas += 1
            if len(preview) < int(max(0, preview_limite)):
                preview.append(
                    {
                        "linha": str(i),
                        "url": url,
                        "cidade": str(reg.get("cidade") or "").strip(),
                        "uf": str(reg.get("uf") or reg.get("estado") or "").strip(),
                    }
                )
    return ResumoCsvLeiloes(
        total_linhas_csv=total,
        total_urls_validas=validas,
        total_sem_url=max(0, total - validas),
        preview=preview,
    )


def _norm_key(s: Any) -> str:
    txt = str(s or "").strip().lower()
    txt = unicodedata.normalize("NFD", txt)
    txt = "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _parse_num_br(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        f = float(s)
    except ValueError:
        return None
    return f


def _parse_url(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    try:
        return normalizar_url_leilao(s)
    except Exception:
        return s


def _guess_delimiter(line: str) -> str:
    cands = (";", ",", "\t", "|")
    best = ";"
    best_n = -1
    for d in cands:
        n = line.count(d)
        if n > best_n:
            best = d
            best_n = n
    return best


def _detectar_header(lines: list[str]) -> tuple[int, str]:
    for i, ln in enumerate(lines[:80]):
        if not ln.strip():
            continue
        d = _guess_delimiter(ln)
        cols = [_norm_key(c) for c in ln.split(d)]
        if any(c in _URL_CANDS for c in cols):
            return i, d
    for i, ln in enumerate(lines[:80]):
        if "http://" in ln.lower() or "https://" in ln.lower():
            d = _guess_delimiter(ln)
            if i > 0:
                return i - 1, d
    return 0, ";"


def ler_registros_csv_leiloes(caminho_csv: str | Path) -> list[dict[str, Any]]:
    path = Path(caminho_csv).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    raw = path.read_bytes()
    txt: str | None = None
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            txt = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if txt is None:
        txt = raw.decode("latin-1", errors="replace")
    lines = txt.splitlines()
    if not lines:
        return []
    idx, delim = _detectar_header(lines)
    data_lines = lines[idx:]
    reader = csv.DictReader(data_lines, delimiter=delim)
    out: list[dict[str, Any]] = []
    for row in reader:
        if not isinstance(row, dict):
            continue
        clean = {(_norm_key(k)): (v.strip() if isinstance(v, str) else v) for k, v in row.items() if k}
        if not any(str(v or "").strip() for v in clean.values()):
            continue
        out.append(clean)
    return out


def _extrair_area_de_descricao(descricao: str, *, terreno: bool = False) -> float | None:
    if not descricao:
        return None
    alvo = "terreno" if terreno else "privativa"
    m = re.search(rf"([0-9]+(?:[.,][0-9]+)?)\s+de\s+area\s+{alvo}", _norm_key(descricao))
    if not m:
        return None
    return _parse_num_br(m.group(1))


def _col_url(reg: dict[str, Any]) -> str:
    for k in _URL_CANDS:
        if k in reg and str(reg.get(k) or "").strip():
            return _parse_url(reg.get(k))
    for v in reg.values():
        sv = str(v or "").strip()
        if sv.lower().startswith(("http://", "https://")):
            return _parse_url(sv)
    return ""


def _payload_de_registro_csv(reg: dict[str, Any], *, url: str) -> dict[str, Any]:
    uf = str(reg.get("uf") or reg.get("estado") or "").strip()
    tipo = str(reg.get("tipo_imovel") or "").strip()
    descricao = str(reg.get("descricao") or reg.get("descrição") or "").strip()
    modalidade = str(reg.get("modalidade de venda") or reg.get("modalidade_venda") or "").strip()
    if not tipo and descricao:
        t0 = _norm_key(descricao)
        if "apartamento" in t0:
            tipo = "apartamento"
        elif "casa" in t0:
            tipo = "casa"
        elif "terreno" in t0 or "lote" in t0:
            tipo = "terreno"
        elif "loja" in t0:
            tipo = "loja"
    tipo_n = normalizar_tipo_imovel(tipo) if tipo else None
    area_util = _parse_num_br(reg.get("area_util") or reg.get("area util"))
    area_total = _parse_num_br(reg.get("area_total") or reg.get("area total"))
    if area_util is None:
        area_util = _extrair_area_de_descricao(descricao, terreno=False)
    if area_total is None:
        area_total = _extrair_area_de_descricao(descricao, terreno=True)
    v_av = _parse_num_br(reg.get("valor de avaliacao") or reg.get("valor_avaliacao"))
    v_lance = _parse_num_br(reg.get("preco") or reg.get("preço") or reg.get("valor_arrematacao"))
    payload: dict[str, Any] = {
        "url_leilao": url,
        "status": STATUS_PENDENTE,
        "cache_media_bairro_ids": [],
        "cidade": str(reg.get("cidade") or "").strip() or None,
        "estado": uf[:2].upper() if uf else None,
        "bairro": str(reg.get("bairro") or "").strip() or None,
        "endereco": str(reg.get("endereco") or reg.get("endereço") or "").strip() or None,
        "tipo_imovel": str(tipo_n or tipo or "").strip() or None,
        "area_util": area_util,
        "area_total": area_total,
        "valor_avaliacao": v_av,
        "valor_arrematacao": v_lance,
        "valor_lance_1_praca": v_lance,
        "leilao_extra_json": {"modalidade_venda_arquivo": modalidade} if modalidade else {},
    }
    return payload


def _upsert_leilao_por_csv(payload: dict[str, Any], client: Client) -> tuple[str, str]:
    url = str(payload.get("url_leilao") or "")
    existente = leilao_imoveis_repo.buscar_por_url_leilao(url, client)
    if existente:
        iid = str(existente.get("id") or "")
        campos = {k: v for k, v in payload.items() if k not in ("url_leilao", "cache_media_bairro_ids") and v is not None}
        if campos:
            leilao_imoveis_repo.atualizar_leilao_imovel(iid, campos, client)
        return iid, "atualizado"
    row = leilao_imoveis_repo.inserir_leilao_imovel(payload, client)
    iid2 = str(row.get("id") or "")
    return iid2, "inserido"


def _geocodificar_payload_csv(payload: dict[str, Any]) -> None:
    """Preenche latitude/longitude quando possível, sem interromper o lote em caso de falha."""
    if payload.get("latitude") is not None and payload.get("longitude") is not None:
        return
    cidade = str(payload.get("cidade") or "").strip()
    estado = str(payload.get("estado") or "").strip()
    if not cidade and not estado:
        return
    try:
        coords = geocodificar_endereco(
            logradouro=str(payload.get("endereco") or "").strip(),
            bairro=str(payload.get("bairro") or "").strip(),
            cidade=cidade,
            estado=estado,
        )
    except Exception:
        logger.debug("geocodificacao csv lote falhou", exc_info=True)
        return
    if not coords:
        return
    try:
        payload["latitude"] = float(coords[0])
        payload["longitude"] = float(coords[1])
    except Exception:
        return


def processar_lote_csv_leiloes(
    caminho_csv: str | Path,
    client: Client,
    *,
    ignorar_cache_firecrawl: bool = False,
    max_itens: int | None = None,
    max_chamadas_api_firecrawl_por_item: int | None = None,
    progress_hook: Callable[[int, int, str], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ResultadoLoteCsv:
    regs = ler_registros_csv_leiloes(caminho_csv)
    resultados: list[LinhaLoteResultado] = []
    urls_validas = 0
    ok = erro = ignorados = processados = 0
    total_previsto = min(len(regs), int(max_itens)) if max_itens is not None and int(max_itens) > 0 else len(regs)
    cancelado = False
    for i, reg in enumerate(regs, start=1):
        if should_stop is not None and should_stop():
            cancelado = True
            break
        if max_itens is not None and processados >= int(max_itens):
            break
        url = _col_url(reg)
        if not url:
            ignorados += 1
            resultados.append(
                LinhaLoteResultado(
                    linha=i,
                    url="",
                    status="ignorado",
                    mensagem="linha sem URL valida",
                )
            )
            if progress_hook is not None:
                progress_hook(processados, total_previsto, "ignorado")
            continue
        urls_validas += 1
        processados += 1
        try:
            payload = _payload_de_registro_csv(reg, url=url)
            _geocodificar_payload_csv(payload)
            leilao_id, modo = _upsert_leilao_por_csv(payload, client)
            res_cache = resolver_cache_media_pos_ingestao(
                client,
                leilao_id,
                ignorar_cache_firecrawl=ignorar_cache_firecrawl,
                max_chamadas_api_firecrawl=max_chamadas_api_firecrawl_por_item,
            )
            firecrawl_calls = int(getattr(res_cache, "firecrawl_chamadas_api", 0) or 0)
            if res_cache.ok:
                ok += 1
                resultados.append(
                    LinhaLoteResultado(
                        linha=i,
                        url=url,
                        status="ok",
                        leilao_id=leilao_id,
                        mensagem=f"{modo}; cache/roi: {res_cache.mensagem}",
                        firecrawl_chamadas_api=firecrawl_calls,
                    )
                )
                if progress_hook is not None:
                    progress_hook(processados, total_previsto, "ok")
            else:
                erro += 1
                resultados.append(
                    LinhaLoteResultado(
                        linha=i,
                        url=url,
                        status="erro",
                        leilao_id=leilao_id,
                        mensagem=res_cache.mensagem,
                        firecrawl_chamadas_api=firecrawl_calls,
                    )
                )
                if progress_hook is not None:
                    progress_hook(processados, total_previsto, "erro")
        except Exception as exc:
            erro += 1
            logger.exception("lote csv linha=%s url=%s", i, url[:120])
            resultados.append(
                LinhaLoteResultado(
                    linha=i,
                    url=url,
                    status="erro",
                    mensagem=str(exc),
                )
            )
            if progress_hook is not None:
                progress_hook(processados, total_previsto, "erro")

    return ResultadoLoteCsv(
        arquivo=str(Path(caminho_csv)),
        total_linhas_csv=len(regs),
        total_urls_validas=urls_validas,
        processados=processados,
        ok=ok,
        erro=erro,
        ignorados=ignorados,
        cancelado=cancelado,
        resultados=resultados,
    )
