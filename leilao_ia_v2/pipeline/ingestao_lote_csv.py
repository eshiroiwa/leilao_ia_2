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
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from supabase import Client

from leilao_ia_v2.constants import STATUS_PENDENTE
from leilao_ia_v2.normalizacao import normalizar_tipo_imovel, normalizar_url_leilao
from leilao_ia_v2.persistence import anuncios_mercado_repo, leilao_imoveis_repo
from leilao_ia_v2.services.cache_media_leilao import resolver_cache_media_pos_ingestao
from leilao_ia_v2.services.extracao_edital_llm import _deve_omitir_temperature, _extrair_json_objeto, _kwargs_limite_saida
from leilao_ia_v2.services.geocoding import geocodificar_endereco
from leilao_ia_v2.vivareal.uf_segmento import estado_livre_para_sigla_uf

logger = logging.getLogger(__name__)


# Default mais conservador que o pós-ingestão URL: em CSVs grandes, gastar 20
# créditos por linha causa estouros. 5 créditos/item permite 1 search + 4
# scrapes refinados, suficiente para a maioria dos casos com BD razoável.
CSV_BATCH_FIRECRAWL_DEFAULT_PER_ITEM = 5

# Quando o BD já tem ao menos esse número de anúncios distintos para
# (cidade, UF, tipo), pulamos completamente Firecrawl no item — mesmo que o
# usuário tenha configurado um cap > 0.
CSV_BATCH_BD_PRE_CHECK_MIN = 10

_URL_CANDS = (
    "url_leilao",
    "url",
    "link",
    "link de acesso",
    "link_de_acesso",
    "link acesso",
    "href",
)

_MAPEAMENTO_CAMPOS_ALVO: tuple[str, ...] = (
    "url_leilao",
    "cidade",
    "estado",
    "bairro",
    "endereco",
    "tipo_imovel",
    "descricao",
    "modalidade_venda",
    "area_util",
    "area_total",
    "valor_avaliacao",
    "valor_lance_1_praca",
    "valor_lance_2_praca",
    "valor_arrematacao",
    "url_foto_imovel",
)

_ALIASES_CAMPOS: dict[str, tuple[str, ...]] = {
    "url_leilao": ("url", "url_leilao", "link", "link de acesso", "href"),
    "cidade": ("cidade", "municipio"),
    "estado": ("uf", "estado"),
    "bairro": ("bairro",),
    "endereco": ("endereco", "endereço", "logradouro"),
    "tipo_imovel": ("tipo_imovel", "tipo de imovel", "tipo"),
    "descricao": ("descricao", "descrição", "detalhes"),
    "modalidade_venda": ("modalidade de venda", "modalidade_venda", "modalidade"),
    "area_util": ("area_util", "area util", "área útil", "area privativa"),
    "area_total": ("area_total", "area total", "área total", "area terreno"),
    "valor_avaliacao": ("valor de avaliacao", "valor_avaliacao", "avaliação", "avaliacao"),
    "valor_lance_1_praca": (
        "valor_lance_1_praca",
        "lance 1",
        "1 leilao",
        "1o leilao",
        "1º leilao",
        "1 praca",
        "1a praca",
        "1ª praça",
    ),
    "valor_lance_2_praca": (
        "valor_lance_2_praca",
        "lance 2",
        "2 leilao",
        "2o leilao",
        "2º leilao",
        "2 praca",
        "2a praca",
        "2ª praça",
    ),
    "valor_arrematacao": ("valor_arrematacao", "preco", "preço", "lance minimo", "valor do imovel"),
    "url_foto_imovel": ("url_foto_imovel", "foto", "imagem", "link_foto", "url da foto"),
}


@dataclass
class MapeamentoCamposCsv:
    campos: dict[str, str] = field(default_factory=dict)
    confianca: dict[str, float] = field(default_factory=dict)
    origem: str = "heuristico"


_MAPEAMENTO_CSV_CACHE: dict[str, MapeamentoCamposCsv] = {}


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
    # Preview precisa ser instantâneo: apenas heurística local (sem chamada LLM).
    mapeamento = resolver_mapeamento_campos_csv(regs, permitir_llm=False)
    total = len(regs)
    validas = 0
    preview: list[dict[str, str]] = []
    for i, reg in enumerate(regs, start=1):
        url = _col_url(reg, mapeamento=mapeamento)
        if url:
            validas += 1
            if len(preview) < int(max(0, preview_limite)):
                preview.append(
                    {
                        "linha": str(i),
                        "url": url,
                        "cidade": str(
                            _valor_mapeado(reg, mapeamento, "cidade", ("cidade", "municipio")) or ""
                        ).strip(),
                        "uf": str(_valor_mapeado(reg, mapeamento, "estado", ("uf", "estado")) or "").strip(),
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


def _score_alias_coluna(coluna_norm: str, aliases: tuple[str, ...]) -> float:
    c = _norm_key(coluna_norm)
    if not c:
        return 0.0
    best = 0.0
    for a in aliases:
        an = _norm_key(a)
        if not an:
            continue
        if c == an:
            best = max(best, 1.0)
        elif c.startswith(an) or c.endswith(an):
            best = max(best, 0.9)
        elif an in c:
            best = max(best, 0.75)
    return best


def _mapear_colunas_heuristico(headers_norm: list[str]) -> MapeamentoCamposCsv:
    out = MapeamentoCamposCsv()
    usados: set[str] = set()
    for campo in _MAPEAMENTO_CAMPOS_ALVO:
        aliases = _ALIASES_CAMPOS.get(campo, ())
        best_col = ""
        best_score = 0.0
        for h in headers_norm:
            if h in usados:
                continue
            sc = _score_alias_coluna(h, aliases)
            if sc > best_score:
                best_score = sc
                best_col = h
        if best_col and best_score >= 0.72:
            out.campos[campo] = best_col
            out.confianca[campo] = round(best_score, 3)
            usados.add(best_col)
    return out


def _modelo_llm_mapeamento_csv() -> str:
    m = str(os.getenv("OPENAI_MODEL_CSV_MAPPING", "") or "").strip()
    if m:
        return m
    m2 = str(os.getenv("OPENAI_CHAT_MODEL", "") or "").strip()
    if m2:
        return m2
    return "gpt-4o-mini"


def _llm_mapeamento_csv_habilitado() -> bool:
    """
    Evita latência inesperada no fluxo interativo; LLM só roda quando explicitamente habilitada.
    """
    raw = str(os.getenv("CSV_MAPPING_USE_LLM", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "sim"}


def _mapear_colunas_llm(
    headers_norm: list[str],
    regs: list[dict[str, Any]],
) -> MapeamentoCamposCsv | None:
    if not os.getenv("OPENAI_API_KEY", "").strip():
        return None
    try:
        from openai import OpenAI
    except Exception:
        return None
    mid = _modelo_llm_mapeamento_csv()
    amostra: list[dict[str, str]] = []
    for reg in regs[:5]:
        item: dict[str, str] = {}
        for h in headers_norm[:80]:
            v = reg.get(h)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                item[h] = s[:120]
        if item:
            amostra.append(item)
    if not headers_norm:
        return None
    system = (
        "Você mapeia colunas de CSV de leilões para campos internos. "
        "Responda APENAS JSON no formato: "
        "{\"mapping\":{\"campo\":{\"column\":\"coluna_csv_ou_vazio\",\"confidence\":0.0-1.0}},\"notes\":[...]}. "
        "Nunca invente coluna inexistente."
    )
    user = {
        "campos_alvo": list(_MAPEAMENTO_CAMPOS_ALVO),
        "headers_csv": headers_norm,
        "amostra_linhas": amostra,
        "regras": {
            "url_foto_imovel": "aceite colunas de foto/imagem/link foto",
            "valor_lance_1_praca": "aceite 1o/1º leilao, 1a/1ª praca",
            "valor_lance_2_praca": "aceite 2o/2º leilao, 2a/2ª praca",
            "valor_arrematacao": "aceite preco/lance minimo/valor de venda",
        },
    }
    cli = OpenAI()
    kw: dict[str, Any] = {
        "model": mid,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
    }
    kw.update(_kwargs_limite_saida(mid))
    if not _deve_omitir_temperature(mid):
        kw["temperature"] = 0.0
    try:
        comp = cli.chat.completions.create(**kw)
    except Exception:
        logger.debug("mapeamento llm csv falhou", exc_info=True)
        return None
    txt = str((comp.choices[0].message.content or "") if comp.choices else "")
    blob = _extrair_json_objeto(txt)
    if not blob.strip():
        return None
    try:
        data = json.loads(blob)
    except Exception:
        return None
    mp = data.get("mapping") if isinstance(data, dict) else None
    if not isinstance(mp, dict):
        return None
    out = MapeamentoCamposCsv(origem="llm")
    valid_headers = set(headers_norm)
    for campo in _MAPEAMENTO_CAMPOS_ALVO:
        ent = mp.get(campo)
        if not isinstance(ent, dict):
            continue
        col = _norm_key(ent.get("column"))
        try:
            conf = float(ent.get("confidence"))
        except Exception:
            conf = 0.0
        if col and col in valid_headers and conf >= 0.55:
            out.campos[campo] = col
            out.confianca[campo] = max(0.0, min(1.0, conf))
    return out if out.campos else None


def resolver_mapeamento_campos_csv(
    regs: list[dict[str, Any]],
    *,
    permitir_llm: bool = False,
) -> MapeamentoCamposCsv:
    headers: list[str] = []
    seen: set[str] = set()
    for reg in regs[:20]:
        for k in reg.keys():
            nk = _norm_key(k)
            if nk and nk not in seen:
                seen.add(nk)
                headers.append(nk)
    key_headers = "|".join(headers)
    key_modo = "llm" if bool(permitir_llm and _llm_mapeamento_csv_habilitado()) else "heur"
    key_cache = f"{key_modo}|{key_headers}"
    cached = _MAPEAMENTO_CSV_CACHE.get(key_cache)
    if cached is not None:
        return cached
    base = _mapear_colunas_heuristico(headers)
    if not (permitir_llm and _llm_mapeamento_csv_habilitado()):
        _MAPEAMENTO_CSV_CACHE[key_cache] = base
        return base
    llm = _mapear_colunas_llm(headers, regs)
    if llm is None:
        _MAPEAMENTO_CSV_CACHE[key_cache] = base
        return base
    # Mescla: LLM ganha quando tiver confiança melhor.
    out = MapeamentoCamposCsv(
        campos=dict(base.campos),
        confianca=dict(base.confianca),
        origem="heuristico+llm",
    )
    for campo, col in llm.campos.items():
        conf_llm = float(llm.confianca.get(campo, 0.0))
        conf_base = float(base.confianca.get(campo, 0.0))
        if campo not in out.campos or conf_llm >= max(0.78, conf_base + 0.05):
            out.campos[campo] = col
            out.confianca[campo] = conf_llm
    _MAPEAMENTO_CSV_CACHE[key_cache] = out
    return out


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


def _parse_url_generica(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    return s


def _url_http_ou_vazio(v: Any) -> str:
    # Para foto, manter URL original (incl. fragment/query), sem normalização de URL de leilão.
    u = _parse_url_generica(v)
    if u.lower().startswith(("http://", "https://")):
        return u
    return ""


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


def _valor_mapeado(
    reg: dict[str, Any],
    mapeamento: MapeamentoCamposCsv | None,
    campo: str,
    aliases: tuple[str, ...],
) -> Any:
    if mapeamento is not None:
        k_map = _norm_key(mapeamento.campos.get(campo, ""))
        if k_map and k_map in reg:
            v = reg.get(k_map)
            if str(v or "").strip():
                return v
    for a in aliases:
        ak = _norm_key(a)
        if ak in reg and str(reg.get(ak) or "").strip():
            return reg.get(ak)
    return None


def _col_url(reg: dict[str, Any], *, mapeamento: MapeamentoCamposCsv | None = None) -> str:
    vmap = _valor_mapeado(reg, mapeamento, "url_leilao", _URL_CANDS)
    if str(vmap or "").strip():
        return _parse_url(vmap)
    for k in _URL_CANDS:
        if k in reg and str(reg.get(k) or "").strip():
            return _parse_url(reg.get(k))
    for v in reg.values():
        sv = str(v or "").strip()
        if sv.lower().startswith(("http://", "https://")):
            return _parse_url(sv)
    return ""


def _payload_de_registro_csv(
    reg: dict[str, Any],
    *,
    url: str,
    mapeamento: MapeamentoCamposCsv | None = None,
) -> dict[str, Any]:
    uf = str(_valor_mapeado(reg, mapeamento, "estado", ("uf", "estado")) or "").strip()
    tipo = str(_valor_mapeado(reg, mapeamento, "tipo_imovel", ("tipo_imovel", "tipo de imovel", "tipo")) or "").strip()
    descricao = str(
        _valor_mapeado(reg, mapeamento, "descricao", ("descricao", "descrição", "detalhes")) or ""
    ).strip()
    modalidade = str(
        _valor_mapeado(
            reg,
            mapeamento,
            "modalidade_venda",
            ("modalidade de venda", "modalidade_venda", "modalidade"),
        )
        or ""
    ).strip()
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
    area_util = _parse_num_br(
        _valor_mapeado(reg, mapeamento, "area_util", ("area_util", "area util", "área útil", "area privativa"))
    )
    area_total = _parse_num_br(
        _valor_mapeado(reg, mapeamento, "area_total", ("area_total", "area total", "área total", "area terreno"))
    )
    if area_util is None:
        area_util = _extrair_area_de_descricao(descricao, terreno=False)
    if area_total is None:
        area_total = _extrair_area_de_descricao(descricao, terreno=True)
    v_av = _parse_num_br(
        _valor_mapeado(reg, mapeamento, "valor_avaliacao", ("valor de avaliacao", "valor_avaliacao", "avaliacao"))
    )
    v_l1 = _parse_num_br(
        _valor_mapeado(
            reg,
            mapeamento,
            "valor_lance_1_praca",
            ("valor_lance_1_praca", "lance 1", "1o leilao", "1º leilao", "1a praca", "1ª praça"),
        )
    )
    v_l2 = _parse_num_br(
        _valor_mapeado(
            reg,
            mapeamento,
            "valor_lance_2_praca",
            ("valor_lance_2_praca", "lance 2", "2o leilao", "2º leilao", "2a praca", "2ª praça"),
        )
    )
    v_ar = _parse_num_br(
        _valor_mapeado(
            reg,
            mapeamento,
            "valor_arrematacao",
            ("valor_arrematacao", "preco", "preço", "lance minimo", "valor do imovel"),
        )
    )
    if v_ar is None:
        v_ar = v_l2 if v_l2 is not None else v_l1
    if v_l1 is None and v_ar is not None:
        v_l1 = v_ar
    foto = _url_http_ou_vazio(
        _valor_mapeado(
            reg,
            mapeamento,
            "url_foto_imovel",
            ("url_foto_imovel", "foto", "imagem", "link_foto", "url da foto"),
        )
    )
    payload: dict[str, Any] = {
        "url_leilao": url,
        "status": STATUS_PENDENTE,
        "cache_media_bairro_ids": [],
        "cidade": str(_valor_mapeado(reg, mapeamento, "cidade", ("cidade", "municipio")) or "").strip() or None,
        "estado": uf[:2].upper() if uf else None,
        "bairro": str(_valor_mapeado(reg, mapeamento, "bairro", ("bairro",)) or "").strip() or None,
        "endereco": str(_valor_mapeado(reg, mapeamento, "endereco", ("endereco", "endereço", "logradouro")) or "").strip() or None,
        "tipo_imovel": str(tipo_n or tipo or "").strip() or None,
        "area_util": area_util,
        "area_total": area_total,
        "valor_avaliacao": v_av,
        "valor_lance_1_praca": v_l1,
        "valor_lance_2_praca": v_l2,
        "valor_arrematacao": v_ar,
        "url_foto_imovel": foto or None,
        "leilao_extra_json": {
            k: v
            for k, v in {
                "modalidade_venda_arquivo": modalidade if modalidade else None,
                "ingestao_csv_mapeamento_origem": (mapeamento.origem if mapeamento else "heuristico"),
                "ingestao_csv_campos_mapeados": (mapeamento.campos if mapeamento else {}),
            }.items()
            if v not in (None, "", {}, [])
        },
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


def _resolver_cap_item_csv(
    client: Client,
    payload: dict[str, Any],
    bd_count_cache: dict[tuple[str, str, str], int],
    *,
    cap_default: int,
) -> int:
    """Decide o cap de Firecrawl para esta linha do CSV.

    Aplica um pre-check no BD: se a tupla (cidade, UF, tipo) já tem
    ``CSV_BATCH_BD_PRE_CHECK_MIN`` ou mais anúncios persistidos, devolvemos
    ``0`` (não vale a pena gastar Firecrawl quando o cache vai usar BD).

    O resultado da contagem é memorizado em ``bd_count_cache`` para que
    linhas seguintes da mesma cidade/UF/tipo não repitam o SELECT.
    """
    cidade = str(payload.get("cidade") or "").strip()
    estado_raw = str(payload.get("estado") or "").strip()
    tipo = str(payload.get("tipo_imovel") or "").strip().lower()
    if not cidade or not estado_raw or not tipo:
        return int(cap_default)
    uf = (estado_livre_para_sigla_uf(estado_raw) or estado_raw[:2]).upper()
    if len(uf) != 2:
        return int(cap_default)
    chave = (cidade.lower(), uf, tipo)
    if chave not in bd_count_cache:
        try:
            ads = anuncios_mercado_repo.listar_por_cidade_estado_tipos(
                client,
                cidade=cidade,
                estado_sigla=uf,
                tipos_imovel=[tipo],
                limite=max(50, CSV_BATCH_BD_PRE_CHECK_MIN * 2),
            )
            bd_count_cache[chave] = len(ads or [])
        except Exception:
            logger.exception(
                "CSV pre-check BD falhou (cidade=%s uf=%s tipo=%s) — usando cap default",
                cidade,
                uf,
                tipo,
            )
            bd_count_cache[chave] = -1  # marca falha para não retentar
    n_bd = bd_count_cache.get(chave, -1)
    if n_bd >= CSV_BATCH_BD_PRE_CHECK_MIN:
        logger.info(
            "CSV item: BD já tem %s anúncios (%s/%s/%s) — Firecrawl desativado.",
            n_bd,
            cidade,
            uf,
            tipo,
        )
        return 0
    return int(cap_default)


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
    # Processamento efetivo pode usar LLM, mas somente se habilitada via env.
    mapeamento = resolver_mapeamento_campos_csv(regs, permitir_llm=True)
    resultados: list[LinhaLoteResultado] = []
    urls_validas = 0
    ok = erro = ignorados = processados = 0
    total_previsto = min(len(regs), int(max_itens)) if max_itens is not None and int(max_itens) > 0 else len(regs)
    cancelado = False

    # Cache local entre linhas: contagem de anúncios já presentes no BD por
    # (cidade, UF, tipo). Um item de Aparecida só consulta o BD uma vez —
    # itens subsequentes da mesma cidade/tipo reusam o número.
    bd_anuncios_count: dict[tuple[str, str, str], int] = {}

    # Cap padrão por item — se o usuário não passou explicitamente, usamos o
    # default conservador (5) em vez do default global da config (~20),
    # evitando estouro em CSVs grandes.
    cap_default_por_item = (
        int(max_chamadas_api_firecrawl_por_item)
        if max_chamadas_api_firecrawl_por_item is not None
        else CSV_BATCH_FIRECRAWL_DEFAULT_PER_ITEM
    )

    for i, reg in enumerate(regs, start=1):
        if should_stop is not None and should_stop():
            cancelado = True
            break
        if max_itens is not None and processados >= int(max_itens):
            break
        url = _col_url(reg, mapeamento=mapeamento)
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
            payload = _payload_de_registro_csv(reg, url=url, mapeamento=mapeamento)
            _geocodificar_payload_csv(payload)
            leilao_id, modo = _upsert_leilao_por_csv(payload, client)

            # Pre-check de BD: se a (cidade, UF, tipo) deste item já tem N
            # anúncios suficientes na base, passamos cap=0 para o resolver
            # — vai listar do BD e nem chamar Firecrawl. Economiza chamadas
            # em lotes onde várias linhas vêm da mesma cidade.
            cap_do_item = _resolver_cap_item_csv(
                client,
                payload,
                bd_anuncios_count,
                cap_default=cap_default_por_item,
            )

            res_cache = resolver_cache_media_pos_ingestao(
                client,
                leilao_id,
                ignorar_cache_firecrawl=ignorar_cache_firecrawl,
                max_chamadas_api_firecrawl=cap_do_item,
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
