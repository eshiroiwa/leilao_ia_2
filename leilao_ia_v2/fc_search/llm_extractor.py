"""Fallback de extração de comparáveis via LLM para markdowns não padronizados."""

from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urljoin

from openai import BadRequestError, OpenAI

from leilao_ia_v2.services.extracao_edital_llm import (
    _deve_omitir_temperature,
    _extrair_json_objeto,
    _kwargs_limite_saida,
)


def llm_extracao_habilitada() -> bool:
    return str(os.getenv("FC_SEARCH_LLM_EXTRACTION_ENABLED", "") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "sim",
    }


def _modelo_llm_fc_search() -> str:
    return (
        str(os.getenv("OPENAI_MODEL_FC_SEARCH_EXTRACTION", "") or "").strip()
        or str(os.getenv("OPENAI_CHAT_MODEL", "") or "").strip()
        or "gpt-4o-mini"
    )


def _normalizar_url_card(url_card: str, url_pagina: str) -> str:
    u = str(url_card or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return f"https:{u}"
    if u.startswith("/"):
        return urljoin(url_pagina, u)
    return u


def extrair_cards_com_llm_markdown(
    *,
    markdown: str,
    url_pagina: str,
    cidade_ref: str,
    estado_ref: str,
    bairro_ref: str,
) -> list[dict[str, Any]]:
    if not llm_extracao_habilitada():
        return []
    key = str(os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not key:
        return []
    texto = str(markdown or "").strip()
    if len(texto) < int(os.getenv("FC_SEARCH_LLM_EXTRACTION_MIN_MD_CHARS", "1800") or "1800"):
        return []
    texto = texto[: int(os.getenv("FC_SEARCH_LLM_MAX_MD_CHARS", "90000") or "90000")]

    schema = {
        "type": "object",
        "properties": {
            "cards": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "url_anuncio": {"type": "string"},
                        "titulo": {"type": "string"},
                        "tipo_imovel": {"type": "string"},
                        "area_m2": {"type": "number"},
                        "valor_venda": {"type": "number"},
                        "quartos": {"type": ["integer", "null"]},
                        "vagas": {"type": ["integer", "null"]},
                    },
                    "required": ["url_anuncio", "area_m2", "valor_venda"],
                },
            }
        },
        "required": ["cards"],
    }
    system = (
        "Você extrai anúncios de imóveis de markdown de listagens brasileiras. "
        "Responda apenas com um JSON válido no formato do schema. "
        "Extraia somente imóveis de venda com URL do anúncio, preço de venda (valor_venda) e área em m² (area_m2). "
        "Ignore blocos de filtros/menu/cookies/nav. "
        "Não invente dados. "
        "Se houver dúvidas de campo textual, prefira null/omitir."
    )
    user = (
        f"Contexto: cidade={cidade_ref}, estado={estado_ref}, bairro={bairro_ref}, url_pagina={url_pagina}\n"
        f"Schema JSON:\n{json.dumps(schema, ensure_ascii=False)}\n\n"
        f"Markdown:\n{texto}"
    )
    cli = OpenAI(api_key=key)
    mid = _modelo_llm_fc_search()
    base_kw: dict[str, Any] = {
        "model": mid,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "response_format": {"type": "json_object"},
    }
    base_kw.update(_kwargs_limite_saida(mid))
    if not _deve_omitir_temperature(mid):
        base_kw["temperature"] = 0.0
    comp = None
    for tentativa in range(3):
        kw = dict(base_kw)
        if tentativa >= 1:
            kw.pop("temperature", None)
        if tentativa >= 2:
            kw.pop("response_format", None)
        try:
            comp = cli.chat.completions.create(**kw)
            break
        except BadRequestError:
            if tentativa < 2:
                continue
            return []
        except Exception:
            return []
    if comp is None:
        return []
    raw = str(getattr(getattr(comp.choices[0], "message", None), "content", "") or "")
    blob = _extrair_json_objeto(raw)
    if not blob.strip():
        return []
    try:
        data = json.loads(blob)
    except Exception:
        return []
    cards_raw = data.get("cards") if isinstance(data, dict) else []
    out: list[dict[str, Any]] = []
    for c in list(cards_raw or []):
        if not isinstance(c, dict):
            continue
        u = _normalizar_url_card(str(c.get("url_anuncio") or ""), url_pagina)
        if not u.startswith("http"):
            continue
        try:
            area = float(c.get("area_m2"))
            valor = float(c.get("valor_venda"))
        except Exception:
            continue
        if area <= 0 or valor <= 0:
            continue
        titulo = str(c.get("titulo") or "").strip()
        tipo = str(c.get("tipo_imovel") or "").strip().lower()
        if tipo and not re.fullmatch(r"[a-z_]{3,40}", tipo):
            tipo = ""
        out.append(
            {
                "url_anuncio": u,
                "portal": urljoin(url_pagina, "/").split("//", 1)[-1].split("/", 1)[0].replace("www.", ""),
                "area_m2": area,
                "valor_venda": valor,
                "quartos": c.get("quartos"),
                "vagas": c.get("vagas"),
                "logradouro": "",
                "titulo": titulo[:500],
                "bairro": bairro_ref,
                "cidade": cidade_ref,
                "estado": estado_ref,
                "_tipo_detectado": tipo,
            }
        )
    return out

