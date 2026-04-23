"""Uma chamada ao modelo OpenAI para estruturar o markdown do edital."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Optional

from openai import BadRequestError, OpenAI

from leilao_ia_v2.schemas.edital import ExtracaoEditalLLM, schema_extracao_edital_dict

logger = logging.getLogger(__name__)


def _deve_omitir_temperature(model_id: str) -> bool:
    """
    Vários modelos novos (ex.: gpt-5, família o*) só aceitam temperatura padrão —
    enviar `temperature` causa 400 Bad Request.
    """
    m = (model_id or "").strip().lower()
    if "gpt-5" in m:
        return True
    if re.match(r"^o\d", m):
        return True
    return False


def _kwargs_limite_saida(model_id: str) -> dict[str, int]:
    """
    ``OPENAI_LLM_MAX_TOKENS`` (opcional): evita JSON truncado em modelos com raciocínio
    ou saídas longas; família gpt-5/o* usa ``max_completion_tokens``.
    """
    raw = (os.getenv("OPENAI_LLM_MAX_TOKENS") or "").strip()
    if not raw:
        return {}
    try:
        n = int(raw)
    except ValueError:
        return {}
    if n <= 0:
        return {}
    m = (model_id or "").strip().lower()
    if "gpt-5" in m or re.match(r"^o\d", m):
        return {"max_completion_tokens": n}
    return {"max_tokens": n}


def _preco_por_milhao_tokens() -> tuple[float, float]:
    """USD por 1M tokens (input, output). Defaults aproximados para gpt-4o-mini."""
    try:
        inp = float(os.getenv("OPENAI_PRICE_INPUT_PER_M_TOK_USD", "0.15"))
    except ValueError:
        inp = 0.15
    try:
        out = float(os.getenv("OPENAI_PRICE_OUTPUT_PER_M_TOK_USD", "0.60"))
    except ValueError:
        out = 0.60
    return inp, out


def _estimar_custo_usd(prompt_tokens: int, completion_tokens: int) -> float:
    pi, po = _preco_por_milhao_tokens()
    return (prompt_tokens / 1_000_000.0) * pi + (completion_tokens / 1_000_000.0) * po


def _extrair_json_objeto(texto: str) -> str:
    texto = (texto or "").strip()
    if not texto:
        return ""
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", texto, re.IGNORECASE)
    if fence:
        texto = fence.group(1).strip()
    i = texto.find("{")
    if i < 0:
        return texto
    depth = 0
    for j in range(i, len(texto)):
        if texto[j] == "{":
            depth += 1
        elif texto[j] == "}":
            depth -= 1
            if depth == 0:
                return texto[i : j + 1]
    return texto[i:]


def extrair_edital_de_markdown(
    markdown: str,
    url_canonica: str,
    *,
    modelo: Optional[str] = None,
) -> tuple[ExtracaoEditalLLM, dict[str, Any]]:
    """
    Uma chamada `chat.completions` com JSON estrito validado por Pydantic.
    Retorna (extracao, metricas) onde metricas inclui tokens e custo estimado.
    """
    client = OpenAI()
    mid = modelo or os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
    schema = schema_extracao_edital_dict()

    system = (
        "Você extrai dados estruturados de edital de leilão de imóveis no Brasil a partir de markdown. "
        "Responda APENAS com um único objeto JSON válido, sem markdown ao redor, sem comentários. "
        "Campos ausentes no texto: use null (não invente). "
        "Preencha `endereco` com o logradouro completo do imóvel sempre que constar no edital (ajuda geocodificação). "
        "Datas no formato YYYY-MM-DD quando conseguir inferir com segurança; senão null. "
        "tipo_imovel: use UMA destas chaves exatas (ASCII, minúsculas, underscore onde indicado): "
        "apartamento, casa, kitnet, casa_condominio, chacara, cobertura, duplex, flat, lote, terreno, sobrado, "
        "predio, edificio, fazenda, sitio, consultorio, galpao, deposito, armazem, imovel_comercial, "
        "ponto_comercial, loja, box, sala, conjunto, desconhecido. "
        "Residenciais: apartamento, casa, kitnet, casa_condominio, chacara, cobertura, duplex, flat, lote, "
        "terreno, sobrado, predio, edificio, fazenda, sitio. "
        "Comerciais: consultorio, galpao, deposito, armazem, imovel_comercial, lote, terreno, ponto_comercial, "
        "loja, box, sala, conjunto, predio, edificio. "
        "Se o texto indicar vários, escolha o mais específico para o bem anunciado. "
        "conservacao: novo, usado ou desconhecido. "
        "tipo_casa (se casa): terrea, sobrado ou desconhecido. "
        "leilao_extra: preencha formas_pagamento, processo_judicial se constar, "
        "regras_leilao_markdown e observacoes_markdown com trechos úteis do edital (pode copiar do markdown). "
        "leilao_extra.modalidade_venda: 'venda_direta' para compra/venda direta (ex.: 'venda direta', "
        "'Tipo de Venda: vendadireta') ou quando não houver `data_leilao_1_praca` nem `data_leilao_2_praca`; "
        "'leilao' quando houver data da 1ª ou da 2ª praça. Se incerto, use null (o sistema infere a partir das datas). "
        "`url_foto_imovel`: se o markdown tiver imagens do imóvel (ex.: ![…](https://…)), "
        "prefira a URL da foto rotulada como primeira da galeria (ex.: alt com 'Imagem 1' / 'Foto 1'); "
        "senão a primeira imagem plausível do imóvel; null se não houver ou for só ícone/logo. "
        "REGRAS OBRIGATÓRIAS PARA DATAS E VALORES (sites como portais de leilão online): "
        "(1) Se o markdown trouxer apenas UMA data de encerramento/praça/leilão (ex.: 'Encerra em', "
        "'Data', '23/04/26', '23/04/2026') e não houver 1ª/2ª praça distintas, preencha `data_leilao` com essa data "
        "(converta DD/MM/AA para YYYY-MM-DD: use século 2000; se o ano for 2 dígitos e o contexto for leilão atual, "
        "ex.: 26 → 2026). "
        "(2) Se houver um único valor de lance mínimo, 'Em leilão pelo valor de R$', 'lance mínimo' ou valor "
        "destacado como referência do leilão em curso, preencha `valor_arrematacao` com esse número (float, sem R$). "
        "Normalize valores brasileiros: remova pontos de milhar e use ponto como separador decimal se houver centavos. "
        "(2b) `valor_avaliacao`: preencha SOMENTE se o markdown indicar valor de avaliação/perícia/venal do imóvel "
        "(ex.: 'valor de avaliação', 'avaliação do imóvel', 'laudo de avaliação', 'valor da avaliação', "
        "'avaliação judicial', 'venal', 'base de cálculo' quando for claramente o valor pericial e não o lance). "
        "Nunca copie o valor de avaliação para `valor_lance_*` nem para `valor_arrematacao` — são conceitos diferentes. "
        "Se não houver valor de avaliação explícito, use null. "
        "(3) Se existirem claramente 1ª e 2ª praças com datas/valores diferentes, use `data_leilao_1_praca`, "
        "`valor_lance_1_praca`, `data_leilao_2_praca`, `valor_lance_2_praca` e também `data_leilao`/`valor_arrematacao` "
        "com a praça mais próxima ou a de menor valor, conforme o texto indicar. "
        "(4) Não deixe `data_leilao`, `valor_arrematacao` e ambos os pares de praça todos nulos se o markdown "
        "exibir explicitamente data e valor do leilão em aberto — pelo menos um par deve refletir o que o usuário vê. "
        "url_leilao deve ser exatamente: "
        + json.dumps(url_canonica, ensure_ascii=False)
    )
    user = (
        "JSON deve obedecer este schema (nomes e tipos):\n"
        + json.dumps(schema, ensure_ascii=False)[:120_000]
        + "\n\n--- MARKDOWN DO EDITAL ---\n"
        + markdown[:100_000]
    )

    logger.info("OpenAI: extraindo edital modelo=%s markdown_chars=%s", mid, len(markdown))

    base_kw: dict[str, Any] = {
        "model": mid,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "response_format": {"type": "json_object"},
    }
    base_kw.update(_kwargs_limite_saida(mid))
    if not _deve_omitir_temperature(mid):
        base_kw["temperature"] = 0.1
    comp = None
    last_err: BaseException | None = None
    for tentativa in range(3):
        kw = dict(base_kw)
        if tentativa >= 1:
            kw.pop("temperature", None)
        if tentativa >= 2:
            kw.pop("response_format", None)
        try:
            comp = client.chat.completions.create(**kw)
            break
        except BadRequestError as e:
            last_err = e
            err = str(e).lower()
            body = getattr(e, "body", None)
            if isinstance(body, dict):
                err = err + " " + json.dumps(body, ensure_ascii=False).lower()
            temp_rejeitada = tentativa == 0 and (
                "temperature" in err
                or "unsupported_parameter" in err
                or "does not support" in err
            )
            if temp_rejeitada and "temperature" in kw:
                logger.warning("OpenAI: modelo rejeitou temperature; repetindo sem temperature.")
                continue
            if tentativa == 1 and ("response_format" in err or "json_object" in err):
                logger.warning("OpenAI: modelo rejeitou response_format; repetindo sem JSON mode.")
                continue
            raise
    if comp is None:
        raise last_err or RuntimeError("OpenAI: falha inesperada na extração")
    choice0 = comp.choices[0]
    msg = getattr(choice0, "message", None)
    raw = getattr(msg, "content", None) if msg is not None else None
    raw = raw if isinstance(raw, str) else ""
    blob = _extrair_json_objeto(raw)
    if not blob.strip():
        fr = getattr(choice0, "finish_reason", None)
        raise ValueError(f"Resposta do modelo vazia ou sem JSON (finish_reason={fr}).")

    data = json.loads(blob)
    ext = ExtracaoEditalLLM.model_validate(data)

    usage = getattr(comp, "usage", None)
    pt = int(getattr(usage, "prompt_tokens", 0) or 0) if usage else 0
    ct = int(getattr(usage, "completion_tokens", 0) or 0) if usage else 0
    custo = _estimar_custo_usd(pt, ct)
    metricas: dict[str, Any] = {
        "modelo": mid,
        "prompt_tokens": pt,
        "completion_tokens": ct,
        "custo_usd_estimado": round(custo, 8),
        "finish_reason": getattr(choice0, "finish_reason", None),
    }
    logger.info(
        "OpenAI: extração OK tokens in=%s out=%s custo_est_usd=%s",
        pt,
        ct,
        metricas["custo_usd_estimado"],
    )
    return ext, metricas
