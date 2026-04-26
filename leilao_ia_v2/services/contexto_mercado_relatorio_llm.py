"""
Uma chamada OpenAI (JSON) para preencher ``relatorio_mercado_contexto_json`` do imóvel.

Não usa busca web: inferências aproximadas + dados numéricos fornecidos no prompt (cache).
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

from openai import BadRequestError, OpenAI

from leilao_ia_v2.schemas.relatorio_mercado_contexto import (
    RelatorioMercadoContextoDocumento,
    normalizar_documento_mercado,
)
from leilao_ia_v2.services.extracao_edital_llm import (
    _deve_omitir_temperature,
    _estimar_custo_usd,
    _kwargs_limite_saida,
)

logger = logging.getLogger(__name__)


def _resolver_modelo_relatorio_mercado(modelo: Optional[str] = None) -> str:
    """
    Resolve o modelo com prioridade:
    1) parâmetro explícito da chamada
    2) OPENAI_MODEL_RELATORIO_MERCADO (override dedicado)
    3) OPENAI_CHAT_MODEL (padrão global existente)
    4) fallback local
    """
    m_arg = str(modelo or "").strip()
    if m_arg:
        return m_arg
    m_rel = str(os.getenv("OPENAI_MODEL_RELATORIO_MERCADO", "") or "").strip()
    if m_rel:
        return m_rel
    m_chat = str(os.getenv("OPENAI_CHAT_MODEL", "") or "").strip()
    if m_chat:
        return m_chat
    return "gpt-4o-mini"


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


def _fmt_brl(v: Any) -> str:
    try:
        x = float(v or 0)
    except (TypeError, ValueError):
        return "—"
    if x <= 0:
        return "—"
    inteiro = int(abs(round(x * 100))) // 100
    cent = int(abs(round(x * 100))) % 100
    neg = "-" if x < 0 else ""
    corpo = f"{inteiro:,}".replace(",", ".")
    return f"{neg}R$ {corpo},{cent:02d}"


def _fmt_pm2_br(v: Any) -> str:
    try:
        x = float(v or 0)
    except (TypeError, ValueError):
        return "—"
    if x <= 0:
        return "—"
    s = f"{x:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".") + " R$/m²"


def montar_texto_entrada_contexto(
    *,
    row: dict[str, Any],
    cache_principal: dict[str, Any] | None,
    n_anuncios_resolvidos: int,
) -> str:
    """Resumo factual + pedido de análise (prompt do usuário)."""
    linhas: list[str] = []
    linhas.append("## Dados do leilão (imóvel)")
    linhas.append(f"- Cidade: {row.get('cidade') or '—'}")
    linhas.append(f"- Estado: {row.get('estado') or '—'}")
    linhas.append(f"- Bairro: {row.get('bairro') or '—'}")
    linhas.append(f"- Endereço: {row.get('endereco') or '—'}")
    linhas.append(f"- Tipo de imóvel: {row.get('tipo_imovel') or '—'}")
    linhas.append(f"- Conservação: {row.get('conservacao') or '—'}")
    au = row.get("area_util")
    at = row.get("area_total")
    if au:
        linhas.append(f"- Área útil (m²): {au}")
    if at:
        linhas.append(f"- Área total / terreno (m²): {at}")
    for label, key in (
        ("Lance 1ª praça", "valor_lance_1_praca"),
        ("Lance 2ª praça", "valor_lance_2_praca"),
        ("Valor arrematação / referência", "valor_arrematacao"),
        ("Valor avaliação", "valor_avaliacao"),
    ):
        v = row.get(key)
        if v is not None and str(v).strip() != "":
            try:
                if float(v) > 0:
                    linhas.append(f"- {label}: {_fmt_brl(v)}")
            except (TypeError, ValueError):
                pass

    linhas.append("")
    linhas.append("## Amostra de mercado (cache principal de comparáveis, se houver)")
    if not cache_principal:
        linhas.append("- Sem cache principal vinculado nesta execução.")
    else:
        try:
            n = int(cache_principal.get("n_amostras") or 0)
        except (TypeError, ValueError):
            n = 0
        linhas.append(f"- Número de anúncios/amostras no cache: {n}")
        linhas.append(f"- Anúncios resolvidos no banco (geocodificados etc.): {n_anuncios_resolvidos}")
        for label, key in (
            ("Menor valor de venda", "menor_valor_venda"),
            ("Valor médio de venda", "valor_medio_venda"),
            ("Maior valor de venda", "maior_valor_venda"),
            ("Preço médio R$/m²", "preco_m2_medio"),
        ):
            v = cache_principal.get(key)
            if v is not None and str(v).strip() != "":
                try:
                    if float(v) > 0:
                        if "preco_m2" in key:
                            linhas.append(f"- {label}: {_fmt_pm2_br(v)}")
                        else:
                            linhas.append(f"- {label}: {_fmt_brl(v)}")
                except (TypeError, ValueError):
                    pass

    linhas.append("")
    linhas.append(
        "### Instruções\n"
        "Responda em **português do Brasil**, direto ao ponto. Cada card deve ter **2 a 6 tópicos** "
        "(frases curtas, iniciadas por indicador lógico ou traço).\n"
        "Não invente estatísticas oficiais precisas; use linguagem prudente (\"tende a\", \"costuma\", "
        "\"pode haver\"). População: ordem de grandeza ou faixa quando não houver dado confiável no contexto.\n"
        "Relacione o **tipo e faixa de preço do imóvel** com o perfil provável do bairro (risco de liquidez: "
        "imóvel caro em área mais modesta, ou imóvel modesto em área de alto padrão).\n"
        "Se houver **muitas amostras** no cache, comente se isso sugere concorrência na venda ou, dependendo do segmento, "
        "bom sinal de liquidez.\n"
        "Mencione **bairros concorrentes** típicos na mesma cidade/região quando fizer sentido.\n"
        "Sobre **condomínios fechados de casas**: indique se é comum na região ou no bairro, quando aplicável.\n"
        "Sempre que possível, explicite a base de inferência em linguagem curta (ex.: amostra, faixa de preço, cobertura do bairro).\n"
    )
    return "\n".join(linhas)


def _schema_instrucao_json() -> str:
    return json.dumps(
        {
            "disclaimer": "Texto curto: análise aproximada; confirmar dados críticos com fontes locais.",
            "insights_oportunidade": ["2 a 5 bullets com oportunidades específicas e acionáveis."],
            "insights_risco": ["2 a 5 bullets com riscos de liquidez, precificação e execução."],
            "checklist_diligencia": [
                "4 a 7 checagens objetivas antes do lance (zoneamento, reforma, saída, documentos)."
            ],
            "dados_populacao_cidade": ["1 a 3 bullets com faixa populacional/porte da cidade, linguagem prudente."],
            "informacoes_bairro": ["2 a 5 bullets com características práticas do bairro para decisão."],
            "estrategia_sugerida": "Frase curta com estratégia de saída (ex.: revenda rápida, reforma+revenda, renda locatícia).",
            "tese_acao": "Parágrafo curto com recomendação prática de decisão e condição de entrada.",
            "cards": [],
        },
        ensure_ascii=False,
        indent=2,
    )


def gerar_contexto_mercado_relatorio_llm(
    texto_entrada: str,
    *,
    modelo: Optional[str] = None,
) -> tuple[RelatorioMercadoContextoDocumento, dict[str, Any]]:
    """
    Uma chamada ``chat.completions`` com ``response_format: json_object``.

    Devolve (documento normalizado, métricas).
    """
    mid = _resolver_modelo_relatorio_mercado(modelo)
    system = (
        "Você é analista imobiliário no Brasil. Produza um JSON com os campos: "
        "`cards` (array), `disclaimer` (string), `insights_oportunidade` (array), "
        "`insights_risco` (array), `checklist_diligencia` (array), "
        "`dados_populacao_cidade` (array), `informacoes_bairro` (array), "
        "`estrategia_sugerida` (string) e `tese_acao` (string).\n"
        "Foco principal: oportunidades, riscos, diligência e ação recomendada.\n"
        "`cards` é opcional e pode vir vazio quando não agregar decisão.\n"
        "Não use markdown no JSON. Mantenha os arrays de insights objetivos e acionáveis.\n"
        "Evite suposições genéricas (ex.: 'pode', 'costuma', 'tende') nos campos "
        "`dados_populacao_cidade` e `informacoes_bairro`; prefira frases assertivas e factuais.\n"
        "Exemplo de forma (substitua conteúdo pelos seus tópicos reais):\n"
        + _schema_instrucao_json()
    )
    user = "--- CONTEXTO FACTUAL ---\n" + (texto_entrada or "")[:60_000]

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
        base_kw["temperature"] = 0.25

    client = OpenAI()
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
            if tentativa == 0 and "temperature" in kw and (
                "temperature" in err or "unsupported_parameter" in err
            ):
                continue
            if tentativa == 1 and ("response_format" in err or "json_object" in err):
                logger.warning("OpenAI contexto relatório: repetindo sem JSON mode.")
                continue
            raise
    if comp is None:
        raise last_err or RuntimeError("OpenAI: falha no contexto de mercado")

    choice0 = comp.choices[0]
    msg = getattr(choice0, "message", None)
    raw = getattr(msg, "content", None) if msg is not None else None
    raw = raw if isinstance(raw, str) else ""
    blob = _extrair_json_objeto(raw)
    if not blob.strip():
        fr = getattr(choice0, "finish_reason", None)
        raise ValueError(f"Resposta vazia (finish_reason={fr}).")

    data = json.loads(blob)
    agora = datetime.now(timezone.utc).isoformat()
    usage = getattr(comp, "usage", None)
    pt = int(getattr(usage, "prompt_tokens", 0) or 0) if usage else 0
    ct = int(getattr(usage, "completion_tokens", 0) or 0) if usage else 0
    custo = round(_estimar_custo_usd(pt, ct), 8)

    doc_raw = {
        "versao": 1,
        "gerado_em_iso": agora,
        "modelo": mid,
        "prompt_tokens": pt,
        "completion_tokens": ct,
        "custo_usd_estimado": custo,
        "cards": data.get("cards") or [],
        "disclaimer": str(data.get("disclaimer") or "").strip(),
        "insights_oportunidade": data.get("insights_oportunidade") or [],
        "insights_risco": data.get("insights_risco") or [],
        "checklist_diligencia": data.get("checklist_diligencia") or [],
        "dados_populacao_cidade": data.get("dados_populacao_cidade") or [],
        "informacoes_bairro": data.get("informacoes_bairro") or [],
        "estrategia_sugerida": str(data.get("estrategia_sugerida") or "").strip(),
        "tese_acao": str(data.get("tese_acao") or "").strip(),
    }
    doc = normalizar_documento_mercado(doc_raw)
    doc = doc.model_copy(
        update={
            "gerado_em_iso": agora,
            "modelo": mid,
            "prompt_tokens": pt,
            "completion_tokens": ct,
            "custo_usd_estimado": custo,
        }
    )
    metricas: dict[str, Any] = {
        "modelo": mid,
        "prompt_tokens": pt,
        "completion_tokens": ct,
        "custo_usd_estimado": custo,
        "finish_reason": getattr(choice0, "finish_reason", None),
    }
    logger.info(
        "OpenAI contexto mercado relatório OK modelo=%s tokens in=%s out=%s custo_est=%s",
        mid,
        pt,
        ct,
        custo,
    )
    return doc, metricas
