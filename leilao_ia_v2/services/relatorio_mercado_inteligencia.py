from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from leilao_ia_v2.schemas.operacao_simulacao import parse_operacao_simulacao_json
from leilao_ia_v2.schemas.relatorio_mercado_contexto import RelatorioMercadoCard

_POPULACAO_CIDADE_APROX: dict[str, str] = {
    "sao jose do rio preto": "SAO JOSE DO RIO PRETO: faixa aproximada de 500 a 600 mil habitantes (estimativa de mercado).",
    "são josé do rio preto": "SAO JOSE DO RIO PRETO: faixa aproximada de 500 a 600 mil habitantes (estimativa de mercado).",
    "campinas": "CAMPINAS: faixa aproximada de 1,0 a 1,3 milhão de habitantes (estimativa de mercado).",
    "ribeirao preto": "RIBEIRAO PRETO: faixa aproximada de 650 a 800 mil habitantes (estimativa de mercado).",
    "ribeirão preto": "RIBEIRAO PRETO: faixa aproximada de 650 a 800 mil habitantes (estimativa de mercado).",
    "curitiba": "CURITIBA: faixa aproximada de 1,7 a 2,0 milhões de habitantes (estimativa de mercado).",
    "sao paulo": "SAO PAULO: faixa acima de 10 milhões de habitantes (estimativa de mercado).",
    "são paulo": "SAO PAULO: faixa acima de 10 milhões de habitantes (estimativa de mercado).",
}


def _norm_txt(v: Any) -> str:
    return str(v or "").strip().lower()


def _pct(n: float, d: float) -> float:
    if d <= 0:
        return 0.0
    return max(0.0, min(100.0, (float(n) / float(d)) * 100.0))


def extrair_sinais_objetivos_por_cards(cards: list[RelatorioMercadoCard]) -> dict[str, Any]:
    """
    Converte conteúdo textual dos cards em sinais objetivos (0-100).
    """
    txt = " ".join(
        _norm_txt(t)
        for c in cards
        for t in ([c.titulo] + list(c.topicos or []) + [c.evidencia or ""])
    )
    liq = 50.0
    press = 50.0
    fit = 50.0

    positivos_liq = ("liquidez", "boa procura", "alta procura", "giro", "absorção", "demanda")
    negativos_liq = ("baixa liquidez", "encalhe", "demora", "tempo de venda alto", "pouca procura")
    positivos_fit = ("aderente", "compatível", "fit", "coerente", "ajustado ao bairro")
    negativos_fit = ("desalinhado", "incompatível", "fora do padrão", "sobrepreço", "pouco aderente")
    positivos_pressao = ("muita oferta", "alto volume", "concorrência elevada", "pressão", "disputa por preço")
    negativos_pressao = ("baixa concorrência", "oferta restrita", "pouca oferta", "estoque baixo")

    for k in positivos_liq:
        if k in txt:
            liq += 6
    for k in negativos_liq:
        if k in txt:
            liq -= 8
    for k in positivos_fit:
        if k in txt:
            fit += 6
    for k in negativos_fit:
        if k in txt:
            fit -= 8
    for k in positivos_pressao:
        if k in txt:
            press += 7
    for k in negativos_pressao:
        if k in txt:
            press -= 7

    liq = max(0.0, min(100.0, liq))
    press = max(0.0, min(100.0, press))
    fit = max(0.0, min(100.0, fit))
    resumo = (
        f"Liquidez {liq:.0f}/100 · Pressão concorrencial {press:.0f}/100 · "
        f"Fit imóvel-bairro {fit:.0f}/100"
    )
    return {
        "liquidez_bairro": int(round(liq)),
        "pressao_concorrencia": int(round(press)),
        "fit_imovel_bairro": int(round(fit)),
        "resumo": resumo,
    }


def extrair_sinais_objetivos_decisao(
    *,
    insights_oportunidade: list[str],
    insights_risco: list[str],
    estrategia_sugerida: str,
    tese_acao: str,
) -> dict[str, Any]:
    txt = " ".join(
        [
            *[str(x or "").strip().lower() for x in (insights_oportunidade or [])],
            *[str(x or "").strip().lower() for x in (insights_risco or [])],
            str(estrategia_sugerida or "").strip().lower(),
            str(tese_acao or "").strip().lower(),
        ]
    )
    liq = 50.0
    press = 50.0
    fit = 50.0
    if any(k in txt for k in ("liquidez", "giro", "demanda ativa", "boa procura", "saída rápida")):
        liq += 12
    if any(k in txt for k in ("baixa liquidez", "saída lenta", "demora", "encalhe")):
        liq -= 15
    if any(k in txt for k in ("aderência", "compatível", "fit", "coerente")):
        fit += 10
    if any(k in txt for k in ("incompatível", "desalinhado", "fora do padrão")):
        fit -= 12
    if any(k in txt for k in ("concorrência elevada", "muita oferta", "disputa por preço")):
        press += 12
    if any(k in txt for k in ("oferta restrita", "baixa concorrência", "pouca oferta")):
        press -= 10
    liq = max(0.0, min(100.0, liq))
    press = max(0.0, min(100.0, press))
    fit = max(0.0, min(100.0, fit))
    return {
        "liquidez_bairro": int(round(liq)),
        "pressao_concorrencia": int(round(press)),
        "fit_imovel_bairro": int(round(fit)),
        "resumo": (
            f"Liquidez {liq:.0f}/100 · Pressão concorrencial {press:.0f}/100 · "
            f"Fit imóvel-bairro {fit:.0f}/100"
        ),
    }


def assinatura_cache_principal(cache_principal: dict[str, Any] | None) -> str:
    if not cache_principal:
        return ""
    base = "|".join(
        [
            str(cache_principal.get("id") or ""),
            str(cache_principal.get("n_amostras") or ""),
            str(cache_principal.get("anuncios_ids") or ""),
            str(cache_principal.get("valor_medio_venda") or ""),
            str(cache_principal.get("preco_m2_medio") or ""),
        ]
    )
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def calcular_qualidade_relatorio(
    *,
    cache_principal: dict[str, Any] | None,
    ads_por_id: dict[str, dict[str, Any]],
    bairro_alvo: str,
) -> dict[str, Any]:
    if not cache_principal:
        return {
            "score_qualidade": 15,
            "n_amostras_cache": 0,
            "n_anuncios_resolvidos": 0,
            "pct_mesmo_bairro": 0.0,
            "pct_geo_valida": 0.0,
            "notas": ["Sem cache principal vinculado no momento da análise."],
        }

    ids = [p.strip() for p in str(cache_principal.get("anuncios_ids") or "").split(",") if p.strip()]
    n_amostras = int(cache_principal.get("n_amostras") or 0)
    n_res = 0
    n_geo = 0
    n_mesmo = 0
    bairro_ref = _norm_txt(bairro_alvo)
    for aid in ids:
        a = ads_por_id.get(aid)
        if not isinstance(a, dict):
            continue
        n_res += 1
        try:
            lat = float(a.get("latitude"))
            lon = float(a.get("longitude"))
            if lat == lat and lon == lon:
                n_geo += 1
        except Exception:
            pass
        if bairro_ref and _norm_txt(a.get("bairro")) == bairro_ref:
            n_mesmo += 1
    pct_geo = _pct(n_geo, max(1, n_res))
    pct_mesmo = _pct(n_mesmo, max(1, n_res))

    score = 35.0
    score += min(float(n_amostras), 40.0) * 0.8
    score += pct_geo * 0.20
    score += pct_mesmo * 0.20
    if n_amostras < 8:
        score -= 12
    if pct_geo < 70:
        score -= 8
    if pct_mesmo < 40:
        score -= 10
    score = max(0.0, min(100.0, score))

    notas: list[str] = []
    if n_amostras < 12:
        notas.append("Amostra enxuta de comparáveis.")
    if pct_mesmo < 50:
        notas.append("Cobertura de mesmo bairro abaixo do ideal.")
    if pct_geo < 80:
        notas.append("Cobertura geográfica parcial nos comparáveis.")
    if not notas:
        notas.append("Base de comparáveis consistente para decisão.")

    return {
        "score_qualidade": int(round(score)),
        "n_amostras_cache": int(n_amostras),
        "n_anuncios_resolvidos": int(n_res),
        "pct_mesmo_bairro": round(float(pct_mesmo), 2),
        "pct_geo_valida": round(float(pct_geo), 2),
        "notas": notas,
    }


def avaliar_validade_relatorio(
    *,
    gerado_em_iso: str,
    ttl_horas: int,
    cache_principal_id: str,
    assinatura_cache: str,
    cache_principal_atual: dict[str, Any] | None,
) -> dict[str, Any]:
    agora = datetime.now(timezone.utc)
    horas_desde = 0.0
    try:
        dt = datetime.fromisoformat(str(gerado_em_iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        horas_desde = max(0.0, (agora - dt.astimezone(timezone.utc)).total_seconds() / 3600.0)
    except Exception:
        horas_desde = float(ttl_horas + 1)
    motivo = ""
    expirado = horas_desde > float(ttl_horas)
    if expirado:
        motivo = f"TTL excedido ({horas_desde:.1f}h > {ttl_horas}h)."
    if cache_principal_atual:
        id_atual = str(cache_principal_atual.get("id") or "")
        ass_atual = assinatura_cache_principal(cache_principal_atual)
        if cache_principal_id and id_atual and cache_principal_id != id_atual:
            expirado = True
            motivo = "Cache principal mudou desde a geração."
        elif assinatura_cache and ass_atual and assinatura_cache != ass_atual:
            expirado = True
            motivo = "Amostra do cache principal mudou desde a geração."
    return {
        "ttl_horas": int(ttl_horas),
        "expirado": bool(expirado),
        "horas_desde_geracao": round(horas_desde, 2),
        "motivo": motivo,
    }


def evidencias_por_card(
    *,
    qualidade: dict[str, Any],
    bairro_alvo: str,
) -> dict[str, str]:
    n = int(qualidade.get("n_amostras_cache") or 0)
    pmb = float(qualidade.get("pct_mesmo_bairro") or 0.0)
    pgeo = float(qualidade.get("pct_geo_valida") or 0.0)
    b = str(bairro_alvo or "").strip() or "bairro do imóvel"
    base = f"Base: {n} amostras; {pmb:.0f}% mesmo bairro ({b}); {pgeo:.0f}% geo válida."
    return {
        "populacao": base,
        "perfil_urbano": base,
        "centralidade": base,
        "classe_renda": base,
        "seguranca": base,
        "procura_imoveis": base,
        "bairros_concorrentes": base,
        "condominios_fechados": base,
        "volume_anuncios": base,
        "ajuste_imovel_bairro": base,
    }


def gerar_insights_decisao(
    *,
    row: dict[str, Any],
    qualidade: dict[str, Any],
    sinais: dict[str, Any],
) -> dict[str, Any]:
    """
    Constrói insights acionáveis sem custo adicional de API.

    Usa somente sinais, qualidade da base e contexto do imóvel para orientar
    a decisão (oportunidade x risco x próximos passos).
    """
    liq = int(sinais.get("liquidez_bairro") or 50)
    press = int(sinais.get("pressao_concorrencia") or 50)
    fit = int(sinais.get("fit_imovel_bairro") or 50)
    n = int(qualidade.get("n_amostras_cache") or 0)
    pmb = float(qualidade.get("pct_mesmo_bairro") or 0.0)
    pgeo = float(qualidade.get("pct_geo_valida") or 0.0)
    score = int(qualidade.get("score_qualidade") or 0)
    bairro = str(row.get("bairro") or "").strip() or "bairro do imóvel"
    cidade = str(row.get("cidade") or "").strip() or "cidade"
    tipo = str(row.get("tipo_imovel") or "").strip() or "imóvel"

    oportunidades: list[str] = []
    riscos: list[str] = []

    if liq >= 60:
        oportunidades.append(
            f"Liquidez local favorável para {tipo} em {bairro}, reduzindo risco de saída lenta."
        )
    if fit >= 60:
        oportunidades.append(
            "Boa aderência imóvel-bairro; chance maior de aceitação pelo público típico da região."
        )
    if n >= 12 and pmb >= 55:
        oportunidades.append(
            f"Base comparável consistente ({n} amostras; {pmb:.0f}% no mesmo bairro), útil para precificação mais assertiva."
        )
    if 50 <= press <= 72:
        oportunidades.append(
            "Concorrência ativa sem saturação extrema, cenário propício para saída com preço competitivo."
        )

    if score < 55:
        riscos.append(
            "Qualidade da base comparável abaixo do ideal; decisão deve considerar margem de segurança maior."
        )
    if pmb < 45:
        riscos.append(
            "Cobertura baixa do mesmo bairro; risco de preço distorcido por micro-localizações diferentes."
        )
    if pgeo < 75:
        riscos.append(
            "Cobertura geográfica incompleta em parte dos comparáveis; proximidade real pode estar subestimada."
        )
    if fit < 45:
        riscos.append(
            "Aderência fraca imóvel-bairro; potencial de liquidez menor que o padrão local."
        )
    if liq < 50:
        riscos.append(
            "Sinais de liquidez moderada/baixa; saída pode exigir desconto maior para acelerar venda."
        )
    if press > 75:
        riscos.append(
            "Concorrência elevada no segmento; disputa por preço tende a comprimir margem de revenda."
        )

    if not oportunidades:
        oportunidades.append(
            f"{cidade} mantém demanda ativa em polos consolidados; oportunidade existe se a entrada vier com desconto disciplinado."
        )
    if not riscos:
        riscos.append("Principais riscos operacionais parecem controlados, mas exigem validação documental e física do ativo.")

    checklist = [
        "Validar zoneamento e uso permitido (residencial/comercial/misto) antes do lance final.",
        "Estimular vistoria técnica focada em elétrica, hidráulica e passivos estruturais de reforma.",
        "Comparar ticket de saída com 3 a 5 anúncios realmente similares (tipo + raio + padrão).",
        "Simular plano A (revenda) e plano B (locação) com margem líquida mínima alvo.",
        "Confirmar documentação e eventuais pendências jurídicas que possam afetar prazo/custo.",
    ]

    if liq >= 65 and fit >= 60 and press <= 70:
        estrategia = "Revenda rápida com preço de entrada disciplinado."
        tese = (
            "Cenário favorece giro de capital se a arrematação ficar abaixo da faixa-alvo local. "
            "Priorizar execução enxuta e posicionamento competitivo na saída."
        )
    elif fit >= 55 and press > 70:
        estrategia = "Revenda com diferenciação e margem protegida."
        tese = (
            "Existe mercado, porém competitivo. Entrar apenas com desconto que cubra atrito comercial, "
            "tempo de exposição e eventual ajuste de preço na saída."
        )
    elif liq >= 50:
        estrategia = "Estratégia híbrida: revenda com plano de locação como backup."
        tese = (
            "O ativo pode performar, mas a previsibilidade de saída não é plena. "
            "A decisão melhora com plano B de renda para reduzir pressão de venda."
        )
    else:
        estrategia = "Aquisição oportunística apenas com desconto robusto."
        tese = (
            "Com sinais de liquidez mais fracos, a operação só tende a compensar com entrada muito descontada "
            "e horizonte de saída mais paciente."
        )

    # Conecta recomendação à simulação gravada (quando existir outputs no JSON).
    doc_sim = parse_operacao_simulacao_json(row.get("operacao_simulacao_json"))
    out_sim = doc_sim.outputs
    if out_sim is not None:
        roi_meta = out_sim.roi_desejado_pct_informado
        modo_meta = str(out_sim.roi_desejado_modo_informado or "bruto").strip().lower()
        roi_base = out_sim.roi_liquido if "liqu" in modo_meta else out_sim.roi_bruto
        lucro_liq = float(out_sim.lucro_liquido or 0.0)
        if roi_meta is not None and roi_base is not None:
            alvo = float(roi_meta) / 100.0
            obt = float(roi_base)
            razao = (obt / alvo) if alvo > 0 else 0.0
            if razao < 0.80:
                riscos.insert(
                    0,
                    f"ROI {'líquido' if 'liqu' in modo_meta else 'bruto'} da simulação fica abaixo de 80% da meta ({obt*100:.1f}% vs alvo {alvo*100:.1f}%).",
                )
                estrategia = "Descarte recomendado (ou rever teto de lance de forma relevante)."
                tese = (
                    "A simulação gravada não atinge o retorno mínimo desejado. "
                    "A operação só deveria seguir com redução material do preço de entrada ou mudança de estratégia."
                )
            elif razao < 1.00:
                riscos.insert(
                    0,
                    f"ROI da simulação ainda abaixo da meta ({obt*100:.1f}% vs alvo {alvo*100:.1f}%).",
                )
                estrategia = "Atenção máxima: só seguir com ajuste de entrada e diligência reforçada."
                tese = (
                    "Retorno projetado abaixo do alvo. Recomendação é cautela forte: "
                    "revisar preço, custos e premissas de saída antes de arrematar."
                )
            elif razao <= 1.30:
                oportunidades.insert(
                    0,
                    f"Simulação gravada atinge a meta de ROI ({obt*100:.1f}% para alvo {alvo*100:.1f}%).",
                )
                estrategia = "Arrematação viável com cautelas operacionais."
                tese = (
                    "Retorno está em faixa aceitável frente à meta definida. "
                    "A recomendação é seguir com disciplina de execução e controle de riscos."
                )
            else:
                oportunidades.insert(
                    0,
                    f"Simulação supera com folga a meta de ROI ({obt*100:.1f}% para alvo {alvo*100:.1f}%).",
                )
                estrategia = "Arrematação recomendada; pode tolerar risco moderado adicional com controle."
                tese = (
                    "Com ROI projetado muito acima da meta, há espaço para assumir risco moderado adicional "
                    "desde que riscos jurídicos e de saída permaneçam monitorados."
                )
        if lucro_liq > 0:
            oportunidades.append(f"Lucro líquido projetado positivo na simulação: R$ {lucro_liq:,.2f}.".replace(",", "X").replace(".", ",").replace("X", "."))
        elif out_sim is not None:
            riscos.append("Lucro líquido projetado não positivo na simulação gravada.")

    return {
        "insights_oportunidade": oportunidades[:5],
        "insights_risco": riscos[:5],
        "checklist_diligencia": checklist,
        "estrategia_sugerida": estrategia,
        "tese_acao": tese,
    }


def montar_contexto_minimo_decisao(
    *,
    row: dict[str, Any],
    qualidade: dict[str, Any],
) -> list[str]:
    # Card removido do front para simplificar leitura; mantido apenas por compatibilidade.
    return []


def montar_contexto_populacao_bairro(
    *,
    row: dict[str, Any],
    qualidade: dict[str, Any],
) -> dict[str, list[str]]:
    cidade = str(row.get("cidade") or "").strip()
    chave = cidade.lower()
    faixa_pop = _POPULACAO_CIDADE_APROX.get(chave)
    pop_linhas: list[str] = []
    if faixa_pop:
        pop_linhas.append(faixa_pop)
    elif cidade:
        pop_linhas.append(f"{cidade.upper()}: sem estimativa de faixa populacional cadastrada no sistema.")
    # Informações de bairro ficam a cargo do LLM (sem fallback numérico/genérico).
    return {"dados_populacao_cidade": pop_linhas[:3], "informacoes_bairro": []}

