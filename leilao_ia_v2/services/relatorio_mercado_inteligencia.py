from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from leilao_ia_v2.schemas.relatorio_mercado_contexto import RelatorioMercadoCard


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

