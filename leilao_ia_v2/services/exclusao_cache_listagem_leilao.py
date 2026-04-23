"""
Exclui anúncios de mercado que, na prática, são listagens do **próprio leilão**
(preço muito próximo de lance de 1ª/2ª praça **e** sinais).

Lances: ``valor_lance_1_praca`` / ``valor_lance_2_praca``, **``valor_arrematacao``** (comum quando
1ª/2ª estão vazias), e o mesmo em ``leilao_extra_json`` quando existir.

Sinais: palavras-chave (leilão, judicial, “lance mínim”, etc.); bairro+área; bairro slug;
texto da ficha em que o **bairro do edital** aparece (título/cidade trocada);
ou mesma cidade + área alinhada + preço idêntico ao lance (bairro vazio em portais).

Quando a 2ª praça não existe, só 1ª praça entra na comparação.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from leilao_ia_v2.vivareal.slug import slug_vivareal


def _area_referencia_m2_leilao(leilao: dict[str, Any]) -> float:
    for k in ("area_util", "area_total"):
        v = leilao.get(k)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _float_pos(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _leilao_extra_como_dict(leilao: dict[str, Any]) -> dict[str, Any]:
    raw = leilao.get("leilao_extra_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return d if isinstance(d, dict) else {}
    return {}


def lances_praca_para_comparar(leilao: dict[str, Any]) -> list[tuple[str, float]]:
    """
    Valores a comparar com ``valor_venda`` do anúncio (lance 1ª/2ª praça e, se existir,
    **valor_arrematacao** — em muitos cadastros só este campo está preenchido).

    Ordem: colunas do imóvel, depois ``leilao_extra_json`` (mesmos nomes).
    """
    out: list[tuple[str, float]] = []
    seen: set[str] = set()

    def _add(label: str, f: float) -> None:
        key = f"{label}:{f:.2f}"
        if key in seen:
            return
        seen.add(key)
        out.append((label, f))

    for key, label in (("valor_lance_1_praca", "1_praca"), ("valor_lance_2_praca", "2_praca")):
        f = _float_pos(leilao.get(key))
        if f is not None:
            _add(label, f)
    extra = _leilao_extra_como_dict(leilao)
    for key, label in (("valor_lance_1_praca", "1_praca"), ("valor_lance_2_praca", "2_praca")):
        f = _float_pos(extra.get(key))
        if f is not None:
            _add(label, f)
    f_ar = _float_pos(leilao.get("valor_arrematacao"))
    if f_ar is not None:
        _add("arrematacao", f_ar)
    f_arx = _float_pos(extra.get("valor_arrematacao"))
    if f_arx is not None:
        _add("arrematacao", f_arx)
    return out


def _preco_proximo_ao_lance(venda: float, lance: float) -> bool:
    if venda <= 0 or lance <= 0:
        return False
    d = abs(venda - lance)
    # mín. R$ 2 + 0,03% do maior (evita ruído em desconto comercial, cobre lances “redondos”)
    tol = max(2.0, 0.0003 * max(venda, lance))
    return d <= tol


def _metadados_anuncio_como_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


def _sinal_texto_ou_url_leilao(titulo: Any, url: Any) -> bool:
    s = f"{titulo} {url}".lower()
    if not s.strip():
        return False
    chaves = (
        "leilão",
        "leilao",
        "judicial",
        "hasta pública",
        "hasta publica",
        "hasta ",
        "venda forçad",
        "venda forcad",
        "forçada",
        "forcada",
        "leiloeir",
        "arremat",
        "extração de",
        "extracao de",
        "lance mínim",
        "lance minim",
        "lance inicia",
    )
    return any(k in s for k in chaves)


def _bairros_equiv(str_a: str, str_l: str) -> bool:
    """Mesmo bairro (slug) ou um contém o outro (erros de cadastro, sufixo)."""
    sa = slug_vivareal(str_a)
    sl = slug_vivareal(str_l)
    if not sa or not sl:
        return False
    if sa == sl:
        return True
    return sa in sl or sl in sa


def _sinal_bairro_e_area_imovel(
    bairro_an: Any,
    bairro_lei: Any,
    area_m2_ads: float,
    area_ref: float,
) -> bool:
    if area_ref <= 0 or area_m2_ads <= 0:
        return False
    if not _bairros_equiv(str(bairro_an or ""), str(bairro_lei or "")):
        return False
    # Até 5%: edital vs anúncio raramente batem o m² ao décimo; 0,2% excluía bons sinais
    return abs(area_m2_ads - area_ref) / area_ref <= 0.05


def _sinal_bairro_igual_mesma_regiao(
    bairro_an: Any,
    bairro_lei: Any,
) -> bool:
    """Bairro alinhado ao edital; usado com preço ~lance (listagem do leiloeiro no portal)."""
    return _bairros_equiv(str(bairro_an or ""), str(bairro_lei or ""))


def _cidades_equiv(c_a: str, c_l: str) -> bool:
    sa = slug_vivareal(c_a)
    sl = slug_vivareal(c_l)
    if not sa or not sl:
        return False
    if sa == sl:
        return True
    return sa in sl or sl in sa


def _bairro_leilao_mencionado_em_ficha(anuncio: dict[str, Any], bairro_lei: str) -> bool:
    """
    O bairro do edital surge em título, logradouro, bairro ou cidade (campos trocados em portais).
    Usa slugs: "centro" em "sao-bernardo-...-centro" ou título "Apartamento no Centro".
    """
    b = (bairro_lei or "").strip()
    if not b or len(b) < 3:
        return False
    sl = slug_vivareal(b)
    if not sl or len(sl) < 3:
        return False
    blob = slug_vivareal(
        f"{anuncio.get('bairro') or ''} {anuncio.get('cidade') or ''} "
        f"{anuncio.get('logradouro') or ''} {anuncio.get('titulo') or ''}"
    )
    if not blob:
        return False
    return sl in blob


def _sinal_preco_lance_mais_contexto_amplo(
    anuncio: dict[str, Any],
    leilao: dict[str, Any],
    *,
    b_lei: str,
    area_ref: float,
    am: float,
    valor: float,
    lance: float,
) -> bool:
    """
    Padrão observado: preço = lance mas bairro vazio, cidade no sítio errado, ou "Centro" só no título.
    Requer (preço quase = lance) ou (área ≈ edital) para não limpar o mercado inteiro.
    """
    if not _preco_proximo_ao_lance(valor, lance):
        return False
    pq = _preco_quase_identico_ao_lance(valor, lance)
    ar_ok = area_ref > 0 and am > 0 and abs(am - area_ref) / area_ref <= 0.05
    if not ar_ok and not pq:
        return False
    c_match = _cidades_equiv(
        str(anuncio.get("cidade") or ""),
        str(leilao.get("cidade") or ""),
    )
    b_txt = _bairro_leilao_mencionado_em_ficha(anuncio, b_lei) if b_lei else False
    if b_txt and b_lei and (ar_ok or pq):
        return True
    if c_match and ar_ok and pq:
        return True
    return False


def _preco_quase_identico_ao_lance(venda: float, lance: float) -> bool:
    """Mais restritivo que a tolerância geral: mesmo “valor de anúncio = valor de lance”."""
    if venda <= 0 or lance <= 0:
        return False
    d = abs(venda - lance)
    return d <= max(1.0, 0.00001 * max(venda, lance))


def anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(
    anuncio: dict[str, Any],
    leilao: dict[str, Any],
) -> bool:
    """
    True = não deve entrar no cache (nem em agregados de m²) por ser listagem do próprio lance.
    Respeita ``metadados_json.incluir_em_cache: false`` gravado na persistência.
    """
    meta = _metadados_anuncio_como_dict(anuncio.get("metadados_json"))
    if meta.get("incluir_em_cache") is False:
        return True
    lances = lances_praca_para_comparar(leilao)
    if not lances:
        return False
    try:
        valor = float(anuncio.get("valor_venda") or 0)
    except (TypeError, ValueError):
        return False
    if valor <= 0:
        return False
    try:
        am = float(anuncio.get("area_construida_m2") or 0)
    except (TypeError, ValueError):
        am = 0.0

    b_lei = str(leilao.get("bairro") or "")
    area_ref = _area_referencia_m2_leilao(leilao)

    for _label, lance in lances:
        if not _preco_proximo_ao_lance(valor, lance):
            continue
        if _sinal_texto_ou_url_leilao(anuncio.get("titulo"), anuncio.get("url_anuncio")):
            return True
        if _sinal_bairro_e_area_imovel(anuncio.get("bairro"), b_lei, am, area_ref):
            return True
        # Preço = lance (quase exato) + mesmo bairro: típico de ficha/ listagem do próprio leilão
        if _preco_quase_identico_ao_lance(valor, lance) and _sinal_bairro_igual_mesma_regiao(
            anuncio.get("bairro"), b_lei
        ):
            return True
        if _sinal_preco_lance_mais_contexto_amplo(
            anuncio,
            leilao,
            b_lei=b_lei,
            area_ref=area_ref,
            am=am,
            valor=valor,
            lance=lance,
        ):
            return True
    return False


def filtrar_anuncios_mantendo_apenas_mercado_comparavel(
    anuncios: list[dict[str, Any]],
    leilao: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """
    Remove da lista (para montagem de cache) os anúncios identificados como listagem de lance.
    Com ``leilao is None`` não altera a lista.
    """
    if not leilao or not anuncios:
        return anuncios
    return [a for a in anuncios if not anuncio_deve_ser_excluido_de_cache_por_listagem_sinc_lance(a, leilao)]
