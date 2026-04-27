"""
Construção determinística de **uma única** frase de busca para Firecrawl Search.

O pacote antigo `leilao_ia_v2/fc_search/query_builder.py` gera até 4 frases por
leilão (Q0 empreendimento, Q1 rua, Q2 cidade, Q3 bairro), o que multiplica o
custo por 4 e amplia o risco de trazer cidades vizinhas em queries muito largas
(causa-raiz #2 do incidente Pindamonhangaba → São Bernardo).

Esta versão prioriza precisão geográfica:

- A frase **sempre** termina com ``"<cidade> <UF>"`` (e o UF por extenso quando
  a cidade é homónima de muitas — desambiguação cara, mas barata em texto).
- Inclui sempre **tipo de imóvel** (apartamento/casa/terreno/sobrado/...) quando
  conhecido — palavra normalmente presente no título dos portais.
- Inclui **bairro** quando conhecido (sinal de proximidade muito mais forte que
  rua, e ortograficamente mais estável).
- Inclui **área aproximada** apenas se vier no edital E for plausível (15 a 1000
  m²) — usado como filtro de "tamanho similar" pelos motores de busca.
- **Nunca** inclui o nome do empreendimento sozinho: o nome de condomínio
  funciona como query de baixa qualidade e foi a fonte do bug Pindamonhangaba
  (o "Empreendimento Vila Maria" matchou um anúncio em SP capital).

A função é pura (sem efeitos colaterais) e totalmente testável.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Optional


_TIPOS_CANONICOS: dict[str, str] = {
    "apartamento": "apartamento",
    "apto": "apartamento",
    "ap": "apartamento",
    "casa": "casa",
    "sobrado": "sobrado",
    "terreno": "terreno",
    "lote": "terreno",
    "gleba": "terreno",
    "sala": "sala comercial",
    "loja": "loja",
    "galpao": "galpão",
    "galpão": "galpão",
    "comercial": "imóvel comercial",
    "industrial": "imóvel industrial",
    "rural": "imóvel rural",
}


def _strip_acentos(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def _normalizar_tipo(tipo_imovel: str) -> str:
    """Mapeia variantes de tipo para um termo canónico em português.

    Retorna string vazia quando o tipo é desconhecido ou irrelevante (não
    arrisca colocar "desconhecido" na busca).

    >>> _normalizar_tipo("Apartamento Padrão")
    'apartamento'
    >>> _normalizar_tipo("LOTE")
    'terreno'
    >>> _normalizar_tipo("xpto")
    ''
    """
    base = _strip_acentos((tipo_imovel or "").strip().lower())
    base = re.sub(r"[^a-z ]+", " ", base).strip()
    if not base:
        return ""
    for token in base.split():
        if token in _TIPOS_CANONICOS:
            return _TIPOS_CANONICOS[token]
    return ""


def _normalizar_uf(uf: str) -> str:
    s = (uf or "").strip().upper()
    return s[:2] if len(s) >= 2 else ""


def _area_plausivel(area_m2: Optional[float]) -> Optional[int]:
    """Devolve a área arredondada se estiver na faixa plausível (15..1000 m²).

    Áreas fora desta faixa são geralmente ruído (ex.: 0, 999999 placeholders
    do CEF) e prejudicam a query mais do que ajudam.
    """
    if area_m2 is None:
        return None
    try:
        v = float(area_m2)
    except (TypeError, ValueError):
        return None
    if not (15.0 <= v <= 1000.0):
        return None
    return int(round(v))


def _limpar_componente(s: str) -> str:
    """Remove caracteres especiais que confundem buscadores, mantendo acentos.

    Buscadores web (Google/Bing usados pelo Firecrawl Search por baixo)
    funcionam melhor com texto natural — não convém remover acentos do
    nome da cidade, mas convém remover aspas, parênteses e abreviaturas
    crípticas.
    """
    s = (s or "").strip()
    s = re.sub(r"[\"'()<>\[\]{}|]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


@dataclass(frozen=True)
class FraseBusca:
    """Resultado da construção de frase. Imutável para uso seguro em logs."""

    texto: str
    componentes: dict[str, str]

    @property
    def vazia(self) -> bool:
        return not self.texto.strip()


def montar_frase_busca(
    *,
    cidade: str,
    estado_uf: str,
    tipo_imovel: str = "",
    bairro: str = "",
    area_m2: Optional[float] = None,
) -> FraseBusca:
    """Monta UMA frase de busca focada em encontrar comparáveis na cidade-alvo.

    Regras (ordem dos componentes na frase final, da direita para a esquerda
    porque os motores dão peso aos últimos termos):

    1. ``"<tipo> [N m²] [bairro] <cidade> <UF>"``
    2. Sem cidade/UF a frase é considerada inválida (devolve vazio).
    3. Termos opcionais ausentes são simplesmente omitidos (sem placeholders).

    Args:
        cidade: nome do município do leilão (obrigatório, sem normalização agressiva).
        estado_uf: sigla de 2 letras (obrigatório).
        tipo_imovel: tipo livre vindo do edital (será mapeado p/ termo canónico).
        bairro: bairro do imóvel leiloado.
        area_m2: área construída em m² (será incluída só se 15..1000).

    Returns:
        :class:`FraseBusca` com o texto final e os componentes usados.
    """
    cid = _limpar_componente(cidade)
    uf = _normalizar_uf(estado_uf)
    if not cid or not uf:
        return FraseBusca(texto="", componentes={"motivo_vazio": "cidade_ou_uf_ausente"})

    partes: list[str] = []
    componentes: dict[str, str] = {"cidade": cid, "uf": uf}

    tipo = _normalizar_tipo(tipo_imovel)
    if tipo:
        partes.append(tipo)
        componentes["tipo"] = tipo

    area = _area_plausivel(area_m2)
    if area is not None:
        partes.append(f"{area} m²")
        componentes["area_m2"] = str(area)

    bai = _limpar_componente(bairro)
    if bai:
        partes.append(bai)
        componentes["bairro"] = bai

    partes.append(cid)
    partes.append(uf)

    texto = " ".join(p for p in partes if p)
    return FraseBusca(texto=texto, componentes=componentes)
