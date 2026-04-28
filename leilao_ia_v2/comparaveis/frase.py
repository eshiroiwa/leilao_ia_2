"""
Construção determinística de **uma única** frase de busca para Firecrawl Search.

Prioriza precisão geográfica e linguagem natural do que utilizadores e portais
escrevem (uma chamada de search por ingestão):

- A frase **sempre** termina com ``"<cidade> <UF>"``.
- O tipo de imóvel entra **no plural com "à venda em"** (ex.: "apartamentos à
  venda em Centro Pindamonhangaba SP") — alinha com o SEO dos portais
  imobiliários, que indexam páginas de listagem em vez de anúncios isolados.
- Inclui **bairro** quando conhecido (sinal de proximidade muito forte e
  ortograficamente mais estável que rua).
- **Não inclui área em m²**: páginas de listagem juntam várias áreas; um
  número específico restringe demais e prejudica a recall.
- **Nunca** inclui o nome do empreendimento sozinho: foi a fonte do bug
  histórico em que o "Empreendimento Vila Maria" matchou um anúncio em SP
  capital.

A função é pura (sem efeitos colaterais) e totalmente testável.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Optional


# Mapeamento tipo livre → (singular canónico, plural usado na frase).
_TIPOS_CANONICOS: dict[str, tuple[str, str]] = {
    "apartamento": ("apartamento", "apartamentos"),
    "apto": ("apartamento", "apartamentos"),
    "ap": ("apartamento", "apartamentos"),
    "casa": ("casa", "casas"),
    "sobrado": ("sobrado", "sobrados"),
    "terreno": ("terreno", "terrenos"),
    "lote": ("terreno", "terrenos"),
    "gleba": ("terreno", "terrenos"),
    "sala": ("sala comercial", "salas comerciais"),
    "loja": ("loja", "lojas"),
    "galpao": ("galpão", "galpões"),
    "galpão": ("galpão", "galpões"),
    "comercial": ("imóvel comercial", "imóveis comerciais"),
    "industrial": ("imóvel industrial", "imóveis industriais"),
    "rural": ("imóvel rural", "imóveis rurais"),
}


def _strip_acentos(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def _normalizar_tipo(tipo_imovel: str) -> tuple[str, str]:
    """Mapeia variantes de tipo para (singular, plural) canónicos.

    Retorna ``("", "")`` quando o tipo é desconhecido — nesse caso, a frase é
    montada sem prefixo de tipo.

    >>> _normalizar_tipo("Apartamento Padrão")
    ('apartamento', 'apartamentos')
    >>> _normalizar_tipo("LOTE")
    ('terreno', 'terrenos')
    >>> _normalizar_tipo("xpto")
    ('', '')
    """
    base = _strip_acentos((tipo_imovel or "").strip().lower())
    base = re.sub(r"[^a-z ]+", " ", base).strip()
    if not base:
        return ("", "")
    for token in base.split():
        if token in _TIPOS_CANONICOS:
            return _TIPOS_CANONICOS[token]
    return ("", "")


def _normalizar_uf(uf: str) -> str:
    s = (uf or "").strip().upper()
    return s[:2] if len(s) >= 2 else ""


def _limpar_componente(s: str) -> str:
    """Remove caracteres especiais que confundem buscadores, mantendo acentos."""
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
    area_m2: Optional[float] = None,  # noqa: ARG001 - aceito para compat; intencionalmente ignorado
) -> FraseBusca:
    """Monta UMA frase de busca focada em encontrar comparáveis na cidade-alvo.

    Estrutura final:

    - ``"<tipos no plural> à venda em <bairro> <cidade> <UF>"`` (com tipo + bairro)
    - ``"<tipos no plural> à venda em <cidade> <UF>"`` (sem bairro)
    - ``"imóveis à venda em <cidade> <UF>"`` (sem tipo conhecido)

    Sem cidade/UF a frase é considerada inválida (devolve vazia).

    Args:
        cidade: nome do município do leilão (obrigatório).
        estado_uf: sigla de 2 letras (obrigatório).
        tipo_imovel: tipo livre vindo do edital (será mapeado p/ plural canónico).
        bairro: bairro do imóvel leiloado.
        area_m2: **ignorado** (mantido por compat de assinatura). Áreas
            específicas no termo de busca prejudicam a recall em listagens
            que agregam vários tamanhos.

    Returns:
        :class:`FraseBusca` com o texto final e os componentes usados.
    """
    cid = _limpar_componente(cidade)
    uf = _normalizar_uf(estado_uf)
    if not cid or not uf:
        return FraseBusca(texto="", componentes={"motivo_vazio": "cidade_ou_uf_ausente"})

    componentes: dict[str, str] = {"cidade": cid, "uf": uf}

    tipo_singular, tipo_plural = _normalizar_tipo(tipo_imovel)
    prefixo = tipo_plural if tipo_plural else "imóveis"
    if tipo_singular:
        componentes["tipo"] = tipo_singular

    bai = _limpar_componente(bairro)
    partes: list[str] = [prefixo, "à venda", "em"]
    if bai:
        partes.append(bai)
        componentes["bairro"] = bai
    partes.append(cid)
    partes.append(uf)

    texto = " ".join(p for p in partes if p)
    return FraseBusca(texto=texto, componentes=componentes)
