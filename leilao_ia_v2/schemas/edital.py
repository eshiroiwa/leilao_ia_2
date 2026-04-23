"""
Modelos Pydantic para extração do edital (uma chamada LLM sobre o markdown).
O campo `leilao_extra` segue um JSON Schema estável para consulta por outros agentes.
"""

from __future__ import annotations

import unicodedata
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _fold_ascii_lower(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    ).lower()


class ProcessoJudicial(BaseModel):
    """Processo judicial relacionado ao leilão (dados públicos do edital)."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    numero: Optional[str] = None
    vara: Optional[str] = None
    comarca: Optional[str] = None


class LeilaoExtraJson(BaseModel):
    """
    Conteúdo gravado em `leilao_imoveis.leilao_extra_json`.
    Textos longos em markdown onde fizer sentido para o próximo agente.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    @field_validator("modalidade_venda", mode="before")
    @classmethod
    def normalizar_modalidade_venda(cls, v: Any) -> Any:
        if v is None or v == "":
            return None
        s = _fold_ascii_lower(str(v).strip()).replace(" ", "_").replace("-", "_")
        if s in ("venda_direta", "vendadireta", "compra_direta", "direct_sale"):
            return "venda_direta"
        if s in ("leilao", "em_leilao", "praca"):
            return "leilao"
        return None

    formas_pagamento: list[str] = Field(default_factory=list)
    processo_judicial: Optional[ProcessoJudicial] = None
    regras_leilao_markdown: Optional[str] = None
    observacoes_markdown: Optional[str] = None
    tipo_pagamento_resumo: Optional[str] = None
    modalidade_venda: Optional[str] = Field(
        default=None,
        description=(
            "leilao = há datas de 1ª e/ou 2ª praça ou leilão explícito; "
            "venda_direta = oferta sem 1ª/2ª praça (ex.: 'venda direta', 'Tipo de Venda: vendadireta') "
            "ou quando não houver data_leilao_1_praca nem data_leilao_2_praca e o anúncio for compra direta"
        ),
    )


class ExtracaoEditalLLM(BaseModel):
    """Resposta esperada do modelo após ler o markdown do Firecrawl."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    url_leilao: str
    url_foto_imovel: Optional[str] = Field(
        default=None,
        description=(
            "URL absoluta (https) da imagem principal do imóvel no edital, "
            "se constar no markdown (ex.: sintaxe ![…](url)); null se não houver"
        ),
    )
    endereco: Optional[str] = None
    cidade: Optional[str] = None
    estado: Optional[str] = None
    bairro: Optional[str] = None
    tipo_imovel: Optional[str] = None
    conservacao: Optional[str] = None
    tipo_casa: Optional[str] = None
    andar: Optional[int] = Field(default=None, ge=0)
    area_util: Optional[float] = Field(default=None, ge=0)
    area_total: Optional[float] = Field(default=None, ge=0)
    quartos: Optional[int] = Field(default=None, ge=0)
    vagas: Optional[int] = Field(default=None, ge=0)
    padrao_imovel: Optional[str] = None
    data_leilao_1_praca: Optional[str] = None
    valor_lance_1_praca: Optional[float] = Field(default=None, ge=0)
    data_leilao_2_praca: Optional[str] = None
    valor_lance_2_praca: Optional[float] = Field(default=None, ge=0)
    valor_arrematacao: Optional[float] = Field(
        default=None,
        ge=0,
        description="Lance atual ou mínimo destacado no site, se houver",
    )
    valor_avaliacao: Optional[float] = Field(
        default=None,
        ge=0,
        description=(
            "Valor de avaliação/perícia/venal do imóvel no edital quando constar explicitamente; "
            "não confundir com lance mínimo, 1ª/2ª praça ou valor de arrematação"
        ),
    )
    data_leilao: Optional[str] = None
    leilao_extra: LeilaoExtraJson = Field(default_factory=LeilaoExtraJson)

    @field_validator(
        "endereco",
        "cidade",
        "estado",
        "bairro",
        "tipo_imovel",
        "conservacao",
        "tipo_casa",
        "padrao_imovel",
        "data_leilao_1_praca",
        "data_leilao_2_praca",
        "data_leilao",
        mode="before",
    )
    @classmethod
    def vazio_para_none(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator("url_foto_imovel", mode="before")
    @classmethod
    def url_foto_absoluta_http(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, str):
            return v
        s = v.strip()
        if not s:
            return None
        if s.startswith("//"):
            s = "https:" + s
        if s.startswith("http://") or s.startswith("https://"):
            return s
        return None


def schema_extracao_edital_dict() -> dict[str, Any]:
    """JSON Schema raiz (objeto) para documentação / validação externa."""
    return ExtracaoEditalLLM.model_json_schema()
