"""
Estrutura versionada de ``leilao_imoveis.operacao_simulacao_json``.

- Comissão leiloeiro e ITBI: **% sobre o lance nominal** (0–100). Registro: **% sobre o lance** ou valor fixo em R$ (se > 0, substitui o %).
- Desconto **pagamento à vista** (opcional): reduz só o **valor pago** do lance; comissão do leiloeiro (e % ITBI/registro) continuam sobre o **lance cheio** (arrematação nominal).
- Corretagem: % sobre venda estimada ou R$ fixo.
- IR: ``ir_aliquota_pf_pct`` / ``ir_aliquota_pj_pct`` em **0–100**. Documentos antigos com fração 0–1 são migrados.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


OPERACAO_SIMULACAO_JSON_VERSAO = 1


class ModoValorVenda(str, Enum):
    """Como estimar o valor de venda a partir do cache/anúncios."""

    CACHE_PRECO_M2_X_AREA = "cache_preco_m2_x_area"
    CACHE_VALOR_MEDIO_VENDA = "cache_valor_medio_venda"
    CACHE_MENOR_VALOR_VENDA = "cache_menor_valor_venda"
    ANUNCIOS_VALOR_MEDIO = "anuncios_valor_medio"
    ANUNCIOS_MENOR_VALOR = "anuncios_menor_valor"
    ANUNCIOS_PRECO_M2_X_AREA = "anuncios_preco_m2_x_area"
    MANUAL = "manual"


class ModoReforma(str, Enum):
    """Reforma: valor manual ou R$/m² por padrão de acabamento."""

    MANUAL = "manual"
    BASICA = "basica"
    MEDIA = "media"
    COMPLETA = "completa"
    ALTO_PADRAO = "alto_padrao"


class ModoRoiDesejado(str, Enum):
    """Qual ROI comparar ao alvo para o lance máximo."""

    BRUTO = "bruto"
    LIQUIDO = "liquido"


class ModoPagamentoSimulacao(str, Enum):
    """Forma de quitar a arrematação na simulação (aba na UI)."""

    VISTA = "vista"
    PRAZO = "prazo"
    FINANCIADO = "financiado"


class SimulacaoOperacaoInputs(BaseModel):
    """Parâmetros editáveis da simulação (persistidos em ``inputs``)."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    tipo_pessoa: Literal["PF", "PJ"] = "PF"
    modo_valor_venda: ModoValorVenda = ModoValorVenda.CACHE_VALOR_MEDIO_VENDA
    valor_venda_manual: Optional[float] = Field(default=None, ge=0)
    cache_media_bairro_id: Optional[str] = Field(
        default=None,
        description=(
            "UUID do cache usado para m² médio / valor médio; vazio = primeiro elegível em "
            "cache_media_bairro_ids (ignora caches só de referência, ex. terrenos)."
        ),
    )

    lance_brl: float = Field(0, ge=0)
    #: UI: referência do lance a partir do edital — ``False`` = 1ª praça, ``True`` = 2ª praça (o valor em R$ continua em ``lance_brl``).
    usar_lance_segunda_praca: bool = False
    #: Desconto para pagamento à vista (ex.: 10%): o caixa pago do lance = nominal × (1 − %/100). Comissão leiloeiro segue o nominal.
    desconto_pagamento_avista: bool = False
    desconto_pagamento_avista_pct: float = Field(10.0, ge=0, le=99)

    modo_pagamento: ModoPagamentoSimulacao = ModoPagamentoSimulacao.VISTA
    #: Meses da arrematação até a venda estimada (ROI/fluxo acompanham T).
    tempo_estimado_venda_meses: float = Field(12.0, ge=0.25, le=600.0)
    # --- Parcelamento judicial (CPC, típ. 25–30% entrada, até 30x; juros editáveis) ---
    prazo_entrada_pct: float = Field(30.0, ge=0.0, le=95.0)
    prazo_num_parcelas: int = Field(30, ge=1, le=60)
    prazo_juros_mensal_pct: float = Field(1.0, ge=0.0, le=5.0)
    # --- Financiamento bancário (SAC/Price, típ. 11% a.a. + TR; entrada 5–20%) ---
    fin_entrada_pct: float = Field(20.0, ge=5.0, le=50.0)
    fin_prazo_meses: int = Field(360, ge=12, le=480)
    fin_taxa_juros_anual_pct: float = Field(14.0, ge=0.0, le=20.0)
    fin_sistema: Literal["SAC", "PRICE"] = "SAC"

    comissao_leiloeiro_pct_sobre_arrematacao: float = Field(5.0, ge=0, le=100)
    comissao_leiloeiro_brl: float = Field(
        0,
        ge=0,
        description="Se > 0, substitui o cálculo percentual sobre o lance.",
    )

    itbi_pct_sobre_arrematacao: float = Field(3.0, ge=0, le=100)
    itbi_brl: float = Field(0, ge=0, description="Legado: se > 0, substitui o % (UI usa só percentual).")

    registro_pct_sobre_arrematacao: float = Field(2.0, ge=0, le=100)
    registro_brl: float = Field(0, ge=0, description="Se > 0, substitui o % sobre o lance (legado).")
    condominio_atrasado_brl: float = Field(0, ge=0)
    iptu_atrasado_brl: float = Field(0, ge=0)

    reforma_modo: ModoReforma = ModoReforma.BASICA
    reforma_brl: float = Field(0, ge=0, description="Usado quando reforma_modo = manual.")

    desocupacao_brl: float = Field(0, ge=0)
    outros_custos_brl: float = Field(0, ge=0)

    comissao_imobiliaria_brl: float = Field(0, ge=0)
    comissao_imobiliaria_pct_sobre_venda: float = Field(6.0, ge=0, le=100)

    ir_aliquota_pf_pct: float = Field(15.0, ge=0, le=100)
    ir_aliquota_pj_pct: float = Field(6.7, ge=0, le=100)
    ir_valor_manual_brl: Optional[float] = Field(default=None, ge=0)

    roi_desejado_pct: Optional[float] = Field(default=50.0, ge=0, le=1000)
    roi_desejado_modo: ModoRoiDesejado = ModoRoiDesejado.BRUTO

    @model_validator(mode="before")
    @classmethod
    def _compat_documentos_antigos(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        # IR antigo em 0–1
        if data.get("ir_aliquota_pf_pct") is None and "ir_aliquota_pf_sobre_lucro_imob" in data:
            f = float(data.get("ir_aliquota_pf_sobre_lucro_imob") or 0)
            data["ir_aliquota_pf_pct"] = f * 100 if 0 < f <= 1 else f
        if data.get("ir_aliquota_pj_pct") is None and "ir_aliquota_pj_sobre_venda_liquida" in data:
            f = float(data.get("ir_aliquota_pj_sobre_venda_liquida") or 0)
            data["ir_aliquota_pj_pct"] = f * 100 if 0 < f <= 1 else f
        # Documentos só com reforma em R$ (sem modo)
        if data.get("reforma_modo") is None and data.get("reforma_brl"):
            data["reforma_modo"] = ModoReforma.MANUAL.value
        return data


class SimulacaoOperacaoOutputs(BaseModel):
    """Resultado determinístico da última execução."""

    model_config = ConfigDict(extra="ignore")

    valor_venda_estimado: float = 0.0
    modo_valor_venda_resolvido: str = ""
    cache_media_bairro_id_usado: Optional[str] = None
    area_m2_usada: Optional[float] = None

    lance_brl: float = 0.0
    #: Valor pago de lance com desconto à vista; sem desconto, igual a ``lance_brl``.
    lance_pago_apos_desconto_brl: float = 0.0
    desconto_pagamento_avista_ativo: bool = False
    desconto_pagamento_avista_pct_efetivo: float = 0.0
    desconto_pagamento_avista_valor_brl: float = 0.0
    comissao_leiloeiro_brl: float = 0.0
    comissao_leiloeiro_pct_efetivo: float = 0.0
    itbi_brl: float = 0.0
    itbi_pct_efetivo: float = 0.0
    registro_brl: float = 0.0
    registro_pct_efetivo: float = 0.0
    condominio_atrasado_brl: float = 0.0
    iptu_atrasado_brl: float = 0.0
    reforma_brl: float = 0.0
    reforma_modo_resolvido: str = ""
    desocupacao_brl: float = 0.0
    outros_custos_brl: float = 0.0
    subtotal_custos_operacao: float = 0.0

    comissao_imobiliaria_brl: float = 0.0
    custo_total_com_corretagem: float = 0.0

    lucro_bruto: float = 0.0
    roi_bruto: Optional[float] = None

    base_ir: float = 0.0
    ir_calculado_brl: float = 0.0
    ir_usou_manual: bool = False

    lucro_liquido: float = 0.0
    roi_liquido: Optional[float] = None

    roi_desejado_pct_informado: Optional[float] = None
    roi_desejado_modo_informado: Optional[str] = None
    lance_maximo_para_roi_desejado: Optional[float] = None
    lance_maximo_roi_notas: list[str] = Field(default_factory=list)

    calculado_em_iso: str = ""
    notas: list[str] = Field(default_factory=list)
    # --- Cenário com tempo (à vista: anualiza ROI; prazo/fin: fluxo até a venda) ---
    modo_pagamento_resolvido: str = ""
    tempo_estimado_venda_meses_resolvido: float = 0.0
    investimento_cash_ate_momento_venda: float = 0.0
    saldo_divida_quitacao_na_venda: float = 0.0
    total_juros_ate_momento_venda: float = 0.0
    pmt_mensal_resolvido: float = Field(
        0.0,
        description=(
            "SAC: 1.ª prestação (P/n + juros s/ saldo). Price: prestação fixa. "
            "A taxa anual informada é convertida em taxa mensal composta."
        ),
    )
    #: À vista: lance pago (pós-desconto). Parcelado/financiado: entrada sobre o lance nominal.
    desembolso_inicial_lance_ou_entrada_brl: float = 0.0
    #: Lance/entrada + comissão leiloeiro + ITBI + registro (1.ª onda de caixa da arrematação).
    subtotal_grupo_arrematacao_brl: float = 0.0
    #: Condomínio, IPTU, reforma, desocupação, outros (sem tributos do lance).
    subtotal_grupo_imovel_obra_brl: float = 0.0
    #: Soma das prestações até T (0 à vista). Alinha com o fluxo do cenário prazo/fin.
    total_parcelas_acumuladas_ate_t_brl: float = 0.0
    #: Nº de prestações no contrato (parcelas judiciais ou prazo do financiamento); 0 à vista.
    num_prestacoes_contrato_resolvido: int = 0
    roi_bruto_anualizado: Optional[float] = None
    roi_liquido_anualizado: Optional[float] = None


class OperacaoSimulacaoDocumento(BaseModel):
    """Documento completo gravado na coluna JSONB."""

    model_config = ConfigDict(extra="ignore")

    versao: int = OPERACAO_SIMULACAO_JSON_VERSAO
    inputs: SimulacaoOperacaoInputs = Field(default_factory=SimulacaoOperacaoInputs)
    outputs: Optional[SimulacaoOperacaoOutputs] = None


def parse_operacao_simulacao_json(raw: Any) -> OperacaoSimulacaoDocumento:
    if not isinstance(raw, dict) or not raw:
        return OperacaoSimulacaoDocumento()
    return OperacaoSimulacaoDocumento.model_validate(
        {
            "versao": int(raw.get("versao") or OPERACAO_SIMULACAO_JSON_VERSAO),
            "inputs": raw.get("inputs") or {},
            "outputs": raw.get("outputs"),
        }
    )


SIMULACOES_MODALIDADES_JSON_VERSAO = 1


class SimulacoesModalidadesBundle(BaseModel):
    """Três simulações (à vista, parcelado judicial, financiado) para comparação e gravação em Supabase."""

    model_config = ConfigDict(extra="ignore")

    versao: int = SIMULACOES_MODALIDADES_JSON_VERSAO
    vista: OperacaoSimulacaoDocumento = Field(default_factory=OperacaoSimulacaoDocumento)
    prazo: OperacaoSimulacaoDocumento = Field(default_factory=OperacaoSimulacaoDocumento)
    financiado: OperacaoSimulacaoDocumento = Field(default_factory=OperacaoSimulacaoDocumento)


def parse_simulacoes_modalidades_json(
    raw: Any,
    *,
    legado_operacao: Any = None,
) -> SimulacoesModalidadesBundle:
    """
    Faz o parse de ``simulacoes_modalidades_json`` ou deriva a partir de ``operacao_simulacao_json`` legado
    (cópia de ``inputs`` nas três modalidades com ``modo_pagamento`` ajustado).
    """
    if isinstance(raw, dict) and any(k in raw for k in ("vista", "prazo", "financiado")):
        return SimulacoesModalidadesBundle.model_validate(
            {
                "versao": int(raw.get("versao") or SIMULACOES_MODALIDADES_JSON_VERSAO),
                "vista": raw.get("vista") or {},
                "prazo": raw.get("prazo") or {},
                "financiado": raw.get("financiado") or {},
            }
        )
    leg = parse_operacao_simulacao_json(legado_operacao or {})
    inv = leg.inputs
    v = leg.versao

    def _doc(m: ModoPagamentoSimulacao) -> OperacaoSimulacaoDocumento:
        return OperacaoSimulacaoDocumento(
            versao=v,
            inputs=inv.model_copy(update={"modo_pagamento": m}),
            outputs=leg.outputs if m == ModoPagamentoSimulacao.VISTA else None,
        )

    return SimulacoesModalidadesBundle(
        vista=_doc(ModoPagamentoSimulacao.VISTA),
        prazo=_doc(ModoPagamentoSimulacao.PRAZO),
        financiado=_doc(ModoPagamentoSimulacao.FINANCIADO),
    )
