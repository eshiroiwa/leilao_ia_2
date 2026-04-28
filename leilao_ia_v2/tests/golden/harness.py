"""Núcleo do harness golden: carrega caso, executa pipeline, compara saída.

Princípios:

- **Pipeline real** — usamos :func:`leilao_ia_v2.comparaveis.pipeline.executar_pipeline`
  com `persistir=False` e capturamos as ``LinhaPersistir`` produzidas pelo
  `montar_linha` real. Isso valida ponta a ponta: search → scrape → filtro →
  extracção → validação geográfica → normalização (`tipo_imovel`, `bairro`,
  condomínio) → persistência (sem upsert).
- **Hooks deterministas** — a única coisa mockada é o transporte: `fn_search`
  devolve URLs do snapshot, `fn_scrape` devolve markdowns do snapshot,
  `fn_valida_municipio` resolve via tabela de geocode do snapshot. Tudo o
  resto é código de produção.
- **Esperado mínimo, falhas claras** — cada caso descreve só o que importa
  (cidades, bairros, tipos, contagens, presença/ausência de strings). A
  comparação produz ``DiferencaGolden`` listando cada divergência com
  contexto suficiente para depurar.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from leilao_ia_v2.comparaveis.busca import ResultadoBusca
from leilao_ia_v2.comparaveis.frase import FraseBusca
from leilao_ia_v2.comparaveis.orcamento import OrcamentoFirecrawl
from leilao_ia_v2.comparaveis.persistencia import LinhaPersistir
from leilao_ia_v2.comparaveis.pipeline import (
    LeilaoAlvo,
    ResultadoPipeline,
    executar_pipeline,
)
from leilao_ia_v2.comparaveis.refino_individual import ResultadoRefino
from leilao_ia_v2.comparaveis.scrape import ResultadoScrape
from leilao_ia_v2.comparaveis.validacao_cidade import (
    PRECISAO_RUA,
    ResultadoValidacaoMunicipio,
)


CASOS_DIR = Path(__file__).parent / "casos"


# -----------------------------------------------------------------------------
# Modelo de caso
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CasoGolden:
    """Estrutura imutável de um caso golden, parseada de JSON.

    Campos opcionais relevantes:

    - ``pendente``: string com motivo. Quando presente, o caso é tratado como
      ``xfail`` — esperamos que falhe (porque documenta um bug conhecido
      ainda não corrigido). Se de repente o caso PASSAR com ``pendente``
      definido, é sinal de que o bug foi corrigido e a marca pode ser
      removida.
    """

    nome: str
    descricao: str
    leilao: dict[str, Any]
    busca_urls: tuple[str, ...]
    scrapes: dict[str, str]
    geocodes: dict[str, dict[str, Any]]
    esperado: dict[str, Any]
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def pendente(self) -> str:
        """Motivo da marca pendente (xfail), se houver. ``""`` quando ativo."""
        return str(self.raw.get("pendente") or "").strip()

    @classmethod
    def carregar(cls, caminho: Path) -> "CasoGolden":
        data = json.loads(caminho.read_text(encoding="utf-8"))
        return cls(
            nome=caminho.stem,
            descricao=str(data.get("descricao") or ""),
            leilao=dict(data.get("leilao") or {}),
            busca_urls=tuple(data.get("busca", {}).get("urls") or []),
            scrapes={str(s["url"]): str(s.get("markdown") or "") for s in data.get("scrapes") or []},
            geocodes=_indexar_geocodes(data.get("geocodes") or []),
            esperado=dict(data.get("esperado") or {}),
            raw=data,
        )


def _indexar_geocodes(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Indexa geocodes por chave de busca (logradouro+bairro+cidade).

    A chave é normalizada para minúsculo; a comparação no harness fará a
    mesma normalização. Permite hit por "Rua X" ou "Rua X | Centro".
    """
    out: dict[str, dict[str, Any]] = {}
    for it in items:
        chaves = it.get("chaves") or [it.get("chave")]
        for k in chaves:
            if k:
                out[str(k).strip().lower()] = dict(it)
    return out


# -----------------------------------------------------------------------------
# Resultado da comparação
# -----------------------------------------------------------------------------

@dataclass
class DiferencaGolden:
    campo: str
    esperado: Any
    obtido: Any

    def __str__(self) -> str:  # pragma: no cover - formatação
        return f"  [DIFF] {self.campo}: esperado={self.esperado!r} obtido={self.obtido!r}"


@dataclass
class ResultadoGolden:
    caso: CasoGolden
    pipeline_resultado: ResultadoPipeline
    linhas_capturadas: list[LinhaPersistir]
    diferencas: list[DiferencaGolden] = field(default_factory=list)

    @property
    def passou(self) -> bool:
        return not self.diferencas


# -----------------------------------------------------------------------------
# Hooks (mocks deterministas)
# -----------------------------------------------------------------------------

def _fn_search_factory(caso: CasoGolden) -> Callable[..., ResultadoBusca]:
    def _fn(texto: str, *, limit: int = 10, orcamento, cliente=None):
        if not caso.busca_urls:
            return ResultadoBusca(executada=False, motivo_nao_executada="snapshot_sem_urls")
        if not orcamento.pode_search(limit=limit):
            return ResultadoBusca(executada=False, motivo_nao_executada="orcamento_search")
        custo = orcamento.consumir_search(limit=limit, query=texto)
        urls = tuple(caso.busca_urls[: max(1, int(limit))])
        return ResultadoBusca(
            urls_aceites=urls,
            urls_descartadas=(),
            custo_creditos=int(custo),
            executada=True,
        )

    return _fn


def _fn_scrape_factory(caso: CasoGolden) -> Callable[..., ResultadoScrape]:
    def _fn(url: str, *, orcamento, cliente=None):
        md = caso.scrapes.get(url, "")
        if not md.strip():
            return ResultadoScrape(url=url, executado=False, motivo_nao_executado="snapshot_sem_markdown")
        if not orcamento.pode_scrape():
            return ResultadoScrape(url=url, executado=False, motivo_nao_executado="orcamento_scrape")
        custo = orcamento.consumir_scrape(url=url)
        return ResultadoScrape(
            url=url,
            markdown=md,
            executado=True,
            custo_creditos=int(custo),
            fonte="firecrawl",
        )

    return _fn


def _fn_valida_factory(caso: CasoGolden) -> Callable[..., ResultadoValidacaoMunicipio]:
    """Resolve `validar_municipio_card` via tabela do snapshot.

    O caso lista geocodes por chave (logradouro, ou ``logradouro | bairro``).
    Se nenhuma chave casa, devolvemos um ``ResultadoValidacaoMunicipio`` que
    aprova *somente* quando ``cidade_no_markdown`` foi marcado pelo extrator
    (defesa em profundidade do pipeline real).
    """
    def _fn(
        *,
        logradouro: str,
        bairro: str,
        estado_uf: str,
        cidade_alvo: str,
        cidade_no_markdown: str = "",
        pagina_confirmada: bool = False,
    ):
        chave_logr = (logradouro or "").strip().lower()
        chave_full = f"{chave_logr} | {(bairro or '').strip().lower()}"
        info: Optional[dict[str, Any]] = None
        for k in (chave_full, chave_logr):
            if k in caso.geocodes:
                info = caso.geocodes[k]
                break
        if info is None:
            # Sem geocode no snapshot — aprova só se a página confirmou a cidade
            # alvo E o card teve evidência textual da cidade (mesmo padrão do
            # pipeline real, que confia em `cidade_no_markdown` como fallback).
            if pagina_confirmada and cidade_no_markdown:
                return ResultadoValidacaoMunicipio(
                    valido=True,
                    motivo="aprovado_por_evidencia_textual_snapshot",
                    municipio_real=cidade_alvo,
                    coordenadas=(-22.0, -45.0),
                    municipio_alvo_slug=_slug(cidade_alvo),
                    municipio_real_slug=_slug(cidade_alvo),
                    precisao_geo=PRECISAO_RUA,
                )
            return ResultadoValidacaoMunicipio(
                valido=False,
                motivo="sem_geocode_no_snapshot",
                municipio_alvo_slug=_slug(cidade_alvo),
            )

        municipio_real = str(info.get("municipio") or info.get("cidade") or "").strip()
        slug_alvo = _slug(cidade_alvo)
        slug_real = _slug(municipio_real)
        if slug_real != slug_alvo:
            return ResultadoValidacaoMunicipio(
                valido=False,
                motivo="municipio_diferente",
                municipio_real=municipio_real,
                municipio_alvo_slug=slug_alvo,
                municipio_real_slug=slug_real,
            )
        return ResultadoValidacaoMunicipio(
            valido=True,
            motivo="ok",
            municipio_real=municipio_real,
            coordenadas=(float(info["lat"]), float(info["lon"])),
            municipio_alvo_slug=slug_alvo,
            municipio_real_slug=slug_real,
            precisao_geo=str(info.get("precisao") or PRECISAO_RUA),
        )

    return _fn


def _refino_noop(cards_validados, **_kw) -> ResultadoRefino:
    return ResultadoRefino(cards_finais=list(cards_validados))


def _slug(s: str) -> str:
    import unicodedata as ud
    s = ud.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")
    return "".join(ch.lower() for ch in s if ch.isalnum())


# -----------------------------------------------------------------------------
# Executor
# -----------------------------------------------------------------------------

def executar_caso(caso: CasoGolden) -> ResultadoGolden:
    """Executa o pipeline real com hooks deterministas e devolve resultado."""
    leilao_dict = dict(caso.leilao)
    alvo = LeilaoAlvo(
        cidade=str(leilao_dict.get("cidade") or "").strip(),
        estado_uf=str(leilao_dict.get("estado_uf") or leilao_dict.get("estado") or "").strip().upper()[:2],
        tipo_imovel=str(leilao_dict.get("tipo_imovel") or "apartamento").strip().lower(),
        bairro=str(leilao_dict.get("bairro") or "").strip(),
        area_m2=float(leilao_dict.get("area_m2") or leilao_dict.get("area_util") or 0) or None,
    )

    capturadas: list[LinhaPersistir] = []

    def _fn_persistir(_client, linhas: list[LinhaPersistir]) -> int:
        capturadas.extend(linhas)
        return len(linhas)

    orc = OrcamentoFirecrawl(cap=int(caso.raw.get("orcamento_cap", 50)))

    # Frase de busca: usamos a real, mas se o snapshot define `frase_override`,
    # respeitamos (útil quando queremos testar variações Q0/Q1/Q2/Q3).
    fn_montar_frase = None
    if caso.raw.get("frase_override"):
        from leilao_ia_v2.comparaveis.frase import FraseBusca as FB
        frase_text = str(caso.raw["frase_override"])
        fn_montar_frase = lambda **kw: FB(texto=frase_text, componentes={})

    # Patch refino para no-op (refino tem testes próprios; aqui isolamos
    # busca→scrape→filtro→validar→persistir).
    import leilao_ia_v2.comparaveis.pipeline as pl
    refino_original = pl.refinar_cards_top_n
    pl.refinar_cards_top_n = _refino_noop  # type: ignore[assignment]
    try:
        kwargs: dict[str, Any] = dict(
            orcamento=orc,
            supabase_client=object(),
            cidades_conhecidas=list(caso.raw.get("cidades_conhecidas") or []),
            leilao_dict=leilao_dict,
            fn_search=_fn_search_factory(caso),
            fn_scrape=_fn_scrape_factory(caso),
            fn_valida_municipio=_fn_valida_factory(caso),
            fn_persistir=_fn_persistir,
            persistir=True,
        )
        if fn_montar_frase is not None:
            kwargs["fn_montar_frase"] = fn_montar_frase
        resultado = executar_pipeline(alvo, **kwargs)
    finally:
        pl.refinar_cards_top_n = refino_original  # type: ignore[assignment]

    diffs = comparar(caso, resultado, capturadas)
    return ResultadoGolden(
        caso=caso,
        pipeline_resultado=resultado,
        linhas_capturadas=capturadas,
        diferencas=diffs,
    )


# -----------------------------------------------------------------------------
# Comparador
# -----------------------------------------------------------------------------

def comparar(
    caso: CasoGolden,
    resultado: ResultadoPipeline,
    linhas: list[LinhaPersistir],
) -> list[DiferencaGolden]:
    """Compara saída do pipeline com expectativas declaradas no caso.

    Cada chave do bloco ``esperado`` é opcional. Quando ausente, não
    verificamos. Quando presente, gera um ``DiferencaGolden`` se divergir.
    """
    esp = caso.esperado
    diffs: list[DiferencaGolden] = []

    if "persistidos" in esp:
        if int(resultado.estatisticas.persistidos) != int(esp["persistidos"]):
            diffs.append(DiferencaGolden("persistidos", esp["persistidos"], resultado.estatisticas.persistidos))

    if "min_persistidos" in esp:
        if int(resultado.estatisticas.persistidos) < int(esp["min_persistidos"]):
            diffs.append(
                DiferencaGolden(
                    "min_persistidos",
                    f">= {esp['min_persistidos']}",
                    resultado.estatisticas.persistidos,
                )
            )

    if "max_persistidos" in esp:
        if int(resultado.estatisticas.persistidos) > int(esp["max_persistidos"]):
            diffs.append(
                DiferencaGolden(
                    "max_persistidos",
                    f"<= {esp['max_persistidos']}",
                    resultado.estatisticas.persistidos,
                )
            )

    if "cidades_anuncios" in esp:
        cidades_obtidas = sorted({l.cidade for l in linhas})
        cidades_esp = sorted(esp["cidades_anuncios"])
        if cidades_obtidas != cidades_esp:
            diffs.append(DiferencaGolden("cidades_anuncios", cidades_esp, cidades_obtidas))

    if "cidades_proibidas" in esp:
        proibidas = {_slug(c) for c in esp["cidades_proibidas"]}
        achadas = sorted({l.cidade for l in linhas if _slug(l.cidade) in proibidas})
        if achadas:
            diffs.append(DiferencaGolden("cidades_proibidas (encontradas!)", [], achadas))

    if "tipos" in esp:
        tipos_obtidos = sorted({l.tipo_imovel for l in linhas})
        tipos_esp = sorted(esp["tipos"])
        if tipos_obtidos != tipos_esp:
            diffs.append(DiferencaGolden("tipos", tipos_esp, tipos_obtidos))

    if "tipos_proibidos" in esp:
        proibidos = set(esp["tipos_proibidos"])
        achados = sorted({l.tipo_imovel for l in linhas if l.tipo_imovel in proibidos})
        if achados:
            diffs.append(DiferencaGolden("tipos_proibidos (encontrados!)", [], achados))

    if "bairros_contem" in esp:
        bairros_esp = [str(x).strip().lower() for x in esp["bairros_contem"]]
        bairros_obtidos = [str(l.bairro or "").strip().lower() for l in linhas]
        faltantes = [b for b in bairros_esp if not any(b in (o or "") for o in bairros_obtidos)]
        if faltantes:
            diffs.append(DiferencaGolden("bairros_contem (não encontrados)", faltantes, bairros_obtidos))

    if "bairros_proibidos" in esp:
        proibidos = [str(x).strip().lower() for x in esp["bairros_proibidos"]]
        achados = sorted(
            {
                l.bairro
                for l in linhas
                if l.bairro and any(p in str(l.bairro).lower() for p in proibidos)
            }
        )
        if achados:
            diffs.append(DiferencaGolden("bairros_proibidos (encontrados!)", [], achados))

    if "bairro_promocao" in esp:
        # Cada anúncio cuja URL/título indica um bairro deve ter `bairro` ≠ ""
        # OU `metadados_json.bairro_origem in {url, titulo, card}`.
        urls_esp = esp["bairro_promocao"]
        for url, info in urls_esp.items():
            achadas = [l for l in linhas if l.url_anuncio == url]
            if not achadas:
                diffs.append(DiferencaGolden(f"bairro_promocao[{url}] (linha ausente)", info, None))
                continue
            l = achadas[0]
            origem = str((l.metadados_json or {}).get("bairro_origem") or "")
            if "origem" in info and origem != info["origem"]:
                diffs.append(DiferencaGolden(f"bairro_promocao[{url}].origem", info["origem"], origem))
            if "bairro" in info and (l.bairro or "") != info["bairro"]:
                diffs.append(DiferencaGolden(f"bairro_promocao[{url}].bairro", info["bairro"], l.bairro))

    if "tipo_promocao" in esp:
        urls_esp = esp["tipo_promocao"]
        for url, info in urls_esp.items():
            achadas = [l for l in linhas if l.url_anuncio == url]
            if not achadas:
                diffs.append(DiferencaGolden(f"tipo_promocao[{url}] (linha ausente)", info, None))
                continue
            l = achadas[0]
            promo = (l.metadados_json or {}).get("tipo_imovel_promocao") or {}
            if "tipo_final" in info and l.tipo_imovel != info["tipo_final"]:
                diffs.append(DiferencaGolden(f"tipo_promocao[{url}].tipo_final", info["tipo_final"], l.tipo_imovel))
            if "promovido" in info and bool(promo.get("promovido")) != bool(info["promovido"]):
                diffs.append(
                    DiferencaGolden(
                        f"tipo_promocao[{url}].promovido", info["promovido"], promo.get("promovido")
                    )
                )

    if "metadados_marcadores" in esp:
        # esp["metadados_marcadores"] = ["cidade_centroide", ...] — TODOS
        # os anúncios persistidos devem ter `precisao_geo` em algum desses.
        permitidos = set(esp["metadados_marcadores"])
        ofensores = sorted(
            {
                str((l.metadados_json or {}).get("precisao_geo") or "")
                for l in linhas
                if str((l.metadados_json or {}).get("precisao_geo") or "") not in permitidos
            }
        )
        if ofensores:
            diffs.append(DiferencaGolden("metadados_marcadores (precisao_geo fora do permitido)", permitidos, ofensores))

    return diffs


# -----------------------------------------------------------------------------
# Loader e formatador
# -----------------------------------------------------------------------------

def listar_casos() -> list[Path]:
    """Devolve todos os JSONs em ``casos/`` ordenados por nome (estável)."""
    if not CASOS_DIR.exists():
        return []
    return sorted(CASOS_DIR.glob("*.json"))


def formatar_resultado(r: ResultadoGolden) -> str:
    """Formata o ResultadoGolden em texto legível para CLI / pytest."""
    estatus = "OK" if r.passou else "FALHA"
    cabec = f"[{estatus}] {r.caso.nome}: {r.caso.descricao}"
    if r.passou:
        return cabec + (
            f"  | persistidos={r.pipeline_resultado.estatisticas.persistidos} "
            f"cards_extraidos={r.pipeline_resultado.estatisticas.cards_extraidos}"
        )
    linhas: list[str] = [cabec]
    linhas.append(
        f"  Stats: persistidos={r.pipeline_resultado.estatisticas.persistidos} "
        f"cards_extraidos={r.pipeline_resultado.estatisticas.cards_extraidos} "
        f"descartados={r.pipeline_resultado.estatisticas.cards_descartados_validacao} "
        f"motivos={dict(r.pipeline_resultado.estatisticas.motivos_descarte_validacao)}"
    )
    for d in r.diferencas:
        linhas.append(str(d))
    if r.linhas_capturadas:
        linhas.append("  Anúncios persistidos:")
        for l in r.linhas_capturadas[:10]:
            linhas.append(
                f"    - {l.cidade}/{l.estado} | {l.tipo_imovel} | bairro={l.bairro!r} "
                f"| origem={(l.metadados_json or {}).get('bairro_origem')!r}"
            )
    return "\n".join(linhas)
