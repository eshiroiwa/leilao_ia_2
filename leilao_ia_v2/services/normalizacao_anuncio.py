"""
Regras canónicas de normalização aplicáveis a um anúncio comparável,
isoladas das integrações com Supabase, Firecrawl e geocoder.

Este módulo centraliza decisões que antes viviam em dois sítios diferentes
(``services/cache_media_leilao.py`` e o caminho v2 ``comparaveis/persistencia.py``)
e tinham comportamento divergente — gerando bug onde o **bairro do leilão**
"contaminava" os anúncios e onde ``casa`` não era promovida a
``casa_condominio`` mesmo havendo nome de condomínio no anúncio.

Princípios:

1. **Funções puras** — sem I/O, sem efeitos colaterais. Recebem o que precisam,
   devolvem o que decidiram. Testáveis sem mocks pesados.
2. **Sem invenção** — quando não há evidência independente para o anúncio,
   o campo fica vazio. Preferimos vazio a um valor inventado que envenene
   o cache.
3. **Salvaguardas explícitas** contra padrões conhecidos de falso positivo
   (boilerplate jurídico de edital Caixa, sufixos ``88m2`` em URLs, ``id-``,
   features tipo "academia/piscina/churrasqueira" em vez de bairro real).

As entidades públicas (sem ``_``) podem ser importadas pelo pipeline v2,
pelo cache de média e por testes. As privadas são detalhe de implementação
e podem mudar sem aviso.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional, Tuple

from leilao_ia_v2.vivareal.slug import slug_vivareal


# -----------------------------------------------------------------------------
# Constantes de regex e listas controladas
# -----------------------------------------------------------------------------

# Termos de uso comum em descrições de bairro que **não** são bairro real:
# vêm de blocos "ATTRIBUTOS" / "FEATURES" colados no campo bairro pelo extrator.
_FEATURES_FALSO_BAIRRO: frozenset[str] = frozenset(
    {
        "academia",
        "piscina",
        "churrasqueira",
        "playground",
        "salao-de-festas",
        "salao",
        "portaria",
        "elevador",
        "garagem",
        "varanda",
        "sacada",
        "armarios",
        "armario",
        "mobiliado",
        "lazer",
        "lavanderia",
    }
)

# Sufixos comuns em URL/título que vazam para o campo bairro quando o extrator
# pega o pedaço errado da URL: "88m2", "120m2-venda", "id-12345", "RS180000"
_RE_SUFIXO_LIXO_BAIRRO = re.compile(
    r"(?ix)\b("
    r"\d{2,5}\s*m[²2]?"           # 88m2, 120 m², 1500m
    r"| id[-_]?\d+"               # id-1, id-12345, id_5678
    r"| rs\s*\d{3,}"              # RS180000
    r"| venda|aluguel|locacao"    # ações
    r"| \d{1,3}(?:\.\d{3})+"      # números de preço
    r")\b"
)

# Boilerplate jurídico que aparece em editais (especialmente Caixa) e NÃO deve
# ser interpretado como evidência de que o imóvel é um condomínio residencial.
_BOILERPLATE_CONDOMINIO_TERMOS: tuple[str, ...] = (
    "regras para pagamento",
    "despesas",
    "sob responsabilidade do comprador",
    "a caixa realizará o pagamento",
    "limite de 10%",
    "valor de avaliação",
    "tributos",
)

# Termos que invalidam um candidato a "nome de empreendimento" (são genéricos
# demais ou são boilerplate). Comparados em forma normalizada.
_NOMES_EMPREENDIMENTO_INVALIDOS: frozenset[str] = frozenset(
    {
        "sob responsabilidade do comprador",
        "responsabilidade do comprador",
        "regras para pagamento",
        "despesas",
        "tributos",
    }
)

# Tokens descartados na hora de exigir match parcial de empreendimento (são
# adjetivos comuns em nomes de condomínio e dariam match espúrio sozinhos).
_TOKENS_GENERICOS_EMPREENDIMENTO: frozenset[str] = frozenset(
    {
        "condominio",
        "residencial",
        "predio",
        "edificio",
        "torre",
        "bloco",
        "vila",
    }
)

# Indicadores positivos — quando estão presentes numa frase do edital,
# realmente sinalizam um condomínio residencial (não boilerplate).
# Exigimos pelo menos 3 letras na palavra após "condomínio " para evitar
# falsos positivos como "condomínio, IPTU" (vírgula) ou "condomínio é".
_RE_CONDOMINIO_POSITIVO = re.compile(
    r"(?i)\b(?:condom[ií]nio\s+(?:residencial|fechado|[a-zà-ÿ]{3,})"
    r"|casa\s+em\s+condom[ií]nio)\b"
)
_RE_CONDOMINIO_KEY_VALUE_NEGATIVO = re.compile(r"(?i)\bcondom[ií]nio\s*:")

# UFs brasileiras em slug — usadas para inferir cidade da URL.
_UF_SLUGS: tuple[str, ...] = (
    "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go", "ma",
    "mg", "ms", "mt", "pa", "pb", "pe", "pi", "pr", "rj", "rn",
    "ro", "rr", "rs", "sc", "se", "sp", "to",
)

# Palavras que jamais devem ser tratadas como bairro num candidato extraído da URL.
_STOPWORDS_BAIRRO_URL: frozenset[str] = frozenset(
    {
        "venda",
        "aluguel",
        "locacao",
        "comprar",
        "alugar",
        "imovel",
        "imoveis",
        "imovel-novo",
        "novo",
        "usado",
        "lancamento",
    }
)


# -----------------------------------------------------------------------------
# Helpers básicos
# -----------------------------------------------------------------------------

def _slug_fold(s: Any) -> str:
    raw = str(s or "").strip().lower()
    if not raw:
        return ""
    txt = slug_vivareal(raw)
    return "" if txt in ("", "-") else txt


def _texto_normalizado_match(v: Any) -> str:
    """Forma comparável: minúsculas, sem acentos, só alfanumérico."""
    return re.sub(r"[^a-z0-9]+", "", _slug_fold(v))


def _parse_extra(leilao: dict[str, Any]) -> dict[str, Any]:
    """Lê ``leilao_extra_json`` aceitando tanto dict quanto string JSON."""
    raw = leilao.get("leilao_extra_json") if isinstance(leilao, dict) else None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


# -----------------------------------------------------------------------------
# Saneamento de bairro
# -----------------------------------------------------------------------------

def sanear_bairro(texto: Any) -> str:
    """Limpa um candidato a bairro removendo lixo conhecido.

    Casos cobertos:

    - sufixos como ``88m2``, ``120 m²``, ``id-12345``, ``RS180000`` (vazam
      quando o extrator pega o token errado da URL/título);
    - termos de feature ("academia", "piscina", "churrasqueira", …) que
      portais às vezes colam no campo bairro;
    - colapso de espaços, hífens iniciais/finais, capitalização básica.

    Devolve ``""`` quando, depois de limpo, o resultado fica curto demais ou
    coincide com uma feature genérica.
    """
    s = str(texto or "").strip()
    if not s:
        return ""

    # Remove sufixos lixo (ex.: "Vila Esplanada 333m2" → "Vila Esplanada").
    s = _RE_SUFIXO_LIXO_BAIRRO.sub(" ", s)

    # Remove caracteres de pontuação extremos e colapsa espaços.
    s = re.sub(r"[\s\-_]+", " ", s).strip(" -_,.;:")
    s = re.sub(r"\s{2,}", " ", s)

    if len(s) < 3:
        return ""

    if _slug_fold(s) in _FEATURES_FALSO_BAIRRO:
        return ""

    return s[:80]


# -----------------------------------------------------------------------------
# Inferência de bairro a partir do anúncio (URL e título)
# -----------------------------------------------------------------------------

# Padrão "...bairro(s)/<slug-do-bairro>" — tolera trailing slash, query, fragment.
_RE_BAIRRO_URL_BARRA = re.compile(
    r"(?i)/bairro[s]?/([a-z0-9-]{3,80})(?:/|\?|#|$)"
)


def _bairro_inferido_da_url(url: str, cidade_alvo: str = "") -> str:
    """Extrai um candidato a bairro a partir da URL do anúncio.

    Estratégia conservadora: só retorna bairro quando o segmento aparece
    **antes** do slug da cidade-alvo (ou de uma UF) e **não** é uma stopword
    (`venda`, `aluguel`, `imovel`, …).
    """
    u = str(url or "").strip().lower()
    if not u:
        return ""

    m = _RE_BAIRRO_URL_BARRA.search(u)
    if m:
        cand = m.group(1).strip("-")
        if cand and cand not in _STOPWORDS_BAIRRO_URL:
            return _bairro_titulo_humano(cand)

    # Padrão Viva Real / Zap: "<tipo>-<rooms>-<bairro>-<cidade>-<UF>-..."
    cidade_slug = _slug_fold(cidade_alvo) if cidade_alvo else ""
    if cidade_slug:
        # Procura "<bairro>-<cidade>-" e captura o segmento imediatamente antes da cidade.
        # Limita a 1..4 tokens para o bairro (evita pegar "casa-3-quartos-vila").
        padrao = re.compile(
            rf"(?ix)-(?P<bairro>[a-z][a-z0-9]*(?:-[a-z0-9]+){{0,3}})-{re.escape(cidade_slug)}-"
        )
        m = padrao.search(u)
        if m:
            cand_slug = m.group("bairro")
            tokens = [t for t in cand_slug.split("-") if t]
            tokens_limpos = [t for t in tokens if t not in _STOPWORDS_BAIRRO_URL]
            if tokens_limpos:
                cand_slug = "-".join(tokens_limpos)
                if cand_slug:
                    return _bairro_titulo_humano(cand_slug)

    return ""


_RE_BAIRRO_TITULO_LABEL = re.compile(
    r"(?i)\bbairro\s+([A-Za-zÀ-ÿ][\wÀ-ÿ\s\-']{2,60})"
)
_RE_BAIRRO_TITULO_PREP = re.compile(
    r"(?i)\b(?:no|na|em|do|da)\s+(?:bairro\s+)?([A-Z][A-Za-zÀ-ÿ][\wÀ-ÿ\s\-']{2,60})"
)


def _bairro_inferido_do_titulo(titulo: str) -> str:
    """Extrai bairro do título do anúncio. Devolve ``""`` se nada confiável."""
    t = str(titulo or "").strip()
    if not t:
        return ""

    m = _RE_BAIRRO_TITULO_LABEL.search(t)
    if m:
        return sanear_bairro(m.group(1))

    m = _RE_BAIRRO_TITULO_PREP.search(t)
    if m:
        cand = m.group(1).strip()
        # Filtra preposições que viraram match acidentalmente.
        if cand.lower() not in {"venda", "aluguel", "casa", "apartamento", "sobrado"}:
            return sanear_bairro(cand)

    return ""


def _bairro_titulo_humano(slug_or_text: str) -> str:
    """Converte ``vila-esplanada`` em ``Vila Esplanada`` (capitalização simples)."""
    s = str(slug_or_text or "").replace("-", " ").strip()
    if not s:
        return ""
    return " ".join(p.capitalize() for p in s.split() if p)[:80]


# -----------------------------------------------------------------------------
# Decisão final de bairro do anúncio
# -----------------------------------------------------------------------------

def inferir_bairro_anuncio(
    *,
    bairro_card: str = "",
    titulo: str = "",
    url: str = "",
    bairro_leilao: str = "",
    cidade_leilao: str = "",
) -> Tuple[str, str]:
    """Decide qual bairro gravar para o anúncio + a origem da decisão.

    Política (em ordem de prioridade):

    1. ``bairro_card`` (vindo do extrator) — depois de saneado.
    2. Bairro inferido da URL.
    3. Bairro inferido do título.
    4. Vazio.

    **Salvaguarda crítica:** se o bairro derivado coincide com o do leilão e
    não há *evidência independente* (URL ou título mencionando um bairro
    distinto), devolve vazio + origem ``vazio_para_evitar_heranca``. Isso
    impede que ads sem bairro real "herdem" o bairro do leilão e contaminem
    o cache (bug histórico de Aparecida/Taubaté).

    Returns:
        Tupla ``(bairro_final, origem)`` onde ``origem`` é uma das:
        ``"card"``, ``"url"``, ``"titulo"``, ``"vazio"``,
        ``"vazio_para_evitar_heranca"``.
    """
    bairro_leilao_norm = _slug_fold(bairro_leilao)

    cand_card = sanear_bairro(bairro_card)
    cand_url = sanear_bairro(_bairro_inferido_da_url(url, cidade_leilao))
    cand_titulo = sanear_bairro(_bairro_inferido_do_titulo(titulo))

    # Se o card já trouxe algo confiável (e não é o mesmo do leilão sem suporte
    # independente), respeitamos.
    if cand_card:
        if not bairro_leilao_norm or _slug_fold(cand_card) != bairro_leilao_norm:
            return cand_card, "card"
        # Card == leilão: só aceitamos se URL ou título reforçam.
        if cand_url and _slug_fold(cand_url) == _slug_fold(cand_card):
            return cand_card, "card"
        if cand_titulo and _slug_fold(cand_titulo) == _slug_fold(cand_card):
            return cand_card, "card"
        # Sem evidência independente — preferimos vazio a herdar.
        return "", "vazio_para_evitar_heranca"

    if cand_url:
        return cand_url, "url"
    if cand_titulo:
        return cand_titulo, "titulo"
    return "", "vazio"


# -----------------------------------------------------------------------------
# Detecção de empreendimento / condomínio
# -----------------------------------------------------------------------------

def normalizar_nome_empreendimento(v: Any) -> str:
    """Limpa um nome candidato a empreendimento (Condomínio Foo → Foo)."""
    s = " ".join(str(v or "").strip().split())
    if not s:
        return ""
    s = re.sub(r"(?i)^(condom[ií]nio|edif[ií]cio|pr[eé]dio)\s+", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip(" -,:;.")
    return s[:160]


def texto_eh_boilerplate_condominio(s: Any) -> bool:
    """Detecta texto jurídico/comercial de edital que **não** é evidência de condomínio.

    Caso típico: edital Caixa traz "Condomínio: Sob responsabilidade do comprador,
    até o limite de 10% em relação ao valor de avaliação do imóvel..." — frase
    obrigatória que vem em **todos** os editais Caixa, mesmo para casas isoladas.
    """
    t = str(s or "").lower()
    if not t:
        return False
    return any(termo in t for termo in _BOILERPLATE_CONDOMINIO_TERMOS)


def nome_empreendimento_valido(v: Any) -> str:
    """Aceita um nome candidato apenas se passar nos filtros de boilerplate."""
    s = normalizar_nome_empreendimento(v)
    if not s or len(s) < 4:
        return ""
    if texto_eh_boilerplate_condominio(s):
        return ""
    if _slug_fold(s) in {_slug_fold(x) for x in _NOMES_EMPREENDIMENTO_INVALIDOS}:
        return ""
    return s


def _candidato_empreendimento_textual_aceitavel(s: str) -> bool:
    """Filtro extra para candidatos extraídos de texto livre.

    Quando o candidato não veio de uma chave estruturada (ex.: foi extraído
    de uma linha de ``observacoes_markdown`` após "condomínio:" ou
    "edifício:"), exigimos qualidade extra para evitar capturar frases
    administrativas como "IPTU pago" ou "sob responsabilidade".

    Critério: pelo menos 2 tokens com 3+ chars cada, e a forma normalizada
    não pode ser uma stopword administrativa conhecida.
    """
    if not s:
        return False
    tokens = [t for t in re.split(r"\s+", s.strip()) if len(t) >= 3]
    if len(tokens) < 2:
        return False
    norm = _slug_fold(s)
    stopwords_administrativas = {
        "iptu",
        "iptu-pago",
        "iptu-incluso",
        "incluso-iptu",
        "valor-incluso",
        "valor-de-avaliacao",
        "responsabilidade-do-comprador",
        "regras-pagamento",
    }
    return norm not in stopwords_administrativas


def nome_empreendimento_leilao(leilao: dict[str, Any]) -> str:
    """Extrai o nome do condomínio/edifício/empreendimento do leilão, se houver.

    Procura em ordem:

    1. Campos estruturados no ``leilao_extra_json`` (``nome_condominio``,
       ``condominio``, ``nome_predio``, ``predio``, ``nome_edificio``,
       ``edificio``, ``nome_empreendimento``, ``empreendimento``).
    2. Campos top-level com os mesmos nomes.
    3. Linhas em ``observacoes_markdown``, ``endereco`` e ``descricao`` que
       contenham "Condomínio/Edifício/Prédio" — só aceita se o resto da linha
       não for boilerplate.
    """
    if not isinstance(leilao, dict):
        return ""
    extra = _parse_extra(leilao)
    chaves = (
        "nome_condominio",
        "condominio",
        "nome_predio",
        "predio",
        "nome_edificio",
        "edificio",
        "nome_empreendimento",
        "empreendimento",
    )
    for k in chaves:
        v = nome_empreendimento_valido(extra.get(k) or leilao.get(k))
        if v:
            return v

    textos = [
        str(extra.get("observacoes_markdown") or "").strip(),
        str(leilao.get("endereco") or "").strip(),
        str(leilao.get("descricao") or "").strip(),
    ]
    for obs in textos:
        if not obs:
            continue
        for ln in obs.splitlines():
            s = " ".join(str(ln or "").strip().split())
            if len(s) < 8:
                continue
            if not re.search(r"(?i)\b(condom[ií]nio|edif[ií]cio|pr[eé]dio)\b", s):
                continue
            m = re.search(
                r"(?i)\b(condom[ií]nio|edif[ií]cio|pr[eé]dio)\b\s*[:\-]?\s*(.+)$",
                s,
            )
            cand = nome_empreendimento_valido((m.group(2) if m else s) or s)
            if cand and _candidato_empreendimento_textual_aceitavel(cand):
                return cand
    return ""


def leilao_indica_condominio(leilao: dict[str, Any]) -> bool:
    """Indica se o edital realmente sinaliza um condomínio residencial.

    Diferente do anterior, esta função aplica salvaguardas:

    - se o texto contém boilerplate Caixa, retorna ``False`` mesmo com a
      palavra "condomínio" presente;
    - se há ``Condomínio:`` (key/value tipo cobrança), retorna ``False``;
    - aceita quando há nome de empreendimento extraído OU quando a frase
      contém um indicador positivo claro
      (``condomínio residencial``, ``casa em condomínio``, etc.).
    """
    if not isinstance(leilao, dict):
        return False

    if nome_empreendimento_leilao(leilao):
        return True

    extra = _parse_extra(leilao)
    blob = " ".join(
        str(x or "")
        for x in (
            leilao.get("endereco"),
            leilao.get("descricao"),
            extra.get("observacoes_markdown"),
            extra.get("edital_resumo"),
            leilao.get("edital_markdown"),
        )
    )
    if not blob.strip():
        return False
    if texto_eh_boilerplate_condominio(blob):
        return False
    if _RE_CONDOMINIO_KEY_VALUE_NEGATIVO.search(blob):
        return False
    return bool(_RE_CONDOMINIO_POSITIVO.search(blob))


def anuncio_match_empreendimento(
    anuncio: dict[str, Any],
    nome_empreendimento_ref: str,
) -> bool:
    """Verifica se um anúncio menciona o mesmo empreendimento que o leilão.

    Olha em ``titulo``, ``url_anuncio``, ``logradouro``, ``bairro`` e
    nos metadados (``nome_empreendimento``, ``condominio``,
    ``nome_condominio``).

    Match por inclusão de string normalizada **OU** por hit de pelo menos 2
    tokens (excluindo genéricos como "condomínio", "residencial", "vila").
    """
    if not isinstance(anuncio, dict):
        return False
    ref = normalizar_nome_empreendimento(nome_empreendimento_ref)
    if not ref:
        return False
    ref_n = _texto_normalizado_match(ref)
    if len(ref_n) < 6:
        return False

    md_raw = anuncio.get("metadados_json") or anuncio.get("metadados") or {}
    if isinstance(md_raw, str):
        try:
            md = json.loads(md_raw)
        except json.JSONDecodeError:
            md = {}
    elif isinstance(md_raw, dict):
        md = md_raw
    else:
        md = {}

    blobs = [
        anuncio.get("titulo"),
        anuncio.get("url_anuncio"),
        anuncio.get("logradouro"),
        anuncio.get("bairro"),
        md.get("nome_empreendimento") if isinstance(md, dict) else None,
        md.get("condominio") if isinstance(md, dict) else None,
        md.get("nome_condominio") if isinstance(md, dict) else None,
    ]
    txt = _texto_normalizado_match(" ".join(str(x or "") for x in blobs))
    if not txt:
        return False
    if ref_n in txt:
        return True
    toks = [
        _texto_normalizado_match(t)
        for t in re.split(r"\s+", ref)
        if _texto_normalizado_match(t)
        and _texto_normalizado_match(t) not in _TOKENS_GENERICOS_EMPREENDIMENTO
    ]
    if not toks:
        return False
    hits = sum(1 for t in toks if t and t in txt)
    return hits >= min(2, len(toks))


# -----------------------------------------------------------------------------
# Decisão final de tipo do imóvel para gravação no anúncio
# -----------------------------------------------------------------------------

# Indicadores textuais no próprio anúncio que sinalizam condomínio fechado.
# Aceita espaço, hífen ou underscore como separador (URLs usam "-").
_RE_ANUNCIO_INDICA_CONDOMINIO = re.compile(
    r"(?ix)("
    r"condom[ií]nio[\s\-_]+(?:residencial|fechado|[a-zà-ÿ]{3,})"
    r"|casa[\s\-_]+em[\s\-_]+condom[ií]nio"
    r"|cond\.[\s\-_]+residencial"
    r")"
)

# Palavras que, quando aparecem nos ~25 chars antes de "condomínio", indicam
# contexto financeiro/cobrança ("taxa de condomínio", "valor do condomínio",
# "paga condomínio", "incluso condomínio") — NÃO promovem o tipo.
_RE_CONTEXTO_FINANCEIRO_CONDOMINIO = re.compile(
    r"(?i)\b(taxa|valor|paga(?:r|m|do)?|inclus[oa]s?|inclu[ií]d[oa]s?"
    r"|isento|cobran[çc]a|m[eê]s|mensal)\b[^.]{0,25}condom[ií]nio"
)


def anuncio_indica_condominio(*, titulo: str = "", url: str = "") -> bool:
    """True se o próprio anúncio (título/URL) sinaliza condomínio.

    Conservador: precisa de um indicador **positivo** (não basta a palavra
    "condomínio" sozinha — pode ser "taxa de condomínio"). Aceita hífen e
    underscore como separador (padrão de URL).

    Salvaguarda: se "condomínio" aparece em contexto financeiro (taxa, valor,
    pagamento, mensal, incluso), considera-se cobrança e não evidência de
    condomínio residencial real.
    """
    blob = f"{titulo or ''} {url or ''}"
    if _RE_CONTEXTO_FINANCEIRO_CONDOMINIO.search(blob):
        return False
    return bool(_RE_ANUNCIO_INDICA_CONDOMINIO.search(blob))


def decidir_tipo_imovel_anuncio(
    *,
    tipo_leilao: str,
    titulo: str = "",
    url: str = "",
    leilao_indica_condominio_flag: Optional[bool] = None,
) -> str:
    """Decide o ``tipo_imovel`` final a gravar para o anúncio.

    Regra: o tipo segue o do leilão (comparáveis fazem sentido entre tipos
    iguais). **Promove** ``casa`` para ``casa_condominio`` quando há
    evidência de condomínio — ou no leilão (``leilao_indica_condominio_flag``)
    ou no próprio anúncio (título/URL). Nunca rebaixa
    ``casa_condominio`` para ``casa``.

    Args:
        tipo_leilao: tipo canónico do leilão alvo.
        titulo: título do anúncio (markdown extraído).
        url: URL do anúncio.
        leilao_indica_condominio_flag: opcional — resultado pré-calculado de
            :func:`leilao_indica_condominio`. Quando ``None``, só usa
            evidência do anúncio.

    Returns:
        Tipo canónico final, em minúsculas.
    """
    base = (tipo_leilao or "desconhecido").strip().lower()
    if base == "casa":
        cond_leilao = bool(leilao_indica_condominio_flag)
        cond_anuncio = anuncio_indica_condominio(titulo=titulo, url=url)
        if cond_leilao or cond_anuncio:
            return "casa_condominio"
    return base


# -----------------------------------------------------------------------------
# Inferência de cidade da URL (defesa anti-contaminação cross-cidade)
# -----------------------------------------------------------------------------

def cidade_inferida_da_url(url: str) -> str:
    """Tenta extrair o slug da cidade a partir do path da URL.

    Suporta os padrões mais comuns dos portais brasileiros (Viva Real, Zap,
    Imovelweb, OLX). Devolve ``""`` quando não consegue inferir com
    confiança.

    Estratégia: usa **lazy match** após o separador de UF e corta no primeiro
    marcador conhecido (``com-``, ``venda-``, ``aluguel-``, ``\\d+m2``,
    ``rs\\d``, ``id-``). Cidades brasileiras nunca contêm dígitos no slug
    (excluímos para evitar contaminação por sufixos tipo ``150m2``).
    """
    u = str(url or "").strip().lower()
    if not u:
        return ""
    uf_pat = "|".join(_UF_SLUGS)
    m_uf = re.search(
        rf"-(?:{uf_pat})-(?P<cidade>[a-z]+(?:-[a-z]+){{0,5}}?)-"
        r"(?=com-|venda-|aluguel-|lancamento-|\d+m2|rs\d|id-)",
        u,
        flags=re.IGNORECASE,
    )
    if m_uf:
        return _slug_fold(m_uf.group("cidade"))
    candidatos = list(
        re.finditer(
            r"-(?P<cidade>[a-z]+(?:-[a-z]+){0,5}?)-(?:com-)?\d+m2-(?:venda|aluguel)-",
            u,
            flags=re.IGNORECASE,
        )
    )
    if candidatos:
        return _slug_fold(candidatos[-1].group("cidade"))
    return ""


def url_indica_cidade_diferente(url: str, cidade_alvo: str) -> bool:
    """True se a URL pertence claramente a outra cidade que não a alvo.

    Conservador: só devolve ``True`` quando consegue inferir a cidade da URL
    **e** essa cidade difere da alvo (sem ser sufixo/prefixo). URL sem
    inferência possível devolve ``False`` (não descarta por dúvida).
    """
    alvo = _slug_fold(cidade_alvo)
    if not alvo:
        return False
    inferida = cidade_inferida_da_url(url)
    if not inferida:
        return False
    if inferida == alvo:
        return False
    if inferida.endswith(f"-{alvo}") or alvo.endswith(f"-{inferida}"):
        return False
    return True


__all__ = [
    "anuncio_indica_condominio",
    "anuncio_match_empreendimento",
    "cidade_inferida_da_url",
    "decidir_tipo_imovel_anuncio",
    "inferir_bairro_anuncio",
    "leilao_indica_condominio",
    "nome_empreendimento_leilao",
    "nome_empreendimento_valido",
    "normalizar_nome_empreendimento",
    "sanear_bairro",
    "texto_eh_boilerplate_condominio",
    "url_indica_cidade_diferente",
]
