"""Exceções de fluxo da ingestão v2."""


class UrlInvalidaIngestaoError(Exception):
    """Scrape vazio, erro de rede ou URL sem conteúdo utilizável — não gravar no banco."""


class IngestaoSemConteudoEditalError(UrlInvalidaIngestaoError):
    """
    Markdown obtido mas insuficiente para tratar como edital (ex.: site genérico tipo hotel).
    Não chama LLM nem grava no banco — o frontend deve orientar abortar ou cadastro manual.
    """

    def __init__(
        self,
        motivo: str,
        *,
        diagnostico: object | None = None,
    ):
        self.motivo = motivo
        self.diagnostico = diagnostico
        super().__init__(motivo)


class EscolhaSobreDuplicataNecessaria(Exception):
    """URL já existe; o chamador deve perguntar ao usuário e chamar de novo com decisão explícita."""

    def __init__(self, registro_existente: dict, mensagem: str = "URL já cadastrada."):
        self.registro_existente = registro_existente
        super().__init__(mensagem)
