from __future__ import annotations

from leilao_ia_v2.services.edital_markdown_limpeza import limpar_edital_markdown_ruido


def test_remove_veja_tambem_ate_fim():
    md = (
        "# Lote\n\nDescrição do imóvel em Bertioga.\n\n"
        "## Veja também\n\n[![Outro](url)](x)\n\nFooter"
    )
    r = limpar_edital_markdown_ruido(md)
    assert "Veja também" not in r.texto
    assert "Descrição do imóvel" in r.texto
    assert "veja_tambem" in r.cortes_aplicados
    assert r.removidos_caracteres > 0


def test_remove_newsletter_google_forms():
    md = "Conteúdo útil\n\n[Newsletter](https://docs.google.com/forms/abc)\n\nFim"
    r = limpar_edital_markdown_ruido(md)
    assert "docs.google.com/forms" not in r.texto
    assert "Conteúdo útil" in r.texto
    assert "google_forms_newsletter" in r.cortes_aplicados


def test_remove_proximidades():
    md = "Topo\n\n### Proximidades\n\nBlog litoral\n\n## Fim fake"
    r = limpar_edital_markdown_ruido(md)
    assert "Proximidades" not in r.texto
    assert "Topo" in r.texto


def test_sem_corte_quando_nao_ha_ancoras():
    md = "Só o edital\n\nSem seções de rodapé conhecidas."
    r = limpar_edital_markdown_ruido(md)
    assert r.texto.strip() == md.strip()
    assert r.cortes_aplicados == []


def test_primeiro_ancora_vence():
    md = (
        "A\n## Veja também\nB\n## Newsletter\nC"
    )
    r = limpar_edital_markdown_ruido(md)
    assert r.texto.strip() == "A"
    assert r.cortes_aplicados == ["veja_tambem"]
