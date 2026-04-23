from __future__ import annotations

from leilao_ia_v2.services.markdown_foto_imovel import extrair_url_foto_imovel_markdown


def test_extrai_markdown_imagem_absoluta():
    md = "Intro\n\n![Fachada](https://cdn.exemplo.com/imoveis/123/foto.jpg)\n\nMais texto"
    u = extrair_url_foto_imovel_markdown(md, "https://leiloeiro.com/lote/1")
    assert u == "https://cdn.exemplo.com/imoveis/123/foto.jpg"


def test_resolve_url_relativa():
    md = "![](/static/media/prop/abc.png)"
    u = extrair_url_foto_imovel_markdown(md, "https://site.com/leilao/item")
    assert u == "https://site.com/static/media/prop/abc.png"


def test_rejeita_favicon():
    md = "![](https://site.com/favicon.ico) e ![](https://site.com/casa.png)"
    u = extrair_url_foto_imovel_markdown(md, "https://site.com/")
    assert u == "https://site.com/casa.png"


def test_img_tag_html():
    md = '<p><img src="https://img.cdn/x.webp" alt="x"></p>'
    u = extrair_url_foto_imovel_markdown(md, "")
    assert u == "https://img.cdn/x.webp"


def test_prioriza_imagem_1_quando_vem_depois_no_markdown():
    """Ordem Zuk: 15–17 antes de 1–14 no markdown; deve escolher Imagem 1."""
    md = (
        "![Imagem 15 do Leilão de Apartamento](https://imagens.zuk/detalhe/15.webp)\n"
        "![Imagem 1 do Leilão de Apartamento](https://imagens.zuk/detalhe/01.webp)\n"
    )
    u = extrair_url_foto_imovel_markdown(md, "https://www.portalzuk.com.br/imovel/x")
    assert u == "https://imagens.zuk/detalhe/01.webp"


def test_imagem_15_nao_confunde_com_imagem_1():
    md = "![Imagem 15 do Leilão](https://imagens.zuk/detalhe/15.webp)\n"
    u = extrair_url_foto_imovel_markdown(md, "https://site.com/")
    assert u == "https://imagens.zuk/detalhe/15.webp"


def test_foto_1_em_portugues_alternativo():
    md = "![Foto 1 — sala](https://cdn.exemplo/a.jpg)\n![Outra](https://cdn.exemplo/b.jpg)\n"
    u = extrair_url_foto_imovel_markdown(md, "")
    assert u == "https://cdn.exemplo/a.jpg"
