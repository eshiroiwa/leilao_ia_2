from __future__ import annotations

from pathlib import Path

from leilao_ia_v2.pipeline import ingestao_lote_csv as mod


def test_ler_registros_csv_detecta_header_com_linhas_extras(tmp_path: Path):
    p = tmp_path / "lista.csv"
    p.write_text(
        "\n".join(
            [
                ";;;;",
                "Lista de Imoveis;;;;",
                "Link de acesso;Cidade;UF;Bairro;Endereco;Preco",
                "https://site.exemplo/imovel/1;Sao Paulo;SP;Centro;Rua A;120000",
            ]
        ),
        encoding="utf-8",
    )
    rows = mod.ler_registros_csv_leiloes(p)
    assert len(rows) == 1
    assert rows[0].get("link de acesso") == "https://site.exemplo/imovel/1"
    assert rows[0].get("cidade") == "Sao Paulo"


def test_ler_registros_csv_coluna_link_de_acesso_caixa(tmp_path: Path):
    p = tmp_path / "lista_caixa.csv"
    p.write_text(
        "\n".join(
            [
                " Lista de Imoveis da Caixa;;Data de geracao:;22/04/26;;;;;;;;",
                " N° do imovel;UF;Cidade;Bairro;Endereco;Preco;Valor de avaliacao;;Financiamento;Descricao;Modalidade de venda;Link de acesso",
                "123;SP;SAO PAULO;CENTRO;RUA A, N. 1;100000;200000;50;Nao;Apartamento;Leilao;https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnimovel=123",
            ]
        ),
        encoding="cp1252",
    )
    rows = mod.ler_registros_csv_leiloes(p)
    assert len(rows) == 1
    assert mod._col_url(rows[0]).startswith("https://venda-imoveis.caixa.gov.br/")


def test_processar_lote_csv_continua_quando_uma_linha_falha(monkeypatch):
    monkeypatch.setattr(
        mod,
        "ler_registros_csv_leiloes",
        lambda _p: [
            {"url": "https://ok.exemplo/1", "cidade": "Sao Paulo", "uf": "SP"},
            {"url": "https://falha.exemplo/2", "cidade": "Sao Paulo", "uf": "SP"},
        ],
    )
    monkeypatch.setattr(mod, "_upsert_leilao_por_csv", lambda payload, _cli: (payload["url_leilao"][-1], "inserido"))

    class _Res:
        def __init__(self, ok: bool, msg: str):
            self.ok = ok
            self.mensagem = msg
            self.firecrawl_chamadas_api = 1 if ok else 0

    def _resolver(_cli, lid, **_kw):
        if lid == "2":
            raise RuntimeError("erro simulacao")
        return _Res(True, "cache ok")

    monkeypatch.setattr(mod, "resolver_cache_media_pos_ingestao", _resolver)
    r = mod.processar_lote_csv_leiloes("x.csv", client=object())  # type: ignore[arg-type]
    assert r.processados == 2
    assert r.ok == 1
    assert r.erro == 1
    assert [x.status for x in r.resultados] == ["ok", "erro"]


def test_resultado_lote_csv_para_dict():
    r = mod.ResultadoLoteCsv(
        arquivo="/tmp/l.csv",
        total_linhas_csv=2,
        total_urls_validas=1,
        processados=1,
        ok=1,
        erro=0,
        ignorados=1,
        resultados=[mod.LinhaLoteResultado(linha=3, url="https://x", status="ok", leilao_id="abc")],
    )
    d = mod.resultado_lote_csv_para_dict(r)
    assert d["arquivo"] == "/tmp/l.csv"
    assert d["ok_itens"] == 1
    assert isinstance(d["resultados"], list)
    assert d["resultados"][0]["leilao_id"] == "abc"


def test_processar_lote_csv_dispara_progress_hook(monkeypatch):
    monkeypatch.setattr(
        mod,
        "ler_registros_csv_leiloes",
        lambda _p: [{"url": "https://ok.exemplo/1", "cidade": "Sao Paulo", "uf": "SP"}],
    )
    monkeypatch.setattr(mod, "_upsert_leilao_por_csv", lambda payload, _cli: ("id1", "inserido"))

    class _Res:
        ok = True
        mensagem = "ok"
        firecrawl_chamadas_api = 0

    monkeypatch.setattr(mod, "resolver_cache_media_pos_ingestao", lambda *_a, **_k: _Res())
    calls: list[tuple[int, int, str]] = []
    r = mod.processar_lote_csv_leiloes(
        "x.csv",
        client=object(),  # type: ignore[arg-type]
        progress_hook=lambda p, t, s: calls.append((p, t, s)),
    )
    assert r.ok == 1
    assert calls
    assert calls[-1][0] == 1
    assert calls[-1][2] == "ok"


def test_processar_lote_csv_respeita_should_stop(monkeypatch):
    monkeypatch.setattr(
        mod,
        "ler_registros_csv_leiloes",
        lambda _p: [
            {"url": "https://ok.exemplo/1", "cidade": "Sao Paulo", "uf": "SP"},
            {"url": "https://ok.exemplo/2", "cidade": "Sao Paulo", "uf": "SP"},
        ],
    )
    monkeypatch.setattr(mod, "_upsert_leilao_por_csv", lambda payload, _cli: (payload["url_leilao"][-1], "inserido"))

    class _Res:
        ok = True
        mensagem = "ok"
        firecrawl_chamadas_api = 0

    monkeypatch.setattr(mod, "resolver_cache_media_pos_ingestao", lambda *_a, **_k: _Res())
    calls = {"n": 0}

    def _stop() -> bool:
        return calls["n"] >= 1

    def _hook(_p: int, _t: int, _s: str) -> None:
        calls["n"] += 1

    r = mod.processar_lote_csv_leiloes(
        "x.csv",
        client=object(),  # type: ignore[arg-type]
        progress_hook=_hook,
        should_stop=_stop,
    )
    assert r.cancelado is True
    assert r.processados == 1


def test_resumir_csv_leiloes_retorna_preview_e_contagens(tmp_path: Path):
    p = tmp_path / "lista.csv"
    p.write_text(
        "\n".join(
            [
                "Link de acesso;Cidade;UF",
                "https://site.exemplo/imovel/1;Sao Paulo;SP",
                ";Sao Paulo;SP",
                "https://site.exemplo/imovel/2;Santos;SP",
            ]
        ),
        encoding="utf-8",
    )
    r = mod.resumir_csv_leiloes(p, preview_limite=1)
    assert r.total_linhas_csv == 3
    assert r.total_urls_validas == 2
    assert r.total_sem_url == 1
    assert len(r.preview) == 1
    assert r.preview[0]["url"] == "https://site.exemplo/imovel/1"


def test_processar_lote_csv_preenche_lat_lon_com_geocode(monkeypatch):
    monkeypatch.setattr(
        mod,
        "ler_registros_csv_leiloes",
        lambda _p: [
            {
                "url": "https://ok.exemplo/1",
                "cidade": "Sao Paulo",
                "uf": "SP",
                "bairro": "Centro",
                "endereco": "Rua A, 100",
            }
        ],
    )
    monkeypatch.setattr(mod, "geocodificar_endereco", lambda **_k: (-23.55, -46.63))
    seen: dict[str, object] = {}

    def _upsert(payload, _cli):
        seen["payload"] = dict(payload)
        return "id1", "inserido"

    monkeypatch.setattr(mod, "_upsert_leilao_por_csv", _upsert)

    class _Res:
        ok = True
        mensagem = "ok"
        firecrawl_chamadas_api = 0

    monkeypatch.setattr(mod, "resolver_cache_media_pos_ingestao", lambda *_a, **_k: _Res())
    r = mod.processar_lote_csv_leiloes("x.csv", client=object())  # type: ignore[arg-type]
    assert r.ok == 1
    p = seen.get("payload")
    assert isinstance(p, dict)
    assert p.get("latitude") == -23.55
    assert p.get("longitude") == -46.63


def test_payload_csv_mapeia_campos_variaveis_lances_e_foto():
    reg = {
        "link de acesso": "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnimovel=123",
        "cidade": "Aparecida",
        "uf": "SP",
        "bairro": "Ponte Alta",
        "endereco": "Rua Exemplo, 100",
        "1º leilao": "420.000,00",
        "2º leilao": "310.000,00",
        "link_foto": "https://cdn.exemplo.com/foto.jpg",
        "valor de avaliacao": "610.000,00",
    }
    mp = mod.resolver_mapeamento_campos_csv([reg])
    p = mod._payload_de_registro_csv(reg, url=mod._col_url(reg, mapeamento=mp), mapeamento=mp)
    assert p["valor_lance_1_praca"] == 420000.0
    assert p["valor_lance_2_praca"] == 310000.0
    assert p["valor_arrematacao"] == 310000.0
    assert p["url_foto_imovel"] == "https://cdn.exemplo.com/foto.jpg"
    assert p["valor_avaliacao"] == 610000.0


def test_url_foto_preserva_fragmento_preview():
    reg = {
        "link de acesso": "https://site.exemplo/lote/1",
        "cidade": "Aparecida",
        "uf": "SP",
        "link da foto": "https://site.exemplo/lote/1/#preview",
    }
    mp = mod.resolver_mapeamento_campos_csv([reg], permitir_llm=False)
    p = mod._payload_de_registro_csv(reg, url=mod._col_url(reg, mapeamento=mp), mapeamento=mp)
    assert p["url_foto_imovel"] == "https://site.exemplo/lote/1/#preview"


# -----------------------------------------------------------------------------
# B4 — pre-check de BD + cap por item
# -----------------------------------------------------------------------------

class TestCsvPreCheckBd:
    def test_resolver_cap_item_zera_quando_bd_tem_amostras_suficientes(
        self, monkeypatch
    ):
        """Quando o BD já tem >= MIN anúncios para a tupla, cap=0."""
        anuncios_fake = [{"id": str(i)} for i in range(mod.CSV_BATCH_BD_PRE_CHECK_MIN + 2)]
        monkeypatch.setattr(
            mod.anuncios_mercado_repo,
            "listar_por_cidade_estado_tipos",
            lambda _cli, **_kw: anuncios_fake,
        )
        cache: dict = {}
        cap = mod._resolver_cap_item_csv(
            object(),
            {"cidade": "Aparecida", "estado": "SP", "tipo_imovel": "casa"},
            cache,
            cap_default=5,
        )
        assert cap == 0
        # Cache populado para reutilização posterior.
        assert ("aparecida", "SP", "casa") in cache

    def test_resolver_cap_item_usa_default_quando_bd_pobre(self, monkeypatch):
        """BD com poucos anúncios → usa cap default (vai chamar Firecrawl)."""
        monkeypatch.setattr(
            mod.anuncios_mercado_repo,
            "listar_por_cidade_estado_tipos",
            lambda _cli, **_kw: [{"id": "1"}],  # 1 < MIN
        )
        cap = mod._resolver_cap_item_csv(
            object(),
            {"cidade": "Aparecida", "estado": "SP", "tipo_imovel": "casa"},
            {},
            cap_default=5,
        )
        assert cap == 5

    def test_resolver_cap_item_reusa_cache_local(self, monkeypatch):
        """Linhas seguintes da mesma tupla NÃO repetem o SELECT."""
        n_chamadas = {"n": 0}

        def _listar(_cli, **_kw):
            n_chamadas["n"] += 1
            return [{"id": str(i)} for i in range(mod.CSV_BATCH_BD_PRE_CHECK_MIN + 1)]

        monkeypatch.setattr(
            mod.anuncios_mercado_repo,
            "listar_por_cidade_estado_tipos",
            _listar,
        )
        cache: dict = {}
        payload = {"cidade": "Aparecida", "estado": "SP", "tipo_imovel": "casa"}
        for _ in range(5):
            mod._resolver_cap_item_csv(object(), payload, cache, cap_default=5)
        assert n_chamadas["n"] == 1

    def test_resolver_cap_item_falha_select_usa_default(self, monkeypatch):
        """Se a query do BD falhar, não trava: usa cap default."""
        def _falha(*_a, **_kw):
            raise RuntimeError("boom")

        monkeypatch.setattr(
            mod.anuncios_mercado_repo,
            "listar_por_cidade_estado_tipos",
            _falha,
        )
        cap = mod._resolver_cap_item_csv(
            object(),
            {"cidade": "Aparecida", "estado": "SP", "tipo_imovel": "casa"},
            {},
            cap_default=5,
        )
        assert cap == 5

    def test_resolver_cap_item_payload_incompleto_usa_default(self):
        """Sem cidade/estado/tipo, devolve default sem consultar BD."""
        cap = mod._resolver_cap_item_csv(
            object(),
            {"cidade": "", "estado": "SP", "tipo_imovel": "casa"},
            {},
            cap_default=7,
        )
        assert cap == 7

    def test_processar_lote_aplica_cap_default_conservador(self, monkeypatch):
        """Sem max_chamadas_api_firecrawl_por_item explícito, usa default 5."""
        monkeypatch.setattr(
            mod,
            "ler_registros_csv_leiloes",
            lambda _p: [{"url": "https://x/1", "cidade": "X", "uf": "SP", "tipo_imovel": "casa"}],
        )
        monkeypatch.setattr(
            mod,
            "_upsert_leilao_por_csv",
            lambda payload, _cli: ("id1", "inserido"),
        )
        # BD pobre — para garantir que o cap default seja usado.
        monkeypatch.setattr(
            mod.anuncios_mercado_repo,
            "listar_por_cidade_estado_tipos",
            lambda _cli, **_kw: [],
        )

        caps_capturados: list[int] = []

        class _Res:
            ok = True
            mensagem = "ok"
            firecrawl_chamadas_api = 0

        def _resolver(_cli, _lid, **kw):
            caps_capturados.append(int(kw.get("max_chamadas_api_firecrawl") or -1))
            return _Res()

        monkeypatch.setattr(mod, "resolver_cache_media_pos_ingestao", _resolver)
        mod.processar_lote_csv_leiloes("x.csv", client=object())  # type: ignore[arg-type]
        assert caps_capturados == [mod.CSV_BATCH_FIRECRAWL_DEFAULT_PER_ITEM]
