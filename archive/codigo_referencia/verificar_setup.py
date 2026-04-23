#!/usr/bin/env python3
"""
Verificação rápida do ambiente leilao-ia (dependências, .env, Supabase, imports).

Uso (na pasta do projeto):
    python verificar_setup.py
    python verificar_setup.py --com-playwright   # testa um GET leve com Chromium (mais lento)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys

# Mesmo critério do supabase-py 2.3.x: só aceita JWT (anon / service_role).
_SUPABASE_JWT_RE = re.compile(
    r"^[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*$"
)


def _jwt_role_from_key(key: str) -> str | None:
    try:
        parts = (key or "").strip().split(".")
        if len(parts) < 2:
            return None
        b64 = parts[1]
        pad = "=" * (-len(b64) % 4)
        data = json.loads(base64.urlsafe_b64decode(b64 + pad).decode("utf-8"))
        r = data.get("role")
        return str(r) if r is not None else None
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--com-playwright",
        action="store_true",
        help="Abre Chromium headless uma vez (útil se a ingestão falhar por browser)",
    )
    args = parser.parse_args()

    print("=== 1. Variáveis de ambiente (.env) ===")
    from dotenv import load_dotenv

    load_dotenv()
    ok = True
    for key in ("SUPABASE_URL", "OPENAI_API_KEY"):
        v = os.getenv(key)
        if v:
            print(f"  [ok] {key} definido ({len(v)} caracteres)")
        else:
            print(f"  [falta] {key}")
            ok = False
    sb_service = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    sb_key = sb_service or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if sb_key:
        if sb_service:
            print(
                f"  [ok] SUPABASE_SERVICE_ROLE_KEY definido ({len(sb_service)} caracteres) — "
                "pipeline usa esta chave (ignora RLS)"
            )
        else:
            print(
                f"  [ok] SUPABASE_KEY ou SUPABASE_ANON_KEY definido ({len(sb_key)} caracteres)"
            )
        jr = _jwt_role_from_key(sb_key)
        if jr:
            print(f"  [info] JWT role desta chave: {jr}")
        if jr == "anon" and not sb_service:
            print(
                "  [aviso] Com RLS ativo, INSERT falha (42501). "
                "Adicione SUPABASE_SERVICE_ROLE_KEY no .env (painel → API → service_role secret)."
            )
        if not _SUPABASE_JWT_RE.match(sb_key.strip()):
            print(
                "  [aviso] supabase-py 2.3.x exige JWT (anon ou service_role, costuma "
                "começar com eyJ...). A chave publishable (sb_publishable_...) não serve."
            )
            print(
                "         No painel: Project Settings → API → copie anon public (legacy) "
                "ou service_role (só backend seguro)."
            )
    else:
        print("  [falta] SUPABASE_SERVICE_ROLE_KEY ou SUPABASE_KEY ou SUPABASE_ANON_KEY")
        ok = False
    if not ok:
        print("\nAjuste o arquivo .env na raiz do projeto e rode de novo.")
        return 1

    print("\n=== 2. Imports dos módulos ===")
    try:
        import anuncios_mercado  # noqa: F401
        import ingestion_agent  # noqa: F401
        import valuation_agent  # noqa: F401
        import financial_agent  # noqa: F401
        import token_efficiency  # noqa: F401
        import pricing_pipeline  # noqa: F401
        import leilao_constants  # noqa: F401
        print("  [ok] Todos os módulos importaram sem erro")
    except Exception as e:
        print(f"  [erro] {e}")
        return 1

    print("\n=== 3. Supabase: leitura em leilao_imoveis ===")
    try:
        from ingestion_agent import SUPABASE_TABLE, get_supabase_client

        cli = get_supabase_client()
        resp = cli.table(SUPABASE_TABLE).select("id,url_leilao,status").limit(3).execute()
        rows = getattr(resp, "data", None) or []
        print(f"  [ok] Conectou. Amostra: {len(rows)} linha(s) (máx. 3)")
        for r in rows:
            print(f"       - id={r.get('id')} status={r.get('status')} url={str(r.get('url_leilao'))[:60]}...")
    except Exception as e:
        print(f"  [erro] {e}")
        print("  Confira URL/key, RLS (políticas) e se a tabela existe.")
        if str(e).strip() == "Invalid API key":
            sk = (
                os.getenv("SUPABASE_SERVICE_ROLE_KEY")
                or os.getenv("SUPABASE_KEY")
                or os.getenv("SUPABASE_ANON_KEY")
                or ""
            )
            if sk.strip() and not _SUPABASE_JWT_RE.match(sk.strip()):
                print(
                    "  Dica: com chave sb_publishable_... use no .env o JWT anon (eyJ...) "
                    "do painel Supabase (API → legacy anon)."
                )
        return 1

    print("\n=== 4. OpenAI (opcional) ===")
    try:
        from openai import OpenAI

        from pricing_pipeline import openai_chat_completions_create_compat

        model = os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
        client = OpenAI()
        r = openai_chat_completions_create_compat(
            client,
            model=model,
            messages=[{"role": "user", "content": "Responda só: ok"}],
            max_tokens=5,
        )
        txt = (r.choices[0].message.content or "").strip()
        print(f"  [ok] Modelo {model} respondeu: {txt[:80]}")
    except Exception as e:
        print(f"  [aviso] Não testado ou falhou: {e}")

    if args.com_playwright:
        print("\n=== 5. Playwright + Chromium ===")
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto("https://example.com", timeout=15_000)
                t = page.title()
                browser.close()
            print(f"  [ok] Chromium abriu example.com — título: {t!r}")
        except Exception as e:
            print(f"  [erro] {e}")
            print("  Rode: playwright install chromium")
            return 1

    print("\n=== Próximos passos manuais ===")
    print("  • Planilha mínima: coluna url_leilao + cidade + estado; opcional bairro, data_leilao, segmento.")
    print("  • Teste pipeline (substitua o caminho):")
    print('    python -c "from pathlib import Path; from pricing_pipeline import LeilaoPricingPipelineConfig, executar_pipeline_precificacao_leiloes; print(executar_pipeline_precificacao_leiloes(LeilaoPricingPipelineConfig(caminho_planilha=Path(\'sua_planilha.xlsx\'), usar_avaliacao_llm=False)))"')
    print("  • Com LLM e triagem completa, use usar_avaliacao_llm=True (gasta tokens).")
    print("  • Tabela cache_media_bairro: necessária para triagem/cache; crie se ainda não existir.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
