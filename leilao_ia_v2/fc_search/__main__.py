"""CLI: ``python -m leilao_ia_v2.fc_search --leilao-id <uuid>``."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    load_dotenv(_REPO / ".env")
    p = argparse.ArgumentParser(description="Complemento de anúncios via Firecrawl Search + scrape.")
    p.add_argument("--leilao-id", required=True, help="UUID em leilao_imoveis")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Só mostra a frase de busca e URLs escolhidas (sem scrape nem Supabase).",
    )
    args = p.parse_args()

    from .query_builder import montar_frase_busca
    from .search_client import executar_busca_web
    from .urls import extrair_urls_da_busca, selecionar_urls_para_scrape
    from leilao_ia_v2.persistence import leilao_imoveis_repo
    from leilao_ia_v2.supabase_client import get_supabase_client

    cli = get_supabase_client()
    row = leilao_imoveis_repo.buscar_por_id(args.leilao_id, cli)
    if not isinstance(row, dict):
        logger.error("Leilão não encontrado: %s", args.leilao_id)
        return 2

    q = montar_frase_busca(row)
    print("Frase de busca:", q)
    if args.dry_run:
        web, _ = executar_busca_web(q)
        urls = extrair_urls_da_busca(web)
        sel = selecionar_urls_para_scrape(urls)
        print("URLs candidatas:", len(urls))
        for u in sel:
            print(" -", u)
        return 0

    from .pipeline import complementar_anuncios_firecrawl_search
    from leilao_ia_v2.config.busca_mercado_parametros import get_busca_mercado_parametros

    area_ref = 0.0
    for k in ("area_util", "area_total"):
        v = row.get(k)
        try:
            if v is not None and float(v) > 0:
                area_ref = float(v)
                break
        except (TypeError, ValueError):
            pass

    cap_fc = int(get_busca_mercado_parametros().max_firecrawl_creditos_analise)
    n, diag, n_api = complementar_anuncios_firecrawl_search(
        cli,
        leilao_imovel_id=str(args.leilao_id),
        cidade=str(row.get("cidade") or ""),
        estado_raw=str(row.get("estado") or ""),
        bairro=str(row.get("bairro") or ""),
        tipo_imovel=str(row.get("tipo_imovel") or "apartamento"),
        area_ref=area_ref,
        ignorar_cache_firecrawl=False,
        max_chamadas_api=cap_fc,
    )
    print("Anúncios gravados (upsert):", n)
    print("Chamadas API (estimadas):", n_api)
    if diag.strip():
        print("--- Diagnóstico ---")
        print(diag)
    return 0 if n else 1


if __name__ == "__main__":
    raise SystemExit(main())
