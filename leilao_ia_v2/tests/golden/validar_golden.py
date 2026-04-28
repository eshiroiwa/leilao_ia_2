"""CLI para rodar a suite golden manualmente (fora do pytest).

Uso típico (a partir da raiz do repo):

    python -m leilao_ia_v2.tests.golden.validar_golden                  # roda todos
    python -m leilao_ia_v2.tests.golden.validar_golden --filtro pinda   # só casos com 'pinda' no nome
    python -m leilao_ia_v2.tests.golden.validar_golden --verbose        # imprime linhas persistidas

Saída: 0 quando todos passam (ignorando pendentes), 1 quando há falhas
não-pendentes. Pendentes que passam (regressão de bug-fix) também
provocam código de saída 1, com mensagem clara para retirar a marca.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from leilao_ia_v2.tests.golden.harness import (
    CasoGolden,
    executar_caso,
    formatar_resultado,
    listar_casos,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Roda a suite golden de comparáveis.")
    parser.add_argument(
        "--filtro",
        default="",
        help="Substring (case-insensitive) que filtra os nomes dos casos.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Imprime detalhes (linhas persistidas) mesmo nos casos OK.",
    )
    args = parser.parse_args(argv)

    casos = listar_casos()
    if args.filtro:
        f = args.filtro.lower()
        casos = [c for c in casos if f in c.stem.lower()]

    if not casos:
        print("Nenhum caso encontrado.", file=sys.stderr)
        return 1

    n_total = 0
    n_ok = 0
    n_falhas_reais = 0
    n_pendentes_falhando = 0
    n_pendentes_passando = 0  # bug foi corrigido — remover marca
    falhas_para_relatar: list[str] = []

    for caminho in casos:
        caso = CasoGolden.carregar(caminho)
        r = executar_caso(caso)
        n_total += 1
        if r.passou:
            if caso.pendente:
                n_pendentes_passando += 1
                falhas_para_relatar.append(
                    f"[REVISAR] {caso.nome} estava marcado 'pendente' mas PASSOU. "
                    f"Remover o campo 'pendente' do JSON.\n  Motivo registado: {caso.pendente}"
                )
            else:
                n_ok += 1
            print(formatar_resultado(r))
            if args.verbose and r.linhas_capturadas:
                for l in r.linhas_capturadas:
                    print(
                        f"    > {l.cidade}/{l.estado} | {l.tipo_imovel} | "
                        f"bairro={l.bairro!r} | origem={(l.metadados_json or {}).get('bairro_origem')!r}"
                    )
        else:
            if caso.pendente:
                n_pendentes_falhando += 1
                print(f"[XFAIL] {caso.nome}: {caso.pendente}")
                if args.verbose:
                    print(formatar_resultado(r))
            else:
                n_falhas_reais += 1
                falhas_para_relatar.append(formatar_resultado(r))
                print(formatar_resultado(r))
        print()

    print("=" * 70)
    print(
        f"Total: {n_total} | OK: {n_ok} | falhas reais: {n_falhas_reais} | "
        f"pendentes (falhando): {n_pendentes_falhando} | "
        f"pendentes (passando — REVISAR!): {n_pendentes_passando}"
    )

    if n_falhas_reais or n_pendentes_passando:
        print()
        print("Detalhes a tratar:")
        for msg in falhas_para_relatar:
            print(msg)
            print("-" * 50)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
