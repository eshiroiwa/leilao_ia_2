"""Helpers puros para seleção e resumo de jobs de ingestão em lote."""

from __future__ import annotations

from typing import Any


def escolher_job_referencia(
    jobs_recentes: list[dict[str, Any]],
    *,
    job_id_atual: str = "",
) -> dict[str, Any] | None:
    """Retorna o job a exibir no monitor (job atual > mais recente)."""
    atual = str(job_id_atual or "").strip()
    if atual:
        for j in jobs_recentes:
            if str(j.get("job_id") or "").strip() == atual:
                return j
    if jobs_recentes:
        return jobs_recentes[0]
    return None


def progresso_job(job: dict[str, Any] | None) -> tuple[str, int, int, float]:
    """
    Resume status/progresso para UI.

    Retorna: (status, processados, total_estimado, frac_0_1).
    """
    if not isinstance(job, dict):
        return "idle", 0, 0, 0.0
    status = str(job.get("status") or "idle")
    proc = int(job.get("processed") or 0)
    total = int(job.get("total_est") or 0)
    if status == "done":
        total_done = total if total > 0 else max(proc, 1)
        return status, proc, total_done, 1.0
    if total > 0:
        frac = min(1.0, max(0.0, float(proc) / float(total)))
    else:
        frac = 0.0 if proc <= 0 else 1.0
    return status, proc, total, frac
