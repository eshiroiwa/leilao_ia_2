from leilao_ia_v2.ui.lote_jobs import escolher_job_referencia, progresso_job


def test_escolher_job_referencia_prioriza_job_atual():
    jobs = [
        {"job_id": "a1", "updated_at": 10},
        {"job_id": "b2", "updated_at": 20},
    ]
    j = escolher_job_referencia(jobs, job_id_atual="a1")
    assert isinstance(j, dict)
    assert j.get("job_id") == "a1"


def test_escolher_job_referencia_fallback_mais_recente():
    jobs = [
        {"job_id": "b2", "updated_at": 20},
        {"job_id": "a1", "updated_at": 10},
    ]
    j = escolher_job_referencia(jobs, job_id_atual="x9")
    assert isinstance(j, dict)
    assert j.get("job_id") == "b2"


def test_progresso_job_sem_referencia_retorna_idle():
    s, p, t, f = progresso_job(None)
    assert s == "idle"
    assert p == 0
    assert t == 0
    assert f == 0.0


def test_progresso_job_com_total_estimado_calcula_fracao():
    s, p, t, f = progresso_job({"status": "running", "processed": 25, "total_est": 100})
    assert s == "running"
    assert p == 25
    assert t == 100
    assert f == 0.25


def test_progresso_job_done_forca_100_porcento():
    s, p, t, f = progresso_job({"status": "done", "processed": 8, "total_est": 12})
    assert s == "done"
    assert p == 8
    assert t == 12
    assert f == 1.0
