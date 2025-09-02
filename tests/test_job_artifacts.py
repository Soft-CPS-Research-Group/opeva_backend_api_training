import sys
from pathlib import Path

import pytest

# Ensure repository root on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import job_service
from app.utils import file_utils


def _use_example_job_dir(monkeypatch):
    """Redirect job directory to bundled examples for testing."""
    base = Path(__file__).resolve().parents[1] / "examples"
    monkeypatch.setattr(job_service.settings, "JOBS_DIR", str(base))
    monkeypatch.setattr(file_utils.settings, "JOBS_DIR", str(base))
    return base


def test_result_progress_logs(monkeypatch):
    _use_example_job_dir(monkeypatch)
    job_id = "sample_job"

    res = job_service.get_result(job_id)
    assert res["status"] == "completed"

    prog = job_service.get_progress(job_id)
    assert prog["percent"] == 100

    logs = "".join(job_service.get_logs(job_id))
    assert "Simulation completed" in logs

    file_log = "".join(job_service.get_file_logs(job_id))
    assert "Simulation started" in file_log


def test_example_job_statuses(monkeypatch):
    _use_example_job_dir(monkeypatch)
    cases = [
        ("sample_job", "finished", 100),
        ("running_job", "running", 45),
        ("failed_job", "failed", 80),
        ("queued_job", "queued", 0),
    ]
    for job_id, expected_status, expected_percent in cases:
        job_service.jobs[job_id] = {"target_host": "remote"}
        status = job_service.get_status(job_id)
        assert status["status"] == expected_status
        prog = job_service.get_progress(job_id)
        assert prog["percent"] == expected_percent
        job_service.jobs.pop(job_id, None)
