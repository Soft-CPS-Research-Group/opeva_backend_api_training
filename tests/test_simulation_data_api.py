import os
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def sim_client(tmp_path):
    from app.config import settings
    from app.api import router as api_router_module
    from app.services import simulation_data_service

    original_jobs_dir = settings.JOBS_DIR
    settings.JOBS_DIR = str(tmp_path / "jobs")
    Path(settings.JOBS_DIR).mkdir(parents=True, exist_ok=True)
    simulation_data_service.settings = settings

    app = FastAPI()
    app.include_router(api_router_module.api_router)
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        settings.JOBS_DIR = original_jobs_dir
        simulation_data_service.settings = settings


def _write_csv(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_simulation_data_index_and_file_latest_session(sim_client):
    from app.config import settings

    job_root = Path(settings.JOBS_DIR) / "job-1" / "results" / "simulation_data"
    s1 = job_root / "session-a"
    s2 = job_root / "session-b"
    _write_csv(s1 / "community.csv", "timestamp,value\n2024-08-01T00:00:00Z,1\n")
    _write_csv(s2 / "community.csv", "timestamp,value\n2024-08-02T00:00:00Z,2\n")
    os.utime(s1, (1, 1))
    os.utime(s2, (2, 2))

    resp = sim_client.post("/simulation-data/index", json={"job_id": "job-1", "session": "latest"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["session"] == "session-b"
    assert "community.csv" in body["files"]
    assert "2024-08-02" in body["available_days"]

    file_resp = sim_client.post(
        "/simulation-data/file",
        json={"job_id": "job-1", "session": "session-b", "relative_path": "community.csv"},
    )
    assert file_resp.status_code == 200
    assert "timestamp,value" in file_resp.text


def test_simulation_data_rejects_path_traversal(sim_client):
    resp = sim_client.post(
        "/simulation-data/file",
        json={"job_id": "job-1", "session": "latest", "relative_path": "../secrets.csv"},
    )
    assert resp.status_code == 400
    assert "relative_path" in resp.text


def test_simulation_data_404_when_missing_job(sim_client):
    resp = sim_client.post("/simulation-data/index", json={"job_id": "missing-job", "session": "latest"})
    assert resp.status_code == 404
