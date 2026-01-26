from pathlib import Path

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    from app.config import settings
    from app.utils import job_utils, file_utils
    from app.services import job_service
    from app.controllers import job_controller
    from app.api import router as api_router_module

    original_values = {
        "VM_SHARED_DATA": settings.VM_SHARED_DATA,
        "CONFIGS_DIR": settings.CONFIGS_DIR,
        "JOBS_DIR": settings.JOBS_DIR,
        "DATASETS_DIR": settings.DATASETS_DIR,
        "QUEUE_DIR": settings.QUEUE_DIR,
        "JOB_TRACK_FILE": settings.JOB_TRACK_FILE,
        "HOST_HEARTBEATS": dict(job_service.host_heartbeats),
    }

    settings.VM_SHARED_DATA = str(tmp_path)
    settings.CONFIGS_DIR = str(Path(settings.VM_SHARED_DATA) / "configs")
    settings.JOBS_DIR = str(Path(settings.VM_SHARED_DATA) / "jobs")
    settings.DATASETS_DIR = str(Path(settings.VM_SHARED_DATA) / "datasets")
    settings.QUEUE_DIR = str(Path(settings.VM_SHARED_DATA) / "queue")
    settings.JOB_TRACK_FILE = str(Path(settings.VM_SHARED_DATA) / "job_track.json")
    for folder in (settings.CONFIGS_DIR, settings.JOBS_DIR, settings.DATASETS_DIR, settings.QUEUE_DIR):
        Path(folder).mkdir(parents=True, exist_ok=True)
    job_track = Path(settings.JOB_TRACK_FILE)
    if not job_track.exists():
        job_track.write_text("{}")

    job_utils.settings = settings
    file_utils.settings = settings
    job_service.settings = settings
    job_service.job_utils.settings = settings
    job_service.file_utils.settings = settings
    job_controller.job_service = job_service
    original_values["HOST_HEARTBEATS"] = dict(job_service.host_heartbeats)

    job_service.host_heartbeats.clear()

    ensure_calls: list[str] = []

    def _ensure_dirs():
        ensure_calls.append(settings.CONFIGS_DIR)
        for folder in (settings.CONFIGS_DIR, settings.JOBS_DIR, settings.DATASETS_DIR, settings.QUEUE_DIR):
            Path(folder).mkdir(parents=True, exist_ok=True)
        job_track = Path(settings.JOB_TRACK_FILE)
        if not job_track.exists():
            job_track.write_text("{}")

    monkeypatch.setattr(job_service.job_utils, "ensure_directories", _ensure_dirs)

    job_service.jobs.clear()

    try:
        app = FastAPI()
        app.include_router(api_router_module.api_router)
        client = TestClient(app)
        client._ensure_calls = ensure_calls  # type: ignore[attr-defined]
        yield client
    finally:
        client.close()
        job_service.jobs.clear()
        for attr, value in original_values.items():
            if attr == "HOST_HEARTBEATS":
                continue
            else:
                setattr(settings, attr, value)
        job_service.host_heartbeats = dict(original_values.get("HOST_HEARTBEATS", {}))
        job_utils.settings = settings
        file_utils.settings = settings
        job_service.settings = settings
        job_service.job_utils.settings = settings
        job_service.file_utils.settings = settings
        job_controller.job_service = job_service


def test_run_simulation_remote_accepts_prefixed_path(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    (Path(settings.CONFIGS_DIR)).mkdir(parents=True, exist_ok=True)
    config_file = Path(settings.CONFIGS_DIR) / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump({"experiment": {"name": "RemoteDemo", "run_name": "RunA"}})
    )

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["local", "remote1"])

    captured = {}

    def fake_enqueue(payload):
        captured["payload"] = payload

    monkeypatch.setattr(job_service.job_utils, "enqueue_job", fake_enqueue)

    response = api_client.post(
        "/run-simulation",
        json={"config_path": "configs/demo.yaml", "target_host": "remote1"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == JobStatus.QUEUED.value
    assert data["host"] == "remote1"
    assert captured["payload"] == {"job_id": data["job_id"], "preferred_host": "remote1", "require_host": True}

    job_id = data["job_id"]
    jobs_resp = api_client.get("/jobs").json()
    assert any(j["status"] == JobStatus.QUEUED.value for j in jobs_resp)

    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.QUEUED.value


def test_run_simulation_local_is_queued(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    payload = {"experiment": {"name": "LocalDemo", "run_name": "RunB"}}

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["local"])

    from app.utils import file_utils
    from app.controllers import job_controller

    assert settings.VM_SHARED_DATA != "/opt/opeva_shared_data"
    assert Path(job_service.settings.CONFIGS_DIR).is_relative_to(Path(settings.VM_SHARED_DATA))
    assert Path(job_service.job_utils.settings.CONFIGS_DIR).is_relative_to(Path(settings.VM_SHARED_DATA))
    assert job_service.job_utils.settings is job_service.settings
    assert Path(file_utils.settings.CONFIGS_DIR).is_relative_to(Path(settings.VM_SHARED_DATA))
    assert file_utils.settings is job_service.settings
    assert job_controller.job_service is job_service

    response = api_client.post(
        "/run-simulation",
        json={"config": payload},
    )
    assert getattr(api_client, "_ensure_calls"), "ensure_directories not invoked"
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == JobStatus.QUEUED.value
    assert data["host"] is None
    job_id = data["job_id"]

    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.QUEUED.value

    jobs_resp = api_client.get("/jobs").json()
    assert any(j["status"] == JobStatus.QUEUED.value for j in jobs_resp)


def test_queue_endpoint_lists_entries(api_client, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    payload = {"experiment": {"name": "Queued", "run_name": "Job"}}
    response = api_client.post(
        "/run-simulation",
        json={"config": payload, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    queue_resp = api_client.get("/queue")
    assert queue_resp.status_code == 200
    entries = queue_resp.json()
    assert any(entry.get("job_id") == job_id for entry in entries)


def test_agent_job_status_updates_exit_code(api_client, monkeypatch):
    from app.config import settings
    from app.services import job_service
    from app.status import JobStatus

    monkeypatch.setattr(settings, "AVAILABLE_HOSTS", ["worker-a"])

    payload = {"experiment": {"name": "Status", "run_name": "Update"}}
    response = api_client.post(
        "/run-simulation",
        json={"config": payload, "target_host": "worker-a"},
    )
    assert response.status_code == 200
    data = response.json()
    job_id = data["job_id"]

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id

    resp = api_client.post(
        "/api/agent/job-status",
        json={
            "job_id": job_id,
            "status": JobStatus.RUNNING.value,
            "worker_id": "worker-a",
            "container_id": "cid-1",
            "container_name": "cont-1",
        },
    )
    assert resp.status_code == 200

    resp = api_client.post(
        "/api/agent/job-status",
        json={
            "job_id": job_id,
            "status": JobStatus.FINISHED.value,
            "worker_id": "worker-a",
            "exit_code": 0,
        },
    )
    assert resp.status_code == 200

    status_resp = api_client.get(f"/status/{job_id}").json()
    assert status_resp["status"] == JobStatus.FINISHED.value
    assert status_resp["exit_code"] == 0
