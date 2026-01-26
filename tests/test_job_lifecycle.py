import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from app.config import settings
from app.status import JobStatus
from app.models.job import JobLaunchRequest
from app.utils import job_utils, file_utils
from app.services import job_service
from fastapi import HTTPException


@pytest.fixture(autouse=True)
def jobs_env(tmp_path, monkeypatch):
    base = tmp_path / "shared"
    configs = base / "configs"
    jobs_dir = base / "jobs"
    datasets = base / "datasets"
    queue = base / "queue"
    for folder in (configs, jobs_dir, datasets, queue):
        folder.mkdir(parents=True, exist_ok=True)
    job_track = base / "job_track.json"
    job_track.write_text("{}")

    original = {
        "VM_SHARED_DATA": settings.VM_SHARED_DATA,
        "CONFIGS_DIR": settings.CONFIGS_DIR,
        "JOBS_DIR": settings.JOBS_DIR,
        "DATASETS_DIR": settings.DATASETS_DIR,
        "QUEUE_DIR": settings.QUEUE_DIR,
        "JOB_TRACK_FILE": settings.JOB_TRACK_FILE,
        "AVAILABLE_HOSTS": list(settings.AVAILABLE_HOSTS),
        "HOST_HEARTBEATS": dict(job_service.host_heartbeats),
    }

    settings.VM_SHARED_DATA = str(base)
    settings.CONFIGS_DIR = str(configs)
    settings.JOBS_DIR = str(jobs_dir)
    settings.DATASETS_DIR = str(datasets)
    settings.QUEUE_DIR = str(queue)
    settings.JOB_TRACK_FILE = str(job_track)

    job_utils.settings = settings
    file_utils.settings = settings
    job_service.settings = settings
    job_service.job_utils.settings = settings
    job_service.file_utils.settings = settings

    job_service.jobs.clear()
    job_service.host_heartbeats.clear()

    try:
        yield SimpleNamespace(base=base, configs=configs, jobs=jobs_dir, queue=queue)
    finally:
        job_service.jobs.clear()
        job_track.write_text("{}")
        for key, value in original.items():
            if key == "AVAILABLE_HOSTS":
                settings.AVAILABLE_HOSTS = value
            elif key == "HOST_HEARTBEATS":
                job_service.host_heartbeats = dict(value)
            else:
                setattr(settings, key, value)
        job_utils.settings = settings
        file_utils.settings = settings
        job_service.settings = settings
        job_service.job_utils.settings = settings
        job_service.file_utils.settings = settings


def test_launch_remote_persists_and_queues(monkeypatch):
    settings.AVAILABLE_HOSTS = ["local", "remote1"]

    config_path = Path(settings.CONFIGS_DIR) / "demo.yaml"
    config_path.write_text(yaml.safe_dump({"experiment": {"name": "Remote", "run_name": "RunA"}}))

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config_path="demo.yaml", target_host="remote1")
        )
    )

    job_id = result["job_id"]
    assert result["status"] == JobStatus.QUEUED.value
    assert result["host"] == "remote1"

    queued_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queued_file.exists()
    queued_payload = json.loads(queued_file.read_text())
    assert queued_payload["job_id"] == job_id
    assert queued_payload["preferred_host"] == "remote1"
    assert queued_payload["require_host"] is True

    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["status"] == JobStatus.QUEUED.value
    assert track[job_id]["config_path"] == "configs/demo.yaml"

    status_path = Path(settings.JOBS_DIR) / job_id / "status.json"
    assert status_path.exists()
    status_data = json.loads(status_path.read_text())
    assert status_data["status"] == JobStatus.QUEUED.value
    assert status_data["preferred_host"] == "remote1"

    info_path = Path(settings.JOBS_DIR) / job_id / "job_info.json"
    info = json.loads(info_path.read_text())
    assert info["target_host"] == "remote1"
    assert info["config_path"] == "configs/demo.yaml"
    assert info["container_id"] == ""

    assert job_service.jobs[job_id]["status"] == JobStatus.QUEUED.value


def test_launch_local_is_queued():
    settings.AVAILABLE_HOSTS = ["local"]

    payload = {"experiment": {"name": "Local", "run_name": "RunB"}}

    result = asyncio.run(
        job_service.launch_simulation(JobLaunchRequest(config=payload))
    )

    job_id = result["job_id"]
    assert result["host"] is None
    assert result["status"] == JobStatus.QUEUED.value

    config_file = Path(settings.CONFIGS_DIR) / f"{job_id}.yaml"
    assert config_file.exists()

    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    status_path = Path(settings.JOBS_DIR) / job_id / "status.json"
    status_data = json.loads(status_path.read_text())
    assert status_data["status"] == JobStatus.QUEUED.value


def test_launch_rejects_unknown_host():
    payload = {"experiment": {"name": "Bad", "run_name": "Run"}}

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            job_service.launch_simulation(
                JobLaunchRequest(config=payload, target_host="ghost")
            )
        )
    assert exc.value.status_code == 400


def test_launch_rejects_traversal(monkeypatch):
    Path(settings.CONFIGS_DIR).mkdir(parents=True, exist_ok=True)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            job_service.launch_simulation(
                JobLaunchRequest(config_path="../evil.yaml", target_host="local")
            )
        )
    assert exc.value.status_code == 400


def test_get_status_updates_on_exit(monkeypatch):
    job_id = "job-exit"
    job_service.jobs[job_id] = {
        "target_host": "local",
        "status": JobStatus.RUNNING.value,
    }
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.FAILED.value, {"exit_code": 5})

    resp = job_service.get_status(job_id)
    assert resp["status"] == JobStatus.FAILED.value
    assert resp["exit_code"] == 5

    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.FAILED.value
    assert status_data["exit_code"] == 5


def test_get_status_remote_uses_file():
    job_id = "job-remote"
    job_service.jobs[job_id] = {
        "target_host": "remote1",
        "status": JobStatus.QUEUED.value,
    }
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.DISPATCHED.value, {})

    resp = job_service.get_status(job_id)
    assert resp["status"] == JobStatus.DISPATCHED.value


def test_record_host_heartbeat_enforces_known_hosts():
    settings.AVAILABLE_HOSTS = ["local", "worker-a"]

    job_service.record_host_heartbeat("worker-a", {"gpu": True})
    assert "worker-a" in job_service.host_heartbeats

    with pytest.raises(HTTPException) as exc:
        job_service.record_host_heartbeat("ghost", {})
    assert exc.value.status_code == 400


def test_list_jobs_reports_latest_status(monkeypatch):
    job_id = "job-list"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "Demo",
        "config_path": "configs/demo.yaml",
        "target_host": "remote",
        "status": JobStatus.QUEUED.value,
    }
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})
    info = {
        "job_id": job_id,
        "job_name": "Demo",
        "config_path": "configs/demo.yaml",
        "target_host": "remote",
    }
    (job_dir / "job_info.json").write_text(json.dumps(info))

    result = job_service.list_jobs()
    [entry] = result
    assert entry["status"] == JobStatus.RUNNING.value
    assert entry["job_info"]["job_name"] == "Demo"


def test_launch_defaults_to_first_host():
    settings.AVAILABLE_HOSTS = ["remoteA", "remoteB"]

    config_path = Path(settings.CONFIGS_DIR) / "auto.yaml"
    config_path.write_text(yaml.safe_dump({"experiment": {"name": "Auto", "run_name": "Remote"}}))

    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config_path="auto.yaml")
        )
    )
    assert result["host"] is None


def test_agent_skips_jobs_for_other_hosts(monkeypatch):
    settings.AVAILABLE_HOSTS = ["worker-a", "worker-b"]

    config_payload = {"experiment": {"name": "Pref", "run_name": "One"}}
    result = asyncio.run(
        job_service.launch_simulation(
            JobLaunchRequest(config=config_payload, target_host="worker-a")
        )
    )
    job_id = result["job_id"]
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    # Worker-b should skip the job because it requires worker-a
    assert job_service.agent_next_job("worker-b") is None
    assert queue_file.exists()

    # Worker-a can claim it
    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched is not None
    assert dispatched["job_id"] == job_id
    assert not queue_file.exists()
    assert dispatched["image"] == settings.DEFAULT_JOB_IMAGE
    assert "--job_id" in dispatched["command"]


def test_agent_flow_updates_status_and_info():
    settings.AVAILABLE_HOSTS = ["local", "worker-a"]
    job_id = "job-agent"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "job_name": "AgentJob",
        "config_path": "configs/demo.yaml",
        "target_host": "worker-a",
        "status": JobStatus.QUEUED.value,
        "experiment_name": "Experiment",
        "run_name": "Run",
    }
    job_utils.save_job_info(
        job_id,
        "AgentJob",
        "configs/demo.yaml",
        "worker-a",
        "",
        "",
        "Experiment",
        "Run",
    )
    job_utils.write_status_file(job_id, JobStatus.QUEUED.value, {})

    job_utils.enqueue_job({
        "job_id": job_id,
        "preferred_host": "worker-a",
    })

    dispatched = job_service.agent_next_job("worker-a")
    assert dispatched["job_id"] == job_id

    status_data = json.loads((Path(settings.JOBS_DIR) / job_id / "status.json").read_text())
    assert status_data["status"] == JobStatus.DISPATCHED.value
    assert status_data["worker_id"] == "worker-a"
    assert job_service.jobs[job_id]["status"] == JobStatus.DISPATCHED.value
    info = json.loads((Path(settings.JOBS_DIR) / job_id / "job_info.json").read_text())
    assert info["target_host"] == "worker-a"

    job_service.agent_update_status(
        job_id,
        JobStatus.RUNNING.value,
        {
            "worker_id": "worker-a",
            "container_id": "cid-123",
            "container_name": "cname",
        },
    )

    status_after = json.loads((Path(settings.JOBS_DIR) / job_id / "status.json").read_text())
    assert status_after["status"] == JobStatus.RUNNING.value
    info = json.loads((Path(settings.JOBS_DIR) / job_id / "job_info.json").read_text())
    assert info["container_id"] == "cid-123"
    assert info["container_name"] == "cname"

    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["container_id"] == "cid-123"
    assert track[job_id]["status"] == JobStatus.RUNNING.value

    job_service.agent_update_status(
        job_id,
        JobStatus.FINISHED.value,
        {
            "worker_id": "worker-a",
            "exit_code": 0,
        },
    )

    status_final = json.loads((Path(settings.JOBS_DIR) / job_id / "status.json").read_text())
    assert status_final["status"] == JobStatus.FINISHED.value
    assert status_final["exit_code"] == 0
    info = json.loads((Path(settings.JOBS_DIR) / job_id / "job_info.json").read_text())
    assert info["exit_code"] == 0
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert track[job_id]["exit_code"] == 0
    assert track[job_id]["status"] == JobStatus.FINISHED.value


def test_list_queue_returns_entries(tmp_path, jobs_env):
    from app.config import settings
    from app.utils import job_utils

    settings.AVAILABLE_HOSTS = ["worker-a"]

    payload = {"job_id": "job-queued", "preferred_host": "worker-a"}
    job_utils.enqueue_job(payload)

    entries = job_service.list_queue()
    expected = dict(payload)
    expected["require_host"] = True
    assert entries == [expected]


def test_host_heartbeat_reporting(monkeypatch):
    settings.AVAILABLE_HOSTS = ["local", "worker-hb"]

    now = 1_000.0
    monkeypatch.setattr(job_service.time, "time", lambda: now)
    job_service.record_host_heartbeat("worker-hb", {"load": 0.5})

    hosts = job_service.get_hosts()["hosts"]
    assert hosts["worker-hb"]["online"] is True
    assert hosts["worker-hb"]["info"]["load"] == 0.5

    monkeypatch.setattr(job_service.time, "time", lambda: now + job_service.HEARTBEAT_TTL + 5)
    hosts = job_service.get_hosts()["hosts"]
    assert hosts["worker-hb"]["online"] is False


def test_stop_job_local(monkeypatch):
    job_id = "job-stop-local"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "local",
        "status": JobStatus.RUNNING.value,
    }
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.RUNNING.value, {})

    resp = job_service.stop_job(job_id)
    assert "Local stop" in resp["message"]
    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.STOPPED.value


def test_stop_job_remote_removes_queue():
    job_id = "job-stop-remote"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "worker-b",
        "status": JobStatus.QUEUED.value,
    }
    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_utils.write_status_file(job_id, JobStatus.QUEUED.value, {})

    payload = {
        "job_id": job_id,
        "preferred_host": "worker-b",
        "require_host": True,
    }
    job_utils.enqueue_job(payload)
    queue_file = Path(settings.QUEUE_DIR) / f"{job_id}.json"
    assert queue_file.exists()

    resp = job_service.stop_job(job_id)
    assert "canceled" in resp["message"].lower()
    assert not queue_file.exists()
    status_data = json.loads((job_dir / "status.json").read_text())
    assert status_data["status"] == JobStatus.CANCELED.value


def test_delete_job_removes_artifacts():
    job_id = "job-delete"
    job_service.jobs[job_id] = {
        "job_id": job_id,
        "target_host": "remote",
        "status": JobStatus.QUEUED.value,
    }
    entry = {
        job_id: job_service.jobs[job_id]
    }
    Path(settings.JOB_TRACK_FILE).write_text(json.dumps(entry))

    job_dir = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "status.json").write_text(json.dumps({"status": JobStatus.QUEUED.value}))

    resp = job_service.delete_job(job_id)
    assert "deleted" in resp["message"]
    assert not job_dir.exists()
    track = json.loads(Path(settings.JOB_TRACK_FILE).read_text())
    assert job_id not in track
    assert job_id not in job_service.jobs
