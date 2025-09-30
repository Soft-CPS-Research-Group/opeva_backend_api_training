import sys
from pathlib import Path
import asyncio
import types
import yaml
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class _DummyRemote:
    def __init__(self, func):
        self.func = func

    def remote(self, *args, **kwargs):
        return self.func(*args, **kwargs)


ray_stub = types.SimpleNamespace(
    init=lambda *args, **kwargs: None,
    remote=lambda f: _DummyRemote(f),
    get=lambda x: x,
)
sys.modules.setdefault("ray", ray_stub)

from app.services import job_service
from app.status import JobStatus, can_transition
from app.models.job import JobLaunchRequest


def test_get_status_local_created(monkeypatch):
    job_service.jobs['job1'] = {'target_host': 'local', 'status': JobStatus.QUEUED.value}
    monkeypatch.setattr(
        job_service,
        '_read_status_payload',
        lambda jid: {'job_id': jid, 'status': JobStatus.RUNNING.value},
    )
    status = job_service.get_status('job1')
    assert status['status'] == JobStatus.RUNNING.value
    job_service.jobs.pop('job1', None)


def test_get_status_remote_created(monkeypatch):
    job_service.jobs['job2'] = {'target_host': 'remote', 'status': JobStatus.QUEUED.value}
    monkeypatch.setattr(
        job_service,
        '_read_status_payload',
        lambda jid: {'job_id': jid, 'status': JobStatus.DISPATCHED.value},
    )
    status = job_service.get_status('job2')
    assert status['status'] == JobStatus.DISPATCHED.value
    job_service.jobs.pop('job2', None)


def test_status_transition_helpers():
    assert can_transition(JobStatus.LAUNCHING, JobStatus.QUEUED)
    assert not can_transition(JobStatus.QUEUED, JobStatus.RUNNING)


def test_write_status_blocks_invalid(monkeypatch):
    monkeypatch.setattr(job_service, '_read_status_file', lambda jid: JobStatus.QUEUED.value)
    monkeypatch.setattr(job_service.job_utils, 'write_status_file', lambda *a, **k: None)
    with pytest.raises(ValueError):
        job_service._write_status('jobx', JobStatus.RUNNING.value)


def test_launch_remote_updates_cache(monkeypatch, tmp_path):
    base = tmp_path
    shared = base / "shared"
    shared.mkdir()
    configs_dir = base / "configs"
    configs_dir.mkdir()
    jobs_dir = base / "jobs"
    jobs_dir.mkdir()
    queue_dir = base / "queue"
    queue_dir.mkdir()

    config_path = configs_dir / "demo.yaml"
    config_path.write_text(yaml.safe_dump({
        "experiment": {"name": "Demo", "run_name": "Run1"}
    }))

    monkeypatch.setattr(job_service, 'jobs', {})
    monkeypatch.setattr(job_service.job_utils, 'ensure_directories', lambda: None)

    monkeypatch.setattr(job_service.settings, 'VM_SHARED_DATA', str(shared))
    monkeypatch.setattr(job_service.settings, 'CONFIGS_DIR', str(configs_dir))
    monkeypatch.setattr(job_service.settings, 'JOBS_DIR', str(jobs_dir))
    monkeypatch.setattr(job_service.settings, 'QUEUE_DIR', str(queue_dir))
    monkeypatch.setattr(job_service.settings, 'JOB_TRACK_FILE', str(base / "job_track.json"))
    monkeypatch.setattr(job_service.settings, 'AVAILABLE_HOSTS', ["local", "remote1"])

    monkeypatch.setattr(job_service.job_utils, 'save_job_info', lambda *a, **k: None)

    queued = []

    def fake_enqueue(payload):
        queued.append(payload)

    monkeypatch.setattr(job_service.job_utils, 'enqueue_job', fake_enqueue)

    request = JobLaunchRequest(config_path="demo.yaml", target_host="remote1")

    result = asyncio.run(job_service.launch_simulation(request))

    assert result["status"] == JobStatus.QUEUED.value
    job_id = result["job_id"]
    assert job_id in job_service.jobs
    assert job_service.jobs[job_id]["status"] == JobStatus.QUEUED.value
    assert queued and queued[0]["preferred_host"] == "remote1"
    assert queued[0]["job_id"] == job_id
