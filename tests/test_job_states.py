import sys
from pathlib import Path
import types
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


def test_get_status_local_created(monkeypatch):
    job_service.jobs['job1'] = {'container_id': 'abc', 'target_host': 'local'}
    monkeypatch.setattr(job_service.docker_manager, 'get_container_status', lambda cid: ('created', None))
    monkeypatch.setattr(job_service.job_utils, 'save_job', lambda *args, **kwargs: None)
    status = job_service.get_status('job1')
    assert status['status'] == JobStatus.RUNNING.value
    job_service.jobs.pop('job1', None)


def test_get_status_remote_created(monkeypatch):
    job_service.jobs['job2'] = {'container_id': 'abc', 'target_host': 'remote', 'status': JobStatus.DISPATCHED.value}
    monkeypatch.setattr(job_service.docker_manager, 'get_container_status', lambda cid: ('created', None))
    monkeypatch.setattr(job_service.job_utils, 'save_job', lambda *args, **kwargs: None)
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
