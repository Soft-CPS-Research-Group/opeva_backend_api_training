from pathlib import Path
from types import SimpleNamespace
import time

import pytest

import worker_agent


class FakeContainer:
    def __init__(self, job_id, exit_code=0, log_lines=None):
        self.id = f"cid-{job_id}"
        self.name = f"name-{job_id}"
        self._exit_code = exit_code
        self._log_lines = log_lines or [b"line1\n", b"line2\n"]

    def logs(self, stream=True, follow=True):
        for line in self._log_lines:
            yield line

    def wait(self):
        return {"StatusCode": self._exit_code}


class FakeContainers:
    def __init__(self, container):
        self._container = container

    def get(self, name):
        raise Exception("not found")

    def run(self, **kwargs):
        return self._container


class FakeDockerClient:
    def __init__(self, container, has_network=False):
        self.images = SimpleNamespace(pull=lambda image: None)
        self.containers = FakeContainers(container)
        self.networks = SimpleNamespace(get=(lambda name: None if has_network else (_ for _ in ()).throw(Exception("no network"))))


@pytest.mark.parametrize("exit_code, expected_status", [(0, "finished"), (3, "failed")])
def test_run_job_posts_statuses(tmp_path, monkeypatch, exit_code, expected_status):
    job_id = "job123"
    log_dir = tmp_path / "jobs" / job_id / "logs"
    log_dir.mkdir(parents=True)

    container = FakeContainer(job_id, exit_code=exit_code)
    fake_client = FakeDockerClient(container)

    monkeypatch.setattr(worker_agent, "SHARED", str(tmp_path))
    monkeypatch.setattr(worker_agent, "_client", fake_client)
    monkeypatch.setattr(worker_agent, "_gpu_device_requests", lambda: None)

    posted = []

    def fake_post(url, json=None, timeout=None):
        posted.append({"url": url, "json": json})
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(worker_agent.requests, "post", fake_post)
    worker_agent._last_heartbeat = 0.0

    worker_agent.run_job({
        "job_id": job_id,
        "image": "img",
        "container_name": "name",
        "command": "cmd",
        "volumes": [],
        "env": {},
    })
    worker_agent._client = None

    log_path = Path(worker_agent._log_path(job_id))
    for _ in range(50):
        if log_path.exists():
            break
        time.sleep(0.01)
    assert log_path.exists()
    status_posts = [call for call in posted if call["json"] and "status" in call["json"]]
    assert status_posts[0]["json"]["status"] == "running"
    assert status_posts[-1]["json"]["status"] == expected_status
    assert status_posts[0]["json"]["container_id"].startswith("cid-")
