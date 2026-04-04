import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def deploy_client(tmp_path):
    from app.config import settings
    from app.api import router as api_router_module

    original = {
        "DEPLOY_BUNDLES_DIR": settings.DEPLOY_BUNDLES_DIR,
        "DEPLOY_BUNDLE_STORAGE_DIR": settings.DEPLOY_BUNDLE_STORAGE_DIR,
        "DEPLOY_BUNDLE_INDEX_FILE": settings.DEPLOY_BUNDLE_INDEX_FILE,
        "DEPLOY_INFERENCE_TARGETS": list(settings.DEPLOY_INFERENCE_TARGETS),
    }

    settings.DEPLOY_BUNDLES_DIR = str(tmp_path / "inference_bundles")
    settings.DEPLOY_BUNDLE_STORAGE_DIR = str(tmp_path / "inference_bundles" / "bundles")
    settings.DEPLOY_BUNDLE_INDEX_FILE = str(tmp_path / "inference_bundles" / "index.json")
    settings.DEPLOY_INFERENCE_TARGETS = [
        {
            "id": "hq",
            "name": "HQ",
            "base_url": "http://inference-hq:8001",
            "container_name": "inference_hq",
            "bundle_mount_path": "/data/bundles",
        }
    ]

    app = FastAPI()
    app.include_router(api_router_module.api_router)
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        settings.DEPLOY_BUNDLES_DIR = original["DEPLOY_BUNDLES_DIR"]
        settings.DEPLOY_BUNDLE_STORAGE_DIR = original["DEPLOY_BUNDLE_STORAGE_DIR"]
        settings.DEPLOY_BUNDLE_INDEX_FILE = original["DEPLOY_BUNDLE_INDEX_FILE"]
        settings.DEPLOY_INFERENCE_TARGETS = original["DEPLOY_INFERENCE_TARGETS"]


def _upload_bundle(client: TestClient, folder_name: str = "bundle") -> dict:
    manifest = {
        "manifest_version": 1,
        "metadata": {},
        "simulator": {},
        "training": {},
        "topology": {"num_agents": 1},
        "algorithm": {"name": "RuleBasedPolicy", "hyperparameters": {}},
        "environment": {
            "observation_names": [["x"]],
            "encoders": [[{"type": "NoNormalization", "params": {}}]],
            "action_bounds": [[{"low": [0], "high": [1]}]],
            "action_names": ["a"],
            "reward_function": {"name": "RewardFunction", "params": {}},
        },
        "agent": {
            "format": "rule_based",
            "artifacts": [
                {
                    "agent_index": 0,
                    "path": "policy_agent_0.json",
                    "format": "rule_based",
                    "config": {},
                }
            ],
        },
    }

    files = [
        (
            "files",
            (f"{folder_name}/artifact_manifest.json", json.dumps(manifest).encode("utf-8"), "application/json"),
        ),
        (
            "files",
            (
                f"{folder_name}/policy_agent_0.json",
                json.dumps({"default_actions": {"a": 0.0}, "rules": []}).encode("utf-8"),
                "application/json",
            ),
        ),
        ("relative_paths", (None, f"{folder_name}/artifact_manifest.json")),
        ("relative_paths", (None, f"{folder_name}/policy_agent_0.json")),
    ]

    response = client.post("/deploy/bundles/upload-folder", files=files)
    assert response.status_code == 200, response.text
    return response.json()


def test_list_inferences_returns_configured_targets(deploy_client: TestClient):
    response = deploy_client.get("/deploy/inferences")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["id"] == "hq"
    assert payload[0]["bundle_mount_path"] == "/data/bundles"


def test_upload_folder_and_list_bundles(deploy_client: TestClient):
    uploaded = _upload_bundle(deploy_client)
    assert "bundle" in uploaded
    bundle = uploaded["bundle"]

    assert bundle["bundle_id"]
    assert Path(bundle["manifest_path_host"]).exists()

    listed = deploy_client.get("/deploy/bundles")
    assert listed.status_code == 200
    rows = listed.json()
    assert any(item["bundle_id"] == bundle["bundle_id"] for item in rows)


def test_upload_folder_requires_manifest(deploy_client: TestClient):
    files = [
        (
            "files",
            ("bundle/policy_agent_0.json", json.dumps({"default_actions": {"a": 0.0}}).encode("utf-8"), "application/json"),
        ),
        ("relative_paths", (None, "bundle/policy_agent_0.json")),
    ]

    response = deploy_client.post("/deploy/bundles/upload-folder", files=files)
    assert response.status_code == 400
    assert "artifact_manifest.json" in response.json()["detail"]


def test_switch_bundle_resolves_container_paths(deploy_client: TestClient, monkeypatch):
    from app.services import deploy_service

    uploaded = _upload_bundle(deploy_client)
    bundle_id = uploaded["bundle"]["bundle_id"]

    class _Resp:
        def __init__(self, payload: dict):
            self._payload = payload
            self.text = json.dumps(payload)

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    captured: dict = {}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, json=None):
            captured["post_url"] = url
            captured["post_payload"] = json or {}
            return _Resp({"status": "loaded", "manifest_path": json.get("manifest_path")})

        def get(self, url):
            captured["get_url"] = url
            return _Resp(
                {
                    "status": "ok",
                    "configured": True,
                    "manifest_path": captured["post_payload"].get("manifest_path"),
                    "loaded_agent_indices": [0],
                }
            )

    monkeypatch.setattr(deploy_service.httpx, "Client", _FakeClient)

    response = deploy_client.post(f"/deploy/inferences/hq/switch-bundle", json={"bundle_id": bundle_id})
    assert response.status_code == 200, response.text
    body = response.json()

    expected_manifest = f"/data/bundles/{bundle_id}/artifact_manifest.json"
    assert captured["post_payload"]["manifest_path"] == expected_manifest
    assert captured["post_payload"]["artifacts_dir"] == f"/data/bundles/{bundle_id}"
    assert body["health"]["active_manifest_path"] == expected_manifest


def test_delete_bundle_blocks_when_active_and_allows_when_inactive(deploy_client: TestClient, monkeypatch):
    from app.services import deploy_service

    uploaded = _upload_bundle(deploy_client)
    bundle_id = uploaded["bundle"]["bundle_id"]

    def _active_probe(target):
        return {
            "reachable": True,
            "configured": True,
            "active_manifest_path": f"{target.bundle_mount_path}/{bundle_id}/artifact_manifest.json",
        }

    monkeypatch.setattr(deploy_service, "_probe_target_health", _active_probe)

    blocked = deploy_client.delete(f"/deploy/bundles/{bundle_id}")
    assert blocked.status_code == 409

    def _inactive_probe(_target):
        return {
            "reachable": True,
            "configured": True,
            "active_manifest_path": "/data/bundles/another/artifact_manifest.json",
        }

    monkeypatch.setattr(deploy_service, "_probe_target_health", _inactive_probe)

    deleted = deploy_client.delete(f"/deploy/bundles/{bundle_id}")
    assert deleted.status_code == 200
    assert deleted.json()["status"] == "deleted"


def test_logs_stream_returns_data(deploy_client: TestClient, monkeypatch):
    from app.services import deploy_service

    def _fake_stream(target_id: str, tail: int = 200):
        assert target_id == "hq"
        assert tail == 123
        yield "line-1\n"
        yield "line-2\n"

    monkeypatch.setattr(deploy_service, "stream_inference_logs", _fake_stream)

    response = deploy_client.get("/deploy/inferences/hq/logs/stream?tail=123")
    assert response.status_code == 200
    assert "line-1" in response.text
    assert "line-2" in response.text
