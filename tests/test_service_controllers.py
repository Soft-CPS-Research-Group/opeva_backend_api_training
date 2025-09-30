import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from fastapi import HTTPException

from app.config import settings
from app.services import config_service, dataset_service, mongo_service, schema_service
from app.controllers import config_controller, dataset_controller, mongo_controller, schema_controller
from app.utils import file_utils, mongo_utils


@pytest.fixture(autouse=True)
def shared_env(tmp_path, monkeypatch):
    base = tmp_path / "shared"
    configs = base / "configs"
    datasets = base / "datasets"
    base.mkdir()
    configs.mkdir()
    datasets.mkdir()

    original = {
        "VM_SHARED_DATA": settings.VM_SHARED_DATA,
        "CONFIGS_DIR": settings.CONFIGS_DIR,
        "DATASETS_DIR": settings.DATASETS_DIR,
    }

    settings.VM_SHARED_DATA = str(base)
    settings.CONFIGS_DIR = str(configs)
    settings.DATASETS_DIR = str(datasets)

    file_utils.settings = settings
    config_service.file_utils = file_utils
    dataset_service.file_utils = file_utils

    try:
        yield SimpleNamespace(base=base, configs=configs, datasets=datasets)
    finally:
        for key, value in original.items():
            setattr(settings, key, value)
        file_utils.settings = settings
        config_service.file_utils = file_utils
        dataset_service.file_utils = file_utils


def test_config_service_crud(shared_env):
    payload = {"foo": "bar"}
    file_name = "demo.yaml"

    resp = config_service.save_config(payload, file_name)
    assert resp["message"] == "Config saved"

    path = Path(settings.CONFIGS_DIR) / file_name
    assert path.exists()

    configs = config_service.list_configs()
    assert file_name in configs

    data = config_service.get_config_by_name(file_name)
    assert data["config"]["foo"] == "bar"

    resp = config_service.delete_config(file_name)
    assert file_name not in config_service.list_configs()
    assert resp["message"].startswith("Config")

    with pytest.raises(FileNotFoundError):
        config_service.delete_config(file_name)


def test_config_controller_create_handles_exists(monkeypatch):
    monkeypatch.setattr(config_service, "save_config", lambda *a, **k: (_ for _ in ()).throw(FileExistsError("exists")))
    with pytest.raises(HTTPException) as exc:
        config_controller.create_config({}, "demo.yaml")
    assert exc.value.status_code == 400


def test_dataset_service_create_calls_file_utils(monkeypatch):
    called = {}

    def fake_create(name, site_id, cfg, description, period, from_ts, until_ts):
        called["args"] = (name, site_id, cfg, description, period, from_ts, until_ts)

    monkeypatch.setattr(file_utils, "create_dataset_dir", fake_create)

    resp = dataset_service.create_dataset("ds1", "site", {"x": 1}, "desc", 30, "2020-01-01", "2020-01-02")
    assert resp["message"] == "Dataset created"
    assert called["args"][0] == "ds1"


def test_dataset_controller_passthrough(monkeypatch):
    monkeypatch.setattr(dataset_service, "list_datasets", lambda: [{"name": "a"}])
    assert dataset_controller.list_datasets()[0]["name"] == "a"

    monkeypatch.setattr(dataset_service, "delete_dataset", lambda name: {"message": f"deleted {name}"})
    assert dataset_controller.delete_dataset("a")["message"] == "deleted a"

    download_path = Path(settings.DATASETS_DIR) / "f.csv"
    download_path.write_text("data")
    monkeypatch.setattr(dataset_service, "get_dataset_file", lambda name: str(download_path))
    resp = dataset_controller.download_dataset("a")
    assert resp.path == str(download_path)


def test_mongo_service_filters_sites(monkeypatch):
    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["admin", "local", "site1", "site2"])
    assert mongo_service.get_all_sites() == ["site1", "site2"]


def test_mongo_service_get_all_collections(monkeypatch):
    class FakeCollection:
        def __init__(self, docs):
            self._docs = docs

        def find(self, filter=None):
            return self._docs

    class FakeDB(dict):
        def list_collection_names(self):
            return list(self.keys())

        def __getitem__(self, item):
            if item not in self:
                raise KeyError(item)
            return dict.__getitem__(self, item)

    docs = {
        "coll1": FakeCollection([
            {"_id": 1, "timestamp": "2024-01-01T00:00:00", "value": 1},
        ])
    }
    fake_db = FakeDB(docs)

    monkeypatch.setattr(mongo_service, "get_db", lambda name: fake_db)

    result = mongo_service.get_all_collections("site1")
    assert "coll1" in result
    assert result["coll1"][0]["value"] == 1


def test_schema_service_create_and_get(monkeypatch):
    created = {}

    class fake_collection(dict):
        def insert_one(self, doc):
            created["doc"] = doc

        def replace_one(self, *args, **kwargs):
            created["replace"] = (args, kwargs)
            created["doc"] = args[1]

        def find_one(self, *args, **kwargs):
            return created.get("doc")

    class FakeDB(dict):
        def create_collection(self, name):
            self[name] = fake_collection()

        def __getitem__(self, item):
            if item not in self:
                self[item] = fake_collection()
            return dict.__getitem__(self, item)

    class FakeClient(dict):
        def list_database_names(self):
            return list(self.keys())

        def __getitem__(self, item):
            if item not in self:
                self[item] = FakeDB()
            return dict.__getitem__(self, item)

    client = FakeClient()

    monkeypatch.setattr(schema_service, "get_client", lambda: client)
    monkeypatch.setattr(schema_service, "get_db", lambda site: client[site])

    schema_service.create_schema("siteA", {"foo": "bar"})
    assert "schema" in client["siteA"]

    with pytest.raises(ValueError):
        schema_service.create_schema("siteA", {"foo": "bar"})

    schema_service.update_schema("siteA", {"foo": "baz"})
    replaced_doc = created["replace"][0][1]
    assert replaced_doc["_id"] == "schema"
    assert replaced_doc["schema"]["foo"] == "baz"

    stored = schema_service.get_schema("siteA")
    assert stored["foo"] == "baz"


def test_schema_controller_errors(monkeypatch):
    monkeypatch.setattr(schema_service, "create_schema", lambda *a, **k: (_ for _ in ()).throw(ValueError("dup")))
    with pytest.raises(HTTPException) as exc:
        schema_controller.create_schema_controller("site", {})
    assert exc.value.status_code == 400

    monkeypatch.setattr(schema_service, "update_schema", lambda *a, **k: (_ for _ in ()).throw(Exception("boom")))
    with pytest.raises(HTTPException) as exc:
        schema_controller.update_schema_controller("site", {})
    assert exc.value.status_code == 500

    monkeypatch.setattr(schema_service, "get_schema", lambda site: None)
    with pytest.raises(HTTPException) as exc:
        schema_controller.get_schema_controller("site")
    assert exc.value.status_code == 404
