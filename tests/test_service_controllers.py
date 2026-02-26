import json
from datetime import datetime, timedelta, timezone
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


def test_mongo_service_lists_energy_communities(monkeypatch):
    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["admin", "local", "site1", "site2"])
    assert mongo_service.list_energy_communities() == ["site1", "site2"]


def test_mongo_service_historical_minutes_pagination_and_schema_exclusion(monkeypatch):
    now = datetime.now(timezone.utc)

    class FakeCursor:
        def __init__(self, docs):
            self._docs = list(docs)

        def sort(self, field, direction):
            self._docs.sort(key=lambda item: item.get(field))
            return self

        def skip(self, offset):
            self._docs = self._docs[offset:]
            return self

        def limit(self, limit):
            self._docs = self._docs[:limit]
            return self

        def __iter__(self):
            return iter(self._docs)

    class FakeCollection:
        def __init__(self, docs):
            self._docs = docs

        def find(self, filter=None):
            docs = list(self._docs)
            if filter and "timestamp" in filter:
                ts_filter = filter["timestamp"]
                gte = ts_filter.get("$gte")
                lte = ts_filter.get("$lte")
                docs = [
                    doc for doc in docs
                    if (gte is None or doc["timestamp"] >= gte) and (lte is None or doc["timestamp"] <= lte)
                ]
            return FakeCursor(docs)

    class FakeDB(dict):
        def list_collection_names(self):
            return list(self.keys())

        def __getitem__(self, item):
            return dict.__getitem__(self, item)

    docs = {
        "schema": FakeCollection([{"_id": "schema"}]),
        "coll1": FakeCollection([
            {"_id": 1, "timestamp": now - timedelta(minutes=3), "value": 10},
            {"_id": 2, "timestamp": now - timedelta(minutes=2), "value": 20},
            {"_id": 3, "timestamp": now - timedelta(minutes=1), "value": 30},
        ]),
    }
    fake_db = FakeDB(docs)

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["admin", "local", "community1"])
    monkeypatch.setattr(mongo_service, "get_db", lambda _: fake_db)

    result = mongo_service.get_historical_data("community1", minutes=10, limit=2, offset=1)
    assert result["energy_community"] == "community1"
    assert result["query"]["minutes"] == 10
    assert result["query"]["limit"] == 2
    assert result["query"]["offset"] == 1
    assert "schema" not in result["collections"]
    assert "coll1" in result["collections"]
    assert len(result["collections"]["coll1"]["items"]) == 2
    assert result["collections"]["coll1"]["items"][0]["value"] == 20
    assert result["collections"]["coll1"]["items"][1]["value"] == 30


def test_mongo_service_historical_range_mode(monkeypatch):
    class FakeCursor:
        def __init__(self, docs):
            self._docs = list(docs)

        def sort(self, field, direction):
            self._docs.sort(key=lambda item: item.get(field))
            return self

        def skip(self, offset):
            self._docs = self._docs[offset:]
            return self

        def limit(self, limit):
            self._docs = self._docs[:limit]
            return self

        def __iter__(self):
            return iter(self._docs)

    class FakeCollection:
        def __init__(self, docs):
            self._docs = docs

        def find(self, filter=None):
            docs = list(self._docs)
            if filter and "timestamp" in filter:
                ts_filter = filter["timestamp"]
                gte = ts_filter.get("$gte")
                lte = ts_filter.get("$lte")
                docs = [
                    doc for doc in docs
                    if (gte is None or doc["timestamp"] >= gte) and (lte is None or doc["timestamp"] <= lte)
                ]
            return FakeCursor(docs)

    class FakeDB(dict):
        def list_collection_names(self):
            return list(self.keys())

        def __getitem__(self, item):
            return dict.__getitem__(self, item)

    docs = {
        "coll1": FakeCollection([
            {"_id": 1, "timestamp": datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc), "value": 10},
            {"_id": 2, "timestamp": datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc), "value": 20},
            {"_id": 3, "timestamp": datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc), "value": 30},
        ]),
    }
    fake_db = FakeDB(docs)

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community1"])
    monkeypatch.setattr(mongo_service, "get_db", lambda _: fake_db)

    result = mongo_service.get_historical_data(
        "community1",
        from_ts="2024-01-01T10:30:00+00:00",
        until_ts="2024-01-01T12:00:00+00:00",
        limit=100,
        offset=0,
    )
    items = result["collections"]["coll1"]["items"]
    assert len(items) == 2
    assert [i["value"] for i in items] == [20, 30]


def test_mongo_service_historical_granularity_mean_first_with_pagination(monkeypatch):
    class FakeCursor:
        def __init__(self, docs):
            self._docs = list(docs)

        def sort(self, field, direction):
            self._docs.sort(key=lambda item: item.get(field))
            return self

        def skip(self, offset):
            self._docs = self._docs[offset:]
            return self

        def limit(self, limit):
            self._docs = self._docs[:limit]
            return self

        def __iter__(self):
            return iter(self._docs)

    class FakeCollection:
        def __init__(self, docs):
            self._docs = docs

        def find(self, filter=None):
            docs = list(self._docs)
            if filter and "timestamp" in filter:
                ts_filter = filter["timestamp"]
                gte = ts_filter.get("$gte")
                lte = ts_filter.get("$lte")
                docs = [
                    doc for doc in docs
                    if (gte is None or doc["timestamp"] >= gte) and (lte is None or doc["timestamp"] <= lte)
                ]
            return FakeCursor(docs)

    class FakeDB(dict):
        def list_collection_names(self):
            return list(self.keys())

        def __getitem__(self, item):
            return dict.__getitem__(self, item)

    docs = {
        "coll1": FakeCollection([
            {"_id": 1, "timestamp": datetime(2024, 1, 1, 10, 1, tzinfo=timezone.utc), "value": 10, "mode": "A"},
            {"_id": 2, "timestamp": datetime(2024, 1, 1, 10, 3, tzinfo=timezone.utc), "value": 20, "mode": "B"},
            {"_id": 3, "timestamp": datetime(2024, 1, 1, 10, 6, tzinfo=timezone.utc), "value": 30, "mode": "C"},
        ]),
    }
    fake_db = FakeDB(docs)

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community1"])
    monkeypatch.setattr(mongo_service, "get_db", lambda _: fake_db)

    result = mongo_service.get_historical_data(
        "community1",
        from_ts="2024-01-01T10:00:00+00:00",
        until_ts="2024-01-01T10:10:00+00:00",
        limit=1,
        offset=1,
        granularity_minutes=5,
    )
    assert result["query"]["granularity_minutes"] == 5
    assert result["query"]["limit"] == 1
    assert result["query"]["offset"] == 1

    items = result["collections"]["coll1"]["items"]
    assert len(items) == 1
    assert items[0]["value"] == 30.0
    assert items[0]["mode"] == "C"
    assert "10:05:00" in items[0]["timestamp"]


def test_mongo_service_historical_validations(monkeypatch):
    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community1"])
    monkeypatch.setattr(mongo_service, "get_db", lambda _: {})

    with pytest.raises(HTTPException) as exc:
        mongo_service.get_historical_data("community1", limit=10, offset=0)
    assert exc.value.status_code == 400

    with pytest.raises(HTTPException) as exc:
        mongo_service.get_historical_data(
            "community1",
            minutes=30,
            from_ts="2024-01-01T10:00:00+00:00",
            until_ts="2024-01-01T11:00:00+00:00",
            limit=10,
            offset=0,
        )
    assert exc.value.status_code == 400

    with pytest.raises(HTTPException) as exc:
        mongo_service.get_historical_data(
            "community1",
            from_ts="2024-01-01T10:00:00+00:00",
            limit=10,
            offset=0,
        )
    assert exc.value.status_code == 400

    with pytest.raises(HTTPException) as exc:
        mongo_service.get_historical_data(
            "community1",
            from_ts="2024-01-01T11:00:00+00:00",
            until_ts="2024-01-01T10:00:00+00:00",
            limit=10,
            offset=0,
        )
    assert exc.value.status_code == 400


def test_mongo_service_historical_missing_community(monkeypatch):
    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community1"])
    with pytest.raises(HTTPException) as exc:
        mongo_service.get_historical_data("unknown", minutes=30, limit=10, offset=0)
    assert exc.value.status_code == 404


def test_mongo_service_historical_error_isolated_per_collection(monkeypatch):
    now = datetime.now(timezone.utc)

    class FakeCursor:
        def __init__(self, docs):
            self._docs = list(docs)

        def sort(self, field, direction):
            self._docs.sort(key=lambda item: item.get(field))
            return self

        def skip(self, offset):
            self._docs = self._docs[offset:]
            return self

        def limit(self, limit):
            self._docs = self._docs[:limit]
            return self

        def __iter__(self):
            return iter(self._docs)

    class GoodCollection:
        def find(self, filter=None):
            return FakeCursor([{"_id": 1, "timestamp": now, "value": 10}])

    class BrokenCollection:
        def find(self, filter=None):
            raise RuntimeError("broken collection")

    class FakeDB(dict):
        def list_collection_names(self):
            return list(self.keys())

        def __getitem__(self, item):
            return dict.__getitem__(self, item)

    fake_db = FakeDB({
        "good": GoodCollection(),
        "bad": BrokenCollection(),
        "schema": GoodCollection(),
    })

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community1"])
    monkeypatch.setattr(mongo_service, "get_db", lambda _: fake_db)

    result = mongo_service.get_historical_data(
        "community1",
        minutes=60,
        limit=50,
        offset=0,
    )
    assert set(result["collections"]) == {"good", "bad"}
    assert result["collections"]["good"]["items"][0]["value"] == 10
    assert result["collections"]["bad"]["items"] == []
    assert "error" in result["collections"]["bad"]


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
