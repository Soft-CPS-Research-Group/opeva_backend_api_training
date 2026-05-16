import json
from datetime import datetime, timedelta, timezone

import pytest

from fastapi import HTTPException

from app.services import mongo_service, schema_service
from app.controllers import mongo_controller, schema_controller
from app.utils import mongo_utils


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
