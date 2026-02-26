from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest


@pytest.fixture
def api_client():
    from app.api import router as api_router_module

    app = FastAPI()
    app.include_router(api_router_module.api_router)
    with TestClient(app) as client:
        yield client


def test_energy_communities_endpoint(api_client, monkeypatch):
    from app.controllers import mongo_controller

    async def fake_get_energy_communities():
        return {"energy_communities": ["community_a", "community_b"]}

    monkeypatch.setattr(mongo_controller, "get_energy_communities", fake_get_energy_communities)

    response = api_client.get("/energy-communities")
    assert response.status_code == 200
    assert response.json() == {"energy_communities": ["community_a", "community_b"]}


def test_historical_data_minutes_endpoint(api_client, monkeypatch):
    from app.controllers import mongo_controller

    captured = {}

    async def fake_get_historical_data(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(mongo_controller, "get_historical_data", fake_get_historical_data)

    response = api_client.get(
        "/historical-data/community_a",
        params={"minutes": 60, "limit": 100, "offset": 0},
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert captured["energy_community"] == "community_a"
    assert captured["minutes"] == 60
    assert captured["limit"] == 100
    assert captured["offset"] == 0


def test_historical_data_range_endpoint(api_client, monkeypatch):
    from app.controllers import mongo_controller

    captured = {}

    async def fake_get_historical_data(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(mongo_controller, "get_historical_data", fake_get_historical_data)

    response = api_client.get(
        "/historical-data/community_a",
        params={
            "from_ts": "2024-01-01T10:00:00+00:00",
            "until_ts": "2024-01-01T11:00:00+00:00",
            "limit": 50,
            "offset": 10,
        },
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert captured["from_ts"] == "2024-01-01T10:00:00+00:00"
    assert captured["until_ts"] == "2024-01-01T11:00:00+00:00"
    assert captured["limit"] == 50
    assert captured["offset"] == 10


def test_historical_data_granularity_forwarded(api_client, monkeypatch):
    from app.controllers import mongo_controller

    captured = {}

    async def fake_get_historical_data(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(mongo_controller, "get_historical_data", fake_get_historical_data)

    response = api_client.get(
        "/historical-data/community_a",
        params={"minutes": 60, "limit": 100, "offset": 0, "granularity_minutes": 5},
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert captured["granularity_minutes"] == 5


def test_historical_data_requires_time_filter(api_client, monkeypatch):
    from app.services import mongo_service

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community_a"])

    response = api_client.get("/historical-data/community_a", params={"limit": 50})
    assert response.status_code == 400
    assert "Provide either 'minutes'" in response.json()["detail"]


def test_historical_data_rejects_mixed_filters(api_client, monkeypatch):
    from app.services import mongo_service

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community_a"])

    response = api_client.get(
        "/historical-data/community_a",
        params={
            "minutes": 60,
            "from_ts": "2024-01-01T10:00:00+00:00",
            "until_ts": "2024-01-01T11:00:00+00:00",
            "limit": 50,
        },
    )
    assert response.status_code == 400


def test_historical_data_rejects_incomplete_range(api_client, monkeypatch):
    from app.services import mongo_service

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community_a"])

    response = api_client.get(
        "/historical-data/community_a",
        params={"from_ts": "2024-01-01T10:00:00+00:00", "limit": 50},
    )
    assert response.status_code == 400


def test_historical_data_rejects_inverted_range(api_client, monkeypatch):
    from app.services import mongo_service

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community_a"])

    response = api_client.get(
        "/historical-data/community_a",
        params={
            "from_ts": "2024-01-01T11:00:00+00:00",
            "until_ts": "2024-01-01T10:00:00+00:00",
            "limit": 50,
        },
    )
    assert response.status_code == 400


def test_historical_data_community_not_found(api_client, monkeypatch):
    from app.services import mongo_service

    monkeypatch.setattr(mongo_service, "list_databases", lambda: ["community_b"])

    response = api_client.get(
        "/historical-data/community_a",
        params={"minutes": 60, "limit": 50, "offset": 0},
    )
    assert response.status_code == 404
