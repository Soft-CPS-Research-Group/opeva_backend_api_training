from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder

from app.utils.mongo_utils import get_db, list_databases

_SYSTEM_DATABASES = {"admin", "local", "config"}


def serialize_mongo_docs(docs):
    return jsonable_encoder(docs, custom_encoder={ObjectId: str, datetime: str})


def list_energy_communities() -> list[str]:
    dbs = list_databases()
    return [db for db in dbs if db not in _SYSTEM_DATABASES]


def _parse_timestamp(value: str, field_name: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field_name}. Expected ISO-8601 format.",
        )
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_time_filter(
    minutes: Optional[int],
    from_ts: Optional[str],
    until_ts: Optional[str],
) -> tuple[dict, dict]:
    has_minutes = minutes is not None
    has_from = from_ts is not None
    has_until = until_ts is not None

    if not has_minutes and not (has_from and has_until):
        raise HTTPException(
            status_code=400,
            detail="Provide either 'minutes' or both 'from_ts' and 'until_ts'.",
        )
    if has_minutes and (has_from or has_until):
        raise HTTPException(
            status_code=400,
            detail="Use either 'minutes' or ('from_ts' + 'until_ts'), not both.",
        )
    if has_from != has_until:
        raise HTTPException(
            status_code=400,
            detail="'from_ts' and 'until_ts' must be provided together.",
        )

    if has_minutes:
        start = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        return {"timestamp": {"$gte": start}}, {"minutes": minutes}

    start = _parse_timestamp(from_ts, "from_ts")
    end = _parse_timestamp(until_ts, "until_ts")
    if end <= start:
        raise HTTPException(
            status_code=400,
            detail="'until_ts' must be greater than 'from_ts'.",
        )
    return {"timestamp": {"$gte": start, "$lte": end}}, {
        "from_ts": from_ts,
        "until_ts": until_ts,
    }


def get_historical_data(
    energy_community: str,
    minutes: Optional[int] = None,
    from_ts: Optional[str] = None,
    until_ts: Optional[str] = None,
):
    available_communities = set(list_energy_communities())
    if energy_community not in available_communities:
        raise HTTPException(status_code=404, detail=f"Energy community '{energy_community}' not found.")

    time_filter, query_time = _build_time_filter(minutes, from_ts, until_ts)
    db = get_db(energy_community)

    target_collections = [c for c in db.list_collection_names() if c != "schema"]

    collections_payload = {}
    for col in target_collections:
        try:
            cursor = db[col].find(time_filter).sort("timestamp", 1)
            docs = list(cursor)
            collections_payload[col] = {
                "items": serialize_mongo_docs(docs),
            }
        except Exception as exc:
            collections_payload[col] = {
                "items": [],
                "error": str(exc),
            }

    return {
        "energy_community": energy_community,
        "query": query_time,
        "collections": collections_payload,
    }
