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


def _is_numeric(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _bucket_start(value, granularity_minutes: int) -> datetime:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    elif isinstance(value, str):
        dt = _parse_timestamp(value, "timestamp")
    else:
        raise ValueError("Unsupported timestamp type for aggregation")

    bucket_minute = (dt.minute // granularity_minutes) * granularity_minutes
    return dt.replace(minute=bucket_minute, second=0, microsecond=0)


def _aggregate_docs(docs: list[dict], granularity_minutes: int) -> list[dict]:
    buckets: dict[datetime, dict] = {}

    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if "timestamp" not in doc:
            continue
        try:
            bucket = _bucket_start(doc.get("timestamp"), granularity_minutes)
        except Exception:
            continue

        state = buckets.setdefault(
            bucket,
            {
                "sum": {},
                "count": {},
                "first_non_numeric": {},
                "first_any": {},
            },
        )

        for key, value in doc.items():
            if key in ("_id", "timestamp"):
                continue

            if key not in state["first_any"]:
                state["first_any"][key] = value

            if _is_numeric(value):
                state["sum"][key] = state["sum"].get(key, 0.0) + float(value)
                state["count"][key] = state["count"].get(key, 0) + 1
            elif key not in state["first_non_numeric"]:
                state["first_non_numeric"][key] = value

    aggregated: list[dict] = []
    for bucket in sorted(buckets.keys()):
        state = buckets[bucket]
        item = {"timestamp": bucket}
        keys = set(state["first_any"].keys()) | set(state["sum"].keys()) | set(state["first_non_numeric"].keys())
        for key in keys:
            if state["count"].get(key, 0) > 0:
                item[key] = state["sum"][key] / state["count"][key]
            elif key in state["first_non_numeric"]:
                item[key] = state["first_non_numeric"][key]
            else:
                item[key] = state["first_any"].get(key)
        aggregated.append(item)

    return aggregated


def get_historical_data(
    energy_community: str,
    limit: int,
    offset: int = 0,
    minutes: Optional[int] = None,
    from_ts: Optional[str] = None,
    until_ts: Optional[str] = None,
    granularity_minutes: Optional[int] = None,
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
            if granularity_minutes is None:
                cursor = db[col].find(time_filter).sort("timestamp", 1).skip(offset).limit(limit)
                docs = list(cursor)
            else:
                cursor = db[col].find(time_filter).sort("timestamp", 1)
                raw_docs = list(cursor)
                aggregated_docs = _aggregate_docs(raw_docs, granularity_minutes)
                docs = aggregated_docs[offset: offset + limit]

            collections_payload[col] = {
                "items": serialize_mongo_docs(docs),
            }
        except Exception as exc:
            collections_payload[col] = {
                "items": [],
                "error": str(exc),
            }

    query_payload = {
        **query_time,
        "limit": limit,
        "offset": offset,
    }
    if granularity_minutes is not None:
        query_payload["granularity_minutes"] = granularity_minutes

    return {
        "energy_community": energy_community,
        "query": query_payload,
        "collections": collections_payload,
    }
