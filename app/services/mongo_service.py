from app.utils.mongo_utils import get_db, list_databases
from fastapi.encoders import jsonable_encoder
from bson import ObjectId
from datetime import datetime, timedelta

def serialize_mongo_docs(docs):
    return jsonable_encoder(docs, custom_encoder={ObjectId: str, datetime: str})

def get_all_sites():
    dbs = list_databases()
    return [db for db in dbs if db not in ("admin", "local", "config")]

def get_all_collections(site_name: str, minutes: int | None = None):
    db = get_db(site_name)
    collections = db.list_collection_names()
    result = {}

    time_filter = {}
    if minutes is not None:
        min_time = datetime.utcnow() - timedelta(minutes=minutes)
        time_filter = {"timestamp": {"$gte": min_time}}

    for col in collections:
        try:
            docs = list(db[col].find(time_filter if minutes else {}))
            result[col] = serialize_mongo_docs(docs)
        except Exception as e:
            result[col] = {"error": str(e)}
    return result
