from pymongo import MongoClient
from app.config import settings

_connections = {}

def get_db(db_name: str):
    if db_name not in _connections:
        _connections[db_name] = MongoClient(settings.mongo_uri(db_name))[db_name]
    return _connections[db_name]