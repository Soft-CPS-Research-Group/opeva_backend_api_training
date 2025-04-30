from app.utils import mongo_utils

def get_collection(db_name: str, collection_name: str):
    db = mongo_utils.get_db(db_name)
    return {collection_name: list(db[collection_name].find({}))}

def get_all_collections(db_name: str):
    db = mongo_utils.get_db(db_name)
    collections = db.list_collection_names()
    return {col: list(db[col].find({})) for col in collections}