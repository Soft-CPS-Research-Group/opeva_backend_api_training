from app.utils.mongo_utils import get_client, get_db

def create_schema(site: str, schema: dict):
    client = get_client()
    
    if site in client.list_database_names():
        raise ValueError(f"Site '{site}' already exists. Use the update endpoint instead.")

    db = client[site]
    db.create_collection("schema")
    db["schema"].insert_one({"_id": "schema", "schema": schema})

def update_schema(site: str, schema: dict):
    client = get_client()
    
    if site not in client.list_database_names():
        raise ValueError(f"Site '{site}' does not exist. Use the create endpoint first.")

    db = client[site]
    db["schema"].replace_one(
        {"_id": "schema"},
        {"_id": "schema", "schema": schema},
        upsert=True
    )

def get_schema(site: str) -> dict | None:
    db = get_db(site)
    doc = db["schema"].find_one({"_id": "schema"})
    return doc.get("schema") if doc else None
