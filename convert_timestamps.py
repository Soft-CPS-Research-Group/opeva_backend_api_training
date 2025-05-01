import argparse
from pymongo import MongoClient
from datetime import datetime
import sys

def convert_timestamps(db_name: str, host: str, port: int, user: str, password: str, auth_source: str, create_index: bool):
    uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource={auth_source}"
    client = MongoClient(uri)
    db = client[db_name]

    print(f"🔁 Converting 'timestamp' fields in database: {db_name}")
    collections = db.list_collection_names()

    for col in collections:
        print(f" → Processing collection: {col}")
        updated_count = 0
        for doc in db[col].find({"timestamp": {"$type": "string"}}):
            try:
                ts = datetime.fromisoformat(doc["timestamp"])
                db[col].update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"timestamp": ts}}
                )
                updated_count += 1
            except Exception as e:
                print(f"   ⚠️ Skipping _id {doc['_id']}: {e}")

        print(f"   ✅ Converted {updated_count} timestamps in '{col}'")

        if create_index:
            try:
                db[col].create_index("timestamp")
                print(f"   🔍 Index created on 'timestamp'")
            except Exception as e:
                print(f"   ⚠️ Failed to create index on '{col}': {e}")

    print("🎉 Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert string 'timestamp' fields to datetime in a MongoDB database.")
    parser.add_argument("--db", required=True, help="MongoDB database name")
    parser.add_argument("--host", default="193.136.62.78", help="MongoDB host (default: 193.136.62.78)")
    parser.add_argument("--port", type=int, default=27017, help="MongoDB port (default: 27017)")
    parser.add_argument("--user", default="runtimeUI", help="MongoDB username")
    parser.add_argument("--password", default="runtimeUIDB", help="MongoDB password")
    parser.add_argument("--authSource", default="admin", help="MongoDB authSource (default: admin)")
    parser.add_argument("--create-index", action="store_true", help="Create index on timestamp field")

    args = parser.parse_args()

    try:
        convert_timestamps(
            db_name=args.db,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            auth_source=args.authSource,
            create_index=args.create_index
        )
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

#python scripts/convert_timestamps.py --db living_lab --create-index