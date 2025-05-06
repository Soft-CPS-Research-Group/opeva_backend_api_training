import os, json, yaml, base64
from app.config import settings
from app.utils import mongo_utils
from datetime import datetime
import shutil
import logging

def save_config_dict(config: dict, file_name: str) -> str:
    full_path = os.path.join(settings.CONFIGS_DIR, file_name)
    with open(full_path, "w") as f:
        yaml.dump(config, f)
    return file_name

def list_config_files():
    return [f for f in os.listdir(settings.CONFIGS_DIR) if f.endswith(('.yaml', '.yml'))]

def load_config_file(file_name):
    path = os.path.join(settings.CONFIGS_DIR, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config {file_name} not found")
    with open(path) as f:
        return yaml.safe_load(f)

def delete_config_by_name(file_name):
    path = os.path.join(settings.CONFIGS_DIR, file_name)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False

def collect_results(job_id):
    path = os.path.join(settings.JOBS_DIR, job_id, "results", "result.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"status": "pending", "message": "Result not ready yet."}

def read_progress(job_id):
    path = os.path.join(settings.JOBS_DIR, job_id, "progress", "progress.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"progress": "No updates yet."}

def create_dataset_dir(name: str, site_id: str, config: dict, from_ts: str = None, until_ts: str = None):
    path = os.path.join(settings.DATASETS_DIR, name)
    os.makedirs(path, exist_ok=True)

    db = mongo_utils.get_db(site_id)
    collection_names = db.list_collection_names()

    # Fetch the structure from the special "schema" collection
    structure_doc = db["schema"].find_one()
    if not structure_doc:
        raise ValueError(f"Missing 'schema' collection in site '{site_id}'")

    # Saves the buildings ids present in the schema for future data fetch
    building_ids = list(structure_doc.get("buildings").keys())
    # ADICIONAR AQUI DOS CARROS

    # Find collections that start with 'building_' followed by each building_id
    # Depois tenho de alterar isto pada incluir o prefixo mas primeiro tenho de alterar do lado do Percepta
    building_collections = [c for c in collection_names if any(c.startswith(building_id) for building_id in building_ids)]
    ev_collections = [c for c in collection_names if c.startswith("ev_")]
    price_collections = [c for c in collection_names if c.startswith("price_")]

    def parse_timestamp(ts: str) -> datetime:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")

    #Pode existir ou não. Se existir, converte para datetime. Se não existir, ignora e tras tudo
    from_dt = parse_timestamp(from_ts) if from_ts else None
    until_dt = parse_timestamp(until_ts) if until_ts else None

    #Aqui é para criar os csvs. Se o timestamp não existir, ignora e tras tudo. Se existir, ignora os que estão fora do range
    #Acho que o ideal era fazer um filtro na query, mas assim é mais simples. Se houver muitos dados, pode ser mais lento
    def write_csv(collection_name, data, header):
        filtered_data = []

        is_timestamp_present = False

        if any(field in header for field in settings.TIMESTAMP_DATASET_CSV_HEADER):
            is_timestamp_present = True

        for doc in data:
            ts = doc.get("timestamp")
            if ts:
                try:
                    ts_dt = datetime.fromisoformat(ts.replace("Z", ""))
                    if (from_dt and ts_dt < from_dt) or (until_dt and ts_dt > until_dt):
                        continue
                except Exception:
                    pass
            filtered_data.append(doc)

        with open(os.path.join(path, f"{collection_name}.csv"), "w") as f:
            # Write the original header (unchanged)
            f.write(",".join(header) + "\n")

            for doc in filtered_data:
                ts = doc.get("timestamp")
                ts_data = {}

                # Prepare timestamp-derived fields only if needed
                if is_timestamp_present and ts:

                    if not isinstance(ts, datetime):
                        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

                    ts_data = {
                        "month": ts.month,
                        "hour": ts.hour,
                        "minutes": ts.minute,
                        "day_type": ts.weekday(),
                        "daylight_savings_status": int(bool(ts.dst()))
                    }

                row = []
                for field in header:
                    if field in ts_data:
                        # Replace "timestamp" value with its components
                        row.append(str(ts_data.get(field, "")))
                    else:
                        row.append(str(doc.get(field, "")))

                # Write the row to the file
                f.write(",".join(row) + "\n")

    for col in building_collections:
        write_csv(col, list(db[col].find({})), settings.BUILDING_DATASET_CSV_HEADER)

    for col in ev_collections:
        write_csv(col, list(db[col].find({})), settings.EV_DATASET_CSV_HEADER)

    for col in price_collections:
        write_csv(col, list(db[col].find({})), settings.PRICE_DATASET_CSV_HEADER)

    # Remove MongoDB _id if present
    structure_doc.pop("_id", None)

    schema = {
        **config,
        "structure": structure_doc
    }

    with open(os.path.join(path, "schema.json"), "w") as f:
        json.dump(schema, f, indent=2)

    return path

def list_available_datasets():
    return [d for d in os.listdir(settings.DATASETS_DIR) if os.path.isdir(os.path.join(settings.DATASETS_DIR, d))]

def delete_dataset_by_name(name: str) -> bool:
    path = os.path.join(settings.DATASETS_DIR, name)
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)
        return True
    return False