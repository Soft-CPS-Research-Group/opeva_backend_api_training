import os, json, yaml, base64
from app.config import settings

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

def create_dataset_dir(name, schema, files=None):
    path = os.path.join(settings.DATASETS_DIR, name)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "schema.json"), "w") as f:
        json.dump(schema, f, indent=2)
    if files:
        for fname, b64 in files.items():
            with open(os.path.join(path, fname), "wb") as f:
                f.write(base64.b64decode(b64))
    return path

def list_available_datasets():
    return [d for d in os.listdir(settings.DATASETS_DIR) if os.path.isdir(os.path.join(settings.DATASETS_DIR, d))]
