import json
import os
import yaml
from uuid import uuid4
from app.config import CONFIGS_DIR, JOB_TRACK_FILE, JOBS_DIR, DATASETS_DIR
import base64
import shutil
import datetime, json

def ensure_directories():
    os.makedirs(CONFIGS_DIR, exist_ok=True)
    os.makedirs(JOBS_DIR, exist_ok=True)
    if not os.path.exists(JOB_TRACK_FILE):
        with open(JOB_TRACK_FILE, "w") as f:
            json.dump({}, f)

def save_job(job_id, container_id):
    jobs = load_jobs()
    jobs[job_id] = container_id
    with open(JOB_TRACK_FILE, "w") as f:
        json.dump(jobs, f)

def load_jobs():
    if os.path.exists(JOB_TRACK_FILE):
        with open(JOB_TRACK_FILE, "r") as f:
            return json.load(f)
    return {}

def collect_results(job_id):
    result_path = os.path.join(JOBS_DIR, job_id, "results", "result.json")
    if os.path.exists(result_path):
        with open(result_path) as f:
            return json.load(f)
    else:
        return {"status": "pending", "message": "Result not ready yet."}

def read_progress(job_id):
    progress_path = os.path.join(JOBS_DIR, job_id, "progress", "progress.json")
    if os.path.exists(progress_path):
        with open(progress_path) as f:
            return json.load(f)
    else:
        return {"progress": "No updates yet."}

def save_config_dict(config: dict, file_name: str) -> str:
    full_path = os.path.join(CONFIGS_DIR, file_name)

    with open(full_path, "w") as f:
        yaml.dump(config, f)

    return f"configs/{file_name}"

def save_job_info(job_id: str, job_name: str, config_path: str, target_host: str, container_id: str, container_name: str, experiment_name: str = None, run_name: str = None):

    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    info_path = os.path.join(job_dir, "job_info.json")
    info = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": config_path,
        "target_host": target_host,
        "container_id": container_id,
        "container_name": container_name,
        "started_at": datetime.datetime.utcnow().isoformat() + "Z",
        "experiment_name": experiment_name,
        "run_name": run_name,
    }
    with open(info_path, "w") as f:
        json.dump(info, f, indent=2)

def save_config_file(config: dict, file_name: str) -> str:
    file_path = os.path.join(CONFIGS_DIR, file_name)
    if os.path.exists(file_path):
        raise FileExistsError(f"File {file_name} already exists")
    with open(file_path, "w") as f:
        yaml.dump(config, f)
    return file_path

def list_config_files() -> list:
    return [f for f in os.listdir(CONFIGS_DIR) if f.endswith(".yaml") or f.endswith(".yml")]

def load_config_file(file_name: str) -> dict:
    file_path = os.path.join(CONFIGS_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config {file_name} not found")
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def create_dataset_dir(name: str, schema: dict, data_files: dict = None):
    dataset_path = os.path.join(DATASETS_DIR, "datasets", name)
    os.makedirs(dataset_path, exist_ok=True)

    schema_path = os.path.join(dataset_path, "schema.json")
    with open(schema_path, "w") as f:
        json.dump(schema, f, indent=2)

    if data_files:
        for fname, b64content in data_files.items():
            file_path = os.path.join(dataset_path, fname)
            with open(file_path, "wb") as f:
                f.write(base64.b64decode(b64content))

    return dataset_path

def list_available_datasets():
    datasets_path = DATASETS_DIR
    return [d for d in os.listdir(datasets_path) if os.path.isdir(os.path.join(datasets_path, d))]

def delete_job_by_id(job_id: str, jobs: dict) -> bool:
    job_path = os.path.join(JOBS_DIR, job_id)
    if os.path.exists(job_path):
        shutil.rmtree(job_path)
        tracked_jobs = load_jobs()
        if job_id in tracked_jobs:
            del tracked_jobs[job_id]
            with open(JOB_TRACK_FILE, "w") as f:
                json.dump(tracked_jobs, f)
        if job_id in jobs:
            del jobs[job_id]  # ✅ remove from memory
        return True
    return False


def delete_config_by_name(file_name: str) -> bool:
    config_path = os.path.join(CONFIGS_DIR, file_name)
    if os.path.exists(config_path):
        os.remove(config_path)
        return True
    return False