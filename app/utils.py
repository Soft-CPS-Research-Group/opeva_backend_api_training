import json
import os
import yaml
from uuid import uuid4
from app.config import CONFIGS_DIR, JOB_TRACK_FILE, JOBS_DIR

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
    print("CCCCC - 1")
    full_path = os.path.join(CONFIGS_DIR, file_name)
    print("CCCCC - 2")

    with open(full_path, "w") as f:
        yaml.dump(config, f)
    print("CCCCC - 3")
    print(full_path)
    return f"configs/{file_name}"

def save_job_info(job_id: str, job_name: str, config_path: str, target_host: str, container_id: str, experiment_name: str = None, run_name: str = None):

    import datetime, json
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    info_path = os.path.join(job_dir, "job_info.json")
    info = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": config_path,
        "target_host": target_host,
        "container_id": container_id,
        "started_at": datetime.datetime.utcnow().isoformat() + "Z",
        "experiment_name": experiment_name,
        "run_name": run_name,
    }
    with open(info_path, "w") as f:
        json.dump(info, f, indent=2)