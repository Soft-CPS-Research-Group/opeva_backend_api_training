import os, json, shutil
from app.config import settings

def ensure_directories():
    os.makedirs(settings.CONFIGS_DIR, exist_ok=True)
    os.makedirs(settings.JOBS_DIR, exist_ok=True)
    os.makedirs(settings.DATASETS_DIR, exist_ok=True)
    if not os.path.exists(settings.JOB_TRACK_FILE):
        with open(settings.JOB_TRACK_FILE, "w") as f:
            json.dump({}, f)

def load_jobs():
    if os.path.exists(settings.JOB_TRACK_FILE):
        with open(settings.JOB_TRACK_FILE) as f:
            return json.load(f)
    return {}

def save_job(job_id, metadata):
    jobs = load_jobs()
    jobs[job_id] = metadata
    with open(settings.JOB_TRACK_FILE, "w") as f:
        json.dump(jobs, f, indent=2)

def save_job_info(job_id, job_name, config_path, host, container_id, container_name, exp, run):
    job_dir = os.path.join(settings.JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    info = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": config_path,
        "target_host": host,
        "container_id": container_id,
        "container_name": container_name,
        "experiment_name": exp,
        "run_name": run,
    }
    with open(os.path.join(job_dir, "job_info.json"), "w") as f:
        json.dump(info, f, indent=2)

def delete_job_by_id(job_id: str, jobs: dict) -> bool:
    job_path = os.path.join(settings.JOBS_DIR, job_id)
    if os.path.exists(job_path):
        shutil.rmtree(job_path)
    if job_id in jobs:
        del jobs[job_id]
    with open(settings.JOB_TRACK_FILE, "w") as f:
        json.dump(jobs, f, indent=2)
    return True


def get_job_log_path(job_id: str):
    return os.path.join(settings.JOBS_DIR, job_id, "logs", f"{job_id}.log")

def get_available_hosts():
    return settings.AVAILABLE_HOSTS

def is_valid_host(target_host: str) -> bool:
    return any(h["host"] == target_host for h in settings.AVAILABLE_HOSTS)
