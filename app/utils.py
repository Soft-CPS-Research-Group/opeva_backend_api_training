import json
import os
from app.config import JOB_TRACK_FILE, RESULTS_DIR, PROGRESS_DIR, CONFIGS_DIR

def ensure_directories():
    for path in [RESULTS_DIR, PROGRESS_DIR, CONFIGS_DIR]:
        os.makedirs(path, exist_ok=True)
    # Initialize job track file if it doesn't exist
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
    result_path = os.path.join(RESULTS_DIR, job_id, "result.json")
    if os.path.exists(result_path):
        with open(result_path) as f:
            return json.load(f)
    else:
        return {"status": "pending", "message": "Result not ready yet."}

def read_progress(job_id):
    progress_path = os.path.join(PROGRESS_DIR, job_id, "progress.json")
    if os.path.exists(progress_path):
        with open(progress_path) as f:
            return json.load(f)
    else:
        return {"progress": "No updates yet."}
