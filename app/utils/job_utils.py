# app/utils/job_utils.py
import os, json, shutil
from app.config import settings

def ensure_directories():
    os.makedirs(settings.CONFIGS_DIR, exist_ok=True)
    os.makedirs(settings.JOBS_DIR, exist_ok=True)
    os.makedirs(settings.DATASETS_DIR, exist_ok=True)
    os.makedirs(settings.QUEUE_DIR, exist_ok=True)
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

def save_job_info(job_id, job_name, config_path, host, container_id, container_name, exp, run, ray_task_id=None):
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
    if ray_task_id is not None:
        info["ray_task_id"] = ray_task_id
    with open(os.path.join(job_dir, "job_info.json"), "w") as f:
        json.dump(info, f, indent=2)

def write_status_file(job_id: str, status: str, extra: dict | None = None):
    job_dir = os.path.join(settings.JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    payload = {"job_id": job_id, "status": status}
    if extra:
        payload.update(extra)
    with open(os.path.join(job_dir, "status.json"), "w") as f:
        json.dump(payload, f, indent=2)

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

def is_valid_host(name: str) -> bool:
    return name in settings.AVAILABLE_HOSTS

# -------- queue helpers (filesystem) --------
def _queue_path(job_id: str) -> str:
    return os.path.join(settings.QUEUE_DIR, f"{job_id}.json")


def enqueue_job(payload: dict):
    os.makedirs(settings.QUEUE_DIR, exist_ok=True)
    path = _queue_path(payload["job_id"])
    entry = {
        "job_id": payload["job_id"],
        "preferred_host": payload.get("preferred_host"),
    }
    with open(path, "w") as f:
        json.dump(entry, f, indent=2)

def agent_pop_next_job(worker_id: str) -> dict | None:
    wdir = settings.QUEUE_DIR
    if not os.path.isdir(wdir):
        return None
    files = sorted(
        [os.path.join(wdir, f) for f in os.listdir(wdir) if f.endswith(".json")],
        key=lambda p: os.path.getmtime(p)
    )
    for path in files:
        claim_path = f"{path}.claim.{worker_id}"
        try:
            os.replace(path, claim_path)
        except FileNotFoundError:
            continue
        except OSError:
            continue

        try:
            with open(claim_path) as f:
                payload = json.load(f)
            preferred = payload.get("preferred_host")
            if preferred and preferred != worker_id:
                os.replace(claim_path, path)
                continue
            os.remove(claim_path)
            return payload
        except Exception:
            try:
                if os.path.exists(claim_path):
                    os.replace(claim_path, path)
            except Exception:
                pass
            continue
    return None


def list_queue() -> list[dict]:
    """Return pending queue entries ordered by enqueue time."""
    wdir = settings.QUEUE_DIR
    if not os.path.isdir(wdir):
        return []
    files = sorted(
        [os.path.join(wdir, f) for f in os.listdir(wdir) if f.endswith(".json")],
        key=lambda p: os.path.getmtime(p),
    )
    entries: list[dict] = []
    for path in files:
        try:
            with open(path) as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                entries.append(payload)
        except Exception:
            continue
    return entries


def remove_from_queue(job_id: str) -> bool:
    path = _queue_path(job_id)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


enqueue_job_for_agent = enqueue_job
