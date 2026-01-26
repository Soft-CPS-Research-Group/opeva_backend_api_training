# app/services/job_service.py
import os, re, json, yaml, time, logging
from uuid import uuid4
from typing import Generator, Optional
from fastapi import HTTPException

from app.config import settings
from app.models.job import JobLaunchRequest
from app.utils import job_utils, file_utils
from app.status import JobStatus, can_transition

# In-memory cache of tracked jobs for fast access and testability
jobs = job_utils.load_jobs()
host_heartbeats: dict[str, dict] = {}
HEARTBEAT_TTL = settings.HOST_HEARTBEAT_TTL  # backward compatibility for tests

_LOGGER = logging.getLogger(__name__)

CAPACITY_COUNT_STATUSES = {
    JobStatus.DISPATCHED.value,
    JobStatus.RUNNING.value,
}

def _refresh_jobs():
    """Reload the job registry from disk to keep multiple workers in sync."""
    global jobs
    try:
        disk_jobs = job_utils.load_jobs()
        if isinstance(disk_jobs, dict):
            # Merge disk state into memory without dropping in-memory additions (useful for tests)
            for jid, meta in disk_jobs.items():
                jobs[jid] = meta
    except Exception:
        _LOGGER.warning("Failed to refresh jobs registry from disk", exc_info=True)


def _persist_job(job_id: str, metadata: dict):
    """Persist job metadata to disk and mirror it in the in-memory cache."""
    _LOGGER.debug("Persisting job %s (status=%s)", job_id, metadata.get("status"))
    job_utils.save_job(job_id, metadata)
    jobs[job_id] = metadata

# ---------- helpers ----------
def _slug(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', s)

def _job_dir(job_id: str) -> str:
    return os.path.join(settings.JOBS_DIR, job_id)

def _status_path(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "status.json")

def _info_path(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "job_info.json")

def _log_dir(job_id: str) -> str:
    return os.path.join(_job_dir(job_id), "logs")

def _log_path(job_id: str) -> str:
    return os.path.join(_log_dir(job_id), f"{job_id}.log")

def _container_name(job_id: str, job_name: str) -> str:
    safe_name = _slug(job_name)[:40]
    return f"{settings.CONTAINER_NAME_PREFIX}_{safe_name}_{job_id[:8]}"

def _read_status_payload(job_id: str) -> Optional[dict]:
    path = _status_path(job_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.setdefault("job_id", job_id)
            return data
    except Exception:
        return None
    return None


def _read_status_file(job_id: str) -> Optional[str]:
    payload = _read_status_payload(job_id)
    if payload:
        return payload.get("status")
    return None

def _write_status(job_id: str, status: str, extra: dict | None = None):
    """Persist status to disk and update the in-memory jobs cache."""
    prev = _read_status_file(job_id)
    if prev and not can_transition(prev, status):
        _LOGGER.error("Invalid status transition for job %s: %s -> %s", job_id, prev, status)
        raise ValueError(f"Invalid status transition {prev} -> {status}")
    _LOGGER.info(
        "Job %s status change %s -> %s (extras=%s)",
        job_id,
        prev,
        status,
        sorted((extra or {}).keys()),
    )
    job_utils.write_status_file(job_id, status, extra or {})
    if job_id in jobs:
        jobs[job_id]["status"] = status
        if extra:
            jobs[job_id].update(extra)
        with open(settings.JOB_TRACK_FILE, "w") as f:
            json.dump(jobs, f, indent=2)

# ---------- API: launch ----------
def _host_active_count(host: str) -> int:
    total = 0
    for job in jobs.values():
        if job.get("target_host") != host:
            continue
        if job.get("status") in CAPACITY_COUNT_STATUSES:
            total += 1
    return total


def _preferred_host(requested: Optional[str]) -> Optional[str]:
    if not requested:
        return None
    if not job_utils.is_valid_host(requested):
        raise HTTPException(400, f"Unknown host '{requested}'. Allowed: {settings.AVAILABLE_HOSTS}")
    return requested


def record_host_heartbeat(worker_id: str, info: dict | None = None) -> None:
    if not job_utils.is_valid_host(worker_id):
        raise HTTPException(400, f"Unknown worker_id '{worker_id}'. Allowed: {settings.AVAILABLE_HOSTS}")
    _LOGGER.debug("Heartbeat received from %s (info keys=%s)", worker_id, sorted((info or {}).keys()))
    host_heartbeats[worker_id] = {
        "last_seen": time.time(),
        "info": info or {},
    }


def _host_status_snapshot() -> dict[str, dict]:
    now = time.time()
    known_hosts = set(settings.AVAILABLE_HOSTS) | set(host_heartbeats.keys())
    snapshot: dict[str, dict] = {}
    for host in sorted(known_hosts):
        hb = host_heartbeats.get(host)
        online = bool(hb and (now - hb["last_seen"]) <= settings.HOST_HEARTBEAT_TTL)
        # Consider hosts with active jobs as online to avoid marking long runs offline
        if not online:
            active = any(
                (job.get("target_host") == host) and job.get("status") in (JobStatus.RUNNING.value, JobStatus.DISPATCHED.value)
                for job in jobs.values()
            )
            if active:
                online = True
        snapshot[host] = {
            "online": online,
            "last_seen": hb["last_seen"] if hb else None,
            "info": hb["info"] if hb else {},
            "running": _host_active_count(host),
        }
    return snapshot


def _mark_stale_jobs():
    """Detect jobs stuck on offline workers and requeue or fail them."""
    now = time.time()
    cutoff = settings.HOST_HEARTBEAT_TTL + settings.WORKER_STALE_GRACE_SECONDS
    for job_id, meta in list(jobs.items()):
        status = meta.get("status")
        host = meta.get("target_host")
        if status not in (JobStatus.DISPATCHED.value, JobStatus.RUNNING.value):
            continue
        if not host:
            continue
        hb = host_heartbeats.get(host)
        last_seen = hb["last_seen"] if hb else None
        if last_seen is None:
            continue  # no heartbeat recorded yet; give it a chance
        offline = (now - last_seen) > cutoff
        if not offline:
            continue
        preferred = meta.get("preferred_host") or meta.get("target_host")
        if status == JobStatus.DISPATCHED.value:
            # Put back in queue for another worker to pick up
            job_utils.enqueue_job({
                "job_id": job_id,
                "preferred_host": preferred if preferred != host else preferred,
                "require_host": bool(preferred),
            })
            meta["status"] = JobStatus.QUEUED.value
            _persist_job(job_id, meta)
            _write_status(job_id, JobStatus.QUEUED.value, {"requeued_from": host, "preferred_host": preferred})
            _LOGGER.warning("Re-queued stale dispatched job %s from offline host %s", job_id, host)
        elif status == JobStatus.RUNNING.value:
            _write_status(job_id, JobStatus.FAILED.value, {"error": "worker_offline", "last_host": host})
            meta["status"] = JobStatus.FAILED.value
            _persist_job(job_id, meta)
            _LOGGER.warning("Marked running job %s as failed because host %s is offline", job_id, host)


async def launch_simulation(request: JobLaunchRequest):
    job_utils.ensure_directories()

    if not settings.AVAILABLE_HOSTS:
        raise HTTPException(503, "No hosts configured")

    preferred_host = _preferred_host(request.target_host)
    job_id = str(uuid4())

    # config
    if request.config_path:
        # Accept both "file.yaml" and "configs/file.yaml" style paths
        config_path = request.config_path.lstrip("/")
        relative_path = config_path[len("configs/"):] if config_path.startswith("configs/") else config_path
        relative_path = os.path.normpath(relative_path)
        if relative_path.startswith(".."):
            raise HTTPException(400, "Invalid config_path")
        with open(os.path.join(settings.CONFIGS_DIR, relative_path)) as f:
            config = yaml.safe_load(f)
        config_path = relative_path
    elif request.config:
        file_name = request.save_as or f"{job_id}.yaml"
        config_path = file_utils.save_config_dict(request.config, file_name)
        config = request.config
    else:
        raise HTTPException(400, "Missing config or config_path")

    experiment_name = config.get("experiment", {}).get("name", "UnnamedExperiment")
    run_name = config.get("experiment", {}).get("run_name", "UnnamedRun")
    job_name = _slug(f"{experiment_name}-{run_name}")

    if not config_path.startswith("configs/"):
        config_path = f"configs/{config_path}"

    os.makedirs(_log_dir(job_id), exist_ok=True)
    meta = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": config_path,
        "target_host": preferred_host,
        "preferred_host": preferred_host,
        "experiment_name": experiment_name,
        "run_name": run_name,
        "status": JobStatus.LAUNCHING.value,
    }
    _persist_job(job_id, meta)
    job_utils.save_job_info(job_id, job_name, config_path, preferred_host or "",
                            container_id="", container_name="", exp=experiment_name, run=run_name)
    _write_status(job_id, JobStatus.LAUNCHING.value, {"preferred_host": preferred_host})

    # enqueue for agent (agent decides how to run the container)
    job_utils.enqueue_job({
        "job_id": job_id,
        "preferred_host": preferred_host,
        "require_host": bool(preferred_host),
    })
    meta.update({"status": JobStatus.QUEUED.value})
    _persist_job(job_id, meta)
    _write_status(job_id, JobStatus.QUEUED.value, {"preferred_host": preferred_host})
    return {"job_id": job_id, "status": JobStatus.QUEUED.value, "host": preferred_host, "job_name": job_name}

# ---------- API: status/result/progress/logs ----------

def get_status(job_id: str):
    """Return the current status payload for a given job."""
    _refresh_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    payload = _read_status_payload(job_id)
    if payload:
        return payload

    status = job.get("status", JobStatus.UNKNOWN.value)
    return {"job_id": job_id, "status": status}


def get_result(job_id: str):
    return file_utils.collect_results(job_id)

def get_progress(job_id: str):
    return file_utils.read_progress(job_id)

def _stream_file(path: str) -> Generator[str, None, None]:
    with open(path) as f:
        for line in f:
            yield line

def get_file_logs(job_id: str):
    path = _log_path(job_id)
    if not os.path.exists(path):
        raise HTTPException(404, "Log file not found")
    return _stream_file(path)

def get_logs(job_id: str):
    path = _log_path(job_id)
    if os.path.exists(path):
        return _stream_file(path)
    jobs = job_utils.load_jobs()
    job = jobs.get(job_id)
    if job and job.get("target_host") == "local" and job.get("container_id"):
        def _msg():
            yield "No file log yet; please check again shortly.\n"
        return _msg()
    raise HTTPException(404, "Logs not available for this job")

# ---------- API: stop/list/info/delete/hosts ----------
def stop_job(job_id: str):
    _refresh_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    host = job.get("target_host") or "local"
    status_now = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)

    removed = job_utils.remove_from_queue(job_id)

    if status_now == JobStatus.QUEUED.value and removed:
        _write_status(job_id, JobStatus.CANCELED.value)
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.CANCELED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Job canceled from queue"}

    if host == "local":
        _write_status(job_id, JobStatus.STOPPED.value, {"stop_requested": True})
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.STOPPED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Local stop recorded"}

    if status_now == JobStatus.RUNNING.value:
        _write_status(job_id, JobStatus.STOPPED.value, {"stop_requested": True})
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.STOPPED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Stop requested; remote worker should terminate the job"}

    if status_now == JobStatus.DISPATCHED.value:
        _write_status(job_id, JobStatus.CANCELED.value, {"stop_requested": True})
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.CANCELED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Dispatch canceled"}

    return {"message": f"Remote job is {status_now}; nothing to stop"}

def list_jobs():
    _refresh_jobs()
    _mark_stale_jobs()
    result = []
    for job_id, job in jobs.items():
        info = {}
        ipath = _info_path(job_id)
        if os.path.exists(ipath):
            with open(ipath) as f:
                info = json.load(f)

        status = get_status(job_id)["status"]

        result.append({"job_id": job_id, "status": status, "job_info": info})
    return result


def list_queue():
    _mark_stale_jobs()
    return job_utils.list_queue()

def get_job_info(job_id: str):
    _refresh_jobs()
    _mark_stale_jobs()
    p = _info_path(job_id)
    if not os.path.exists(p):
        raise HTTPException(404, "Job info not found")
    with open(p) as f:
        return json.load(f)

def delete_job(job_id: str):
    _refresh_jobs()
    _mark_stale_jobs()
    if job_id not in jobs:
        raise HTTPException(404, "Job not found or already deleted")
    ok = job_utils.delete_job_by_id(job_id, jobs)
    if not ok:
        raise HTTPException(500, "Failed to delete job")
    return {"message": f"Job {job_id} deleted successfully"}

def get_hosts():
    _refresh_jobs()
    _mark_stale_jobs()
    return {
        "available_hosts": settings.AVAILABLE_HOSTS,
        "hosts": _host_status_snapshot(),
    }

# ---------- hooks used by agent endpoints ----------
def agent_next_job(worker_id: str):
    _refresh_jobs()
    _mark_stale_jobs()
    job_queue_entry = job_utils.agent_pop_next_job(worker_id)
    if not job_queue_entry:
        _LOGGER.debug("Worker %s polled queue but no job was available", worker_id)
        return None

    job_id = job_queue_entry["job_id"]

    meta = jobs.get(job_id)
    if not meta:
        meta = job_utils.load_jobs().get(job_id, {})

    config_path = meta.get("config_path")
    job_name = meta.get("job_name", job_id)

    if not config_path:
        info_path = _info_path(job_id)
        if os.path.exists(info_path):
            with open(info_path) as f:
                info_data = json.load(f)
            config_path = info_data.get("config_path")
            job_name = info_data.get("job_name", job_name)
        if not config_path:
            raise HTTPException(500, f"Missing config path for job {job_id}")

    container_name = _container_name(job_id, job_name)
    command = f"--config /data/{config_path} --job_id {job_id}"

    response = {
        "job_id": job_id,
        "job_name": job_name,
        "config_path": config_path,
        "preferred_host": job_queue_entry.get("preferred_host"),
        "image": settings.DEFAULT_JOB_IMAGE,
        "command": command,
        "container_name": container_name,
        "volumes": [{
            "host": settings.VM_SHARED_DATA,
            "container": "/data",
            "mode": "rw",
        }],
        "env": {},
    }

    _LOGGER.info(
        "Dispatching job %s to worker %s (config=%s, preferred=%s)",
        job_id,
        worker_id,
        config_path,
        job_queue_entry.get("preferred_host"),
    )

    meta["status"] = JobStatus.DISPATCHED.value
    meta["target_host"] = worker_id
    _persist_job(job_id, meta)

    _write_status(job_id, JobStatus.DISPATCHED.value, {"worker_id": worker_id})

    info_path = _info_path(job_id)
    info = {}
    if os.path.exists(info_path):
        with open(info_path) as f:
            info = json.load(f)
    info["target_host"] = worker_id
    if "job_name" not in info:
        info["job_name"] = job_name
    if "config_path" not in info:
        info["config_path"] = config_path
    with open(info_path, "w") as f:
        json.dump(info, f, indent=2)

    return response

def agent_update_status(job_id: str, status: str, extra: dict | None = None):
    _refresh_jobs()
    _mark_stale_jobs()
    extra = extra or {}
    _LOGGER.info(
        "Agent reported status for job %s: %s (extra keys=%s)",
        job_id,
        status,
        sorted(extra.keys()),
    )
    _write_status(job_id, status, extra)

    worker = extra.get("worker_id")
    if worker:
        try:
            record_host_heartbeat(worker, extra.get("details"))
        except HTTPException:
            # If the worker is unknown, don't block status updates
            _LOGGER.warning("Ignoring heartbeat from unknown worker %s", worker)

    if worker and job_id in jobs:
        meta = jobs[job_id]
        if meta.get("target_host") != worker:
            meta["target_host"] = worker
            _LOGGER.debug("Updating job %s target host to %s", job_id, worker)
            _persist_job(job_id, meta)

    # If agent provided container info, persist to job_info.json and job_track.json
    if {"container_id", "container_name", "exit_code", "error", "details"} & extra.keys():
        info_path = _info_path(job_id)
        info = {}
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
        if worker:
            info["target_host"] = worker
        if "container_id" in extra:
            info["container_id"] = extra["container_id"]
        if "container_name" in extra:
            info["container_name"] = extra["container_name"]
        if "exit_code" in extra:
            info["exit_code"] = extra["exit_code"]
        if "error" in extra:
            info["error"] = extra["error"]
        if "details" in extra and isinstance(extra["details"], dict):
            info["details"] = extra["details"]
        with open(info_path, "w") as f:
            json.dump(info, f, indent=2)
        _LOGGER.debug("Persisted container metadata for job %s", job_id)

        tracked = job_utils.load_jobs()
        if job_id in tracked:
            updated = tracked[job_id]
            if "container_id" in extra:
                updated["container_id"] = extra["container_id"]
            if "container_name" in extra:
                updated["container_name"] = extra["container_name"]
            if "exit_code" in extra:
                updated["exit_code"] = extra["exit_code"]
            if "error" in extra:
                updated["error"] = extra["error"]
            if "details" in extra and isinstance(extra["details"], dict):
                updated["details"] = extra["details"]
            _persist_job(job_id, updated)
            _LOGGER.debug("Updated tracked metadata for job %s", job_id)
        elif job_id in jobs:
            # fall back to the in-memory version if the track file is missing
            meta = jobs[job_id]
            if "container_id" in extra:
                meta["container_id"] = extra["container_id"]
            if "container_name" in extra:
                meta["container_name"] = extra["container_name"]
            if "exit_code" in extra:
                meta["exit_code"] = extra["exit_code"]
            if "error" in extra:
                meta["error"] = extra["error"]
            if "details" in extra and isinstance(extra["details"], dict):
                meta["details"] = extra["details"]
            _persist_job(job_id, meta)
            _LOGGER.debug("Updated in-memory metadata for job %s", job_id)

    return {"ok": True}
