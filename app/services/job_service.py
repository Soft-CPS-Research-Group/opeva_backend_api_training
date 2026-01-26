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
    JobStatus.STOP_REQUESTED.value,
}

def _refresh_jobs():
    """Reload the job registry from disk to keep multiple workers in sync."""
    try:
        disk_jobs = job_utils.load_jobs()
        if isinstance(disk_jobs, dict):
            jobs.clear()
            jobs.update(disk_jobs)
    except Exception:
        _LOGGER.warning("Failed to refresh jobs registry from disk", exc_info=True)


def _persist_job(job_id: str, metadata: dict):
    """Persist job metadata to disk and mirror it in the in-memory cache."""
    _LOGGER.debug("Persisting job %s (status=%s)", job_id, metadata.get("status"))
    job_utils.save_job(job_id, metadata)
    jobs[job_id] = metadata

def _job_exists(job_id: str) -> bool:
    if job_id in jobs:
        return True
    try:
        return job_id in job_utils.load_jobs()
    except Exception:
        return False

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


def _status_last_update(job_id: str) -> Optional[float]:
    payload = _read_status_payload(job_id)
    if payload:
        ts = payload.get("status_updated_at")
        if isinstance(ts, (int, float)):
            return float(ts)
    path = _status_path(job_id)
    if os.path.exists(path):
        try:
            return os.path.getmtime(path)
        except OSError:
            return None
    return None

def _write_status(job_id: str, status: str, extra: dict | None = None):
    """Persist status to disk and update the in-memory jobs cache."""
    prev = _read_status_file(job_id)
    if prev and prev != status and not can_transition(prev, status):
        _LOGGER.error("Invalid status transition for job %s: %s -> %s", job_id, prev, status)
        raise ValueError(f"Invalid status transition {prev} -> {status}")
    _LOGGER.info(
        "Job %s status change %s -> %s (extras=%s)",
        job_id,
        prev,
        status,
        sorted((extra or {}).keys()),
    )
    extra_payload = dict(extra or {})
    extra_payload.setdefault("status_updated_at", time.time())
    job_utils.write_status_file(job_id, status, extra_payload)
    if job_id in jobs:
        jobs[job_id]["status"] = status
        if extra_payload:
            jobs[job_id].update(extra_payload)
        job_utils.save_job(job_id, jobs[job_id])


def _force_status(job_id: str, status: str, extra: dict | None = None) -> None:
    """Write status without enforcing state transitions (ops override)."""
    extra_payload = dict(extra or {})
    extra_payload.setdefault("status_updated_at", time.time())
    job_utils.write_status_file(job_id, status, extra_payload)
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    if meta:
        meta["status"] = status
        meta.update(extra_payload)
        job_utils.save_job(job_id, meta)
        jobs[job_id] = meta

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


def _safe_filename(value: str) -> str:
    cleaned = os.path.normpath(value).lstrip(os.sep)
    if cleaned.startswith("..") or os.path.isabs(value) or os.sep in cleaned:
        raise HTTPException(400, "Invalid file name")
    return cleaned


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
                (job.get("target_host") == host)
                and job.get("status")
                in (JobStatus.RUNNING.value, JobStatus.DISPATCHED.value, JobStatus.STOP_REQUESTED.value)
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
        if status not in (JobStatus.DISPATCHED.value, JobStatus.RUNNING.value, JobStatus.STOP_REQUESTED.value):
            continue

        last_update = _status_last_update(job_id)
        if last_update and (now - last_update) > settings.JOB_STATUS_TTL:
            preferred = meta.get("preferred_host") or meta.get("target_host")
            require_host = bool(meta.get("require_host", bool(preferred)))
            if status == JobStatus.DISPATCHED.value:
                job_utils.enqueue_job({
                    "job_id": job_id,
                    "preferred_host": preferred,
                    "require_host": require_host,
                })
                meta["status"] = JobStatus.QUEUED.value
                _persist_job(job_id, meta)
                _write_status(
                    job_id,
                    JobStatus.QUEUED.value,
                    {"requeued_from": host, "preferred_host": preferred, "stale_status": True},
                )
                _LOGGER.warning("Re-queued dispatched job %s due to stale status update", job_id)
            else:
                _write_status(job_id, JobStatus.FAILED.value, {"error": "stale_status", "last_host": host})
                meta["status"] = JobStatus.FAILED.value
                _persist_job(job_id, meta)
                _LOGGER.warning("Marked job %s as failed due to stale status update", job_id)
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
        require_host = bool(meta.get("require_host", bool(preferred)))
        if status == JobStatus.DISPATCHED.value:
            # Put back in queue for another worker to pick up
            job_utils.enqueue_job({
                "job_id": job_id,
                "preferred_host": preferred,
                "require_host": require_host,
            })
            meta["status"] = JobStatus.QUEUED.value
            _persist_job(job_id, meta)
            _write_status(job_id, JobStatus.QUEUED.value, {"requeued_from": host, "preferred_host": preferred})
            _LOGGER.warning("Re-queued stale dispatched job %s from offline host %s", job_id, host)
        elif status in (JobStatus.RUNNING.value, JobStatus.STOP_REQUESTED.value):
            _write_status(job_id, JobStatus.FAILED.value, {"error": "worker_offline", "last_host": host})
            meta["status"] = JobStatus.FAILED.value
            _persist_job(job_id, meta)
            _LOGGER.warning("Marked job %s as failed because host %s is offline", job_id, host)


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
        file_name = _safe_filename(file_name)
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
        "require_host": bool(preferred_host),
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

    status_now = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)

    job_utils.remove_from_queue(job_id)

    if status_now in (JobStatus.LAUNCHING.value, JobStatus.QUEUED.value):
        _write_status(job_id, JobStatus.CANCELED.value)
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.CANCELED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Job canceled from queue"}

    if status_now in (JobStatus.DISPATCHED.value, JobStatus.RUNNING.value):
        _write_status(job_id, JobStatus.STOP_REQUESTED.value, {"stop_requested": True})
        if job_id in jobs:
            jobs[job_id]["status"] = JobStatus.STOP_REQUESTED.value
            _persist_job(job_id, jobs[job_id])
        return {"message": "Stop requested; worker should terminate the job"}

    if status_now == JobStatus.STOP_REQUESTED.value:
        return {"message": "Stop already requested"}

    if status_now in (
        JobStatus.FINISHED.value,
        JobStatus.FAILED.value,
        JobStatus.STOPPED.value,
        JobStatus.CANCELED.value,
    ):
        return {"message": f"Job already finished ({status_now})"}

    return {"message": f"Job is {status_now}; nothing to stop"}

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
    ok = job_utils.delete_job_by_id(job_id)
    if not ok:
        raise HTTPException(500, "Failed to delete job")
    jobs.pop(job_id, None)
    return {"message": f"Job {job_id} deleted successfully"}

def get_hosts():
    _refresh_jobs()
    _mark_stale_jobs()
    return {
        "available_hosts": settings.AVAILABLE_HOSTS,
        "hosts": _host_status_snapshot(),
    }


def ops_requeue_job(
    job_id: str,
    force: bool = False,
    preferred_host: Optional[str] = None,
    require_host: Optional[bool] = None,
):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status_now = _read_status_file(job_id) or meta.get("status", JobStatus.UNKNOWN.value)

    if preferred_host:
        if not job_utils.is_valid_host(preferred_host):
            raise HTTPException(400, f"Unknown host '{preferred_host}'. Allowed: {settings.AVAILABLE_HOSTS}")
    preferred = preferred_host or meta.get("preferred_host") or meta.get("target_host")
    if require_host is None:
        require_host = bool(meta.get("require_host", bool(preferred)))

    if not force:
        if status_now in (
            JobStatus.FINISHED.value,
            JobStatus.FAILED.value,
            JobStatus.STOPPED.value,
            JobStatus.CANCELED.value,
        ):
            raise HTTPException(409, f"Job already terminal ({status_now}); use force to requeue")
        if status_now in (JobStatus.RUNNING.value, JobStatus.STOP_REQUESTED.value):
            raise HTTPException(409, f"Job is {status_now}; use force to requeue")

    prev_host = meta.get("target_host")
    job_utils.remove_from_queue(job_id)
    job_utils.enqueue_job({
        "job_id": job_id,
        "preferred_host": preferred,
        "require_host": require_host,
    })

    meta["status"] = JobStatus.QUEUED.value
    meta["preferred_host"] = preferred
    meta["require_host"] = require_host
    meta["target_host"] = preferred if require_host else None
    _persist_job(job_id, meta)

    extra = {
        "requeued_by_ops": True,
        "force": force,
        "requeued_from": prev_host,
        "preferred_host": preferred,
    }
    if force:
        _force_status(job_id, JobStatus.QUEUED.value, extra)
    else:
        _write_status(job_id, JobStatus.QUEUED.value, extra)

    return {"message": "Job requeued", "job_id": job_id, "status": JobStatus.QUEUED.value}


def ops_fail_job(job_id: str, reason: str = "ops_failed", force: bool = False):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status_now = _read_status_file(job_id) or meta.get("status", JobStatus.UNKNOWN.value)

    if not force:
        if status_now in (
            JobStatus.FINISHED.value,
            JobStatus.FAILED.value,
            JobStatus.STOPPED.value,
            JobStatus.CANCELED.value,
        ):
            raise HTTPException(409, f"Job already terminal ({status_now})")
        if status_now in (JobStatus.QUEUED.value, JobStatus.LAUNCHING.value):
            raise HTTPException(409, f"Job is {status_now}; use cancel or force to fail")

    job_utils.remove_from_queue(job_id)
    meta["status"] = JobStatus.FAILED.value
    meta["error"] = reason
    _persist_job(job_id, meta)

    extra = {"error": reason, "failed_by_ops": True, "force": force}
    if force:
        _force_status(job_id, JobStatus.FAILED.value, extra)
    else:
        _write_status(job_id, JobStatus.FAILED.value, extra)

    return {"message": "Job failed", "job_id": job_id, "status": JobStatus.FAILED.value}


def ops_cancel_job(job_id: str, reason: str = "ops_canceled", force: bool = False):
    _refresh_jobs()
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
    status_now = _read_status_file(job_id) or meta.get("status", JobStatus.UNKNOWN.value)

    if not force and status_now in (
        JobStatus.FINISHED.value,
        JobStatus.FAILED.value,
        JobStatus.STOPPED.value,
        JobStatus.CANCELED.value,
    ):
        raise HTTPException(409, f"Job already terminal ({status_now})")

    job_utils.remove_from_queue(job_id)
    meta["status"] = JobStatus.CANCELED.value
    meta["error"] = reason
    _persist_job(job_id, meta)

    extra = {"error": reason, "canceled_by_ops": True, "force": force}
    if force:
        _force_status(job_id, JobStatus.CANCELED.value, extra)
    else:
        _write_status(job_id, JobStatus.CANCELED.value, extra)

    return {"message": "Job canceled", "job_id": job_id, "status": JobStatus.CANCELED.value}


def ops_cleanup_queue() -> dict:
    _refresh_jobs()
    removed: list[str] = []
    wdir = settings.QUEUE_DIR
    if not os.path.isdir(wdir):
        return {"removed": removed, "count": 0}
    for fname in os.listdir(wdir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(wdir, fname)
        try:
            with open(path) as f:
                payload = json.load(f)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        job_id = payload.get("job_id") or fname.rsplit(".", 1)[0]
        meta = jobs.get(job_id) or job_utils.load_jobs().get(job_id, {})
        status_now = _read_status_file(job_id) or meta.get("status")
        if not meta or status_now not in (JobStatus.QUEUED.value, JobStatus.LAUNCHING.value):
            try:
                os.remove(path)
                removed.append(job_id)
            except OSError:
                continue
    return {"removed": removed, "count": len(removed)}

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
        _LOGGER.warning("Queue entry for unknown job %s; skipping dispatch", job_id)
        return None

    status_now = _read_status_file(job_id) or meta.get("status")
    if status_now not in (JobStatus.QUEUED.value, JobStatus.LAUNCHING.value):
        _LOGGER.warning("Skipping job %s with status %s (queue entry likely stale)", job_id, status_now)
        return None

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
            _write_status(job_id, JobStatus.FAILED.value, {"error": "missing_config"})
            _LOGGER.error("Missing config path for job %s; marked failed", job_id)
            return None

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
    if not _job_exists(job_id):
        raise HTTPException(404, f"Job {job_id} not found")
    try:
        JobStatus(status)
    except ValueError:
        raise HTTPException(400, f"Unknown status '{status}'")
    _LOGGER.info(
        "Agent reported status for job %s: %s (extra keys=%s)",
        job_id,
        status,
        sorted(extra.keys()),
    )
    try:
        _write_status(job_id, status, extra)
    except ValueError as exc:
        raise HTTPException(409, str(exc))

    if status != JobStatus.QUEUED.value:
        job_utils.remove_from_queue(job_id)

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
