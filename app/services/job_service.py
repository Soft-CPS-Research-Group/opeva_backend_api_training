# app/services/job_service.py
import os, re, json, yaml
from uuid import uuid4
from typing import Generator, Optional
from fastapi import HTTPException, Response

from app.config import settings
from app.models.job import SimulationRequest, JobLaunchRequest
from app.utils import docker_manager, job_utils, file_utils
from app.status import JobStatus

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

def _read_status_file(job_id: str) -> Optional[str]:
    p = _status_path(job_id)
    if os.path.exists(p):
        try:
            with open(p) as f:
                return json.load(f).get("status")
        except Exception:
            return None
    return None

def _write_status(job_id: str, status: str, extra: dict | None = None):
    job_utils.write_status_file(job_id, status, extra or {})
    jobs = job_utils.load_jobs()
    if job_id in jobs:
        jobs[job_id]["status"] = status
        if extra:
            jobs[job_id].update(extra)
        with open(settings.JOB_TRACK_FILE, "w") as f:
            json.dump(jobs, f, indent=2)

# ---------- API: launch ----------
async def launch_simulation(request: JobLaunchRequest):
    job_utils.ensure_directories()

    if not job_utils.is_valid_host(request.target_host):
        raise HTTPException(400, f"Unknown host '{request.target_host}'. Allowed: {settings.AVAILABLE_HOSTS}")

    job_id = str(uuid4())

    # config
    if request.config_path:
        config_path = request.config_path
        with open(os.path.join(settings.CONFIGS_DIR, config_path)) as f:
            config = yaml.safe_load(f)
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
        "target_host": request.target_host,
        "experiment_name": experiment_name,
        "run_name": run_name,
        "status": JobStatus.LAUNCHING.value,
    }
    job_utils.save_job(job_id, meta)
    job_utils.save_job_info(job_id, job_name, config_path, request.target_host,
                            container_id="", container_name="", exp=experiment_name, run=run_name)
    _write_status(job_id, JobStatus.LAUNCHING.value)

    if request.target_host == "local":
        sim_req = SimulationRequest(config_path=config_path, job_name=job_name)
        container = docker_manager.run_simulation(job_id, sim_req, settings.VM_SHARED_DATA)
        meta.update({"container_id": container.id, "container_name": container.name, "status": JobStatus.RUNNING.value})
        job_utils.save_job(job_id, meta)
        job_utils.save_job_info(job_id, job_name, config_path, "local",
                                container.id, container.name, experiment_name, run_name)
        _write_status(job_id, JobStatus.RUNNING.value)
        return {
            "job_id": job_id,
            "container_id": container.id,
            "status": "launched",
            "host": "local",
            "job_name": job_name,
        }

    # enqueue for agent (agent applies GPU/network defaults)
    sim_req = SimulationRequest(config_path=config_path, job_name=job_name)
    payload = {
        "job_id": job_id,
        "image": "calof/opeva_simulator:latest",
        "container_name": f"opeva_sim_{job_id}_{job_name}",
        "command": f"--config /data/{sim_req.config_path} --job_id {job_id}",
        "volumes": [
            {"host": settings.VM_SHARED_DATA, "container": "/data", "mode": "rw"}
        ],
        "env": {
            "MLFLOW_TRACKING_URI": "http://MAIN-SERVER:5000"
        }
    }
    worker_id = request.target_host
    job_utils.enqueue_job_for_agent(worker_id, payload)
    meta.update({"status": JobStatus.QUEUED.value})
    job_utils.save_job(job_id, meta)
    _write_status(job_id, JobStatus.QUEUED.value, {"worker_id": worker_id})
    return {"job_id": job_id, "status": JobStatus.QUEUED.value, "host": worker_id, "job_name": job_name}

# ---------- API: status/result/progress/logs ----------
def get_status(job_id: str):
    jobs = job_utils.load_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    if job.get("target_host") == "local" and job.get("container_id"):
        docker_status = docker_manager.get_container_status(job["container_id"])
        if docker_status and docker_status != JobStatus.NOT_FOUND.value:
            return {"job_id": job_id, "status": docker_status}
    status = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)
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
    jobs = job_utils.load_jobs()
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    host = job.get("target_host", "local")
    status_now = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)

    if host == "local":
        cid = job.get("container_id")
        if cid:
            msg = docker_manager.stop_container(cid)
            _write_status(job_id, JobStatus.STOPPED.value)
            return {"message": msg}
        _write_status(job_id, JobStatus.CANCELED.value)
        return {"message": "Local job canceled (not running)"}
    else:
        # cancel if still queued
        if status_now == JobStatus.QUEUED.value:
            qfile = os.path.join(settings.QUEUE_DIR, host, f"{job_id}.json")
            if os.path.exists(qfile):
                os.remove(qfile)
            _write_status(job_id, JobStatus.CANCELED.value)
            return {"message": "Remote job canceled (was queued)"}
        if status_now == JobStatus.RUNNING.value:
            return {"message": "Remote stop requires agent support; not implemented"}
        return {"message": f"Remote job is {status_now}; nothing to stop"}

def list_jobs():
    jobs = job_utils.load_jobs()
    result = []
    for job_id, job in jobs.items():
        info = {}
        ipath = _info_path(job_id)
        if os.path.exists(ipath):
            with open(ipath) as f:
                info = json.load(f)

        if job.get("target_host") == "local" and job.get("container_id"):
            status = docker_manager.get_container_status(job["container_id"])
            if not status or status == JobStatus.NOT_FOUND.value:
                status = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)
        else:
            status = _read_status_file(job_id) or job.get("status", JobStatus.UNKNOWN.value)

        result.append({"job_id": job_id, "status": status, "job_info": info})
    return result

def get_job_info(job_id: str):
    p = _info_path(job_id)
    if not os.path.exists(p):
        raise HTTPException(404, "Job info not found")
    with open(p) as f:
        return json.load(f)

def delete_job(job_id: str):
    jobs = job_utils.load_jobs()
    if job_id not in jobs:
        raise HTTPException(404, "Job not found or already deleted")
    ok = job_utils.delete_job_by_id(job_id, jobs)
    if not ok:
        raise HTTPException(500, "Failed to delete job")
    return {"message": f"Job {job_id} deleted successfully"}

def get_hosts():
    return {"available_hosts": settings.AVAILABLE_HOSTS}

# ---------- hooks used by agent endpoints ----------
def agent_next_job(worker_id: str):
    job = job_utils.agent_pop_next_job(worker_id)
    if not job:
        return None
    _write_status(job["job_id"], JobStatus.DISPATCHED.value, {"worker_id": worker_id})
    return job

def agent_update_status(job_id: str, status: str, extra: dict | None = None):
    extra = extra or {}
    _write_status(job_id, status, extra)

    # If agent provided container info, persist to job_info.json and job_track.json
    if "container_id" in extra or "container_name" in extra:
        info_path = _info_path(job_id)
        info = {}
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
        if "container_id" in extra:
            info["container_id"] = extra["container_id"]
        if "container_name" in extra:
            info["container_name"] = extra["container_name"]
        with open(info_path, "w") as f:
            json.dump(info, f, indent=2)

        jobs = job_utils.load_jobs()
        if job_id in jobs:
            if "container_id" in extra:
                jobs[job_id]["container_id"] = extra["container_id"]
            if "container_name" in extra:
                jobs[job_id]["container_name"] = extra["container_name"]
            with open(settings.JOB_TRACK_FILE, "w") as f:
                json.dump(jobs, f, indent=2)

    return {"ok": True}
