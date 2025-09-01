import json
import os
import re
import time
from uuid import uuid4

import ray
import yaml
from fastapi import HTTPException

from app.config import settings
from app.models.job import JobLaunchRequest, SimulationRequest, JobStatus
from app.utils import docker_manager, file_utils, job_utils

ray_address = settings.RAY_ADDRESS
try:
    ray.init(address=ray_address, ignore_reinit_error=True)
except Exception as exc:
    raise RuntimeError(f"Could not connect to Ray at {ray_address}") from exc

@ray.remote
def run_simulation_task(job_id, sim_request_dict, target_host):
    sim_request = SimulationRequest(**sim_request_dict)
    container = docker_manager.run_simulation(job_id, sim_request, target_host)
    return {"container_id": container.id, "container_name": container.name}

jobs = job_utils.load_jobs()

async def launch_simulation(request: JobLaunchRequest):
    job_id = str(uuid4())
    if request.config_path:
        config_path = request.config_path
        with open(os.path.join(settings.CONFIGS_DIR, config_path)) as f:
            config = yaml.safe_load(f)
    elif request.config:
        file_name = request.save_as or f"{job_id}.yaml"
        config_path = file_utils.save_config_dict(request.config, file_name)
        config = request.config
    else:
        raise ValueError("Missing config or config_path")

    experiment_name = config.get("experiment", {}).get("name", "UnnamedExperiment")
    run_name = config.get("experiment", {}).get("run_name", "UnnamedRun")
    job_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', f"{experiment_name}-{run_name}")

    if not config_path.startswith("configs/"):
        config_path = f"configs/{config_path}"

    sim_request = SimulationRequest(config_path=config_path, job_name=job_name)
    job_metadata = {
        "job_name": job_name,
        "config_path": config_path,
        "target_host": request.target_host,
        "experiment_name": experiment_name,
        "run_name": run_name,
        "status": JobStatus.PENDING.value,
    }

    job_utils.save_job(job_id, job_metadata)
    jobs[job_id] = job_metadata

    task = run_simulation_task.remote(job_id, sim_request.dict(), request.target_host)
    result = ray.get(task)

    status = JobStatus.DISPATCHED.value
    if request.target_host == "local":
        status = JobStatus.RUNNING.value

    job_metadata.update({
        "container_id": result["container_id"],
        "container_name": result["container_name"],
        "ray_task_id": task.hex(),
        "status": status,
    })
    job_utils.save_job(job_id, job_metadata)
    job_utils.save_job_info(
        job_id,
        job_name,
        config_path,
        request.target_host,
        result["container_id"],
        result["container_name"],
        experiment_name,
        run_name,
        task.hex(),
    )
    jobs[job_id] = job_metadata

    return {
        "job_id": job_id,
        "container_id": result["container_id"],
        "status": status,
        "host": request.target_host,
        "job_name": job_name,
        "ray_task_id": task.hex(),
    }

def get_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    container_status, exit_code = docker_manager.get_container_status(job["container_id"])
    if container_status == "running":
        status = JobStatus.RUNNING.value
    elif container_status == "exited":
        status = JobStatus.COMPLETED.value if exit_code == 0 else JobStatus.FAILED.value
    elif container_status in ("created", "restarting"):
        if job.get("target_host") == "local":
            status = JobStatus.RUNNING.value
        else:
            status = JobStatus.DISPATCHED.value
    elif container_status == "stopped":
        status = JobStatus.STOPPED.value
    elif container_status == "not_found":
        status = job.get("status", JobStatus.FAILED.value)
    else:
        status = JobStatus.FAILED.value
    job["status"] = status
    job_utils.save_job(job_id, job)
    return {"job_id": job_id, "status": status}

def get_result(job_id: str):
    return file_utils.collect_results(job_id)

def get_progress(job_id: str):
    return file_utils.read_progress(job_id)

def get_logs(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return docker_manager.stream_container_logs(job["container_id"])

def stop_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    message = docker_manager.stop_container(job["container_id"])
    job["status"] = JobStatus.STOPPED.value
    job_utils.save_job(job_id, job)
    return {"message": message, "status": job["status"]}

def list_jobs():
    result = []
    for job_id in list(jobs.keys()):
        status_resp = get_status(job_id)
        info_path = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "job_info.json")
        info = {}
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
        result.append({"job_id": job_id, "status": status_resp["status"], "job_info": info})
    return result

def get_job_info(job_id: str):
    info_path = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "job_info.json")
    if not os.path.exists(info_path):
        raise HTTPException(status_code=404, detail="Job info not found")
    with open(info_path) as f:
        return json.load(f)

def delete_job(job_id: str):
    success = job_utils.delete_job_by_id(job_id, jobs)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found or already deleted")
    return {"message": f"Job {job_id} deleted successfully"}

def get_file_logs(job_id: str):
    log_dir = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "logs")
    if not os.path.exists(log_dir):
        raise HTTPException(status_code=404, detail="Log folder not found for this job")
    for filename in os.listdir(log_dir):
        if filename.endswith(".log"):
            path = os.path.join(log_dir, filename)
            def read_log():
                with open(path) as f:
                    while True:
                        line = f.readline()
                        if line:
                            yield line
                        else:
                            status_resp = get_status(job_id)["status"]
                            if status_resp == JobStatus.RUNNING.value:
                                time.sleep(0.5)
                                continue
                            break
            return read_log()
    raise HTTPException(status_code=404, detail="Log file not found in logs folder")

def get_hosts():
    return {"available_hosts": settings.AVAILABLE_HOSTS}
