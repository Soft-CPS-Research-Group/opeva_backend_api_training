import os
import re
import yaml
import json
from uuid import uuid4
from app.models.job import SimulationRequest, JobLaunchRequest
from app.utils import docker_manager, job_utils, file_utils
from app.config import settings
from fastapi import HTTPException

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
    container = docker_manager.run_simulation(job_id, sim_request, request.target_host)

    job_metadata = {
        "container_id": container.id,
        "container_name": container.name,
        "job_name": job_name,
        "config_path": config_path,
        "target_host": request.target_host,
        "experiment_name": experiment_name,
        "run_name": run_name,
    }

    job_utils.save_job(job_id, job_metadata)
    job_utils.save_job_info(job_id, job_name, config_path, request.target_host, container.id, container.name, experiment_name, run_name)
    jobs[job_id] = job_metadata

    return {
        "job_id": job_id,
        "container_id": container.id,
        "status": "launched",
        "host": request.target_host,
        "job_name": job_name,
    }

def get_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": docker_manager.get_container_status(job["container_id"]) }

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
    return {"message": docker_manager.stop_container(job["container_id"])}

def list_jobs():
    result = []
    for job_id, job in jobs.items():
        info_path = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "job_info.json")
        info = {}
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
        result.append({"job_id": job_id, "status": docker_manager.get_container_status(job["container_id"]), "job_info": info})
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
                    for line in f:
                        yield line
            return read_log()
    raise HTTPException(status_code=404, detail="Log file not found in logs folder")

def get_hosts():
    return {"available_hosts": settings.AVAILABLE_HOSTS}
