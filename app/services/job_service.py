import json
import os
import re
from uuid import uuid4

import ray
import yaml
from fastapi import HTTPException

from app.config import settings
from app.models.job import JobLaunchRequest, SimulationRequest
from app.utils import docker_manager, file_utils, job_utils

# --- Lazy Ray Initialization ---
_ray_initialized = False

def ensure_ray_initialized():
    global _ray_initialized
    if not _ray_initialized:
        try:
            ray.init(address=settings.RAY_ADDRESS, ignore_reinit_error=True, runtime_env={"working_dir": "/app"})
            _ray_initialized = True
        except Exception as exc:
            raise RuntimeError(f"Could not connect to Ray at {settings.RAY_ADDRESS}") from exc

# --- Ray Remote Task ---
@ray.remote
def run_simulation_task(job_id: str, sim_request_dict: dict):
    from app.models.job import SimulationRequest
    from app.utils import docker_manager

    sim_request = SimulationRequest(**sim_request_dict)
    container = docker_manager.run_simulation(job_id, sim_request)
    return {"container_id": container.id, "container_name": container.name}

# --- Job Memory ---
jobs = job_utils.load_jobs()

# --- Launch Simulation ---
async def launch_simulation(request: JobLaunchRequest):
    ensure_ray_initialized()

    job_id = str(uuid4())

    # Step 1: Handle config file
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

    # Step 2: Extract job info
    experiment_name = config.get("experiment", {}).get("name", "UnnamedExperiment")
    run_name = config.get("experiment", {}).get("run_name", "UnnamedRun")
    job_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', f"{experiment_name}-{run_name}")

    if not config_path.startswith("configs/"):
        config_path = f"configs/{config_path}"

    # Step 3: Build SimulationRequest
    sim_request = SimulationRequest(config_path=config_path, job_name=job_name)

    # Step 4: Launch Ray task
    try:
        task = run_simulation_task.remote(job_id, sim_request.dict())
        result = ray.get(task, timeout=300)  # 5-minute timeout
    except ray.exceptions.GetTimeoutError:
        raise HTTPException(status_code=504, detail="Simulation task timed out")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ray task failed: {str(exc)}")

    # Step 5: Save metadata
    job_metadata = {
        "container_id": result["container_id"],
        "container_name": result["container_name"],
        "job_name": job_name,
        "config_path": config_path,
        "experiment_name": experiment_name,
        "run_name": run_name,
        "ray_task_id": task.hex(),
    }

    job_utils.save_job(job_id, job_metadata)
    job_utils.save_job_info(
        job_id,
        job_name,
        config_path,
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
        "status": "launched",
        "job_name": job_name,
        "ray_task_id": task.hex(),
    }

# --- Get container status ---
def get_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": docker_manager.get_container_status(job["container_id"]) }

# --- Get result files ---
def get_result(job_id: str):
    return file_utils.collect_results(job_id)

# --- Get progress file ---
def get_progress(job_id: str):
    return file_utils.read_progress(job_id)

# --- Get live logs from container ---
def get_logs(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return docker_manager.stream_container_logs(job["container_id"])

# --- Stop container/job ---
def stop_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"message": docker_manager.stop_container(job["container_id"])}

# --- List all jobs ---
def list_jobs():
    result = []
    for job_id, job in jobs.items():
        info_path = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "job_info.json")
        info = {}
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
        result.append({
            "job_id": job_id,
            "status": docker_manager.get_container_status(job["container_id"]),
            "job_info": info
        })
    return result

# --- Get full job info ---
def get_job_info(job_id: str):
    info_path = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "job_info.json")
    if not os.path.exists(info_path):
        raise HTTPException(status_code=404, detail="Job info not found")
    with open(info_path) as f:
        return json.load(f)

# --- Delete job ---
def delete_job(job_id: str):
    success = job_utils.delete_job_by_id(job_id, jobs)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found or already deleted")
    return {"message": f"Job {job_id} deleted successfully"}

# --- Read file-based logs ---
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

