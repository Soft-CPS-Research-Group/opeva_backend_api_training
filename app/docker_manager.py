import docker
import os
from app.config import VM_SHARED_DATA
from app.models import SimulationRequest
from app.utils import load_jobs, get_job_log_path

jobs = load_jobs()

def get_docker_client(target_host: str):
    if target_host == "local":
        return docker.DockerClient(base_url="unix://var/run/docker.sock")
    return docker.DockerClient(base_url=f"ssh://{target_host}")

def run_simulation(job_id, request: SimulationRequest, target_host: str):
    docker_client = get_docker_client(target_host)
    volumes = {
        VM_SHARED_DATA: {"bind": "/data", "mode": "rw"}
    }
    container_name = f"opeva_sim_{job_id}_{request.job_name}"
    try:
        docker_client.containers.get(container_name).remove(force=True)
    except docker.errors.NotFound:
        pass
    container = docker_client.containers.run(
        image="calof/opeva_simulator:latest",
        name=container_name,
        command=f"--config /data/{request.config_path} --job_id {job_id}",
        volumes=volumes,
        detach=True
    )
    return container

def get_container_status(container_id):
    try:
        return docker.from_env().containers.get(container_id).status
    except:
        return "not_found"

def stop_container(container_id):
    try:
        docker.from_env().containers.get(container_id).stop()
        return "stopped"
    except:
        return "not_found"

def stream_container_logs(container_id):
    for job_id, meta in jobs.items():
        if meta.get("container_id") == container_id:
            log_file = get_job_log_path(job_id)
            if os.path.exists(log_file):
                with open(log_file) as f:
                    for line in f:
                        yield line
                return
    yield f"Log file not found for container ID: {container_id}"
