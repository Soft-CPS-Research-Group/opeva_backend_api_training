import docker
import os
from app.config import settings
from app.models.job import SimulationRequest
from app.utils.job_utils import load_jobs, get_job_log_path, is_valid_host

jobs = load_jobs()

def get_docker_client(target_host):
    if target_host == "local":
        return docker.DockerClient(base_url="unix://var/run/docker.sock")
    elif target_host.startswith("tcp://"):
        return docker.DockerClient(base_url=target_host)
    else:
        return docker.DockerClient(base_url=f"ssh://{target_host}")

def run_simulation(job_id, request: SimulationRequest, target_host):
    if not is_valid_host(target_host):
        raise ValueError(f"Invalid host: {target_host}")

    # RESOLVE o host real a partir do nome (se aplicável)
    host_entry = next((h for h in settings.AVAILABLE_HOSTS if h["name"] == target_host), None)
    if host_entry:
        docker_host = host_entry["host"]
    else:
        docker_host = target_host

    client = get_docker_client(docker_host)

    volumes = {settings.VM_SHARED_DATA: {"bind": "/data", "mode": "rw"}}
    container_name = f"opeva_sim_{job_id}_{request.job_name}"

    try:
        client.containers.get(container_name).remove(force=True)
    except docker.errors.NotFound:
        pass

    return client.containers.run(
        image="calof/opeva_simulator:latest",
        name=container_name,
        command=f"--config /data/{request.config_path} --job_id {job_id}",
        volumes=volumes,
        detach=True
    )


def get_container_status(container_id, target_host):
    host_entry = next((h for h in settings.AVAILABLE_HOSTS if h["name"] == target_host), None)
    docker_host = host_entry["host"] if host_entry else target_host
    client = get_docker_client(docker_host)
    try:
        return client.containers.get(container_id).status
    except:
        return "not_found"

def stop_container(container_id, target_host):
    host_entry = next((h for h in settings.AVAILABLE_HOSTS if h["name"] == target_host), None)
    docker_host = host_entry["host"] if host_entry else target_host
    client = get_docker_client(docker_host)
    try:
        client.containers.get(container_id).stop()
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
    yield f"Log not found for container ID: {container_id}"