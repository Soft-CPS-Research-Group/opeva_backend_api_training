# app/utils/docker_manager.py
import docker
from app.models.job import SimulationRequest

def get_docker_client():
    return docker.DockerClient(base_url="unix://var/run/docker.sock")

def run_simulation(job_id: str, request: SimulationRequest, shared_dir: str):
    client = get_docker_client()
    container_name = f"opeva_sim_{job_id}_{request.job_name}"
    try:
        client.containers.get(container_name).remove(force=True)
    except docker.errors.NotFound:
        pass

    return client.containers.run(
        image="calof/opeva_simulator:latest",
        name=container_name,
        command=f"--config /data/{request.config_path} --job_id {job_id}",
        volumes={shared_dir: {"bind": "/data", "mode": "rw"}},
        labels={"opeva.job_id": job_id, "opeva.job_name": request.job_name},
        detach=True
    )

def get_container_status(container_id: str) -> str:
    client = get_docker_client()
    try:
        return client.containers.get(container_id).status
    except Exception:
        return "not_found"

def stop_container(container_id: str) -> str:
    client = get_docker_client()
    try:
        client.containers.get(container_id).stop()
        return "stopped"
    except Exception:
        return "not_found"
