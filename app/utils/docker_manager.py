# app/utils/docker_manager.py
import docker
from app.models.job import SimulationRequest
from app.status import JobStatus


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

def get_container_status(container_id: str) -> tuple[str, int | None]:
    """Return raw container status and exit code."""
    client = get_docker_client()
    try:
        c = client.containers.get(container_id)
        c.reload()
        status = c.status  # 'created','running','exited','paused','dead'
        exit_code = None
        try:
            exit_code = c.attrs.get("State", {}).get("ExitCode")
        except Exception:
            pass
        return status, exit_code
    except Exception:
        return "not_found", None


# Backwards-compat convenience mapping
def get_container_phase(container_id: str) -> str:
    state, _ = get_container_status(container_id)
    if state == "running":
        return JobStatus.RUNNING.value
    if state == "exited":
        return JobStatus.FINISHED.value
    if state == "not_found":
        return JobStatus.NOT_FOUND.value
    return JobStatus.UNKNOWN.value

def stop_container(container_id: str) -> str:
    client = get_docker_client()
    try:
        client.containers.get(container_id).stop()
        return "stopped"
    except Exception:
        return "not_found"
