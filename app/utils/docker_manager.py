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

def get_container_phase(container_id: str) -> tuple[str, int | None]:
    """
    Return (phase, exit_code) where phase is one of:
    'running', 'exited', 'unknown', 'not_found'.
    exit_code is None unless phase == 'exited'.
    """
    client = get_docker_client()
    try:
        c = client.containers.get(container_id)
        # ensure attrs are fresh
        c.reload()
        status = c.status  # 'created','running','exited','paused','dead'
        if status == "running":
            return ("running", None)
        if status in ("exited", "dead"):
            code = None
            try:
                code = c.attrs.get("State", {}).get("ExitCode")
            except Exception:
                pass
            return ("exited", code)
        if status in ("created", "paused"):
            return ("unknown", None)
        return ("unknown", None)
    except Exception:
        return ("not_found", None)

def get_container_status(container_id: str) -> str:
    """(Optional legacy) Return raw-ish status for compatibility."""
    phase, _ = get_container_phase(container_id)
    if phase == "running":
        return JobStatus.RUNNING.value
    if phase == "exited":
        # unknown finish type without exit code mapping; job_service will refine
        return JobStatus.UNKNOWN.value
    if phase == "not_found":
        return JobStatus.NOT_FOUND.value
    return JobStatus.UNKNOWN.value

def stop_container(container_id: str) -> str:
    client = get_docker_client()
    try:
        client.containers.get(container_id).stop()
        return "stopped"
    except Exception:
        return "not_found"
