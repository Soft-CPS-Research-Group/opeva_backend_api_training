import docker
import os
from app.config import VM_SHARED_DATA
from app.models import SimulationRequest
from app.utils import load_jobs  # <-- required for log streaming

jobs = load_jobs()

def get_docker_client(target_host: str):
    if target_host == "local":
        return docker.from_env()
    else:
        try:
            client = docker.DockerClient(base_url=f"ssh://{target_host}")
            client.containers.list()
            return client
        except Exception as e:
            raise RuntimeError(f"Could not reach Docker daemon on '{target_host}': {e}")

def run_simulation(job_id, request: SimulationRequest, target_host: str):
    shared_host = VM_SHARED_DATA
    shared_container = "/data"

    volumes = {
        shared_host: {"bind": shared_container, "mode": "rw"}
    }

    command = (
        f"python wrapper.py "
        f"--config /data/{request.config_path} "
        f"--job_id {job_id}"
    )

    print("FFFF - 5")

    docker_client = get_docker_client(target_host)

    print("DEBUG: docker_client =", docker_client)
    print("DEBUG: docker_client.containers =", docker_client.containers)
    print("DEBUG: type(docker_client.containers) =", type(docker_client.containers))

    container = docker_client.containers.run(
        image="calof/opeva_simulator:latest",
        command=command,
        volumes=volumes,
        detach=True,
        stdout=True,
        stderr=True
    )

    return container

def get_container_status(container_id):
    try:
        container = docker.from_env().containers.get(container_id)
        container.reload()
        return container.status
    except docker.errors.NotFound:
        return "not_found"
    except Exception as e:
        return f"error: {str(e)}"

def stop_container(container_id):
    try:
        container = docker.from_env().containers.get(container_id)
        container.stop()
        return "stopped"
    except Exception as e:
        return f"error: {str(e)}"

def stream_container_logs(container_id):
    try:
        log_path = os.path.join(VM_SHARED_DATA, "jobs")
        for job_id, cid in jobs.items():
            if cid == container_id:
                path = os.path.join(log_path, job_id, "logs", f"{job_id}.log")
                if os.path.exists(path):
                    with open(path) as f:
                        for line in f:
                            yield line
                else:
                    yield f"Log file not found for job_id {job_id}"
                return
        yield f"Job not found for container ID: {container_id}"
    except Exception as e:
        yield f"Error reading logs: {e}"
