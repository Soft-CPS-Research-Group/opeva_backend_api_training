import docker
import os
from app.config import CONTAINER_SHARED_DATA
from app.utils import collect_results, read_progress

client = docker.from_env()

def run_simulation(job_id, request):
    try:
        volumes = {
            "/opt/opeva_shared_data": {"bind": "/data", "mode": "rw"}
        }
        environment = {
            "PARAM1": request.param1,
            "PARAM2": request.param2,
            "MLFLOW_TRACKING_URI": "http://mlflow:5000"
        }
        command = f"python wrapper.py --config /data/configs/{request.config_file} --job_id {job_id}"

        container = client.containers.run(
            image="your-simulation-image:latest",
            command=command,
            volumes=volumes,
            environment=environment,
            detach=True,
            stdout=True,
            stderr=True
        )

        # Immediately stream logs to file
        log_file_path = f"/opt/opeva_shared_data/logs/{job_id}.log"
        with open(log_file_path, "wb") as f:
            for log in container.logs(stream=True):
                f.write(log)

        return container
    except Exception as e:
        raise RuntimeError(f"Error launching simulation: {e}")

def get_container_status(container_id):
    try:
        container = client.containers.get(container_id)
        container.reload()
        return container.status
    except docker.errors.NotFound:
        return "not_found"
    except Exception as e:
        return f"error: {str(e)}"

def stop_container(container_id):
    try:
        container = client.containers.get(container_id)
        container.stop()
        return "stopped"
    except Exception as e:
        return f"error: {str(e)}"

def get_simulation_result(job_id):
    return collect_results(job_id)

def get_simulation_progress(job_id):
    return read_progress(job_id)

def stream_container_logs(container_id):
    try:
        container = client.containers.get(container_id)
        for log in container.logs(stream=True, follow=True):
            yield log.decode('utf-8')
    except Exception as e:
        yield f"Error streaming logs: {e}"
