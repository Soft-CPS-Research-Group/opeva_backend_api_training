#!/usr/bin/env python3
# agents/worker/agent.py
import os, time, requests, docker, threading, signal
from pathlib import Path
from docker.types import DeviceRequest

SERVER = os.environ.get("OPEVA_SERVER", "http://MAIN-SERVER:8000")
WORKER_ID = os.environ.get("WORKER_ID", os.uname().nodename)
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
SHARED = os.environ.get("OPEVA_SHARED_DIR", "/opt/opeva_shared_data")
DEFAULT_NETWORK = os.environ.get("OPEVA_DOCKER_NETWORK", "opeva_network")

stop_flag = False
def _sig(*_): 
    global stop_flag; stop_flag = True
signal.signal(signal.SIGTERM, _sig)
signal.signal(signal.SIGINT, _sig)

client = docker.DockerClient(base_url="unix://var/run/docker.sock")

def _log_path(job_id): 
    return f"{SHARED}/jobs/{job_id}/logs/{job_id}.log"

def _has_network(name: str) -> bool:
    try:
        client.networks.get(name)
        return True
    except Exception:
        return False

def _gpu_device_requests():
    try:
        return [DeviceRequest(count=-1, capabilities=[["gpu"]])]
    except Exception:
        return None

def run_job(job):
    job_id = job["job_id"]
    Path(f"{SHARED}/jobs/{job_id}/logs").mkdir(parents=True, exist_ok=True)

    image = job["image"]
    try:
        client.images.pull(image)
    except Exception:
        pass  # use local cache if pull fails

    # cleanup stale
    try:
        client.containers.get(job["container_name"]).remove(force=True)
    except Exception:
        pass

    volumes = { v["host"]: {"bind": v["container"], "mode": v.get("mode","rw")} 
                for v in job.get("volumes",[]) }

    device_requests = _gpu_device_requests()
    network_name = DEFAULT_NETWORK if _has_network(DEFAULT_NETWORK) else None

    env = job.get("env", {}).copy()
    env.setdefault("NVIDIA_VISIBLE_DEVICES", "all")
    env.setdefault("NVIDIA_DRIVER_CAPABILITIES", "compute,utility")

    container = client.containers.run(
        image=image,
        name=job["container_name"],
        command=job["command"],
        volumes=volumes,
        environment=env,
        network=network_name,
        device_requests=device_requests,
        detach=True
    )

    def stream_logs():
        with open(_log_path(job_id), "a") as f:
            for line in container.logs(stream=True, follow=True):
                try:
                    s = line.decode("utf-8", errors="ignore")
                except Exception:
                    s = str(line)
                f.write(s); f.flush()
    threading.Thread(target=stream_logs, daemon=True).start()

    try:
        requests.post(f"{SERVER}/api/agent/job-status",
                      json={"job_id": job_id, "status": "running",
                            "worker_id": WORKER_ID,
                            "container_id": container.id,
                            "container_name": job["container_name"]},
                      timeout=5)
    except Exception:
        pass

    exit_code = container.wait()["StatusCode"]
    status = "finished" if exit_code == 0 else "failed"
    try:
        requests.post(f"{SERVER}/api/agent/job-status",
                      json={"job_id": job_id, "status": status, "worker_id": WORKER_ID},
                      timeout=5)
    except Exception:
        pass

def main():
    while not stop_flag:
        try:
            r = requests.post(f"{SERVER}/api/agent/next-job",
                              json={"worker_id": WORKER_ID}, timeout=10)
            if r.status_code == 200:
                run_job(r.json())
            time.sleep(POLL_INTERVAL if r.status_code != 200 else 0)
        except Exception:
            time.sleep(min(POLL_INTERVAL * 2, 30))

if __name__ == "__main__":
    main()



# /etc/systemd/system/opeva-agent.service
# /etc/systemd/system/opeva-agent.service
[Unit]
Description=OPEVA Worker Agent
After=network-online.target docker.service
Wants=network-online.target

[Service]
User=root
Group=root
Environment=OPEVA_SERVER=http://MAIN-SERVER:8000
Environment=WORKER_ID=gpu-server-1
Environment=OPEVA_SHARED_DIR=/opt/opeva_shared_data
Environment=OPEVA_DOCKER_NETWORK=opeva_network
WorkingDirectory=/opt/opeva_agent
ExecStart=/usr/bin/python3 /opt/opeva_agent/agent.py
Restart=always
RestartSec=5
KillMode=process
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
