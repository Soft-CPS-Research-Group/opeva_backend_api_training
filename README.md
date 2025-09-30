# OPEVA Backend API

Backend service that orchestrates MARL simulations and energy flexibility scheduling jobs across a fleet of hosts. The API coordinates Docker workloads, persists artefacts on a shared NFS volume, and exposes datasets and MongoDB utilities for the OPEVA infrastructure.

---

## Table of Contents
1. [Concept Overview](#concept-overview)
2. [Architecture](#architecture)
3. [Shared Storage Layout](#shared-storage-layout)
4. [Server Setup (Main Node)](#server-setup-main-node)
5. [Worker Setup (Dynamic VPN Nodes)](#worker-setup-dynamic-vpn-nodes)
6. [Running the API](#running-the-api)
7. [Job Lifecycle](#job-lifecycle)
8. [Launching Jobs](#launching-jobs)
9. [Monitoring & Managing Jobs](#monitoring--managing-jobs)
10. [Experiment Config Management](#experiment-config-management)
11. [Agent API Reference](#agent-api-reference)
12. [Dataset Management](#dataset-management)
13. [MongoDB Utilities](#mongodb-utilities)
14. [Configuration Reference](#configuration-reference)
15. [Directory Map](#directory-map)
16. [Testing](#testing)
17. [Troubleshooting](#troubleshooting)

---

## Concept Overview
- **Main server** exposes a FastAPI service. It validates job requests, prepares metadata, stores configurations, and records status transitions.
- **Worker agents** connect over VPN, mount a shared NFS directory, poll for work, and run Docker containers locally.
- **Shared storage** (`/opt/opeva_shared_data`) is the single source of truth for configs, queued jobs, logs, progress, and results.
- **Datasets and Mongo endpoints** allow exporting time-series data from site databases for downstream processing.

---

## Architecture
```
+-------------------+           +-------------------+
|    CLIENT / UI    |  REST     |   MAIN SERVER     |
|  (curl, UI, etc.) | <-------> |  FastAPI + Docker |
+-------------------+           +-------------------+
                                         |
                                         | read/write
                                         v
                              /opt/opeva_shared_data (NFS)
                                         ^
                                         |
                               +-------------------+
                               | WORKER AGENT (n) |
                               | Docker + NFS     |
                               +-------------------+
```
- Jobs submitted to `/run-simulation` are written to a global queue; each payload may specify a preferred host or allow any agent to execute it.
- Worker agents poll `/api/agent/next-job`, execute Docker containers with the shared volume mounted as `/data`, stream logs to the shared directory, and post status/heartbeat updates.
- The API serves artefacts directly from shared storage, so both local and remote runs follow the same layout and queue state.

## Domain Model
- **Job** — Core unit of work representing a simulation. Metadata lives in memory, `job_track.json`, and the per-job folder (`status.json`, `job_info.json`, logs, progress, results) while status transitions follow the `JobStatus` state machine. Jobs are always queued with an optional host preference; agents decide when to pick them up and may impose their own concurrency policies.
- **Worker Agent** — Remote process that polls the global queue, decides locally when to start work, runs Docker workloads with the shared volume mounted at `/data`, streams logs, reports status transitions (`running`, `finished`, `failed`), and periodically sends heartbeats so the API can track availability.
- See `docs/worker_agent.md` for the detailed contract expected from worker implementations.
- **Config** — Experiment definitions stored under `configs/`. Jobs may reference an existing file or an inline payload that is persisted before launch, ensuring reproducibility.
- **Dataset** — Aggregated CSV outputs stored beneath `datasets/` alongside a `schema.json`. Creation pulls from Mongo collections, list/delete/download endpoints expose the artefacts.
- **Mongo Schema** — Canonical `schema` document per site database that describes available installations. CRUD endpoints allow seeding and updates before dataset generation.
- **Shared Storage** — The mounted NFS root (`/opt/opeva_shared_data` by default) that contains configs, datasets, job artefacts, queues, and the job registry, acting as the coordination layer between API node and worker agents.

---

## Shared Storage Layout
Default root: `/opt/opeva_shared_data` (configurable through `settings.VM_SHARED_DATA`).

```
/opt/opeva_shared_data
├── configs/            # YAML experiment configs
├── datasets/           # Generated CSV datasets + schema.json
├── jobs/
│   └── <job_id>/
│       ├── job_info.json
│       ├── status.json
│       ├── logs/<job_id>.log
│       ├── progress/progress.json
│       └── results/result.json
├── queue/              # One JSON payload per queued job (global queue)
└── job_track.json      # Registry of known jobs (persisted cache)
```

Every component (server and workers) must mount this path in the same location.

---

## Server Setup (Main Node)
### Prerequisites
- Ubuntu/Debian host with Docker Engine.
- Python 3.10+
- Root or sudo access.

### 1. Install NFS server
```bash
sudo apt update
sudo apt install nfs-kernel-server
```

### 2. Create the shared directory
```bash
sudo mkdir -p /opt/opeva_shared_data
sudo chown $USER:$USER /opt/opeva_shared_data
```

### 3. Export the directory (adjust VPN subnet)
Edit `/etc/exports`:
```
/opt/opeva_shared_data 10.8.0.0/24(rw,sync,no_subtree_check,fsid=0)
```
Reload exports:
```bash
sudo exportfs -ra
```
Open required ports in your firewall: `2049`, `111`, `20048`, `4045`, `32765-32768`.

### 4. Clone and install Python dependencies
```bash
git clone https://github.com/<org>/opeva_backend_api_training.git
cd opeva_backend_api_training
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 5. Configure environment (optional)
The application reads configuration from environment variables via `pydantic-settings`. Common overrides:

| Variable | Description |
|----------|-------------|
| `VM_SHARED_DATA` | Root path of the shared storage (default `/opt/opeva_shared_data`). |
| `AVAILABLE_HOSTS` | Comma-separated list of host names (`local` must be included). |
| `MONGO_*` vars     | Credentials/host/port for site databases. |

Set them in your shell or a systemd unit before starting the API.

---

## Worker Setup (Dynamic VPN Nodes)
Workers can join the VPN with changing IPs. We identify each worker by a logical `WORKER_ID` that appears in `settings.AVAILABLE_HOSTS`.

### Checklist for every worker
1. **Register a name**: edit `app/config.py` or set `AVAILABLE_HOSTS` so it includes the worker identifier (e.g. `"tiago-gpu"`).
2. **Install dependencies**:
   ```bash
   sudo apt update
   sudo apt install docker.io nfs-common python3 python3-pip
   # Optional for GPU nodes: NVIDIA drivers + nvidia-docker2
   ```
3. **Mount shared storage** (after VPN is connected):
   ```bash
   sudo mkdir -p /opt/opeva_shared_data
   sudo mount -t nfs <SERVER_VPN_IP>:/opt/opeva_shared_data /opt/opeva_shared_data
   ```
   To persist across reboots, add to `/etc/fstab`:
   ```
   <SERVER_VPN_IP>:/opt/opeva_shared_data  /opt/opeva_shared_data  nfs  defaults,_netdev  0  0
   ```
4. **Deploy the worker agent**:
   - Copy `worker_agent.py` to `/opt/opeva_worker/agent.py`.
   - Optionally create a Python environment and install `docker` and `requests`.
5. **Run the agent (manual test)**:
   ```bash
   export OPEVA_SERVER="http://<SERVER_VPN_IP>:8000"
   export WORKER_ID="tiago-gpu"
   export OPEVA_SHARED_DIR="/opt/opeva_shared_data"
   export OPEVA_DOCKER_NETWORK="opeva_network"  # optional
   python3 /opt/opeva_worker/agent.py
   ```
   You should see periodic polling with HTTP 204 responses when no jobs are queued.
6. **Install as a service (recommended)**: create `/etc/systemd/system/opeva-worker.service`:
   ```ini
   [Unit]
   Description=OPEVA Worker Agent
   After=network-online.target docker.service
   Wants=network-online.target

   [Service]
   User=root
   Group=root
   Environment=OPEVA_SERVER=http://<SERVER_VPN_IP>:8000
   Environment=WORKER_ID=tiago-gpu
   Environment=OPEVA_SHARED_DIR=/opt/opeva_shared_data
   Environment=OPEVA_DOCKER_NETWORK=opeva_network
   WorkingDirectory=/opt/opeva_worker
   ExecStart=/usr/bin/python3 /opt/opeva_worker/agent.py
   Restart=always
   RestartSec=5
   KillMode=process
   TimeoutStopSec=30

   [Install]
   WantedBy=multi-user.target
   ```
   Enable & start:
   ```bash
   sudo systemctl enable opeva-worker
   sudo systemctl start opeva-worker
   ```

Queued jobs remain on disk under `/opt/opeva_shared_data/queue/<WORKER_ID>/` and will be processed automatically when the agent reconnects.

---

## Running the API
### Local development
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker container
A simple Dockerfile is provided. Build and run:
```bash
docker build -t opeva-backend .
docker run --rm -p 8000:8000 \
  -v /opt/opeva_shared_data:/opt/opeva_shared_data \
  -e VM_SHARED_DATA=/opt/opeva_shared_data \
  opeva-backend
```

The API root is `http://<SERVER_IP>:8000`. Combine with a reverse proxy or authentication layer as needed.

---

## Job Lifecycle
Jobs transition through the following states (defined in `app/status.py`):

| State | Meaning |
|-------|---------|
| `launching` | Metadata prepared; directories seeded. |
| `queued` | Enqueued for a remote worker (job file created). |
| `dispatched` | Worker agent popped the job but has not started the container. |
| `running` | Docker container is executing. |
| `finished` | Exit code 0. |
| `failed` | Non-zero exit code. |
| `stopped` | Local job stopped via API. |
| `canceled` | Job removed before execution. |
| `not_found` | Container/job artefacts missing. |
| `unknown` | Fallback when status cannot be determined. |

Allowed transitions:
- Local jobs: `launching → running → finished|failed|stopped`
- Remote jobs: `launching → queued → dispatched → running → finished|failed`
- Manual stop on queued job: `queued → canceled`

### Metadata JSONs
`job_info.json` (written by the server, updated by the agent):
```json
{
  "job_id": "1234-5678",
  "job_name": "Demo-Run1",
  "config_path": "configs/demo.yaml",
  "target_host": "tiago-gpu",
  "container_id": "<docker-id>",
  "container_name": "opeva_sim_1234_Demo-Run1",
  "experiment_name": "Demo",
  "run_name": "Run1"
}
```

`status.json` (updated on every transition):
```json
{
  "job_id": "1234-5678",
  "status": "running",
  "worker_id": "tiago-gpu",
  "exit_code": null
}
```

`progress/progress.json` and `results/result.json` are produced by the simulation container itself. Logs are appended to `logs/<job_id>.log` by the worker agent.

---

## Launching Jobs
### 1. Prepare a configuration
Upload or create a YAML file under `configs/` or provide inline config in the request body. Example snippet:
```yaml
experiment:
  name: Demo
  run_name: Run1
simulation:
  episodes: 10
```

### 2. Launch against the server itself
```bash
curl -X POST http://SERVER:8000/run-simulation \
  -H "Content-Type: application/json" \
  -d '{
        "config_path": "demo.yaml",
        "target_host": "local"
      }'
```

### 3. Launch on a remote worker
```bash
curl -X POST http://SERVER:8000/run-simulation \
  -H "Content-Type: application/json" \
  -d '{
        "config_path": "demo.yaml",
        "target_host": "tiago-gpu"
      }'
```

Responses include `job_id`, container info (for local runs), and the host assigned. Remote jobs move to `queued` until a worker polls them.

---

## Monitoring & Managing Jobs
```bash
# Status / metadata
curl http://SERVER:8000/status/<job_id>
curl http://SERVER:8000/job-info/<job_id>

# Artefacts
curl http://SERVER:8000/progress/<job_id>
curl http://SERVER:8000/result/<job_id>
curl http://SERVER:8000/logs/<job_id>
curl http://SERVER:8000/file-logs/<job_id>

# Stop or delete
curl -X POST http://SERVER:8000/stop/<job_id>
curl -X DELETE http://SERVER:8000/job/<job_id>

# List all tracked jobs
curl http://SERVER:8000/jobs

# Discover target hosts accepted by the API
curl http://SERVER:8000/hosts
```

Stopping remote jobs currently cancels queued entries; stopping running jobs requires agent-side support.

---

## Experiment Config Management
Use these endpoints to manage YAML configuration files stored under `configs/`.

```bash
# Create or overwrite a config file
curl -X POST http://SERVER:8000/experiment-config/create \
  -H "Content-Type: application/json" \
  -d '{"config": {...}, "file_name": "demo.yaml"}'

# List available config files
curl http://SERVER:8000/experiment-configs

# Retrieve a config
curl http://SERVER:8000/experiment-config/demo.yaml

# Delete a config
curl -X DELETE http://SERVER:8000/experiment-config/demo.yaml
```

Launching jobs can reference any file stored in `configs/` via `config_path`. Inline configs are also supported by providing a `config` object in the job submission payload; the API will persist it using `save_as` or a generated name.

---

## Agent API Reference
These endpoints power `worker_agent.py` and are available for custom agent implementations:

| Method | Endpoint | Body | Description |
|--------|----------|------|-------------|
| POST | `/api/agent/next-job` | `{ "worker_id": "tiago-gpu" }` | Returns 200 with a job payload or 204 when no jobs are queued. |
| POST | `/api/agent/job-status` | `{ "job_id": "...", "status": "running", "worker_id": "tiago-gpu", "container_id": "..." }` | Record status changes and optional container metadata. |

### Job payload returned to agents
```json
{
  "job_id": "1234-5678",
  "image": "calof/opeva_simulator:latest",
  "container_name": "opeva_sim_1234_Demo-Run1",
  "command": "--config /data/configs/demo.yaml --job_id 1234-5678",
  "volumes": [{
    "host": "/opt/opeva_shared_data",
    "container": "/data",
    "mode": "rw"
  }],
  "env": {
    "MLFLOW_TRACKING_URI": "http://MAIN-SERVER:5000"
  }
}
```

Agents must mount `volumes[0].host` into the container at `volumes[0].container`.

---

## Dataset Management
Use the dataset endpoints to export MongoDB site data into CSV bundles:

```bash
# Create dataset
curl -X POST http://SERVER:8000/dataset \
  -H "Content-Type: application/json" \
  -d '{
        "name": "living_lab_2025",
        "site_id": "living_lab",
        "citylearn_configs": {...},
        "description": "Latest export",
        "period": 60,
        "from_ts": "2025-01-01T00:00:00Z",
        "until_ts": "2025-01-07T00:00:00Z"
      }'

# Discover available datasets and metadata
curl http://SERVER:8000/datasets

# Download
curl -L http://SERVER:8000/dataset/download/living_lab_2025 -o living_lab_2025.zip

# Delete
curl -X DELETE http://SERVER:8000/dataset/living_lab_2025

# Inspect available data windows for a site
curl http://SERVER:8000/dataset/dates-available/living_lab
```

Datasets are stored under `/opt/opeva_shared_data/datasets/<name>/` with accompanying `schema.json` describing structure and description.

---

## MongoDB Utilities
- `GET /sites` – list MongoDB databases (sites) the backend can access.
- `GET /real-time-data/{site_name}` – dump all documents for a site.
- `GET /real-time-data/{site_name}?minutes=60` – restrict results to the last X minutes.

Credentials and host details come from `settings.MONGO_*`. Ensure workers and the server can reach the database host.

---

## Configuration Reference
All runtime settings live in `app/config.py` (`Settings` class). Key attributes:

| Attribute | Default | Purpose |
|-----------|---------|---------|
| `VM_SHARED_DATA` | `/opt/opeva_shared_data` | Root shared directory. |
| `CONFIGS_DIR` | `${VM_SHARED_DATA}/configs` | Location of YAML configs. |
| `JOBS_DIR` | `${VM_SHARED_DATA}/jobs` | Job artefacts. |
| `DATASETS_DIR` | `${VM_SHARED_DATA}/datasets` | Exported datasets. |
| `QUEUE_DIR` | `${VM_SHARED_DATA}/queue` | Agent job queue. |
| `JOB_TRACK_FILE` | `${VM_SHARED_DATA}/job_track.json` | Persistent job registry. |
| `AVAILABLE_HOSTS` | `["local", "gpu-server-1", "gpu-server-2", "tiago-laptop"]` | Valid `target_host` values. |
| `HOST_HEARTBEAT_TTL` | `60` | Seconds before a host is considered offline if no heartbeat is received. |
| `MONGO_*` | `runtimeUI` / `runtimeUIDB` / host `193.136.62.78` | Mongo connection details. |
| `ACCEPTABLE_GAP_IN_MINUTES` | `60` | Controls interpolation in dataset exports. |

Override via environment variables (uppercase) or `.env` file compatible with `pydantic-settings`.

---

## Directory Map
Repository highlights:
- `app/main.py` – FastAPI application bootstrap.
- `app/api/` – REST endpoints grouped by domain (jobs, datasets, mongo, schema, agent).
- `app/controllers/` – Thin layer around services, handles HTTP errors.
- `app/services/` – Business logic (job orchestration, dataset export, Mongo operations). The job service manages metadata, Docker calls, and worker interactions.
- `app/utils/` – Helpers for Docker, filesystem manipulation, Mongo connections.
- `worker_agent.py` – Reference worker implementation using Docker SDK.
- `examples/` – Sample job artefacts for testing endpoints without running real simulations.
- `tests/` – Pytest suite covering job state transitions, artefact loading, and dataset helpers.

---

## Testing
Install pytest in your environment and run:
```bash
pip install pytest
pytest
# or specific modules
pytest tests/test_job_states.py
```

Example tests use the sample artefacts under `examples/` and temporary directories to emulate job queues.

---

## Troubleshooting
| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Job stuck in `queued` | Worker agent offline or `WORKER_ID` mismatch. | Ensure worker is running, `WORKER_ID` appears in `AVAILABLE_HOSTS`, and shared storage is mounted. |
| `not_found` status after run | Container removed or `status.json` missing. | Check Docker container on the host and verify shared storage access rights. |
| Worker cannot mount NFS | VPN not connected or firewall blocked. | Verify VPN tunnel, server export, and firewall rules. |
| API returns 500 on `/dataset` | Mongo credentials or schema missing. | Confirm MongoDB connectivity and presence of `schema` collection. |
| Logs endpoint empty | Job still running or log file not created yet. | Wait for the worker to stream logs or inspect container stdout directly. |

For additional debugging, enable FastAPI logging (`uvicorn --log-level debug`) and inspect `/opt/opeva_shared_data/job_track.json`.

---

Happy experimenting! Contributions and improvements to the workflow are welcome.
