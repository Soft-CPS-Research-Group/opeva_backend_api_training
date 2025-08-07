# 🚀 OPEVA Backend API Training

This repository contains the backend API service for managing the execution of simulations and algorithms in the OPEVA infrastructure.

The service provides a REST API to:
- Launch simulation jobs dynamically inside Docker containers
- Track job status
- Stream and persist container logs
- Track progress and collect results
- Stop running jobs if needed
- Maintain persistent tracking of jobs even after restarts
- Manage and delete configs or datasets
- ✨ Stream training log files from simulation container (not just stdout)
- Retrieve data from MongoDB collections (Living Lab and iCharging Headquarters)
- Create datasets from MongoDB data with per-building and per-EV CSVs

The backend is fully integrated with:
- **OPEVA shared data storage** (`/opt/opeva_shared_data/`)
- **OPEVA Docker network** (`opeva_network`)
- **MLflow tracking server** (automatic metrics reporting)
- **Watchtower** for automatic CI/CD updates

---

## 📦 Project Structure

```
opeva_backend_api_training/
app/
├── api/                        # Routes (API layer)
│   ├── endpoints/
│   │   ├── jobs.py
│   │   ├── configs.py
│   │   ├── datasets.py
│   │   ├── mongo.py
│   │   └── health.py
│   └── router.py               # Main APIRouter() mounting subroutes
├── controllers/               # Controllers - HTTP-facing logic
│   ├── job_controller.py
│   ├── config_controller.py
│   ├── dataset_controller.py
│   └── mongo_controller.py
├── services/                  # Business logic
│   ├── job_service.py
│   ├── config_service.py
│   ├── dataset_service.py
│   └── mongo_service.py
├── models/                    # Pydantic models and domain entities
│   └── job.py
├── utils/                     # Low-level utilities
│   ├── docker_manager.py
│   ├── job_utils.py
│   ├── file_utils.py
│   └── mongo_utils.py
├── config.py
├── main.py                    # Just mounts router
├── Dockerfile                # Containerization
├── requirements.txt          # Dependencies
├── .github/workflows/        # GitHub Actions CI/CD
│   └── docker-publish.yml
├── README.md                 # This file
```

---

## 🧹 Infrastructure Integration

This service is part of the **OPEVA Infra Services** stack and communicates with:
- MLflow: `http://mlflow:5000`
- Simulation services: dynamically launched containers
- Shared storage: `/opt/opeva_shared_data/`
- MongoDB databases: `living_lab` and `i-charging_headquarters`

The service attaches to the external Docker network:
```
networks:
  opeva_network:
    external: true
```

It uses the shared `/opt/opeva_shared_data/` folder to store:

- All outputs (logs, results, progress, metadata) are stored under `/jobs/{job_id}/`, including:
  - `logs/{job_id}.log`
  - `results/result.json`
  - `progress/progress.json`
  - `job_info.json`

## Getting Started
### Requirements
- Docker
- Docker Compose
- Docker network: `opeva_network` (external, global)
- Shared data folder: `/opt/opeva_shared_data/`

**Build and run locally**
```bash
docker build -t opeva_backend_api_training .
docker run -p 8000:8000 --network opeva_network -v /opt/opeva_shared_data:/data opeva_backend_api_training
```

**Using Docker Compose**
```bash
cd /opt/opeva_infra_services/opeva_backend_api
docker-compose up -d
```
This will start:
- The backend API on port 8000
- Watchtower for automatic deployment updates

## API Overview

| Method | Endpoint                                | Description                                                                 |
|--------|-----------------------------------------|-----------------------------------------------------------------------------|
| GET    | /sites                                  | List available MongoDB databases (each representing a "site")              |
| GET    | /real-time-data/{site_name}             | Retrieve all collections and documents from the specified MongoDB site     |
| GET    | /real-time-data/{site_name}?minutes=X   | Retrieve only documents from the last X minutes across all collections     |
| POST   | /run-simulation                         | Launch a new simulation job                                                |
| GET    | /status/{job_id}                        | Check job status                                                           |
| GET    | /result/{job_id}                        | Get final results of job                                                   |
| GET    | /progress/{job_id}                      | Get progress updates                                                       |
| GET    | /logs/{job_id}                          | Stream container logs                                                      |
| GET    | /logs/file/{job_id}                     | Stream simulation log file (.log)                                          |
| POST   | /stop/{job_id}                          | Stop a running container/job                                               |
| GET    | /jobs                                   | List all tracked jobs                                                      |
| GET    | /job-info/{job_id}                      | Get metadata about a job                                                   |
| DELETE | /job/{job_id}                           | Delete job and its files                                                   |
| GET    | /health                                 | Health check of the API                                                    |
| POST   | /experiment-configs/create              | Create new config file                                                     |
| GET    | /experiment-configs                     | List all config files                                                      |
| GET    | /experiment-configs/{file}              | View a config file                                                         |
| DELETE | /experiment-configs/{file}              | Delete a config file                                                       |
| POST   | /dataset                                | Create a new dataset from a MongoDB site (buildings + EVs to CSVs)         |
| GET    | /datasets                               | List all available datasets                                                |
| DELETE | /dataset/{name}                         | Delete a dataset and its contents                                          |
| GET    | /dataset/dates-available/{site}         | Check available dates to generate a dataset                                |
| GET    | /hosts                                  | List all available hosts                                                   |
| POST   | /schema/create                          | Create a new site with its schema. Fails if the site already exists.     |
| PUT    | /schema/update/{site}                   | Update the schema for an existing site.                                  |
| GET    | /schema/{site}                          | Retrieve the schema for a specific site.                                 |
---

## CI/CD Pipeline

This repository uses GitHub Actions to build and publish Docker images to GitHub Container Registry:
```
.github/workflows/docker-publish.yml
```
On every push to `main`:
- Docker image is built and pushed to `ghcr.io/tiagofonseca/opeva_backend_api_training:latest`
- Watchtower running in the VM will automatically detect updates and redeploy the service

**Polling interval for Watchtower**: every 24 hours (`WATCHTOWER_POLL_INTERVAL=86400`)

## Persistent Job Tracking
Job container IDs are saved across reboots:
```
/opt/opeva_shared_data/job_track.json
```

## Logs and Results
Simulation outputs are persisted under `/opt/opeva_shared_data/jobs/{job_id}/`:
- Logs: `logs/{job_id}.log`
- Results: `results/result.json`
- Progress: `progress/progress.json`
- Metadata: `job_info.json`

---

## 😓 Best Practices & Gotchas

✅ Always point to `/data/` for datasets/configs inside containers  
✅ Use inline config when launching dynamically from UI  
✅ Always mount `/opt/opeva_shared_data` as `/data` in containers  

❌ Do **not** use relative paths like `./datasets/...` in configs  
❌ Do **not** rely on container stdout logs — use the generated `.log` files

---

## 📁 Output Paths (in /opt/opeva_shared_data)
```
jobs/{job_id}/
├── logs/
│   └── {job_id}.log
├── progress/
│   └── progress.json
├── results/
│   └── result.json
├── job_info.json
```

---

## 📰 Full API Usage Examples

### ✅ Launch a Simulation (existing config)
```bash
curl -X POST http://<IP>:8000/run-simulation \
  -H "Content-Type: application/json" \
  -d '{
    "config_path": "configs/my_config.yaml",
    "target_host": "local"
}'
```

### ✅ Launch a Simulation (inline config)
```bash
curl -X POST http://<IP>:8000/run-simulation \
  -H "Content-Type: application/json" \
  -d '{
    "config": { ... },
    "save_as": "generated_config.yaml",
    "target_host": "local"
}'
```

### 🔍 Check Job Status
```bash
curl http://<IP>:8000/status/{job_id}
```

### 📊 Get Simulation Results
```bash
curl http://<IP>:8000/result/{job_id}
```

### 📈 Get Training Progress
```bash
curl http://<IP>:8000/progress/{job_id}
```

### 📄 Stream Logs (stdout)
```bash
curl http://<IP>:8000/logs/{job_id}
```

### 🟞️ Stream Training Log File (.log)
```bash
curl http://<IP>:8000/logs/file/{job_id}
```

### ❌ Stop Running Job
```bash
curl -X POST http://<IP>:8000/stop/{job_id}
```

### 📃 List Jobs
```bash
curl http://<IP>:8000/jobs
```

### 📰 Job Metadata
```bash
curl http://<IP>:8000/job-info/{job_id}
```

### Available Hosts
```bash
curl http://<IP>:8000/hosts
```

### ❤️ Health Check
```bash
curl http://<IP>:8000/health
```

---

## 🔧 Configs & Datasets Management

### ✅ Create Config
```bash
curl -X POST http://<IP>:8000/config/create \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "custom.yaml",
    "config": { ...config... }
}'
```

### 📜 List Configs
```bash
curl http://<IP>:8000/configs
```

### 🔍 View Config File
```bash
curl http://<IP>:8000/config/custom.yaml
```

### ❌ Delete Config File
```bash
curl -X DELETE http://<IP>:8000/config/custom.yaml
```

### ✅ Create Dataset (from MongoDB site)
```bash
curl -X POST http://<IP>:8000/dataset \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dataset1",
    "site_id": "living_lab",
    "config": {
      "parameter1": "value1",
      "parameter2": 42
    },
    "from_ts": "2023-01-01 00:00:00",
    "until_ts": "2023-01-01 00:00:00"
}'
```

### 📃 List Datasets
```bash
curl http://<IP>:8000/datasets
```

### ❌ Delete Dataset
```bash
curl -X DELETE http://<IP>:8000/dataset/dataset1
```

### ❌ Delete Job (and its folder)
```bash
curl -X DELETE http://<IP>:8000/job/{job_id}
```

## 📰 MongoDB Endpoints Usage Examples

### ✅ List all available MongoDB sites (databases)
```bash
curl http://<IP>:8000/sites
```

### ✅ Retrieve all real-time data from a specific site (e.g., iCharging Headquarters)
```bash
curl http://<IP>:8000/real-time-data/i-charging_headquarters
```

### ✅ Retrieve only the last 60 minutes of data from a specific site
```bash
curl "http://<IP>:8000/real-time-data/living_lab?minutes=60"
```




# 🖥️ Setting Up a New Slave Host for OPEVA Backend

This guide walks you through two key stages:

🔧 Configure a new PC as a slave host (PC side + server side)

🚀 Run jobs on the slave host (from the OPEVA server)

## 1️⃣ CONFIGURE A NEW PC TO BE A SLAVE HOST

🔒 On the Slave PC (your personal computer)

✅ Requirements:

Docker installed and working (use docker info to confirm)

VPN to the ISEP network working (your PC must get a 10.8.0.X address)

SSH access configured to the server (e.g., alias softcps works)

📁 Folder Setup

Create the local shared data folder:

    sudo mkdir -p /opt/opeva_shared_data
    sudo chown $USER:$USER /opt/opeva_shared_data

🔁 (Recommended) Mount NFS Shared Folder from Server

Install NFS client:

    sudo apt update && sudo apt install nfs-common

Mount manually:

    sudo mount -t nfs softcps:/opt/opeva_shared_data /opt/opeva_shared_data

🔐 Note: May require VPN and NFS ports open on the server:

2049 (nfs), 111 (rpcbind), 20048 (mountd), 4045 (lockd), 32765-32768 (statd)

To mount at boot or when VPN is up, you can edit /etc/fstab or use autofs.

🔌 Create a reverse SSH tunnel script

Create the file ~/bin/start_opeva_tunnel.sh with:

    #!/bin/bash

    REMOTE_HOST=softcps
    REMOTE_PORT=23750
    LOCAL_SOCK=/var/run/docker.sock

    echo "[tunnel] Opening Docker tunnel to $REMOTE_HOST:$REMOTE_PORT..."
    ssh -N -R $REMOTE_PORT:$LOCAL_SOCK $REMOTE_HOST

Make it executable:

    chmod +x ~/bin/start_opeva_tunnel.sh

Run it manually after VPN is up:

    ~/bin/start_opeva_tunnel.sh

🔄 (Optional) Create Docker network (match server)

docker network create opeva_network || true

🖥️ On the Server (softcps VM)

🗂️ Edit app/config.py

Add your PC entry to the AVAILABLE_HOSTS list:

    AVAILABLE_HOSTS: list = [
        {"name": "local", "host": "local"},
        {"name": "tiago_pc", "host": "tcp://127.0.0.1:23750"}  # reverse tunnel
    ]

✅ Confirm it's reachable

From your PC, start the tunnel:

    ~/bin/start_opeva_tunnel.sh

Then on the server:

      docker -H tcp://127.0.0.1:23750 info

You should see the Docker info of your PC.

## 2️⃣ RUN A JOB ON THE SLAVE HOST

✅ Launch a simulation using your PC

From the server:

    curl -X POST http://localhost:8000/run-simulation \
      -H "Content-Type: application/json" \
      -d '{
        "config_path": "configs/my_config.yaml",
        "target_host": "tiago_pc"
    }'

🧪 Monitor the job

    curl http://localhost:8000/status/{job_id}          # check status
    curl http://localhost:8000/logs/{job_id}            # live logs
    curl http://localhost:8000/progress/{job_id}        # progress file
    curl http://localhost:8000/result/{job_id}          # final results

