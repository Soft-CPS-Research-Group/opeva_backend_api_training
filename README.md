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

The backend is fully integrated with:
- **OPEVA shared data storage** (`/opt/opeva_shared_data/`)
- **OPEVA Docker network** (`opeva_network`)
- **MLflow tracking server** (automatic metrics reporting)
- **Watchtower** for automatic CI/CD updates

---

## 📦 Project Structure

```
opeva_backend_api_training/
├── app/
│   ├── config.py             # Central config (paths, shared data, MongoDB)
│   ├── docker_manager.py     # Docker operations (run, stop, status, logs)
│   ├── main.py               # FastAPI application (includes MongoDB endpoints)
│   ├── models.py             # Request models
│   └── utils.py              # Utilities: job persistence, results, progress, MongoDB helpers
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

| Method | Endpoint                  | Description                       |
|--------|---------------------------|-----------------------------------|
| GET    | /api/icharging-headquarters | Retrieve iCharging headquarters data from MongoDB |
| GET    | /api/living-lab           | Retrieve all Living Lab data from MongoDB |
| POST   | /run-simulation           | Launch a new simulation job       |
| GET    | /status/{job_id}          | Check job status                  |
| GET    | /result/{job_id}          | Get final results of job          |
| GET    | /progress/{job_id}        | Get progress updates              |
| GET    | /logs/{job_id}            | Stream container logs             |
| GET    | /logs/file/{job_id}       | Stream simulation log file (.log) |
| POST   | /stop/{job_id}            | Stop a running container/job      |
| GET    | /jobs                     | List all tracked jobs             |
| GET    | /job-info/{job_id}        | Get metadata about a job          |
| DELETE | /job/{job_id}             | Delete job and its files          |
| GET    | /health                   | Health check of the API           |
| POST   | /config/create            | Create new config file            |
| GET    | /configs                  | List all config files             |
| GET    | /config/{file}            | View a config file                |
| DELETE | /config/{file}            | Delete a config file              |
| POST   | /dataset                  | Create a new dataset structure    |
| GET    | /datasets                 | List all available datasets       |
| GET    | /hosts                    | List all available hosts          |

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

### 🗞️ Stream Training Log File (.log)
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

### 📠 Job Metadata
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

### ✅ Create Dataset
```bash
curl -X POST http://<IP>:8000/dataset \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dataset1",
    "schema": { ...schema contents... }
}'
```

### 📃 List Datasets
```bash
curl http://<IP>:8000/datasets
```

### ❌ Delete Job (and its folder)
```bash
curl -X DELETE http://<IP>:8000/job/{job_id}
```

## 📰 MongoDB Endpoints Usage Examples

### ✅ Retrieve iCharging Headquarters data
```bash
curl http://<IP>:8000/api/icharging-headquarters
```

### ✅ Retrieve all Living Lab data
```bash
curl http://<IP>:8000/api/living-lab
```

---

