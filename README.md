# 🚀 OPEVA Backend API Training

This repository contains the backend API service for managing the execution of simulations and algorithms in the OPEVA infrastructure.

The service provides a REST API to:
- Launch simulation jobs dynamically inside Docker containers
- Track job status
- Stream and persist container logs
- Track progress and collect results
- Stop running jobs if needed
- Maintain persistent tracking of jobs even after restarts

The backend is fully integrated with:
- **OPEVA shared data storage** (`/opt/opeva_shared_data/`)
- **OPEVA Docker network** (`opeva_network`)
- **MLflow tracking server** (automatic metrics reporting)
- **Watchtower** for automatic CI/CD updates

---

## 📦 Project Structure

```
opeva_backend_api_training/    ├── app/ 
│   ├── config.py # Central config (paths, shared data) 
│   ├── docker_manager.py # Docker operations (run, stop, status, logs) 
│   ├── main.py # FastAPI application 
│   ├── models.py # Request models 
│   └── utils.py # Utilities: job persistence, results, progress 
├── Dockerfile # Containerization 
├── requirements.txt # Dependencies 
├── .github/workflows/ # GitHub Actions CI/CD 
│   └── docker-publish.yml 
├── README.md # This file
```


---

## 🧩 Infrastructure Integration

This service is part of the **OPEVA Infra Services** stack and communicates with:
- MLflow: `http://mlflow:5000`
- Simulation services: dynamically launched containers
- Shared storage: `/opt/opeva_shared_data/`

The service attaches to the external Docker network:
```
networks:
  opeva_network:
    external: true
```

It uses the shared /opt/opeva_shared_data/ folder to store:

- All outputs (logs, results, progress, metadata) are stored under `/jobs/{job_id}/`, including:
  - `logs/{job_id}.log`
  - `results/result.json`
  - `progress/progress.json`
  - `job_info.json`

## Getting Started
#### Requirements
- Docker
- Docker Compose
- Docker network: opeva_network (external, global)
- Shared data folder: /opt/opeva_shared_data/

**Build and run locally**
If you want to build and run manually:

```
docker build -t opeva_backend_api_training .
docker run -p 8000:8000 --network opeva_network -v /opt/opeva_shared_data:/data opeva_backend_api_training
```

**Using Docker Compose** 

```
cd /opt/opeva_infra_services/opeva_backend_api
docker-compose up -d
```

This will start:

- The backend API on port 8000
- Watchtower for automatic deployment updates

## API Overview
The backend provides the following endpoints:

| Method | Endpoint |	Description |
|----------|----------|----------|
| POST	| /run-simulation| 	Launch a new simulation job
| GET	| /status/{job_id}	| Check job status
| GET	| /result/{job_id}	| Get final results of job
| GET	| /progress/{job_id}| 	Get progress updates
| GET	| /logs/{job_id}	| Stream container logs (static file read)
| POST	| /stop/{job_id}| 	Stop a running container/job
| GET	| /jobs	List all|  tracked jobs
| GET	| /job-info/{job_id} | Get metadata about a job
| GET	| /health | 	Health check of the API


**Example Request: Run Simulation**

```
curl -X POST "http://<VM-IP>:8000/run-simulation" -H "Content-Type: application/json" -d '{"param1": "value1", "param2": "value2", "config_file": "config.yml"}'
```

## CI/CD Pipeline
This repository uses GitHub Actions to build and publish Docker images to GitHub Container Registry.

Pipeline location:

```
.github/workflows/docker-publish.yml
```

On every push to main:

- Docker image is built and pushed to ghcr.io/tiagofonseca/opeva_backend_api_training:latest

- Watchtower running in the VM will automatically detect updates and redeploy the service.

**Polling interval for Watchtower**: every 24 hours (WATCHTOWER_POLL_INTERVAL=86400)

## Persistent Job Tracking
The backend keeps track of all job container IDs persistently across restarts in:

```
/opt/opeva_shared_data/job_track.json
```

If the API container restarts, it reloads the active jobs from this file.

## Logs and Results
All logs and results are stored in the shared data volume:

- Logs: /opt/opeva_shared_data/logs/{job_id}.log

- Results: /opt/opeva_shared_data/results/{job_id}/result.json

- Progress: /opt/opeva_shared_data/progress/{job_id}/progress.json


## Contributing
Fork this repo, clone it, and work on a branch.

- Keep Docker images lean.

- Document your changes clearly.

- Avoid committing secrets or large binaries.

## Support
If you need help deploying or integrating with this API, reach out to Tiago Fonseca.

---

## 🧠 Simulation Logging (Updated)

Simulation containers now handle their own logging internally and save logs directly to:
```
/opt/opeva_shared_data/jobs/{job_id}/logs/{job_id}.log
```
The backend no longer captures stdout/stderr logs directly. It simply streams from the file if needed.

