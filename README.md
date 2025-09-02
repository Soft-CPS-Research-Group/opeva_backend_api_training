# 🚀 OPEVA Backend API Training (Multi-Host with NFS & Worker Agents)

This repository contains the backend API service for managing the execution of **MARL simulations** and **energy flexibility scheduling** jobs in the **OPEVA infrastructure**.

The system supports **multi-host execution** using **Worker Agents** connected to the main server via **NFS shared storage** and optional **reverse SSH tunnels** for Docker remote control.

---

## 📜 Table of Contents

- Architecture Overview
- Job Lifecycle & States
- API Overview
- 1️⃣ Server Setup (Main Server)
- 2️⃣ Worker Setup (Slave Host)
- 3️⃣ Launching a Job
- 4️⃣ Monitoring & Stopping Jobs
- 5️⃣ Best Practices

---

## Architecture Overview

```
+-------------------+
|   MAIN SERVER     |
| (Backend API)     |
+-------------------+
        |
        | REST API
        v
+-------------------+
|   JOB QUEUE       |
+-------------------+
        |
        | Assign jobs to available worker
        v
+-------------------+       +-------------------+
| WORKER AGENT 1    |  ...  | WORKER AGENT N    |
| (Docker + NFS)    |       | (Docker + NFS)    |
+-------------------+       +-------------------+
        |
        | Runs container with simulator + algorithm
        v
+-------------------+
|  Shared Storage   |
| (/opt/opeva_shared_data) |
+-------------------+
        |
        | Results, logs, progress back to server
        v
+-------------------+
|  Backend stores   |
|  and serves data  |
+-------------------+
```

---

## Job Lifecycle & States

Jobs move through the following states:

| State        | Meaning |
|--------------|---------|
| `launching`  | Job metadata being prepared on the server |
| `queued`     | Waiting to be assigned to a worker |
| `dispatched` | Worker agent fetched the job but hasn't started it |
| `running`    | Worker has started the job |
| `finished`   | Job completed successfully |
| `failed`     | Job ended with an error |
| `stopped`    | Job was manually stopped |
| `canceled`   | Job was canceled before starting |
| `not_found`  | Job or container information no longer available |
| `unknown`    | State could not be determined |

### State transitions

- Local jobs: `launching` → `running` → `finished`/`failed`/`stopped`
- Remote jobs: `launching` → `queued` → `dispatched` → `running` → `finished`/`failed`/`stopped`
- A `queued` or `running` job may transition to `canceled` if stopped before completion

Retrieve the current state with `GET /status/{job_id}`. Progress can be polled via `GET /progress/{job_id}`.

**Stop Eligibility:** Only `queued` or `running` jobs can be stopped.

### Sample job artifacts for testing

The `examples/` directory contains ready‑made job folders representing common
states (finished, running, failed, queued). Copy one of these folders into your
server's jobs directory to exercise the status, result, progress, and log
endpoints without launching real jobs.

---

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
| GET    | /file-logs/{job_id}                     | Stream simulation log file (.log)                                          |
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
| GET    | /datasets                               | List all available datasets with metadata      |
| GET    | /dataset/download/{name}                | Download a dataset (zips directories)          |
| DELETE | /dataset/{name}                         | Delete a dataset and its contents                                          |
| GET    | /dataset/dates-available/{site}         | Check available dates to generate a dataset                                |
| GET    | /hosts                                  | List all available hosts                                                   |
| POST   | /schema/create                          | Create a new site with its schema. Fails if the site already exists.     |
| PUT    | /schema/update/{site}                   | Update the schema for an existing site.                                  |
| GET    | /schema/{site}                          | Retrieve the schema for a specific site.                                 |
---

## 1️⃣ Server Setup (Main Server)

### Install NFS Server
```
sudo apt update
sudo apt install nfs-kernel-server
```

### Create Shared Folder
```
sudo mkdir -p /opt/opeva_shared_data
sudo chown $USER:$USER /opt/opeva_shared_data
```


## Logs and Results
Simulation outputs are persisted under `/opt/opeva_shared_data/jobs/{job_id}/`:
- Logs: `logs/{job_id}.log`
- Results: `results/result.json`
- Progress: `progress/progress.json`
- Metadata: `job_info.json`

## 😓 Best Practices & Gotchas

✅ Always point to `/data/` for datasets/configs inside containers  
✅ Use inline config when launching dynamically from UI  
✅ Always mount `/opt/opeva_shared_data` as `/data` in containers  

❌ Do **not** use relative paths like `./datasets/...` in configs  
❌ Do **not** rely on container stdout logs — use the generated `.log` files

---
### Export via NFS
Edit `/etc/exports`:
```
/opt/opeva_shared_data *(rw,sync,no_subtree_check)
```


Apply changes:
```
sudo exportfs -ra
```

**Open NFS ports** in VPN/internal network:
2049 (nfs), 111 (rpcbind), 20048 (mountd), 4045 (lockd), 32765-32768 (statd)

---

## 2️⃣ Worker Setup (Slave Host)

### Install Requirements
```
sudo apt update
sudo apt install docker.io nfs-common
```

### Mount Shared Folder
```
sudo mkdir -p /opt/opeva_shared_data
sudo mount -t nfs SERVER_IP:/opt/opeva_shared_data /opt/opeva_shared_data
```
> To auto-mount, add to `/etc/fstab`.

### Install Worker Agent
Save `agent.py` to `/opt/opeva_worker/agent.py`, make executable:
```
chmod +x /opt/opeva_worker/agent.py
```

### Create systemd Service
File: `/etc/systemd/system/opeva-worker.service`
```
[Unit]
Description=OPEVA Worker Agent
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/opeva_worker/agent.py
Restart=always
Environment=OPEVA_SERVER=http://MAIN-SERVER:8000
Environment=WORKER_ID=%H
Environment=POLL_INTERVAL=5
WorkingDirectory=/opt/opeva_worker

[Install]
WantedBy=multi-user.target
```
Enable & start:
```
sudo systemctl enable opeva-worker
sudo systemctl start opeva-worker
```

---

### Optional: Reverse SSH Tunnel for Remote Docker
On **worker**:
```
ssh -N -R 23750:/var/run/docker.sock softcps
```
On **server**:
```
docker -H tcp://127.0.0.1:23750 info
```

---

## 3️⃣ Launching a Job

**Local (Server)**
```
curl -X POST http://SERVER:8000/run-simulation   -H "Content-Type: application/json"   -d '{"config_path":"configs/my_config.yaml","target_host":"local"}'
```

**Remote (Worker)**
```
curl -X POST http://SERVER:8000/run-simulation   -H "Content-Type: application/json"   -d '{"config_path":"configs/my_config.yaml","target_host":"worker_name"}'
```

---


### 📃 List Datasets
List dataset names along with size and creation time.
```bash
curl http://<IP>:8000/datasets
```

### 💾 Download Dataset
```bash
curl -L http://<IP>:8000/dataset/download/dataset1 -o dataset1.zip
```

### ❌ Delete Dataset
```bash
curl -X DELETE http://<IP>:8000/dataset/dataset1

## 4️⃣ Monitoring & Stopping Jobs


```
curl http://SERVER:8000/status/{job_id}
curl http://SERVER:8000/progress/{job_id}
curl http://SERVER:8000/result/{job_id}
curl http://SERVER:8000/logs/{job_id}
curl http://SERVER:8000/file-logs/{job_id}
curl -X POST http://SERVER:8000/stop/{job_id}
```

---

## 5️⃣ Best Practices

- ✅ Always mount `/opt/opeva_shared_data` on **both server and workers**
- ✅ Always name hosts in `config.py` so API can target them
- ✅ Worker should **pull images locally** before running jobs
- ✅ GPU support: install NVIDIA drivers + `nvidia-docker2` on workers
- ❌ Do not store configs with relative paths

---

© 2025 OPEVA Infrastructure – Multi-Host Training Ready
