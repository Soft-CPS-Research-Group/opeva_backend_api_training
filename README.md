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
| `queued`     | Waiting to be assigned to a worker |
| `running`    | Worker has started the job |
| `finished`   | Job completed successfully |
| `failed`     | Job ended with an error |
| `stopped`    | Job was manually stopped |
| `timeout`    | Job exceeded max runtime |

**Stop Eligibility:** Only `queued` or `running` jobs can be stopped.

---

## API Overview

| Method | Endpoint                                | Description |
|--------|-----------------------------------------|-------------|
| POST   | `/run-simulation`                       | Launch new simulation job |
| GET    | `/status/{job_id}`                      | Get job status |
| GET    | `/result/{job_id}`                      | Retrieve results |
| GET    | `/progress/{job_id}`                    | Retrieve progress JSON |
| GET    | `/logs/{job_id}`                        | Stream stdout logs |
| GET    | `/logs/file/{job_id}`                   | Stream training log file |
| POST   | `/stop/{job_id}`                        | Stop a running job |
| GET    | `/jobs`                                 | List all jobs |
| GET    | `/job-info/{job_id}`                    | Get job metadata |
| DELETE | `/job/{job_id}`                         | Delete job folder & metadata |
| GET    | `/hosts`                                | List available hosts |
| POST   | `/experiment-config/create`             | Create new config file |
| GET    | `/experiment-configs`                   | List config files |
| GET    | `/experiment-config/{file}`             | View a config |
| DELETE | `/experiment-config/{file}`             | Delete a config |
| POST   | `/dataset`                              | Create dataset from MongoDB |
| GET    | `/datasets`                             | List datasets |
| DELETE | `/dataset/{name}`                       | Delete dataset |
| GET    | `/sites`                                | List MongoDB sites |
| GET    | `/real-time-data/{site}`                | Retrieve real-time MongoDB data |
| GET    | `/health`                               | API health check |

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

## 4️⃣ Monitoring & Stopping Jobs

```
curl http://SERVER:8000/status/{job_id}
curl http://SERVER:8000/progress/{job_id}
curl http://SERVER:8000/logs/{job_id}
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