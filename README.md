# OPEVA Backend API

FastAPI service for the non-job OPEVA platform APIs: MongoDB utilities, schema management, inference bundle deployment, real-time data and health checks.

Job orchestration has moved to the separate **OPEVA Job Orchestrator** service:

- repository: `job_orchestrator_agent`
- default public port: `8011`
- worker base URL inside Docker: `http://job_orchestrator_agent:8011`

Use that service for `/run-simulation`, `/jobs`, `/queue`, `/status/{job_id}`, `/api/agent/*`, `/ops/*`, `/experiment-config*`, `/dataset*`, `/datasets`, logs, progress, results and job artefact APIs.

## Running

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Docker:

```bash
docker build -t opeva-backend .
docker run --rm -p 8000:8000 \
  -v /opt/opeva_shared_data:/opt/opeva_shared_data \
  opeva-backend
```

## API Areas

- `GET /health`
- Mongo APIs: `/energy-communities`, `/historical-data/{energy_community}`, schema collection helpers
- Schema APIs: `/schema/*`
- Deploy APIs: `/deploy/*`
- Real-time APIs: `/real-time/*`

The Postman collection in this repository now covers only these non-job APIs. The jobs/datasets/configs/agent/ops collection lives in `job_orchestrator_agent`.

## Shared Storage

This backend still uses `/opt/opeva_shared_data` for inference bundles:

```text
/opt/opeva_shared_data
└── inference_bundles/
```

The Job Orchestrator owns `configs/`, `datasets/`, `jobs/`, `queue/` and `job_track.json`.

## Configuration

Common environment variables:

| Variable | Description |
| --- | --- |
| `VM_SHARED_DATA` | Shared storage root, default `/opt/opeva_shared_data`. |
| `DEPLOY_BUNDLES_DIR` | Inference bundle storage root. |
| `DEPLOY_INFERENCE_TARGETS` | JSON list of inference services exposed by `/deploy/*`. |
| `MONGO_*` | Credentials/host/port for site databases. |
| `CORS_ALLOWED_ORIGINS` | JSON or CSV list of allowed frontend origins. |

## Testing

```bash
pytest
```

The remaining suite covers Mongo/schema, deploy, real-time and service/controller behavior. Job lifecycle, datasets and worker-contract tests live in `job_orchestrator_agent`.
