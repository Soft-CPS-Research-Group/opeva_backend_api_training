from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from app.docker_manager import (
    run_simulation,
    get_container_status,
    stop_container,
    get_simulation_result,
    stream_container_logs,
    get_simulation_progress
)
from app.models import SimulationRequest
from app.utils import load_jobs, save_job, ensure_directories
import uuid

app = FastAPI()

# Ensure folders exist on startup
ensure_directories()
jobs = load_jobs()

@app.post("/run-simulation")
async def run_simulation_endpoint(request: SimulationRequest):
    try:
        job_id = str(uuid.uuid4())
        container = run_simulation(job_id, request)
        jobs[job_id] = container.id
        save_job(job_id, container.id)
        return {"job_id": job_id, "container_id": container.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{job_id}")
async def check_status(job_id: str):
    container_id = jobs.get(job_id)
    if not container_id:
        raise HTTPException(status_code=404, detail="Job not found")
    status = get_container_status(container_id)
    return {"job_id": job_id, "status": status}

@app.get("/result/{job_id}")
async def get_result(job_id: str):
    return get_simulation_result(job_id)

@app.get("/progress/{job_id}")
async def get_progress(job_id: str):
    return get_simulation_progress(job_id)

@app.get("/logs/{job_id}")
async def get_logs(job_id: str):
    container_id = jobs.get(job_id)
    if not container_id:
        raise HTTPException(status_code=404, detail="Job not found")
    return StreamingResponse(stream_container_logs(container_id), media_type="text/plain")

@app.post("/stop/{job_id}")
async def stop_job(job_id: str):
    container_id = jobs.get(job_id)
    if not container_id:
        raise HTTPException(status_code=404, detail="Job not found")
    result = stop_container(container_id)
    return {"message": result}

@app.get("/jobs")
async def list_jobs():
    return [{"job_id": job_id, "container_id": container_id, "status": get_container_status(container_id)} for job_id, container_id in jobs.items()]

@app.get("/health")
async def health_check():
    return {"status": "ok"}
