from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.controllers import job_controller
from app.models.job import JobLaunchRequest

router = APIRouter()

@router.post("/run-simulation")
async def run_sim(request: JobLaunchRequest):
    return await job_controller.run_simulation(request)

@router.get("/status/{job_id}")
async def get_status(job_id: str):
    return job_controller.get_status(job_id)

@router.get("/result/{job_id}")
async def get_result(job_id: str):
    return job_controller.get_result(job_id)

@router.get("/progress/{job_id}")
async def get_progress(job_id: str):
    return job_controller.get_progress(job_id)

@router.get("/logs/{job_id}")
async def get_logs(job_id: str):
    return StreamingResponse(job_controller.get_logs(job_id), media_type="text/plain")

@router.post("/stop/{job_id}")
async def stop_job(job_id: str):
    return job_controller.stop_job(job_id)

@router.get("/jobs")
async def list_jobs():
    return job_controller.list_jobs()

@router.get("/job-info/{job_id}")
async def job_info(job_id: str):
    return job_controller.get_job_info(job_id)

@router.delete("/job/{job_id}")
async def delete_job(job_id: str):
    return job_controller.delete_job(job_id)

@router.get("/file-logs/{job_id}")
async def file_logs(job_id: str):
    return StreamingResponse(job_controller.get_file_logs(job_id), media_type="text/plain")

@router.get("/hosts")
def hosts():
    return job_controller.get_hosts()