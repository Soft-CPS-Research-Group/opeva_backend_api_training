from fastapi import HTTPException
from app.services import job_service
from app.models.job import JobLaunchRequest

async def run_simulation(request: JobLaunchRequest):
    try:
        return await job_service.launch_simulation(request)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_status(job_id: str):
    return job_service.get_status(job_id)

def get_result(job_id: str):
    return job_service.get_result(job_id)

def get_progress(job_id: str):
    return job_service.get_progress(job_id)

def get_logs(job_id: str):
    return job_service.get_logs(job_id)

def stop_job(job_id: str):
    return job_service.stop_job(job_id)

def list_jobs():
    return job_service.list_jobs()


def list_queue():
    return job_service.list_queue()

def get_job_info(job_id: str):
    return job_service.get_job_info(job_id)

def delete_job(job_id: str):
    return job_service.delete_job(job_id)

def get_file_logs(job_id: str):
    return job_service.get_file_logs(job_id)

def get_hosts():
    return job_service.get_hosts()
