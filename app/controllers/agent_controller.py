# app/controllers/agent_controller.py
from fastapi import HTTPException
from app.services import job_service
from app.models.agent import NextJobRequest, StatusRequest

def next_job(req: NextJobRequest):
    job = job_service.agent_next_job(req.worker_id)
    if not job:
        # the endpoint will return 204; controller returns None to signal that
        return None
    return job

def job_status(req: StatusRequest):
    try:
        extra = {}
        if req.worker_id is not None:
            extra["worker_id"] = req.worker_id
        if req.container_id is not None:
            extra["container_id"] = req.container_id
        if req.container_name is not None:
            extra["container_name"] = req.container_name
        return job_service.agent_update_status(req.job_id, req.status, extra)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
