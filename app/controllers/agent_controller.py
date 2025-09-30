# app/controllers/agent_controller.py
from fastapi import HTTPException
from app.services import job_service
from app.models.agent import NextJobRequest, StatusRequest, HeartbeatRequest

def next_job(req: NextJobRequest):
    job = job_service.agent_next_job(req.worker_id)
    if not job:
        # the endpoint will return 204; controller returns None to signal that
        return None
    return job

def job_status(req: StatusRequest):
    try:
        payload = req.model_dump(exclude_none=True)
        job_id = payload.pop("job_id")
        status = payload.pop("status")
        return job_service.agent_update_status(job_id, status, payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def heartbeat(req: HeartbeatRequest):
    job_service.record_host_heartbeat(req.worker_id, req.info or {})
    return {"ok": True}
