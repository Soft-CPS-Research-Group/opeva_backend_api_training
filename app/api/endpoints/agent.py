# app/api/endpoints/agent.py
from fastapi import APIRouter, Response, Depends
from app.controllers import agent_controller
from app.models.agent import NextJobRequest, StatusRequest, HeartbeatRequest

router = APIRouter()

def _close_connection(response: Response) -> None:
    response.headers["Connection"] = "close"

@router.post("/api/agent/next-job")
def next_job(req: NextJobRequest, response: Response = Depends(_close_connection)):
    job = agent_controller.next_job(req)
    if job is None:
        response.status_code = 204
        return
    return job

@router.post("/api/agent/job-status")
def job_status(req: StatusRequest, response: Response = Depends(_close_connection)):
    return agent_controller.job_status(req)


@router.post("/api/agent/heartbeat")
def heartbeat(req: HeartbeatRequest, response: Response = Depends(_close_connection)):
    return agent_controller.heartbeat(req)
