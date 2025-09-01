# app/models/agent.py
from pydantic import BaseModel

class NextJobRequest(BaseModel):
    worker_id: str

class StatusRequest(BaseModel):
    job_id: str
    status: str
    worker_id: str | None = None
    container_id: str | None = None
    container_name: str | None = None
