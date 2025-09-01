from enum import Enum
from pydantic import BaseModel
from typing import Optional, Dict

class JobLaunchRequest(BaseModel):
    config: Optional[Dict] = None
    config_path: Optional[str] = None
    target_host: str = "local"
    save_as: Optional[str] = None

class SimulationRequest(BaseModel):
    config_path: str
    job_name: Optional[str] = None


class JobStatus(str, Enum):
    PENDING = "pending"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"

