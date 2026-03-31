from pydantic import BaseModel
from typing import Optional, Dict


class JobLaunchRequest(BaseModel):
    config: Optional[Dict] = None
    config_path: Optional[str] = None
    target_host: Optional[str] = None
    save_as: Optional[str] = None
    job_name: Optional[str] = None
    submitted_by: Optional[str] = None
    image: Optional[str] = None

class SimulationRequest(BaseModel):
    config_path: str
    job_name: Optional[str] = None
