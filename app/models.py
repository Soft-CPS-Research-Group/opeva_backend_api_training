from pydantic import BaseModel
from typing import Optional, Dict

class JobLaunchRequest(BaseModel):
    config: Optional[Dict] = None               # Full config content (for new jobs)
    config_path: Optional[str] = None           # Or use an existing config file
    target_host: str = "local"                 # Local or remote machine via SSH
    save_as: Optional[str] = None               # If sending config, filename to save it

class SimulationRequest(BaseModel):
    config_path: str                            # Path in /data/ to config file
    job_name: Optional[str] = None              # Name for tracking/logging only