from pydantic import BaseModel

class SimulationRequest(BaseModel):
    param1: str
    param2: str
    config_file: str