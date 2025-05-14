from pydantic import BaseModel
from typing import Dict

class SchemaCreateRequest(BaseModel):
    site: str
    schema: Dict

class SchemaUpdateRequest(BaseModel):
    schema: Dict