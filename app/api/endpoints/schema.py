from fastapi import APIRouter
from app.controllers.schema_controller import (
    create_schema_controller,
    update_schema_controller,
    get_schema_controller
)
from app.models.schema import SchemaCreateRequest, SchemaUpdateRequest

router = APIRouter()

@router.post("/schema/create")
def create_schema(request: SchemaCreateRequest):
    return create_schema_controller(request.site, request.schema)

@router.put("/schema/update/{site}")
def update_schema(site: str, request: SchemaUpdateRequest):
    return update_schema_controller(site, request.schema)

@router.get("/schema/{site}")
def get_schema(site: str):
    return get_schema_controller(site)

