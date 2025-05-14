from fastapi import APIRouter, Body, HTTPException
from app.controllers import config_controller

router = APIRouter()

@router.post("/experiment-config/create")
async def create_config(config: dict = Body(...), file_name: str = Body(...)):
    return config_controller.create_config(config, file_name)

@router.get("/experiment-configs")
async def list_configs():
    return config_controller.list_configs()

@router.get("/experiment-config/{file_name}")
async def get_config(file_name: str):
    return config_controller.get_config(file_name)

@router.delete("/experiment-config/{file_name}")
async def delete_config(file_name: str):
    return config_controller.delete_config(file_name)