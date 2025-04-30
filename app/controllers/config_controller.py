from fastapi import HTTPException
from app.services import config_service

def create_config(config: dict, file_name: str):
    try:
        return config_service.save_config(config, file_name)
    except FileExistsError as e:
        raise HTTPException(status_code=400, detail=str(e))

def list_configs():
    return config_service.list_configs()

def get_config(file_name: str):
    return config_service.get_config_by_name(file_name)

def delete_config(file_name: str):
    return config_service.delete_config(file_name)