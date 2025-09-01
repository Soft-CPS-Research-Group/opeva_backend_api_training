from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import FileResponse
from app.controllers import dataset_controller
from typing import Optional
import os

router = APIRouter()

@router.post("/dataset")
async def create_dataset(
    name: str = Body(...),
    site_id: str = Body(...),
    citylearn_configs: dict = Body(...),
    period : Optional[int] = Body(60),
    from_ts: Optional[str] = Body(None),
    until_ts: Optional[str] = Body(None)
):
    return dataset_controller.create_dataset(name, site_id, citylearn_configs, period, from_ts, until_ts)

@router.get("/dataset/dates-available/{site_id}")
async def list_dates_available_per_collection(site_id : str):
    return dataset_controller.list_dates_available_per_collection(site_id)
@router.get("/datasets")
async def list_datasets():
    return dataset_controller.list_datasets()

@router.get("/dataset/download/{name}")
async def download_dataset(name: str):
    try:
        file_path = dataset_controller.download_dataset(name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return FileResponse(file_path, filename=os.path.basename(file_path))

@router.delete("/dataset/{name}")
async def delete_dataset(name: str):
    try:
        return dataset_controller.delete_dataset(name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dataset not found")
