from fastapi import APIRouter, Body
from app.controllers import dataset_controller
from typing import Optional

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

@router.get("/datasets/dates-available")
async def list_dates_available_per_collection():
    return dataset_controller.list_dates_available_per_collection()
@router.get("/datasets")
async def list_datasets():
    return dataset_controller.list_datasets()

@router.delete("/dataset/{name}")
async def delete_dataset(name: str):
    return dataset_controller.delete_dataset(name)