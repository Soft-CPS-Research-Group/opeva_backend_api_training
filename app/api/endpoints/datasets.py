from fastapi import APIRouter, Body
from app.controllers import dataset_controller

router = APIRouter()

@router.post("/dataset")
async def create_dataset(name: str = Body(...), schema: dict = Body(...), data_files: dict = Body(default={})):
    return dataset_controller.create_dataset(name, schema, data_files)

@router.get("/datasets")
async def list_datasets():
    return dataset_controller.list_datasets()