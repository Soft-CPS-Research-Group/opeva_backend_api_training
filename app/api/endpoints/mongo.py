from fastapi import APIRouter, Path, Query
from app.controllers import mongo_controller

router = APIRouter()

@router.get("/sites", tags=["MongoDB"])
async def list_sites():
    return await mongo_controller.get_available_sites()

@router.get("/real-time-data/{site_name}", tags=["MongoDB"])
async def get_site_data(
    site_name: str = Path(..., description="MongoDB database name"),
    minutes: int = Query(None, description="Optional: only data from last X minutes")
):
    return await mongo_controller.get_site_data(site_name, minutes)