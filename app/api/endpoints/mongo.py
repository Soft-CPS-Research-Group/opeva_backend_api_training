from fastapi import APIRouter
from app.controllers import mongo_controller

router = APIRouter()

@router.get("/icharging-headquarters")
async def get_icharging():
    return mongo_controller.get_icharging_data()

@router.get("/living-lab")
async def get_living_lab():
    return mongo_controller.get_living_lab_data()