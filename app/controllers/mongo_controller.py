from fastapi import HTTPException
from app.services import mongo_service

def get_icharging_data():
    try:
        return mongo_service.get_collection("i-charging_headquarters", "i-charging headquarters")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_living_lab_data():
    try:
        return mongo_service.get_all_collections("living_lab")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))