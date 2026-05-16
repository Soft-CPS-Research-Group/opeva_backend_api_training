# app/api/router.py
from fastapi import APIRouter
from app.api.endpoints import (
    deploy,
    health,
    mongo,
    schema,
    real_time
)

api_router = APIRouter()
api_router.include_router(mongo.router, prefix="", tags=["Mongo"])
api_router.include_router(health.router, prefix="", tags=["Health"])
api_router.include_router(schema.router, prefix="", tags=["Schema"])
api_router.include_router(deploy.router, prefix="", tags=["Deploy"])
api_router.include_router(real_time.router, prefix="", tags=["RealTime"])
