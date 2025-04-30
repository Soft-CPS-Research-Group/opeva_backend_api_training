from fastapi import APIRouter
from app.api.endpoints import jobs, configs, datasets, mongo, health

api_router = APIRouter()
api_router.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])
api_router.include_router(configs.router, prefix="/configs", tags=["Configs"])
api_router.include_router(datasets.router, prefix="/datasets", tags=["Datasets"])
api_router.include_router(mongo.router, prefix="/api", tags=["Mongo"])
api_router.include_router(health.router, prefix="/health", tags=["Health"])