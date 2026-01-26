# app/api/router.py
from fastapi import APIRouter
from app.api.endpoints import jobs, configs, datasets, mongo, health, schema, agent, ops  # <-- add agent

api_router = APIRouter()
api_router.include_router(jobs.router, prefix="", tags=["Jobs"])
api_router.include_router(configs.router, prefix="", tags=["Configs"])
api_router.include_router(datasets.router, prefix="", tags=["Datasets"])
api_router.include_router(mongo.router, prefix="", tags=["Mongo"])
api_router.include_router(health.router, prefix="", tags=["Health"])
api_router.include_router(schema.router, prefix="", tags=["Schema"])
api_router.include_router(agent.router, prefix="", tags=["Agent"])  # <-- add this line
api_router.include_router(ops.router, prefix="", tags=["Ops"])
