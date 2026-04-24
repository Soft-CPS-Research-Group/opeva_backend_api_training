# app/api/router.py
from fastapi import APIRouter
from app.api.endpoints import (
    agent,
    configs,
    datasets,
    deploy,
    health,
    jobs,
    mongo,
    ops,
    schema,
    simulation_data,
    real_time
)

api_router = APIRouter()
api_router.include_router(jobs.router, prefix="", tags=["Jobs"])
api_router.include_router(configs.router, prefix="", tags=["Configs"])
api_router.include_router(datasets.router, prefix="", tags=["Datasets"])
api_router.include_router(mongo.router, prefix="", tags=["Mongo"])
api_router.include_router(health.router, prefix="", tags=["Health"])
api_router.include_router(schema.router, prefix="", tags=["Schema"])
api_router.include_router(agent.router, prefix="", tags=["Agent"])
api_router.include_router(ops.router, prefix="", tags=["Ops"])
api_router.include_router(simulation_data.router, prefix="", tags=["SimulationData"])
api_router.include_router(deploy.router, prefix="", tags=["Deploy"])
api_router.include_router(real_time.router, prefix="", tags=["RealTime"])
