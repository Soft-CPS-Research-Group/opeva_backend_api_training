from fastapi import APIRouter, Path, Query

from app.controllers import mongo_controller

router = APIRouter()


@router.get("/energy-communities", tags=["MongoDB"])
async def list_energy_communities():
    return await mongo_controller.get_energy_communities()


@router.get("/historical-data/{energy_community}", tags=["MongoDB"])
async def get_historical_data(
    energy_community: str = Path(..., description="MongoDB database name"),
    limit: int = Query(..., ge=1, description="Required page size per collection."),
    offset: int = Query(0, ge=0, description="Optional offset per collection."),
    minutes: int | None = Query(None, ge=1, description="Fetch only the last X minutes."),
    from_ts: str | None = Query(None, description="Range start in ISO-8601."),
    until_ts: str | None = Query(None, description="Range end in ISO-8601."),
    granularity_minutes: int | None = Query(
        None,
        ge=1,
        description="Optional aggregation bucket size in minutes.",
    ),
):
    return await mongo_controller.get_historical_data(
        energy_community=energy_community,
        limit=limit,
        offset=offset,
        minutes=minutes,
        from_ts=from_ts,
        until_ts=until_ts,
        granularity_minutes=granularity_minutes,
    )
