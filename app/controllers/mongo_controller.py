from typing import Optional

from app.services import mongo_service


async def get_energy_communities():
    return {"energy_communities": mongo_service.list_energy_communities()}


async def get_historical_data(
    energy_community: str,
    minutes: Optional[int] = None,
    from_ts: Optional[str] = None,
    until_ts: Optional[str] = None,
):
    return mongo_service.get_historical_data(
        energy_community=energy_community,
        minutes=minutes,
        from_ts=from_ts,
        until_ts=until_ts,
    )
