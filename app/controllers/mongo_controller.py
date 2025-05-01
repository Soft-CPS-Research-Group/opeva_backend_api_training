from app.services import mongo_service

async def get_available_sites():
    return {"sites": mongo_service.get_all_sites()}

async def get_site_data(site_name: str, minutes: int | None = None):
    return mongo_service.get_all_collections(site_name, minutes)