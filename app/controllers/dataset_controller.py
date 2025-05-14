from app.services import dataset_service

def create_dataset(name: str, site_id: str, config: dict, period : int = 60, from_ts: str = None, until_ts: str = None):
    return dataset_service.create_dataset(name, site_id, config, period, from_ts, until_ts)

def list_dates_available_per_collection(site_id: str):
    return dataset_service.list_dates_available_per_collection(site_id)

def list_datasets():
    return dataset_service.list_datasets()

def delete_dataset(name: str):
    return dataset_service.delete_dataset(name)