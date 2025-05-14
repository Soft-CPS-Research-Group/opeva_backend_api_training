from app.utils import file_utils

def create_dataset(name: str, site_id: str, citylearn_configs: dict, period : int = 60, from_ts: str = None, until_ts: str = None):
    file_utils.create_dataset_dir(name, site_id, citylearn_configs, period, from_ts, until_ts)
    return {"message": "Dataset created", "name": name}

def list_dates_available_per_collection(site_id: str):
    return file_utils.list_dates_available_per_collection(site_id)

def list_datasets():
    return file_utils.list_available_datasets()

def delete_dataset(name: str):
    success = file_utils.delete_dataset_by_name(name)
    if not success:
        raise FileNotFoundError(f"Dataset {name} not found")
    return {"message": f"Dataset '{name}' deleted"}