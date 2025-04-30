from app.services import dataset_service

def create_dataset(name: str, schema: dict, data_files: dict):
    return dataset_service.create_dataset(name, schema, data_files)

def list_datasets():
    return dataset_service.list_datasets()