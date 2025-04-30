from app.utils import file_utils

def create_dataset(name: str, schema: dict, data_files: dict):
    file_utils.create_dataset_dir(name, schema, data_files)
    return {"message": "Dataset created", "name": name}

def list_datasets():
    return file_utils.list_available_datasets()