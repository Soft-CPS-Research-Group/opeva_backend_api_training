from app.utils import file_utils

def save_config(config: dict, file_name: str):
    file_utils.save_config_dict(config, file_name)
    return {"message": "Config saved", "file": file_name}

def list_configs():
    return file_utils.list_config_files()

def get_config_by_name(file_name: str):
    return {"config": file_utils.load_config_file(file_name)}

def delete_config(file_name: str):
    success = file_utils.delete_config_by_name(file_name)
    if not success:
        raise FileNotFoundError("Config file not found")
    return {"message": f"Config {file_name} deleted successfully"}