import os
from pydantic_settings import BaseSettings
from typing import ClassVar

class Settings(BaseSettings):
    VM_SHARED_DATA: str = "/opt/opeva_shared_data"

    CONFIGS_DIR: str = os.path.join(VM_SHARED_DATA, "configs")
    JOB_TRACK_FILE: str = os.path.join(VM_SHARED_DATA, "job_track.json")
    JOBS_DIR: str = os.path.join(VM_SHARED_DATA, "jobs")
    DATASETS_DIR: str = os.path.join(VM_SHARED_DATA, "datasets")

    AVAILABLE_HOSTS: list = [
        {"name": "local", "host": "local"},
        {"name": "gpu-server-1", "host": "192.168.1.100"},
        {"name": "gpu-server-2", "host": "192.168.1.101"}
    ]

    MONGO_USER: str = "runtimeUI"
    MONGO_PASSWORD: str = "runtimeUIDB"
    MONGO_HOST: str = "193.136.62.78"
    MONGO_PORT: int = 27017
    MONGO_AUTH_SOURCE: str = "admin"
    ACCEPTABLE_GAP_IN_MINUTES: int = 60


    BUILDING_DATASET_CSV_HEADER: ClassVar[dict[str, str]] = {
        "month": "first",
        "hour": "first",
        "minutes": "first",
        "day_type": "first",
        "daylight_savings_status": "first",
        "indoor_dry_bulb_temperature": "sum",
        "average_unmet_cooling_setpoint_difference": "sum",
        "indoor_relative_humidity": "sum",
        "non_shiftable_load": "sum",
        "dhw_demand": "sum",
        "cooling_demand": "sum",
        "heating_demand": "sum",
        "solar_generation": "sum"
    }

    TIMESTAMP_DATASET_CSV_HEADER: ClassVar[list[str]] = [
        "month", "hour", "minutes", "day_type", "daylight_savings_status"
    ]

    EV_DATASET_CSV_HEADER: ClassVar[dict[str, str]] = {
        "electric_vehicle_charger_state" : "first",
        "power" : "first",
        "electric_vehicle_id" : "first",
        "electric_vehicle_battery_capacity_khw" : "first",
        "current_soc" : "first",
        "electric_vehicle_departure_time" : "first",
        "electric_vehicle_required_soc_departure" : "first",
        "electric_vehicle_estimated_arrival_time" : "first",
        "electric_vehicle_estimated_soc_arrival" : "first"
    }

    PRICE_DATASET_CSV_HEADER: ClassVar[dict[str, str]] = {
        "electricity_pricing" : "mean",
        "electricity_pricing_predicted_1" : "",
        "electricity_pricing_predicted_2" : "",
        "electricity_pricing_predicted_3" : ""
    }

    def mongo_uri(self, db_name: str) -> str:
        return f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/{db_name}?authSource={self.MONGO_AUTH_SOURCE}"

settings = Settings()