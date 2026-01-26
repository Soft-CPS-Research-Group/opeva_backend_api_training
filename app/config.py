# app/config.py
import os
from pydantic_settings import BaseSettings
from typing import ClassVar

class Settings(BaseSettings):
    # ── Shared root ──────────────────────────────────────────────────────────────
    VM_SHARED_DATA: str = "/opt/opeva_shared_data"

    # ── Directories used by the app (must exist) ────────────────────────────────
    CONFIGS_DIR: str   = os.path.join(VM_SHARED_DATA, "configs")
    JOB_TRACK_FILE: str = os.path.join(VM_SHARED_DATA, "job_track.json")
    JOBS_DIR: str      = os.path.join(VM_SHARED_DATA, "jobs")
    DATASETS_DIR: str  = os.path.join(VM_SHARED_DATA, "datasets")
    QUEUE_DIR: str     = os.path.join(VM_SHARED_DATA, "queue")  # for worker-agent jobs
    QUEUE_CLAIM_TTL: int = 300  # seconds before a claimed queue file is re-queued

    # ── Hosts: simple names. "local" runs on server; others go to that worker's agent ─
    AVAILABLE_HOSTS: list[str] = ["local", "server", "gpu-server-1", "gpu-server-2", "tiago-laptop"]
    HOST_HEARTBEAT_TTL: int = 60  # seconds

    # ── Mongo (unchanged from your setup) ───────────────────────────────────────
    MONGO_USER: str = "runtimeUI"
    MONGO_PASSWORD: str = "runtimeUIDB"
    MONGO_HOST: str = "193.136.62.78"
    MONGO_PORT: int = 27017
    MONGO_AUTH_SOURCE: str = "admin"
    ACCEPTABLE_GAP_IN_MINUTES: int = 60

    # ── CSV headers you already had (keep as-is) ────────────────────────────────
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
        "timestamp": "first",
        "electric_vehicle_charger_state": "first",
        "power": "first",
        "electric_vehicle_id": "first",
        "electric_vehicle_battery_capacity_khw": "first",
        "current_soc": "first",
        "electric_vehicle_departure_time": "first",
        "electric_vehicle_required_soc_departure": "first",
        "electric_vehicle_estimated_arrival_time": "first",
        "electric_vehicle_estimated_soc_arrival": "first",
        "charger": "first",
        "mode": "first"
    }

    PRICE_DATASET_CSV_HEADER: ClassVar[dict[str, str]] = {
        "energy_price": "mean",
        "energy_price_predicted_1": "",
        "energy_price_predicted_2": "",
        "energy_price_predicted_3": ""
    }

    # ── Job/agent defaults ──────────────────────────────────────────────────────
    DEFAULT_JOB_IMAGE: str = "calof/opeva_simulator:latest"
    CONTAINER_NAME_PREFIX: str = "opeva_job"
    WORKER_STALE_GRACE_SECONDS: int = 120  # additional grace beyond heartbeat TTL

    def mongo_uri(self, db_name: str) -> str:
        return (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOST}:{self.MONGO_PORT}/{db_name}"
            f"?authSource={self.MONGO_AUTH_SOURCE}"
        )

settings = Settings()
