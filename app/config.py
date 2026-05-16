# app/config.py
import json
import os
from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import Any


def _parse_cors_allowed_origins(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []

        if raw.startswith("["):
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError:
                # Fall back to CSV-style parsing when env is not valid JSON.
                pass
            else:
                if isinstance(decoded, list):
                    return [str(item).strip() for item in decoded if str(item).strip()]

        normalized = raw.lstrip("[").rstrip("]")
        origins = []
        for item in normalized.split(","):
            candidate = item.strip().strip('"').strip("'").strip()
            if candidate:
                origins.append(candidate)
        return origins

    return []


def _parse_deploy_targets(value: Any) -> list[dict[str, str]]:
    if value in (None, "", []):
        return []

    raw = value
    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("DEPLOY_INFERENCE_TARGETS must be valid JSON") from exc

    if not isinstance(raw, list):
        raise ValueError("DEPLOY_INFERENCE_TARGETS must be a list")

    targets: list[dict[str, str]] = []
    required = ("id", "name", "base_url", "container_name", "bundle_mount_path")
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError("Each DEPLOY_INFERENCE_TARGETS item must be an object")
        normalized = {key: str(entry.get(key, "")).strip() for key in required}
        missing = [key for key, candidate in normalized.items() if not candidate]
        if missing:
            raise ValueError(f"Invalid target entry; missing keys: {', '.join(missing)}")
        targets.append(normalized)

    return targets

class Settings(BaseSettings):
    # ── Shared root ──────────────────────────────────────────────────────────────
    VM_SHARED_DATA: str = "/opt/opeva_shared_data"

    # ── Directories used by the app (must exist) ────────────────────────────────
    DEPLOY_BUNDLES_DIR: str = os.path.join(VM_SHARED_DATA, "inference_bundles")
    DEPLOY_BUNDLE_STORAGE_DIR: str = os.path.join(DEPLOY_BUNDLES_DIR, "bundles")
    DEPLOY_BUNDLE_INDEX_FILE: str = os.path.join(DEPLOY_BUNDLES_DIR, "index.json")
    DEPLOY_INFERENCE_TARGETS: list[dict[str, str]] = [
        {
            "id": "hq",
            "name": "HQ",
            "base_url": "http://energaize_inference_hq:8002",
            "container_name": "energaize_inference_hq",
            "bundle_mount_path": "/data",
        },
        {
            "id": "sao_mamede",
            "name": "Sao Mamede",
            "base_url": "http://energaize_inference_sao_mamede:8002",
            "container_name": "energaize_inference_sao_mamede",
            "bundle_mount_path": "/data",
        },
        {
            "id": "rh01",
            "name": "RH01",
            "base_url": "http://energaize_inference_rh01:8002",
            "container_name": "energaize_inference_rh01",
            "bundle_mount_path": "/data",
        },
    ]

    # ── Mongo (unchanged from your setup) ───────────────────────────────────────
    MONGO_USER: str = "runtimeUI"
    MONGO_PASSWORD: str = "runtimeUIDB"
    MONGO_HOST: str = "193.136.62.78"
    MONGO_PORT: int = 27017
    MONGO_AUTH_SOURCE: str = "admin"

    CORS_ALLOWED_ORIGINS: str | list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8006",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8006",
        "http://193.136.62.78:3000",
        "http://193.136.62.78:8006",
        "https://softcps.dei.isep.ipp.pt:3001",
        "https://softcps.dei.isep.ipp.pt:8001",
    ]

    @field_validator("CORS_ALLOWED_ORIGINS", mode="after")
    @classmethod
    def _parse_cors_origins(cls, value: Any) -> list[str]:
        return _parse_cors_allowed_origins(value)

    @field_validator("DEPLOY_INFERENCE_TARGETS", mode="before")
    @classmethod
    def _parse_deploy_inference_targets(cls, value: Any) -> list[dict[str, str]]:
        return _parse_deploy_targets(value)

    def mongo_uri(self, db_name: str) -> str:
        return (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOST}:{self.MONGO_PORT}/{db_name}"
            f"?authSource={self.MONGO_AUTH_SOURCE}"
        )

settings = Settings()
