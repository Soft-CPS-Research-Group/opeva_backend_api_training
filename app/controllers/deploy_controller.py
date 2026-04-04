from __future__ import annotations

from fastapi import UploadFile

from app.services import deploy_service


def list_inferences() -> list[dict]:
    return deploy_service.list_inference_targets()


def get_inference_health(target_id: str) -> dict:
    return deploy_service.get_inference_health(target_id)


def switch_bundle(target_id: str, bundle_id: str) -> dict:
    return deploy_service.switch_inference_bundle(target_id, bundle_id)


def list_bundles() -> list[dict]:
    return deploy_service.list_bundles()


def upload_bundle_folder(files: list[UploadFile], relative_paths: list[str] | None = None) -> dict:
    return deploy_service.upload_bundle_folder(files, relative_paths)


def delete_bundle(bundle_id: str) -> dict:
    return deploy_service.delete_bundle(bundle_id)


def stream_logs(target_id: str, tail: int = 200):
    return deploy_service.stream_inference_logs(target_id, tail)
