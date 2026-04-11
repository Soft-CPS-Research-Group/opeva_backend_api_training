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


def list_bundle_files(bundle_id: str) -> dict:
    return deploy_service.list_bundle_files(bundle_id)


def read_bundle_file_content(bundle_id: str, rel_path: str) -> dict:
    return deploy_service.read_bundle_file_content(bundle_id, rel_path)


def upload_bundle_folder(files: list[UploadFile], relative_paths: list[str] | None = None) -> dict:
    return deploy_service.upload_bundle_folder(files, relative_paths)


def delete_bundle(bundle_id: str) -> dict:
    return deploy_service.delete_bundle(bundle_id)


def stream_logs(target_id: str, tail: int = 200):
    return deploy_service.stream_inference_logs(target_id, tail)


def logs_history_chunk(
    target_id: str,
    *,
    since_ts: str,
    until_ts: str | None = None,
    cursor: str | None = None,
    limit_lines: int = 500,
    search: str | None = None,
) -> dict:
    return deploy_service.fetch_inference_logs_history_chunk(
        target_id,
        since_ts=since_ts,
        until_ts=until_ts,
        cursor=cursor,
        limit_lines=limit_lines,
        search=search,
    )
