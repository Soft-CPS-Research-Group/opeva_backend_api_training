from __future__ import annotations

from fastapi import APIRouter, File, Form, Query, UploadFile
from fastapi.responses import StreamingResponse

from app.controllers import deploy_controller
from app.models.deploy import SwitchBundleRequest

router = APIRouter()


@router.get("/deploy/inferences")
def list_inferences():
    return deploy_controller.list_inferences()


@router.get("/deploy/inferences/{target_id}/health")
def inference_health(target_id: str):
    return deploy_controller.get_inference_health(target_id)


@router.post("/deploy/inferences/{target_id}/switch-bundle")
def switch_bundle(target_id: str, payload: SwitchBundleRequest):
    return deploy_controller.switch_bundle(target_id, payload.bundle_id)


@router.get("/deploy/inferences/{target_id}/logs/stream")
def stream_logs(target_id: str, tail: int = Query(default=200, ge=0, le=5000)):
    return StreamingResponse(
        deploy_controller.stream_logs(target_id, tail),
        media_type="text/plain",
        headers={"Cache-Control": "no-cache"},
    )


@router.get("/deploy/inferences/{target_id}/logs/history/chunk")
def logs_history_chunk(
    target_id: str,
    since_ts: str = Query(..., min_length=1),
    until_ts: str | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit_lines: int = Query(default=500, ge=1, le=2000),
    search: str | None = Query(default=None),
):
    return deploy_controller.logs_history_chunk(
        target_id,
        since_ts=since_ts,
        until_ts=until_ts,
        cursor=cursor,
        limit_lines=limit_lines,
        search=search,
    )


@router.get("/deploy/bundles")
def list_bundles():
    return deploy_controller.list_bundles()


@router.get("/deploy/bundles/{bundle_id}/files")
def list_bundle_files(bundle_id: str):
    return deploy_controller.list_bundle_files(bundle_id)


@router.get("/deploy/bundles/{bundle_id}/files/content")
def read_bundle_file_content(bundle_id: str, path: str = Query(..., min_length=1)):
    return deploy_controller.read_bundle_file_content(bundle_id, path)


@router.post("/deploy/bundles/upload-folder")
def upload_bundle_folder(
    files: list[UploadFile] = File(...),
    relative_paths: list[str] | None = Form(default=None),
):
    return deploy_controller.upload_bundle_folder(files, relative_paths)


@router.delete("/deploy/bundles/{bundle_id}")
def delete_bundle(bundle_id: str):
    return deploy_controller.delete_bundle(bundle_id)
