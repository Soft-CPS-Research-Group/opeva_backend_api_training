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


@router.get("/deploy/bundles")
def list_bundles():
    return deploy_controller.list_bundles()


@router.post("/deploy/bundles/upload-folder")
def upload_bundle_folder(
    files: list[UploadFile] = File(...),
    relative_paths: list[str] | None = Form(default=None),
):
    return deploy_controller.upload_bundle_folder(files, relative_paths)


@router.delete("/deploy/bundles/{bundle_id}")
def delete_bundle(bundle_id: str):
    return deploy_controller.delete_bundle(bundle_id)
