from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Iterable

import fcntl
import httpx
from fastapi import HTTPException, UploadFile

from app import config as app_config


@dataclass(frozen=True)
class InferenceTarget:
    id: str
    name: str
    base_url: str
    container_name: str
    bundle_mount_path: str


def _settings():
    return app_config.settings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_deploy_dirs() -> None:
    settings = _settings()
    os.makedirs(settings.DEPLOY_BUNDLES_DIR, exist_ok=True)
    os.makedirs(settings.DEPLOY_BUNDLE_STORAGE_DIR, exist_ok=True)
    if not os.path.exists(settings.DEPLOY_BUNDLE_INDEX_FILE):
        with open(settings.DEPLOY_BUNDLE_INDEX_FILE, "w", encoding="utf-8") as handle:
            json.dump({"bundles": []}, handle, indent=2)


def _index_lock_path() -> str:
    settings = _settings()
    return f"{settings.DEPLOY_BUNDLE_INDEX_FILE}.lock"


@contextmanager
def _index_lock() -> Iterable[None]:
    _ensure_deploy_dirs()
    lock_path = _index_lock_path()
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _read_index_unlocked() -> dict:
    settings = _settings()
    try:
        with open(settings.DEPLOY_BUNDLE_INDEX_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict) and isinstance(data.get("bundles"), list):
            return data
    except FileNotFoundError:
        return {"bundles": []}
    except json.JSONDecodeError:
        return {"bundles": []}
    return {"bundles": []}


def _write_index_unlocked(data: dict) -> None:
    settings = _settings()
    os.makedirs(os.path.dirname(settings.DEPLOY_BUNDLE_INDEX_FILE), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(settings.DEPLOY_BUNDLE_INDEX_FILE),
        prefix="deploy-index.",
        suffix=".json",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, settings.DEPLOY_BUNDLE_INDEX_FILE)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def _normalize_rel_path(value: str) -> str:
    normalized = value.replace("\\", "/").strip().lstrip("/")
    if not normalized:
        raise HTTPException(status_code=400, detail="Invalid empty relative path")
    parts = [part for part in normalized.split("/") if part not in ("", ".")]
    if any(part == ".." for part in parts):
        raise HTTPException(status_code=400, detail=f"Invalid relative path: {value}")
    clean = "/".join(parts)
    if not clean:
        raise HTTPException(status_code=400, detail="Invalid empty relative path")
    return clean


def _sanitize_uploaded_relative_paths(files: list[UploadFile], relative_paths: list[str] | None) -> list[str]:
    if relative_paths and len(relative_paths) != len(files):
        raise HTTPException(
            status_code=400,
            detail="relative_paths length must match files length",
        )

    normalized_paths: list[str] = []
    for index, upload in enumerate(files):
        candidate = None
        if relative_paths and index < len(relative_paths):
            candidate = relative_paths[index]
        if not candidate:
            candidate = upload.filename or ""
        normalized_paths.append(_normalize_rel_path(candidate))

    if len(set(normalized_paths)) != len(normalized_paths):
        raise HTTPException(status_code=400, detail="Duplicate relative paths in upload")

    return normalized_paths


def _resolve_bundle_root(staging_dir: Path) -> Path:
    root_manifest = staging_dir / "artifact_manifest.json"
    if root_manifest.exists():
        return staging_dir

    candidates = []
    for child in staging_dir.iterdir():
        if child.is_dir() and (child / "artifact_manifest.json").exists():
            candidates.append(child)

    if len(candidates) == 1:
        return candidates[0]

    raise HTTPException(status_code=400, detail="Uploaded folder must contain artifact_manifest.json at bundle root")


def _hash_bundle(root: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    file_count = 0
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel = path.relative_to(root).as_posix()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        digest.update(b"\0")
        file_count += 1

    bundle_id = digest.hexdigest()[:16]
    return bundle_id, file_count


def _targets_raw() -> list[dict]:
    settings = _settings()
    raw = getattr(settings, "DEPLOY_INFERENCE_TARGETS", [])
    return raw if isinstance(raw, list) else []


def _target_from_entry(entry: dict) -> InferenceTarget:
    required = ("id", "name", "base_url", "container_name", "bundle_mount_path")
    missing = [key for key in required if not str(entry.get(key, "")).strip()]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Invalid deploy target config; missing fields: {', '.join(missing)}",
        )
    return InferenceTarget(
        id=str(entry["id"]).strip(),
        name=str(entry["name"]).strip(),
        base_url=str(entry["base_url"]).strip().rstrip("/"),
        container_name=str(entry["container_name"]).strip(),
        bundle_mount_path=str(entry["bundle_mount_path"]).rstrip("/").strip(),
    )


def list_inference_targets() -> list[dict]:
    targets = [_target_from_entry(item) for item in _targets_raw()]
    return [
        {
            "id": target.id,
            "name": target.name,
            "base_url": target.base_url,
            "container_name": target.container_name,
            "bundle_mount_path": target.bundle_mount_path,
        }
        for target in targets
    ]


def _get_target(target_id: str) -> InferenceTarget:
    for item in _targets_raw():
        target = _target_from_entry(item)
        if target.id == target_id:
            return target
    raise HTTPException(status_code=404, detail=f"Inference target '{target_id}' not found")


def _bundle_index() -> list[dict]:
    _ensure_deploy_dirs()
    with _index_lock():
        return list(_read_index_unlocked().get("bundles", []))


def list_bundles() -> list[dict]:
    bundles = _bundle_index()
    bundles.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return bundles


def _bundle_record(bundle_id: str) -> dict:
    for item in _bundle_index():
        if item.get("bundle_id") == bundle_id:
            return item
    raise HTTPException(status_code=404, detail=f"Bundle '{bundle_id}' not found")


def upload_bundle_folder(files: list[UploadFile], relative_paths: list[str] | None = None) -> dict:
    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")

    _ensure_deploy_dirs()
    settings = _settings()

    normalized_paths = _sanitize_uploaded_relative_paths(files, relative_paths)

    with tempfile.TemporaryDirectory(prefix="deploy-upload-", dir=settings.DEPLOY_BUNDLES_DIR) as staging:
        staging_path = Path(staging)

        for upload, rel in zip(files, normalized_paths):
            destination = staging_path / rel
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("wb") as out:
                while True:
                    chunk = upload.file.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)

        bundle_root = _resolve_bundle_root(staging_path)
        bundle_id, file_count = _hash_bundle(bundle_root)

        final_dir = Path(settings.DEPLOY_BUNDLE_STORAGE_DIR) / bundle_id
        manifest_path = final_dir / "artifact_manifest.json"

        created = False
        with _index_lock():
            payload = _read_index_unlocked()
            existing = next((item for item in payload.get("bundles", []) if item.get("bundle_id") == bundle_id), None)
            if existing is None:
                if final_dir.exists():
                    shutil.rmtree(final_dir)
                shutil.copytree(bundle_root, final_dir)
                now = _utc_now_iso()
                existing = {
                    "bundle_id": bundle_id,
                    "name": bundle_root.name,
                    "file_count": file_count,
                    "artifacts_dir_host": str(final_dir),
                    "manifest_path_host": str(manifest_path),
                    "created_at": now,
                    "updated_at": now,
                }
                payload.setdefault("bundles", []).append(existing)
                _write_index_unlocked(payload)
                created = True

        return {
            "created": created,
            "bundle": existing,
        }


def _expected_container_manifest_path(target: InferenceTarget, bundle_id: str) -> str:
    mount = target.bundle_mount_path.rstrip("/")
    return f"{mount}/{bundle_id}/artifact_manifest.json"


def _probe_target_health(target: InferenceTarget) -> dict:
    url = f"{target.base_url}/health"
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.get(url)
        response.raise_for_status()
        parsed = response.json()
        data = parsed if isinstance(parsed, dict) else {}
        active_manifest = data.get("manifest_path")
        return {
            "id": target.id,
            "name": target.name,
            "base_url": target.base_url,
            "container_name": target.container_name,
            "bundle_mount_path": target.bundle_mount_path,
            "reachable": True,
            "configured": bool(data.get("configured", False)),
            "healthy": str(data.get("status", "")).lower() == "ok",
            "active_manifest_path": active_manifest,
            "raw": data,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "id": target.id,
            "name": target.name,
            "base_url": target.base_url,
            "container_name": target.container_name,
            "bundle_mount_path": target.bundle_mount_path,
            "reachable": False,
            "configured": False,
            "healthy": False,
            "active_manifest_path": None,
            "error": str(exc),
            "raw": None,
        }


def get_inference_health(target_id: str) -> dict:
    target = _get_target(target_id)
    return _probe_target_health(target)


def switch_inference_bundle(target_id: str, bundle_id: str) -> dict:
    target = _get_target(target_id)
    _bundle_record(bundle_id)

    artifacts_dir = f"{target.bundle_mount_path.rstrip('/')}/{bundle_id}"
    manifest_path = f"{artifacts_dir}/artifact_manifest.json"

    payload = {
        "manifest_path": manifest_path,
        "artifacts_dir": artifacts_dir,
    }

    try:
        with httpx.Client(timeout=15.0) as client:
            response = client.post(f"{target.base_url}/admin/load", json=payload)
        response.raise_for_status()
        load_result = response.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text or str(exc)
        raise HTTPException(status_code=502, detail=f"Inference load failed: {detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Inference load failed: {exc}") from exc

    health = _probe_target_health(target)

    return {
        "status": "switched",
        "target_id": target.id,
        "bundle_id": bundle_id,
        "requested_manifest_path": manifest_path,
        "load_response": load_result,
        "health": health,
    }


def delete_bundle(bundle_id: str) -> dict:
    record = _bundle_record(bundle_id)

    for entry in _targets_raw():
        target = _target_from_entry(entry)
        health = _probe_target_health(target)
        if not health.get("reachable"):
            continue
        if not health.get("configured"):
            continue
        expected_manifest = _expected_container_manifest_path(target, bundle_id)
        if str(health.get("active_manifest_path") or "") == expected_manifest:
            raise HTTPException(
                status_code=409,
                detail=f"Bundle '{bundle_id}' is active on inference target '{target.id}'",
            )

    artifacts_dir = Path(str(record.get("artifacts_dir_host", "")))

    with _index_lock():
        payload = _read_index_unlocked()
        bundles = payload.get("bundles", [])
        payload["bundles"] = [item for item in bundles if item.get("bundle_id") != bundle_id]
        _write_index_unlocked(payload)

    if artifacts_dir.exists() and artifacts_dir.is_dir():
        shutil.rmtree(artifacts_dir)

    return {"status": "deleted", "bundle_id": bundle_id}


def stream_inference_logs(target_id: str, tail: int = 200) -> Generator[str, None, None]:
    target = _get_target(target_id)
    safe_tail = max(0, int(tail))

    process = subprocess.Popen(
        [
            "docker",
            "logs",
            "--follow",
            "--tail",
            str(safe_tail),
            target.container_name,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    try:
        if process.stdout is None:
            return
        for line in process.stdout:
            yield line
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
