from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import base64
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Iterable

import fcntl
import docker
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


def _sanitize_storage_dir_name(raw_name: str, *, fallback: str) -> str:
    base = (raw_name or "").replace("\\", "/").strip().split("/")[-1]
    candidate = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._-")
    return candidate or fallback


def _bundle_storage_dir_name(record: dict) -> str:
    configured = str(record.get("storage_dir_name") or "").strip()
    if configured:
        return configured

    artifacts_dir = str(record.get("artifacts_dir_host") or "").strip()
    if artifacts_dir:
        resolved = Path(artifacts_dir).expanduser()
        if resolved.name:
            return resolved.name

    return str(record.get("bundle_id") or "").strip()


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


def _bundle_artifacts_dir(bundle_id: str) -> tuple[dict, Path]:
    record = _bundle_record(bundle_id)
    root = Path(str(record.get("artifacts_dir_host", ""))).expanduser()
    if not root.exists() or not root.is_dir():
        raise HTTPException(status_code=404, detail=f"Bundle '{bundle_id}' artifacts directory is missing")
    return record, root


def _resolve_bundle_file_path(bundle_root: Path, rel_path: str) -> tuple[str, Path]:
    normalized = _normalize_rel_path(rel_path)
    root_resolved = bundle_root.resolve()
    candidate = (root_resolved / normalized).resolve()
    if root_resolved != candidate and root_resolved not in candidate.parents:
        raise HTTPException(status_code=400, detail="Bundle file path escapes artifacts directory")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"Bundle file '{normalized}' not found")
    return normalized, candidate


def list_bundle_files(bundle_id: str) -> dict:
    record, root = _bundle_artifacts_dir(bundle_id)
    files: list[dict] = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel = path.relative_to(root).as_posix()
        files.append(
            {
                "path": rel,
                "size_bytes": path.stat().st_size,
            }
        )

    return {
        "bundle_id": bundle_id,
        "bundle_name": record.get("name") or bundle_id,
        "file_count": len(files),
        "files": files,
    }


def read_bundle_file_content(bundle_id: str, rel_path: str, max_bytes: int = 200_000) -> dict:
    _, root = _bundle_artifacts_dir(bundle_id)
    normalized, path = _resolve_bundle_file_path(root, rel_path)
    size_bytes = path.stat().st_size

    with open(path, "rb") as handle:
        content_bytes = handle.read(max_bytes + 1)
    truncated = len(content_bytes) > max_bytes
    if truncated:
        content_bytes = content_bytes[:max_bytes]

    try:
        content = content_bytes.decode("utf-8")
        is_text = True
    except UnicodeDecodeError:
        content = None
        is_text = False

    return {
        "bundle_id": bundle_id,
        "path": normalized,
        "is_text": is_text,
        "size_bytes": size_bytes,
        "truncated": truncated,
        "content": content,
    }


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
        storage_dir_name = _sanitize_storage_dir_name(bundle_root.name, fallback=f"bundle_{bundle_id[:8]}")

        final_dir = Path(settings.DEPLOY_BUNDLE_STORAGE_DIR) / storage_dir_name
        manifest_path = final_dir / "artifact_manifest.json"

        created = False
        with _index_lock():
            payload = _read_index_unlocked()
            bundles = payload.setdefault("bundles", [])
            existing_by_name = next(
                (item for item in bundles if _bundle_storage_dir_name(item) == storage_dir_name),
                None,
            )
            existing_by_id = next((item for item in bundles if item.get("bundle_id") == bundle_id), None)

            existing = existing_by_name or existing_by_id
            previous_storage_name = _bundle_storage_dir_name(existing) if existing else None

            # Remove duplicate entry if both lookup keys matched different records.
            if existing_by_name is not None and existing_by_id is not None and existing_by_name is not existing_by_id:
                duplicate = existing_by_id if existing is existing_by_name else existing_by_name
                duplicate_dir = Path(settings.DEPLOY_BUNDLE_STORAGE_DIR) / _bundle_storage_dir_name(duplicate)
                bundles[:] = [item for item in bundles if item is not duplicate]
                if duplicate_dir.exists() and duplicate_dir.is_dir() and duplicate_dir != final_dir:
                    shutil.rmtree(duplicate_dir)

            if previous_storage_name and previous_storage_name != storage_dir_name:
                previous_dir = Path(settings.DEPLOY_BUNDLE_STORAGE_DIR) / previous_storage_name
                if previous_dir.exists() and previous_dir.is_dir() and previous_dir != final_dir:
                    shutil.rmtree(previous_dir)

            if final_dir.exists():
                shutil.rmtree(final_dir)
            shutil.copytree(bundle_root, final_dir)

            now = _utc_now_iso()
            if existing is None:
                existing = {
                    "bundle_id": bundle_id,
                    "name": bundle_root.name,
                    "storage_dir_name": storage_dir_name,
                    "file_count": file_count,
                    "artifacts_dir_host": str(final_dir),
                    "manifest_path_host": str(manifest_path),
                    "created_at": now,
                    "updated_at": now,
                }
                bundles.append(existing)
                created = True
            else:
                existing["bundle_id"] = bundle_id
                existing["name"] = bundle_root.name
                existing["storage_dir_name"] = storage_dir_name
                existing["file_count"] = file_count
                existing["artifacts_dir_host"] = str(final_dir)
                existing["manifest_path_host"] = str(manifest_path)
                existing["updated_at"] = now
                existing.setdefault("created_at", now)

            _write_index_unlocked(payload)

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
    record = _bundle_record(bundle_id)
    storage_dir_name = _bundle_storage_dir_name(record)

    artifacts_dir = f"{target.bundle_mount_path.rstrip('/')}/{storage_dir_name}"
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
    storage_dir_name = _bundle_storage_dir_name(record)

    for entry in _targets_raw():
        target = _target_from_entry(entry)
        health = _probe_target_health(target)
        if not health.get("reachable"):
            continue
        if not health.get("configured"):
            continue
        expected_manifest = _expected_container_manifest_path(target, storage_dir_name)
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
    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    try:
        container = client.containers.get(target.container_name)
    except docker.errors.NotFound as exc:
        client.close()
        raise HTTPException(
            status_code=404,
            detail=f"Inference container '{target.container_name}' not found",
        ) from exc
    except docker.errors.DockerException as exc:
        client.close()
        raise HTTPException(
            status_code=502,
            detail=f"Could not access Docker daemon for logs: {exc}",
        ) from exc

    try:
        stream = container.logs(
            stream=True,
            follow=True,
            tail=safe_tail,
            stdout=True,
            stderr=True,
        )
    except docker.errors.DockerException as exc:
        client.close()
        raise HTTPException(
            status_code=502,
            detail=f"Could not open log stream for '{target.container_name}': {exc}",
        ) from exc

    try:
        for chunk in stream:
            if isinstance(chunk, bytes):
                yield chunk.decode("utf-8", errors="replace")
            else:
                yield str(chunk)
    except docker.errors.DockerException as exc:
        yield f"\n[deploy] log stream interrupted: {exc}\n"
    finally:
        client.close()


_DOCKER_LOG_TIMESTAMP_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<tz>Z|[+-]\d{2}:\d{2})$"
)


def _format_utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc_query_timestamp(raw: str, field_name: str) -> datetime:
    value = str(raw or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail=f"Missing required query param '{field_name}'")

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timestamp '{field_name}'. Expected ISO-8601 UTC.",
        ) from exc

    if parsed.tzinfo is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timestamp '{field_name}'. Timezone is required (UTC).",
        )

    offset = parsed.utcoffset()
    if offset != timedelta(0):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timestamp '{field_name}'. Must be UTC (suffix Z or +00:00).",
        )

    return parsed.astimezone(timezone.utc)


def _parse_docker_log_timestamp(raw: str) -> datetime | None:
    candidate = str(raw or "").strip()
    if not candidate:
        return None

    match = _DOCKER_LOG_TIMESTAMP_RE.match(candidate)
    if not match:
        return None

    fraction = (match.group("fraction") or "")[:6].ljust(6, "0")
    tz_raw = match.group("tz")
    tz = "+00:00" if tz_raw == "Z" else tz_raw
    payload = f"{match.group('date')}T{match.group('time')}"
    if fraction:
        payload = f"{payload}.{fraction}"
    payload = f"{payload}{tz}"

    try:
        parsed = datetime.fromisoformat(payload)
    except ValueError:
        return None

    return parsed.astimezone(timezone.utc)


def _encode_logs_history_cursor(offset: int) -> str:
    payload = json.dumps({"offset": max(0, int(offset))}, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def _decode_logs_history_cursor(cursor: str) -> int:
    value = str(cursor or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="Invalid cursor")

    try:
        raw = base64.urlsafe_b64decode(value.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc

    offset = data.get("offset")
    if not isinstance(offset, int):
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return max(0, offset)


def _history_response(
    *,
    target: InferenceTarget,
    since_dt: datetime,
    until_dt: datetime,
    lines: list[dict] | None = None,
    next_cursor: str | None = None,
    prev_cursor: str | None = None,
    has_more_before: bool = False,
    has_more_after: bool = False,
    available: bool = True,
    message: str | None = None,
) -> dict:
    return {
        "target_id": target.id,
        "since_ts": _format_utc_iso(since_dt),
        "until_ts": _format_utc_iso(until_dt),
        "lines": lines or [],
        "next_cursor": next_cursor,
        "prev_cursor": prev_cursor,
        "has_more_before": has_more_before,
        "has_more_after": has_more_after,
        "available": available,
        "message": message,
    }


def fetch_inference_logs_history_chunk(
    target_id: str,
    *,
    since_ts: str,
    until_ts: str | None = None,
    cursor: str | None = None,
    limit_lines: int = 500,
    search: str | None = None,
) -> dict:
    target = _get_target(target_id)
    since_dt = _parse_utc_query_timestamp(since_ts, "since_ts")
    until_dt = _parse_utc_query_timestamp(until_ts, "until_ts") if until_ts else datetime.now(timezone.utc)
    if since_dt > until_dt:
        raise HTTPException(status_code=400, detail="Invalid time window. since_ts must be <= until_ts")

    safe_limit = max(1, min(2000, int(limit_lines)))
    search_token = str(search or "").strip()
    search_folded = search_token.casefold()
    source = f"docker:{target.container_name}"

    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    try:
        try:
            container = client.containers.get(target.container_name)
        except docker.errors.NotFound:
            return _history_response(
                target=target,
                since_dt=since_dt,
                until_dt=until_dt,
                available=False,
                message=f"Container '{target.container_name}' not found.",
            )
        except docker.errors.DockerException as exc:
            return _history_response(
                target=target,
                since_dt=since_dt,
                until_dt=until_dt,
                available=False,
                message=f"Docker daemon unavailable: {exc}",
            )

        try:
            raw = container.logs(
                stream=False,
                follow=False,
                stdout=True,
                stderr=True,
                timestamps=True,
                since=since_dt,
                until=until_dt,
                tail="all",
            )
        except docker.errors.DockerException as exc:
            return _history_response(
                target=target,
                since_dt=since_dt,
                until_dt=until_dt,
                available=False,
                message=f"Could not read logs from Docker: {exc}",
            )
    finally:
        client.close()

    payload = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw or "")
    entries: list[dict] = []
    for raw_line in payload.splitlines():
        ts_value: str | None = None
        text = raw_line

        if raw_line:
            timestamp_candidate, separator, remainder = raw_line.partition(" ")
            parsed_ts = _parse_docker_log_timestamp(timestamp_candidate) if separator else None
            if parsed_ts is not None:
                ts_value = _format_utc_iso(parsed_ts)
                text = remainder

        if search_folded and search_folded not in text.casefold():
            continue

        entries.append({"ts": ts_value, "text": text, "source": source})

    if not entries:
        empty_message = (
            f"No log lines matched '{search_token}' in this window."
            if search_token
            else "No logs found in this window (they may be outside retention)."
        )
        return _history_response(
            target=target,
            since_dt=since_dt,
            until_dt=until_dt,
            available=True,
            message=empty_message,
        )

    total = len(entries)
    if cursor:
        start = _decode_logs_history_cursor(cursor)
    else:
        start = max(total - safe_limit, 0)

    if start >= total:
        start = max(total - safe_limit, 0)

    end = min(start + safe_limit, total)
    page = entries[start:end]

    has_more_before = start > 0
    has_more_after = end < total

    next_cursor = _encode_logs_history_cursor(max(start - safe_limit, 0)) if has_more_before else None
    prev_cursor = _encode_logs_history_cursor(end) if has_more_after else None

    return _history_response(
        target=target,
        since_dt=since_dt,
        until_dt=until_dt,
        lines=page,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        has_more_before=has_more_before,
        has_more_after=has_more_after,
        available=True,
        message=None,
    )
