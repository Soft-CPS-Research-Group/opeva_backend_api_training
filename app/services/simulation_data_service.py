from __future__ import annotations

import csv
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Iterable

from fastapi import HTTPException

from app.config import settings


def _job_simulation_root(job_id: str) -> Path:
    clean_id = str(job_id).strip().strip("/\\")
    if not clean_id or ".." in clean_id or "/" in clean_id or "\\" in clean_id:
        raise HTTPException(status_code=400, detail="Invalid job_id")
    return Path(settings.JOBS_DIR) / clean_id / "results" / "simulation_data"


def _session_candidates(root: Path) -> list[Path]:
    if not root.exists():
        return []
    if not root.is_dir():
        return []
    children = [item for item in root.iterdir() if item.is_dir()]
    if children:
        return sorted(children, key=lambda item: item.stat().st_mtime)
    return [root]


def _select_session(root: Path, requested_session: str | None) -> tuple[str, Path]:
    candidates = _session_candidates(root)
    if not candidates:
        raise HTTPException(status_code=404, detail="No simulation data found for this job")

    if not requested_session or requested_session == "latest":
        chosen = candidates[-1]
        if chosen == root:
            return "root", chosen
        return chosen.name, chosen

    if requested_session == "root":
        if root in candidates:
            return "root", root
        raise HTTPException(status_code=404, detail="Root simulation session not found")

    for candidate in candidates:
        if candidate.name == requested_session:
            return requested_session, candidate

    raise HTTPException(status_code=404, detail=f"Simulation session '{requested_session}' not found")


def _iter_csv_files(session_path: Path) -> list[Path]:
    files = [
        path
        for path in session_path.rglob("*.csv")
        if path.is_file()
    ]
    return sorted(files, key=lambda item: str(item.relative_to(session_path)).lower())


def _to_epoch_seconds(raw: str) -> datetime | None:
    value = raw.strip()
    if not value:
        return None
    try:
        numeric = float(value)
    except ValueError:
        numeric = None

    if numeric is not None:
        if numeric > 9999999999:
            numeric = numeric / 1000.0
        if numeric > 0:
            try:
                return datetime.fromtimestamp(numeric, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
    return None


def _to_datetime(raw: str) -> datetime | None:
    parsed_epoch = _to_epoch_seconds(raw)
    if parsed_epoch is not None:
        return parsed_epoch

    value = raw.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_available_days(csv_path: Path, days: set[str]) -> None:
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                return
            ts_index = 0
            for index, name in enumerate(header):
                if "timestamp" in str(name).lower() or "time" in str(name).lower() or "date" in str(name).lower():
                    ts_index = index
                    break
            for row in reader:
                if ts_index >= len(row):
                    continue
                dt = _to_datetime(row[ts_index])
                if dt is None:
                    continue
                days.add(dt.date().isoformat())
    except OSError:
        return


def _normalise_relative_path(value: str) -> Path:
    normalized = Path(value.replace("\\", "/"))
    if normalized.is_absolute():
        raise HTTPException(status_code=400, detail="relative_path must be relative")
    if ".." in normalized.parts:
        raise HTTPException(status_code=400, detail="Invalid relative_path")
    if normalized.suffix.lower() != ".csv":
        raise HTTPException(status_code=400, detail="Only .csv files are supported")
    return normalized


def index_simulation_data(job_id: str, session: str | None = "latest") -> dict:
    root = _job_simulation_root(job_id)
    selected_session, session_path = _select_session(root, session)
    csv_files = _iter_csv_files(session_path)

    available_days: set[str] = set()
    for csv_file in csv_files:
        _extract_available_days(csv_file, available_days)

    return {
        "root_path": str(session_path),
        "session": selected_session,
        "files": [str(path.relative_to(session_path)).replace("\\", "/") for path in csv_files],
        "available_days": sorted(available_days),
    }


def read_simulation_data_file(job_id: str, relative_path: str, session: str | None = "latest") -> str:
    rel = _normalise_relative_path(relative_path)
    root = _job_simulation_root(job_id)
    _, session_path = _select_session(root, session)
    target = (session_path / rel).resolve()
    session_resolved = session_path.resolve()
    if not str(target).startswith(str(session_resolved) + os.sep) and target != session_resolved:
        raise HTTPException(status_code=400, detail="Invalid relative_path")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Simulation data file not found")

    try:
        return target.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {exc}")
