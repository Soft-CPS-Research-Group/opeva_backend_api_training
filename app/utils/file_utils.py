import os, json, yaml, base64, re, zipfile
import tempfile
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import OrderedDict
import shutil
import logging
import math

import numpy as np
import pandas as pd
from fastapi import HTTPException

from app.config import settings
from app.utils import mongo_utils
from app.utils import citylearn_dataset

def _safe_config_filename(file_name: str) -> str:
    normalized = os.path.normpath(file_name).strip().lstrip("/\\")
    if normalized.startswith("..") or os.path.isabs(file_name) or os.sep in normalized:
        raise ValueError("Invalid config file name")
    return normalized

def save_config_dict(config: dict, file_name: str) -> str:
    safe_file_name = _safe_config_filename(file_name)
    full_path = os.path.join(settings.CONFIGS_DIR, safe_file_name)
    with open(full_path, "w") as f:
        yaml.dump(config, f)
    return safe_file_name


def save_config_yaml_content(yaml_content: str, file_name: str) -> str:
    safe_file_name = _safe_config_filename(file_name)
    yaml.safe_load(yaml_content or "")
    full_path = os.path.join(settings.CONFIGS_DIR, safe_file_name)
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(yaml_content)
    return safe_file_name

def list_config_files():
    return [f for f in os.listdir(settings.CONFIGS_DIR) if f.endswith(('.yaml', '.yml'))]

def load_config_file(file_name):
    safe_file_name = _safe_config_filename(file_name)
    path = os.path.join(settings.CONFIGS_DIR, safe_file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config {safe_file_name} not found")
    with open(path) as f:
        return yaml.safe_load(f)


def load_config_file_text(file_name: str) -> str:
    safe_file_name = _safe_config_filename(file_name)
    path = os.path.join(settings.CONFIGS_DIR, safe_file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config {safe_file_name} not found")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def delete_config_by_name(file_name):
    safe_file_name = _safe_config_filename(file_name)
    path = os.path.join(settings.CONFIGS_DIR, safe_file_name)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


def collect_results(job_id):
    path = os.path.join(settings.JOBS_DIR, job_id, "results", "result.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"status": "pending", "message": "Result not ready yet."}

def read_progress(job_id):
    path = os.path.join(settings.JOBS_DIR, job_id, "progress", "progress.json")
    if os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
        return data
    return {"progress": "No updates yet."}

# Utility function to convert timestamp strings to datetime objects
def parse_timestamp(ts):
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc)

    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts).astimezone(timezone.utc)
        except ValueError:
            try:
                return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    raise ValueError(f"Invalid timestamp string format: {ts}")

    raise TypeError(f"Unsupported timestamp type: {type(ts)}")

def create_dataset_dir(
    name: str,
    site_id: str,
    config: dict,
    description: str = "",
    period: int = 60,
    from_ts: str = None,
    until_ts: str = None,
):
    return citylearn_dataset.generate_citylearn_dataset(
        name=name,
        site_id=site_id,
        citylearn_configs=config,
        description=description,
        period=period,
        from_ts=from_ts,
        until_ts=until_ts,
    )


def list_dates_available_per_collection(site_id: str):
    db = mongo_utils.get_db(site_id)

    # List all collections in the database
    collections = db.list_collection_names()

    results = []

    # Iterate over all collections in the database
    for collection_name in collections:
        if collection_name == "schema":
            continue

        collection = db[collection_name]

        # Find the oldest and newest documents based on 'timestamp'
        doc_oldest = collection.find_one(sort=[('_id', 1)])
        doc_newest = collection.find_one(sort=[('_id', -1)])

        # Parse and normalize timestamps
        ts_oldest = parse_timestamp(doc_oldest["timestamp"])
        ts_newest = parse_timestamp(doc_newest["timestamp"])

        # Append the results for this collection
        results.append({
            "installation": collection_name,
            "oldest_record": ts_oldest.isoformat(),
            "newest_record": ts_newest.isoformat()
        })

    return results


def list_dataset_sites():
    return citylearn_dataset.list_citylearn_compatible_sites()

def _get_path_size(path: str) -> int:
    """Return total size in bytes for a file or directory."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def list_available_datasets():
    datasets = []
    if not os.path.exists(settings.DATASETS_DIR):
        return datasets

    for name in os.listdir(settings.DATASETS_DIR):
        path = os.path.join(settings.DATASETS_DIR, name)
        if not os.path.exists(path):
            continue

        description = ""
        schema_path = os.path.join(path, "schema.json")
        try:
            if os.path.isfile(schema_path):
                with open(schema_path) as f:
                    schema_data = json.load(f)
                    description = schema_data.get("description", "")
        except Exception:
            # Be resilient if a dataset folder is malformed
            description = ""

        datasets.append({"name": name, "description": description})
    return datasets


def _safe_dataset_name(name: str) -> str:
    candidate = os.path.basename(str(name or "").strip())
    if not candidate:
        raise ValueError("Dataset name is required")
    if len(candidate) > 128:
        raise ValueError("Dataset name is too long")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", candidate):
        raise ValueError("Dataset name contains invalid characters")
    return candidate


def _is_safe_zip_member(member_name: str) -> bool:
    normalized = os.path.normpath(member_name).replace("\\", "/")
    if normalized.startswith("../") or normalized.startswith("/"):
        return False
    if "/../" in normalized:
        return False
    return normalized not in {"", ".", ".."}


def upload_dataset_archive(file_obj, source_filename: str, dataset_name: str | None = None) -> dict:
    if not source_filename.lower().endswith(".zip"):
        raise ValueError("Only .zip dataset uploads are supported")

    inferred_name = os.path.splitext(os.path.basename(source_filename))[0]
    safe_name = _safe_dataset_name(dataset_name or inferred_name)
    os.makedirs(settings.DATASETS_DIR, exist_ok=True)
    target_path = os.path.join(settings.DATASETS_DIR, safe_name)
    if os.path.exists(target_path):
        raise FileExistsError(f"Dataset '{safe_name}' already exists")

    fd, tmp_zip_path = tempfile.mkstemp(prefix="dataset_upload_", suffix=".zip")
    os.close(fd)
    extract_root = tempfile.mkdtemp(prefix="dataset_extract_")
    try:
        with open(tmp_zip_path, "wb") as tmp_file:
            shutil.copyfileobj(file_obj, tmp_file)

        with zipfile.ZipFile(tmp_zip_path) as archive:
            members = archive.infolist()
            if not members:
                raise ValueError("Uploaded ZIP is empty")
            for member in members:
                if not _is_safe_zip_member(member.filename):
                    raise ValueError("ZIP contains unsafe file paths")
            archive.extractall(extract_root)

        extracted_entries = [
            os.path.join(extract_root, name)
            for name in os.listdir(extract_root)
        ]
        if not extracted_entries:
            raise ValueError("Uploaded ZIP does not contain files")

        # If the archive already contains a root dataset folder, preserve its contents.
        if len(extracted_entries) == 1 and os.path.isdir(extracted_entries[0]):
            shutil.move(extracted_entries[0], target_path)
        else:
            os.makedirs(target_path, exist_ok=True)
            for entry in extracted_entries:
                shutil.move(entry, os.path.join(target_path, os.path.basename(entry)))

        metadata = {
            "name": safe_name,
            "description": "Uploaded dataset",
            "uploaded_from": source_filename,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(os.path.join(target_path, "upload_metadata.json"), "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        return {
            "name": safe_name,
            "path": target_path,
            "size_bytes": _get_path_size(target_path),
        }
    except Exception:
        if os.path.exists(target_path):
            shutil.rmtree(target_path, ignore_errors=True)
        raise
    finally:
        try:
            if os.path.exists(tmp_zip_path):
                os.remove(tmp_zip_path)
        except OSError:
            pass
        shutil.rmtree(extract_root, ignore_errors=True)


def get_dataset_file(name: str) -> str:
    """Return a path to the dataset file. If the dataset is a directory it will be zipped."""
    path = os.path.join(settings.DATASETS_DIR, name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset {name} not found")

    if os.path.isdir(path):
        tmp_dir = tempfile.gettempdir()
        archive_base = os.path.join(tmp_dir, name)
        archive_path = shutil.make_archive(archive_base, 'zip', path)
        return archive_path
    return path


def delete_dataset_by_name(name: str) -> bool:
    path = os.path.join(settings.DATASETS_DIR, name)
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        return True
    return False
