import os

VM_SHARED_DATA = "/opt/opeva_shared_data"

CONFIGS_DIR = os.path.join(VM_SHARED_DATA, "configs")
JOB_TRACK_FILE = os.path.join(VM_SHARED_DATA, "job_track.json")
JOBS_DIR = os.path.join(VM_SHARED_DATA, "jobs")
DATASETS_DIR = os.path.join(VM_SHARED_DATA, "datasets")

AVAILABLE_HOSTS = [
    {"name": "local", "host": "local"},
    {"name": "gpu-server-1", "host": "192.168.1.100"},
    {"name": "gpu-server-2", "host": "192.168.1.101"}
]