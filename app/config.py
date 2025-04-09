import os

# VM shared storage path (cleanly separated!)
VM_SHARED_DATA = "/opt/opeva_shared_data"

# Mounted path inside Docker container
CONTAINER_SHARED_DATA = "/data"

# Paths inside container (pointing to VM shared storage)
RESULTS_DIR = os.path.join(CONTAINER_SHARED_DATA, "results")
PROGRESS_DIR = os.path.join(CONTAINER_SHARED_DATA, "progress")
CONFIGS_DIR = os.path.join(CONTAINER_SHARED_DATA, "configs")
JOB_TRACK_FILE = os.path.join(CONTAINER_SHARED_DATA, "job_track.json")
