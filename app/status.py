from enum import Enum

class JobStatus(str, Enum):
    # creation/dispatch
    LAUNCHING = "launching"      # created on server; preparing metadata
    QUEUED = "queued"            # written to /queue/<worker_id> but not yet taken
    DISPATCHED = "dispatched"    # agent popped from queue but not started yet

    # active
    RUNNING = "running"          # container started

    # terminal
    FINISHED = "finished"        # exit code 0
    FAILED = "failed"            # non-zero exit code
    STOPPED = "stopped"          # intentionally stopped/cancelled

    # utility
    NOT_FOUND = "not_found"      # container/file missing
    UNKNOWN = "unknown"          # fallback
    CANCELED = "canceled"        # (optional) queued/dispatched canceled before start