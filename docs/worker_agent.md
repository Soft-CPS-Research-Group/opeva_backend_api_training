# Worker Agent Integration Guide

This document summarises the contract between the backend API and any worker
agent implementation.

## Queue Semantics
- The API stores every launch request as a JSON file in the global
  `queue/` directory. Each file contains:
  - `job_id`
  - `preferred_host`: string or `null`
  - `require_host`: boolean (true if the requester targeted a specific host)
- Agents obtain work by POSTing to `/api/agent/next-job` with their
  `worker_id`. The server returns the first job whose host requirement is
  satisfied (matching host for required jobs, or any host for optional jobs).
- The response to `/api/agent/next-job` includes the fully populated payload
  (image, command, volumes, env, job name) derived from backend metadata, so
  agents do not need to read anything from the queue file beyond the job id.
- Once dispatched, the backend marks the job as `dispatched` and updates
  `target_host` in `job_info.json` and the job registry. Agents are expected to
  begin execution immediately; should they choose not to run the job they
  must call `/api/agent/job-status` with a terminal status so the queue can be
  resubmitted manually.

## Lifecycle Hooks
- **Start:** POST `/api/agent/job-status` with `status="running"` (include
  `worker_id`, `container_id`, `container_name`).
- **Completion:** POST `/api/agent/job-status` with
  `status="finished"` or `"failed"` plus `worker_id` and optional
  container metadata.
- **Cancellation:** If a job is cancelled while queued, the API removes the
  queue file and updates `status.json` to `canceled`. Running jobs should be
  stopped cooperatively by the agent when future support exists.

## Heartbeats
- Agents must send a heartbeat at least every
  `OPEVA_HEARTBEAT_INTERVAL` seconds (default 30) by POSTing to
  `/api/agent/heartbeat` with `{"worker_id": ..., "info": {...}}`. The
  backend records the timestamp and optional free-form info block.
- Hosts are reported as `online` if a heartbeat was received within
  `HOST_HEARTBEAT_TTL` seconds (default 60). Offline hosts remain visible so
  queued jobs can be inspected even if the worker is disconnected.

## Concurrency
- The backend does not enforce concurrency. Agents are responsible for deciding
  whether they have capacity to start a new job before claiming it.

## NFS Requirements
- Every worker must mount the shared storage (same path as the server,
  `/opt/opeva_shared_data` by default). Job payloads assume `config` is
  available under `/data/configs/...` when the container is started.

Implementations that follow this contract can live in a separate repository.
The `worker_agent.py` script in this codebase demonstrates the expected calls
and heartbeat logic.
