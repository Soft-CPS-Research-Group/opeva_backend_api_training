from fastapi import FastAPI
from app.api.router import api_router
from app.utils.job_utils import ensure_directories

app = FastAPI()
ensure_directories()
app.include_router(api_router)


# from fastapi import FastAPI, HTTPException, Body
# from fastapi.responses import StreamingResponse
# from app.models import JobLaunchRequest, SimulationRequest
# from app.utils import get_available_hosts
# from app.config import CONFIGS_DIR
# from app.utils import delete_job_by_id, delete_config_by_name, ensure_directories, save_job, save_job_info, load_jobs, save_config_dict, collect_results, read_progress, list_config_files, load_config_file, create_dataset_dir,list_available_datasets
# from app.docker_manager import run_simulation, get_container_status, stop_container, stream_container_logs
# import os
# import json
# import yaml
# import re
# import traceback
# from uuid import uuid4
# from utils import get_db

# app = FastAPI()
# jobs = load_jobs()
# ensure_directories()

# @app.post("/run-simulation")
# async def run_simulation_from_ui(request: JobLaunchRequest):
#     try:
#         job_id = str(uuid4())
#         if request.config_path:
#             config_path = request.config_path
#             with open(os.path.join(CONFIGS_DIR, config_path)) as f:
#                 config = yaml.safe_load(f)
#         elif request.config:
#             file_name = request.save_as or f"{job_id}.yaml"
#             config_path = save_config_dict(request.config, file_name)
#             config = request.config
#         else:
#             raise HTTPException(status_code=400, detail="Missing config or config_path")

#         experiment_name = config.get("experiment", {}).get("name", "UnnamedExperiment")
#         run_name = config.get("experiment", {}).get("run_name", "UnnamedRun")
#         job_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', f"{experiment_name}-{run_name}")

#         if not config_path.startswith("configs/"):
#             config_path = f"configs/{config_path}"

#         sim_request = SimulationRequest(
#             config_path=config_path,
#             job_name=job_name
#         )

#         container = run_simulation(job_id, sim_request, request.target_host)

#         job_metadata = {
#             "container_id": container.id,
#             "container_name": container.name,
#             "job_name": job_name,
#             "config_path": config_path,
#             "target_host": request.target_host,
#             "experiment_name": experiment_name,
#             "run_name": run_name,
#         }

#         save_job(job_id, job_metadata)
#         save_job_info(
#             job_id=job_id,
#             job_name=job_name,
#             config_path=config_path,
#             target_host=request.target_host,
#             container_id=container.id,
#             container_name=container.name,
#             experiment_name=experiment_name,
#             run_name=run_name
#         )

#         jobs[job_id] = job_metadata

#         return {
#             "job_id": job_id,
#             "container_id": container.id,
#             "status": "launched",
#             "host": request.target_host,
#             "job_name": job_name,
#         }
#     except Exception as e:
#         print("\n\n--- Exception Traceback ---")
#         print(traceback.format_exc())
#         print("---------------------------\n")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/status/{job_id}")
# async def check_status(job_id: str):
#     job = jobs.get(job_id)
#     if not job:
#         raise HTTPException(status_code=404, detail="Job not found")
#     container_id = job.get("container_id")
#     return {"job_id": job_id, "status": get_container_status(container_id)}

# @app.get("/result/{job_id}")
# async def get_result(job_id: str):
#     return collect_results(job_id)

# @app.get("/progress/{job_id}")
# async def get_progress(job_id: str):
#     return read_progress(job_id)

# @app.get("/logs/{job_id}")
# async def get_logs(job_id: str):
#     job = jobs.get(job_id)
#     if not job:
#         raise HTTPException(status_code=404, detail="Job not found")
#     container_id = job.get("container_id")
#     return StreamingResponse(stream_container_logs(container_id), media_type="text/plain")

# @app.post("/stop/{job_id}")
# async def stop_job(job_id: str):
#     job = jobs.get(job_id)
#     if not job:
#         raise HTTPException(status_code=404, detail="Job not found")
#     container_id = job.get("container_id")
#     return {"message": stop_container(container_id)}

# @app.get("/jobs")
# async def list_jobs():
#     result = []
#     for job_id, job in jobs.items():
#         job_info_path = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "job_info.json")
#         job_info = {}
#         if os.path.exists(job_info_path):
#             with open(job_info_path) as f:
#                 job_info = json.load(f)
#         result.append({
#             "job_id": job_id,
#             "status": get_container_status(job.get("container_id")),
#             "job_info": job_info
#         })
#     return result

# @app.get("/job-info/{job_id}")
# async def get_job_info(job_id: str):
#     job_info_path = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "job_info.json")
#     if not os.path.exists(job_info_path):
#         raise HTTPException(status_code=404, detail="Job info not found")
#     with open(job_info_path) as f:
#         return json.load(f)

# @app.get("/health")
# async def health():
#     return {"status": "ok"}

# @app.post("/config")
# async def create_config_file(config: dict = Body(...), file_name: str = Body(...)):
#     try:
#         save_config_dict(config, file_name)
#         return {"message": "Config saved", "file": file_name}
#     except FileExistsError as e:
#         raise HTTPException(status_code=400, detail=str(e))

# @app.get("/configs")
# async def get_configs():
#     return list_config_files()

# @app.get("/config/{file_name}")
# async def get_config_by_name(file_name: str):
#     try:
#         return {"config": load_config_file(file_name)}
#     except FileNotFoundError as e:
#         raise HTTPException(status_code=404, detail=str(e))

# @app.post("/dataset")
# async def create_dataset(name: str = Body(...), schema: dict = Body(...), data_files: dict = Body(default={})):
#     create_dataset_dir(name, schema, data_files)
#     return {"message": "Dataset created", "name": name}

# @app.get("/datasets")
# async def get_datasets():
#     return list_available_datasets()

# @app.delete("/job/{job_id}")
# async def delete_job(job_id: str):
#     success = delete_job_by_id(job_id, jobs)
#     if not success:
#         raise HTTPException(status_code=404, detail="Job not found or already deleted")
#     return {"message": f"Job {job_id} deleted successfully"}

# @app.delete("/config/{file_name}")
# async def delete_config(file_name: str):
#     success = delete_config_by_name(file_name)
#     if not success:
#         raise HTTPException(status_code=404, detail="Config file not found")
#     return {"message": f"Config {file_name} deleted successfully"}

# @app.get("/file-logs/{job_id}")
# async def get_file_logs(job_id: str):
#     log_dir = os.path.join("/opt/opeva_shared_data", "jobs", job_id, "logs")

#     if not os.path.exists(log_dir):
#         raise HTTPException(status_code=404, detail="Log folder not found for this job")

#     # Try to locate the .log file (we assume only one per job)
#     for filename in os.listdir(log_dir):
#         if filename.endswith(".log"):
#             log_path = os.path.join(log_dir, filename)
#             def iter_logs():
#                 with open(log_path) as f:
#                     for line in f:
#                         yield line
#             return StreamingResponse(iter_logs(), media_type="text/plain")

#     raise HTTPException(status_code=404, detail="Log file not found in logs folder")

# @app.get("/hosts")
# def list_hosts():
#     return {"available_hosts": get_available_hosts()}

# app = FastAPI()

# @app.get("/api/icharging-headquarters")
# async def icharging_headquarters():
#     try:
#         db = get_db('i-charging_headquarters')
#         collection_name = 'i-charging headquarters'
#         docs = list(db[collection_name].find({}))
#         return {collection_name: docs}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to retrieve iCharging data: {str(e)}")

# @app.get("/api/living-lab")
# async def living_lab():
#     try:
#         db = get_db('living_lab')
#         collection_names = db.list_collection_names()
#         result = {col: list(db[col].find({})) for col in collection_names}
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to retrieve Living Lab data: {str(e)}")