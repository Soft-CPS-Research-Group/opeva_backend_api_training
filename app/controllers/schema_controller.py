from fastapi import HTTPException
from app.services import schema_service

def create_schema_controller(site: str, schema: dict):
    try:
        schema_service.create_schema(site, schema)
        return {"message": f"Schema created for site '{site}'."}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def update_schema_controller(site: str, schema: dict):
    try:
        schema_service.update_schema(site, schema)
        return {"message": f"Schema updated for site '{site}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_schema_controller(site: str):
    try:
        schema = schema_service.get_schema(site)
        if schema is None:
            raise HTTPException(status_code=404, detail="Schema not found.")
        return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
