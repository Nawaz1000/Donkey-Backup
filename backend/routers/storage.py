from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json

router = APIRouter()

class StorageCreate(BaseModel):
    name: str
    type: Literal["azure", "gcs"]
    # Azure fields
    azure_account_name: Optional[str] = None
    azure_account_key: Optional[str] = None
    azure_container: Optional[str] = None
    # GCS fields
    gcs_bucket: Optional[str] = None
    gcs_credentials_json: Optional[str] = None

@router.get("/")
def list_storages(user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    result = []
    for s in storages:
        if s["user_id"] == user["sub"]:
            safe = {k: v for k, v in s.items() if k not in ["azure_account_key", "gcs_credentials_json"]}
            result.append(safe)
    return result

@router.post("/")
def add_storage(req: StorageCreate, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    storage = {
        "id": str(uuid.uuid4()),
        "user_id": user["sub"],
        "name": req.name,
        "type": req.type,
        "azure_account_name": req.azure_account_name,
        "azure_account_key": req.azure_account_key,
        "azure_container": req.azure_container,
        "gcs_bucket": req.gcs_bucket,
        "gcs_credentials_json": req.gcs_credentials_json,
        "created_at": datetime.utcnow().isoformat(),
    }
    storages.append(storage)
    write_json("data/storages.json", storages)
    safe = {k: v for k, v in storage.items() if k not in ["azure_account_key", "gcs_credentials_json"]}
    return safe

@router.delete("/{storage_id}")
def delete_storage(storage_id: str, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    storages = [s for s in storages if not (s["id"] == storage_id and s["user_id"] == user["sub"])]
    write_json("data/storages.json", storages)
    return {"message": "Deleted"}
