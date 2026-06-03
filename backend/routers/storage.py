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

@router.put("/{storage_id}")
def update_storage(storage_id: str, req: StorageCreate, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    for s in storages:
        if s["id"] == storage_id and s["user_id"] == user["sub"]:
            s["name"] = req.name
            s["type"] = req.type
            s["azure_account_name"] = req.azure_account_name
            if req.azure_account_key and req.azure_account_key != "********":
                s["azure_account_key"] = req.azure_account_key
            s["azure_container"] = req.azure_container
            s["gcs_bucket"] = req.gcs_bucket
            if req.gcs_credentials_json and req.gcs_credentials_json != "********":
                s["gcs_credentials_json"] = req.gcs_credentials_json
            write_json("data/storages.json", storages)
            safe = {k: v for k, v in s.items() if k not in ["azure_account_key", "gcs_credentials_json"]}
            return safe
    raise HTTPException(404, "Storage not found")

@router.get("/{storage_id}/files")
def list_storage_files(storage_id: str, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    storage = next((s for s in storages if s["id"] == storage_id and s["user_id"] == user["sub"]), None)
    if not storage:
        raise HTTPException(404, "Storage not found")
        
    files = []
    try:
        if storage["type"] == "azure":
            from azure.storage.blob import BlobServiceClient
            conn_str = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={storage['azure_account_name']};"
                f"AccountKey={storage['azure_account_key']};"
                f"EndpointSuffix=core.windows.net"
            )
            client = BlobServiceClient.from_connection_string(conn_str)
            container = client.get_container_client(storage["azure_container"])
            for blob in container.list_blobs():
                if blob.name.endswith(".gz") or blob.name.endswith(".zst") or blob.name.endswith(".archive") or "backup.archive" in blob.name:
                    # Robust fallback for Azure blob creation/modification time
                    lm = blob.last_modified or getattr(blob, 'creation_time', None)
                    files.append({
                        "name": blob.name,
                        "size": blob.size,
                        "last_modified": lm.isoformat() if lm else None
                    })
        elif storage["type"] == "gcs":
            import json
            from google.cloud import storage as gcs
            from google.oauth2 import service_account
            creds_dict = json.loads(storage["gcs_credentials_json"])
            creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = gcs.Client(credentials=creds)
            bucket = client.bucket(storage["gcs_bucket"])
            for blob in bucket.list_blobs():
                if blob.name.endswith(".gz") or blob.name.endswith(".zst") or blob.name.endswith(".archive") or "backup.archive" in blob.name:
                    # Robust fallback for GCS blob creation/modification time
                    lm = blob.updated or getattr(blob, 'time_created', None)
                    files.append({
                        "name": blob.name,
                        "size": blob.size,
                        "last_modified": lm.isoformat() if lm else None
                    })
    except Exception as e:
        raise HTTPException(500, f"Failed to fetch files: {str(e)}")
        
    return sorted(files, key=lambda x: x["last_modified"] or "", reverse=True)
