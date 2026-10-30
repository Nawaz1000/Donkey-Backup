from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json

router = APIRouter()

SECRET_FIELDS = {"azure_account_key", "gcs_credentials_json", "s3_secret_key", "bunny_access_key"}

class StorageCreate(BaseModel):
    name: str
    type: Literal["azure", "gcs", "s3", "bunny"]
    # Azure fields
    azure_account_name: Optional[str] = None
    azure_account_key: Optional[str] = None
    azure_container: Optional[str] = None
    # GCS fields
    gcs_bucket: Optional[str] = None
    gcs_credentials_json: Optional[str] = None
    # S3 / S3-compatible fields
    s3_bucket: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    s3_region: Optional[str] = None
    s3_endpoint_url: Optional[str] = None
    # BunnyCDN Storage fields
    bunny_storage_zone: Optional[str] = None
    bunny_access_key: Optional[str] = None
    bunny_region: Optional[str] = None

def _mask_storage(s: dict) -> dict:
    return {k: ("********" if k in SECRET_FIELDS and v else v) for k, v in s.items()}

@router.get("/")
def list_storages(user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    result = []
    for s in storages:
        if s["user_id"] == user["sub"]:
            result.append(_mask_storage(s))
    return result

@router.post("/")
def add_storage(req: StorageCreate, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    storage = {
        "id": str(uuid.uuid4()),
        "user_id": user["sub"],
        "name": req.name,
        "type": req.type,
        # Azure
        "azure_account_name": req.azure_account_name,
        "azure_account_key": req.azure_account_key,
        "azure_container": req.azure_container,
        # GCS
        "gcs_bucket": req.gcs_bucket,
        "gcs_credentials_json": req.gcs_credentials_json,
        # S3
        "s3_bucket": req.s3_bucket,
        "s3_access_key": req.s3_access_key,
        "s3_secret_key": req.s3_secret_key,
        "s3_region": req.s3_region,
        "s3_endpoint_url": req.s3_endpoint_url,
        # BunnyCDN
        "bunny_storage_zone": req.bunny_storage_zone,
        "bunny_access_key": req.bunny_access_key,
        "bunny_region": req.bunny_region or "de",
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    storages.append(storage)
    write_json("data/storages.json", storages)
    return _mask_storage(storage)

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
            # Azure
            s["azure_account_name"] = req.azure_account_name
            if req.azure_account_key and req.azure_account_key != "********":
                s["azure_account_key"] = req.azure_account_key
            s["azure_container"] = req.azure_container
            # GCS
            s["gcs_bucket"] = req.gcs_bucket
            if req.gcs_credentials_json and req.gcs_credentials_json != "********":
                s["gcs_credentials_json"] = req.gcs_credentials_json
            # S3
            s["s3_bucket"] = req.s3_bucket
            s["s3_access_key"] = req.s3_access_key
            if req.s3_secret_key and req.s3_secret_key != "********":
                s["s3_secret_key"] = req.s3_secret_key
            s["s3_region"] = req.s3_region
            s["s3_endpoint_url"] = req.s3_endpoint_url
            # BunnyCDN
            s["bunny_storage_zone"] = req.bunny_storage_zone
            if req.bunny_access_key and req.bunny_access_key != "********":
                s["bunny_access_key"] = req.bunny_access_key
            s["bunny_region"] = req.bunny_region or "de"

            write_json("data/storages.json", storages)
            return _mask_storage(s)
    raise HTTPException(404, "Storage not found")

@router.post("/{storage_id}/test")
def test_storage_connection(storage_id: str, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    storage = next((s for s in storages if s["id"] == storage_id and s["user_id"] == user["sub"]), None)
    if not storage:
        raise HTTPException(404, "Storage not found")

    stype = storage.get("type")
    try:
        if stype == "azure":
            from azure.storage.blob import BlobServiceClient
            conn_str = f"DefaultEndpointsProtocol=https;AccountName={storage['azure_account_name']};AccountKey={storage['azure_account_key']};EndpointSuffix=core.windows.net"
            client = BlobServiceClient.from_connection_string(conn_str)
            container = client.get_container_client(storage["azure_container"])
            container.get_container_properties()
            return {"success": True, "message": f"Successfully connected to Azure container '{storage['azure_container']}'."}

        elif stype == "gcs":
            import json as _json
            from google.cloud import storage as gcs
            from google.oauth2 import service_account
            creds_dict = _json.loads(storage["gcs_credentials_json"])
            creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = gcs.Client(credentials=creds)
            bucket = client.bucket(storage["gcs_bucket"])
            bucket.exists()
            return {"success": True, "message": f"Successfully connected to GCS bucket '{storage['gcs_bucket']}'."}

        elif stype == "s3":
            from engine import get_s3_client
            client = get_s3_client(storage)
            client.head_bucket(Bucket=storage["s3_bucket"])
            return {"success": True, "message": f"Successfully connected to S3 bucket '{storage['s3_bucket']}'."}

        elif stype == "bunny":
            import requests
            from engine import get_bunny_host
            bunny_host = get_bunny_host(storage.get("bunny_region"))
            url = f"https://{bunny_host}/{storage['bunny_storage_zone']}/"
            headers = {"AccessKey": storage["bunny_access_key"], "Accept": "application/json"}
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                return {"success": True, "message": f"Successfully connected to BunnyCDN Storage Zone '{storage['bunny_storage_zone']}'."}
            elif resp.status_code == 401:
                raise HTTPException(400, "Invalid BunnyCDN Storage Password / Access Key (Unauthorized).")
            elif resp.status_code == 404:
                raise HTTPException(400, f"BunnyCDN Storage Zone '{storage['bunny_storage_zone']}' not found.")
            else:
                raise HTTPException(400, f"BunnyCDN Storage error ({resp.status_code}): {resp.text}")
        else:
            raise HTTPException(400, f"Unknown storage type: {stype}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Storage connection failed: {str(e)}")

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
                    lm = blob.updated or getattr(blob, 'time_created', None)
                    files.append({
                        "name": blob.name,
                        "size": blob.size,
                        "last_modified": lm.isoformat() if lm else None
                    })

        elif storage["type"] == "s3":
            from engine import get_s3_client
            client = get_s3_client(storage)
            paginator = client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=storage["s3_bucket"]):
                for item in page.get("Contents", []):
                    name = item["Key"]
                    if name.endswith(".gz") or name.endswith(".zst") or name.endswith(".archive") or "backup.archive" in name:
                        files.append({
                            "name": name,
                            "size": item["Size"],
                            "last_modified": item["LastModified"].isoformat() if item.get("LastModified") else None
                        })

        elif storage["type"] == "bunny":
            import requests
            from engine import get_bunny_host
            bunny_host = get_bunny_host(storage.get("bunny_region"))
            headers = {"AccessKey": storage["bunny_access_key"], "Accept": "application/json"}

            def list_bunny_dir(subpath=""):
                clean = subpath.strip("/")
                url = f"https://{bunny_host}/{storage['bunny_storage_zone']}/{clean}/" if clean else f"https://{bunny_host}/{storage['bunny_storage_zone']}/"
                r = requests.get(url, headers=headers, timeout=20)
                if r.status_code == 200:
                    for obj in r.json():
                        obj_name = obj.get("ObjectName", "")
                        full_name = f"{clean}/{obj_name}".strip("/") if clean else obj_name
                        if obj.get("IsDirectory"):
                            list_bunny_dir(full_name)
                        else:
                            if full_name.endswith(".gz") or full_name.endswith(".zst") or full_name.endswith(".archive") or "backup.archive" in full_name:
                                files.append({
                                    "name": full_name,
                                    "size": obj.get("Length", 0),
                                    "last_modified": obj.get("LastChanged")
                                })

            list_bunny_dir("")

    except Exception as e:
        raise HTTPException(500, f"Failed to fetch files: {str(e)}")
        
    return sorted(files, key=lambda x: x["last_modified"] or "", reverse=True)
