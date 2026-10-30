from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json

router = APIRouter()

SECRET_FIELDS = {"azure_account_key", "gcs_credentials_json", "s3_secret_key", "bunny_access_key", "bunny_password"}

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
    bunny_storage_zone: Optional[str] = None       # Access Key ID (name of zone)
    bunny_access_key: Optional[str] = None         # Access Key
    bunny_password: Optional[str] = None           # Password (Secret Key)
    bunny_region_url: Optional[str] = None         # Region URL (e.g. https://de-s3.storage.bunnycdn.com)
    bunny_region: Optional[str] = None             # Region code fallback
    folder_name: Optional[str] = None              # Optional base/default folder

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
    folder = (req.folder_name or "").strip().strip("/")
    while "//" in folder:
        folder = folder.replace("//", "/")
    storage = {
        "id": str(uuid.uuid4()),
        "user_id": user["sub"],
        "name": req.name,
        "type": req.type,
        "folder_name": folder,
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
        "bunny_access_key": req.bunny_access_key or req.bunny_password,
        "bunny_password": req.bunny_password or req.bunny_access_key,
        "bunny_region_url": req.bunny_region_url,
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
@router.patch("/{storage_id}")
def update_storage(storage_id: str, req: StorageCreate, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    for s in storages:
        if s["id"] == storage_id and s["user_id"] == user["sub"]:
            s["name"] = req.name
            s["type"] = req.type
            folder = (req.folder_name or "").strip().strip("/")
            while "//" in folder:
                folder = folder.replace("//", "/")
            s["folder_name"] = folder
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
            if req.bunny_password and req.bunny_password != "********":
                s["bunny_password"] = req.bunny_password
            if not s.get("bunny_access_key") and s.get("bunny_password"):
                s["bunny_access_key"] = s["bunny_password"]
            if not s.get("bunny_password") and s.get("bunny_access_key"):
                s["bunny_password"] = s["bunny_access_key"]
            s["bunny_region_url"] = req.bunny_region_url
            s["bunny_region"] = req.bunny_region or "de"

            write_json("data/storages.json", storages)
            return _mask_storage(s)
    raise HTTPException(404, "Storage not found")

def perform_test_storage(storage: dict) -> dict:
    stype = storage.get("type")
    try:
        if stype == "azure":
            from azure.storage.blob import BlobServiceClient
            conn_str = f"DefaultEndpointsProtocol=https;AccountName={storage.get('azure_account_name')};AccountKey={storage.get('azure_account_key')};EndpointSuffix=core.windows.net"
            client = BlobServiceClient.from_connection_string(conn_str)
            container = client.get_container_client(storage.get("azure_container"))
            container.get_container_properties()
            return {"success": True, "message": f"Successfully connected to Azure container '{storage.get('azure_container')}'."}

        elif stype == "gcs":
            import json as _json
            from google.cloud import storage as gcs
            from google.oauth2 import service_account
            creds_dict = _json.loads(storage.get("gcs_credentials_json") or "{}")
            creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = gcs.Client(credentials=creds)
            bucket = client.bucket(storage.get("gcs_bucket"))
            bucket.exists()
            return {"success": True, "message": f"Successfully connected to GCS bucket '{storage.get('gcs_bucket')}'."}

        elif stype == "s3":
            from engine import get_s3_client
            client = get_s3_client(storage)
            client.head_bucket(Bucket=storage.get("s3_bucket"))
            return {"success": True, "message": f"Successfully connected to S3 bucket '{storage.get('s3_bucket')}'."}

        elif stype == "bunny":
            import requests
            from engine import resolve_bunny_hosts
            zone = (storage.get("bunny_storage_zone") or "").strip()
            pwd = (storage.get("bunny_password") or storage.get("bunny_access_key") or "").strip()
            region_url = (storage.get("bunny_region_url") or storage.get("bunny_region") or "").strip()

            if not zone:
                raise HTTPException(400, "Access Key ID (name of zone) is required.")
            if not pwd:
                raise HTTPException(400, "Password / Access Key is required.")

            rest_host, s3_url = resolve_bunny_hosts(region_url)
            rest_err_msg = ""

            # 1. Attempt test via REST API
            rest_url = f"https://{rest_host}/{zone}/"
            headers = {"AccessKey": pwd, "Accept": "application/json"}
            try:
                resp = requests.get(rest_url, headers=headers, timeout=12)
                if resp.status_code == 200:
                    return {"success": True, "message": f"Successfully connected to BunnyCDN Storage Zone '{zone}' ({rest_host})."}
                elif resp.status_code == 401:
                    raise HTTPException(400, "Invalid BunnyCDN Password / Access Key (Unauthorized).")
                elif resp.status_code == 404:
                    raise HTTPException(400, f"BunnyCDN Storage Zone '{zone}' not found. Please verify Access Key ID (zone name).")
                else:
                    rest_err_msg = f"HTTP {resp.status_code}: {resp.text}"
            except HTTPException:
                raise
            except Exception as e:
                rest_err_msg = str(e)

            # 2. Also attempt S3-compatible API test
            try:
                import boto3
                from botocore.config import Config
                access_key = (storage.get("bunny_access_key") or zone).strip()
                s3 = boto3.client(
                    "s3",
                    endpoint_url=s3_url,
                    aws_access_key_id=access_key or zone,
                    aws_secret_access_key=pwd,
                    region_name="de",
                    config=Config(s3={'addressing_style': 'path'}, signature_version="s3v4", connect_timeout=10, read_timeout=10)
                )
                s3.head_bucket(Bucket=zone)
                return {"success": True, "message": f"Successfully connected to BunnyCDN Storage Zone '{zone}' via S3 API ({s3_url})."}
            except Exception as s3_err:
                err_str = str(s3_err)
                if "403" in err_str or "Forbidden" in err_str or "InvalidAccessKeyId" in err_str:
                    raise HTTPException(400, "Invalid BunnyCDN Password / Access Key or Zone Name.")
                elif "404" in err_str or "NoSuchBucket" in err_str:
                    raise HTTPException(400, f"BunnyCDN Storage Zone '{zone}' not found.")
                else:
                    raise HTTPException(400, f"BunnyCDN connection failed: {rest_err_msg or err_str}")
        else:
            raise HTTPException(400, f"Unknown storage type: {stype}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Storage connection failed: {str(e)}")

@router.post("/test-connection")
def test_new_storage_connection(req: StorageCreate, user=Depends(get_current_user)):
    return perform_test_storage(req.dict())

@router.post("/{storage_id}/test")
def test_storage_connection(storage_id: str, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    storage = next((s for s in storages if s["id"] == storage_id and s["user_id"] == user["sub"]), None)
    if not storage:
        raise HTTPException(404, "Storage not found")
    return perform_test_storage(storage)

@router.get("/{storage_id}/files")
def list_storage_files(storage_id: str, user=Depends(get_current_user)):
    storages = read_json("data/storages.json")
    storage = next((s for s in storages if s["id"] == storage_id and s["user_id"] == user["sub"]), None)
    if not storage:
        raise HTTPException(404, "Storage not found")
        
    files = []
    def is_backup_file(fn: str) -> bool:
        if not fn or fn.endswith(".indexes.json"):
            return False
        return (
            fn.endswith(".gz")
            or fn.endswith(".zst")
            or fn.endswith(".archive")
            or "backup.archive" in fn
            or fn.endswith(".tar.gz")
            or fn.endswith(".dump")
            or fn.endswith(".sql")
        )

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
                if is_backup_file(blob.name):
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
                if is_backup_file(blob.name):
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
                    if is_backup_file(name):
                        files.append({
                            "name": name,
                            "size": item["Size"],
                            "last_modified": item["LastModified"].isoformat() if item.get("LastModified") else None
                        })

        elif storage["type"] == "bunny":
            import requests
            from engine import get_bunny_host, get_bunny_key, resolve_bunny_hosts
            bunny_host = get_bunny_host(storage)
            bunny_key = get_bunny_key(storage)
            headers = {"AccessKey": bunny_key, "Accept": "application/json"}

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
                            if is_backup_file(full_name):
                                files.append({
                                    "name": full_name,
                                    "size": obj.get("Length", 0),
                                    "last_modified": obj.get("LastChanged")
                                })

            try:
                list_bunny_dir("")
            except Exception:
                pass

            # Fallback to Bunny S3-compatible API if REST listing found no files
            if not files:
                try:
                    import boto3
                    from botocore.config import Config
                    _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or storage.get("bunny_region") or "")
                    zone = (storage.get("bunny_storage_zone") or "").strip()
                    access_key = (storage.get("bunny_access_key") or zone).strip()
                    s3 = boto3.client(
                        "s3",
                        endpoint_url=s3_url,
                        aws_access_key_id=access_key or zone,
                        aws_secret_access_key=bunny_key,
                        region_name="de",
                        config=Config(s3={'addressing_style': 'path'}, signature_version="s3v4", retries={'max_attempts': 2, 'mode': 'standard'}, connect_timeout=10, read_timeout=15)
                    )
                    paginator = s3.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=zone):
                        for item in page.get("Contents", []):
                            name = item["Key"]
                            if is_backup_file(name):
                                files.append({
                                    "name": name,
                                    "size": item["Size"],
                                    "last_modified": item["LastModified"].isoformat() if item.get("LastModified") else None
                                })
                except Exception:
                    pass

    except Exception as e:
        raise HTTPException(500, f"Failed to fetch files: {str(e)}")
        
    sorted_files = sorted(files, key=lambda x: x["last_modified"] or "", reverse=True)
    return {"files": sorted_files}
