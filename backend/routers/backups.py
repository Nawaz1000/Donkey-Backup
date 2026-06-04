from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
import os
from datetime import datetime
from utils import get_current_user, read_json, write_json
from engine import do_backup, do_restore, cancel_job

router = APIRouter()

class BackupCreate(BaseModel):
    database_id: str
    storage_id: str
    label: Optional[str] = ""
    collection: Optional[str] = ""
    backup_method: Literal["full", "incremental", "differential"] = "full"
    incremental_field: Optional[str] = None
    indexing_mode: Literal["with_index", "without_index", "only_index"] = "with_index"
    send_notifications: Optional[bool] = True

class RestoreRequest(BaseModel):
    backup_id: Optional[str] = None
    storage_id: Optional[str] = None
    remote_name: Optional[str] = None
    target_database_id: str
    new_database: bool = False
    new_database_name: Optional[str] = None
    indexing_mode: Literal["with_index", "without_index", "only_index"] = "with_index"

@router.get("/")
def list_backups(user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    result = []
    for b in backups:
        if b["database_id"] in user_db_ids:
            db = next((d for d in dbs if d["id"] == b["database_id"]), {})
            st = next((s for s in storages if s["id"] == b["storage_id"]), {})
            result.append({**b,
                "connection_name": db.get("name",""),
                "actual_db_name": db.get("database_name",""),
                "database_name": db.get("name",""),
                "database_type": db.get("type",""),
                "storage_name": st.get("name",""),
                "storage_type": st.get("type",""),
            })
    return sorted(result, key=lambda x: x["created_at"], reverse=True)

@router.post("/")
def create_backup(req: BackupCreate, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    db = next((d for d in dbs if d["id"] == req.database_id and d["user_id"] == user["sub"]), None)
    if not db:
        raise HTTPException(404, "Database not found")
    storages = read_json("data/storages.json")
    storage = next((s for s in storages if s["id"] == req.storage_id and s["user_id"] == user["sub"]), None)
    if not storage:
        raise HTTPException(404, "Storage not found")

    backups = read_json("data/backups.json")
    backup = {
        "id": str(uuid.uuid4()),
        "database_id": req.database_id,
        "source_dbname": db.get("database_name", ""),
        "storage_id": req.storage_id,
        "label": req.label or f"backup-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "collection": req.collection or "full",
        "backup_method": req.backup_method,
        "incremental_field": req.incremental_field,
        "indexing_mode": req.indexing_mode,
        "send_notifications": req.send_notifications,
        "status": "running",
        "size_mb": None,
        "remote_path": None,
        "remote_name": None,
        "error": None,
        "created_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "duration_seconds": None,
    }
    backups.append(backup)
    write_json("data/backups.json", backups)
    background_tasks.add_task(do_backup, backup["id"])
    return backup

@router.get("/{backup_id}/logs")
def get_backup_logs(backup_id: str, user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    backup = next((b for b in backups if b["id"] == backup_id and b["database_id"] in user_db_ids), None)
    if not backup:
        raise HTTPException(404, "Backup not found")
    from engine import get_log_path, read_log
    live_logs = read_log(get_log_path(backup_id))
    logs = live_logs or backup.get("logs", "")
    return {
        "logs": logs,
        "status": backup["status"],
        "error": backup.get("error"),
        "progress_percentage": backup.get("progress_percentage", 0)
    }

@router.delete("/{backup_id}")
def delete_backup(backup_id: str, delete_from_remote: bool = False, user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    
    backup = next((b for b in backups if b["id"] == backup_id and b["database_id"] in user_db_ids), None)
    if not backup:
        raise HTTPException(404, "Backup not found")
        
    if delete_from_remote and backup.get("remote_name"):
        try:
            storages = read_json("data/storages.json")
            storage = next((s for s in storages if s["id"] == backup["storage_id"]), None)
            if storage:
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
                    container.delete_blob(backup["remote_name"])
                elif storage["type"] == "gcs":
                    import json
                    from google.cloud import storage as gcs
                    from google.oauth2 import service_account
                    creds_dict = json.loads(storage["gcs_credentials_json"])
                    creds = service_account.Credentials.from_service_account_info(creds_dict)
                    client = gcs.Client(credentials=creds)
                    bucket = client.bucket(storage["gcs_bucket"])
                    blob = bucket.blob(backup["remote_name"])
                    blob.delete()
        except Exception as e:
            print(f"Failed to delete remote backup: {e}")
            raise HTTPException(400, f"Failed to delete remote backup: {str(e)}")

    backups = [b for b in backups if b["id"] != backup_id]
    write_json("data/backups.json", backups)
    return {"message": "Deleted"}

@router.delete("/restores/{restore_id}")
def delete_restore(restore_id: str, user=Depends(get_current_user)):
    if not os.path.exists("data/restores.json"):
        raise HTTPException(404, "No restores found")
    restores = read_json("data/restores.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    restore = next((r for r in restores if r["id"] == restore_id), None)
    if not restore:
        raise HTTPException(404, "Restore not found")
    if restore["target_database_id"] not in user_db_ids:
        raise HTTPException(403, "Access denied")
    restores = [r for r in restores if r["id"] != restore_id]
    write_json("data/restores.json", restores)
    return {"message": "Deleted"}

@router.get("/stats")
def backup_stats(user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    if not os.path.exists("data/restores.json"):
        write_json("data/restores.json", [])
    restores = read_json("data/restores.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    user_backups = [b for b in backups if b["database_id"] in user_db_ids]
    completed = [b for b in user_backups if b["status"] == "completed"]
    total_size = sum(b.get("size_mb", 0) or 0 for b in completed)
    user_restores = [r for r in restores if r["target_database_id"] in user_db_ids]
    return {
        "total": len(user_backups),
        "completed": len(completed),
        "running": len([b for b in user_backups if b["status"] == "running"]),
        "failed": len([b for b in user_backups if b["status"] == "failed"]),
        "total_size_mb": round(total_size, 2),
        "databases": len(user_db_ids),
        "total_restores": len(user_restores),
    }

@router.get("/restores")
def list_restores(user=Depends(get_current_user)):
    if not os.path.exists("data/restores.json"):
        write_json("data/restores.json", [])
    restores = read_json("data/restores.json")
    dbs = read_json("data/databases.json")
    backups = read_json("data/backups.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    result = []
    for r in restores:
        if r["target_database_id"] in user_db_ids:
            db = next((d for d in dbs if d["id"] == r["target_database_id"]), {})
            bk = next((b for b in backups if b["id"] == r["backup_id"]), {})
            result.append({**r,
                "target_connection_name": db.get("name",""),
                "target_actual_db_name": db.get("database_name",""),
                "target_database_name": db.get("name",""),
                "backup_label": bk.get("label",""),
                "backup_collection": bk.get("collection","full"),
                "error": r.get("error"),
            })
    return sorted(result, key=lambda x: x["started_at"], reverse=True)

@router.get("/restores/{restore_id}/logs")
def get_restore_logs(restore_id: str, user=Depends(get_current_user)):
    if not os.path.exists("data/restores.json"):
        raise HTTPException(404, "No restores found")
    restores = read_json("data/restores.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    restore = next((r for r in restores if r["id"] == restore_id and r["target_database_id"] in user_db_ids), None)
    if not restore:
        raise HTTPException(404, "Restore not found")
    from engine import get_log_path, read_log
    live_logs = read_log(get_log_path(f"restore_{restore_id}"))
    logs = live_logs or restore.get("logs", "")
    return {
        "logs": logs,
        "status": restore["status"],
        "error": restore.get("error"),
        "progress_percentage": restore.get("progress_percentage", 0)
    }

@router.post("/restore")
def restore_backup(req: RestoreRequest, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    if not os.path.exists("data/restores.json"):
        write_json("data/restores.json", [])

    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}

    if req.backup_id:
        backups = read_json("data/backups.json")
        backup = next((b for b in backups if b["id"] == req.backup_id and b["database_id"] in user_db_ids), None)
        if not backup:
            raise HTTPException(404, "Backup not found")
        if backup["status"] != "completed":
            raise HTTPException(400, "Only completed backups can be restored")
        if not backup.get("remote_name"):
            raise HTTPException(400, "Backup has no remote file stored")
    elif req.storage_id and req.remote_name:
        storages = read_json("data/storages.json")
        storage = next((s for s in storages if s["id"] == req.storage_id and s["user_id"] == user["sub"]), None)
        if not storage:
            raise HTTPException(404, "Storage not found")
    else:
        raise HTTPException(400, "Must provide either backup_id or both storage_id and remote_name")

    target_db = next((d for d in dbs if d["id"] == req.target_database_id and d["user_id"] == user["sub"]), None)
    if not target_db:
        raise HTTPException(404, "Target database not found")

    restores = read_json("data/restores.json")
    restore = {
        "id": str(uuid.uuid4()),
        "backup_id": req.backup_id,
        "storage_id": req.storage_id,
        "remote_name": req.remote_name,
        "target_database_id": req.target_database_id,
        "new_database": req.new_database,
        "new_database_name": req.new_database_name if req.new_database else None,
        "indexing_mode": req.indexing_mode,
        "status": "running",
        "error": None,
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "duration_seconds": None,
    }
    restores.append(restore)
    write_json("data/restores.json", restores)
    background_tasks.add_task(do_restore, restore["id"])
    return restore


@router.post("/backups/{backup_id}/cancel")
def cancel_backup(backup_id: str, user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    for b in backups:
        if b["id"] == backup_id:
            if b.get("status") == "running":
                if cancel_job(backup_id):
                    b["status"] = "failed"
                    b["error"] = "Cancelled by user"
                    write_json("data/backups.json", backups)
                    return {"success": True, "message": "Backup cancelled"}
                return {"success": False, "message": "Could not cancel backup (process not found)"}
            return {"success": False, "message": "Backup is not running"}
    raise HTTPException(404, "Backup not found")


@router.post("/restores/{restore_id}/cancel")
def cancel_restore(restore_id: str, user=Depends(get_current_user)):
    if not os.path.exists("data/restores.json"):
        raise HTTPException(404, "Restore not found")
    restores = read_json("data/restores.json")
    for r in restores:
        if r["id"] == restore_id:
            if r.get("status") == "running":
                if cancel_job(restore_id):
                    r["status"] = "failed"
                    r["error"] = "Cancelled by user"
                    write_json("data/restores.json", restores)
                    return {"success": True, "message": "Restore cancelled"}
                return {"success": False, "message": "Could not cancel restore (process not found)"}
            return {"success": False, "message": "Restore is not running"}
    raise HTTPException(404, "Restore not found")
