from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import uuid
import os
from datetime import datetime
from utils import get_current_user, read_json, write_json
from engine import do_backup, do_restore

router = APIRouter()

class BackupCreate(BaseModel):
    database_id: str
    storage_id: str
    label: Optional[str] = ""
    collection: Optional[str] = ""

class RestoreRequest(BaseModel):
    backup_id: str
    target_database_id: str
    new_database: bool = False
    new_database_name: Optional[str] = None

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
        "storage_id": req.storage_id,
        "label": req.label or f"backup-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "collection": req.collection or "full",
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
    return {"logs": logs, "status": backup["status"], "error": backup.get("error")}

@router.delete("/{backup_id}")
def delete_backup(backup_id: str, user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    backups = [b for b in backups if not (b["id"] == backup_id and b["database_id"] in user_db_ids)]
    write_json("data/backups.json", backups)
    return {"message": "Deleted"}

@router.get("/stats")
def backup_stats(user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    user_backups = [b for b in backups if b["database_id"] in user_db_ids]
    completed = [b for b in user_backups if b["status"] == "completed"]
    total_size = sum(b.get("size_mb", 0) or 0 for b in completed)
    return {
        "total": len(user_backups),
        "completed": len(completed),
        "running": len([b for b in user_backups if b["status"] == "running"]),
        "failed": len([b for b in user_backups if b["status"] == "failed"]),
        "total_size_mb": round(total_size, 2),
        "databases": len(user_db_ids),
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
    return {"logs": logs, "status": restore["status"], "error": restore.get("error")}

@router.post("/restore")
def restore_backup(req: RestoreRequest, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    if not os.path.exists("data/restores.json"):
        write_json("data/restores.json", [])

    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}

    backup = next((b for b in backups if b["id"] == req.backup_id and b["database_id"] in user_db_ids), None)
    if not backup:
        raise HTTPException(404, "Backup not found")
    if backup["status"] != "completed":
        raise HTTPException(400, "Only completed backups can be restored")
    if not backup.get("remote_name"):
        raise HTTPException(400, "Backup has no remote file stored")

    target_db = next((d for d in dbs if d["id"] == req.target_database_id and d["user_id"] == user["sub"]), None)
    if not target_db:
        raise HTTPException(404, "Target database not found")

    restores = read_json("data/restores.json")
    restore = {
        "id": str(uuid.uuid4()),
        "backup_id": req.backup_id,
        "target_database_id": req.target_database_id,
        "new_database": req.new_database,
        "new_database_name": req.new_database_name if req.new_database else None,
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
            result.append({**b, "database_name": db.get("name",""), "database_type": db.get("type",""), "storage_name": st.get("name",""), "storage_type": st.get("type","")})
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
        "storage_id": req.storage_id,
        "label": req.label or f"backup-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "collection": req.collection or "full",
        "status": "running",
        "size_mb": None,
        "created_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "duration_seconds": None,
    }
    backups.append(backup)
    write_json("data/backups.json", backups)
    background_tasks.add_task(simulate_backup, backup["id"])
    return backup

@router.delete("/{backup_id}")
def delete_backup(backup_id: str, user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    backups = [b for b in backups if not (b["id"] == backup_id and b["database_id"] in user_db_ids)]
    write_json("data/backups.json", backups)
    return {"message": "Deleted"}

@router.get("/stats")
def backup_stats(user=Depends(get_current_user)):
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    user_backups = [b for b in backups if b["database_id"] in user_db_ids]
    completed = [b for b in user_backups if b["status"] == "completed"]
    total_size = sum(b.get("size_mb", 0) or 0 for b in completed)
    return {
        "total": len(user_backups),
        "completed": len(completed),
        "running": len([b for b in user_backups if b["status"] == "running"]),
        "failed": len([b for b in user_backups if b["status"] == "failed"]),
        "total_size_mb": round(total_size, 2),
        "databases": len(user_db_ids),
    }

# ---- RESTORE ----

@router.get("/restores")
def list_restores(user=Depends(get_current_user)):
    import os
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
                "target_database_name": db.get("name",""),
                "backup_label": bk.get("label",""),
                "backup_collection": bk.get("collection","full"),
            })
    return sorted(result, key=lambda x: x["started_at"], reverse=True)

@router.post("/restore")
def restore_backup(req: RestoreRequest, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    import os
    if not os.path.exists("data/restores.json"):
        write_json("data/restores.json", [])

    # Validate backup exists and belongs to user
    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    backup = next((b for b in backups if b["id"] == req.backup_id and b["database_id"] in user_db_ids), None)
    if not backup:
        raise HTTPException(404, "Backup not found")
    if backup["status"] != "completed":
        raise HTTPException(400, "Only completed backups can be restored")

    # Validate target DB
    target_db = next((d for d in dbs if d["id"] == req.target_database_id and d["user_id"] == user["sub"]), None)
    if not target_db:
        raise HTTPException(404, "Target database not found")

    restores = read_json("data/restores.json")
    restore = {
        "id": str(uuid.uuid4()),
        "backup_id": req.backup_id,
        "target_database_id": req.target_database_id,
        "status": "running",
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "duration_seconds": None,
    }
    restores.append(restore)
    write_json("data/restores.json", restores)
    background_tasks.add_task(simulate_restore, restore["id"])
    return restore