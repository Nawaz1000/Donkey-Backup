from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import uuid
import random
from datetime import datetime
from utils import get_current_user, read_json, write_json

router = APIRouter()

class BackupCreate(BaseModel):
    database_id: str
    storage_id: str
    label: Optional[str] = ""
    collection: Optional[str] = ""  # specific collection/table, empty = full backup

def simulate_backup(backup_id: str):
    import time, random
    time.sleep(2)  # Simulate backup time
    backups = read_json("data/backups.json")
    for b in backups:
        if b["id"] == backup_id:
            b["status"] = "completed"
            b["size_mb"] = round(random.uniform(10, 500), 2)
            b["completed_at"] = datetime.utcnow().isoformat()
            b["duration_seconds"] = random.randint(5, 120)
            break
    write_json("data/backups.json", backups)

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