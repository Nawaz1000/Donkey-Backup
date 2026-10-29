from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json, calculate_next_run
from engine import do_backup

router = APIRouter()

class ScheduleCreate(BaseModel):
    name: str
    database_id: str
    storage_id: str
    frequency: Literal["hourly", "daily", "weekly", "monthly", "weekly_mixed"]
    time: Optional[str] = "02:00"  # HH:MM
    enabled: bool = True
    backup_method: Literal["full", "incremental", "differential"] = "full"
    incremental_field: Optional[str] = None
    speed_profile: Literal["default", "safe", "balanced", "extreme"] = "default"
    timezone_offset: int = 0
    database_name: Optional[str] = None
    collection: Optional[str] = None

@router.get("/")
def list_schedules(user=Depends(get_current_user)):
    schedules = read_json("data/schedules.json")
    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    result = []
    for s in schedules:
        if s["database_id"] in user_db_ids:
            db = next((d for d in dbs if d["id"] == s["database_id"]), {})
            st = next((st for st in storages if st["id"] == s["storage_id"]), {})
            # Ensure next_run is populated
            if not s.get("next_run"):
                s["next_run"] = calculate_next_run(s)
            target_db = s.get("database_name")
            if not target_db or (db.get("type") == "postgresql" and target_db in ("postgres", "ADMIN_CLIENT_PROD") and s.get("collection", "full") in ("full", "", None)):
                target_db = "__all__"
            db_display = f"{db.get('name', '')} (All Databases)" if target_db == "__all__" else f"{db.get('name', '')} ({target_db})"
            result.append({
                **s,
                "target_database": target_db,
                "database_name": db_display,
                "connection_name": db.get("name", ""),
                "storage_name": st.get("name", "")
            })
    return result

@router.post("/")
def create_schedule(req: ScheduleCreate, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    db = next((d for d in dbs if d["id"] == req.database_id and d["user_id"] == user["sub"]), None)
    if not db:
        raise HTTPException(404, "Database not found")
    schedules = read_json("data/schedules.json")
    target_dbname = req.database_name
    if not target_dbname:
        target_dbname = "__all__"
    schedule = {
        "id": str(uuid.uuid4()),
        "name": req.name,
        "database_id": req.database_id,
        "storage_id": req.storage_id,
        "frequency": req.frequency,
        "time": req.time,
        "enabled": req.enabled,
        "backup_method": req.backup_method,
        "incremental_field": req.incremental_field,
        "speed_profile": req.speed_profile,
        "timezone_offset": req.timezone_offset,
        "database_name": target_dbname,
        "collection": req.collection,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "last_run": None,
        "next_run": None,
    }
    schedule["next_run"] = calculate_next_run(schedule)
    schedules.append(schedule)
    write_json("data/schedules.json", schedules)
    return schedule

@router.post("/{schedule_id}/run")
def trigger_schedule(schedule_id: str, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    schedules = read_json("data/schedules.json")
    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}

    s = next((sch for sch in schedules if sch["id"] == schedule_id and sch["database_id"] in user_db_ids), None)
    if not s:
        raise HTTPException(404, "Schedule not found")

    db = next((d for d in dbs if d["id"] == s["database_id"]), None)
    if not db:
        raise HTTPException(404, "Database not found")

    storage = next((st for st in storages if st["id"] == s["storage_id"]), None)
    if not storage:
        raise HTTPException(404, "Storage destination not found")

    now = datetime.utcnow()
    method = s.get("backup_method", "full")
    profile = s.get("speed_profile", "default")
    target_dbname = s.get("database_name")
    if not target_dbname or target_dbname == "__all__" or (db.get("type") == "postgresql" and s.get("collection", "full") in ("full", "", None) and target_dbname in ("postgres", "ADMIN_CLIENT_PROD")):
        target_dbname = "__all__"

    backups = read_json("data/backups.json")
    backup = {
        "id": str(uuid.uuid4()),
        "database_id": s["database_id"],
        "source_dbname": target_dbname or "__all__",
        "storage_id": s["storage_id"],
        "label": f"{s['name']}-{method}-{now.strftime('%Y%m%d-%H%M%S')}",
        "collection": s.get("collection", "full") or "full",
        "database_name": target_dbname,
        "backup_method": method,
        "incremental_field": s.get("incremental_field", None),
        "indexing_mode": s.get("indexing_mode", "with_index"),
        "send_notifications": True,
        "speed_profile": profile,
        "status": "running",
        "size_mb": None,
        "remote_path": None,
        "remote_name": None,
        "is_scheduled": True,
        "error": None,
        "created_at": now.isoformat() + "Z",
        "completed_at": None,
        "duration_seconds": None,
    }
    backups.append(backup)
    write_json("data/backups.json", backups)

    s["last_run"] = now.isoformat() + "Z"
    s["next_run"] = calculate_next_run(s, now)
    write_json("data/schedules.json", schedules)

    background_tasks.add_task(do_backup, backup["id"])
    return {"message": f"Schedule '{s['name']}' started manually", "backup_id": backup["id"]}

@router.post("/trigger")
def trigger_all_schedules(background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    schedules = read_json("data/schedules.json")
    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}

    triggered = []
    now = datetime.utcnow()
    for s in schedules:
        if not s.get("enabled", True) or s["database_id"] not in user_db_ids:
            continue
        db = next((d for d in dbs if d["id"] == s["database_id"]), None)
        storage = next((st for st in storages if st["id"] == s["storage_id"]), None)
        if not db or not storage:
            continue

        method = s.get("backup_method", "full")
        profile = s.get("speed_profile", "default")
        target_dbname = s.get("database_name")
        if not target_dbname or target_dbname == "__all__" or (db.get("type") == "postgresql" and s.get("collection", "full") in ("full", "", None) and target_dbname in ("postgres", "ADMIN_CLIENT_PROD")):
            target_dbname = "__all__"

        backups = read_json("data/backups.json")
        backup = {
            "id": str(uuid.uuid4()),
            "database_id": s["database_id"],
            "source_dbname": target_dbname or "__all__",
            "storage_id": s["storage_id"],
            "label": f"{s['name']}-{method}-{now.strftime('%Y%m%d-%H%M%S')}",
            "collection": s.get("collection", "full") or "full",
            "database_name": target_dbname,
            "backup_method": method,
            "incremental_field": s.get("incremental_field", None),
            "indexing_mode": s.get("indexing_mode", "with_index"),
            "send_notifications": True,
            "speed_profile": profile,
            "status": "running",
            "size_mb": None,
            "remote_path": None,
            "remote_name": None,
            "is_scheduled": True,
            "error": None,
            "created_at": now.isoformat() + "Z",
            "completed_at": None,
            "duration_seconds": None,
        }
        backups.append(backup)
        write_json("data/backups.json", backups)

        s["last_run"] = now.isoformat() + "Z"
        s["next_run"] = calculate_next_run(s, now)
        triggered.append(s["name"])
        background_tasks.add_task(do_backup, backup["id"])

    write_json("data/schedules.json", schedules)
    return {"message": f"Triggered {len(triggered)} schedule(s)", "schedules": triggered}

@router.patch("/{schedule_id}/toggle")
def toggle_schedule(schedule_id: str, user=Depends(get_current_user)):
    schedules = read_json("data/schedules.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    for s in schedules:
        if s["id"] == schedule_id and s["database_id"] in user_db_ids:
            s["enabled"] = not s["enabled"]
            break
    write_json("data/schedules.json", schedules)
    return {"message": "Toggled"}

@router.delete("/{schedule_id}")
def delete_schedule(schedule_id: str, user=Depends(get_current_user)):
    schedules = read_json("data/schedules.json")
    dbs = read_json("data/databases.json")
    user_db_ids = {d["id"] for d in dbs if d["user_id"] == user["sub"]}
    schedules = [s for s in schedules if not (s["id"] == schedule_id and s["database_id"] in user_db_ids)]
    write_json("data/schedules.json", schedules)
    return {"message": "Deleted"}

