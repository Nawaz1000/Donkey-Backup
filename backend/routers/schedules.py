from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json

router = APIRouter()

class ScheduleCreate(BaseModel):
    name: str
    database_id: str
    storage_id: str
    frequency: Literal["hourly", "daily", "weekly", "monthly"]
    time: Optional[str] = "02:00"  # HH:MM
    enabled: bool = True

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
            result.append({**s, "database_name": db.get("name",""), "storage_name": st.get("name","")})
    return result

@router.post("/")
def create_schedule(req: ScheduleCreate, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    db = next((d for d in dbs if d["id"] == req.database_id and d["user_id"] == user["sub"]), None)
    if not db:
        raise HTTPException(404, "Database not found")
    schedules = read_json("data/schedules.json")
    schedule = {
        "id": str(uuid.uuid4()),
        "name": req.name,
        "database_id": req.database_id,
        "storage_id": req.storage_id,
        "frequency": req.frequency,
        "time": req.time,
        "enabled": req.enabled,
        "created_at": datetime.utcnow().isoformat(),
        "last_run": None,
        "next_run": datetime.utcnow().isoformat(),
    }
    schedules.append(schedule)
    write_json("data/schedules.json", schedules)
    return schedule

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
