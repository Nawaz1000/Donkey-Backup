from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import asynccontextmanager
import uvicorn
import json
import os
import asyncio
from datetime import datetime, timedelta
import threading
import uuid
from utils import read_json, write_json
from engine import do_backup

from routers import auth, databases, backups, storage, schedules, settings

async def scheduler_loop():
    while True:
        try:
            if not os.path.exists("data/schedules.json"):
                await asyncio.sleep(60)
                continue
                
            schedules = read_json("data/schedules.json")
            now = datetime.utcnow()
            day_of_week = now.weekday()
            day_of_month = now.day

            modified = False
            for s in schedules:
                if not s.get("enabled", True):
                    continue

                freq = s.get("frequency")
                target_time = s.get("time")
                timezone_offset = s.get("timezone_offset", 0)
                local_now = now - timedelta(minutes=timezone_offset)
                day_of_week = local_now.weekday()
                day_of_month = local_now.day

                is_due = False
                backup_method_override = None
                
                if freq == "hourly":
                    last_run = s.get("last_run")
                    if not last_run:
                        is_due = True
                    else:
                        last_run_dt = datetime.fromisoformat(last_run)
                        if now - last_run_dt >= timedelta(hours=1):
                            is_due = True
                else:
                    try:
                        th, tm = map(int, target_time.split(":"))
                        target_dt = local_now.replace(hour=th, minute=tm, second=0, microsecond=0)
                    except Exception:
                        continue
                        
                    if target_dt <= local_now < target_dt + timedelta(minutes=5):
                        day_ok = False
                        if freq in ("daily", "weekly_mixed"):
                            day_ok = True
                        elif freq == "weekly" and day_of_week == 0:
                            day_ok = True
                        elif freq == "monthly" and day_of_month == 1:
                            day_ok = True
                            
                        if day_ok:
                            last_run = s.get("last_run")
                            if not last_run:
                                is_due = True
                            else:
                                last_run_dt = datetime.fromisoformat(last_run)
                                last_run_local = last_run_dt - timedelta(minutes=timezone_offset)
                                if last_run_local.date() < local_now.date():
                                    is_due = True
                            
                            if is_due and freq == "weekly_mixed":
                                if day_of_week == 6:  # Sunday
                                    backup_method_override = "full"
                                else:
                                    backup_method_override = "incremental"

                if is_due:
                    method = backup_method_override or s.get("backup_method", "full")
                    method_label = f" [{method.upper()}]" if freq == "weekly_mixed" else ""
                    print(f"Triggering scheduled backup: {s['name']}{method_label}")
                    s["last_run"] = now.isoformat() + "Z"
                    modified = True
                    
                    profile = s.get("speed_profile", "default")
                    if profile == "default" and 0 <= now.hour < 6:
                        profile = "extreme"
                        print("Night-time window detected. Overriding speed_profile to 'extreme'.")
                    
                    backups = read_json("data/backups.json")
                    backup = {
                        "id": str(uuid.uuid4()),
                        "database_id": s["database_id"],
                        "storage_id": s["storage_id"],
                        "label": f"{s['name']}-{method}-{now.strftime('%Y%m%d-%H%M%S')}",
                        "collection": s.get("collection", "full") or "full",
                        "database_name": s.get("database_name"),
                        "backup_method": method,
                        "incremental_field": s.get("incremental_field", None),
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
                    
                    threading.Thread(target=do_backup, args=(backup["id"],)).start()
            
            if modified:
                write_json("data/schedules.json", schedules)
                
        except Exception as e:
            print(f"Scheduler error: {e}")
        
        await asyncio.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    for f, default in [
        ("data/users.json", []),
        ("data/databases.json", []),
        ("data/backups.json", []),
        ("data/storages.json", []),
        ("data/schedules.json", []),
        ("data/syncs.json", []),
    ]:
        os.makedirs("data", exist_ok=True)
        if not os.path.exists(f):
            with open(f, "w") as fp:
                json.dump(default, fp)
                
    asyncio.create_task(scheduler_loop())
    yield


app = FastAPI(title="BackupVault API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(databases.router, prefix="/api/databases", tags=["databases"])
app.include_router(backups.router, prefix="/api/backups", tags=["backups"])
app.include_router(storage.router, prefix="/api/storage", tags=["storage"])
app.include_router(schedules.router, prefix="/api/schedules", tags=["schedules"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])

@app.get("/api/health")
def health():
    return {"status": "ok", "app": "BackupVault"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
