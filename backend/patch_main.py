import re

with open("backend/main.py", "r", encoding="utf-8") as f:
    content = f.read()

scheduler_code = """import asyncio
from datetime import datetime, timedelta
import threading
import uuid
from utils import read_json, write_json
from engine import do_backup

async def scheduler_loop():
    while True:
        try:
            if not os.path.exists("data/schedules.json"):
                await asyncio.sleep(60)
                continue
                
            schedules = read_json("data/schedules.json")
            now = datetime.utcnow()
            current_time = now.strftime("%H:%M")
            day_of_week = now.weekday()
            day_of_month = now.day

            modified = False
            for s in schedules:
                if not s.get("enabled", True):
                    continue

                freq = s.get("frequency")
                target_time = s.get("time")

                is_due = False
                if freq == "hourly":
                    last_run = s.get("last_run")
                    if not last_run:
                        is_due = True
                    else:
                        last_run_dt = datetime.fromisoformat(last_run)
                        if now - last_run_dt >= timedelta(hours=1):
                            is_due = True
                else:
                    if current_time == target_time:
                        last_run = s.get("last_run")
                        if last_run:
                            last_run_dt = datetime.fromisoformat(last_run)
                            if now - last_run_dt < timedelta(minutes=2):
                                continue

                        if freq == "daily":
                            is_due = True
                        elif freq == "weekly" and day_of_week == 0:
                            is_due = True
                        elif freq == "monthly" and day_of_month == 1:
                            is_due = True

                if is_due:
                    print(f"Triggering scheduled backup: {s['name']}")
                    s["last_run"] = now.isoformat()
                    modified = True
                    
                    backups = read_json("data/backups.json")
                    backup = {
                        "id": str(uuid.uuid4()),
                        "database_id": s["database_id"],
                        "storage_id": s["storage_id"],
                        "label": f"{s['name']}-{now.strftime('%Y%m%d-%H%M%S')}",
                        "collection": "full",
                        "backup_method": s.get("backup_method", "full"),
                        "incremental_field": s.get("incremental_field", None),
                        "status": "running",
                        "size_mb": None,
                        "remote_path": None,
                        "remote_name": None,
                        "error": None,
                        "created_at": now.isoformat(),
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
    ]:
        os.makedirs("data", exist_ok=True)
        if not os.path.exists(f):
            with open(f, "w") as fp:
                json.dump(default, fp)
                
    asyncio.create_task(scheduler_loop())
    yield
"""

# Replace imports and lifespan
content = content.replace("import os\n", "import os\n" + "\n".join(scheduler_code.split("\n")[:6]) + "\n")
content = re.sub(r"@asynccontextmanager.*?yield", "\n".join(scheduler_code.split("\n")[7:]), content, flags=re.DOTALL)

with open("backend/main.py", "w", encoding="utf-8") as f:
    f.write(content)
