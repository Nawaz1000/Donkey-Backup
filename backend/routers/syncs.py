from fastapi import APIRouter, HTTPException, BackgroundTasks
import uuid
from datetime import datetime
from pydantic import BaseModel
from typing import Optional

from utils import read_json, write_json
from engine import do_sync

router = APIRouter()

class SyncRequest(BaseModel):
    source_database_id: str
    target_database_id: str
    collection: Optional[str] = "full"
    override_target_name: Optional[str] = ""
    drop_existing: Optional[bool] = False
    label: str

@router.post("/")
async def create_sync(req: SyncRequest, background_tasks: BackgroundTasks):
    dbs = read_json("data/databases.json")
    
    src_db = next((d for d in dbs if d["id"] == req.source_database_id), None)
    tgt_db = next((d for d in dbs if d["id"] == req.target_database_id), None)
    
    if not src_db or not tgt_db:
        raise HTTPException(status_code=400, detail="Invalid source or target database ID")
        
    if src_db["type"] != tgt_db["type"]:
        raise HTTPException(status_code=400, detail="Source and Target database types must match (e.g. MongoDB to MongoDB)")

    sync_id = str(uuid.uuid4())
    sync_job = {
        "id": sync_id,
        "label": req.label,
        "source_database_id": req.source_database_id,
        "target_database_id": req.target_database_id,
        "database_type": src_db["type"],
        "collection": req.collection,
        "override_target_name": req.override_target_name,
        "drop_existing": req.drop_existing,
        "status": "running",
        "progress": 0,
        "size_mb": 0,
        "logs": "",
        "created_at": datetime.utcnow().isoformat()
    }

    syncs = read_json("data/syncs.json")
    syncs.append(sync_job)
    write_json("data/syncs.json", syncs)

    background_tasks.add_task(do_sync, sync_id)
    return {"id": sync_id, "message": "Sync job started"}


@router.get("/")
async def get_syncs():
    return read_json("data/syncs.json")


@router.delete("/{sync_id}")
async def delete_sync(sync_id: str):
    syncs = read_json("data/syncs.json")
    filtered = [s for s in syncs if s["id"] != sync_id]
    if len(syncs) == len(filtered):
        raise HTTPException(status_code=404, detail="Sync job not found")
    
    write_json("data/syncs.json", filtered)
    return {"message": "Deleted sync job history"}
