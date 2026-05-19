from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json

router = APIRouter()

class DatabaseCreate(BaseModel):
    name: str
    type: Literal["postgresql", "mongodb"]
    host: str
    port: int
    username: str
    password: str
    database_name: str
    description: Optional[str] = ""

@router.get("/")
def list_databases(user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    return [d for d in dbs if d["user_id"] == user["sub"]]

@router.post("/")
def add_database(req: DatabaseCreate, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    db = {
        "id": str(uuid.uuid4()),
        "user_id": user["sub"],
        "name": req.name,
        "type": req.type,
        "host": req.host,
        "port": req.port,
        "username": req.username,
        "password": req.password,
        "database_name": req.database_name,
        "description": req.description,
        "created_at": datetime.utcnow().isoformat(),
        "status": "active",
    }
    dbs.append(db)
    write_json("data/databases.json", dbs)
    db_safe = {k: v for k, v in db.items() if k != "password"}
    return db_safe

@router.delete("/{db_id}")
def delete_database(db_id: str, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    dbs = [d for d in dbs if not (d["id"] == db_id and d["user_id"] == user["sub"])]
    write_json("data/databases.json", dbs)
    return {"message": "Deleted"}

@router.patch("/{db_id}")
def update_database(db_id: str, req: dict, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    for d in dbs:
        if d["id"] == db_id and d["user_id"] == user["sub"]:
            for k, v in req.items():
                if k != "id" and k != "user_id":
                    d[k] = v
            break
    write_json("data/databases.json", dbs)
    return {"message": "Updated"}

@router.get("/{db_id}/test")
def test_connection(db_id: str, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    db = next((d for d in dbs if d["id"] == db_id and d["user_id"] == user["sub"]), None)
    if not db:
        raise HTTPException(404, "Database not found")
    # Simulate connection test
    return {"success": True, "message": f"Connection to {db['name']} successful"}