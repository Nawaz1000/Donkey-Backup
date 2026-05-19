from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json
 
def sanitize_mongo_uri(uri: str) -> str:
    from urllib.parse import quote
    try:
        if "://" not in uri:
            return uri
        scheme_end = uri.index("://") + 3
        scheme = uri[:scheme_end]
        rest = uri[scheme_end:]
        if "@" not in rest:
            return uri
        at_pos = rest.rfind("@")
        userinfo = rest[:at_pos]
        hostpart = rest[at_pos+1:]
        if ":" in userinfo:
            colon_pos = userinfo.index(":")
            user = userinfo[:colon_pos]
            password = userinfo[colon_pos+1:]
            safe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
            return f"{scheme}{quote(user, safe=safe)}:{quote(password, safe=safe)}@{hostpart}"
        return uri
    except Exception:
        return uri
 
router = APIRouter()
 
class DatabaseCreate(BaseModel):
    name: str
    type: Literal["postgresql", "mongodb"]
    host: Optional[str] = ""
    port: Optional[int] = 27017
    username: Optional[str] = ""
    password: Optional[str] = ""
    database_name: Optional[str] = ""
    description: Optional[str] = ""
    mongo_uri: Optional[str] = ""
 
class TestUriRequest(BaseModel):
    uri: str
    type: str = "mongodb"
 
@router.get("/")
def list_databases(user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    return [
        {k: v for k, v in d.items() if k != "password"}
        for d in dbs if d["user_id"] == user["sub"]
    ]
 
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
        "mongo_uri": req.mongo_uri or "",
        "created_at": datetime.utcnow().isoformat(),
        "status": "active",
    }
    dbs.append(db)
    write_json("data/databases.json", dbs)
    return {k: v for k, v in db.items() if k != "password"}
 
@router.post("/test-uri")
def test_uri(req: TestUriRequest, user=Depends(get_current_user)):
    if req.type != "mongodb":
        raise HTTPException(400, "Only MongoDB URI testing is supported")
    try:
        from pymongo import MongoClient
        clean_uri = sanitize_mongo_uri(req.uri)
        client = MongoClient(clean_uri, serverSelectionTimeoutMS=5000)
        info = client.server_info()
        version = info.get("version", "unknown")
        client.close()
        return {"success": True, "message": f"Connected successfully! MongoDB version: {version}"}
    except Exception as e:
        raise HTTPException(400, f"Connection failed: {str(e)}")
 
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
                if k not in ["id", "user_id"]:
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
 
    if db["type"] == "mongodb":
        try:
            from pymongo import MongoClient
            uri = db.get("mongo_uri") or f"mongodb://{db['username']}:{db['password']}@{db['host']}:{db['port']}/{db['database_name']}"
            client = MongoClient(sanitize_mongo_uri(uri), serverSelectionTimeoutMS=5000)
            info = client.server_info()
            version = info.get("version", "unknown")
            client.close()
            return {"success": True, "message": f"Connected to {db['name']} — MongoDB v{version}"}
        except Exception as e:
            raise HTTPException(400, f"Connection failed: {str(e)}")
 
    elif db["type"] == "postgresql":
        try:
            import subprocess, os
            env = os.environ.copy()
            env["PGPASSWORD"] = db.get("password", "")
            result = subprocess.run(
                ["psql", f"--host={db['host']}", f"--port={db['port']}",
                 f"--username={db['username']}", f"--dbname={db['database_name']}",
                 "--no-password", "-c", "SELECT version();"],
                capture_output=True, text=True, env=env, timeout=10
            )
            if result.returncode == 0:
                version = result.stdout.strip().split('\n')[2].strip() if result.stdout else ""
                return {"success": True, "message": f"Connected to {db['name']} — {version[:60]}"}
            raise HTTPException(400, f"Connection failed: {result.stderr.strip()}")
        except Exception as e:
            raise HTTPException(400, f"Connection failed: {str(e)}")
 
    return {"success": True, "message": f"Connection to {db['name']} OK"}