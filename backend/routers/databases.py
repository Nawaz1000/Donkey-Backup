from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal
import uuid
from datetime import datetime
from utils import get_current_user, read_json, write_json, parse_mongo_uri

router = APIRouter()


def get_solr_base_url(host: str, port: int) -> str:
    host = host.strip()
    if host.startswith("http://") or host.startswith("https://"):
        from urllib.parse import urlparse
        parsed = urlparse(host)
        return f"{parsed.scheme}://{parsed.netloc}"
    scheme = "https" if port == 443 else "http"
    return f"{scheme}://{host}:{port}"

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
        hostpart = rest[at_pos + 1:]
        if ":" in userinfo:
            colon_pos = userinfo.index(":")
            user = userinfo[:colon_pos]
            password = userinfo[colon_pos + 1:]
            safe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
            return f"{scheme}{quote(user, safe=safe)}:{quote(password, safe=safe)}@{hostpart}"
        return uri
    except Exception:
        return uri


def connect_mongo(uri: str):
    from pymongo import MongoClient
    m = parse_mongo_uri(uri)
    kwargs = {
        "host": m["hosts"],
        "port": m["port"] or 27017,
        "serverSelectionTimeoutMS": 5000,
        "connectTimeoutMS": 5000,
        "socketTimeoutMS": 10000,
    }
    if m["username"]:
        kwargs["username"] = m["username"]
    if m["password"]:
        kwargs["password"] = m["password"]
    
    opts = m["options"]
    if "authSource" in opts:
        kwargs["authSource"] = opts["authSource"]
    if "authMechanism" in opts:
        kwargs["authMechanism"] = opts["authMechanism"]
    if "directConnection" in opts:
        kwargs["directConnection"] = str(opts["directConnection"]).lower() == "true"
        
    return MongoClient(**kwargs)


class DatabaseCreate(BaseModel):
    name: str
    type: Literal["postgresql", "mongodb", "solr"]
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


router = APIRouter()


@router.get("/")
def list_databases(user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    modified = False
    for d in dbs:
        if not d.get("type"):
            name = d.get("name", "").lower()
            host = d.get("host", "").lower()
            if "solr" in name or "solr" in host or d.get("port") == 8983:
                d["type"] = "solr"
                modified = True
            elif "mongo" in name or "mongo" in host or d.get("mongo_uri") or d.get("port") == 27017:
                d["type"] = "mongodb"
                modified = True
            else:
                d["type"] = "postgresql"
                modified = True
    if modified:
        write_json("data/databases.json", dbs)

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

        "created_at": datetime.utcnow().isoformat() + "Z",
        "status": "active",
    }
    dbs.append(db)
    write_json("data/databases.json", dbs)
    return {k: v for k, v in db.items() if k != "password"}


@router.post("/test-connection")
def test_new_connection(db: DatabaseCreate, user=Depends(get_current_user)):
    if db.type == "mongodb":
        try:
            uri = sanitize_mongo_uri(db.mongo_uri or "")
            if not uri:
                uri = f"mongodb://{db.host}:{db.port}"
            client = connect_mongo(uri)
            info = client.server_info()
            version = info.get("version", "?")
            client.close()
            return {"success": True, "message": f"Connected! MongoDB v{version}"}
        except Exception as e:
            raise HTTPException(400, f"Connection failed: {str(e)}")

    elif db.type == "postgresql":
        try:
            import subprocess, os
            env = os.environ.copy()
            if db.password:
                env["PGPASSWORD"] = db.password
            cmd = ["psql", "-h", db.host, "-p", str(db.port), "-U", db.username, "-d", db.database_name or "postgres", "-c", "SELECT version();"]
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(result.stderr.strip())
            version_line = result.stdout.strip().split('\n')[0][:50]
            return {"success": True, "message": f"Connected! {version_line}..."}
        except Exception as e:
            raise HTTPException(400, f"Connection failed: {str(e)}")

    elif db.type == "solr":
        try:
            import urllib.request, json, base64, ssl
            solr_base_url = get_solr_base_url(db.host, db.port)
            context = ssl._create_unverified_context()
            cores_url = f"{solr_base_url}/solr/admin/cores?action=STATUS&wt=json"
            req_cores = urllib.request.Request(cores_url)
            if db.username and db.password:
                auth_str = f"{db.username}:{db.password}"
                encoded_auth = base64.b64encode(auth_str.encode()).decode()
                req_cores.add_header("Authorization", f"Basic {encoded_auth}")
            with urllib.request.urlopen(req_cores, context=context, timeout=10) as response:
                data = json.loads(response.read().decode())
                cores = len(data.get("status", {}))
                return {"success": True, "message": f"Connected! Found {cores} Solr cores."}
        except Exception as e:
            raise HTTPException(400, f"Connection failed: {str(e)}")

    raise HTTPException(400, "Unknown database type")

@router.post("/test-uri")
def test_uri(req: TestUriRequest, user=Depends(get_current_user)):
    if req.type != "mongodb":
        raise HTTPException(400, "Only MongoDB URI testing is supported")
    try:
        clean = sanitize_mongo_uri(req.uri)
        client = connect_mongo(clean)
        info = client.server_info()
        version = info.get("version", "unknown")
        client.close()
        return {"success": True, "message": f"Connected! MongoDB v{version}"}
    except Exception as e:
        raise HTTPException(400, f"Connection failed: {str(e)}")


@router.get("/{db_id}/databases")
def list_db_names(db_id: str, user=Depends(get_current_user)):
    """Fetch all databases from the server dynamically."""
    dbs = read_json("data/databases.json")
    db = next((d for d in dbs if d["id"] == db_id and d["user_id"] == user["sub"]), None)
    if not db:
        raise HTTPException(404, "Database not found")

    if db["type"] == "mongodb":
        try:
            uri = sanitize_mongo_uri(db.get("mongo_uri") or "")
            if not uri:
                uri = f"mongodb://{db['host']}:{db['port']}"
            client = connect_mongo(uri)
            db_names = [d for d in client.list_database_names()
                        if d not in ("local", "config")]
            client.close()
            return {"databases": db_names}
        except Exception as e:
            raise HTTPException(400, f"Failed to fetch databases: {str(e)}")

    elif db["type"] == "postgresql":
        try:
            import subprocess, os
            env = os.environ.copy()
            env["PGPASSWORD"] = db.get("password", "")
            result = subprocess.run(
                ["psql", f"--host={db['host']}", f"--port={db['port']}",
                 f"--username={db['username']}", "--no-password",
                 "--tuples-only", "--command",
                 "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname;"],
                capture_output=True, text=True, env=env, timeout=10
            )
            if result.returncode != 0:
                raise HTTPException(400, f"Failed: {result.stderr.strip()}")
            names = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
            return {"databases": names}
        except Exception as e:
            raise HTTPException(400, f"Failed to fetch databases: {str(e)}")
            
    elif db["type"] == "solr":
        try:
            import urllib.request
            import json
            import base64
            import ssl
            
            host = db["host"]
            solr_base_url = get_solr_base_url(host, db['port'])
                
            context = ssl._create_unverified_context()
            
            # 1. Try to fetch collections via the Collections API first (preferred for SolrCloud)
            collections_url = f"{solr_base_url}/solr/admin/collections?action=LIST&wt=json"
            req_coll = urllib.request.Request(collections_url)
            
            auth_str = None
            if db.get("username") and db.get("password"):
                auth_str = f"{db['username']}:{db['password']}"
                encoded_auth = base64.b64encode(auth_str.encode()).decode()
                req_coll.add_header("Authorization", f"Basic {encoded_auth}")
            
            try:
                with urllib.request.urlopen(req_coll, context=context, timeout=10) as response:
                    data = json.loads(response.read().decode())
                    if "collections" in data:
                        return {"databases": sorted(list(data["collections"]))}
            except Exception:
                # Fall back to Cores API if Collections API fails or is unsupported
                pass

            # 2. Fallback: query Cores API
            cores_url = f"{solr_base_url}/solr/admin/cores?action=STATUS&wt=json"
            req_cores = urllib.request.Request(cores_url)
            if auth_str:
                encoded_auth = base64.b64encode(auth_str.encode()).decode()
                req_cores.add_header("Authorization", f"Basic {encoded_auth}")
                
            with urllib.request.urlopen(req_cores, context=context, timeout=10) as response:
                data = json.loads(response.read().decode())
                cores_status = data.get("status", {})
                
                collections_set = set()
                for core_name, core_info in cores_status.items():
                    # Deduplicate/resolve back to collection name if cloud parameters exist
                    coll_name = core_info.get("cloud", {}).get("collection")
                    if coll_name:
                        collections_set.add(coll_name)
                    else:
                        collections_set.add(core_name)
                        
                return {"databases": sorted(list(collections_set))}
        except Exception as e:
            raise HTTPException(400, f"Failed to fetch Solr collections: {str(e)}")


@router.get("/{db_id}/collections")
def list_collections(db_id: str, database: str = "", user=Depends(get_current_user)):
    """Fetch all collections/tables for a given database."""
    dbs = read_json("data/databases.json")
    db = next((d for d in dbs if d["id"] == db_id and d["user_id"] == user["sub"]), None)
    if not db:
        raise HTTPException(404, "Database not found")

    target_db = database or db.get("database_name", "")

    if db["type"] == "mongodb":
        try:
            uri = sanitize_mongo_uri(db.get("mongo_uri") or "")
            if not uri:
                uri = f"mongodb://{db['host']}:{db['port']}"
            client = connect_mongo(uri)
            collections = client[target_db].list_collection_names()
            client.close()
            return {"collections": sorted(collections)}
        except Exception as e:
            raise HTTPException(400, f"Failed to fetch collections: {str(e)}")

    elif db["type"] == "postgresql":
        try:
            import subprocess, os
            env = os.environ.copy()
            env["PGPASSWORD"] = db.get("password", "")
            result = subprocess.run(
                ["psql", f"--host={db['host']}", f"--port={db['port']}",
                 f"--username={db['username']}", f"--dbname={target_db}",
                 "--no-password", "--tuples-only", "--command",
                 "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;"],
                capture_output=True, text=True, env=env, timeout=10
            )
            if result.returncode != 0:
                raise HTTPException(400, f"Failed: {result.stderr.strip()}")
            tables = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
            return {"collections": tables}
        except Exception as e:
            raise HTTPException(400, f"Failed to fetch tables: {str(e)}")
            
    elif db["type"] == "solr":
        try:
            import urllib.request
            import json
            import base64
            
            host = db["host"]
            solr_base_url = get_solr_base_url(host, db['port'])
                
            if not target_db or target_db == "default":
                return {"collections": []}
                
            url = f"{solr_base_url}/solr/{target_db}/schema/fields?wt=json"
            req = urllib.request.Request(url)
            if db.get("username") and db.get("password"):
                auth_str = f"{db['username']}:{db['password']}"
                encoded_auth = base64.b64encode(auth_str.encode()).decode()
                req.add_header("Authorization", f"Basic {encoded_auth}")
                
            import ssl
            context = ssl._create_unverified_context()
            with urllib.request.urlopen(req, context=context, timeout=10) as response:
                data = json.loads(response.read().decode())
                fields = [f["name"] for f in data.get("fields", [])]
                return {"collections": sorted(fields)}
        except Exception as e:
            raise HTTPException(400, f"Failed to fetch Solr schemas: {str(e)}")


@router.get("/{db_id}/test")
def test_connection(db_id: str, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    db = next((d for d in dbs if d["id"] == db_id and d["user_id"] == user["sub"]), None)
    if not db:
        raise HTTPException(404, "Database not found")

    if db["type"] == "mongodb":
        try:
            uri = sanitize_mongo_uri(db.get("mongo_uri") or "")
            if not uri:
                uri = f"mongodb://{db['host']}:{db['port']}"
            client = connect_mongo(uri)
            info = client.server_info()
            version = info.get("version", "?")
            db_list = [d for d in client.list_database_names() if d not in ("local","config")]
            # Auto-fetch indexes
            indexes = {}
            for db_name in db_list[:10]: # Limit to avoid timeouts
                try:
                    for coll in client[db_name].list_collection_names():
                        idx = client[db_name][coll].index_information()
                        if idx: indexes[f"{db_name}.{coll}"] = idx
                except:
                    pass
            client.close()
            
            # Save indexes to db record
            db["indexes"] = indexes
            write_json("data/databases.json", dbs)
            
            return {"success": True, "message": f"Connected to {db['name']} — MongoDB v{version} | {len(db_list)} databases found"}
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
                lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip()]
                version = lines[2] if len(lines) > 2 else "connected"
                
                # Auto-fetch indexes
                idx_res = subprocess.run(
                    ["psql", f"--host={db['host']}", f"--port={db['port']}",
                     f"--username={db['username']}", f"--dbname={db['database_name']}",
                     "--no-password", "--tuples-only", "-c", "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='public';"],
                    capture_output=True, text=True, env=env, timeout=10
                )
                if idx_res.returncode == 0:
                    db["indexes"] = [i.strip() for i in idx_res.stdout.strip().split('\n') if i.strip()]
                    write_json("data/databases.json", dbs)
                
                return {"success": True, "message": f"Connected to {db['name']} — {version[:80]}"}
            raise HTTPException(400, f"Connection failed: {result.stderr.strip()}")
        except Exception as e:
            raise HTTPException(400, f"Connection failed: {str(e)}")
            
    elif db["type"] == "solr":
        try:
            import urllib.request
            import json
            import base64
            
            host = db["host"]
            solr_base_url = get_solr_base_url(host, db['port'])
                
            url = f"{solr_base_url}/solr/admin/info/system?wt=json"
            req = urllib.request.Request(url)
            if db.get("username") and db.get("password"):
                auth_str = f"{db['username']}:{db['password']}"
                encoded_auth = base64.b64encode(auth_str.encode()).decode()
                req.add_header("Authorization", f"Basic {encoded_auth}")
                
            import ssl
            context = ssl._create_unverified_context()
            with urllib.request.urlopen(req, context=context, timeout=10) as response:
                data = json.loads(response.read().decode())
                version = data.get("lucene", {}).get("solr-spec-version", "unknown")
                return {"success": True, "message": f"Connected to {db['name']} — Apache Solr v{version}"}
        except Exception as e:
            raise HTTPException(400, f"Connection failed: {str(e)}")

    return {"success": True, "message": f"Connected to {db['name']}"}


@router.patch("/{db_id}")
def update_database(db_id: str, req: dict, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    for d in dbs:
        if d["id"] == db_id and d["user_id"] == user["sub"]:
            for k, v in req.items():
                if k not in ["id", "user_id", "type"]:
                    d[k] = v
            break
    write_json("data/databases.json", dbs)
    return {"message": "Updated"}


@router.delete("/{db_id}")
def delete_database(db_id: str, user=Depends(get_current_user)):
    dbs = read_json("data/databases.json")
    dbs = [d for d in dbs if not (d["id"] == db_id and d["user_id"] == user["sub"])]
    write_json("data/databases.json", dbs)
    return {"message": "Deleted"}