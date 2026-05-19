import subprocess
import os
import tempfile
import gzip
import shutil
from datetime import datetime
from utils import read_json, write_json
 
BACKUP_TMP = "/tmp/backupvault"
os.makedirs(BACKUP_TMP, exist_ok=True)
 
 
# ─── STORAGE UPLOAD / DOWNLOAD ────────────────────────────────────────────────
 
def upload_to_storage(storage: dict, local_path: str, remote_name: str) -> str:
    """Upload file to Azure or GCS. Returns remote path."""
    if storage["type"] == "azure":
        from azure.storage.blob import BlobServiceClient
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage['azure_account_name']};"
            f"AccountKey={storage['azure_account_key']};"
            f"EndpointSuffix=core.windows.net"
        )
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(storage["azure_container"])
        with open(local_path, "rb") as f:
            container.upload_blob(name=remote_name, data=f, overwrite=True)
        return f"azure://{storage['azure_container']}/{remote_name}"
 
    elif storage["type"] == "gcs":
        import json
        from google.cloud import storage as gcs
        from google.oauth2 import service_account
        creds_dict = json.loads(storage["gcs_credentials_json"])
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        client = gcs.Client(credentials=creds)
        bucket = client.bucket(storage["gcs_bucket"])
        blob = bucket.blob(remote_name)
        blob.upload_from_filename(local_path)
        return f"gcs://{storage['gcs_bucket']}/{remote_name}"
 
    raise ValueError(f"Unknown storage type: {storage['type']}")
 
 
def download_from_storage(storage: dict, remote_name: str, local_path: str):
    """Download file from Azure or GCS to local_path."""
    if storage["type"] == "azure":
        from azure.storage.blob import BlobServiceClient
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage['azure_account_name']};"
            f"AccountKey={storage['azure_account_key']};"
            f"EndpointSuffix=core.windows.net"
        )
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(storage["azure_container"])
        with open(local_path, "wb") as f:
            data = container.download_blob(remote_name)
            data.readinto(f)
 
    elif storage["type"] == "gcs":
        import json
        from google.cloud import storage as gcs
        from google.oauth2 import service_account
        creds_dict = json.loads(storage["gcs_credentials_json"])
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        client = gcs.Client(credentials=creds)
        bucket = client.bucket(storage["gcs_bucket"])
        blob = bucket.blob(remote_name)
        blob.download_to_filename(local_path)
    else:
        raise ValueError(f"Unknown storage type: {storage['type']}")
 
 
# ─── MONGODB ──────────────────────────────────────────────────────────────────
 
def mongo_uri(db: dict) -> str:
    # Prefer stored URI if available
    if db.get("mongo_uri"):
        return db["mongo_uri"]
 
    host = db.get('host', '').strip()
    port = db.get('port', '')
    user = db.get('username', '')
    password = db.get('password', '')
    dbname = db.get('database_name', '')
 
    if not host:
        raise ValueError("Database host is empty")
    if not port or str(port).strip() == '':
        raise ValueError("Database port is empty or invalid")
 
    port = int(str(port).strip())
    if port < 1 or port > 65535:
        raise ValueError(f"Database port {port} is out of range [1, 65535]")
 
    if user and password:
        return f"mongodb://{user}:{password}@{host}:{port}/{dbname}"
    return f"mongodb://{host}:{port}/{dbname}"
 
def run_mongo_backup(db: dict, collection: str, tmp_dir: str) -> str:
    """Run mongodump and return path to .gz file."""
    dump_dir = os.path.join(tmp_dir, "dump")
    cmd = [
        "mongodump",
        f"--uri={mongo_uri(db)}",
        f"--out={dump_dir}",
    ]
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]
 
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"mongodump failed: {result.stderr}")
 
    # Compress dump dir to .tar.gz
    archive = os.path.join(tmp_dir, "backup.tar.gz")
    shutil.make_archive(archive.replace(".tar.gz", ""), "gztar", dump_dir)
    return archive
 
def run_mongo_restore(db: dict, collection: str, archive_path: str, tmp_dir: str):
    """Extract archive and run mongorestore."""
    restore_dir = os.path.join(tmp_dir, "restore")
    os.makedirs(restore_dir, exist_ok=True)
    shutil.unpack_archive(archive_path, restore_dir, "gztar")
 
    cmd = [
        "mongorestore",
        f"--uri={mongo_uri(db)}",
        "--drop",  # drop existing before restore
        restore_dir,
    ]
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]
 
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"mongorestore failed: {result.stderr}")
 
 
# ─── POSTGRESQL ───────────────────────────────────────────────────────────────
 
def pg_env(db: dict) -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = db["password"]
    return env
 
def run_pg_backup(db: dict, collection: str, tmp_dir: str) -> str:
    """Run pg_dump and return path to .gz file."""
    dump_file = os.path.join(tmp_dir, "backup.sql")
    cmd = [
        "pg_dump",
        f"--host={db['host']}",
        f"--port={db['port']}",
        f"--username={db['username']}",
        f"--dbname={db['database_name']}",
        "--no-password",
        f"--file={dump_file}",
    ]
    if collection and collection != "full":
        cmd += [f"--table={collection}"]
 
    result = subprocess.run(cmd, capture_output=True, text=True, env=pg_env(db))
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr}")
 
    # Gzip the dump
    gz_file = dump_file + ".gz"
    with open(dump_file, "rb") as f_in, gzip.open(gz_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(dump_file)
    return gz_file
 
def run_pg_restore(db: dict, archive_path: str, tmp_dir: str):
    """Decompress and run psql to restore."""
    dump_file = os.path.join(tmp_dir, "restore.sql")
    with gzip.open(archive_path, "rb") as f_in, open(dump_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
 
    cmd = [
        "psql",
        f"--host={db['host']}",
        f"--port={db['port']}",
        f"--username={db['username']}",
        f"--dbname={db['database_name']}",
        "--no-password",
        f"--file={dump_file}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=pg_env(db))
    if result.returncode != 0:
        raise RuntimeError(f"psql restore failed: {result.stderr}")
 
 
# ─── MAIN BACKUP TASK ────────────────────────────────────────────────────────
 
def do_backup(backup_id: str):
    backups = read_json("data/backups.json")
    backup = next((b for b in backups if b["id"] == backup_id), None)
    if not backup:
        return
 
    start = datetime.utcnow()
    tmp_dir = os.path.join(BACKUP_TMP, backup_id)
    os.makedirs(tmp_dir, exist_ok=True)
 
    def fail(msg):
        for b in backups:
            if b["id"] == backup_id:
                b["status"] = "failed"
                b["error"] = msg
                b["completed_at"] = datetime.utcnow().isoformat()
                break
        write_json("data/backups.json", backups)
        shutil.rmtree(tmp_dir, ignore_errors=True)
 
    try:
        dbs = read_json("data/databases.json")
        storages = read_json("data/storages.json")
        db = next((d for d in dbs if d["id"] == backup["database_id"]), None)
        storage = next((s for s in storages if s["id"] == backup["storage_id"]), None)
        if not db or not storage:
            return fail("Database or storage not found")
 
        collection = backup.get("collection", "full")
 
        # Run actual backup
        if db["type"] == "mongodb":
            archive = run_mongo_backup(db, collection, tmp_dir)
        elif db["type"] == "postgresql":
            archive = run_pg_backup(db, collection, tmp_dir)
        else:
            return fail(f"Unsupported DB type: {db['type']}")
 
        # Upload to storage
        size_mb = round(os.path.getsize(archive) / (1024 * 1024), 2)
        remote_name = f"{backup_id}/{os.path.basename(archive)}"
        remote_path = upload_to_storage(storage, archive, remote_name)
 
        # Update backup record
        duration = int((datetime.utcnow() - start).total_seconds())
        for b in backups:
            if b["id"] == backup_id:
                b["status"] = "completed"
                b["size_mb"] = size_mb
                b["remote_path"] = remote_path
                b["remote_name"] = remote_name
                b["completed_at"] = datetime.utcnow().isoformat()
                b["duration_seconds"] = duration
                break
        write_json("data/backups.json", backups)
 
    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
 
 
# ─── MAIN RESTORE TASK ───────────────────────────────────────────────────────
 
def do_restore(restore_id: str):
    restores = read_json("data/restores.json")
    restore = next((r for r in restores if r["id"] == restore_id), None)
    if not restore:
        return
 
    start = datetime.utcnow()
    tmp_dir = os.path.join(BACKUP_TMP, f"restore_{restore_id}")
    os.makedirs(tmp_dir, exist_ok=True)
 
    def fail(msg):
        for r in restores:
            if r["id"] == restore_id:
                r["status"] = "failed"
                r["error"] = msg
                r["completed_at"] = datetime.utcnow().isoformat()
                break
        write_json("data/restores.json", restores)
        shutil.rmtree(tmp_dir, ignore_errors=True)
 
    try:
        backups = read_json("data/backups.json")
        dbs = read_json("data/databases.json")
        storages = read_json("data/storages.json")
 
        backup = next((b for b in backups if b["id"] == restore["backup_id"]), None)
        target_db = next((d for d in dbs if d["id"] == restore["target_database_id"]), None)
        if not backup or not target_db:
            return fail("Backup or target database not found")
 
        storage = next((s for s in storages if s["id"] == backup["storage_id"]), None)
        if not storage:
            return fail("Storage not found")
 
        # Download backup file from storage
        remote_name = backup.get("remote_name")
        if not remote_name:
            return fail("Backup has no remote file — was it actually completed?")
 
        local_archive = os.path.join(tmp_dir, os.path.basename(remote_name))
        download_from_storage(storage, remote_name, local_archive)
 
        collection = backup.get("collection", "full")
 
        # Run actual restore
        if target_db["type"] == "mongodb":
            run_mongo_restore(target_db, collection, local_archive, tmp_dir)
        elif target_db["type"] == "postgresql":
            run_pg_restore(target_db, local_archive, tmp_dir)
        else:
            return fail(f"Unsupported DB type: {target_db['type']}")
 
        # Update restore record
        duration = int((datetime.utcnow() - start).total_seconds())
        for r in restores:
            if r["id"] == restore_id:
                r["status"] = "completed"
                r["completed_at"] = datetime.utcnow().isoformat()
                r["duration_seconds"] = duration
                break
        write_json("data/restores.json", restores)
 
    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)