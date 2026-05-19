import subprocess
import os
import gzip
import shutil
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote, quote
from utils import read_json, write_json

BACKUP_TMP = "/tmp/backupvault"
os.makedirs(BACKUP_TMP, exist_ok=True)

# ─── STRUCTURED LOGGER ───────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [BACKUP] %(levelname)s %(message)s",
    level=logging.INFO
)
log = logging.getLogger("backupvault")


# ─── MONGO HELPERS ───────────────────────────────────────────────────────────

def sanitize_mongo_uri(uri: str) -> str:
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


def parse_mongo_uri(db: dict):
    """Return (uri_str, host, port, username, password, dbname, auth_source, auth_mechanism)"""
    raw_uri = db.get("mongo_uri") or ""
    if raw_uri:
        uri = sanitize_mongo_uri(raw_uri)
    else:
        host = db.get("host", "").strip()
        port = int(str(db.get("port", 27017)).strip())
        user = db.get("username", "")
        password = db.get("password", "")
        dbname = db.get("database_name", "")
        if user and password:
            uri = sanitize_mongo_uri(f"mongodb://{user}:{password}@{host}:{port}/{dbname}")
        else:
            uri = f"mongodb://{host}:{port}/{dbname}"

    parsed = urlparse(uri)
    query = parse_qs(parsed.query)
    return {
        "uri": uri,
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 27017,
        "username": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        # Use the database_name field from db record — NOT from URI path
        # This ensures user-selected DB is always used
        "dbname": db.get("database_name", "").strip() or (parsed.path.lstrip("/") if parsed.path.lstrip("/") else ""),
        "auth_source": query.get("authSource", ["admin"])[0],
        "auth_mechanism": query.get("authMechanism", [None])[0],
        "direct": query.get("directConnection", ["false"])[0].lower() == "true",
    }


def mongo_cmd_args(m: dict) -> list:
    """Build common mongodump/restore auth args from parsed URI dict."""
    args = [f"--host={m['host']}", f"--port={m['port']}",
            f"--authenticationDatabase={m['auth_source']}"]
    if m["username"]:
        args += [f"--username={m['username']}"]
    if m["password"]:
        args += [f"--password={m['password']}"]
    if m["auth_mechanism"]:
        args += [f"--authenticationMechanism={m['auth_mechanism']}"]
    return args


# ─── STORAGE ─────────────────────────────────────────────────────────────────

def upload_to_storage(storage: dict, local_path: str, remote_name: str) -> str:
    if storage["type"] == "azure":
        from azure.storage.blob import BlobServiceClient, ContentSettings
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage['azure_account_name']};"
            f"AccountKey={storage['azure_account_key']};"
            f"EndpointSuffix=core.windows.net"
        )
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(storage["azure_container"])
        # Stream upload in chunks — faster + less memory
        with open(local_path, "rb") as f:
            container.upload_blob(
                name=remote_name, data=f, overwrite=True,
                max_concurrency=4, length=os.path.getsize(local_path)
            )
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
        blob.upload_from_filename(local_path, num_retries=3)
        return f"gcs://{storage['gcs_bucket']}/{remote_name}"

    raise ValueError(f"Unknown storage type: {storage['type']}")


def download_from_storage(storage: dict, remote_name: str, local_path: str):
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
            stream = container.download_blob(remote_name, max_concurrency=4)
            stream.readinto(f)

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


# ─── MONGODB BACKUP / RESTORE ────────────────────────────────────────────────

def run_mongo_backup(db: dict, collection: str, tmp_dir: str):
    m = parse_mongo_uri(db)
    dbname = m["dbname"]

    log.info(f"MongoDB backup | host={m['host']}:{m['port']} db={dbname} collection={collection or 'full'}")

    if not dbname:
        raise ValueError("Database name is required. Please set database_name in the DB config.")

    dump_dir = os.path.join(tmp_dir, "dump")
    os.makedirs(dump_dir, exist_ok=True)

    cmd = ["mongodump"] + mongo_cmd_args(m) + [
        f"--db={dbname}",
        f"--out={dump_dir}",
        "--gzip",           # compress at dump time — faster overall
        "--numParallelCollections=4",  # parallel collection dump
    ]
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]

    log.info(f"Running: {' '.join(c for c in cmd if '--password' not in c)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    logs = (result.stderr or "") + (result.stdout or "")

    if result.returncode != 0:
        log.error(f"mongodump failed:\n{logs}")
        raise RuntimeError(f"mongodump failed: {logs}")

    log.info(f"mongodump done. Creating archive...")
    archive = os.path.join(tmp_dir, "backup.tar.gz")
    # Use faster compression level
    result2 = subprocess.run(
        ["tar", "-czf", archive, "-C", dump_dir, "."],
        capture_output=True, text=True
    )
    if result2.returncode != 0:
        raise RuntimeError(f"Archive creation failed: {result2.stderr}")

    log.info(f"Archive created: {archive} ({os.path.getsize(archive)/1024/1024:.2f} MB)")
    return archive, logs


def run_mongo_restore(db: dict, collection: str, archive_path: str, tmp_dir: str):
    m = parse_mongo_uri(db)
    dbname = m["dbname"]

    log.info(f"MongoDB restore | host={m['host']}:{m['port']} db={dbname}")

    restore_dir = os.path.join(tmp_dir, "restore")
    os.makedirs(restore_dir, exist_ok=True)

    # Extract archive
    subprocess.run(["tar", "-xzf", archive_path, "-C", restore_dir], check=True)

    cmd = ["mongorestore"] + mongo_cmd_args(m) + [
        "--drop",
        "--gzip",
        "--numParallelCollections=4",
        f"--db={dbname}",
        restore_dir,
    ]
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]

    log.info(f"Running mongorestore...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    logs = (result.stderr or "") + (result.stdout or "")

    if result.returncode != 0:
        log.error(f"mongorestore failed:\n{logs}")
        raise RuntimeError(f"mongorestore failed: {logs}")

    log.info("mongorestore completed successfully")
    return logs


# ─── POSTGRESQL BACKUP / RESTORE ─────────────────────────────────────────────

def pg_env(db: dict) -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = db.get("password", "")
    return env


def run_pg_backup(db: dict, collection: str, tmp_dir: str):
    dbname = db.get("database_name", "").strip()
    log.info(f"PostgreSQL backup | host={db['host']}:{db['port']} db={dbname} table={collection or 'full'}")

    dump_file = os.path.join(tmp_dir, "backup.sql.gz")
    cmd = [
        "pg_dump",
        f"--host={db['host']}",
        f"--port={db['port']}",
        f"--username={db['username']}",
        f"--dbname={dbname}",
        "--no-password",
        "--format=custom",       # custom format — faster + smaller
        "--compress=6",          # gzip level 6
        f"--file={dump_file}",
        "--no-acl",
        "--no-owner",
    ]
    if collection and collection != "full":
        cmd += [f"--table={collection}"]

    log.info(f"Running pg_dump...")
    result = subprocess.run(cmd, capture_output=True, text=True, env=pg_env(db), timeout=1800)
    logs = (result.stderr or "") + (result.stdout or "")

    if result.returncode != 0:
        log.error(f"pg_dump failed:\n{logs}")
        raise RuntimeError(f"pg_dump failed: {logs}")

    log.info(f"pg_dump done: {dump_file} ({os.path.getsize(dump_file)/1024/1024:.2f} MB)")
    return dump_file, logs


def run_pg_restore(db: dict, archive_path: str, tmp_dir: str):
    dbname = db.get("database_name", "").strip()
    log.info(f"PostgreSQL restore | host={db['host']}:{db['port']} db={dbname}")

    cmd = [
        "pg_restore",
        f"--host={db['host']}",
        f"--port={db['port']}",
        f"--username={db['username']}",
        f"--dbname={dbname}",
        "--no-password",
        "--clean",
        "--if-exists",
        "--no-acl",
        "--no-owner",
        "--jobs=4",    # parallel restore
        archive_path,
    ]
    log.info(f"Running pg_restore...")
    result = subprocess.run(cmd, capture_output=True, text=True, env=pg_env(db), timeout=1800)
    logs = (result.stderr or "") + (result.stdout or "")

    if result.returncode not in (0, 1):  # pg_restore exits 1 on warnings, ok
        log.error(f"pg_restore failed:\n{logs}")
        raise RuntimeError(f"pg_restore failed: {logs}")

    log.info("pg_restore completed")
    return logs


# ─── MAIN BACKUP TASK ────────────────────────────────────────────────────────

def update_backup(backup_id: str, **kwargs):
    backups = read_json("data/backups.json")
    for b in backups:
        if b["id"] == backup_id:
            b.update(kwargs)
            break
    write_json("data/backups.json", backups)


def do_backup(backup_id: str):
    start = datetime.utcnow()
    tmp_dir = os.path.join(BACKUP_TMP, backup_id)
    os.makedirs(tmp_dir, exist_ok=True)

    backups = read_json("data/backups.json")
    backup = next((b for b in backups if b["id"] == backup_id), None)
    if not backup:
        log.error(f"Backup {backup_id} not found")
        return

    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")
    db = next((d for d in dbs if d["id"] == backup["database_id"]), None)
    storage = next((s for s in storages if s["id"] == backup["storage_id"]), None)
    collection = backup.get("collection", "full")

    log.info(f"=== BACKUP START | id={backup_id} db={db and db['name']} collection={collection} ===")

    def fail(msg):
        log.error(f"=== BACKUP FAILED | id={backup_id} | {msg} ===")
        update_backup(backup_id,
            status="failed", error=msg,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=int((datetime.utcnow() - start).total_seconds())
        )
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not db or not storage:
            return fail("Database or storage not found")

        logs = ""
        if db["type"] == "mongodb":
            archive, logs = run_mongo_backup(db, collection, tmp_dir)
        elif db["type"] == "postgresql":
            archive, logs = run_pg_backup(db, collection, tmp_dir)
        else:
            return fail(f"Unsupported DB type: {db['type']}")

        size_mb = round(os.path.getsize(archive) / (1024 * 1024), 2)
        remote_name = f"{backup_id}/{os.path.basename(archive)}"

        log.info(f"Uploading {size_mb} MB to {storage['type']}...")
        remote_path = upload_to_storage(storage, archive, remote_name)

        duration = int((datetime.utcnow() - start).total_seconds())
        log.info(f"=== BACKUP COMPLETE | id={backup_id} size={size_mb}MB duration={duration}s ===")

        update_backup(backup_id,
            status="completed", size_mb=size_mb,
            remote_path=remote_path, remote_name=remote_name,
            logs=logs, error=None,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=duration
        )

    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── MAIN RESTORE TASK ───────────────────────────────────────────────────────

def update_restore(restore_id: str, **kwargs):
    restores = read_json("data/restores.json")
    for r in restores:
        if r["id"] == restore_id:
            r.update(kwargs)
            break
    write_json("data/restores.json", restores)


def do_restore(restore_id: str):
    start = datetime.utcnow()
    tmp_dir = os.path.join(BACKUP_TMP, f"restore_{restore_id}")
    os.makedirs(tmp_dir, exist_ok=True)

    restores = read_json("data/restores.json")
    restore = next((r for r in restores if r["id"] == restore_id), None)
    if not restore:
        log.error(f"Restore {restore_id} not found")
        return

    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")

    backup = next((b for b in backups if b["id"] == restore["backup_id"]), None)
    target_db = next((d for d in dbs if d["id"] == restore["target_database_id"]), None)

    log.info(f"=== RESTORE START | id={restore_id} target_db={target_db and target_db['name']} ===")

    def fail(msg):
        log.error(f"=== RESTORE FAILED | id={restore_id} | {msg} ===")
        update_restore(restore_id,
            status="failed", error=msg,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=int((datetime.utcnow() - start).total_seconds())
        )
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not backup or not target_db:
            return fail("Backup or target database not found")

        storage = next((s for s in storages if s["id"] == backup["storage_id"]), None)
        if not storage:
            return fail("Storage not found")

        remote_name = backup.get("remote_name")
        if not remote_name:
            return fail("Backup has no remote file — was it actually completed with the new engine?")

        local_archive = os.path.join(tmp_dir, os.path.basename(remote_name))
        log.info(f"Downloading backup from {storage['type']}...")
        download_from_storage(storage, remote_name, local_archive)

        collection = backup.get("collection", "full")
        logs = ""

        if target_db["type"] == "mongodb":
            logs = run_mongo_restore(target_db, collection, local_archive, tmp_dir)
        elif target_db["type"] == "postgresql":
            logs = run_pg_restore(target_db, local_archive, tmp_dir)
        else:
            return fail(f"Unsupported DB type: {target_db['type']}")

        duration = int((datetime.utcnow() - start).total_seconds())
        log.info(f"=== RESTORE COMPLETE | id={restore_id} duration={duration}s ===")

        update_restore(restore_id,
            status="completed", logs=logs, error=None,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=duration
        )

    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)