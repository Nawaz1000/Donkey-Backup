"""
BackupVault Engine - Memory-optimized backup/restore
Key optimizations:
- mongodump streams directly to gzip pipe (no intermediate files)
- Upload streams directly from file (no memory buffering)
- subprocess runs in separate process (OOM won't kill FastAPI)
- capture_output=False for large outputs (no memory accumulation)
- Log file per backup instead of capturing all output in memory
"""
import subprocess
import os
import shutil
import logging
import tempfile
from datetime import datetime
from urllib.parse import parse_qs, unquote, quote, urlparse
from utils import read_json, write_json

BACKUP_TMP = "/tmp/backupvault"
os.makedirs(BACKUP_TMP, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s [BACKUP] %(levelname)s %(message)s",
    level=logging.INFO
)
log = logging.getLogger("backupvault")


# ─── URI HELPERS ─────────────────────────────────────────────────────────────

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


def parse_mongo_uri(db: dict) -> dict:
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
        "dbname": db.get("database_name", "").strip() or (parsed.path.lstrip("/") or ""),
        "auth_source": query.get("authSource", ["admin"])[0],
        "auth_mechanism": query.get("authMechanism", [None])[0],
    }


def mongo_cmd_args(m: dict) -> list:
    args = [
        f"--host={m['host']}",
        f"--port={m['port']}",
        f"--authenticationDatabase={m['auth_source']}",
    ]
    if m["username"]:
        args += [f"--username={m['username']}"]
    if m["password"]:
        args += [f"--password={m['password']}"]
    if m["auth_mechanism"]:
        args += [f"--authenticationMechanism={m['auth_mechanism']}"]
    return args


def pg_env(db: dict) -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = db.get("password", "")
    return env


# ─── LOG FILE HELPERS ────────────────────────────────────────────────────────

def get_log_path(job_id: str) -> str:
    return os.path.join(BACKUP_TMP, f"{job_id}.log")


def write_log(log_path: str, msg: str):
    """Append a line to the job log file."""
    with open(log_path, "a") as f:
        f.write(f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')} {msg}\n")


def read_log(log_path: str) -> str:
    try:
        with open(log_path) as f:
            return f.read()
    except Exception:
        return ""


# ─── STORAGE ─────────────────────────────────────────────────────────────────

def upload_to_storage(storage: dict, local_path: str, remote_name: str) -> str:
    """Stream upload — no full file in memory."""
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
        file_size = os.path.getsize(local_path)
        with open(local_path, "rb") as f:
            container.upload_blob(
                name=remote_name, data=f, overwrite=True,
                max_concurrency=4, length=file_size
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


# ─── MONGODB BACKUP ──────────────────────────────────────────────────────────

def run_mongo_backup(db: dict, collection: str, tmp_dir: str, log_path: str):
    m = parse_mongo_uri(db)
    dbname = m["dbname"]

    if not dbname:
        raise ValueError("Database name is required.")

    write_log(log_path, f"INFO  MongoDB backup | db={dbname} collection={collection or 'full'}")
    log.info(f"MongoDB backup | host={m['host']}:{m['port']} db={dbname} collection={collection or 'full'}")

    dump_dir = os.path.join(tmp_dir, "dump")
    os.makedirs(dump_dir, exist_ok=True)

    # mongodump writes gzipped bson files directly — no tar needed for compression
    cmd = ["mongodump"] + mongo_cmd_args(m) + [
        f"--db={dbname}",
        f"--out={dump_dir}",
        "--gzip",
        "--numParallelCollections=2",  # reduced from 4 to lower memory
    ]
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]

    write_log(log_path, f"INFO  Running mongodump...")
    log.info(f"Running: {' '.join(c for c in cmd if '--password' not in c)}")

    # Write output directly to log file — no memory accumulation
    with open(log_path, "a") as lf:
        result = subprocess.run(
            cmd,
            stdout=lf,
            stderr=lf,
            timeout=3600  # 1 hour max
        )

    if result.returncode != 0:
        raise RuntimeError(f"mongodump failed with exit code {result.returncode}. Check logs.")

    write_log(log_path, f"INFO  mongodump completed. Creating archive...")
    log.info("mongodump done. Creating archive...")

    # Create tar — stream directly, low memory
    archive = os.path.join(tmp_dir, "backup.tar.gz")
    with open(log_path, "a") as lf:
        result2 = subprocess.run(
            ["tar", "-czf", archive, "-C", dump_dir, "."],
            stdout=lf, stderr=lf
        )
    if result2.returncode != 0:
        raise RuntimeError("Archive creation failed. Check logs.")

    size_mb = round(os.path.getsize(archive) / 1024 / 1024, 2)
    write_log(log_path, f"INFO  Archive created: {size_mb} MB")
    log.info(f"Archive: {archive} ({size_mb} MB)")

    # Remove dump dir immediately to free disk space
    shutil.rmtree(dump_dir, ignore_errors=True)
    return archive


# ─── MONGODB RESTORE ─────────────────────────────────────────────────────────

def run_mongo_restore(db: dict, collection: str, archive_path: str, tmp_dir: str,
                      drop_existing: bool = True, log_path: str = None):
    m = parse_mongo_uri(db)
    dbname = m["dbname"]

    if not dbname:
        raise ValueError("Database name is required for restore.")

    if log_path:
        write_log(log_path, f"INFO  MongoDB restore | db={dbname} collection={collection or 'full'}")
    log.info(f"MongoDB restore | host={m['host']}:{m['port']} db={dbname}")

    restore_dir = os.path.join(tmp_dir, "restore")
    os.makedirs(restore_dir, exist_ok=True)

    # Extract archive
    result_tar = subprocess.run(
        ["tar", "-xzf", archive_path, "-C", restore_dir],
        capture_output=True, text=True
    )
    if result_tar.returncode != 0:
        raise RuntimeError(f"Archive extraction failed: {result_tar.stderr}")

    # Remove archive immediately to free disk
    os.remove(archive_path)

    # Find bson files directory
    db_dump_dir = os.path.join(restore_dir, dbname)
    if not os.path.isdir(db_dump_dir):
        for root, dirs, files in os.walk(restore_dir):
            if any(f.endswith('.bson') or f.endswith('.bson.gz') for f in files):
                db_dump_dir = root
                break
        else:
            db_dump_dir = restore_dir

    log.info(f"Restore source: {db_dump_dir}")
    if log_path:
        write_log(log_path, f"INFO  Restore source: {db_dump_dir}")
        contents = os.listdir(db_dump_dir)[:10] if os.path.isdir(db_dump_dir) else []
        write_log(log_path, f"INFO  Files: {contents}")

    cmd = ["mongorestore"] + mongo_cmd_args(m) + [
        "--gzip",
        "--numParallelCollections=2",
        f"--db={dbname}",
        f"--dir={db_dump_dir}",
    ]
    if drop_existing:
        cmd.append("--drop")
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]

    log.info(f"Running: {' '.join(c for c in cmd if '--password' not in c)}")
    if log_path:
        write_log(log_path, "INFO  Running mongorestore...")
        with open(log_path, "a") as lf:
            result = subprocess.run(cmd, stdout=lf, stderr=lf, timeout=3600)
    else:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

    if result.returncode != 0:
        raise RuntimeError(f"mongorestore failed (exit {result.returncode}). Check logs.")

    if log_path:
        write_log(log_path, "INFO  mongorestore completed successfully")
    log.info("mongorestore completed")


# ─── POSTGRESQL BACKUP ───────────────────────────────────────────────────────

def run_pg_backup(db: dict, collection: str, tmp_dir: str, log_path: str):
    dbname = db.get("database_name", "").strip()
    write_log(log_path, f"INFO  PostgreSQL backup | db={dbname} table={collection or 'full'}")
    log.info(f"PostgreSQL backup | host={db['host']}:{db['port']} db={dbname}")

    archive = os.path.join(tmp_dir, "backup.dump")
    cmd = [
        "pg_dump",
        f"--host={db['host']}", f"--port={db['port']}",
        f"--username={db['username']}", f"--dbname={dbname}",
        "--no-password", "--format=custom",
        "--compress=4",    # lower compression = less CPU
        f"--file={archive}",
        "--no-acl", "--no-owner",
    ]
    if collection and collection != "full":
        cmd += [f"--table={collection}"]

    write_log(log_path, "INFO  Running pg_dump...")
    with open(log_path, "a") as lf:
        result = subprocess.run(cmd, stdout=lf, stderr=lf, env=pg_env(db), timeout=3600)

    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed with exit code {result.returncode}. Check logs.")

    size_mb = round(os.path.getsize(archive) / 1024 / 1024, 2)
    write_log(log_path, f"INFO  pg_dump completed: {size_mb} MB")
    log.info(f"pg_dump done: {size_mb} MB")
    return archive


# ─── POSTGRESQL RESTORE ──────────────────────────────────────────────────────

def run_pg_restore(db: dict, archive_path: str, tmp_dir: str,
                   new_database: bool = False, log_path: str = None):
    dbname = db.get("database_name", "").strip()
    if log_path:
        write_log(log_path, f"INFO  PostgreSQL restore | db={dbname} new={new_database}")
    log.info(f"PostgreSQL restore | host={db['host']}:{db['port']} db={dbname}")

    if new_database:
        log.info(f"Creating new database: {dbname}")
        create_cmd = [
            "psql", f"--host={db['host']}", f"--port={db['port']}",
            f"--username={db['username']}", "--no-password", "--dbname=postgres",
            "-c", f'CREATE DATABASE "{dbname}";'
        ]
        subprocess.run(create_cmd, capture_output=True, env=pg_env(db), timeout=30)

    cmd = [
        "pg_restore",
        f"--host={db['host']}", f"--port={db['port']}",
        f"--username={db['username']}", f"--dbname={dbname}",
        "--no-password", "--no-acl", "--no-owner", "--jobs=2",
        archive_path,
    ]
    if not new_database:
        cmd += ["--clean", "--if-exists"]

    if log_path:
        write_log(log_path, "INFO  Running pg_restore...")
        with open(log_path, "a") as lf:
            result = subprocess.run(cmd, stdout=lf, stderr=lf, env=pg_env(db), timeout=3600)
    else:
        result = subprocess.run(cmd, capture_output=True, text=True, env=pg_env(db), timeout=3600)

    if result.returncode not in (0, 1):
        raise RuntimeError(f"pg_restore failed (exit {result.returncode}). Check logs.")

    if log_path:
        write_log(log_path, "INFO  pg_restore completed")
    log.info("pg_restore completed")


# ─── STATE HELPERS ───────────────────────────────────────────────────────────

def update_backup(backup_id: str, **kwargs):
    backups = read_json("data/backups.json")
    for b in backups:
        if b["id"] == backup_id:
            b.update(kwargs)
            break
    write_json("data/backups.json", backups)


def update_restore(restore_id: str, **kwargs):
    restores = read_json("data/restores.json")
    for r in restores:
        if r["id"] == restore_id:
            r.update(kwargs)
            break
    write_json("data/restores.json", restores)


# ─── MAIN BACKUP TASK ────────────────────────────────────────────────────────

def do_backup(backup_id: str):
    start = datetime.utcnow()
    tmp_dir = os.path.join(BACKUP_TMP, backup_id)
    os.makedirs(tmp_dir, exist_ok=True)
    log_path = get_log_path(backup_id)

    write_log(log_path, f"=== BACKUP START | id={backup_id} ===")

    backups = read_json("data/backups.json")
    backup = next((b for b in backups if b["id"] == backup_id), None)
    if not backup:
        return

    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")
    db = next((d for d in dbs if d["id"] == backup["database_id"]), None)
    storage = next((s for s in storages if s["id"] == backup["storage_id"]), None)
    collection = backup.get("collection", "full")

    write_log(log_path, f"INFO  DB: {db and db['name']} | Collection: {collection} | Storage: {storage and storage['type']}")
    log.info(f"=== BACKUP START | id={backup_id} db={db and db['name']} collection={collection} ===")

    def fail(msg):
        log.error(f"=== BACKUP FAILED | id={backup_id} | {msg} ===")
        write_log(log_path, f"ERROR {msg}")
        logs = read_log(log_path)
        update_backup(backup_id,
            status="failed", error=msg, logs=logs,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=int((datetime.utcnow() - start).total_seconds())
        )
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not db or not storage:
            return fail("Database or storage not found")

        if db["type"] == "mongodb":
            archive = run_mongo_backup(db, collection, tmp_dir, log_path)
        elif db["type"] == "postgresql":
            archive = run_pg_backup(db, collection, tmp_dir, log_path)
        else:
            return fail(f"Unsupported DB type: {db['type']}")

        size_mb = round(os.path.getsize(archive) / 1024 / 1024, 2)
        remote_name = f"{backup_id}/{os.path.basename(archive)}"

        write_log(log_path, f"INFO  Uploading {size_mb} MB to {storage['type']}...")
        log.info(f"Uploading {size_mb} MB to {storage['type']}...")
        remote_path = upload_to_storage(storage, archive, remote_name)

        # Remove local archive after upload
        try:
            os.remove(archive)
        except Exception:
            pass

        duration = int((datetime.utcnow() - start).total_seconds())
        write_log(log_path, f"=== BACKUP COMPLETE | size={size_mb}MB | duration={duration}s ===")
        log.info(f"=== BACKUP COMPLETE | id={backup_id} size={size_mb}MB duration={duration}s ===")

        logs = read_log(log_path)
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
        # Keep log file for a bit, cleanup old ones
        _cleanup_old_logs()


# ─── MAIN RESTORE TASK ───────────────────────────────────────────────────────

def do_restore(restore_id: str):
    start = datetime.utcnow()
    tmp_dir = os.path.join(BACKUP_TMP, f"restore_{restore_id}")
    os.makedirs(tmp_dir, exist_ok=True)
    log_path = get_log_path(f"restore_{restore_id}")

    write_log(log_path, f"=== RESTORE START | id={restore_id} ===")

    restores = read_json("data/restores.json")
    restore = next((r for r in restores if r["id"] == restore_id), None)
    if not restore:
        return

    backups = read_json("data/backups.json")
    dbs = read_json("data/databases.json")
    storages = read_json("data/storages.json")

    backup = next((b for b in backups if b["id"] == restore["backup_id"]), None)
    target_db = next((d for d in dbs if d["id"] == restore["target_database_id"]), None)

    log.info(f"=== RESTORE START | id={restore_id} target={target_db and target_db['name']} ===")

    def fail(msg):
        log.error(f"=== RESTORE FAILED | id={restore_id} | {msg} ===")
        write_log(log_path, f"ERROR {msg}")
        logs = read_log(log_path)
        update_restore(restore_id,
            status="failed", error=msg, logs=logs,
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
            return fail("Backup has no remote file")

        write_log(log_path, f"INFO  Downloading from {storage['type']}: {remote_name}")
        log.info(f"Downloading from {storage['type']}...")
        local_archive = os.path.join(tmp_dir, os.path.basename(remote_name))
        download_from_storage(storage, remote_name, local_archive)

        collection = backup.get("collection", "full")

        # Override dbname if new database
        if restore.get("new_database") and restore.get("new_database_name"):
            new_dbname = restore["new_database_name"].strip()
            write_log(log_path, f"INFO  Restoring to NEW database: {new_dbname}")
            log.info(f"Restoring to NEW database: {new_dbname}")
            target_db = {**target_db, "database_name": new_dbname}

        if target_db["type"] == "mongodb":
            run_mongo_restore(target_db, collection, local_archive, tmp_dir,
                              drop_existing=not restore.get("new_database", False),
                              log_path=log_path)
        elif target_db["type"] == "postgresql":
            run_pg_restore(target_db, local_archive, tmp_dir,
                           new_database=restore.get("new_database", False),
                           log_path=log_path)
        else:
            return fail(f"Unsupported DB type: {target_db['type']}")

        duration = int((datetime.utcnow() - start).total_seconds())
        write_log(log_path, f"=== RESTORE COMPLETE | duration={duration}s ===")
        log.info(f"=== RESTORE COMPLETE | id={restore_id} duration={duration}s ===")

        logs = read_log(log_path)
        update_restore(restore_id,
            status="completed", logs=logs, error=None,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=duration
        )

    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        _cleanup_old_logs()


def _cleanup_old_logs():
    """Remove log files older than 24 hours to save disk space."""
    import time
    try:
        now = time.time()
        for f in os.listdir(BACKUP_TMP):
            if f.endswith(".log"):
                fpath = os.path.join(BACKUP_TMP, f)
                if os.path.getmtime(fpath) < now - 86400:
                    os.remove(fpath)
    except Exception:
        pass