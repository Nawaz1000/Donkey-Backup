"""
BackupVault Engine - Enterprise Direct Stream Backup
Key optimizations:
- mongodump and pg_dump stream directly to Azure/GCS (Zero Local Disk IO)
- No intermediate files, no disk space used on container
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



# ─── STORAGE STREAMING ───────────────────────────────────────────────────────

import io
import queue
import threading
import shutil

from collections import deque

class QueueReader(io.RawIOBase):
    def __init__(self, q: queue.Queue, stop_event: threading.Event):
        self.q = q
        self.stop_event = stop_event
        self.buffer = deque()
        self.buffer_size = 0
        self.eof = False
        self.lock = threading.Lock()

    def readable(self):
        return True

    def seekable(self):
        return False

    def readinto(self, b):
        with self.lock:
            size = len(b)
            
            # 1. Block and wait for at least some data if buffer is empty
            if self.buffer_size == 0 and not self.eof:
                while not self.eof:
                    try:
                        chunk = self.q.get(timeout=1.0)
                        if chunk is None:
                            self.eof = True
                            break
                        self.buffer.append(chunk)
                        self.buffer_size += len(chunk)
                        break
                    except queue.Empty:
                        if self.stop_event.is_set():
                            self.eof = True
                            break

            # 2. Drain any available chunks without blocking to fill up to size
            if self.buffer_size > 0 and self.buffer_size < size and not self.eof:
                while self.buffer_size < size:
                    try:
                        chunk = self.q.get_nowait()
                        if chunk is None:
                            self.eof = True
                            break
                        self.buffer.append(chunk)
                        self.buffer_size += len(chunk)
                    except queue.Empty:
                        break

            if self.buffer_size == 0:
                return 0  # EOF

            # 3. Copy from self.buffer to b
            bytes_to_copy = min(size, self.buffer_size)
            copied = 0
            
            while copied < bytes_to_copy:
                chunk = self.buffer.popleft()
                chunk_len = len(chunk)
                remaining = bytes_to_copy - copied
                
                if chunk_len <= remaining:
                    b[copied:copied+chunk_len] = chunk
                    copied += chunk_len
                    self.buffer_size -= chunk_len
                else:
                    b[copied:copied+remaining] = chunk[:remaining]
                    self.buffer.appendleft(chunk[remaining:])
                    self.buffer_size -= remaining
                    copied += remaining
                    
            return copied

    def read(self, size=-1):
        if size is None or size < 0:
            raise ValueError("Reading the entire stream without a size limit is not supported to prevent OOM errors.")
        b = bytearray(size)
        n = self.readinto(b)
        if n == 0:
            return b""
        return bytes(b[:n])


def reader_thread_fn(stream, q: queue.Queue, stop_event: threading.Event, chunk_size: int = 1024 * 1024):
    try:
        while not stop_event.is_set():
            data = stream.read(chunk_size)
            if not data:
                break
            while not stop_event.is_set():
                try:
                    q.put(data, timeout=1.0)
                    break
                except queue.Full:
                    continue
    except Exception as e:
        print(f"Error in reader thread: {e}")
    finally:
        q.put(None)


def get_compressor_info(log_path: str = None) -> dict:
    if shutil.which("zstd"):
        if log_path:
            write_log(log_path, "INFO  Using Zstandard (zstd) for multi-threaded fast compression.")
        return {
            "cmd": ["zstd", "-1", "--threads=0"],
            "ext": "zst",
            "type": "zstd"
        }
    elif shutil.which("pigz"):
        if log_path:
            write_log(log_path, "INFO  Using pigz (parallel gzip) for multi-threaded compression.")
        return {
            "cmd": ["pigz", "-1"],
            "ext": "gz",
            "type": "pigz"
        }
    elif shutil.which("gzip"):
        if log_path:
            write_log(log_path, "INFO  Using system gzip for fast compression level 1.")
        return {
            "cmd": ["gzip", "-1"],
            "ext": "gz",
            "type": "gzip"
        }
    return None


def stream_backup_to_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, compression_cmd: list = None) -> str:
    write_log(log_path, f"INFO  Starting streaming backup to {storage['type']}: {remote_name}")
    
    processes = []
    with open(log_path, "a") as err_file:
        try:
            if compression_cmd:
                p_dump = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=err_file,
                    env=env
                )
                processes.append(p_dump)
                
                p_comp = subprocess.Popen(
                    compression_cmd,
                    stdin=p_dump.stdout,
                    stdout=subprocess.PIPE,
                    stderr=err_file
                )
                processes.append(p_comp)
                
                p_dump.stdout.close()
                stdout_stream = p_comp.stdout
            else:
                p_dump = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=err_file,
                    env=env
                )
                processes.append(p_dump)
                stdout_stream = p_dump.stdout
                
        except Exception as e:
            for p in processes:
                try:
                    p.kill()
                except Exception:
                    pass
            raise RuntimeError(f"Failed to start subprocesses: {e}")

        # Scale chunk size and queue capacity for higher throughput (1MB chunk, 32 capacity = 32MB buffer)
        q = queue.Queue(maxsize=32)
        stop_event = threading.Event()
        
        reader_thread = threading.Thread(
            target=reader_thread_fn,
            args=(stdout_stream, q, stop_event, 1024 * 1024)  # 1MB chunks
        )
        reader_thread.daemon = True
        reader_thread.start()
        
        queue_reader = QueueReader(q, stop_event)

        try:
            if storage["type"] == "azure":
                from azure.storage.blob import BlobServiceClient
                conn_str = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={storage['azure_account_name']};"
                    f"AccountKey={storage['azure_account_key']};"
                    f"EndpointSuffix=core.windows.net"
                )
                client = BlobServiceClient.from_connection_string(conn_str, max_block_size=4 * 1024 * 1024)
                container = client.get_container_client(storage["azure_container"])
                container.upload_blob(
                    name=remote_name, 
                    data=queue_reader, 
                    overwrite=True,
                    max_concurrency=4
                )
                remote_path = f"azure://{storage['azure_container']}/{remote_name}"
            
            elif storage["type"] == "gcs":
                import json
                from google.cloud import storage as gcs
                from google.oauth2 import service_account
                creds_dict = json.loads(storage["gcs_credentials_json"])
                creds = service_account.Credentials.from_service_account_info(creds_dict)
                client = gcs.Client(credentials=creds)
                bucket = client.bucket(storage["gcs_bucket"])
                blob = bucket.blob(remote_name)
                blob.chunk_size = 16 * 1024 * 1024  # 16MB chunk size to force resumable upload and speed up uploads
                blob.upload_from_file(queue_reader, num_retries=3)
                remote_path = f"gcs://{storage['gcs_bucket']}/{remote_name}"
            else:
                raise ValueError(f"Unknown storage type: {storage['type']}")
                
        except Exception as e:
            stop_event.set()
            for p in processes:
                try:
                    p.kill()
                except Exception:
                    pass
            raise RuntimeError(f"Streaming upload failed: {e}")
        finally:
            stop_event.set()
            for p in processes:
                try:
                    p.wait()
                except Exception:
                    pass

        for p in processes:
            if p.returncode != 0:
                raise RuntimeError(f"Backup process failed with exit code {p.returncode}. Check logs.")
            
        write_log(log_path, f"INFO  Streaming upload successful.")
        return remote_path


def stream_restore_from_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, decompression_cmd: list = None):
    write_log(log_path, f"INFO  Starting streaming restore from {storage['type']}: {remote_name}")
    
    processes = []
    with open(log_path, "a") as err_file:
        try:
            if decompression_cmd:
                p_dec = subprocess.Popen(
                    decompression_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=err_file
                )
                processes.append(p_dec)
                
                p_restore = subprocess.Popen(
                    cmd,
                    stdin=p_dec.stdout,
                    stdout=err_file,
                    stderr=err_file,
                    env=env
                )
                processes.append(p_restore)
                p_dec.stdout.close()
                stdin_stream = p_dec.stdin
            else:
                p_restore = subprocess.Popen(
                    cmd,
                    stdin=subprocess.PIPE,
                    stdout=err_file,
                    stderr=err_file,
                    env=env
                )
                processes.append(p_restore)
                stdin_stream = p_restore.stdin
                
        except Exception as e:
            for p in processes:
                try: p.kill()
                except Exception: pass
            raise RuntimeError(f"Failed to start restore processes: {e}")

        try:
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
                stream = container.download_blob(remote_name, max_concurrency=4)
                for chunk in stream.chunks():
                    stdin_stream.write(chunk)
                stdin_stream.close()
                
            elif storage["type"] == "gcs":
                import json
                from google.cloud import storage as gcs
                from google.oauth2 import service_account
                creds_dict = json.loads(storage["gcs_credentials_json"])
                creds = service_account.Credentials.from_service_account_info(creds_dict)
                client = gcs.Client(credentials=creds)
                bucket = client.bucket(storage["gcs_bucket"])
                blob = bucket.blob(remote_name)
                blob.download_to_file(stdin_stream)
                stdin_stream.close()
            else:
                raise ValueError(f"Unknown storage type: {storage['type']}")
                
        except Exception as e:
            for p in processes:
                try: p.kill()
                except Exception: pass
            raise RuntimeError(f"Streaming download failed: {e}")

        for p in processes:
            p.wait()
            if p.returncode != 0:
                raise RuntimeError(f"Restore command failed with exit code {p.returncode}. Check logs.")
            
        write_log(log_path, f"INFO  Streaming restore successful.")


# ─── MONGODB BACKUP ──────────────────────────────────────────────────────────

def run_mongo_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str) -> str:
    m = parse_mongo_uri(db)
    dbname = m["dbname"]
    if not dbname: raise ValueError("Database name is required.")
    
    collection = backup_info.get("collection", "full")
    backup_method = backup_info.get("backup_method", "full")
    incremental_field = backup_info.get("incremental_field", "")
    
    cmd = ["mongodump"] + mongo_cmd_args(m) + [
        f"--db={dbname}",
        "--archive",
        "--numParallelCollections=4",
    ]
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]
        
    if backup_method == "incremental" and incremental_field:
        # Find last successful backup for this db and collection
        backups = read_json("data/backups.json")
        last_success = None
        for b in sorted(backups, key=lambda x: x.get("created_at", ""), reverse=True):
            if b.get("status") == "completed" and b.get("database_id") == backup_info.get("database_id") and b.get("collection") == collection and b.get("id") != backup_info.get("id"):
                last_success = b.get("completed_at")
                break
                
        if last_success:
            write_log(log_path, f"INFO  Incremental backup mode detected. Querying {incremental_field} > {last_success}")
            # Construct mongodb query for ISODate
            query = f'{{"{incremental_field}": {{"$gt": {{"$date": "{last_success}"}}}}}}'
            cmd += ["--query", query]
        else:
            write_log(log_path, f"INFO  No previous successful backup found. Falling back to Full Backup.")
            
    comp_info = get_compressor_info(log_path)
    compression_cmd = None
    if comp_info:
        compression_cmd = comp_info["cmd"]
    else:
        cmd.append("--gzip")
        write_log(log_path, "INFO  No fast compressor found in PATH. Using native mongodump compression.")
        
    return stream_backup_to_storage(cmd, os.environ.copy(), storage, remote_name, log_path, compression_cmd=compression_cmd)


# ─── MONGODB RESTORE ─────────────────────────────────────────────────────────

def run_mongo_restore(db: dict, collection: str, storage: dict, remote_name: str, log_path: str, drop_existing: bool = True, compression: str = None):
    m = parse_mongo_uri(db)
    dbname = m["dbname"]
    if not dbname: raise ValueError("Database name is required for restore.")
    
    cmd = ["mongorestore"] + mongo_cmd_args(m) + [
        "--archive",
        "--numParallelCollections=1",
        f"--db={dbname}",
    ]
    if drop_existing:
        cmd.append("--drop")
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]
        
    decompression_cmd = None
    if compression == "zstd":
        decompression_cmd = ["zstd", "-d", "-c"]
    elif compression in ("pigz", "gzip"):
        if shutil.which("pigz"):
            decompression_cmd = ["pigz", "-d", "-c"]
        elif shutil.which("gzip"):
            decompression_cmd = ["gzip", "-d", "-c"]
    else:
        # Native or old backup: let mongorestore handle it using --gzip
        cmd.append("--gzip")
        
    stream_restore_from_storage(cmd, os.environ.copy(), storage, remote_name, log_path, decompression_cmd=decompression_cmd)


# ─── POSTGRESQL BACKUP ───────────────────────────────────────────────────────

def run_pg_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str) -> str:
    dbname = db.get("database_name", "").strip()
    collection = backup_info.get("collection", "full")
    backup_method = backup_info.get("backup_method", "full")
    
    if backup_method == "incremental":
        write_log(log_path, "ERROR Incremental backups via pg_dump are not supported. Only Full backups are allowed for PostgreSQL.")
        raise ValueError("Incremental backups are not supported for PostgreSQL via pg_dump.")
        
    cmd = [
        "pg_dump",
        f"--host={db['host']}", f"--port={db['port']}",
        f"--username={db['username']}", f"--dbname={dbname}",
        "--no-password", "--format=custom",
        "--compress=0",
        "--no-acl", "--no-owner",
    ]
    if collection and collection != "full":
        cmd += [f"--table={collection}"]
        
    comp_info = get_compressor_info(log_path)
    compression_cmd = None
    if comp_info:
        compression_cmd = comp_info["cmd"]
    else:
        cmd[7] = "--compress=1"
        write_log(log_path, "INFO  No fast compressor found in PATH. Using single-threaded native pg_dump compression.")
        
    env = pg_env(db)
    env["PGOPTIONS"] = "-c statement_timeout=0 -c work_mem=128MB"
        
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd)


# ─── POSTGRESQL RESTORE ──────────────────────────────────────────────────────

def run_pg_restore(db: dict, storage: dict, remote_name: str, log_path: str, new_database: bool = False, compression: str = None):
    dbname = db.get("database_name", "").strip()
    if new_database:
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
        "--no-password", "--no-acl", "--no-owner",
    ]
    if not new_database:
        cmd += ["--clean", "--if-exists"]
        
    decompression_cmd = None
    if compression == "zstd":
        decompression_cmd = ["zstd", "-d", "-c"]
    elif compression in ("pigz", "gzip"):
        if shutil.which("pigz"):
            decompression_cmd = ["pigz", "-d", "-c"]
        elif shutil.which("gzip"):
            decompression_cmd = ["gzip", "-d", "-c"]
            
    env = pg_env(db)
    env["PGOPTIONS"] = "-c statement_timeout=0 -c work_mem=128MB"
        
    stream_restore_from_storage(cmd, env, storage, remote_name, log_path, decompression_cmd=decompression_cmd)


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

        comp_info = get_compressor_info(log_path)
        ext = comp_info["ext"] if comp_info else "gz"
        compression = comp_info["type"] if comp_info else "native"
        remote_name = f"{backup_id}/backup.archive.{ext}"

        if db["type"] == "mongodb":
            remote_path = run_mongo_backup(db, backup, storage, remote_name, log_path)
        elif db["type"] == "postgresql":
            remote_path = run_pg_backup(db, backup, storage, remote_name, log_path)
        else:
            return fail(f"Unsupported DB type: {db['type']}")

        size_mb = 0  # Unknown since we streamed it directly

        duration = int((datetime.utcnow() - start).total_seconds())
        write_log(log_path, f"=== BACKUP COMPLETE | size={size_mb}MB | duration={duration}s ===")
        log.info(f"=== BACKUP COMPLETE | id={backup_id} size={size_mb}MB duration={duration}s ===")

        logs = read_log(log_path)
        update_backup(backup_id,
            status="completed", size_mb=size_mb,
            remote_path=remote_path, remote_name=remote_name,
            compression=compression,
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

        collection = backup.get("collection", "full")

        # Override dbname if new database
        if restore.get("new_database") and restore.get("new_database_name"):
            new_dbname = restore["new_database_name"].strip()
            write_log(log_path, f"INFO  Restoring to NEW database: {new_dbname}")
            log.info(f"Restoring to NEW database: {new_dbname}")
            target_db = {**target_db, "database_name": new_dbname}

        compression = backup.get("compression")

        if target_db["type"] == "mongodb":
            run_mongo_restore(target_db, collection, storage, remote_name, log_path, drop_existing=not restore.get("new_database", False), compression=compression)
        elif target_db["type"] == "postgresql":
            run_pg_restore(target_db, storage, remote_name, log_path, new_database=restore.get("new_database", False), compression=compression)
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