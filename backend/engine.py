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
import time
import re
from collections import deque

class ProgressTracker:
    def __init__(self, job_id: str, is_restore: bool = False, total_size: int = 0, log_path: str = None):
        self.job_id = job_id
        self.is_restore = is_restore
        self.total_size = total_size
        self.log_path = log_path
        self.bytes_processed = 0
        self.last_pct = -1
        self.last_update_time = 0

    def update_bytes(self, num_bytes: int):
        self.bytes_processed += num_bytes
        if self.total_size > 0:
            pct = min(99, int((self.bytes_processed / self.total_size) * 100))
            self.update_pct(pct)

    def update_pct(self, pct: int):
        now = time.time()
        if pct != self.last_pct or now - self.last_update_time >= 1:
            self.last_pct = pct
            self.last_update_time = now
            
            job_type = "RESTORE" if self.is_restore else "BACKUP"
            processed_mb = round(self.bytes_processed / (1024 * 1024), 2)
            log_msg = f"[{job_type} PROGRESS] ID: {self.job_id} | Progress: {pct}% | Processed: {processed_mb} MB"
            print(log_msg)
            
            if self.log_path:
                write_log(self.log_path, f"INFO  Progress: {pct}% ({processed_mb} MB processed)")
                
            if self.is_restore:
                update_restore(self.job_id, progress_percentage=pct)
            else:
                update_backup(self.job_id, progress_percentage=pct)


class ProgressWriter:
    def __init__(self, dest_stream, tracker: ProgressTracker = None):
        self.dest_stream = dest_stream
        self.tracker = tracker
        
    def write(self, b):
        self.dest_stream.write(b)
        if self.tracker:
            self.tracker.update_bytes(len(b))
        return len(b)
        
    def flush(self):
        self.dest_stream.flush()


def pipe_and_count(src, dest, tracker: ProgressTracker = None):
    try:
        while True:
            chunk = src.read(262144)  # 256KB chunks
            if not chunk:
                break
            if tracker:
                tracker.update_bytes(len(chunk))
            dest.write(chunk)
    except Exception as e:
        print(f"Error in pipe_and_count: {e}")
    finally:
        try:
            dest.close()
        except:
            pass


def log_and_parse_stderr(stderr_stream, log_path: str, tracker: ProgressTracker = None):
    try:
        with open(log_path, "a") as f:
            for line_bytes in iter(stderr_stream.readline, b""):
                line = line_bytes.decode("utf-8", errors="ignore")
                f.write(line)
                f.flush()
                
                pct_match = re.search(r'\((\d+(?:\.\d+)?)\%\)', line)
                if pct_match and tracker:
                    pct = int(float(pct_match.group(1)))
                    tracker.update_pct(pct)
    except Exception as e:
        print(f"Error in log_and_parse_stderr: {e}")


def get_pg_db_size(db: dict, dbname: str) -> int:
    try:
        cmd = [
            "psql", f"--host={db['host']}", f"--port={db['port']}",
            f"--username={db['username']}", "--no-password", "--dbname=postgres",
            "-t", "-A", "-c", f"SELECT pg_database_size('{dbname}');"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, env=pg_env(db), timeout=10)
        if res.returncode == 0:
            val = res.stdout.strip()
            if val.isdigit():
                return int(val)
    except Exception as e:
        print(f"Error getting PG DB size: {e}")
    return 0


class QueueReader(io.RawIOBase):
    def __init__(self, q: queue.Queue, stop_event: threading.Event):
        self.q = q
        self.stop_event = stop_event
        self.buffer = deque()
        self.buffer_size = 0
        self.buffer_offset = 0
        self.eof = False
        self.lock = threading.Lock()

    def readable(self):
        return True

    def seekable(self):
        return False

    def readinto(self, b):
        size = len(b)
        if size == 0:
            return 0

        # 1. Block and wait for at least some data if buffer is empty
        if self.buffer_size == 0 and not self.eof:
            while not self.eof:
                try:
                    chunk = self.q.get(timeout=1.0)
                    with self.lock:
                        if chunk is None:
                            self.eof = True
                        else:
                            self.buffer.append(memoryview(chunk))
                            self.buffer_size += len(chunk)
                    break
                except queue.Empty:
                    if self.stop_event.is_set():
                        with self.lock:
                            self.eof = True
                        break

        # 2. Drain any available chunks without blocking to fill up to size
        while self.buffer_size < size and not self.eof:
            try:
                chunk = self.q.get_nowait()
                with self.lock:
                    if chunk is None:
                        self.eof = True
                    else:
                        self.buffer.append(memoryview(chunk))
                        self.buffer_size += len(chunk)
            except queue.Empty:
                break

        with self.lock:
            if self.buffer_size == 0:
                return 0  # EOF

            # 3. Copy from self.buffer to b
            copied = 0
            while copied < size and self.buffer:
                chunk = self.buffer[0]
                chunk_len = len(chunk)
                available = chunk_len - self.buffer_offset
                needed = size - copied
                
                if available <= needed:
                    b[copied:copied+available] = chunk[self.buffer_offset:]
                    copied += available
                    self.buffer_size -= available
                    self.buffer.popleft()
                    self.buffer_offset = 0
                else:
                    b[copied:copied+needed] = chunk[self.buffer_offset:self.buffer_offset+needed]
                    copied += needed
                    self.buffer_size -= needed
                    self.buffer_offset += needed
                    
            return copied

    def read(self, size=-1):
        if size is None or size < 0:
            raise ValueError("Reading the entire stream without a size limit is not supported to prevent OOM errors.")
        b = bytearray(size)
        n = self.readinto(b)
        if n == 0:
            return b""
        return bytes(b[:n])


def reader_thread_fn(stream, q: queue.Queue, stop_event: threading.Event, chunk_size: int = 1024 * 1024, tracker: ProgressTracker = None):
    try:
        while not stop_event.is_set():
            data = stream.read(chunk_size)
            if not data:
                break
            if tracker:
                tracker.update_bytes(len(data))
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
            write_log(log_path, "INFO  Using Zstandard (zstd) with 1 thread for capped CPU usage.")
        return {
            "cmd": ["zstd", "-1", "--threads=1"],
            "ext": "zst",
            "type": "zstd"
        }
    elif shutil.which("pigz"):
        if log_path:
            write_log(log_path, "INFO  Using pigz (parallel gzip) with 1 thread for capped CPU usage.")
        return {
            "cmd": ["pigz", "-1", "-p", "1"],
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


def stream_backup_to_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, compression_cmd: list = None, tracker: ProgressTracker = None) -> str:
    write_log(log_path, f"INFO  Starting streaming backup to {storage['type']}: {remote_name}")
    
    processes = []
    pipe_thread = None
    stderr_thread = None
    
    is_mongo = "mongodump" in cmd[0]
    p_dump_stderr = subprocess.PIPE if is_mongo else open(log_path, "a")
    
    try:
        if compression_cmd:
            p_dump = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=p_dump_stderr,
                env=env
            )
            processes.append(p_dump)
            
            p_comp = subprocess.Popen(
                compression_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=open(log_path, "a")
            )
            processes.append(p_comp)
            
            # Start pipe thread
            pipe_thread = threading.Thread(
                target=pipe_and_count,
                args=(p_dump.stdout, p_comp.stdin, tracker if not is_mongo else None)
            )
            pipe_thread.daemon = True
            pipe_thread.start()
            
            stdout_stream = p_comp.stdout
        else:
            p_dump = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=p_dump_stderr,
                env=env
            )
            processes.append(p_dump)
            stdout_stream = p_dump.stdout
            
        if is_mongo:
            stderr_thread = threading.Thread(
                target=log_and_parse_stderr,
                args=(p_dump.stderr, log_path, tracker)
            )
            stderr_thread.daemon = True
            stderr_thread.start()
            
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
    
    reader_tracker = tracker if (not compression_cmd and not is_mongo) else None
    
    reader_thread = threading.Thread(
        target=reader_thread_fn,
        args=(stdout_stream, q, stop_event, 1024 * 1024, reader_tracker)  # 1MB chunks
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
            blob_props = container.get_blob_client(remote_name).get_blob_properties()
            stream_backup_to_storage.last_size_bytes = blob_props.size
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
            blob.chunk_size = 16 * 1024 * 1024
            blob.upload_from_file(queue_reader, num_retries=3)
            blob.reload()
            stream_backup_to_storage.last_size_bytes = blob.size
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
        if pipe_thread:
            pipe_thread.join(timeout=5.0)
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


def stream_restore_from_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, decompression_cmd: list = None, tracker: ProgressTracker = None):
    write_log(log_path, f"INFO  Starting streaming restore from {storage['type']}: {remote_name}")
    
    processes = []
    stderr_thread = None
    
    is_mongo = "mongorestore" in cmd[0]
    
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
                    stderr=subprocess.PIPE,  # Pipe stderr to parse progress
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
                    stderr=subprocess.PIPE,  # Pipe stderr to parse progress
                    env=env
                )
                processes.append(p_restore)
                stdin_stream = p_restore.stdin
                
            # Log stderr and optionally parse progress for Mongo
            stderr_thread = threading.Thread(
                target=log_and_parse_stderr,
                args=(p_restore.stderr, log_path, tracker if is_mongo else None)
            )
            stderr_thread.daemon = True
            stderr_thread.start()
                
        except Exception as e:
            for p in processes:
                try: p.kill()
                except Exception: pass
            raise RuntimeError(f"Failed to start restore processes: {e}")

        dl_tracker = tracker if not is_mongo else None

        try:
            if storage["type"] == "azure":
                from azure.storage.blob import BlobServiceClient
                conn_str = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={storage['azure_account_name']};"
                    f"AccountKey={storage['azure_account_key']};"
                    f"EndpointSuffix=core.windows.net"
                )
                client = BlobServiceClient.from_connection_string(conn_str, max_single_get_size=16 * 1024 * 1024, max_chunk_get_size=16 * 1024 * 1024)
                container = client.get_container_client(storage["azure_container"])
                stream = container.download_blob(remote_name, max_concurrency=4)
                for chunk in stream.chunks():
                    stdin_stream.write(chunk)
                    if dl_tracker:
                        dl_tracker.update_bytes(len(chunk))
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
                blob.chunk_size = 16 * 1024 * 1024
                
                progress_writer = ProgressWriter(stdin_stream, dl_tracker)
                blob.download_to_file(progress_writer)
                stdin_stream.close()
            else:
                raise ValueError(f"Unknown storage type: {storage['type']}")
                
        except Exception as e:
            for p in processes:
                try: p.kill()
                except Exception: pass
            raise RuntimeError(f"Streaming download failed: {e}")

        for p in reversed(processes):
            p.wait()

        p_restore = processes[-1]
        if p_restore.returncode != 0:
            raise RuntimeError(f"Restore command failed with exit code {p_restore.returncode}. Check logs.")
            
        write_log(log_path, f"INFO  Streaming restore successful.")


# ─── MONGODB BACKUP ──────────────────────────────────────────────────────────

def run_mongo_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, tracker: ProgressTracker = None) -> str:
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
        backups = read_json("data/backups.json")
        last_success = None
        for b in sorted(backups, key=lambda x: x.get("created_at", ""), reverse=True):
            if b.get("status") == "completed" and b.get("database_id") == backup_info.get("database_id") and b.get("collection") == collection and b.get("id") != backup_info.get("id"):
                last_success = b.get("completed_at")
                break
                
        if last_success:
            write_log(log_path, f"INFO  Incremental backup mode detected. Querying {incremental_field} > {last_success}")
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
        
    return stream_backup_to_storage(cmd, os.environ.copy(), storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker)


# ─── MONGODB RESTORE ─────────────────────────────────────────────────────────

def run_mongo_restore(db: dict, collection: str, storage: dict, remote_name: str, log_path: str, drop_existing: bool = True, compression: str = None, source_dbname: str = None, tracker: ProgressTracker = None):
    m = parse_mongo_uri(db)
    dbname = m["dbname"]
    if not dbname: raise ValueError("Database name is required for restore.")

    cmd = ["mongorestore"] + mongo_cmd_args(m) + [
        "--archive",
        "--numParallelCollections=4",
        "--numInsertionWorkersPerCollection=4",
        "--verbose=1",
    ]

    if source_dbname and source_dbname != dbname:
        write_log(log_path, f"INFO  DB name mismatch: backup='{source_dbname}' target='{dbname}' — using namespace remapping")
        cmd += [
            f"--nsFrom={source_dbname}.*",
            f"--nsTo={dbname}.*",
        ]
        if collection and collection != "full":
            cmd += [f"--nsInclude={source_dbname}.{collection}"]
    else:
        cmd += [f"--db={dbname}"]
        if collection and collection != "full":
            cmd += [f"--collection={collection}"]

    if drop_existing:
        cmd.append("--drop")

    decompression_cmd = None
    if compression == "zstd":
        decompression_cmd = ["zstd", "-d", "-c", "--threads=1"]
    elif compression in ("pigz", "gzip"):
        if shutil.which("pigz"):
            decompression_cmd = ["pigz", "-d", "-c", "-p", "1"]
        elif shutil.which("gzip"):
            decompression_cmd = ["gzip", "-d", "-c"]
    elif compression in ("native", None):
        cmd.append("--gzip")

    write_log(log_path, f"INFO  mongorestore cmd: {' '.join(cmd)}")
    stream_restore_from_storage(cmd, os.environ.copy(), storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker)


# ─── POSTGRESQL BACKUP ───────────────────────────────────────────────────────

def run_pg_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, tracker: ProgressTracker = None) -> str:
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
        "--verbose"
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
    env["PGOPTIONS"] = "-c statement_timeout=0 -c work_mem=256MB"
        
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker)


# ─── POSTGRESQL RESTORE ──────────────────────────────────────────────────────

def run_pg_restore(db: dict, storage: dict, remote_name: str, log_path: str, new_database: bool = False, compression: str = None, tracker: ProgressTracker = None):
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
        "--verbose"
    ]
    if not new_database:
        cmd += ["--clean", "--if-exists"]
        
    decompression_cmd = None
    if compression == "zstd":
        decompression_cmd = ["zstd", "-d", "-c", "--threads=1"]
    elif compression in ("pigz", "gzip"):
        if shutil.which("pigz"):
            decompression_cmd = ["pigz", "-d", "-c", "-p", "1"]
        elif shutil.which("gzip"):
            decompression_cmd = ["gzip", "-d", "-c"]
            
    env = pg_env(db)
    env["PGOPTIONS"] = "-c statement_timeout=0 -c work_mem=256MB -c maintenance_work_mem=512MB"
        
    stream_restore_from_storage(cmd, env, storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker)


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

        # Initialize progress tracker
        total_size = 0
        if db["type"] == "postgresql":
            total_size = get_pg_db_size(db, db.get("database_name", ""))
            write_log(log_path, f"INFO  Estimated database size: {round(total_size / (1024*1024), 2)} MB")

        tracker = ProgressTracker(backup_id, is_restore=False, total_size=total_size, log_path=log_path)

        source_dbname = None
        if db["type"] == "mongodb":
            remote_path = run_mongo_backup(db, backup, storage, remote_name, log_path, tracker=tracker)
            m = parse_mongo_uri(db)
            source_dbname = m["dbname"]
        elif db["type"] == "postgresql":
            remote_path = run_pg_backup(db, backup, storage, remote_name, log_path, tracker=tracker)
        else:
            return fail(f"Unsupported DB type: {db['type']}")

        # Fetch actual uploaded size from storage (set by stream_backup_to_storage)
        raw_bytes = getattr(stream_backup_to_storage, "last_size_bytes", 0) or 0
        size_mb = round(raw_bytes / (1024 * 1024), 2)

        duration = int((datetime.utcnow() - start).total_seconds())
        write_log(log_path, f"=== BACKUP COMPLETE | size={size_mb}MB | duration={duration}s ===")
        log.info(f"=== BACKUP COMPLETE | id={backup_id} size={size_mb}MB duration={duration}s ===")

        tracker.update_pct(100)

        logs = read_log(log_path)
        update_backup(backup_id,
            status="completed", size_mb=size_mb,
            remote_path=remote_path, remote_name=remote_name,
            compression=compression,
            source_dbname=source_dbname if db["type"] == "mongodb" else None,
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
        source_dbname = backup.get("source_dbname")  # saved at backup time

        # Initialize progress tracker
        total_size = int(backup.get("size_mb", 0) * 1024 * 1024)
        tracker = ProgressTracker(restore_id, is_restore=True, total_size=total_size, log_path=log_path)

        if target_db["type"] == "mongodb":
            run_mongo_restore(target_db, collection, storage, remote_name, log_path, drop_existing=not restore.get("new_database", False), compression=compression, source_dbname=source_dbname, tracker=tracker)
        elif target_db["type"] == "postgresql":
            run_pg_restore(target_db, storage, remote_name, log_path, new_database=restore.get("new_database", False), compression=compression, tracker=tracker)
        else:
            return fail(f"Unsupported DB type: {target_db['type']}")

        tracker.update_pct(100)

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