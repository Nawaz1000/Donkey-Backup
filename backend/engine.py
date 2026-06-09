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
from utils import read_json, write_json, send_notification, parse_mongo_uri

BACKUP_TMP = "/tmp/backupvault"
os.makedirs(BACKUP_TMP, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s [BACKUP] %(levelname)s %(message)s",
    level=logging.INFO
)
log = logging.getLogger("backupvault")

ACTIVE_PROCESSES = {}
ACTIVE_STOP_EVENTS = {}

def cancel_job(job_id: str) -> bool:
    found = False
    if job_id in ACTIVE_PROCESSES:
        for p in ACTIVE_PROCESSES[job_id]:
            try:
                p.terminate()
                p.kill()
            except Exception:
                pass
        found = True
    if job_id in ACTIVE_STOP_EVENTS:
        try:
            ACTIVE_STOP_EVENTS[job_id].set()
            found = True
        except Exception:
            pass
    return found



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


def mask_cmd(cmd: list) -> list:
    log_cmd = []
    for arg in cmd:
        if arg.startswith("--uri="):
            try:
                val = arg[6:]
                if "@" in val:
                    scheme_end = val.index("://") + 3
                    scheme = val[:scheme_end]
                    rest = val[scheme_end:]
                    at_pos = rest.rfind("@")
                    userinfo = rest[:at_pos]
                    hostpart = rest[at_pos:]
                    if ":" in userinfo:
                        colon_pos = userinfo.index(":")
                        user = userinfo[:colon_pos]
                        masked_val = f"{scheme}{user}:******{hostpart}"
                    else:
                        masked_val = f"{scheme}******{hostpart}"
                    log_cmd.append(f"--uri={masked_val}")
                else:
                    log_cmd.append(arg)
            except Exception:
                log_cmd.append("--uri=******")
        else:
            log_cmd.append(arg)
    return log_cmd


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
            log.info(log_msg)
            
            if self.log_path:
                write_log(self.log_path, f"INFO  Progress: {pct}% ({processed_mb} MB processed)")
                
            if self.is_restore:
                update_restore(self.job_id, progress_percentage=pct)
            else:
                update_backup(self.job_id, progress_percentage=pct)


class ProgressWriter:
    def __init__(self, dest_stream, tracker: ProgressTracker = None, stop_event=None):
        self.dest_stream = dest_stream
        self.tracker = tracker
        self.stop_event = stop_event
        
    def write(self, b):
        if self.stop_event and self.stop_event.is_set():
            raise RuntimeError("Restore cancelled by user.")
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
                
                # Also log to container console logs
                clean_line = line.strip()
                if clean_line:
                    log.info(clean_line)
                
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


def get_pg_modified_tables(db: dict, dbname: str, since_timestamp: str, log_path: str = None) -> list:
    """Query pg_stat_user_tables to find tables modified since the last backup.
    Returns a list of 'schema.table' strings, or None if detection fails."""
    try:
        query = (
            "SELECT schemaname || '.' || relname FROM pg_stat_user_tables "
            "WHERE (n_tup_ins + n_tup_upd + n_tup_del) > 0 "
            f"AND greatest(last_vacuum, last_autovacuum, last_analyze, last_autoanalyze) >= '{since_timestamp}' "
            "ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC;"
        )
        cmd = [
            "psql", f"--host={db['host']}", f"--port={db['port']}",
            f"--username={db['username']}", "--no-password", f"--dbname={dbname}",
            "-t", "-A", "-c", query
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, env=pg_env(db), timeout=15)
        if res.returncode == 0:
            tables = [t.strip() for t in res.stdout.strip().split('\n') if t.strip()]
            if log_path:
                write_log(log_path, f"INFO  pg_stat detected {len(tables)} modified tables")
            return tables
    except Exception as e:
        if log_path:
            write_log(log_path, f"WARN  Failed to detect modified tables: {e}")
        print(f"Error detecting PG modified tables: {e}")
    return None


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
            time.sleep(0.001)
    except Exception as e:
        print(f"Error in reader thread: {e}")
    finally:
        q.put(None)


def get_speed_settings(profile: str):
    cores = os.cpu_count() or 4
    if profile == "safe":
        return {"threads": 1, "chunk_size": 4 * 1024 * 1024, "queue_max": 4, "concurrency": 1, "mongo_parallel": 1}
    elif profile == "balanced":
        return {"threads": max(1, cores // 2), "chunk_size": 16 * 1024 * 1024, "queue_max": 8, "concurrency": 4, "mongo_parallel": 2}
    elif profile == "extreme":
        return {"threads": cores, "chunk_size": 128 * 1024 * 1024, "queue_max": 128, "concurrency": 64, "mongo_parallel": 16}
    else: # default
        return {"threads": max(1, cores - 1), "chunk_size": 32 * 1024 * 1024, "queue_max": 32, "concurrency": 8, "mongo_parallel": 4}

def get_compressor_info(profile: str = "default", log_path: str = None) -> dict:
    settings = get_speed_settings(profile)
    threads = settings["threads"]
    if shutil.which("zstd"):
        if log_path:
            write_log(log_path, f"INFO  Using Zstandard (zstd) with {threads} threads for {profile} profile.")
        return {
            "cmd": ["zstd", "--fast=1", f"--threads={threads}"] if profile == "extreme" else (["zstd", "--fast=3", f"--threads={threads}"] if profile == "default" else ["zstd", "-1", f"--threads={threads}"]),
            "ext": "zst",
            "type": "zstd"
        }
    elif shutil.which("pigz"):
        if log_path:
            write_log(log_path, f"INFO  Using pigz (parallel gzip) with {threads} threads for optimized speed.")
        return {
                "cmd": ["pigz", "-1", "-p", str(threads)],
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


def stream_backup_to_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, compression_cmd: list = None, tracker: ProgressTracker = None, speed_profile: str = "default") -> str:
    write_log(log_path, f"INFO  Starting streaming backup to {storage['type']}: {remote_name} (Profile: {speed_profile})")
    
    settings = get_speed_settings(speed_profile)
    
    processes = []
    if tracker:
        ACTIVE_PROCESSES[tracker.job_id] = processes
        if tracker.job_id in ACTIVE_STOP_EVENTS and ACTIVE_STOP_EVENTS[tracker.job_id].is_set():
            raise RuntimeError("Backup cancelled by user.")
    pipe_thread = None
    stderr_thread = None
    
    is_mongo = "mongodump" in cmd[0]
    p_dump_stderr = subprocess.PIPE if is_mongo else open(log_path, "a")
    
    creationflags = 0x00004000 if os.name == 'nt' else 0
    
    try:
        if compression_cmd:
            p_dump = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=p_dump_stderr,
                env=env,
                creationflags=creationflags
            )
            processes.append(p_dump)
            
            p_comp = subprocess.Popen(
                compression_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=open(log_path, "a"),
                creationflags=creationflags
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
                env=env,
                creationflags=creationflags
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

    q = queue.Queue(maxsize=settings["queue_max"])
    stop_event = ACTIVE_STOP_EVENTS.get(tracker.job_id) if tracker else threading.Event()
    if tracker and tracker.job_id not in ACTIVE_STOP_EVENTS:
        ACTIVE_STOP_EVENTS[tracker.job_id] = stop_event
    
    reader_tracker = tracker if (not compression_cmd and not is_mongo) else None
    
    reader_thread = threading.Thread(
        target=reader_thread_fn,
        args=(stdout_stream, q, stop_event, settings["chunk_size"], reader_tracker)
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
            client = BlobServiceClient.from_connection_string(conn_str, max_block_size=settings["chunk_size"], connection_timeout=300, read_timeout=3600)
            container = client.get_container_client(storage["azure_container"])
            container.upload_blob(
                name=remote_name, 
                data=queue_reader, 
                overwrite=True,
                max_concurrency=settings["concurrency"],
                read_timeout=3600
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
            blob.chunk_size = settings["chunk_size"]
            blob.upload_from_file(queue_reader, num_retries=3, timeout=3600)
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
            try:
                p.wait()
            except Exception:
                pass

    for p in processes:
        if p.returncode != 0:
            raise RuntimeError(f"Backup process failed with exit code {p.returncode}. Check logs.")
        
    write_log(log_path, f"INFO  Streaming upload successful.")
    
        
    return remote_path


def stream_restore_from_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, decompression_cmd: list = None, tracker: ProgressTracker = None, speed_profile: str = "default"):
    write_log(log_path, f"INFO  Starting streaming restore from {storage['type']}: {remote_name} (Profile: {speed_profile})")
    
    settings = get_speed_settings(speed_profile)
    
    processes = []
    stop_event = ACTIVE_STOP_EVENTS.get(tracker.job_id) if tracker else threading.Event()
    if tracker:
        ACTIVE_PROCESSES[tracker.job_id] = processes
        if tracker.job_id not in ACTIVE_STOP_EVENTS:
            ACTIVE_STOP_EVENTS[tracker.job_id] = stop_event
        if stop_event.is_set():
            raise RuntimeError("Restore cancelled by user.")
    stderr_thread = None
    
    is_mongo = "mongorestore" in cmd[0]
    creationflags = 0x00004000 if os.name == 'nt' else 0
    
    with open(log_path, "a") as err_file:
        try:
            if decompression_cmd:
                p_dec = subprocess.Popen(
                    decompression_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=err_file,
                    creationflags=creationflags
                )
                processes.append(p_dec)
                
                p_restore = subprocess.Popen(
                    cmd,
                    stdin=p_dec.stdout,
                    stdout=err_file,
                    stderr=subprocess.PIPE,  # Pipe stderr to parse progress
                    env=env,
                    creationflags=creationflags
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
                    env=env,
                    creationflags=creationflags
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

        dl_tracker = tracker

        try:
            if storage["type"] == "azure":
                from azure.storage.blob import BlobServiceClient
                conn_str = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={storage['azure_account_name']};"
                    f"AccountKey={storage['azure_account_key']};"
                    f"EndpointSuffix=core.windows.net"
                )
                client = BlobServiceClient.from_connection_string(conn_str, max_single_get_size=settings["chunk_size"], max_chunk_get_size=settings["chunk_size"], connection_timeout=300, read_timeout=3600)
                container = client.get_container_client(storage["azure_container"])
                stream = container.download_blob(remote_name, max_concurrency=settings["concurrency"], read_timeout=3600)
                for chunk in stream.chunks():
                    if stop_event.is_set():
                        raise RuntimeError("Restore cancelled by user.")
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
                blob.chunk_size = settings["chunk_size"]
                
                progress_writer = ProgressWriter(stdin_stream, dl_tracker, stop_event=stop_event)
                blob.download_to_file(progress_writer, timeout=3600)
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
            if is_mongo or p_restore.returncode != 1:
                raise RuntimeError(f"Restore command failed with exit code {p_restore.returncode}. Check logs.")
            else:
                write_log(log_path, f"WARN  Restore completed with warnings (exit code 1). This is often harmless in pg_restore.")
            
        write_log(log_path, f"INFO  Streaming restore successful.")


# ─── MONGODB BACKUP ──────────────────────────────────────────────────────────

def run_mongo_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default") -> str:
    m = parse_mongo_uri(db)
    dbname = m["dbname"]
    if not dbname: raise ValueError("Database name is required.")
    
    collection = backup_info.get("collection", "full")
    backup_method = backup_info.get("backup_method", "full")
    incremental_field = backup_info.get("incremental_field", "")
    
    if indexing_mode == "only_index":
        import pymongo
        import json
        import tempfile
        write_log(log_path, "INFO  MongoDB only_index backup requested. Fetching indexes via PyMongo.")
        try:
            client = pymongo.MongoClient(m["uri"])
            mongo_db = client[dbname]
            indexes = {}
            collections = [collection] if collection and collection != "full" else mongo_db.list_collection_names()
            for coll in collections:
                idx_info = mongo_db[coll].index_information()
                if idx_info:
                    indexes[coll] = idx_info
            
            tmp_json = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}_indexes.json")
            with open(tmp_json, "w") as f:
                json.dump(indexes, f)
            
            cmd = ["cat", tmp_json] if os.name != 'nt' else ["cmd", "/c", "type", tmp_json]
            res = stream_backup_to_storage(cmd, os.environ.copy(), storage, remote_name, log_path, tracker=tracker, speed_profile=speed_profile)
            os.remove(tmp_json)
            client.close()
            return res
        except Exception as e:
            raise RuntimeError(f"Failed to backup MongoDB indexes: {e}")

    settings = get_speed_settings(speed_profile)
    cmd = [
        "mongodump",
        f"--uri={m['uri']}",
        "--archive",
        f"--numParallelCollections={settings['mongo_parallel']}",
        "--readPreference=secondaryPreferred",  # Offload reads to secondary replicas, reduces primary server load
    ]
    if collection and collection != "full":
        cmd += [f"--collection={collection}"]
        
    if backup_method in ("incremental", "differential") and incremental_field:
        backups = read_json("data/backups.json")
        last_success = None
        for b in sorted(backups, key=lambda x: x.get("created_at", ""), reverse=True):
            if b.get("status") == "completed" and b.get("database_id") == backup_info.get("database_id") and b.get("collection") == collection and b.get("id") != backup_info.get("id"):
                if backup_method == "incremental" or b.get("backup_method") == "full":
                    last_success = b.get("completed_at")
                    break
                
        if last_success:
            write_log(log_path, f"INFO  {backup_method.capitalize()} backup mode detected. Querying {incremental_field} > {last_success}")
            query = f'{{"{incremental_field}": {{"$gt": {{"$date": "{last_success}"}}}}}}'
            cmd += ["--query", query]
        else:
            write_log(log_path, f"INFO  No previous successful {'Full ' if backup_method == 'differential' else ''}backup found. Falling back to Full Backup.")
            
    comp_info = get_compressor_info(speed_profile, log_path)
    compression_cmd = None
    if comp_info:
        compression_cmd = comp_info["cmd"]
    else:
        cmd.append("--gzip")
        write_log(log_path, "INFO  No fast compressor found in PATH. Using native mongodump compression.")
        
    log_cmd = mask_cmd(cmd)
    write_log(log_path, f"INFO  mongodump cmd: {' '.join(log_cmd)}")
    env = os.environ.copy()
    env["GOGC"] = "100"
    env["GOMAXPROCS"] = str(settings["threads"])
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)


# ─── MONGODB RESTORE ─────────────────────────────────────────────────────────

def run_mongo_restore(db: dict, collection: str, storage: dict, remote_name: str, log_path: str, drop_existing: bool = True, compression: str = None, source_dbname: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default"):
    m = parse_mongo_uri(db)
    dbname = m["dbname"]
    if not dbname: raise ValueError("Database name is required for restore.")

    if indexing_mode == "only_index":
        import pymongo
        import json
        import tempfile
        write_log(log_path, "INFO  MongoDB only_index restore requested. Applying indexes via PyMongo.")
        try:
            tmp_json = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}_indexes.json")
            cmd = ["cat"] if os.name != 'nt' else ["findstr", "^"]
            with open(tmp_json, "wb") as f:
                p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=f)
                stream_restore_from_storage(["cat"] if os.name != 'nt' else ["findstr", "^"], os.environ.copy(), storage, remote_name, log_path, tracker=tracker)
            
            # This requires custom download logic for only_index since stream_restore pipes to stdout/file.
            # Instead of modifying stream_restore heavily, we can rely on standard Python storage SDKs directly.
            write_log(log_path, "WARN  only_index for Mongo relies on backend python script execution.")
        except Exception as e:
            pass
            
    settings = get_speed_settings(speed_profile)
    workers = max(10, settings['threads'] * 4)
    batch_size = 500
    write_concern = "{w: 1}"
    if speed_profile == "extreme":
        workers = 32  # Dialed back from 500 to prevent 18GB memory spike
        batch_size = 5000  # Safe but fast batch size
        write_concern = "{w: 1}"
        
    cmd = [
        "mongorestore",
        f"--uri={m['uri']}",
        "--archive",
        f"--numParallelCollections={min(settings['mongo_parallel'], 4)}",
        f"--numInsertionWorkersPerCollection={workers}",
        f"--batchSize={batch_size}",
        "--bypassDocumentValidation",
        f"--writeConcern={write_concern}",
        "--verbose=1",
    ]

    if indexing_mode == "without_index":
        cmd.append("--noIndexRestore")

    # Always use wildcard namespace remapping to force restore to dbname
    cmd += [
        "--nsFrom=$database$.$collection$",
        f"--nsTo={dbname}.$collection$",
    ]
    if collection and collection != "full":
        cmd += [f"--nsInclude=*.{collection}"]

    if drop_existing:
        cmd.append("--drop")

    settings = get_speed_settings(speed_profile)
    decompression_threads = settings["threads"]
    decompression_cmd = None
    if compression == "zstd":
        decompression_cmd = ["zstd", "-d", "-c", f"--threads={decompression_threads}"]
    elif compression in ("pigz", "gzip"):
        if shutil.which("pigz"):
            decompression_cmd = ["pigz", "-d", "-c", "-p", str(decompression_threads)]
        elif shutil.which("gzip"):
            decompression_cmd = ["gzip", "-d", "-c"]
    elif compression in ("native", None):
        cmd.append("--gzip")

    log_cmd = mask_cmd(cmd)
    write_log(log_path, f"INFO  mongorestore cmd: {' '.join(log_cmd)}")
    env = os.environ.copy()
    env["GOGC"] = "100"
    env["GOMAXPROCS"] = str(settings["threads"])
    stream_restore_from_storage(cmd, env, storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker, speed_profile=speed_profile)


# ─── POSTGRESQL BACKUP ───────────────────────────────────────────────────────

def run_pg_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default") -> str:
    dbname = db.get("database_name", "").strip()
    collection = backup_info.get("collection", "full")
    backup_method = backup_info.get("backup_method", "full")
    
    incremental_tables = None
    if backup_method in ("incremental", "differential"):
        # Find last successful backup for this database
        backups = read_json("data/backups.json")
        last_success = None
        for b in sorted(backups, key=lambda x: x.get("created_at", ""), reverse=True):
            if (b.get("status") == "completed" and 
                b.get("database_id") == backup_info.get("database_id") and
                b.get("id") != backup_info.get("id")):
                if backup_method == "incremental" or b.get("backup_method") == "full":
                    last_success = b.get("completed_at") or b.get("created_at")
                    break
        
        if last_success:
            write_log(log_path, f"INFO  PostgreSQL {backup_method} mode: detecting tables modified since {last_success}")
            incremental_tables = get_pg_modified_tables(db, dbname, last_success, log_path)
            if incremental_tables is not None and len(incremental_tables) == 0:
                write_log(log_path, f"INFO  No tables modified since last {backup_method} backup. Creating minimal archive.")
                # Still create a backup record but with 0 tables — very fast
            elif incremental_tables is None:
                write_log(log_path, "WARN  Could not detect modified tables. Falling back to full backup.")
        else:
            write_log(log_path, f"INFO  No previous successful {'Full ' if backup_method == 'differential' else ''}backup found. Performing full backup.")
        
    cmd = [
        "pg_dump",
        f"--host={db['host']}", f"--port={db['port']}",
        f"--username={db['username']}", f"--dbname={dbname}",
        "--no-password", "--format=custom",
        "--compress=0",
        "--no-acl", "--no-owner",
        "--verbose"
    ]
    
    if indexing_mode == "without_index":
        cmd += ["--section=pre-data", "--section=data"]
        write_log(log_path, "INFO  PostgreSQL backup without_index: omitting post-data section.")
    elif indexing_mode == "only_index":
        cmd += ["--section=post-data"]
        write_log(log_path, "INFO  PostgreSQL backup only_index: dumping post-data section only.")
    
    if incremental_tables is not None and len(incremental_tables) > 0:
        # Incremental: dump only modified tables
        for tbl in incremental_tables:
            cmd += [f"--table={tbl}"]
        write_log(log_path, f"INFO  Incremental: dumping {len(incremental_tables)} modified tables: {', '.join(incremental_tables[:10])}{'...' if len(incremental_tables)>10 else ''}")
    elif collection and collection != "full":
        cmd += [f"--table={collection}"]
        
    comp_info = get_compressor_info(speed_profile, log_path)
    compression_cmd = None
    if comp_info:
        compression_cmd = comp_info["cmd"]
    else:
        # Find the --compress=0 index dynamically
        for i, c in enumerate(cmd):
            if c == "--compress=0":
                cmd[i] = "--compress=1"
                break
        write_log(log_path, "INFO  No fast compressor found in PATH. Using single-threaded native pg_dump compression.")
        
    env = pg_env(db)
    if speed_profile == "extreme":
        env["PGOPTIONS"] = f"-c statement_timeout=0 -c work_mem=128MB -c maintenance_work_mem=1GB -c max_parallel_workers_per_gather=4 -c effective_io_concurrency=32"
    elif speed_profile == "balanced":
        env["PGOPTIONS"] = f"-c statement_timeout=0 -c work_mem=32MB -c maintenance_work_mem=128MB -c max_parallel_workers_per_gather=2 -c effective_io_concurrency=4"
    else:
        env["PGOPTIONS"] = f"-c statement_timeout=0 -c work_mem=16MB -c maintenance_work_mem=64MB -c max_parallel_workers_per_gather=0 -c effective_io_concurrency=1"
        
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)


# ─── POSTGRESQL RESTORE ──────────────────────────────────────────────────────

def run_pg_restore(db: dict, storage: dict, remote_name: str, log_path: str, new_database: bool = False, compression: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default"):
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
        "--disable-triggers",  # Prevent trigger execution during restore — huge server CPU saver
        "--verbose"
    ]
    if not new_database:
        cmd += ["--clean", "--if-exists"]
        
    if indexing_mode == "without_index":
        cmd += ["--section=pre-data", "--section=data"]
    elif indexing_mode == "only_index":
        cmd += ["--section=post-data"]
        
    settings = get_speed_settings(speed_profile)
    decompression_cmd = None
    if compression == "zstd":
        decompression_cmd = ["zstd", "-d", "-c", f"--threads={settings['threads']}"]
    elif compression in ("pigz", "gzip"):
        if shutil.which("pigz"):
            decompression_cmd = ["pigz", "-d", "-c", "-p", str(settings['threads'])]
        elif shutil.which("gzip"):
            decompression_cmd = ["gzip", "-d", "-c"]
            
    env = pg_env(db)
    
    # Base safe settings
    sync_commit = "on"
    work_mem = "16MB"
    maint_work_mem = "64MB"
    
    if speed_profile == "extreme":
        sync_commit = "off"
        work_mem = "64MB"
        maint_work_mem = "1GB"
        cmd += ["--disable-triggers"] # Redundant but safe
        
    env["PGOPTIONS"] = f"-c statement_timeout=0 -c work_mem={work_mem} -c maintenance_work_mem={maint_work_mem} -c synchronous_commit={sync_commit}"
        
    stream_restore_from_storage(cmd, env, storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker, speed_profile=speed_profile)

# ─── SOLR BACKUP & RESTORE ───────────────────────────────────────────────────

def get_solr_base_url(host: str, port: int) -> str:
    host = host.strip()
    if host.startswith("http://") or host.startswith("https://"):
        from urllib.parse import urlparse
        parsed = urlparse(host)
        return f"{parsed.scheme}://{parsed.netloc}"
    scheme = "https" if port == 443 else "http"
    return f"{scheme}://{host}:{port}"


def run_solr_backup(db: dict, backup: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default") -> str:
    import base64
    import urllib.request
    import json
    import ssl
    import sys
    
    solr_host = db["host"]
    solr_port = db["port"]
    
    collection = db.get("database_name")
    if not collection or collection == "default" or collection == "full":
        collection = backup.get("collection")
    if not collection or collection == "full":
        collection = "default"
        
    solr_base_url = get_solr_base_url(solr_host, solr_port)
    
    auth_str = "None"
    if db.get("username") and db.get("password"):
        auth_raw = f"{db['username']}:{db['password']}"
        auth_str = f"Basic {base64.b64encode(auth_raw.encode()).decode()}"

    collections_to_backup = []
    if collection == "all_collections":
        try:
            context = ssl._create_unverified_context()
            headers = {"Authorization": auth_str} if auth_str != "None" else {}
            cores_url = f"{solr_base_url}/solr/admin/cores?action=STATUS&wt=json"
            req_cores = urllib.request.Request(cores_url, headers=headers)
            with urllib.request.urlopen(req_cores, context=context, timeout=10) as response:
                data = json.loads(response.read().decode())
                collections_to_backup = list(data.get("status", {}).keys())
        except Exception as e:
            raise RuntimeError(f"Failed to fetch Solr collections list: {e}")
    else:
        collections_to_backup = [collection]

    if not collections_to_backup:
        raise RuntimeError("No Solr collections found to backup.")

    write_log(log_path, f"INFO  Starting HTTP-based Solr backup for collections: {collections_to_backup}")

    # Use gzip compression for JSON lines
    comp_info = get_compressor_info(speed_profile, log_path)
    compression_cmd = comp_info["cmd"] if comp_info else ["gzip", "-c"]

    if len(collections_to_backup) > 1:
        write_log(log_path, f"WARN  Multiple collections selected. Only '{collections_to_backup[0]}' will be backed up in this stream.")
        
    coll = collections_to_backup[0]
    
    # Fetch total size to enable progress percentage
    if tracker:
        try:
            context = ssl._create_unverified_context()
            headers = {"Authorization": auth_str} if auth_str != "None" else {}
            core_url = f"{solr_base_url}/solr/admin/cores?action=STATUS&wt=json"
            req_cores = urllib.request.Request(core_url, headers=headers)
            with urllib.request.urlopen(req_cores, context=context, timeout=5) as response:
                data = json.loads(response.read().decode())
                total_bytes = 0
                for core_name, core_info in data.get("status", {}).items():
                    coll_name = core_info.get("cloud", {}).get("collection", core_name)
                    if coll_name == coll:
                        total_bytes += core_info.get("index", {}).get("sizeInBytes", 0)
                if total_bytes > 0:
                    tracker.total_size = total_bytes
        except Exception as e:
            write_log(log_path, f"WARN  Could not fetch Solr index size: {e}")
    
    cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "solr_helper.py"), "dump", solr_base_url, coll, auth_str]
    
    stream_backup_to_storage(cmd, {}, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)
    write_log(log_path, "INFO  Solr backup streamed successfully via HTTP API.")
        
    return f"{storage['type']}://{remote_name}"


def run_solr_restore(db: dict, collection: str, storage: dict, remote_name: str, log_path: str, drop_existing: bool = False, compression: str = None, source_dbname: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default"):
    import base64
    import sys
    
    solr_host = db["host"]
    solr_port = db["port"]
    
    target_collection = db.get("database_name")
    if not target_collection or target_collection in ("default", "full", "all_collections"):
        target_collection = collection
        
    if not target_collection or target_collection in ("default", "full", "all_collections"):
        raise RuntimeError("A specific collection name is required for Solr HTTP restore.")
        
    solr_base_url = get_solr_base_url(solr_host, solr_port)
    
    auth_str = "None"
    if db.get("username") and db.get("password"):
        auth_raw = f"{db['username']}:{db['password']}"
        auth_str = f"Basic {base64.b64encode(auth_raw.encode()).decode()}"

    write_log(log_path, f"INFO  Starting HTTP-based Solr restore to collection '{target_collection}'")

    if not drop_existing:
        write_log(log_path, f"INFO  Creating new Solr collection/core: '{target_collection}'")
        try:
            import urllib.request
            import ssl
            context = ssl._create_unverified_context()
            headers = {"Authorization": auth_str} if auth_str != "None" else {}
            
            # Try collections API first
            coll_url = f"{solr_base_url}/solr/admin/collections?action=CREATE&name={target_collection}&numShards=1&wt=json"
            req = urllib.request.Request(coll_url, headers=headers)
            try:
                with urllib.request.urlopen(req, context=context, timeout=15) as resp:
                    pass
            except Exception:
                # Fallback to Core API
                core_url = f"{solr_base_url}/solr/admin/cores?action=CREATE&name={target_collection}&wt=json"
                req2 = urllib.request.Request(core_url, headers=headers)
                with urllib.request.urlopen(req2, context=context, timeout=15) as resp2:
                    pass
            write_log(log_path, f"INFO  Successfully created new Solr collection/core.")
        except Exception as e:
            write_log(log_path, f"WARN  Failed to create new Solr collection. It may already exist or API not supported: {e}")

    comp_info = get_compressor_info(speed_profile, log_path)
    decompression_cmd = comp_info.get("decompress_cmd")

    if not decompression_cmd:
        if remote_name.endswith(".zst"):
            decompression_cmd = ["zstd", "-d", "-c"]
        elif remote_name.endswith(".gz") or remote_name.endswith(".tar.gz"):
            if shutil.which("pigz"):
                decompression_cmd = ["pigz", "-d", "-c"]
            else:
                decompression_cmd = ["gzip", "-d", "-c"]
            
    cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "solr_helper.py"), "restore", solr_base_url, target_collection, auth_str]
    
    stream_restore_from_storage(cmd, {}, storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker, speed_profile=speed_profile)
    write_log(log_path, "INFO  Solr restore streamed successfully via HTTP API.")

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
    ACTIVE_STOP_EVENTS[backup_id] = threading.Event()
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
        if backup.get("send_notifications", True):
            send_notification("Backup Failed \u274c", f"Backup for {collection if collection != 'full' else (db and db.get('name'))} in {storage and storage.get('name')} failed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}. Error: {msg}", is_error=True, log_path=log_path)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not db or not storage:
            return fail("Database or storage not found")
            
        if backup.get("send_notifications", True):
            send_notification("Backup Started \u23f3", f"Backup for {collection if collection != 'full' else db.get('name')} in {storage.get('name')} started at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", is_error=False, log_path=log_path)

        speed_profile = backup.get("speed_profile", "default")
        
        comp_info = get_compressor_info(speed_profile, log_path)
        ext = comp_info["ext"] if comp_info else "gz"
        compression = comp_info["type"] if comp_info else "native"
        
        safe_db_name = db.get("database_name", "db").strip().replace(" ", "_")
        if not safe_db_name:
            safe_db_name = db.get("name", "db").strip().replace(" ", "_")
        date_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        
        if collection and collection != "full":
            base_name = collection.strip().replace(" ", "_")
        else:
            base_name = safe_db_name
            
        remote_name = f"{base_name}-{date_str}/backup.archive.{ext}"

        # Initialize progress tracker
        total_size = 0
        if db["type"] == "postgresql":
            total_size = get_pg_db_size(db, db.get("database_name", ""))
            write_log(log_path, f"INFO  Estimated database size: {round(total_size / (1024*1024), 2)} MB")

        tracker = ProgressTracker(backup_id, is_restore=False, total_size=total_size, log_path=log_path)
        indexing_mode = backup.get("indexing_mode", "with_index")

        source_dbname = None
        if db["type"] == "mongodb":
            remote_path = run_mongo_backup(db, backup, storage, remote_name, log_path, indexing_mode=indexing_mode, tracker=tracker, speed_profile=speed_profile)
            m = parse_mongo_uri(db)
            source_dbname = m["dbname"]
        elif db["type"] == "postgresql":
            remote_path = run_pg_backup(db, backup, storage, remote_name, log_path, indexing_mode=indexing_mode, tracker=tracker, speed_profile=speed_profile)
        elif db["type"] == "solr":
            remote_path = run_solr_backup(db, backup, storage, remote_name, log_path, indexing_mode=indexing_mode, tracker=tracker, speed_profile=speed_profile)
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
        if backup.get("send_notifications", True):
            send_notification("Backup Successful \u2705", f"Backup for {collection if collection != 'full' else (db and db.get('name'))} in {storage and storage.get('name')} completed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}. Size: {size_mb}MB. Duration: {duration}s", is_error=False, log_path=log_path)

    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        # Keep log file for a bit, cleanup old ones
        _cleanup_old_logs()


# ─── MAIN RESTORE TASK ───────────────────────────────────────────────────────

def do_restore(restore_id: str):
    ACTIVE_STOP_EVENTS[restore_id] = threading.Event()
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
        send_notification("Restore Failed \u274c", f"Restore for {restore.get('remote_name', 'backup')} to {target_db and target_db.get('name')} failed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}. Error: {msg}", is_error=True, log_path=log_path)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not target_db:
            return fail("Target database not found")

        if restore.get("backup_id"):
            backup = next((b for b in backups if b["id"] == restore["backup_id"]), None)
            if not backup:
                return fail("Backup not found")
            storage = next((s for s in storages if s["id"] == backup["storage_id"]), None)
            if not storage:
                return fail("Storage not found")
            remote_name = backup.get("remote_name")
            if not remote_name:
                return fail("Backup has no remote file")
            collection = backup.get("collection", "full")
            compression = backup.get("compression")
            source_db_connection = next((d for d in dbs if d["id"] == backup.get("database_id")), None)
            fallback_source_dbname = source_db_connection["database_name"] if source_db_connection else None
            source_dbname = backup.get("source_dbname") or fallback_source_dbname
            total_size = int(backup.get("size_mb", 0) * 1024 * 1024)
        elif restore.get("storage_id") and restore.get("remote_name"):
            storage = next((s for s in storages if s["id"] == restore["storage_id"]), None)
            if not storage:
                return fail("Storage not found")
            remote_name = restore["remote_name"]
            collection = "full"
            if ".zst" in remote_name:
                compression = "zstd"
            elif ".gz" in remote_name:
                compression = "gzip"
            else:
                compression = "native"
            source_dbname = None
            total_size = 0
        else:
            return fail("Backup ID or Storage/Remote Name not provided")

        send_notification("Restore Started \u23f3", f"Restore for {remote_name} from {storage.get('name')} to {target_db.get('name')} started at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}", is_error=False, log_path=log_path)

        # Override dbname if new database
        if restore.get("new_database") and restore.get("new_database_name"):
            new_dbname = restore["new_database_name"].strip()
            write_log(log_path, f"INFO  Restoring to NEW database: {new_dbname}")
            log.info(f"Restoring to NEW database: {new_dbname}")
            target_db = {**target_db, "database_name": new_dbname}

        # Initialize progress tracker
        tracker = ProgressTracker(restore_id, is_restore=True, total_size=total_size, log_path=log_path)
        indexing_mode = restore.get("indexing_mode", "with_index")

        if target_db["type"] == "mongodb":
            run_mongo_restore(target_db, collection, storage, remote_name, log_path, drop_existing=not restore.get("new_database", False), compression=compression, source_dbname=source_dbname, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"))
        elif target_db["type"] == "postgresql":
            run_pg_restore(target_db, storage, remote_name, log_path, new_database=restore.get("new_database", False), compression=compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"))
        elif target_db["type"] == "solr":
            run_solr_restore(target_db, collection, storage, remote_name, log_path, drop_existing=not restore.get("new_database", False), compression=compression, source_dbname=source_dbname, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"))
        else:
            return fail(f"Unsupported DB type: {target_db['type']}")

        tracker.update_pct(100)

        duration = int((datetime.utcnow() - start).total_seconds())
        write_log(log_path, f"=== RESTORE COMPLETE | duration={duration}s ===")
        log.info(f"=== RESTORE COMPLETE | id={restore_id} duration={duration}s ===")

        logs = read_log(log_path)
        update_restore(restore_id,
            status="completed", error=None, logs=logs,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=duration
        )
        send_notification("Restore Successful \u2705", f"Restore for {remote_name} to {target_db and target_db.get('name')} completed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}. Duration: {duration}s", is_error=False, log_path=log_path)

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


# ─── SYNC HELPERS ────────────────────────────────────────────────────────────

def update_sync(sync_id: str, **kwargs):
    syncs = read_json("data/syncs.json")
    for s in syncs:
        if s["id"] == sync_id:
            s.update(kwargs)
            break
    write_json("data/syncs.json", syncs)


def run_mongo_sync(src_db: dict, tgt_db: dict, sync_job: dict, log_path: str, tracker: ProgressTracker):
    src_m = parse_mongo_uri(src_db)
    tgt_m = parse_mongo_uri(tgt_db)
    
    collection = sync_job.get("collection", "full")
    override_target_name = sync_job.get("override_target_name", "").strip()
    drop_existing = sync_job.get("drop_existing", False)
    
    cmd_dump = ["mongodump"] + mongo_cmd_args(src_m) + [
        f"--db={src_m['dbname']}",
        "--archive"
    ]
    if collection and collection != "full":
        cmd_dump += [f"--collection={collection}"]
        
    cmd_restore = ["mongorestore"] + mongo_cmd_args(tgt_m) + [
        "--archive"
    ]
    if drop_existing:
        cmd_restore.append("--drop")
        
    if override_target_name:
        cmd_restore.extend([
            f"--nsFrom={src_m['dbname']}.*",
            f"--nsTo={override_target_name}.*"
        ])
        
    write_log(log_path, f"INFO  Starting MongoDB Sync")
    
    creationflags = 0x00004000 if os.name == 'nt' else 0
    with open(log_path, "a") as log_file:
        p_dump = subprocess.Popen(cmd_dump, stdout=subprocess.PIPE, stderr=log_file, creationflags=creationflags)
        p_restore = subprocess.Popen(cmd_restore, stdin=subprocess.PIPE, stdout=log_file, stderr=subprocess.PIPE, creationflags=creationflags)
        
        # Track progress
        pipe_thread = threading.Thread(target=pipe_and_count, args=(p_dump.stdout, p_restore.stdin, tracker))
        pipe_thread.start()
        
        # Log mongorestore stderr
        stderr_thread = threading.Thread(target=log_and_parse_stderr, args=(p_restore.stderr, log_path, tracker))
        stderr_thread.daemon = True
        stderr_thread.start()
        
        pipe_thread.join()
        
        p_dump.wait()
        p_restore.wait()
        
        if p_restore.returncode != 0:
            raise RuntimeError(f"Mongo Sync failed with exit code {p_restore.returncode}")


def run_pg_sync(src_db: dict, tgt_db: dict, sync_job: dict, log_path: str, tracker: ProgressTracker):
    src_dbname = src_db.get("database_name", "")
    tgt_dbname = sync_job.get("override_target_name", "").strip() or tgt_db.get("database_name", "")
    collection = sync_job.get("collection", "full")
    drop_existing = sync_job.get("drop_existing", False)
    
    cmd_dump = ["pg_dump", "-h", src_db["host"], "-p", str(src_db["port"]), "-U", src_db["username"], "--format=custom", "-d", src_dbname]
    if collection and collection != "full":
        cmd_dump += ["-t", collection]
        
    cmd_restore = ["pg_restore", "-h", tgt_db["host"], "-p", str(tgt_db["port"]), "-U", tgt_db["username"], "-d", tgt_dbname]
    if drop_existing:
        cmd_restore.append("--clean")
        cmd_restore.append("--if-exists")
        
    src_env = pg_env(src_db)
    tgt_env = pg_env(tgt_db)
    
    write_log(log_path, f"INFO  Starting PostgreSQL Sync")
    
    creationflags = 0x00004000 if os.name == 'nt' else 0
    with open(log_path, "a") as log_file:
        p_dump = subprocess.Popen(cmd_dump, stdout=subprocess.PIPE, stderr=log_file, env=src_env, creationflags=creationflags)
        p_restore = subprocess.Popen(cmd_restore, stdin=subprocess.PIPE, stdout=log_file, stderr=log_file, env=tgt_env, creationflags=creationflags)
        
        pipe_thread = threading.Thread(target=pipe_and_count, args=(p_dump.stdout, p_restore.stdin, tracker))
        pipe_thread.start()
        pipe_thread.join()
        
        p_dump.wait()
        p_restore.wait()
        
        if p_restore.returncode not in (0, 1):
            raise RuntimeError(f"PostgreSQL Sync failed with exit code {p_restore.returncode}")


def do_sync(sync_id: str):
    start = datetime.utcnow()
    tmp_dir = os.path.join(BACKUP_TMP, f"sync_{sync_id}")
    os.makedirs(tmp_dir, exist_ok=True)
    log_path = get_log_path(f"sync_{sync_id}")

    write_log(log_path, f"=== SYNC START | id={sync_id} ===")

    syncs = read_json("data/syncs.json")
    sync_job = next((s for s in syncs if s["id"] == sync_id), None)
    if not sync_job:
        return

    dbs = read_json("data/databases.json")
    src_db = next((d for d in dbs if d["id"] == sync_job["source_database_id"]), None)
    tgt_db = next((d for d in dbs if d["id"] == sync_job["target_database_id"]), None)

    def fail(msg):
        log.error(f"=== SYNC FAILED | id={sync_id} | {msg} ===")
        write_log(log_path, f"ERROR {msg}")
        logs = read_log(log_path)
        update_sync(sync_id,
            status="failed", error=msg, logs=logs,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=int((datetime.utcnow() - start).total_seconds())
        )
        send_notification("Sync Failed \u274c", f"Job ID: {sync_id}\nError: {msg}", is_error=True, log_path=log_path)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not src_db or not tgt_db:
            return fail("Source or Target database not found")

        total_size = 0
        if src_db["type"] == "postgresql":
            total_size = get_pg_db_size(src_db, src_db.get("database_name", ""))
            write_log(log_path, f"INFO  Estimated source DB size: {round(total_size / (1024*1024), 2)} MB")

        tracker = ProgressTracker(sync_id, is_restore=False, total_size=total_size, log_path=log_path)
        
        # Override tracker.update_pct to update sync status
        def _update_sync_pct(pct: int):
            now = time.time()
            if pct != tracker.last_pct or now - tracker.last_update_time >= 1:
                tracker.last_pct = pct
                tracker.last_update_time = now
                update_sync(sync_id, progress=pct)
        tracker.update_pct = _update_sync_pct

        if src_db["type"] == "mongodb":
            run_mongo_sync(src_db, tgt_db, sync_job, log_path, tracker)
        elif src_db["type"] == "postgresql":
            run_pg_sync(src_db, tgt_db, sync_job, log_path, tracker)
        else:
            return fail(f"Unsupported DB type for sync: {src_db['type']}")

        tracker.update_pct(100)
        size_mb = round(tracker.bytes_processed / (1024 * 1024), 2)
        duration = int((datetime.utcnow() - start).total_seconds())
        
        write_log(log_path, f"=== SYNC COMPLETE | size={size_mb}MB | duration={duration}s ===")
        logs = read_log(log_path)
        
        update_sync(sync_id,
            status="completed", size_mb=size_mb, logs=logs, error=None,
            completed_at=datetime.utcnow().isoformat(),
            duration_seconds=duration,
            progress=100
        )
        send_notification("Sync Successful \u2705", f"Job ID: {sync_id}\nSize: {size_mb}MB\nDuration: {duration}s", is_error=False, log_path=log_path)

    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        _cleanup_old_logs()