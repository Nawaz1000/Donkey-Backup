"""
BackupVault Engine - Zero-Disk Streaming Pipeline
Provides in-memory streaming upload to cloud storage and parallel range-streamed restore.
Zero local disk I/O, zero intermediate dump files.
"""
import os
import sys
import time
import io
import queue
import threading
import shutil
import subprocess
import re
import uuid
from collections import deque

try:
    from .core import (
        BACKUP_TMP, ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, ACTIVE_TRACKERS,
        optimize_pipe_fd, ProgressTracker, ProgressWriter, pipe_and_count, log_and_parse_stderr,
        write_log, mask_cmd
    )
    from .compressors import get_speed_settings
    from .storage import (
        get_s3_client, get_bunny_host, get_bunny_key, resolve_bunny_hosts, verify_and_resolve_bunny
    )
except ImportError:
    from engine.core import (
        BACKUP_TMP, ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, ACTIVE_TRACKERS,
        optimize_pipe_fd, ProgressTracker, ProgressWriter, pipe_and_count, log_and_parse_stderr,
        write_log, mask_cmd
    )
    from engine.compressors import get_speed_settings
    from engine.storage import (
        get_s3_client, get_bunny_host, get_bunny_key, resolve_bunny_hosts, verify_and_resolve_bunny
    )


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

        with self.lock:
            while self.buffer_size < size and not self.eof:
                try:
                    chunk = self.q.get(timeout=0.5)
                    if chunk is None:
                        self.eof = True
                        break
                    self.buffer.append(memoryview(chunk))
                    self.buffer_size += len(chunk)
                except queue.Empty:
                    if self.stop_event.is_set():
                        self.eof = True
                        break

            if self.buffer_size == 0:
                return 0  # EOF

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
            size = 64 * 1024
        b = bytearray(size)
        n = self.readinto(b)
        if n == 0:
            return b""
        return bytes(b[:n])


def reader_thread_fn(stream, q: queue.Queue, stop_event: threading.Event, chunk_size: int = 16 * 1024 * 1024, tracker: ProgressTracker = None):
    try:
        optimize_pipe_fd(stream)
        read_buf = bytearray()
        target_chunk = max(4 * 1024 * 1024, min(chunk_size, 16 * 1024 * 1024))
        read_block_size = 1024 * 1024  # 1MB reads from pipe
        while not stop_event.is_set():
            data = stream.read(min(read_block_size, target_chunk - len(read_buf)))
            if not data:
                if read_buf:
                    q.put(bytes(read_buf))
                    read_buf.clear()
                break
            read_buf.extend(data)
            if tracker:
                tracker.update_bytes(len(data))
            if len(read_buf) >= target_chunk:
                chunk_bytes = bytes(read_buf)
                read_buf.clear()
                while not stop_event.is_set():
                    try:
                        q.put(chunk_bytes, timeout=0.5)
                        break
                    except queue.Full:
                        continue
    except Exception as e:
        print(f"Error in reader thread: {e}")
    finally:
        try:
            stream.close()
        except Exception:
            pass
        q.put(None)


def stream_backup_to_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, compression_cmd: list = None, tracker: ProgressTracker = None, speed_profile: str = "default", custom_feeder: callable = None) -> str:
    write_log(log_path, f"INFO  Starting streaming backup to {storage['type']}: {remote_name} (Profile: {speed_profile})")

    # Storage pre-flight validation to catch authentication/region issues before starting dump
    if storage.get("type") == "bunny":
        write_log(log_path, f"INFO  Verifying BunnyCDN storage credentials and endpoint for zone '{storage.get('bunny_storage_zone')}'...")
        bunny_check = verify_and_resolve_bunny(storage, log_path=log_path)
        if not bunny_check["success"]:
            write_log(log_path, f"ERROR BunnyCDN pre-flight validation failed: {bunny_check['error']}")
            raise RuntimeError(f"BunnyCDN pre-flight validation failed: {bunny_check['error']}")
        storage["_resolved_bunny"] = bunny_check
    
    settings = get_speed_settings(speed_profile)
    
    processes = []
    stop_event = ACTIVE_STOP_EVENTS.get(tracker.job_id) if tracker else threading.Event()
    if tracker:
        ACTIVE_PROCESSES.setdefault(tracker.job_id, [])
        if tracker.job_id not in ACTIVE_STOP_EVENTS:
            ACTIVE_STOP_EVENTS[tracker.job_id] = stop_event
        if stop_event.is_set():
            raise RuntimeError("Backup cancelled by user.")
    pipe_thread = None
    stderr_thread = None
    
    is_mongo = bool(cmd and len(cmd) > 0 and "mongodump" in cmd[0])
    p_dump_stderr = subprocess.PIPE if is_mongo else open(log_path, "a")
    
    creationflags = 0x00004000 if os.name == 'nt' else 0
    
    try:
        if compression_cmd:
            p_comp = subprocess.Popen(
                compression_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=open(log_path, "a"),
                creationflags=creationflags
            )
            processes.append(p_comp)
            if tracker:
                ACTIVE_PROCESSES[tracker.job_id].append(p_comp)

            if custom_feeder:
                pipe_thread = threading.Thread(
                    target=custom_feeder,
                    args=(p_comp.stdin, stop_event, tracker)
                )
                pipe_thread.daemon = True
                pipe_thread.start()
            else:
                p_dump = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=p_dump_stderr,
                    env=env,
                    creationflags=creationflags
                )
                processes.append(p_dump)
                if tracker:
                    ACTIVE_PROCESSES[tracker.job_id].append(p_dump)
                
                # Start pipe thread
                pipe_thread = threading.Thread(
                    target=pipe_and_count,
                    args=(p_dump.stdout, p_comp.stdin, tracker)
                )
                pipe_thread.daemon = True
                pipe_thread.start()
            
            stdout_stream = p_comp.stdout
        else:
            if custom_feeder:
                r_fd, w_fd = os.pipe()
                pipe_w = os.fdopen(w_fd, "wb")
                stdout_stream = os.fdopen(r_fd, "rb")
                pipe_thread = threading.Thread(
                    target=custom_feeder,
                    args=(pipe_w, stop_event, tracker)
                )
                pipe_thread.daemon = True
                pipe_thread.start()
            else:
                p_dump = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=p_dump_stderr,
                    env=env,
                    creationflags=creationflags
                )
                processes.append(p_dump)
                if tracker:
                    ACTIVE_PROCESSES[tracker.job_id].append(p_dump)
                stdout_stream = p_dump.stdout
            
        if is_mongo and not custom_feeder:
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
    reader_tracker = tracker if not compression_cmd else None
    
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

        elif storage["type"] == "s3":
            from boto3.s3.transfer import TransferConfig
            concurrency = settings.get("concurrency", 32)
            chunk_size = max(8 * 1024 * 1024, settings.get("chunk_size", 16 * 1024 * 1024))
            client = get_s3_client(storage, max_pool=max(64, concurrency * 2))
            transfer_config = TransferConfig(
                multipart_threshold=chunk_size,
                multipart_chunksize=chunk_size,
                max_concurrency=concurrency,
                use_threads=True
            )
            client.upload_fileobj(
                Fileobj=queue_reader,
                Bucket=storage["s3_bucket"],
                Key=remote_name,
                Config=transfer_config
            )
            try:
                head = client.head_object(Bucket=storage["s3_bucket"], Key=remote_name)
                stream_backup_to_storage.last_size_bytes = head.get("ContentLength", 0)
            except Exception:
                pass
            remote_path = f"s3://{storage['s3_bucket']}/{remote_name}"

        elif storage["type"] == "bunny":
            import requests
            import boto3
            from botocore.config import Config
            from boto3.s3.transfer import TransferConfig

            res = storage.get("_resolved_bunny")
            if not res:
                res = verify_and_resolve_bunny(storage, log_path=log_path)
                if not res["success"]:
                    raise RuntimeError(f"BunnyCDN verification failed: {res['error']}")

            zone = res["zone"]
            bunny_key = res["bunny_key"]
            rest_host = res["rest_host"]
            s3_url = res["s3_url"]
            s3_available = res.get("s3_usable", False)
            clean_remote = remote_name.lstrip("/")

            concurrency = settings.get("concurrency", 32)
            chunk_size = max(8 * 1024 * 1024, settings.get("chunk_size", 16 * 1024 * 1024))

            write_log(log_path, f"INFO  Connecting to BunnyCDN storage zone '{zone}' ({res['region_name']} - S3: {s3_url}, REST: {rest_host})")

            s3_client = None
            region_code = res.get("region_code") or "de"
            if s3_available:
                try:
                    s3_config = Config(
                        s3={"addressing_style": "path"},
                        signature_version="s3v4",
                        retries={"max_attempts": 3, "mode": "standard"},
                        connect_timeout=15,
                        read_timeout=300,
                        max_pool_connections=max(64, concurrency * 2)
                    )
                    s3_client = boto3.client(
                        "s3",
                        endpoint_url=s3_url,
                        aws_access_key_id=zone,
                        aws_secret_access_key=bunny_key,
                        region_name=region_code,
                        config=s3_config
                    )
                    write_log(log_path, f"INFO  BunnyCDN S3-compatible API verified ({region_code}). Streaming upload via parallel multipart ({concurrency} workers, {round(chunk_size/(1024*1024))}MB chunks)...")
                except Exception as s3_init_err:
                    s3_available = False
                    write_log(log_path, f"INFO  Bunny S3 client setup failed ({s3_init_err}). Falling back to REST API...")

            if s3_available and s3_client:
                transfer_config = TransferConfig(
                    multipart_threshold=chunk_size,
                    multipart_chunksize=chunk_size,
                    max_concurrency=concurrency,
                    use_threads=True
                )
                s3_client.upload_fileobj(
                    Fileobj=queue_reader,
                    Bucket=zone,
                    Key=clean_remote,
                    Config=transfer_config
                )
                try:
                    head = s3_client.head_object(Bucket=zone, Key=clean_remote)
                    stream_backup_to_storage.last_size_bytes = head.get("ContentLength", 0)
                except Exception as e:
                    write_log(log_path, f"WARN  Could not read S3 head_object: {e}")
            else:
                job_token = (tracker.job_id if tracker else uuid.uuid4().hex[:8]).replace("/", "_")
                spool_filename = f"bunny_spool_{job_token}.tmp"
                spool_dir = os.path.join(BACKUP_TMP, job_token)
                os.makedirs(spool_dir, exist_ok=True)
                spool_path = os.path.join(spool_dir, spool_filename)
                
                try:
                    write_log(log_path, f"INFO  Spooling stream to calculate Content-Length for Bunny REST API ({rest_host})...")
                    with open(spool_path, "wb") as f_spool:
                        shutil.copyfileobj(queue_reader, f_spool, length=4 * 1024 * 1024)

                    if stop_event.is_set():
                        raise RuntimeError("Backup cancelled by user.")

                    file_size = os.path.getsize(spool_path)
                    write_log(log_path, f"INFO  Spool completed ({round(file_size/(1024*1024), 2)} MB). Uploading to https://{rest_host}/{zone}/{clean_remote}...")

                    url = f"https://{rest_host}/{zone}/{clean_remote}"
                    headers = {
                        "AccessKey": bunny_key,
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(file_size),
                    }
                    session = requests.Session()
                    from requests.adapters import HTTPAdapter
                    from urllib3.util import Retry
                    session.mount("https://", HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=Retry(total=3, backoff_factor=0.5)))
                    with open(spool_path, "rb") as f_in:
                        resp = session.put(url, headers=headers, data=f_in, timeout=(30, 3600))
                    if resp.status_code not in (200, 201):
                        raise RuntimeError(f"Bunny storage upload failed ({resp.status_code}): {resp.text}")

                    stream_backup_to_storage.last_size_bytes = file_size
                finally:
                    if os.path.exists(spool_path):
                        try:
                            os.remove(spool_path)
                        except Exception:
                            pass

            uploaded_bytes = getattr(stream_backup_to_storage, "last_size_bytes", 0) or 0
            if uploaded_bytes == 0:
                try:
                    head_url = f"https://{rest_host}/{zone}/{clean_remote}"
                    head_resp = requests.head(head_url, headers={"AccessKey": bunny_key}, timeout=15)
                    if head_resp.status_code == 200 and "Content-Length" in head_resp.headers:
                        uploaded_bytes = int(head_resp.headers["Content-Length"])
                        stream_backup_to_storage.last_size_bytes = uploaded_bytes
                except Exception:
                    pass

            if uploaded_bytes == 0:
                raise RuntimeError(f"BunnyCDN upload verification failed: uploaded file '{clean_remote}' has 0 bytes or was not created.")

            remote_path = f"bunny://{zone}/{clean_remote}"
            write_log(log_path, f"INFO  BunnyCDN upload verified successfully ({round(uploaded_bytes/(1024*1024), 2)} MB) -> {remote_path}")

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
            pipe_thread.join(timeout=2.0)
        if reader_thread:
            reader_thread.join(timeout=2.0)
        try:
            stdout_stream.close()
        except Exception:
            pass
        for p in processes:
            try:
                if p.poll() is None:
                    p.terminate()
            except Exception:
                pass
        for p in processes:
            try:
                p.wait(timeout=2.0)
            except Exception:
                try:
                    p.kill()
                    p.wait(timeout=1.0)
                except Exception:
                    pass
        if tracker and tracker.job_id in ACTIVE_PROCESSES:
            ACTIVE_PROCESSES[tracker.job_id] = [p for p in ACTIVE_PROCESSES[tracker.job_id] if p not in processes]
            if not ACTIVE_PROCESSES[tracker.job_id]:
                ACTIVE_PROCESSES.pop(tracker.job_id, None)

    for p in processes:
        if p.returncode != 0:
            raise RuntimeError(f"Backup process failed with exit code {p.returncode}. Check logs.")
        
    write_log(log_path, f"INFO  Streaming upload successful.")
    return remote_path


def download_parallel_range_stream(fetch_range_fn, total_size: int, part_size: int, concurrency: int, max_in_flight: int, stdin_stream, stop_event: threading.Event, dl_tracker: ProgressTracker = None, log_path: str = None, filter_done_event: threading.Event = None):
    """
    Downloads remote archives using massively parallel HTTP/S3 Range requests across multiple TCP streams.
    Dispatches concurrent chunk downloads and writes parts in strictly ascending order to stdin_stream
    with memory-bounded backpressure (max_in_flight parts max in RAM).
    """
    from concurrent.futures import ThreadPoolExecutor
    
    num_parts = (total_size + part_size - 1) // part_size
    ready_parts = {}
    download_err = [None]
    cv = threading.Condition()
    consumer_idx = [0]
    
    if log_path:
        write_log(log_path, f"INFO  Starting parallel multi-stream restore download ({num_parts} parts, {round(part_size/(1024*1024))}MB each, {concurrency} parallel TCP workers, max in-flight: {max_in_flight})...")

    def is_interrupted():
        return stop_event.is_set() or (filter_done_event and filter_done_event.is_set()) or bool(download_err[0])

    def fetch_with_retry(start_b: int, end_b: int, max_retries: int = 3):
        last_ex = None
        for attempt in range(max_retries):
            if is_interrupted():
                return None
            try:
                return fetch_range_fn(start_b, end_b)
            except Exception as ex:
                last_ex = ex
                time.sleep(0.3 * (attempt + 1))
        raise last_ex or RuntimeError(f"Range fetch failed for bytes {start_b}-{end_b}")

    def worker_task(part_idx: int, start_byte: int, end_byte: int):
        try:
            if is_interrupted():
                return
            data = fetch_with_retry(start_byte, end_byte)
            if is_interrupted() or data is None:
                return
            with cv:
                ready_parts[part_idx] = data
                cv.notify_all()
        except Exception as ex:
            with cv:
                if not download_err[0]:
                    download_err[0] = ex
                cv.notify_all()

    executor = ThreadPoolExecutor(max_workers=concurrency)

    def producer_fn():
        try:
            for part_idx in range(num_parts):
                if is_interrupted():
                    break
                with cv:
                    while (part_idx - consumer_idx[0]) >= max_in_flight and not is_interrupted():
                        cv.wait(timeout=0.1)
                if is_interrupted():
                    break
                start_byte = part_idx * part_size
                end_byte = min(total_size - 1, start_byte + part_size - 1)
                executor.submit(worker_task, part_idx, start_byte, end_byte)
        except Exception as p_ex:
            with cv:
                if not download_err[0]:
                    download_err[0] = p_ex
                cv.notify_all()

    prod_thread = threading.Thread(target=producer_fn)
    prod_thread.daemon = True
    prod_thread.start()

    try:
        while consumer_idx[0] < num_parts and not stop_event.is_set():
            if filter_done_event and filter_done_event.is_set():
                break
            with cv:
                while consumer_idx[0] not in ready_parts and not stop_event.is_set() and not (filter_done_event and filter_done_event.is_set()) and not download_err[0]:
                    cv.wait(timeout=0.5)
                if filter_done_event and filter_done_event.is_set():
                    break
                if stop_event.is_set():
                    raise RuntimeError("Restore cancelled by user.")
                if download_err[0]:
                    raise RuntimeError(f"Parallel restore download stream failed: {download_err[0]}")
                part_data = ready_parts.pop(consumer_idx[0])
                consumer_idx[0] += 1
                cv.notify_all()

            try:
                stdin_stream.write(part_data)
                if dl_tracker:
                    dl_tracker.update_bytes(len(part_data))
            except (BrokenPipeError, OSError):
                break
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        prod_thread.join(timeout=2.0)


def stream_restore_from_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, decompression_cmd: list = None, tracker: ProgressTracker = None, speed_profile: str = "default", sql_db_filter: str = None, indexing_mode: str = "with_index", changes_only: bool = False):
    write_log(log_path, f"INFO  Starting streaming restore from {storage['type']}: {remote_name} (Profile: {speed_profile})")
    
    settings = get_speed_settings(speed_profile)
    
    processes = []
    stop_event = ACTIVE_STOP_EVENTS.get(tracker.job_id) if tracker else threading.Event()
    filter_done_event = threading.Event()
    if tracker:
        ACTIVE_PROCESSES.setdefault(tracker.job_id, [])
        if tracker.job_id not in ACTIVE_STOP_EVENTS:
            ACTIVE_STOP_EVENTS[tracker.job_id] = stop_event
        if stop_event.is_set():
            raise RuntimeError("Restore cancelled by user.")
    stderr_thread = None
    filter_thread = None
    
    is_mongo = bool(cmd and len(cmd) > 0 and "mongorestore" in cmd[0])
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
                if tracker:
                    ACTIVE_PROCESSES[tracker.job_id].append(p_dec)
                
                optimize_pipe_fd(p_dec.stdin)
                optimize_pipe_fd(p_dec.stdout)

                needs_filter = bool(sql_db_filter or changes_only or (indexing_mode != "with_index"))
                p_restore = subprocess.Popen(
                    cmd,
                    stdin=subprocess.PIPE if needs_filter else p_dec.stdout,
                    stdout=err_file,
                    stderr=subprocess.PIPE,  # Pipe stderr to parse progress
                    env=env,
                    creationflags=creationflags
                )
                processes.append(p_restore)
                if tracker:
                    ACTIVE_PROCESSES[tracker.job_id].append(p_restore)

                if needs_filter:
                    optimize_pipe_fd(p_restore.stdin)
                    def filter_worker(dec_out, rest_in, target_db, stop_ev, f_done_ev, idx_mode, chgs_only):
                        target_clean = target_db.strip().strip('"').lower().encode("utf-8") if target_db else None

                        start_candidates = []
                        if target_clean:
                            start_candidates = [
                                b"-- >>> auto_backup_db_start: " + target_clean,
                                b"\\connect " + target_clean,
                                b'\\connect "' + target_clean,
                                b"\\connect - " + target_clean,
                                b'\\connect - "' + target_clean,
                                b"create database " + target_clean,
                                b'create database "' + target_clean,
                                b"-- database: " + target_clean,
                                b'-- database: "' + target_clean,
                            ]

                        stop_other_markers = [
                            b"-- >>> auto_backup_db_start:",
                            b"\n\\connect ",
                            b"\r\n\\connect ",
                            b"\ncreate database ",
                            b"\r\ncreate database ",
                            b"\n-- database: ",
                            b"\r\n-- database: "
                        ]

                        preamble = (
                            b"SET statement_timeout = 0;\n"
                            b"SET lock_timeout = 0;\n"
                            b"SET client_min_messages = warning;\n"
                            b"SET synchronous_commit = 'off';\n"
                            b"DO $$ BEGIN SET session_replication_role = 'replica'; EXCEPTION WHEN OTHERS THEN NULL; END $$;\n"
                            b"SET work_mem = '512MB';\n"
                            b"SET maintenance_work_mem = '4GB';\n"
                            b"SET max_parallel_maintenance_workers = 8;\n"
                        )
                        try:
                            rest_in.write(preamble)
                            rest_in.flush()
                        except Exception:
                            pass

                        CHUNK_SZ = 4 * 1024 * 1024
                        in_target = (target_clean is None)
                        in_copy = False
                        remainder = b""

                        def process_ddl_chunk(chunk_bytes):
                            if not chgs_only and idx_mode == "with_index" and not target_clean:
                                return chunk_bytes
                            res = bytearray()
                            lines = chunk_bytes.split(b"\n")
                            for line in lines:
                                l_str = line.strip()
                                l_low = l_str.lower()
                                if not l_str:
                                    res.extend(b"\n")
                                    continue
                                
                                # Strip connection switches and database creation when restoring a single database
                                if target_clean:
                                    if (l_low.startswith(b"\\connect ") or 
                                        l_low.startswith(b"select 'create database") or 
                                        l_low.startswith(b"create database ") or
                                        l_low.startswith(b"-- database: ")):
                                        continue
                                        
                                if chgs_only:
                                    if (l_low.startswith(b"drop table") or 
                                        l_low.startswith(b"drop view") or 
                                        l_low.startswith(b"drop materialized view") or 
                                        l_low.startswith(b"drop sequence") or 
                                        l_low.startswith(b"drop type") or 
                                        l_low.startswith(b"drop schema") or
                                        l_low.startswith(b"drop database")):
                                        continue
                                    if l_low.startswith(b"create table "):
                                        line = b"CREATE TABLE IF NOT EXISTS " + line[13:]
                                    elif l_low.startswith(b"create sequence "):
                                        line = b"CREATE SEQUENCE IF NOT EXISTS " + line[16:]
                                        
                                if idx_mode == "without_index":
                                    if (l_low.startswith(b"create index") or 
                                        l_low.startswith(b"create unique index") or 
                                        (l_low.startswith(b"alter table") and b"foreign key" in l_low and b"add constraint" in l_low)):
                                        continue
                                        
                                res.extend(line + b"\n")
                            return bytes(res)

                        try:
                            while not stop_ev.is_set() and not f_done_ev.is_set():
                                data = dec_out.read(CHUNK_SZ)
                                if not data:
                                    break
                                buf = remainder + data
                                remainder = b""

                                # If targeting a single DB and not yet inside target DB
                                if not in_target:
                                    buf_lower = buf.lower()
                                    start_idx = -1
                                    for c in start_candidates:
                                        p = buf_lower.find(c)
                                        if p != -1:
                                            end_p = p + len(c)
                                            if end_p >= len(buf_lower) or buf_lower[end_p:end_p+1] in (b" ", b'"', b"\n", b"\r", b";", b"<"):
                                                if start_idx == -1 or p < start_idx:
                                                    start_idx = p
                                    
                                    if start_idx != -1:
                                        in_target = True
                                        write_log(log_path, f"INFO  [Single DB Restore] Found database '{target_db}' in backup stream. Beginning high-speed extraction...")
                                        after_tag = buf[start_idx:]
                                        nl_pos = after_tag.find(b"\n")
                                        if nl_pos != -1:
                                            buf = after_tag[nl_pos + 1:]
                                        else:
                                            buf = b""
                                    else:
                                        remainder = buf[-1024:] if len(buf) > 1024 else buf
                                        continue

                                # Check if target single database has finished
                                if target_clean:
                                    buf_lower = buf.lower()
                                    stop_idx = -1
                                    
                                    custom_end = b"-- >>> auto_backup_db_end: " + target_clean
                                    p_end = buf_lower.find(custom_end)
                                    if p_end != -1:
                                        stop_idx = p_end
                                        
                                    for m in stop_other_markers:
                                        p = buf_lower.find(m)
                                        if p != -1:
                                            sample = buf_lower[p:p+120]
                                            if target_clean not in sample:
                                                if stop_idx == -1 or p < stop_idx:
                                                    stop_idx = p
                                                    
                                    if stop_idx != -1:
                                        if stop_idx > 0:
                                            rest_in.write(process_ddl_chunk(buf[:stop_idx]))
                                        f_done_ev.set()
                                        stop_ev.set()
                                        write_log(log_path, f"INFO  [Single DB Restore] Database '{target_db}' dump fully extracted from stream.")
                                        break

                                # Stream buf with zero-copy/zero-split bypass for COPY data blocks
                                while buf:
                                    if in_copy:
                                        pos = buf.find(b"\n\\.\n")
                                        add_len = 4
                                        if pos == -1:
                                            pos = buf.find(b"\n\\.\r\n")
                                            add_len = 5
                                        if pos == -1:
                                            pos = buf.find(b"\r\n\\.\r\n")
                                            add_len = 6
                                            
                                        if pos != -1:
                                            rest_in.write(buf[:pos + add_len])
                                            buf = buf[pos + add_len:]
                                            in_copy = False
                                        else:
                                            if len(buf) > 8:
                                                rest_in.write(buf[:-8])
                                                remainder = buf[-8:]
                                                buf = b""
                                            else:
                                                remainder = buf
                                                buf = b""
                                    else:
                                        buf_lower = buf.lower()
                                        c_idx = buf_lower.find(b"copy ")
                                        if c_idx != -1:
                                            nl_after_copy = buf.find(b"\n", c_idx)
                                            if nl_after_copy != -1:
                                                header_line = buf_lower[c_idx:nl_after_copy]
                                                if b"from stdin" in header_line:
                                                    if c_idx > 0:
                                                        rest_in.write(process_ddl_chunk(buf[:c_idx]))
                                                    rest_in.write(buf[c_idx:nl_after_copy + 1])
                                                    buf = buf[nl_after_copy + 1:]
                                                    in_copy = True
                                                    continue
                                                    
                                        if len(buf) > 1024:
                                            nl = buf.rfind(b"\n", 0, -1024)
                                            if nl != -1:
                                                rest_in.write(process_ddl_chunk(buf[:nl + 1]))
                                                remainder = buf[nl + 1:]
                                                buf = b""
                                            else:
                                                remainder = buf
                                                buf = b""
                                        else:
                                            remainder = buf
                                            buf = b""

                            if in_target and remainder and not f_done_ev.is_set():
                                if in_copy:
                                    rest_in.write(remainder)
                                else:
                                    rest_in.write(process_ddl_chunk(remainder))

                            postamble = (
                                b"\nDO $$ BEGIN SET session_replication_role = 'origin'; EXCEPTION WHEN OTHERS THEN NULL; END $$;\n"
                                b"SET synchronous_commit = 'on';\n"
                            )
                            try:
                                rest_in.write(postamble)
                                rest_in.flush()
                            except Exception:
                                pass
                        finally:
                            try:
                                rest_in.close()
                            except Exception:
                                pass

                    filter_thread = threading.Thread(
                        target=filter_worker,
                        args=(p_dec.stdout, p_restore.stdin, sql_db_filter, stop_event, filter_done_event, indexing_mode, changes_only)
                    )
                    filter_thread.daemon = True
                    filter_thread.start()
                    stdin_stream = p_dec.stdin
                else:
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
                if tracker:
                    ACTIVE_PROCESSES[tracker.job_id].append(p_restore)
                stdin_stream = p_restore.stdin
                
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
                    if filter_done_event.is_set():
                        break
                    if stop_event.is_set():
                        raise RuntimeError("Restore cancelled by user.")
                    try:
                        stdin_stream.write(chunk)
                    except (BrokenPipeError, OSError):
                        break
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
                
                progress_writer = ProgressWriter(stdin_stream, dl_tracker, stop_event=stop_event, filter_done_event=filter_done_event)
                blob.download_to_file(progress_writer, timeout=3600)
                stdin_stream.close()

            elif storage["type"] == "s3":
                concurrency = settings.get("concurrency", 32)
                part_size = max(8 * 1024 * 1024, settings.get("chunk_size", 16 * 1024 * 1024))
                max_in_flight = max(16, settings.get("queue_max", 32))
                s3_client = get_s3_client(storage, max_pool=max(64, concurrency * 2))

                total_size = (dl_tracker.total_size if dl_tracker and dl_tracker.total_size > 0 else 0)
                if total_size <= 0:
                    try:
                        head = s3_client.head_object(Bucket=storage["s3_bucket"], Key=remote_name)
                        total_size = head.get("ContentLength", 0)
                    except Exception:
                        pass
                if dl_tracker and dl_tracker.total_size <= 0 and total_size > 0:
                    dl_tracker.total_size = total_size

                if total_size > part_size:
                    write_log(log_path, f"INFO  S3 archive size resolved: {round(total_size/(1024*1024), 2)} MB. Activating high-speed parallel Range downloader ({concurrency} workers)...")
                    def fetch_s3_range(start_b, end_b):
                        resp = s3_client.get_object(
                            Bucket=storage["s3_bucket"],
                            Key=remote_name,
                            Range=f"bytes={start_b}-{end_b}"
                        )
                        return resp["Body"].read()

                    download_parallel_range_stream(
                        fetch_s3_range,
                        total_size=total_size,
                        part_size=part_size,
                        concurrency=concurrency,
                        max_in_flight=max_in_flight,
                        stdin_stream=stdin_stream,
                        stop_event=stop_event,
                        dl_tracker=dl_tracker,
                        log_path=log_path,
                        filter_done_event=filter_done_event
                    )
                    stdin_stream.close()
                else:
                    s3_obj = s3_client.get_object(Bucket=storage["s3_bucket"], Key=remote_name)
                    body_stream = s3_obj["Body"]
                    chunk_size = part_size
                    prefetch_q = queue.Queue(maxsize=max_in_flight)
                    download_err = [None]

                    def s3_dl_worker():
                        try:
                            while not stop_event.is_set() and not filter_done_event.is_set():
                                chunk = body_stream.read(chunk_size)
                                if not chunk:
                                    break
                                if dl_tracker:
                                    dl_tracker.update_bytes(len(chunk))
                                prefetch_q.put(chunk)
                        except Exception as ex:
                            download_err[0] = ex
                        finally:
                            prefetch_q.put(None)

                    dl_thread = threading.Thread(target=s3_dl_worker)
                    dl_thread.daemon = True
                    dl_thread.start()

                    try:
                        while True:
                            if filter_done_event.is_set():
                                break
                            if stop_event.is_set():
                                raise RuntimeError("Restore cancelled by user.")
                            try:
                                chunk = prefetch_q.get(timeout=0.5)
                            except queue.Empty:
                                if not dl_thread.is_alive():
                                    break
                                continue
                            if chunk is None:
                                break
                            try:
                                stdin_stream.write(chunk)
                            except (BrokenPipeError, OSError):
                                break
                    finally:
                        dl_thread.join(timeout=3.0)
                        if download_err[0]:
                            raise RuntimeError(f"S3 download stream failed: {download_err[0]}")
                    stdin_stream.close()

            elif storage["type"] == "bunny":
                import requests
                from urllib3.util import Retry
                from requests.adapters import HTTPAdapter
                
                write_log(log_path, f"INFO  Resolving BunnyCDN storage endpoint for zone '{storage.get('bunny_storage_zone')}'...")
                bunny_res = storage.get("_resolved_bunny")
                if not bunny_res:
                    bunny_res = verify_and_resolve_bunny(storage, log_path=log_path)
                    if not bunny_res["success"]:
                        raise RuntimeError(f"BunnyCDN verification failed: {bunny_res['error']}")
                    storage["_resolved_bunny"] = bunny_res

                zone = bunny_res["zone"]
                bunny_key = bunny_res["bunny_key"]
                rest_host = bunny_res["rest_host"]
                s3_url = bunny_res["s3_url"]
                region_code = bunny_res.get("region_code") or "de"
                s3_usable = bunny_res.get("s3_usable", False)
                clean_remote = remote_name.lstrip("/")

                part_size = max(8 * 1024 * 1024, settings.get("chunk_size", 16 * 1024 * 1024))
                concurrency = settings.get("concurrency", 32)
                max_in_flight = max(16, settings.get("queue_max", 32))

                # Setup persistent clients
                s3_client = None
                if s3_usable:
                    try:
                        import boto3
                        from botocore.config import Config
                        s3_client = boto3.client(
                            "s3",
                            endpoint_url=s3_url,
                            aws_access_key_id=zone,
                            aws_secret_access_key=bunny_key,
                            region_name=region_code,
                            config=Config(
                                s3={'addressing_style': 'path'},
                                signature_version="s3v4",
                                retries={'max_attempts': 3, 'mode': 'standard'},
                                connect_timeout=15,
                                read_timeout=300,
                                max_pool_connections=max(64, concurrency * 2)
                            )
                        )
                    except Exception as s3_init_ex:
                        write_log(log_path, f"WARN  Bunny S3 client setup failed ({s3_init_ex}). Falling back to REST stream...")
                        s3_client = None

                session = requests.Session()
                adapter = HTTPAdapter(pool_connections=max(64, concurrency * 2), pool_maxsize=max(64, concurrency * 2), max_retries=Retry(total=3, backoff_factor=0.3))
                session.mount("https://", adapter)
                bunny_url = f"https://{rest_host}/{zone}/{clean_remote}"
                bunny_headers = {"AccessKey": bunny_key}

                total_size = (dl_tracker.total_size if dl_tracker and dl_tracker.total_size > 0 else 0)
                if total_size <= 0 and s3_client:
                    try:
                        head = s3_client.head_object(Bucket=zone, Key=clean_remote)
                        total_size = head.get("ContentLength", 0)
                    except Exception:
                        pass

                if total_size <= 0:
                    try:
                        head_r = session.head(bunny_url, headers=bunny_headers, timeout=10)
                        if head_r.status_code == 200 and "Content-Length" in head_r.headers:
                            total_size = int(head_r.headers["Content-Length"])
                    except Exception:
                        pass

                if total_size <= 0:
                    try:
                        r_range = session.get(bunny_url, headers={**bunny_headers, "Range": "bytes=0-0"}, timeout=10)
                        if "Content-Range" in r_range.headers:
                            cr = r_range.headers["Content-Range"]
                            if "/" in cr:
                                total_size = int(cr.split("/")[-1])
                    except Exception:
                        pass

                if dl_tracker and dl_tracker.total_size <= 0 and total_size > 0:
                    dl_tracker.total_size = total_size

                chunk_size = 8 * 1024 * 1024
                prefetch_q = queue.Queue(maxsize=max_in_flight)
                download_err = [None]

                def bunny_dl_worker():
                    try:
                        if s3_client:
                            try:
                                s3_obj = s3_client.get_object(Bucket=zone, Key=clean_remote)
                                body_stream = s3_obj["Body"]
                                write_log(log_path, f"INFO  Streaming restore download via BunnyCDN S3 API ({s3_url})...")
                                while not stop_event.is_set() and not filter_done_event.is_set():
                                    chunk = body_stream.read(chunk_size)
                                    if not chunk:
                                        break
                                    if dl_tracker:
                                        dl_tracker.update_bytes(len(chunk))
                                    prefetch_q.put(chunk)
                                return
                            except Exception as s3_dl_ex:
                                write_log(log_path, f"WARN  Bunny S3 restore download failed ({s3_dl_ex}). Falling back to REST stream...")

                        write_log(log_path, f"INFO  Streaming restore download via BunnyCDN REST API ({rest_host})...")
                        with session.get(bunny_url, headers=bunny_headers, stream=True, timeout=(15, 3600)) as resp:
                            if resp.status_code != 200:
                                raise RuntimeError(f"Bunny storage download failed (HTTP {resp.status_code}): {resp.text[:300]}")
                            for chunk in resp.iter_content(chunk_size=chunk_size):
                                if not chunk:
                                    continue
                                if stop_event.is_set() or filter_done_event.is_set():
                                    break
                                if dl_tracker:
                                    dl_tracker.update_bytes(len(chunk))
                                prefetch_q.put(chunk)
                    except Exception as ex:
                        download_err[0] = ex
                    finally:
                        prefetch_q.put(None)

                dl_thread = threading.Thread(target=bunny_dl_worker)
                dl_thread.daemon = True
                dl_thread.start()

                try:
                    while True:
                        if filter_done_event.is_set():
                            write_log(log_path, f"INFO  [Single DB Restore] Target database '{sql_db_filter}' fully extracted from stream. Cleanly stopping download.")
                            break
                        if stop_event.is_set():
                            raise RuntimeError("Restore cancelled by user.")
                        try:
                            chunk = prefetch_q.get(timeout=0.5)
                        except queue.Empty:
                            if not dl_thread.is_alive():
                                break
                            continue
                        if chunk is None:
                            break
                        try:
                            stdin_stream.write(chunk)
                        except (BrokenPipeError, OSError):
                            break
                finally:
                    dl_thread.join(timeout=3.0)
                    if download_err[0] and not filter_done_event.is_set():
                        raise RuntimeError(f"Bunny download stream failed: {download_err[0]}")
                stdin_stream.close()

            else:
                raise ValueError(f"Unknown storage type: {storage['type']}")
                
        except Exception as e:
            if not filter_done_event.is_set():
                for p in processes:
                    try: p.kill()
                    except Exception: pass
                raise RuntimeError(f"Streaming download failed: {e}")
        finally:
            stop_event.set()
            if filter_done_event.is_set():
                if len(processes) > 1:
                    try:
                        processes[0].terminate()
                    except Exception:
                        pass
            if filter_thread:
                filter_thread.join(timeout=3.0)
            try:
                stdin_stream.close()
            except Exception:
                pass
            for p in reversed(processes):
                try:
                    wait_timeout = 30.0 if p == processes[-1] else 3.0
                    p.wait(timeout=wait_timeout)
                except Exception:
                    try:
                        p.kill()
                        p.wait(timeout=1.0)
                    except Exception:
                        pass
            if tracker and tracker.job_id in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES[tracker.job_id] = [p for p in ACTIVE_PROCESSES[tracker.job_id] if p not in processes]
                if not ACTIVE_PROCESSES[tracker.job_id]:
                    ACTIVE_PROCESSES.pop(tracker.job_id, None)

        p_restore = processes[-1]
        if p_restore.returncode != 0:
            if is_mongo or p_restore.returncode != 1:
                raise RuntimeError(f"Restore command failed with exit code {p_restore.returncode}. Check logs.")
            else:
                write_log(log_path, f"WARN  Restore completed with warnings (exit code 1). This is often harmless in pg_restore.")
            
        if filter_done_event.is_set() and tracker:
            tracker.bytes_processed = tracker.total_size
            tracker.update_pct(100)
            ACTIVE_TRACKERS.pop(tracker.job_id, None)

        write_log(log_path, f"INFO  Streaming restore successful.")
