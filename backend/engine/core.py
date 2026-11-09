"""
BackupVault Engine - Core Process, Progress, Logging, and Pipe Management
"""
import os
import sys
import time
import re
import shutil
import logging
import tempfile
import uuid
import threading
import subprocess
from collections import deque
from datetime import datetime

try:
    import fcntl
except ImportError:
    fcntl = None

from utils import read_json, write_json

log = logging.getLogger("backupvault")

BACKUP_TMP = "/tmp/backupvault"
os.makedirs(BACKUP_TMP, exist_ok=True)

ACTIVE_PROCESSES = {}
ACTIVE_STOP_EVENTS = {}
ACTIVE_TRACKERS = {}


# ─── STATE HELPERS ───────────────────────────────────────────────────────────

def update_backup(backup_id: str, **kwargs):
    from database import db_manager
    if not db_manager.update_item_atomic("backups", backup_id, **kwargs):
        backups = read_json("data/backups.json")
        for b in backups:
            if b["id"] == backup_id:
                b.update(kwargs)
                break
        write_json("data/backups.json", backups)


def update_restore(restore_id: str, **kwargs):
    from database import db_manager
    if not db_manager.update_item_atomic("restores", restore_id, **kwargs):
        restores = read_json("data/restores.json")
        for r in restores:
            if r["id"] == restore_id:
                r.update(kwargs)
                break
        write_json("data/restores.json", restores)


def update_sync(sync_id: str, **kwargs):
    from database import db_manager
    if not db_manager.update_item_atomic("syncs", sync_id, **kwargs):
        syncs = read_json("data/syncs.json")
        for s in syncs:
            if s["id"] == sync_id:
                s.update(kwargs)
                break
        write_json("data/syncs.json", syncs)


# ─── PROCESS & JOB CONTROLS ──────────────────────────────────────────────────

def cancel_job(job_id: str) -> bool:
    found = False
    ACTIVE_TRACKERS.pop(job_id, None)
    if job_id in ACTIVE_STOP_EVENTS:
        try:
            ACTIVE_STOP_EVENTS[job_id].set()
            found = True
        except Exception:
            pass

    if job_id in ACTIVE_PROCESSES:
        for p in ACTIVE_PROCESSES[job_id]:
            try:
                if p.poll() is None:
                    p.terminate()
            except Exception:
                pass
        for p in ACTIVE_PROCESSES[job_id]:
            try:
                p.wait(timeout=1.0)
            except Exception:
                try:
                    p.kill()
                    p.wait(timeout=1.0)
                except Exception:
                    pass
        found = True
        ACTIVE_PROCESSES.pop(job_id, None)

    ACTIVE_STOP_EVENTS.pop(job_id, None)
    return found


def kill_stale_backup_subprocesses() -> int:
    """
    Terminates and reaps any orphaned backup/compression CLI processes
    (e.g., pg_dump, pg_dumpall, zstd, pigz, mongodump, mongorestore, psql)
    left behind by aborted, crashed, or interrupted jobs.
    Returns the count of killed stale processes.
    """
    killed_count = 0
    target_names = {"pg_dump", "pg_dumpall", "zstd", "pigz", "mongodump", "mongorestore"}

    # 1. Kill any active processes tracked in memory
    for j_id in list(ACTIVE_PROCESSES.keys()):
        try:
            cancel_job(j_id)
            killed_count += 1
        except Exception:
            pass
    ACTIVE_PROCESSES.clear()
    ACTIVE_STOP_EVENTS.clear()

    # 2. On Linux, scan /proc for any orphaned tool processes running without an active job
    if os.path.exists("/proc"):
        my_pid = os.getpid()
        import signal
        try:
            for pid_dir in os.listdir("/proc"):
                if not pid_dir.isdigit():
                    continue
                pid = int(pid_dir)
                if pid == my_pid or pid == 1:
                    continue
                comm_path = f"/proc/{pid}/comm"
                if os.path.exists(comm_path):
                    try:
                        with open(comm_path, "r") as f:
                            comm = f.read().strip()
                        if comm in target_names:
                            log.warning(f"Reaping stale orphan backup process: {comm} (PID {pid})")
                            try:
                                os.kill(pid, signal.SIGTERM)
                                time.sleep(0.05)
                                os.kill(pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            killed_count += 1
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"Could not inspect /proc for stale processes: {e}")

    return killed_count


def mongo_cmd_args(m: dict) -> list:
    args = [
        f"--host={m['host']}",
        f"--port={m['port']}",
        f"--authenticationDatabase={m['auth_source']}",
    ]
    if m.get("username"):
        args += [f"--username={m['username']}"]
    if m.get("password"):
        args += [f"--password={m['password']}"]
    if m.get("auth_mechanism"):
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
    try:
        with open(log_path, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')} {msg}\n")
    except Exception:
        pass


def read_log(log_path: str) -> str:
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception:
        return ""


def _cleanup_old_logs(max_days: int = 30):
    """Delete log files in BACKUP_TMP older than max_days to prevent disk bloat."""
    try:
        now = time.time()
        cutoff = now - (max_days * 86400)
        if os.path.exists(BACKUP_TMP):
            for fname in os.listdir(BACKUP_TMP):
                if fname.endswith(".log"):
                    fpath = os.path.join(BACKUP_TMP, fname)
                    try:
                        if os.path.getmtime(fpath) < cutoff:
                            os.remove(fpath)
                    except Exception:
                        pass
    except Exception as e:
        log.warning(f"Failed to cleanup old logs: {e}")


# ─── PIPE OPTIMIZATION & PROGRESS TRACKING ───────────────────────────────────

def optimize_pipe_fd(stream_or_fd):
    """
    On Linux, increase pipe capacity from standard 64KB to 1MB (1048576 bytes)
    to minimize context switching between subprocesses in the streaming pipeline.
    """
    if not fcntl or not hasattr(fcntl, "F_SETPIPE_SZ"):
        return
    try:
        fd = stream_or_fd.fileno() if hasattr(stream_or_fd, "fileno") else stream_or_fd
        fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, 1048576)
    except Exception:
        pass


class ProgressTracker:
    def __init__(self, job_id: str, is_restore: bool = False, total_size: int = 0, log_path: str = None):
        self.job_id = job_id
        self.is_restore = is_restore
        self.total_size = total_size
        self.log_path = log_path
        self.bytes_processed = 0
        self.last_pct = -1
        self.last_update_time = 0
        self.start_time = time.time()
        self.transfer_start_time = time.time()
        self.speed_mbps = 0.0
        self.rate_window = deque()  # stores (timestamp, cumulative_bytes)
        ACTIVE_TRACKERS[self.job_id] = self

    def update_bytes(self, num_bytes: int):
        now = time.time()
        if self.transfer_start_time is None:
            self.transfer_start_time = now
            self.rate_window.append((now, self.bytes_processed))

        self.bytes_processed += num_bytes
        self.rate_window.append((now, self.bytes_processed))

        # Maintain sliding window of last 1.5 - 2.0 seconds for high-precision real-time rate
        while len(self.rate_window) > 2 and (now - self.rate_window[0][0]) > 2.0:
            self.rate_window.popleft()

        window_dt = now - self.rate_window[0][0]
        if window_dt >= 0.1:
            window_bytes = self.bytes_processed - self.rate_window[0][1]
            raw_instant_speed = (window_bytes / (1024 * 1024)) / max(0.01, window_dt)
            if self.speed_mbps <= 0.0:
                self.speed_mbps = round(raw_instant_speed, 1)
            else:
                self.speed_mbps = round(0.7 * raw_instant_speed + 0.3 * self.speed_mbps, 1)
        else:
            elapsed = max(0.1, now - self.transfer_start_time)
            self.speed_mbps = round((self.bytes_processed / (1024 * 1024)) / elapsed, 1)

        if self.total_size > 0:
            pct = min(99, int((self.bytes_processed / self.total_size) * 100))
            self.update_pct(pct)
        elif (now - self.last_update_time) >= 1.0:
            self.update_pct(self.last_pct if self.last_pct >= 0 else 0)

    def update_pct(self, pct: int):
        now = time.time()
        time_diff = now - self.last_update_time
        # Throttle updates: run if completed (pct=100), or every 1.0s, or if pct changed and at least 0.4s elapsed
        if pct == 100 or time_diff >= 1.0 or (pct != self.last_pct and time_diff >= 0.4):
            self.last_pct = pct
            self.last_update_time = now
            
            job_type = "RESTORE" if self.is_restore else "BACKUP"
            processed_mb = round(self.bytes_processed / (1024 * 1024), 2)
            total_mb = round(self.total_size / (1024 * 1024), 2) if self.total_size > 0 else 0
            speed_text = f" at {self.speed_mbps} MB/s" if self.speed_mbps > 0 else ""
            log_msg = f"[{job_type} PROGRESS] ID: {self.job_id} | Progress: {pct}% | Processed: {processed_mb} MB{speed_text}"
            log.info(log_msg)
            
            if self.log_path:
                write_log(self.log_path, f"INFO  Progress: {pct}% ({processed_mb} MB processed{speed_text})")
                
            if self.is_restore:
                update_restore(self.job_id, progress_percentage=pct, processed_mb=processed_mb, speed_mbps=self.speed_mbps, total_mb=total_mb)
            else:
                update_backup(self.job_id, progress_percentage=pct, processed_mb=processed_mb, speed_mbps=self.speed_mbps, total_mb=total_mb)


class ProgressWriter:
    def __init__(self, dest_stream, tracker: ProgressTracker = None, stop_event=None, filter_done_event=None):
        self.dest_stream = dest_stream
        self.tracker = tracker
        self.stop_event = stop_event
        self.filter_done_event = filter_done_event
        
    def write(self, b):
        if self.filter_done_event and self.filter_done_event.is_set():
            return 0
        if self.stop_event and self.stop_event.is_set():
            raise RuntimeError("Restore cancelled by user.")
        try:
            self.dest_stream.write(b)
        except (BrokenPipeError, OSError):
            return 0
        if self.tracker:
            self.tracker.update_bytes(len(b))
        return len(b)
        
    def flush(self):
        try:
            self.dest_stream.flush()
        except Exception:
            pass


def pipe_and_count(src, dest, tracker: ProgressTracker = None):
    try:
        optimize_pipe_fd(src)
        optimize_pipe_fd(dest)
        buf_size = 4 * 1024 * 1024  # 4MB chunks for ultra-high-throughput pipe
        byte_acc = 0
        last_trk_time = time.time()
        while True:
            chunk = src.read(buf_size)
            if not chunk:
                break
            dest.write(chunk)
            if tracker:
                byte_acc += len(chunk)
                now = time.time()
                if byte_acc >= 4 * 1024 * 1024 or (now - last_trk_time) >= 0.5:
                    tracker.update_bytes(byte_acc)
                    byte_acc = 0
                    last_trk_time = now
        if tracker and byte_acc > 0:
            tracker.update_bytes(byte_acc)
    except Exception as e:
        log.warning(f"Error in pipe_and_count: {e}")
    finally:
        try:
            dest.close()
        except Exception:
            pass
        try:
            src.close()
        except Exception:
            pass


def log_and_parse_stderr(stderr_stream, log_path: str, tracker: ProgressTracker = None):
    try:
        with open(log_path, "a", encoding="utf-8", errors="replace") as f:
            for line_bytes in iter(stderr_stream.readline, b""):
                line = line_bytes.decode("utf-8", errors="ignore")
                f.write(line)
                f.flush()
                
                clean_line = line.strip()
                if clean_line:
                    log.info(clean_line)
                
                pct_match = re.search(r'\((\d+(?:\.\d+)?)\%\)', line)
                if pct_match and tracker and tracker.total_size <= 0:
                    pct = int(float(pct_match.group(1)))
                    tracker.update_pct(pct)
    except Exception as e:
        log.warning(f"Error in log_and_parse_stderr: {e}")
