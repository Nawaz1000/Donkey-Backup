"""
BackupVault Engine - Compressor Tools and Speed Profiles
"""
import os
import shutil

try:
    from .core import write_log
except ImportError:
    from engine.core import write_log


def get_speed_settings(profile: str):
    cores = os.cpu_count() or 4
    if profile == "safe":
        return {"threads": 2, "chunk_size": 8 * 1024 * 1024, "queue_max": 16, "concurrency": 8, "mongo_parallel": 4, "restore_workers": 1}
    elif profile == "balanced":
        return {"threads": max(4, cores), "chunk_size": 16 * 1024 * 1024, "queue_max": 48, "concurrency": 24, "mongo_parallel": 8, "restore_workers": 2}
    elif profile == "extreme":
        return {"threads": max(4, cores), "chunk_size": 16 * 1024 * 1024, "queue_max": 64, "concurrency": 32, "mongo_parallel": 16, "restore_workers": 4}
    else:  # default (tuned for flash speed)
        return {"threads": max(4, cores), "chunk_size": 16 * 1024 * 1024, "queue_max": 64, "concurrency": 32, "mongo_parallel": 16, "restore_workers": 4}


def get_compressor_info(profile: str = "default", log_path: str = None) -> dict:
    settings = get_speed_settings(profile)
    threads = settings["threads"]
    if shutil.which("zstd"):
        fast_lvl = "--fast=5" if profile in ("extreme", "default") else ("--fast=3" if profile == "balanced" else "--fast=1")
        if log_path:
            write_log(log_path, f"INFO  Using Zstandard (zstd {fast_lvl}) with {threads} threads for {profile} profile.")
        return {
            "cmd": ["zstd", fast_lvl, f"--threads={threads}"],
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
