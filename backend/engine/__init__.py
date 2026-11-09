"""
BackupVault Engine - Enterprise Modular Package
Drop-in backward compatible export facade for all engine subsystems:
- core: process lifecycle, cancel_job, logging, progress tracker, pipe utilities
- compressors: compressor discovery and speed profiles (pigz, zstd, gzip)
- storage: multi-cloud integrations (S3, BunnyCDN, Azure Blob, GCS)
- pipeline: zero-disk in-memory streaming upload and parallel download
- drivers: PostgreSQL, MongoDB, and Apache Solr engines
- tasks: high-level backup, restore, and cross-database sync orchestrators
"""

from .core import (
    BACKUP_TMP, ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, ACTIVE_TRACKERS,
    cancel_job, kill_stale_backup_subprocesses,
    mongo_cmd_args, pg_env, mask_cmd,
    get_log_path, write_log, read_log, _cleanup_old_logs,
    optimize_pipe_fd, ProgressTracker, ProgressWriter, pipe_and_count, log_and_parse_stderr,
    update_backup, update_restore, update_sync
)

from .compressors import (
    get_speed_settings, get_compressor_info
)

from .storage import (
    BUNNY_KNOWN_REGIONS, resolve_bunny_hosts, get_bunny_key, get_bunny_host,
    verify_and_resolve_bunny, get_s3_client, list_storage_objects, get_storage_object_size,
    upload_metadata_file, download_metadata_file, delete_remote_backup, prune_schedule_backups
)

from .pipeline import (
    QueueReader, reader_thread_fn,
    stream_backup_to_storage, download_parallel_range_stream, stream_restore_from_storage
)

from .drivers.postgres import (
    check_pg_options_supported, get_pg_db_size, get_pg_modified_tables,
    run_pg_backup, run_pg_restore
)

from .drivers.mongo import (
    backup_mongo_indexes, run_mongo_backup, run_mongo_restore
)

from .drivers.solr import (
    get_solr_base_url, run_solr_backup, run_solr_restore
)

from .tasks import (
    do_backup, do_restore, run_mongo_sync, run_pg_sync, do_sync
)

__all__ = [
    # Core & Process Tracking
    "BACKUP_TMP", "ACTIVE_PROCESSES", "ACTIVE_STOP_EVENTS", "ACTIVE_TRACKERS",
    "cancel_job", "kill_stale_backup_subprocesses",
    "mongo_cmd_args", "pg_env", "mask_cmd",
    "get_log_path", "write_log", "read_log", "_cleanup_old_logs",
    "optimize_pipe_fd", "ProgressTracker", "ProgressWriter", "pipe_and_count", "log_and_parse_stderr",
    "update_backup", "update_restore", "update_sync",

    # Compressors
    "get_speed_settings", "get_compressor_info",

    # Storage
    "BUNNY_KNOWN_REGIONS", "resolve_bunny_hosts", "get_bunny_key", "get_bunny_host",
    "verify_and_resolve_bunny", "get_s3_client", "list_storage_objects", "get_storage_object_size",
    "upload_metadata_file", "download_metadata_file", "delete_remote_backup", "prune_schedule_backups",

    # Pipeline
    "QueueReader", "reader_thread_fn",
    "stream_backup_to_storage", "download_parallel_range_stream", "stream_restore_from_storage",

    # Database Drivers
    "check_pg_options_supported", "get_pg_db_size", "get_pg_modified_tables",
    "run_pg_backup", "run_pg_restore",
    "backup_mongo_indexes", "run_mongo_backup", "run_mongo_restore",
    "get_solr_base_url", "run_solr_backup", "run_solr_restore",

    # Tasks
    "do_backup", "do_restore", "run_mongo_sync", "run_pg_sync", "do_sync"
]
