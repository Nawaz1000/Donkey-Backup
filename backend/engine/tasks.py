"""
BackupVault Engine - High-Level Tasks
Coordinates end-to-end execution of backups, restores, and cross-database syncs.
"""
import os
import sys
import time
import shutil
import logging
import tempfile
import uuid
import threading
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from .core import (
        BACKUP_TMP, ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, ACTIVE_TRACKERS,
        cancel_job, get_log_path, write_log, read_log, _cleanup_old_logs,
        optimize_pipe_fd, ProgressTracker, pipe_and_count, log_and_parse_stderr,
        mongo_cmd_args, pg_env, update_backup, update_restore, update_sync, log
    )
    from .compressors import get_speed_settings, get_compressor_info
    from .storage import (
        get_s3_client, list_storage_objects, get_storage_object_size,
        delete_remote_backup, prune_schedule_backups
    )
    from .pipeline import stream_backup_to_storage
    from .drivers.postgres import get_pg_db_size, run_pg_backup, run_pg_restore
    from .drivers.mongo import run_mongo_backup, run_mongo_restore
    from .drivers.solr import run_solr_backup, run_solr_restore
except ImportError:
    from engine.core import (
        BACKUP_TMP, ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, ACTIVE_TRACKERS,
        cancel_job, get_log_path, write_log, read_log, _cleanup_old_logs,
        optimize_pipe_fd, ProgressTracker, pipe_and_count, log_and_parse_stderr,
        mongo_cmd_args, pg_env, update_backup, update_restore, update_sync, log
    )
    from engine.compressors import get_speed_settings, get_compressor_info
    from engine.storage import (
        get_s3_client, list_storage_objects, get_storage_object_size,
        delete_remote_backup, prune_schedule_backups
    )
    from engine.pipeline import stream_backup_to_storage
    from engine.drivers.postgres import get_pg_db_size, run_pg_backup, run_pg_restore
    from engine.drivers.mongo import run_mongo_backup, run_mongo_restore
    from engine.drivers.solr import run_solr_backup, run_solr_restore

from utils import (
    read_json, write_json, send_notification, parse_mongo_uri,
    get_decrypted_db, get_decrypted_storage
)


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
    if db:
        db = get_decrypted_db(db)
    if storage:
        storage = get_decrypted_storage(storage)
    collection = backup.get("collection", "full")

    write_log(log_path, f"INFO  DB: {db and db['name']} | Collection: {collection} | Storage: {storage and storage['type']}")
    log.info(f"=== BACKUP START | id={backup_id} db={db and db['name']} collection={collection} ===")

    active_remote_name = [None]
    active_folder = [None]

    def fail(msg):
        log.error(f"=== BACKUP FAILED | id={backup_id} | {msg} ===")
        write_log(log_path, f"ERROR {msg}")
        cancel_job(backup_id)
        rn = active_remote_name[0]
        fld = active_folder[0]
        if rn and storage:
            try:
                delete_remote_backup(storage, rn, folder_name=fld)
                write_log(log_path, f"INFO  Cleaned up incomplete backup from remote storage: {rn}")
            except Exception as ex_del:
                write_log(log_path, f"WARN  Could not auto-clean remote artifact after failure: {ex_del}")
        logs = read_log(log_path)
        update_backup(backup_id,
            status="failed", error=msg, logs=logs,
            completed_at=datetime.utcnow().isoformat() + "Z",
            duration_seconds=int((datetime.utcnow() - start).total_seconds())
        )
        if backup.get("send_notifications", True):
            send_notification(
                title="Backup Failed ❌",
                details={
                    "Target": collection if collection != 'full' else (db and db.get('name')),
                    "Storage": storage and storage.get('name'),
                    "Time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
                    "Error": msg
                },
                is_error=True, log_path=log_path
            )
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not db or not storage:
            return fail("Database or storage not found")
            
        if backup.get("send_notifications", True):
            send_notification(
                title="Backup Started ⏳",
                details={
                    "Target": collection if collection != 'full' else db.get('name'),
                    "Storage": storage.get('name'),
                    "Time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                },
                is_error=False, log_path=log_path
            )

        speed_profile = backup.get("speed_profile", "default")
        
        comp_info = get_compressor_info(speed_profile, log_path)
        ext = comp_info["ext"] if comp_info else "gz"
        compression = comp_info["type"] if comp_info else "native"
        
        safe_db_name = str(db.get("name") or db.get("database_name") or "db").strip().replace(" ", "_")
        date_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        
        target_db_name = backup.get("database_name") or db.get("database_name")
        archive_mode = backup.get("archive_mode", "single")

        if collection and collection != "full" and (not target_db_name or target_db_name == "__all__"):
            return fail("A specific database must be selected when backing up an individual collection or table.")

        is_cluster = False
        if target_db_name == "__all__":
            is_cluster = True
            base_name = f"{safe_db_name}_all"
            if archive_mode == "multiple":
                remote_name = f"{base_name}_{date_str}/"
            else:
                remote_name = f"{base_name}-{date_str}/backup.archive.{ext}"
        elif db["type"] == "mongodb" and collection == "full" and backup.get("is_scheduled") and not target_db_name:
            is_cluster = True
            base_name = "Mongdbbackup"
            if archive_mode == "multiple":
                remote_name = f"{base_name}_{date_str}/"
            else:
                remote_name = f"{base_name}-{date_str}/backup.archive.{ext}"
        elif collection and collection != "full":
            base_name = collection.strip().replace(" ", "_")
            remote_name = f"{base_name}-{date_str}/backup.archive.{ext}"
        else:
            base_name = safe_db_name
            remote_name = f"{base_name}-{date_str}/backup.archive.{ext}"

        folder = (backup.get("folder_name") or storage.get("folder_name") or "").strip().strip("/")
        while "//" in folder:
            folder = folder.replace("//", "/")
        if folder:
            remote_name = f"{folder}/{remote_name}"

        active_remote_name[0] = remote_name
        active_folder[0] = folder
        update_backup(backup_id, remote_name=remote_name, folder_name=folder, compression=compression)

        total_size = 0
        if db["type"] == "postgresql":
            if target_db_name != "__all__":
                total_size = get_pg_db_size(db, target_db_name or db.get("database_name", ""))
                write_log(log_path, f"INFO  Estimated database size: {round(total_size / (1024*1024), 2)} MB")
            else:
                try:
                    cmd_sz = [
                        "psql", f"--host={db['host']}", f"--port={db['port']}",
                        f"--username={db['username']}", "--no-password", "--dbname=postgres",
                        "-t", "-A", "-c", "SELECT sum(pg_database_size(datname)) FROM pg_database WHERE datistemplate=false;"
                    ]
                    res_sz = subprocess.run(cmd_sz, capture_output=True, text=True, env=pg_env(db), timeout=5)
                    if res_sz.returncode == 0 and res_sz.stdout.strip().isdigit():
                        total_size = int(res_sz.stdout.strip())
                        write_log(log_path, f"INFO  Estimated total cluster size: {round(total_size / (1024*1024), 2)} MB")
                except Exception:
                    pass
        elif db["type"] == "mongodb":
            try:
                import pymongo
                m = parse_mongo_uri(db)
                client = pymongo.MongoClient(m["uri"], serverSelectionTimeoutMS=3000)
                if is_cluster or target_db_name == "__all__":
                    db_names = [d for d in client.list_database_names() if d not in ("local", "config", "admin")]
                    total_size = sum(client[d].command("dbStats").get("dataSize", 0) for d in db_names)
                    write_log(log_path, f"INFO  Estimated total MongoDB cluster data size ({len(db_names)} databases): {round(total_size / (1024*1024), 2)} MB")
                else:
                    target_mongo_db = target_db_name or m["dbname"] or db.get("database_name")
                    if target_mongo_db:
                        total_size = client[target_mongo_db].command("dbStats").get("dataSize", 0)
                        write_log(log_path, f"INFO  Estimated MongoDB data size: {round(total_size / (1024*1024), 2)} MB")
                client.close()
            except Exception as e:
                write_log(log_path, f"WARN  Could not estimate MongoDB total size: {e}")

        if total_size > 0:
            update_backup(backup_id, total_mb=round(total_size / (1024 * 1024), 2))

        tracker = ProgressTracker(backup_id, is_restore=False, total_size=total_size, log_path=log_path)
        indexing_mode = backup.get("indexing_mode", "with_index")

        source_dbname = None
        if db["type"] == "mongodb":
            remote_path = run_mongo_backup(db, backup, storage, remote_name, log_path, indexing_mode=indexing_mode, tracker=tracker, speed_profile=speed_profile, is_cluster=is_cluster)
            m = parse_mongo_uri(db)
            source_dbname = m["dbname"]
        elif db["type"] == "postgresql":
            remote_path = run_pg_backup(db, backup, storage, remote_name, log_path, indexing_mode=indexing_mode, tracker=tracker, speed_profile=speed_profile, is_cluster=is_cluster)
        elif db["type"] == "solr":
            remote_path = run_solr_backup(db, backup, storage, remote_name, log_path, indexing_mode=indexing_mode, tracker=tracker, speed_profile=speed_profile)
        else:
            return fail(f"Unsupported DB type: {db['type']}")

        raw_bytes = getattr(stream_backup_to_storage, "last_size_bytes", 0) or 0
        if raw_bytes == 0:
            return fail(f"Backup verification failed: Uploaded backup on storage '{storage.get('name')}' is 0 bytes or was not saved.")
        size_mb = round(raw_bytes / (1024 * 1024), 2)

        if is_cluster:
            update_backup(backup_id, archive_mode=archive_mode, is_cluster=True)

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
            completed_at=datetime.utcnow().isoformat() + "Z",
            duration_seconds=duration
        )

        sch_id = backup.get("schedule_id")
        if sch_id:
            try:
                if os.path.exists("data/schedules.json"):
                    schedules = read_json("data/schedules.json")
                    sch = next((s for s in schedules if s["id"] == sch_id), None)
                    if sch and sch.get("retention_count"):
                        ret_cnt = int(sch["retention_count"])
                        if ret_cnt > 0:
                            write_log(log_path, f"INFO  Applying schedule retention policy (keeping last {ret_cnt} backups)...")
                            prune_schedule_backups(sch_id, ret_cnt)
            except Exception as e:
                write_log(log_path, f"WARN  Could not run retention pruning: {e}")
        if backup.get("send_notifications", True):
            send_notification(
                title="Backup Successful ✅",
                details={
                    "Target": collection if collection != 'full' else (db and db.get('name')),
                    "Storage": storage and storage.get('name'),
                    "Size": f"{size_mb} MB",
                    "Duration": f"{duration}s",
                    "Time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                },
                is_error=False, log_path=log_path
            )

    except Exception as e:
        fail(str(e))
    finally:
        ACTIVE_TRACKERS.pop(backup_id, None)
        cancel_job(backup_id)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        _cleanup_old_logs()


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
        cancel_job(restore_id)
        logs = read_log(log_path)
        update_restore(restore_id,
            status="failed", error=msg, logs=logs,
            completed_at=datetime.utcnow().isoformat() + "Z",
            duration_seconds=int((datetime.utcnow() - start).total_seconds())
        )
        send_notification(
            title="Restore Failed ❌",
            details={
                "Source": restore.get('remote_name', 'backup'),
                "Target DB": target_db and target_db.get('name'),
                "Time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
                "Error": msg
            },
            is_error=True, log_path=log_path
        )
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not target_db:
            return fail("Target database not found")
        
        send_notification(
            title="Restore Started ⏳",
            details={
                "Target DB": target_db.get('name'),
                "Time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            },
            is_error=False, log_path=log_path
        )

        backup = None
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
            if restore.get("collection") and restore.get("collection") != "full":
                collection = restore["collection"]
            compression = backup.get("compression")
            if not compression:
                if ".zst" in remote_name:
                    compression = "zstd"
                elif ".gz" in remote_name:
                    compression = "gzip"
                else:
                    compression = "native"
            source_db_connection = next((d for d in dbs if d["id"] == backup.get("database_id")), None)
            fallback_source_dbname = source_db_connection["database_name"] if source_db_connection else None
            source_dbname = backup.get("source_dbname") or fallback_source_dbname
            total_size = int(backup.get("size_mb", 0) * 1024 * 1024)
        elif restore.get("storage_id") and restore.get("remote_name"):
            storage = next((s for s in storages if s["id"] == restore["storage_id"]), None)
            if not storage:
                return fail("Storage not found")
            remote_name = restore["remote_name"]
            collection = restore.get("collection") or "full"
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

        send_notification(
            title="Restore Started ⏳",
            details={
                "Source": remote_name,
                "Storage": storage.get('name'),
                "Target DB": target_db.get('name'),
                "Time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            },
            is_error=False, log_path=log_path
        )

        if restore.get("new_database") and restore.get("new_database_name"):
            new_dbname = restore["new_database_name"].strip()
            write_log(log_path, f"INFO  Restoring to NEW database: {new_dbname}")
            log.info(f"Restoring to NEW database: {new_dbname}")
            target_db = {**target_db, "database_name": new_dbname}
        elif restore.get("target_database_name"):
            explicit_dbname = restore["target_database_name"].strip()
            write_log(log_path, f"INFO  Restoring to database: {explicit_dbname}")
            log.info(f"Restoring to database: {explicit_dbname}")
            target_db = {**target_db, "database_name": explicit_dbname}

        if total_size <= 0 and not remote_name.endswith("/"):
            total_size = get_storage_object_size(storage, remote_name)
            if total_size > 0:
                write_log(log_path, f"INFO  Remote backup archive size: {round(total_size / (1024*1024), 2)} MB")

        if total_size > 0:
            update_restore(restore_id, total_mb=round(total_size / (1024 * 1024), 2))

        tracker = ProgressTracker(restore_id, is_restore=True, total_size=total_size, log_path=log_path)
        indexing_mode = restore.get("indexing_mode", "with_index")
        changes_only = bool(restore.get("changes_only", False))
        if changes_only:
            write_log(log_path, "INFO  Smart Sync / Changes Only (Diff Mode) enabled: preserving existing tables and applying only diffs.")

        selected_database = (restore.get("selected_database") or "").strip()
        if selected_database.lower() in ("__all__", "all", ""):
            selected_database = None

        is_folder = remote_name.endswith("/") or not any(remote_name.endswith(ext) for ext in [".zst", ".gz", ".tar.gz", ".sql", ".dump", ".archive"])
        if is_folder and not remote_name.endswith("/"):
            remote_name = remote_name + "/"

        target_dbname = (target_db.get("database_name") or "").strip()
        is_cluster_backup = ("_all-" in remote_name or "_cluster-" in remote_name or "Mongdbbackup-" in remote_name or (backup and backup.get("is_cluster", False)) or is_folder)
        if not selected_database and is_cluster_backup and target_dbname and target_dbname.lower() not in ("__all__", "all", "postgres"):
            selected_database = target_dbname
            write_log(log_path, f"INFO  [Auto-Detected Single DB Target] Cluster backup archive detected and target is '{selected_database}'. Restoring ONLY '{selected_database}'.")

        if selected_database:
            write_log(log_path, f"INFO  Selective restore: targeting single database '{selected_database}' from backup.")

        if target_db["type"] == "mongodb":
            if is_folder:
                write_log(log_path, f"INFO  MongoDB folder restore detected: {remote_name}")
                objects = [b for b in list_storage_objects(storage, remote_name) if ".archive." in b]
                if not objects:
                    return fail(f"No database archives found in folder: {remote_name}")

                if selected_database:
                    matched_obj = next((obj for obj in objects if obj.split("/")[-1].split(".archive.")[0].lower() == selected_database.lower()), None)
                    if not matched_obj:
                        avail = [obj.split("/")[-1].split(".archive.")[0] for obj in objects]
                        return fail(f"Database '{selected_database}' not found in backup folder. Available databases: {', '.join(avail)}")

                    write_log(log_path, f"INFO  [Single DB Restore] Restoring database '{selected_database}' from backup: {matched_obj}")
                    obj_compression = "zstd" if ".zst" in matched_obj else ("gzip" if ".gz" in matched_obj else compression)
                    target_db_name = target_db.get("database_name") or selected_database
                    run_mongo_restore(target_db, "full", storage, matched_obj, log_path, drop_existing=not restore.get("new_database", False), compression=obj_compression, source_dbname=selected_database, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), target_dbname=target_db_name, is_cluster_restore=False, changes_only=changes_only)
                else:
                    write_log(log_path, f"INFO  Found {len(objects)} databases to restore.")
                    for obj in objects:
                        db_name = obj.split("/")[-1].split(".archive.")[0]
                        write_log(log_path, f"INFO  [Cluster Restore] Restoring database: {db_name}")
                        obj_compression = "zstd" if ".zst" in obj else ("gzip" if ".gz" in obj else compression)
                        if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                            ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()
                        run_mongo_restore(target_db, "full", storage, obj, log_path, drop_existing=not restore.get("new_database", False), compression=obj_compression, source_dbname=db_name, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), target_dbname=db_name, changes_only=changes_only)
            else:
                is_cluster_archive = ("_all-" in remote_name or "Mongdbbackup-" in remote_name or "_cluster-" in remote_name or target_db.get("database_name") == "__all__")
                if selected_database:
                    write_log(log_path, f"INFO  [Single DB Restore] Extracting database '{selected_database}' from unified archive: {remote_name}")
                    target_db_name = target_db.get("database_name") or selected_database
                    run_mongo_restore(target_db, collection, storage, remote_name, log_path, drop_existing=not restore.get("new_database", False), compression=compression, source_dbname=selected_database, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), target_dbname=target_db_name, is_cluster_restore=False, selected_database=selected_database, changes_only=changes_only)
                else:
                    run_mongo_restore(target_db, collection, storage, remote_name, log_path, drop_existing=not restore.get("new_database", False), compression=compression, source_dbname=source_dbname, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), target_dbname=target_db.get("database_name"), is_cluster_restore=is_cluster_archive, changes_only=changes_only)
        elif target_db["type"] == "postgresql":
            if is_folder:
                write_log(log_path, f"INFO  PostgreSQL folder restore detected: {remote_name}")
                objects = [b for b in list_storage_objects(storage, remote_name) if ".archive." in b]

                if not objects:
                    return fail(f"No database archives found in instance backup folder: {remote_name}")

                if selected_database:
                    matched_obj = next((obj for obj in objects if obj.split("/")[-1].split(".archive.")[0].lower() == selected_database.lower()), None)
                    if not matched_obj:
                        avail = [obj.split("/")[-1].split(".archive.")[0] for obj in objects]
                        return fail(f"Database '{selected_database}' not found in backup folder. Available databases: {', '.join(avail)}")

                    write_log(log_path, f"INFO  [Single DB Restore] Restoring database '{selected_database}' from backup folder: {matched_obj}")
                    obj_compression = "zstd" if ".zst" in matched_obj else ("gzip" if ".gz" in matched_obj else compression)
                    target_db_name = target_db.get("database_name") or selected_database
                    restore_db = {**target_db, "database_name": target_db_name}
                    run_pg_restore(restore_db, storage, matched_obj, log_path, new_database=restore.get("new_database", False), compression=obj_compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), collection=collection, is_all=False, changes_only=changes_only)
                else:
                    settings = get_speed_settings(restore.get("speed_profile", "default"))
                    restore_workers = settings.get("restore_workers", 2)
                    write_log(log_path, f"INFO  Found {len(objects)} database archives to restore (Parallel concurrency: {restore_workers} databases simultaneously).")
                    
                    # Priority 4: Concurrent parallel database restores across worker connections
                    if restore_workers > 1 and len(objects) > 1:
                        def restore_single_archive(obj):
                            db_file = obj.split("/")[-1]
                            db_name = db_file.split(".archive.")[0]
                            write_log(log_path, f"INFO  [Parallel Instance Restore] Restoring database: {db_name}")
                            obj_compression = "zstd" if ".zst" in obj else ("gzip" if ".gz" in obj else compression)
                            restore_db = {**target_db, "database_name": db_name}
                            run_pg_restore(restore_db, storage, obj, log_path, new_database=restore.get("new_database", False), compression=obj_compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), collection=collection, is_all=False, changes_only=changes_only)
                        
                        with ThreadPoolExecutor(max_workers=min(restore_workers, len(objects))) as executor:
                            futures = [executor.submit(restore_single_archive, obj) for obj in objects]
                            for f in as_completed(futures):
                                f.result()
                    else:
                        for idx_obj, obj in enumerate(objects):
                            db_file = obj.split("/")[-1]
                            db_name = db_file.split(".archive.")[0]
                            write_log(log_path, f"INFO  [Instance Restore] Restoring database {idx_obj+1}/{len(objects)}: {db_name}")
                            obj_compression = "zstd" if ".zst" in obj else ("gzip" if ".gz" in obj else compression)
                            if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                                ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()

                            restore_db = {**target_db, "database_name": db_name}
                            run_pg_restore(restore_db, storage, obj, log_path, new_database=restore.get("new_database", False), compression=obj_compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), collection=collection, is_all=False, changes_only=changes_only)

                    write_log(log_path, f"INFO  Instance restore completed: {len(objects)} databases restored.")
            else:
                is_all = ("_all-" in remote_name or target_db.get("database_name") == "__all__")
                if selected_database:
                    write_log(log_path, f"INFO  [Single DB Restore from Unified Archive] Restoring database '{selected_database}' from {remote_name}")
                    target_db_name = target_db.get("database_name") or selected_database
                    restore_db = {**target_db, "database_name": target_db_name}
                    run_pg_restore(restore_db, storage, remote_name, log_path, new_database=restore.get("new_database", False), compression=compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), collection=collection, is_all=False, selected_database=selected_database, changes_only=changes_only)
                else:
                    run_pg_restore(target_db, storage, remote_name, log_path, new_database=restore.get("new_database", False), compression=compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), collection=collection, is_all=is_all, changes_only=changes_only)
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
            completed_at=datetime.utcnow().isoformat() + "Z",
            duration_seconds=duration
        )
        send_notification(
            title="Restore Successful ✅",
            details={
                "Source": remote_name,
                "Target DB": target_db and target_db.get('name'),
                "Duration": f"{duration}s",
                "Time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            },
            is_error=False, log_path=log_path
        )

    except Exception as e:
        fail(str(e))
    finally:
        ACTIVE_TRACKERS.pop(restore_id, None)
        cancel_job(restore_id)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        _cleanup_old_logs()


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
        
        pipe_thread = threading.Thread(target=pipe_and_count, args=(p_dump.stdout, p_restore.stdin, tracker))
        pipe_thread.start()
        
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
            completed_at=datetime.utcnow().isoformat() + "Z",
            duration_seconds=int((datetime.utcnow() - start).total_seconds())
        )
        send_notification(
            title="Sync Failed ❌",
            details={
                "Job ID": sync_id,
                "Error": msg
            },
            is_error=True, log_path=log_path
        )
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        if not src_db or not tgt_db:
            return fail("Source or Target database not found")

        total_size = 0
        if src_db["type"] == "postgresql":
            total_size = get_pg_db_size(src_db, src_db.get("database_name", ""))
            write_log(log_path, f"INFO  Estimated source DB size: {round(total_size / (1024*1024), 2)} MB")

        tracker = ProgressTracker(sync_id, is_restore=False, total_size=total_size, log_path=log_path)
        
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
            completed_at=datetime.utcnow().isoformat() + "Z",
            duration_seconds=duration,
            progress=100
        )
        send_notification(
            title="Sync Successful ✅",
            details={
                "Job ID": sync_id,
                "Size": f"{size_mb} MB",
                "Duration": f"{duration}s"
            },
            is_error=False, log_path=log_path
        )

    except Exception as e:
        fail(str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        _cleanup_old_logs()
