"""
BackupVault Engine - MongoDB Driver
High-performance MongoDB backup and restore engine with index metadata separation,
parallel collections, upsert diff mode, and cluster archive support.
"""
import os
import sys
import time
import shutil
import threading
import subprocess

try:
    from ..core import (
        write_log, log, mask_cmd, ProgressTracker,
        ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, update_backup
    )
    from ..compressors import get_compressor_info, get_speed_settings
    from ..storage import upload_metadata_file, download_metadata_file
    from ..pipeline import stream_backup_to_storage, stream_restore_from_storage
except ImportError:
    from engine.core import (
        write_log, log, mask_cmd, ProgressTracker,
        ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, update_backup
    )
    from engine.compressors import get_compressor_info, get_speed_settings
    from engine.storage import upload_metadata_file, download_metadata_file
    from engine.pipeline import stream_backup_to_storage, stream_restore_from_storage

from utils import read_json, parse_mongo_uri


def backup_mongo_indexes(uri: str, dbname: str, collection_name: str, storage: dict, remote_indexes_name: str, log_path: str):
    import pymongo
    import json
    try:
        write_log(log_path, f"INFO  Fetching index definitions from database: {dbname or 'all'}")
        client = pymongo.MongoClient(uri)
        db_names = [dbname] if dbname else [d for d in client.list_database_names() if d not in ("local", "config", "admin")]
        
        all_indexes = {}
        for db_name in db_names:
            mongo_db = client[db_name]
            all_indexes[db_name] = {}
            colls = [collection_name] if collection_name and collection_name != "full" else mongo_db.list_collection_names()
            for coll in colls:
                try:
                    idx_info = mongo_db[coll].index_information()
                    if idx_info:
                        all_indexes[db_name][coll] = idx_info
                except Exception as e:
                    write_log(log_path, f"WARN  Could not read index info for {db_name}.{coll}: {e}")
                    
        client.close()
        
        indexes_json = json.dumps(all_indexes, default=str)
        upload_metadata_file(storage, remote_indexes_name, indexes_json)
        write_log(log_path, f"INFO  Successfully uploaded index definitions metadata file: {remote_indexes_name}")
    except Exception as e:
        write_log(log_path, f"WARN  Failed to fetch or upload MongoDB index metadata: {e}")


def run_mongo_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", is_cluster: bool = False) -> str:
    m = parse_mongo_uri(db)
    dbname = backup_info.get("database_name") or db.get("database_name") or m["dbname"]
    if not dbname and not is_cluster:
        raise ValueError("Database name is required.")
    collection = backup_info.get("collection", "full")
    if collection and collection != "full" and (not dbname or dbname == "__all__"):
        raise ValueError("A specific database must be selected when backing up an individual collection.")
    backup_method = backup_info.get("backup_method", "full")
    incremental_field = backup_info.get("incremental_field", "")
    
    if indexing_mode == "only_index":
        import pymongo
        import json
        write_log(log_path, "INFO  MongoDB only_index backup requested. Fetching indexes via PyMongo.")
        try:
            client = pymongo.MongoClient(m["uri"])
            mongo_db = client[dbname]
            indexes = {}
            collections = [collection] if collection and collection != "full" else mongo_db.list_collection_names()
            for coll in collections:
                try:
                    idx_info = mongo_db[coll].index_information()
                    if idx_info:
                        indexes[coll] = idx_info
                except Exception as e:
                    write_log(log_path, f"WARN  Could not read index info for {dbname}.{coll}: {e}")
            client.close()
            
            indexes_json = json.dumps(indexes)
            upload_metadata_file(storage, remote_name, indexes_json)
            
            stream_backup_to_storage.last_size_bytes = len(indexes_json.encode('utf-8'))
            
            if storage["type"] == "azure":
                res = f"azure://{storage['azure_container']}/{remote_name}"
            elif storage["type"] == "gcs":
                res = f"gcs://{storage['gcs_bucket']}/{remote_name}"
            elif storage["type"] == "s3":
                res = f"s3://{storage['s3_bucket']}/{remote_name}"
            elif storage["type"] == "bunny":
                res = f"bunny://{storage['bunny_storage_zone']}/{remote_name}"
            else:
                res = f"unknown://{remote_name}"
                
            write_log(log_path, "INFO  only_index backup completed successfully.")
            if tracker:
                tracker.update_pct(100)
            return res
        except Exception as e:
            raise RuntimeError(f"Failed to backup MongoDB indexes: {e}")

    settings = get_speed_settings(speed_profile)
    archive_mode = backup_info.get("archive_mode", "single")
    if is_cluster:
        import pymongo
        write_log(log_path, f"INFO  Cluster-wide backup requested (Format: {archive_mode}). Fetching databases...")
        try:
            client = pymongo.MongoClient(m["uri"], serverSelectionTimeoutMS=5000)
            db_names = [d for d in client.list_database_names() if d not in ("local", "config", "admin")]
            client.close()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch databases for cluster backup: {e}")

        write_log(log_path, f"INFO  Found {len(db_names)} databases to backup: {', '.join(db_names)}")
        if tracker:
            update_backup(tracker.job_id, cluster_databases=db_names)
        elif backup_info.get("id"):
            update_backup(backup_info["id"], cluster_databases=db_names)

        comp_info = get_compressor_info(speed_profile, log_path)
        ext = comp_info["ext"] if comp_info else "gz"
        compression_cmd = comp_info["cmd"] if comp_info else None
        uri_no_db = m["uri"].replace(f"/{m['dbname']}?", "/?").replace(f"/{m['dbname']}", "/") if m['dbname'] else m["uri"]

        if archive_mode == "multiple" or remote_name.endswith("/"):
            folder_remote_name = remote_name if remote_name.endswith("/") else f"{remote_name}/"
            total_uploaded_bytes = 0
            for idx, db_name in enumerate(db_names):
                if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                    ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()

                write_log(log_path, f"INFO  [Cluster Backup] Backing up database {idx+1}/{len(db_names)}: {db_name}")
                file_remote_name = f"{folder_remote_name}{db_name}.archive.{ext}"
                backup_mongo_indexes(uri_no_db, db_name, "full", storage, file_remote_name + ".indexes.json", log_path)

                cmd = [
                    "mongodump",
                    f"--uri={uri_no_db}",
                    f"--db={db_name}",
                    "--archive",
                    f"--numParallelCollections={settings['mongo_parallel']}",
                    "--readPreference=secondaryPreferred",
                ]
                if not compression_cmd:
                    cmd.append("--gzip")

                env = os.environ.copy()
                env["GOGC"] = "100"
                env["GOMAXPROCS"] = str(settings["threads"])
                stream_backup_to_storage(cmd, env, storage, file_remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)
                total_uploaded_bytes += getattr(stream_backup_to_storage, "last_size_bytes", 0) or 0

            stream_backup_to_storage.last_size_bytes = total_uploaded_bytes
            return folder_remote_name
        else:
            backup_mongo_indexes(uri_no_db, None, "full", storage, remote_name + ".indexes.json", log_path)
            cmd = [
                "mongodump",
                f"--uri={uri_no_db}",
                "--archive",
                f"--numParallelCollections={settings['mongo_parallel']}",
                "--readPreference=secondaryPreferred",
            ]
            if not compression_cmd:
                cmd.append("--gzip")

            env = os.environ.copy()
            env["GOGC"] = "100"
            env["GOMAXPROCS"] = str(settings["threads"])
            write_log(log_path, f"INFO  [Cluster Backup] Streaming all databases to unified archive: {remote_name}")
            return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)

    cmd = [
        "mongodump",
        f"--uri={m['uri']}",
        f"--db={dbname}",
        "--archive",
        f"--numParallelCollections={settings['mongo_parallel']}",
        "--readPreference=secondaryPreferred",
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
    
    backup_mongo_indexes(m["uri"], dbname, collection, storage, remote_name + ".indexes.json", log_path)
    
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)


def run_mongo_restore(db: dict, collection: str, storage: dict, remote_name: str, log_path: str, drop_existing: bool = True, compression: str = None, source_dbname: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", target_dbname: str = None, is_cluster_restore: bool = False, selected_database: str = None, changes_only: bool = False):
    m = parse_mongo_uri(db)
    dbname = target_dbname or db.get("database_name") or m["dbname"]
    if not dbname and not is_cluster_restore:
        raise ValueError("Database name is required for restore.")

    use_no_index_restore = False
    indexes_metadata = None
    if indexing_mode == "with_index":
        indexes_metadata = download_metadata_file(storage, remote_name + ".indexes.json")
        if indexes_metadata:
            use_no_index_restore = True
            write_log(log_path, "INFO  Found index metadata file on storage. Enabling sequential index restoration.")

    if indexing_mode == "only_index":
        import pymongo
        import json
        write_log(log_path, "INFO  MongoDB only_index restore requested. Applying indexes via PyMongo.")
        try:
            indexes_metadata = download_metadata_file(storage, remote_name)
            if not indexes_metadata:
                indexes_metadata = download_metadata_file(storage, remote_name + ".indexes.json")
                
            if not indexes_metadata:
                raise RuntimeError("Index metadata file not found on storage")
                
            indexes_data = json.loads(indexes_metadata)
            
            is_new_format = False
            for k, v in indexes_data.items():
                if isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        if isinstance(sub_v, dict) and "key" not in sub_v:
                            is_new_format = True
                            break
                    if is_new_format:
                        break
            
            write_log(log_path, f"INFO  Applying indexes to database: {dbname}")
            client = pymongo.MongoClient(m["uri"])
            
            if is_new_format:
                for src_db_name, collections_data in indexes_data.items():
                    mongo_db = client[dbname]
                    for coll, idx_info in collections_data.items():
                        if collection and collection != "full" and coll != collection:
                            continue
                        write_log(log_path, f"INFO  Creating indexes for collection {dbname}.{coll} ({len(idx_info)} indexes total)...")
                        for index_name, index_spec in idx_info.items():
                            if index_name == "_id_":
                                continue
                            try:
                                keys = index_spec["key"]
                                key_tuples = [(k[0], k[1]) for k in keys]
                                
                                options = {}
                                for opt in ["unique", "sparse", "expireAfterSeconds", "partialFilterExpression", "collation", "weights", "default_language", "language_override", "textIndexVersion"]:
                                    if opt in index_spec:
                                        options[opt] = index_spec[opt]
                                        
                                write_log(log_path, f"INFO    - Building index: {index_name} on {coll}...")
                                mongo_db[coll].create_index(key_tuples, name=index_name, **options)
                                write_log(log_path, f"INFO    - Successfully built index: {index_name}")
                            except Exception as idx_err:
                                write_log(log_path, f"WARN    - Failed to build index {index_name} on {coll}: {idx_err}")
            else:
                mongo_db = client[dbname]
                for coll, idx_info in indexes_data.items():
                    if collection and collection != "full" and coll != collection:
                        continue
                    write_log(log_path, f"INFO  Creating indexes for collection {dbname}.{coll} ({len(idx_info)} indexes total)...")
                    for index_name, index_spec in idx_info.items():
                        if index_name == "_id_":
                            continue
                        try:
                            keys = index_spec["key"]
                            key_tuples = [(k[0], k[1]) for k in keys]
                            
                            options = {}
                            for opt in ["unique", "sparse", "expireAfterSeconds", "partialFilterExpression", "collation", "weights", "default_language", "language_override", "textIndexVersion"]:
                                if opt in index_spec:
                                    options[opt] = index_spec[opt]
                                    
                            write_log(log_path, f"INFO    - Building index: {index_name} on {coll}...")
                            mongo_db[coll].create_index(key_tuples, name=index_name, **options)
                            write_log(log_path, f"INFO    - Successfully built index: {index_name}")
                        except Exception as idx_err:
                            write_log(log_path, f"WARN    - Failed to build index {index_name} on {coll}: {idx_err}")
                            
            client.close()
            write_log(log_path, "INFO  only_index restore completed successfully.")
            if tracker:
                tracker.update_pct(100)
            return
        except Exception as e:
            write_log(log_path, f"ERROR Failed to restore indexes: {e}")
            raise RuntimeError(f"Failed to restore MongoDB indexes: {e}")
            
    settings = get_speed_settings(speed_profile)
    workers = max(16, settings['threads'] * 4)
    batch_size = 5000
    write_concern = "{w: 1}"
    if speed_profile == "extreme":
        workers = 48
        batch_size = 10000
        write_concern = "{w: 1}"
        
    uri_to_use = (m["uri"].replace(f"/{m['dbname']}?", "/?").replace(f"/{m['dbname']}", "/") if m['dbname'] else m["uri"]) if is_cluster_restore else m["uri"]
    cmd = [
        "mongorestore",
        f"--uri={uri_to_use}",
        "--archive",
        f"--numParallelCollections={min(settings['mongo_parallel'], 16)}",
        f"--numInsertionWorkersPerCollection={workers}",
        f"--batchSize={batch_size}",
        "--bypassDocumentValidation",
        f"--writeConcern={write_concern}",
        "--verbose=1",
    ]

    if indexing_mode == "without_index" or use_no_index_restore:
        cmd.append("--noIndexRestore")

    if selected_database:
        cmd += [f"--nsInclude={selected_database}.*"]
        if dbname and dbname != selected_database and dbname != "__all__":
            cmd += [
                f"--nsFrom={selected_database}.$collection$",
                f"--nsTo={dbname}.$collection$",
            ]
    elif not is_cluster_restore and dbname and dbname != "__all__":
        cmd += [
            "--nsFrom=$database$.$collection$",
            f"--nsTo={dbname}.$collection$",
        ]
    if collection and collection != "full":
        cmd += [f"--nsInclude=*.{collection}"]

    if changes_only:
        drop_existing = False
        cmd.append("--mode=upsert")
        write_log(log_path, "INFO  Smart Sync / Diff Mode: applying changes with --mode=upsert (existing data preserved).")
    elif drop_existing:
        cmd.append("--drop")

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
    stream_restore_from_storage(cmd, env, storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker, speed_profile=speed_profile, indexing_mode=indexing_mode, changes_only=changes_only)

    if use_no_index_restore and indexes_metadata:
        try:
            import pymongo
            import json
            from concurrent.futures import ThreadPoolExecutor
            all_indexes = json.loads(indexes_metadata)
            write_log(log_path, f"INFO  Starting parallel index creation for database: {dbname}")
            
            client = pymongo.MongoClient(m["uri"])
            mongo_db = client[dbname]

            def build_col_indexes(coll_name, indexes_spec):
                for index_name, index_spec in indexes_spec.items():
                    if index_name == "_id_":
                        continue
                    try:
                        keys = index_spec["key"]
                        key_tuples = [(k[0], k[1]) for k in keys]
                        options = {}
                        for opt in ["unique", "sparse", "expireAfterSeconds", "partialFilterExpression", "collation", "weights", "default_language", "language_override", "textIndexVersion"]:
                            if opt in index_spec:
                                options[opt] = index_spec[opt]
                        mongo_db[coll_name].create_index(key_tuples, name=index_name, **options)
                        write_log(log_path, f"INFO    - Successfully built index: {index_name} on {coll_name}")
                    except Exception as idx_err:
                        write_log(log_path, f"WARN    - Failed to build index {index_name} on {coll_name}: {idx_err}")

            with ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 4)) as idx_exec:
                futures = []
                for src_db_name, collections_data in all_indexes.items():
                    actual_target_db = client[dbname] if (not is_cluster_restore and dbname and dbname != "__all__") else client[src_db_name]
                    def build_col_indexes_for_db(target_db_inst, coll_n, idx_s):
                        for index_name, index_spec in idx_s.items():
                            if index_name == "_id_":
                                continue
                            try:
                                keys = index_spec["key"]
                                key_tuples = [(k[0], k[1]) for k in keys]
                                options = {}
                                for opt in ["unique", "sparse", "expireAfterSeconds", "partialFilterExpression", "collation", "weights", "default_language", "language_override", "textIndexVersion"]:
                                    if opt in index_spec:
                                        options[opt] = index_spec[opt]
                                target_db_inst[coll_n].create_index(key_tuples, name=index_name, **options)
                                write_log(log_path, f"INFO    - Successfully built index: {index_name} on {target_db_inst.name}.{coll_n}")
                            except Exception as idx_err:
                                write_log(log_path, f"WARN    - Failed to build index {index_name} on {target_db_inst.name}.{coll_n}: {idx_err}")

                    for coll, idx_info in collections_data.items():
                        if collection and collection != "full" and coll != collection:
                            continue
                        write_log(log_path, f"INFO  Creating indexes for collection {actual_target_db.name}.{coll} ({len(idx_info)} indexes total)...")
                        futures.append(idx_exec.submit(build_col_indexes_for_db, actual_target_db, coll, idx_info))
                for f in futures:
                    f.result()
                            
            client.close()
            write_log(log_path, "INFO  Parallel index creation completed successfully.")
        except Exception as e:
            write_log(log_path, f"WARN  Error during index restoration: {e}")

