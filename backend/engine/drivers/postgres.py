"""
BackupVault Engine - PostgreSQL Driver
High-performance PostgreSQL backup and restore engine.
Supports single DB, custom-format (-Fc), table-filtered incremental/differential,
without-index/smart-diff modes, and parallel cluster restores.
"""
import os
import sys
import time
import shutil
import threading
import subprocess

try:
    from ..core import (
        pg_env, write_log, log, optimize_pipe_fd, ProgressTracker,
        ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, update_backup
    )
    from ..compressors import get_compressor_info, get_speed_settings
    from ..pipeline import stream_backup_to_storage, stream_restore_from_storage
except ImportError:
    from engine.core import (
        pg_env, write_log, log, optimize_pipe_fd, ProgressTracker,
        ACTIVE_PROCESSES, ACTIVE_STOP_EVENTS, update_backup
    )
    from engine.compressors import get_compressor_info, get_speed_settings
    from engine.pipeline import stream_backup_to_storage, stream_restore_from_storage

from utils import read_json, get_decrypted_db, get_decrypted_storage

_pg_options_support_cache = {}


def check_pg_options_supported(db: dict, test_options: str = "-c statement_timeout=0", dbname: str = None) -> bool:
    """
    Check if PGOPTIONS can be passed safely to PostgreSQL.
    Connection poolers like PgBouncer (commonly on port 6432 or proxies) reject startup parameters
    in PGOPTIONS with 'FATAL: unsupported startup parameter in options: statement_timeout'.
    """
    db = get_decrypted_db(db)
    host = str(db.get("host", "")).strip()
    port = str(db.get("port", "")).strip()
    cache_key = f"{host}:{port}"
    if cache_key in _pg_options_support_cache:
        return _pg_options_support_cache[cache_key]

    if port == "6432":
        _pg_options_support_cache[cache_key] = False
        return False

    target_db = (dbname or db.get("database_name") or "postgres").strip()
    try:
        env = pg_env(db)
        env["PGOPTIONS"] = test_options
        cmd = [
            "psql", f"--host={host}", f"--port={port}",
            f"--username={db.get('username', '')}", "--no-password", f"--dbname={target_db}",
            "-t", "-A", "-c", "SELECT 1;"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=4)
        if res.returncode != 0:
            _pg_options_support_cache[cache_key] = False
            return False
        _pg_options_support_cache[cache_key] = True
        return True
    except Exception:
        _pg_options_support_cache[cache_key] = False
        return False


def get_pg_db_size(db: dict, dbname: str) -> int:
    try:
        db = get_decrypted_db(db)
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
        db = get_decrypted_db(db)
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


def run_pg_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", is_cluster: bool = False) -> str:
    db = get_decrypted_db(db)
    storage = get_decrypted_storage(storage)
    dbname = (backup_info.get("database_name") or db.get("database_name") or "").strip()
    collection = backup_info.get("collection", "full")
    backup_method = backup_info.get("backup_method", "full")
    if collection and collection != "full" and (not dbname or dbname == "__all__"):
        raise ValueError("A specific database must be selected when backing up an individual table.")
    
    # Priority 4: Default cluster backups to "multiple" custom archives for parallel restore support
    archive_mode = backup_info.get("archive_mode", "multiple" if (is_cluster or dbname == "__all__") else "single")
    if is_cluster or dbname == "__all__":
        write_log(log_path, f"INFO  All Databases requested (Format: {archive_mode}). Enumerating accessible databases...")
        cmd_list = [
            "psql", f"--host={db['host']}", f"--port={db['port']}",
            f"--username={db['username']}", "--no-password", "--dbname=postgres",
            "-t", "-A", "-c",
            "SELECT datname FROM pg_database WHERE datallowconn=true AND datistemplate=false ORDER BY datname;"
        ]
        res = subprocess.run(cmd_list, capture_output=True, text=True, env=pg_env(db), timeout=10)
        db_names = []
        if res.returncode == 0:
            db_names = [l.strip() for l in res.stdout.strip().split("\n") if l.strip() and l.strip() not in ('rdsadmin', 'azure_maintenance', 'cloudsqladmin')]

        if not db_names:
            write_log(log_path, "WARN  Could not enumerate databases via psql. Falling back to default database.")
            db_names = [db.get("database_name") or "postgres"]

        write_log(log_path, f"INFO  Found {len(db_names)} databases: {', '.join(db_names)}")
        if tracker:
            update_backup(tracker.job_id, cluster_databases=db_names)
        elif backup_info.get("id"):
            update_backup(backup_info["id"], cluster_databases=db_names)

        comp_info = get_compressor_info(speed_profile, log_path)
        ext = comp_info["ext"] if comp_info else "gz"
        compression_cmd = comp_info["cmd"] if comp_info else None

        if archive_mode == "multiple" or remote_name.endswith("/"):
            folder_remote_name = remote_name if remote_name.endswith("/") else f"{remote_name}/"
            total_uploaded_bytes = 0
            success_count = 0

            for idx, single_db in enumerate(db_names):
                if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                    ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()

                write_log(log_path, f"INFO  [Instance Backup] Backing up database {idx+1}/{len(db_names)}: {single_db} (-Fc custom format)")
                file_remote_name = f"{folder_remote_name}{single_db}.archive.{ext}"

                cmd = [
                    "pg_dump",
                    f"--host={db['host']}", f"--port={db['port']}",
                    f"--username={db['username']}", f"--dbname={single_db}",
                    "--no-password", "--format=custom",
                    "--compress=0",
                    "--no-acl", "--no-owner",
                    "--verbose"
                ]
                if indexing_mode == "without_index":
                    cmd += ["--section=pre-data", "--section=data"]
                elif indexing_mode == "only_index":
                    cmd += ["--section=post-data"]

                env = pg_env(db)
                pg_opts = "-c statement_timeout=0 -c work_mem=128MB -c maintenance_work_mem=512MB"
                if check_pg_options_supported(db, pg_opts, single_db):
                    env["PGOPTIONS"] = pg_opts

                try:
                    stream_backup_to_storage(cmd, env, storage, file_remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)
                    total_uploaded_bytes += getattr(stream_backup_to_storage, "last_size_bytes", 0) or 0
                    success_count += 1
                except Exception as e:
                    write_log(log_path, f"ERROR Failed to backup database {single_db}: {e}")

            if success_count == 0:
                raise RuntimeError("Failed to backup any databases in PostgreSQL instance.")

            stream_backup_to_storage.last_size_bytes = total_uploaded_bytes
            write_log(log_path, f"INFO  Successfully completed backup of {success_count}/{len(db_names)} databases.")
            return folder_remote_name
        else:
            write_log(log_path, f"INFO  [Cluster Backup] Streaming all {len(db_names)} databases into unified archive: {remote_name}")
            creationflags = 0x00004000 if os.name == 'nt' else 0

            def cluster_pg_feeder(target_stdin, stop_event, trk):
                try:
                    for idx, single_db in enumerate(db_names):
                        if stop_event.is_set():
                            break
                        write_log(log_path, f"INFO  [Cluster Backup {idx+1}/{len(db_names)}] Dumping database: {single_db}")
                        
                        hdr = (
                            f"\n-- >>> AUTO_BACKUP_DB_START: {single_db} <<<\n"
                            f"SELECT 'CREATE DATABASE \"{single_db}\"' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '{single_db}')\\gexec\n"
                            f"\\connect \"{single_db}\"\n"
                            f"SET synchronous_commit = 'off';\n"
                            f"DO $$ BEGIN SET session_replication_role = 'replica'; EXCEPTION WHEN OTHERS THEN NULL; END $$;\n"
                            f"SET work_mem = '256MB';\n"
                            f"SET maintenance_work_mem = '1GB';\n"
                        ).encode("utf-8")
                        target_stdin.write(hdr)
                        target_stdin.flush()
                        
                        dump_cmd = [
                            "pg_dump",
                            f"--host={db['host']}", f"--port={db['port']}",
                            f"--username={db['username']}", f"--dbname={single_db}",
                            "--no-password", "--format=plain",
                            "--clean", "--if-exists",
                            "--no-acl", "--no-owner",
                            "--verbose"
                        ]
                        if indexing_mode == "without_index":
                            dump_cmd += ["--section=pre-data", "--section=data"]
                        elif indexing_mode == "only_index":
                            dump_cmd += ["--section=post-data"]

                        env_db = pg_env(db)
                        pg_opts = None
                        if speed_profile == "extreme":
                            pg_opts = "-c statement_timeout=0 -c work_mem=256MB -c maintenance_work_mem=1GB -c max_parallel_workers_per_gather=4 -c effective_io_concurrency=32"
                        elif speed_profile == "balanced":
                            pg_opts = "-c statement_timeout=0 -c work_mem=64MB -c maintenance_work_mem=256MB -c max_parallel_workers_per_gather=2 -c effective_io_concurrency=8"
                        else:
                            pg_opts = "-c statement_timeout=0 -c work_mem=32MB -c maintenance_work_mem=128MB -c max_parallel_workers_per_gather=0 -c effective_io_concurrency=1"

                        if pg_opts and check_pg_options_supported(db, pg_opts, single_db):
                            env_db["PGOPTIONS"] = pg_opts

                        p_db = subprocess.Popen(
                            dump_cmd,
                            stdout=subprocess.PIPE,
                            stderr=open(log_path, "a"),
                            env=env_db,
                            creationflags=creationflags
                        )
                        if trk:
                            ACTIVE_PROCESSES.setdefault(trk.job_id, []).append(p_db)

                        optimize_pipe_fd(p_db.stdout)
                        optimize_pipe_fd(target_stdin)

                        buf_size = 4 * 1024 * 1024
                        byte_acc = 0
                        last_trk_time = time.time()

                        while True:
                            if stop_event.is_set():
                                try: p_db.kill()
                                except Exception: pass
                                break
                            chunk = p_db.stdout.read(buf_size)
                            if not chunk:
                                break
                            target_stdin.write(chunk)
                            if trk:
                                byte_acc += len(chunk)
                                now = time.time()
                                if byte_acc >= 4 * 1024 * 1024 or (now - last_trk_time) >= 0.5:
                                    trk.update_bytes(byte_acc)
                                    byte_acc = 0
                                    last_trk_time = now

                        if trk and byte_acc > 0:
                            trk.update_bytes(byte_acc)

                        p_db.wait()
                        if p_db.returncode != 0:
                            write_log(log_path, f"WARN  pg_dump for '{single_db}' exited with code {p_db.returncode}")

                        footer = (
                            f"\nDO $$ BEGIN SET session_replication_role = 'origin'; EXCEPTION WHEN OTHERS THEN NULL; END $$;\n"
                            f"SET synchronous_commit = 'on';\n"
                            f"-- >>> AUTO_BACKUP_DB_END: {single_db} <<<\n\n"
                        ).encode("utf-8")
                        target_stdin.write(footer)
                        target_stdin.flush()
                finally:
                    try:
                        target_stdin.close()
                    except Exception:
                        pass

            return stream_backup_to_storage(
                cmd=None,
                env=pg_env(db),
                storage=storage,
                remote_name=remote_name,
                log_path=log_path,
                compression_cmd=compression_cmd,
                tracker=tracker,
                speed_profile=speed_profile,
                custom_feeder=cluster_pg_feeder
            )

    incremental_tables = None
    if backup_method in ("incremental", "differential"):
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
        for i, c in enumerate(cmd):
            if c == "--compress=0":
                cmd[i] = "--compress=1"
                break
        write_log(log_path, "INFO  No fast compressor found in PATH. Using single-threaded native pg_dump compression.")
        
    env = pg_env(db)
    pg_opts = None
    if speed_profile == "extreme":
        pg_opts = "-c statement_timeout=0 -c work_mem=256MB -c maintenance_work_mem=1GB -c max_parallel_workers_per_gather=8 -c effective_io_concurrency=64"
    elif speed_profile == "safe":
        pg_opts = "-c statement_timeout=0 -c work_mem=16MB -c maintenance_work_mem=64MB -c max_parallel_workers_per_gather=0 -c effective_io_concurrency=1"
    else:  # default and balanced
        pg_opts = "-c statement_timeout=0 -c work_mem=128MB -c maintenance_work_mem=512MB -c max_parallel_workers_per_gather=4 -c effective_io_concurrency=32"

    if pg_opts:
        if check_pg_options_supported(db, pg_opts, "postgres" if (is_cluster or dbname == "__all__") else dbname):
            env["PGOPTIONS"] = pg_opts
        else:
            write_log(log_path, "INFO  Connection pooler / proxy detected (or startup parameters unsupported). Skipping PGOPTIONS for connection compatibility.")
        
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)


def run_pg_restore(db: dict, storage: dict, remote_name: str, log_path: str, new_database: bool = False, compression: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", collection: str = None, is_all: bool = False, selected_database: str = None, changes_only: bool = False, jobs: int = None):
    db = get_decrypted_db(db)
    storage = get_decrypted_storage(storage)
    dbname = (db.get("database_name") or "").strip()
    sql_db_filter = None
    
    settings = get_speed_settings(speed_profile)
    parallel_jobs = jobs or settings.get("restore_workers", 2)

    if selected_database:
        target_name = dbname if dbname and dbname != "__all__" else selected_database
        if new_database:
            create_cmd = [
                "psql", f"--host={db['host']}", f"--port={db['port']}",
                f"--username={db['username']}", "--no-password", "--dbname=postgres",
                "-c", f'CREATE DATABASE "{target_name}";'
            ]
            subprocess.run(create_cmd, capture_output=True, env=pg_env(db), timeout=30)
            
        cmd = [
            "psql",
            f"--host={db['host']}", f"--port={db['port']}",
            f"--username={db['username']}", f"--dbname={target_name}",
            "--no-password",
            "-v", "ON_ERROR_STOP=0"
        ]
        sql_db_filter = selected_database
    elif is_all:
        cmd = [
            "psql",
            f"--host={db['host']}", f"--port={db['port']}",
            f"--username={db['username']}", "--dbname=postgres",
            "--no-password",
            "-v", "ON_ERROR_STOP=0"
        ]
    else:
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
            "--disable-triggers",
            "--verbose"
        ]
        # Priority 4: if the input is a seekable file on disk, enable pg_restore -j
        if os.path.exists(remote_name) and parallel_jobs > 1:
            cmd += [f"--jobs={parallel_jobs}"]
            write_log(log_path, f"INFO  Enabling parallel table restore with pg_restore --jobs={parallel_jobs}")

        if not new_database:
            if changes_only:
                cmd += ["--data-only", "--no-data-for-failed-tables"]
                write_log(log_path, "INFO  Smart Sync / Diff Mode: applying changes with --data-only --no-data-for-failed-tables (tables and schema preserved).")
            else:
                cmd += ["--clean", "--if-exists"]
            
        if collection and collection != "full":
            cmd += ["-t", collection]
            
        if indexing_mode == "without_index":
            cmd += ["--section=pre-data", "--section=data"]
        elif indexing_mode == "only_index":
            cmd += ["--section=post-data"]
        
    decompression_cmd = None
    if compression == "zstd":
        decompression_cmd = ["zstd", "-d", "-c", "-q"]
    elif compression in ("pigz", "gzip"):
        if shutil.which("pigz"):
            decompression_cmd = ["pigz", "-d", "-c", "-p", str(settings['threads'])]
        elif shutil.which("gzip"):
            decompression_cmd = ["gzip", "-d", "-c"]
            
    env = pg_env(db)
    
    # Priority 4: PostgreSQL Server-Side Parallel Bulk Ingestion Options
    # sync_commit = off eliminates per-transaction fsync stalls during bulk ingestion
    # max_parallel_maintenance_workers allows PG to build each index using parallel background workers
    sync_commit = "off"
    work_mem = "256MB"
    maint_work_mem = "2GB"
    parallel_maint_workers = 4
    
    if speed_profile == "extreme":
        work_mem = "512MB"
        maint_work_mem = "4GB"
        parallel_maint_workers = 8
    elif speed_profile == "safe":
        sync_commit = "on"
        work_mem = "32MB"
        maint_work_mem = "128MB"
        parallel_maint_workers = 1
        
    pg_opts = f"-c statement_timeout=0 -c work_mem={work_mem} -c maintenance_work_mem={maint_work_mem} -c synchronous_commit={sync_commit} -c max_parallel_maintenance_workers={parallel_maint_workers} -c client_min_messages=warning"
    target_check_db = (dbname if dbname and dbname != "__all__" else (selected_database or "postgres"))
    if check_pg_options_supported(db, pg_opts, target_check_db):
        env["PGOPTIONS"] = pg_opts
    else:
        write_log(log_path, "INFO  Connection pooler / proxy detected. Skipping PGOPTIONS for restore compatibility.")
        
    stream_restore_from_storage(cmd, env, storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker, speed_profile=speed_profile, sql_db_filter=sql_db_filter, indexing_mode=indexing_mode, changes_only=changes_only)

