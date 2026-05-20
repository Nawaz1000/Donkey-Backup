import re

with open("backend/engine.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace run_mongo_backup
new_mongo_backup = '''def run_mongo_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str) -> str:
    m = parse_mongo_uri(db)
    dbname = m["dbname"]
    if not dbname: raise ValueError("Database name is required.")
    
    collection = backup_info.get("collection", "full")
    backup_method = backup_info.get("backup_method", "full")
    incremental_field = backup_info.get("incremental_field", "")
    
    cmd = ["mongodump"] + mongo_cmd_args(m) + [
        f"--db={dbname}",
        "--archive",
        "--gzip",
        "--numParallelCollections=1",
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
            
    return stream_backup_to_storage(cmd, os.environ.copy(), storage, remote_name, log_path)
'''
content = re.sub(r"def run_mongo_backup\(db: dict, collection: str,.*?return stream_backup_to_storage\(cmd, os\.environ\.copy\(\), storage, remote_name, log_path\)\n", new_mongo_backup, content, flags=re.DOTALL)

# Replace run_pg_backup
new_pg_backup = '''def run_pg_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str) -> str:
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
        "--compress=1",
        "--no-acl", "--no-owner",
    ]
    if collection and collection != "full":
        cmd += [f"--table={collection}"]
        
    return stream_backup_to_storage(cmd, pg_env(db), storage, remote_name, log_path)
'''
content = re.sub(r"def run_pg_backup\(db: dict, collection: str,.*?return stream_backup_to_storage\(cmd, pg_env\(db\), storage, remote_name, log_path\)\n", new_pg_backup, content, flags=re.DOTALL)

# Fix do_backup calls
content = content.replace("remote_path = run_mongo_backup(db, collection, storage, remote_name, log_path)", "remote_path = run_mongo_backup(db, backup, storage, remote_name, log_path)")
content = content.replace("remote_path = run_pg_backup(db, collection, storage, remote_name, log_path)", "remote_path = run_pg_backup(db, backup, storage, remote_name, log_path)")

with open("backend/engine.py", "w", encoding="utf-8") as f:
    f.write(content)
