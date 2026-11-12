"""
BackupVault Engine - Apache Solr Driver
HTTP-based JSON-streamed Solr collection backup and restore engine.
"""
import os
import sys
import time
import shutil
import base64
import urllib.request
import json
import ssl

try:
    from ..core import write_log, log, ProgressTracker
    from ..compressors import get_compressor_info
    from ..pipeline import stream_backup_to_storage, stream_restore_from_storage
except ImportError:
    from engine.core import write_log, log, ProgressTracker
    from engine.compressors import get_compressor_info
    from engine.pipeline import stream_backup_to_storage, stream_restore_from_storage

from utils import get_decrypted_db, get_decrypted_storage


def get_solr_base_url(host: str, port: int) -> str:
    host = host.strip()
    if host.startswith("http://") or host.startswith("https://"):
        from urllib.parse import urlparse
        parsed = urlparse(host)
        return f"{parsed.scheme}://{parsed.netloc}"
    scheme = "https" if port == 443 else "http"
    return f"{scheme}://{host}:{port}"


def _get_solr_helper_path() -> str:
    # Look in backend root first, then fallback to current directory
    backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    helper_path = os.path.join(backend_dir, "solr_helper.py")
    if os.path.exists(helper_path):
        return helper_path
    return os.path.join(os.path.dirname(__file__), "solr_helper.py")


def run_solr_backup(db: dict, backup: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default") -> str:
    db = get_decrypted_db(db)
    storage = get_decrypted_storage(storage)
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

    comp_info = get_compressor_info(speed_profile, log_path)
    compression_cmd = comp_info["cmd"] if comp_info else ["gzip", "-c"]

    if len(collections_to_backup) > 1:
        write_log(log_path, f"WARN  Multiple collections selected. Only '{collections_to_backup[0]}' will be backed up in this stream.")
        
    coll = collections_to_backup[0]
    helper_path = _get_solr_helper_path()
    cmd = [sys.executable, helper_path, "dump", solr_base_url, coll, auth_str]
    
    stream_backup_to_storage(cmd, {}, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)
    write_log(log_path, "INFO  Solr backup streamed successfully via HTTP API.")
        
    return f"{storage['type']}://{remote_name}"


def run_solr_restore(db: dict, collection: str, storage: dict, remote_name: str, log_path: str, drop_existing: bool = False, compression: str = None, source_dbname: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default"):
    db = get_decrypted_db(db)
    storage = get_decrypted_storage(storage)
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
            context = ssl._create_unverified_context()
            headers = {"Authorization": auth_str} if auth_str != "None" else {}
            
            coll_url = f"{solr_base_url}/solr/admin/collections?action=CREATE&name={target_collection}&numShards=1&wt=json"
            req = urllib.request.Request(coll_url, headers=headers)
            try:
                with urllib.request.urlopen(req, context=context, timeout=15) as resp:
                    pass
            except Exception:
                core_url = f"{solr_base_url}/solr/admin/cores?action=CREATE&name={target_collection}&wt=json"
                req2 = urllib.request.Request(core_url, headers=headers)
                with urllib.request.urlopen(req2, context=context, timeout=15) as resp2:
                    pass
            write_log(log_path, f"INFO  Successfully created new Solr collection/core.")
        except Exception as e:
            write_log(log_path, f"WARN  Failed to create new Solr collection. It may already exist or API not supported: {e}")

    comp_info = get_compressor_info(speed_profile, log_path)
    decompression_cmd = comp_info.get("decompress_cmd") if comp_info else None

    if not decompression_cmd:
        if remote_name.endswith(".zst"):
            decompression_cmd = ["zstd", "-d", "-c"]
        elif remote_name.endswith(".gz") or remote_name.endswith(".tar.gz"):
            if shutil.which("pigz"):
                decompression_cmd = ["pigz", "-d", "-c"]
            else:
                decompression_cmd = ["gzip", "-d", "-c"]
            
    helper_path = _get_solr_helper_path()
    cmd = [sys.executable, helper_path, "restore", solr_base_url, target_collection, auth_str]
    
    stream_restore_from_storage(cmd, {}, storage, remote_name, log_path, decompression_cmd=decompression_cmd, tracker=tracker, speed_profile=speed_profile)
    write_log(log_path, "INFO  Solr restore streamed successfully via HTTP API.")

