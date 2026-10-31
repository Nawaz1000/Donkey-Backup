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
import uuid
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



BUNNY_KNOWN_REGIONS = [
    {"code": "de", "name": "Falkenstein / Europe (DE)", "rest_host": "storage.bunnycdn.com", "s3_url": "https://de-s3.storage.bunnycdn.com"},
    {"code": "ny", "name": "New York / US East (NY)", "rest_host": "ny.storage.bunnycdn.com", "s3_url": "https://ny-s3.storage.bunnycdn.com"},
    {"code": "la", "name": "Los Angeles / US West (LA)", "rest_host": "la.storage.bunnycdn.com", "s3_url": "https://la-s3.storage.bunnycdn.com"},
    {"code": "sg", "name": "Singapore / Asia (SG)", "rest_host": "sg.storage.bunnycdn.com", "s3_url": "https://sg-s3.storage.bunnycdn.com"},
    {"code": "syd", "name": "Sydney / Oceania (SYD)", "rest_host": "syd.storage.bunnycdn.com", "s3_url": "https://syd-s3.storage.bunnycdn.com"},
    {"code": "uk", "name": "London / United Kingdom (UK)", "rest_host": "uk.storage.bunnycdn.com", "s3_url": "https://uk-s3.storage.bunnycdn.com"},
    {"code": "se", "name": "Stockholm / Europe North (SE)", "rest_host": "se.storage.bunnycdn.com", "s3_url": "https://se-s3.storage.bunnycdn.com"},
    {"code": "br", "name": "São Paulo / South America (BR)", "rest_host": "br.storage.bunnycdn.com", "s3_url": "https://br-s3.storage.bunnycdn.com"},
    {"code": "jh", "name": "Johannesburg / Africa (JH)", "rest_host": "jh.storage.bunnycdn.com", "s3_url": "https://jh-s3.storage.bunnycdn.com"},
]


def resolve_bunny_hosts(region_url_or_code: str = "") -> tuple:
    """
    Returns (rest_host, s3_endpoint_url) based on region url or region code.
    E.g.
    'https://de-s3.storage.bunnycdn.com' -> ('storage.bunnycdn.com', 'https://de-s3.storage.bunnycdn.com')
    'https://ny-s3.storage.bunnycdn.com' -> ('ny.storage.bunnycdn.com', 'https://ny-s3.storage.bunnycdn.com')
    'https://ny.storage.bunnycdn.com'    -> ('ny.storage.bunnycdn.com', 'https://ny-s3.storage.bunnycdn.com')
    'https://storage.bunnycdn.com'       -> ('storage.bunnycdn.com', 'https://de-s3.storage.bunnycdn.com')
    'ny'                                 -> ('ny.storage.bunnycdn.com', 'https://ny-s3.storage.bunnycdn.com')
    'de' or ''                           -> ('storage.bunnycdn.com', 'https://de-s3.storage.bunnycdn.com')
    """
    raw = (region_url_or_code or "").strip().lower()
    if raw.startswith("http://"):
        raw = raw[7:]
    elif raw.startswith("https://"):
        raw = raw[8:]
    raw = raw.strip("/")

    if "storage.bunnycdn.com" in raw:
        prefix = raw.split(".")[0]
        if prefix in ("storage", "de", "de-s3"):
            return "storage.bunnycdn.com", "https://de-s3.storage.bunnycdn.com"
        reg = prefix.replace("-s3", "")
        return f"{reg}.storage.bunnycdn.com", f"https://{reg}-s3.storage.bunnycdn.com"

    if not raw or raw in ("de", "falkenstein", "default", "main"):
        return "storage.bunnycdn.com", "https://de-s3.storage.bunnycdn.com"

    reg = raw.replace("-s3", "")
    return f"{reg}.storage.bunnycdn.com", f"https://{reg}-s3.storage.bunnycdn.com"


def get_bunny_key(storage: dict) -> str:
    """
    Returns the real storage zone password/key.
    Prioritizes non-empty values that do not match the storage zone name.
    """
    if not isinstance(storage, dict):
        return ""
    if "_resolved_bunny" in storage and storage["_resolved_bunny"].get("bunny_key"):
        return storage["_resolved_bunny"]["bunny_key"]

    pwd = (storage.get("bunny_password") or "").strip()
    acc = (storage.get("bunny_access_key") or "").strip()
    zone = (storage.get("bunny_storage_zone") or "").strip()

    # If bunny_password is set and is NOT identical to zone name, use it
    if pwd and pwd.lower() != zone.lower():
        return pwd
    # If bunny_access_key is set and is NOT identical to zone name, use it
    if acc and acc.lower() != zone.lower():
        return acc
    return pwd or acc


def get_bunny_host(region_or_storage: any = None) -> str:
    if isinstance(region_or_storage, dict):
        if "_resolved_bunny" in region_or_storage and region_or_storage["_resolved_bunny"].get("rest_host"):
            return region_or_storage["_resolved_bunny"]["rest_host"]
        reg = region_or_storage.get("bunny_region_url") or region_or_storage.get("bunny_region") or ""
    else:
        reg = region_or_storage or ""
    rest_host, _ = resolve_bunny_hosts(reg)
    return rest_host


def verify_and_resolve_bunny(storage: dict, log_path: str = None) -> dict:
    """
    Validates Bunny credentials, auto-detects the storage zone's region endpoint
    across all known BunnyCDN regions, and verifies S3 multipart compatibility.
    """
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    zone = (storage.get("bunny_storage_zone") or "").strip()
    bunny_key = get_bunny_key(storage)
    pref_region = (storage.get("bunny_region_url") or storage.get("bunny_region") or "").strip()

    if not zone:
        return {"success": False, "error": "BunnyCDN Storage Zone Name is required."}
    if not bunny_key:
        return {
            "success": False,
            "error": "BunnyCDN Storage Zone Password is required. Please check your storage settings."
        }
    if bunny_key.strip().lower() == zone.strip().lower():
        return {
            "success": False,
            "error": (
                f"Invalid Storage Zone Password: The password cannot be the Storage Zone Name ('{zone}'). "
                "In Bunny.net, the password is found under Storage -> [Your Zone] -> 'FTP & API Access' tab. "
                "Please update your storage settings with the actual Password (do NOT use your Bunny Account API Key)."
            )
        }

    cand_rest, cand_s3 = resolve_bunny_hosts(pref_region)
    headers = {"AccessKey": bunny_key, "Accept": "application/json"}

    # 1. Test candidate host first
    cand_worked = False
    cand_status = None
    try:
        r = requests.get(f"https://{cand_rest}/{zone}/", headers=headers, timeout=6)
        cand_status = r.status_code
        if r.status_code == 200:
            cand_worked = True
    except Exception as e:
        cand_status = str(e)

    matched_region = None
    if cand_worked:
        matched_reg_info = next((rg for rg in BUNNY_KNOWN_REGIONS if rg["rest_host"] == cand_rest), None)
        matched_region = matched_reg_info or {
            "code": "custom",
            "name": cand_rest,
            "rest_host": cand_rest,
            "s3_url": cand_s3
        }
    else:
        # Candidate failed (e.g. 401, 404, or network timeout). Probe other known regions concurrently.
        other_regions = [rg for rg in BUNNY_KNOWN_REGIONS if rg["rest_host"] != cand_rest]
        statuses = {cand_rest: cand_status}

        def _probe(reg_info):
            host = reg_info["rest_host"]
            try:
                resp = requests.get(f"https://{host}/{zone}/", headers=headers, timeout=5)
                return reg_info, resp.status_code
            except Exception as ex:
                return reg_info, str(ex)

        with ThreadPoolExecutor(max_workers=min(8, len(other_regions))) as executor:
            future_to_reg = {executor.submit(_probe, rg): rg for rg in other_regions}
            for future in as_completed(future_to_reg):
                reg_info, st = future.result()
                statuses[reg_info["rest_host"]] = st
                if st == 200 and not matched_region:
                    matched_region = reg_info
                    for f in future_to_reg:
                        f.cancel()
                    break

        if matched_region and log_path:
            write_log(log_path, f"INFO  Auto-detected BunnyCDN storage zone '{zone}' is in region '{matched_region['name']}' ({matched_region['rest_host']}).")

    if not matched_region:
        all_401 = all(st == 401 for st in statuses.values() if isinstance(st, int))
        all_404 = all(st == 404 for st in statuses.values() if isinstance(st, int))
        if all_401:
            err = (
                "BunnyCDN authentication failed (401 Unauthorized) across all regions. "
                "Please verify your credentials in Bunny.net dashboard under Storage -> [Your Zone] -> 'FTP & API Access'. "
                "Make sure you copy the 'Password' (Read-Write password), NOT your main Bunny Account API Key."
            )
        elif all_404:
            err = f"BunnyCDN Storage Zone '{zone}' not found in any region. Please check that the Storage Zone Name matches your Bunny dashboard."
        else:
            err = f"Failed to connect to BunnyCDN storage zone '{zone}'. Endpoint statuses: {statuses}"
        return {"success": False, "error": err}

    rest_host = matched_region["rest_host"]
    s3_url = matched_region["s3_url"]

    # 2. Check S3-compatible API usability
    s3_usable = False
    try:
        import boto3
        from botocore.config import Config
        s3_cfg = Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            retries={"max_attempts": 2, "mode": "standard"},
            connect_timeout=6,
            read_timeout=10
        )
        s3 = boto3.client(
            "s3",
            endpoint_url=s3_url,
            aws_access_key_id=zone,  # Storage Zone Name is Access Key ID
            aws_secret_access_key=bunny_key,
            region_name="de",
            config=s3_cfg
        )
        try:
            s3.head_bucket(Bucket=zone)
            s3_usable = True
        except Exception:
            try:
                s3.list_objects_v2(Bucket=zone, MaxKeys=1)
                s3_usable = True
            except Exception as s3_err:
                s3_usable = False
                if log_path:
                    write_log(log_path, f"INFO  Bunny S3 API not usable ({s3_err}). Will use REST API.")

    return {
        "success": True,
        "zone": zone,
        "bunny_key": bunny_key,
        "rest_host": rest_host,
        "s3_url": s3_url,
        "region_code": matched_region.get("code", "de"),
        "region_name": matched_region.get("name", rest_host),
        "s3_usable": s3_usable,
        "error": None
    }


def get_s3_client(storage: dict):
    import boto3
    from botocore.config import Config
    
    config = Config(
        retries={'max_attempts': 3, 'mode': 'standard'},
        connect_timeout=30,
        read_timeout=3600
    )
    kwargs = {
        "aws_access_key_id": storage.get("s3_access_key"),
        "aws_secret_access_key": storage.get("s3_secret_key"),
        "config": config
    }
    if storage.get("s3_region"):
        kwargs["region_name"] = storage["s3_region"].strip()
    if storage.get("s3_endpoint_url"):
        kwargs["endpoint_url"] = storage["s3_endpoint_url"].strip()
        
    return boto3.client("s3", **kwargs)


def list_storage_objects(storage: dict, prefix: str) -> list:
    """List object keys matching prefix in the given storage."""
    stype = storage["type"]
    prefix = prefix or ""
    if stype == "azure":
        from azure.storage.blob import BlobServiceClient
        conn_str = f"DefaultEndpointsProtocol=https;AccountName={storage['azure_account_name']};AccountKey={storage['azure_account_key']};EndpointSuffix=core.windows.net"
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(storage["azure_container"])
        return [b.name for b in container.list_blobs(name_starts_with=prefix)]
    elif stype == "gcs":
        import json as _json
        from google.cloud import storage as gcs
        from google.oauth2 import service_account
        creds_dict = _json.loads(storage["gcs_credentials_json"])
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        client = gcs.Client(credentials=creds)
        bucket = client.bucket(storage["gcs_bucket"])
        return [b.name for b in bucket.list_blobs(prefix=prefix)]
    elif stype == "s3":
        s3 = get_s3_client(storage)
        paginator = s3.get_paginator('list_objects_v2')
        results = []
        for page in paginator.paginate(Bucket=storage["s3_bucket"], Prefix=prefix):
            for item in page.get("Contents", []):
                results.append(item["Key"])
        return results
    elif stype == "bunny":
        import requests
        from engine import resolve_bunny_hosts
        bunny_host = get_bunny_host(storage)
        bunny_key = get_bunny_key(storage)
        headers = {"AccessKey": bunny_key, "Accept": "application/json"}
        results = []

        def list_bunny_dir(subpath=""):
            clean = subpath.strip("/")
            url = f"https://{bunny_host}/{storage['bunny_storage_zone']}/{clean}/" if clean else f"https://{bunny_host}/{storage['bunny_storage_zone']}/"
            resp = requests.get(url, headers=headers, timeout=25)
            if resp.status_code == 200:
                for item in resp.json():
                    obj_name = item.get("ObjectName", "")
                    full_name = f"{clean}/{obj_name}".strip("/") if clean else obj_name
                    if item.get("IsDirectory"):
                        list_bunny_dir(full_name)
                    else:
                        if not prefix or full_name.startswith(prefix.strip("/")):
                            results.append(full_name)

        clean_prefix = prefix.strip("/")
        start_dir = clean_prefix if prefix.endswith("/") else (clean_prefix.rsplit("/", 1)[0] if "/" in clean_prefix else "")
        try:
            list_bunny_dir(start_dir)
        except Exception:
            pass

        # Fallback to Bunny S3-compatible API if REST returned nothing
        if not results:
            try:
                import boto3
                from botocore.config import Config
                _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or storage.get("bunny_region") or "")
                zone = (storage.get("bunny_storage_zone") or "").strip()
                s3 = boto3.client(
                    "s3",
                    endpoint_url=s3_url,
                    aws_access_key_id=zone,
                    aws_secret_access_key=bunny_key,
                    region_name="de",
                    config=Config(s3={'addressing_style': 'path'}, signature_version="s3v4", retries={'max_attempts': 2, 'mode': 'standard'}, connect_timeout=10, read_timeout=15)
                )
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=zone, Prefix=prefix):
                    for item in page.get("Contents", []):
                        results.append(item["Key"])
            except Exception:
                pass
        return results
    return []


def get_storage_object_size(storage: dict, remote_name: str) -> int:
    """Returns the size of a remote object in bytes, or 0 if unknown/error."""
    if not remote_name or not storage:
        return 0
    clean_remote = remote_name.lstrip("/")
    stype = storage.get("type")
    try:
        if stype == "azure":
            from azure.storage.blob import BlobServiceClient
            conn_str = f"DefaultEndpointsProtocol=https;AccountName={storage['azure_account_name']};AccountKey={storage['azure_account_key']};EndpointSuffix=core.windows.net"
            client = BlobServiceClient.from_connection_string(conn_str)
            container = client.get_container_client(storage["azure_container"])
            blob_props = container.get_blob_client(clean_remote).get_blob_properties()
            return blob_props.size or 0
        elif stype == "gcs":
            import json as _json
            from google.cloud import storage as gcs
            from google.oauth2 import service_account
            creds_dict = _json.loads(storage["gcs_credentials_json"])
            creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = gcs.Client(credentials=creds)
            bucket = client.bucket(storage["gcs_bucket"])
            blob = bucket.get_blob(clean_remote)
            return blob.size if blob else 0
        elif stype == "s3":
            s3 = get_s3_client(storage)
            head = s3.head_object(Bucket=storage["s3_bucket"], Key=clean_remote)
            return head.get("ContentLength", 0)
        elif stype == "bunny":
            import requests
            bunny_host = get_bunny_host(storage)
            bunny_key = get_bunny_key(storage)
            zone = (storage.get("bunny_storage_zone") or "").strip()
            url = f"https://{bunny_host}/{zone}/{clean_remote}"
            try:
                resp = requests.head(url, headers={"AccessKey": bunny_key}, timeout=15)
                if resp.status_code == 200 and "Content-Length" in resp.headers:
                    return int(resp.headers["Content-Length"])
            except Exception:
                pass
            try:
                import boto3
                from botocore.config import Config
                _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or storage.get("bunny_region") or "")
                s3 = boto3.client(
                    "s3",
                    endpoint_url=s3_url,
                    aws_access_key_id=zone,
                    aws_secret_access_key=bunny_key,
                    region_name="de",
                    config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4', connect_timeout=10, read_timeout=15)
                )
                head = s3.head_object(Bucket=zone, Key=clean_remote)
                return head.get("ContentLength", 0)
            except Exception:
                pass
    except Exception as e:
        log.warning(f"Failed to get storage object size for {remote_name}: {e}")
    return 0


def upload_metadata_file(storage: dict, remote_name: str, data_str: str):
    stype = storage["type"]
    if stype == "azure":
        from azure.storage.blob import BlobServiceClient
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage['azure_account_name']};"
            f"AccountKey={storage['azure_account_key']};"
            f"EndpointSuffix=core.windows.net"
        )
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(storage["azure_container"])
        container.upload_blob(name=remote_name, data=data_str, overwrite=True)
    elif stype == "gcs":
        import json
        from google.cloud import storage as gcs
        from google.oauth2 import service_account
        creds_dict = json.loads(storage["gcs_credentials_json"])
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        client = gcs.Client(credentials=creds)
        bucket = client.bucket(storage["gcs_bucket"])
        blob = bucket.blob(remote_name)
        blob.upload_from_string(data_str)
    elif stype == "s3":
        s3 = get_s3_client(storage)
        s3.put_object(
            Bucket=storage["s3_bucket"],
            Key=remote_name,
            Body=data_str.encode("utf-8"),
            ContentType="application/json"
        )
    elif stype == "bunny":
        import requests
        bunny_host = get_bunny_host(storage)
        bunny_key = get_bunny_key(storage)
        zone = (storage.get("bunny_storage_zone") or "").strip()
        access_key = (storage.get("bunny_access_key") or zone).strip()
        clean_remote = remote_name.lstrip("/")
        encoded = data_str.encode("utf-8")
        url = f"https://{bunny_host}/{zone}/{clean_remote}"
        headers = {"AccessKey": bunny_key, "Content-Type": "application/json", "Content-Length": str(len(encoded))}
        success = False
        try:
            resp = requests.put(url, headers=headers, data=encoded, timeout=30)
            if resp.status_code in (200, 201):
                success = True
        except Exception:
            pass
        if not success:
            try:
                import boto3
                from botocore.config import Config
                _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or storage.get("bunny_region") or "")
                s3 = boto3.client(
                    "s3",
                    endpoint_url=s3_url,
                    aws_access_key_id=zone,
                    aws_secret_access_key=bunny_key,
                    region_name="de",
                    config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4', connect_timeout=10, read_timeout=15)
                )
                s3.put_object(Bucket=zone, Key=clean_remote, Body=encoded, ContentType="application/json")
            except Exception:
                pass

def download_metadata_file(storage: dict, remote_name: str) -> str:
    try:
        stype = storage["type"]
        if stype == "azure":
            from azure.storage.blob import BlobServiceClient
            conn_str = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={storage['azure_account_name']};"
                f"AccountKey={storage['azure_account_key']};"
                f"EndpointSuffix=core.windows.net"
            )
            client = BlobServiceClient.from_connection_string(conn_str)
            container = client.get_container_client(storage["azure_container"])
            blob_client = container.get_blob_client(remote_name)
            if blob_client.exists():
                return blob_client.download_blob().readall().decode("utf-8")
        elif stype == "gcs":
            import json
            from google.cloud import storage as gcs
            from google.oauth2 import service_account
            creds_dict = json.loads(storage["gcs_credentials_json"])
            creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = gcs.Client(credentials=creds)
            bucket = client.bucket(storage["gcs_bucket"])
            blob = bucket.blob(remote_name)
            if blob.exists():
                return blob.download_as_string().decode("utf-8")
        elif stype == "s3":
            s3 = get_s3_client(storage)
            obj = s3.get_object(Bucket=storage["s3_bucket"], Key=remote_name)
            return obj["Body"].read().decode("utf-8")
        elif stype == "bunny":
            import requests
            bunny_host = get_bunny_host(storage)
            bunny_key = get_bunny_key(storage)
            zone = (storage.get("bunny_storage_zone") or "").strip()
            access_key = (storage.get("bunny_access_key") or zone).strip()
            clean_remote = remote_name.lstrip("/")
            url = f"https://{bunny_host}/{zone}/{clean_remote}"
            headers = {"AccessKey": bunny_key}
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    return resp.text
            except Exception:
                pass
            try:
                import boto3
                from botocore.config import Config
                _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or storage.get("bunny_region") or "")
                s3 = boto3.client(
                    "s3",
                    endpoint_url=s3_url,
                    aws_access_key_id=zone,
                    aws_secret_access_key=bunny_key,
                    region_name="de",
                    config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4', connect_timeout=10, read_timeout=15)
                )
                obj = s3.get_object(Bucket=zone, Key=clean_remote)
                return obj["Body"].read().decode("utf-8")
            except Exception:
                pass
    except Exception:
        pass
    return None


def delete_remote_backup(storage: dict, remote_name: str, folder_name: str = None):
    """
    Deletes a backup and its associated files and directory from remote storage.
    For BunnyCDN Edge Storage, deletes all matching files and sends a DELETE
    request with a trailing slash to remove the backup's directory as well,
    ensuring no empty folders remain behind.
    """
    if not remote_name or not storage:
        return

    stype = storage.get("type")
    clean_rn = remote_name.strip("/")
    base_folder = (folder_name or storage.get("folder_name") or "").strip().strip("/")
    while "//" in base_folder:
        base_folder = base_folder.replace("//", "/")

    # Determine if there is a backup-specific folder (e.g. timestamped directory)
    backup_dir = None
    if remote_name.endswith("/"):
        if clean_rn and clean_rn != base_folder:
            backup_dir = clean_rn
    else:
        if "/" in clean_rn:
            parent = clean_rn.rsplit("/", 1)[0]
            if parent != base_folder:
                backup_dir = parent

    if stype == "bunny":
        import requests
        bunny_host = get_bunny_host(storage)
        bunny_key = get_bunny_key(storage)
        zone = (storage.get("bunny_storage_zone") or "").strip()
        headers = {"AccessKey": bunny_key}

        # 1. Discover all files/objects associated with this backup
        list_prefix = (backup_dir + "/") if backup_dir else remote_name
        objects = list_storage_objects(storage, list_prefix)
        if clean_rn not in objects and not remote_name.endswith("/"):
            objects.append(clean_rn)

        # Delete all individual files
        for obj in objects:
            del_url = f"https://{bunny_host}/{zone}/{obj.lstrip('/')}"
            try:
                r = requests.delete(del_url, headers=headers, timeout=25)
                log.info(f"[BunnyCDN] Deleted file {obj} (status {r.status_code})")
            except Exception as e:
                log.warning(f"[BunnyCDN] Failed to delete file {obj}: {e}")

        # 2. Delete the backup directory itself from BunnyCDN Edge Storage!
        # BunnyCDN Edge Storage REST API deletes an entire directory when a DELETE
        # request is sent with a trailing slash.
        if backup_dir:
            clean_dir = backup_dir.strip("/")
            del_folder_url = f"https://{bunny_host}/{zone}/{clean_dir}/"
            try:
                r = requests.delete(del_folder_url, headers=headers, timeout=25)
                log.info(f"[BunnyCDN] Deleted directory {clean_dir}/ (status {r.status_code})")
            except Exception as e:
                log.warning(f"[BunnyCDN] Failed to delete directory {clean_dir}/: {e}")

            # Also attempt to delete virtual folder marker via S3 if present
            try:
                import boto3
                from botocore.config import Config
                _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or storage.get("bunny_region") or "")
                s3 = boto3.client(
                    "s3",
                    endpoint_url=s3_url,
                    aws_access_key_id=zone,
                    aws_secret_access_key=bunny_key,
                    region_name="de",
                    config=Config(s3={'addressing_style': 'path'}, signature_version="s3v4", retries={'max_attempts': 2, 'mode': 'standard'}, connect_timeout=10, read_timeout=15)
                )
                s3.delete_object(Bucket=zone, Key=f"{clean_dir}/")
            except Exception:
                pass

    elif stype == "s3":
        s3 = get_s3_client(storage)
        bucket = storage["s3_bucket"]
        search_prefix = (backup_dir + "/") if backup_dir else remote_name
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
            objects_to_delete = [{'Key': item['Key']} for item in page.get('Contents', [])]
            if objects_to_delete:
                s3.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
        if not remote_name.endswith("/"):
            try:
                s3.delete_object(Bucket=bucket, Key=clean_rn)
            except Exception:
                pass

    elif stype == "azure":
        from azure.storage.blob import BlobServiceClient
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage['azure_account_name']};"
            f"AccountKey={storage['azure_account_key']};"
            f"EndpointSuffix=core.windows.net"
        )
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(storage["azure_container"])
        search_prefix = (backup_dir + "/") if backup_dir else remote_name
        blobs = container.list_blobs(name_starts_with=search_prefix)
        for blob in blobs:
            try:
                container.delete_blob(blob.name)
            except Exception:
                pass
        if not remote_name.endswith("/"):
            try:
                container.delete_blob(clean_rn)
            except Exception:
                pass

    elif stype == "gcs":
        import json as _json
        from google.cloud import storage as gcs
        from google.oauth2 import service_account
        creds_dict = _json.loads(storage["gcs_credentials_json"])
        creds = service_account.Credentials.from_service_account_info(creds_dict)
        client = gcs.Client(credentials=creds)
        bucket = client.bucket(storage["gcs_bucket"])
        search_prefix = (backup_dir + "/") if backup_dir else remote_name
        blobs = bucket.list_blobs(prefix=search_prefix)
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                pass
        if not remote_name.endswith("/"):
            try:
                blob = bucket.get_blob(clean_rn)
                if blob:
                    blob.delete()
            except Exception:
                pass


def prune_schedule_backups(schedule_id: str, retention_count: int):
    """
    Prunes completed backups for a schedule exceeding the retention_count limit.
    Deletes both the database records and remote storage files/directories.
    """
    if not schedule_id or retention_count <= 0:
        return
    if not os.path.exists("data/backups.json") or not os.path.exists("data/storages.json"):
        return

    try:
        backups = read_json("data/backups.json")
        storages = read_json("data/storages.json")

        # Find all completed backups created by this schedule
        schedule_backups = [
            b for b in backups
            if b.get("schedule_id") == schedule_id and b.get("status") == "completed"
        ]

        # Sort descending by created_at (newest first)
        schedule_backups.sort(key=lambda x: x.get("created_at") or "", reverse=True)

        if len(schedule_backups) <= retention_count:
            return

        to_prune = schedule_backups[retention_count:]
        prune_ids = {b["id"] for b in to_prune}

        for old_b in to_prune:
            rn = old_b.get("remote_name")
            sid = old_b.get("storage_id")
            if rn and sid:
                st = next((s for s in storages if s["id"] == sid), None)
                if st:
                    try:
                        delete_remote_backup(st, rn, folder_name=old_b.get("folder_name"))
                        log.info(f"Retention prune: deleted remote backup {old_b.get('label')}")
                    except Exception as e:
                        log.warning(f"Retention prune failed for {old_b.get('label')}: {e}")

        updated_backups = [b for b in backups if b["id"] not in prune_ids]
        write_json("data/backups.json", updated_backups)
        log.info(f"Retention prune: removed {len(prune_ids)} old backup(s) for schedule {schedule_id}")
    except Exception as e:
        log.error(f"Error during schedule backup pruning: {e}")


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
        self.start_time = time.time()
        self.speed_mbps = 0.0

    def update_bytes(self, num_bytes: int):
        self.bytes_processed += num_bytes
        now = time.time()
        elapsed = max(0.1, now - self.start_time)
        self.speed_mbps = round((self.bytes_processed / (1024 * 1024)) / elapsed, 2)

        if self.total_size > 0:
            pct = min(99, int((self.bytes_processed / self.total_size) * 100))
            self.update_pct(pct)
        elif now - self.last_update_time >= 1.0:
            # Periodic update every second even if total_size is not yet known
            self.update_pct(self.last_pct if self.last_pct >= 0 else 0)

    def update_pct(self, pct: int):
        now = time.time()
        if pct != self.last_pct or now - self.last_update_time >= 1.0:
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
        buf_size = 2 * 1024 * 1024  # 2MB chunks for maximum streaming throughput
        while True:
            chunk = src.read(buf_size)
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
        except Exception:
            pass
        try:
            src.close()
        except Exception:
            pass
        try:
            src.close()
        except Exception:
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

        with self.lock:
            # 1. Block and wait for at least some data if buffer is empty
            if self.buffer_size == 0 and not self.eof:
                while not self.eof:
                    try:
                        chunk = self.q.get(timeout=0.5)
                        if chunk is None:
                            self.eof = True
                        else:
                            self.buffer.append(memoryview(chunk))
                            self.buffer_size += len(chunk)
                        break
                    except queue.Empty:
                        if self.stop_event.is_set():
                            self.eof = True
                            break

            # 2. Drain any available chunks without blocking to fill up to size
            while self.buffer_size < size and not self.eof:
                try:
                    chunk = self.q.get_nowait()
                    if chunk is None:
                        self.eof = True
                    else:
                        self.buffer.append(memoryview(chunk))
                        self.buffer_size += len(chunk)
                except queue.Empty:
                    break

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
            size = 64 * 1024
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
                    q.put(data, timeout=0.5)
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


def get_speed_settings(profile: str):
    cores = os.cpu_count() or 4
    if profile == "safe":
        return {"threads": 1, "chunk_size": 4 * 1024 * 1024, "queue_max": 4, "concurrency": 2, "mongo_parallel": 1}
    elif profile == "balanced":
        return {"threads": max(1, cores // 2), "chunk_size": 16 * 1024 * 1024, "queue_max": 8, "concurrency": 4, "mongo_parallel": 2}
    elif profile == "extreme":
        return {"threads": cores, "chunk_size": 64 * 1024 * 1024, "queue_max": 8, "concurrency": 16, "mongo_parallel": 8}
    else: # default
        return {"threads": max(1, cores - 1), "chunk_size": 32 * 1024 * 1024, "queue_max": 8, "concurrency": 8, "mongo_parallel": 4}

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
    if tracker:
        ACTIVE_PROCESSES.setdefault(tracker.job_id, [])
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
            if tracker:
                ACTIVE_PROCESSES[tracker.job_id].append(p_dump)
            
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
            if tracker:
                ACTIVE_PROCESSES[tracker.job_id].append(p_dump)
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

        elif storage["type"] == "s3":
            from boto3.s3.transfer import TransferConfig
            client = get_s3_client(storage)
            chunk_size = max(16 * 1024 * 1024, settings.get("chunk_size", 32 * 1024 * 1024))
            concurrency = settings.get("concurrency", 8)
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

            concurrency = settings.get("concurrency", 8)
            chunk_size = max(16 * 1024 * 1024, settings.get("chunk_size", 32 * 1024 * 1024))

            write_log(log_path, f"INFO  Connecting to BunnyCDN storage zone '{zone}' ({res['region_name']} - S3: {s3_url}, REST: {rest_host})")

            s3_client = None
            if s3_available:
                try:
                    s3_config = Config(
                        s3={"addressing_style": "path"},
                        signature_version="s3v4",
                        retries={"max_attempts": 3, "mode": "standard"},
                        connect_timeout=15,
                        read_timeout=300,
                        max_pool_connections=max(32, concurrency * 2)
                    )
                    s3_client = boto3.client(
                        "s3",
                        endpoint_url=s3_url,
                        aws_access_key_id=zone,
                        aws_secret_access_key=bunny_key,
                        region_name="de",
                        config=s3_config
                    )
                    write_log(log_path, f"INFO  BunnyCDN S3-compatible API verified. Streaming upload via parallel multipart ({concurrency} workers, {round(chunk_size/(1024*1024))}MB chunks)...")
                except Exception as s3_init_err:
                    s3_available = False
                    write_log(log_path, f"INFO  Bunny S3 client setup failed ({s3_init_err}). Falling back to REST API...")

            if s3_available and s3_client:
                # Bunny S3 multipart streaming: parallel multi-threaded chunks
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
                # Bunny REST API fallback: spool stream using fast C-level 4MB chunk buffer
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

            # Final verification that file exists and has size > 0
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


def stream_restore_from_storage(cmd: list, env: dict, storage: dict, remote_name: str, log_path: str, decompression_cmd: list = None, tracker: ProgressTracker = None, speed_profile: str = "default"):
    write_log(log_path, f"INFO  Starting streaming restore from {storage['type']}: {remote_name} (Profile: {speed_profile})")
    
    settings = get_speed_settings(speed_profile)
    
    processes = []
    stop_event = ACTIVE_STOP_EVENTS.get(tracker.job_id) if tracker else threading.Event()
    if tracker:
        ACTIVE_PROCESSES.setdefault(tracker.job_id, [])
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
                if tracker:
                    ACTIVE_PROCESSES[tracker.job_id].append(p_dec)
                
                p_restore = subprocess.Popen(
                    cmd,
                    stdin=p_dec.stdout,
                    stdout=err_file,
                    stderr=subprocess.PIPE,  # Pipe stderr to parse progress
                    env=env,
                    creationflags=creationflags
                )
                processes.append(p_restore)
                if tracker:
                    ACTIVE_PROCESSES[tracker.job_id].append(p_restore)
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

            elif storage["type"] == "s3":
                s3_client = get_s3_client(storage)
                s3_obj = s3_client.get_object(Bucket=storage["s3_bucket"], Key=remote_name)
                body_stream = s3_obj["Body"]
                chunk_size = max(4 * 1024 * 1024, settings.get("chunk_size", 16 * 1024 * 1024))
                prefetch_q = queue.Queue(maxsize=max(4, settings.get("queue_max", 8)))
                download_err = [None]

                def s3_dl_worker():
                    try:
                        while not stop_event.is_set():
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
                        if stop_event.is_set():
                            raise RuntimeError("Restore cancelled by user.")
                        try:
                            chunk = prefetch_q.get(timeout=1.0)
                        except queue.Empty:
                            if not dl_thread.is_alive():
                                break
                            continue
                        if chunk is None:
                            break
                        stdin_stream.write(chunk)
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
                s3_usable = bunny_res.get("s3_usable", False)
                clean_remote = remote_name.lstrip("/")

                chunk_size = max(4 * 1024 * 1024, settings.get("chunk_size", 16 * 1024 * 1024))
                prefetch_q = queue.Queue(maxsize=max(4, settings.get("queue_max", 8)))
                download_err = [None]

                def bunny_dl_worker():
                    try:
                        # Attempt S3 streaming first if usable
                        if s3_usable:
                            try:
                                import boto3
                                from botocore.config import Config
                                s3 = boto3.client(
                                    "s3",
                                    endpoint_url=s3_url,
                                    aws_access_key_id=zone,
                                    aws_secret_access_key=bunny_key,
                                    region_name="de",
                                    config=Config(
                                        s3={'addressing_style': 'path'},
                                        signature_version="s3v4",
                                        retries={'max_attempts': 3, 'mode': 'standard'},
                                        connect_timeout=15,
                                        read_timeout=3600,
                                        max_pool_connections=16
                                    )
                                )
                                s3_obj = s3.get_object(Bucket=zone, Key=clean_remote)
                                body_stream = s3_obj["Body"]
                                write_log(log_path, f"INFO  Streaming restore download via BunnyCDN S3 API ({s3_url}) with decoupled prefetch buffer...")
                                while not stop_event.is_set():
                                    chunk = body_stream.read(chunk_size)
                                    if not chunk:
                                        break
                                    if dl_tracker:
                                        dl_tracker.update_bytes(len(chunk))
                                    prefetch_q.put(chunk)
                                return
                            except Exception as s3_dl_ex:
                                write_log(log_path, f"WARN  Bunny S3 restore download failed ({s3_dl_ex}). Falling back to REST stream...")

                        # High-speed REST stream with persistent connection pooling
                        session = requests.Session()
                        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=Retry(total=3, backoff_factor=0.5))
                        session.mount("https://", adapter)
                        url = f"https://{rest_host}/{zone}/{clean_remote}"
                        headers = {"AccessKey": bunny_key}
                        write_log(log_path, f"INFO  Streaming restore download via BunnyCDN REST API ({rest_host}) with decoupled prefetch buffer...")
                        with session.get(url, headers=headers, stream=True, timeout=(15, 3600)) as resp:
                            if resp.status_code != 200:
                                raise RuntimeError(f"Bunny storage download failed (HTTP {resp.status_code}): {resp.text[:300]}")
                            for chunk in resp.iter_content(chunk_size=chunk_size):
                                if not chunk:
                                    continue
                                if stop_event.is_set():
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
                        if stop_event.is_set():
                            raise RuntimeError("Restore cancelled by user.")
                        try:
                            chunk = prefetch_q.get(timeout=1.0)
                        except queue.Empty:
                            if not dl_thread.is_alive():
                                break
                            continue
                        if chunk is None:
                            break
                        stdin_stream.write(chunk)
                finally:
                    dl_thread.join(timeout=3.0)
                    if download_err[0]:
                        raise RuntimeError(f"Bunny download stream failed: {download_err[0]}")
                stdin_stream.close()

            else:
                raise ValueError(f"Unknown storage type: {storage['type']}")
                
        except Exception as e:
            for p in processes:
                try: p.kill()
                except Exception: pass
            raise RuntimeError(f"Streaming download failed: {e}")
        finally:
            stop_event.set()
            try:
                stdin_stream.close()
            except Exception:
                pass
            for p in reversed(processes):
                try:
                    p.wait(timeout=5.0)
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
            
        write_log(log_path, f"INFO  Streaming restore successful.")


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


# ─── MONGODB BACKUP ──────────────────────────────────────────────────────────

def run_mongo_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", is_cluster: bool = False) -> str:
    m = parse_mongo_uri(db)
    dbname = backup_info.get("database_name") or db.get("database_name") or m["dbname"]
    if not dbname and not is_cluster: raise ValueError("Database name is required.")
    
    collection = backup_info.get("collection", "full")
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
            
            # Update size for history records
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
    if is_cluster:
        import pymongo
        write_log(log_path, "INFO  Cluster-wide scheduled backup requested. Fetching databases...")
        try:
            client = pymongo.MongoClient(m["uri"])
            db_names = [d for d in client.list_database_names() if d not in ("local", "config", "admin")]
            client.close()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch databases for cluster backup: {e}")
            
        write_log(log_path, f"INFO  Found {len(db_names)} databases to backup: {', '.join(db_names)}")
        
        comp_info = get_compressor_info(speed_profile, log_path)
        ext = comp_info["ext"] if comp_info else "gz"
        compression_cmd = comp_info["cmd"] if comp_info else None
        
        uri_no_db = m["uri"].replace(f"/{m['dbname']}?", "/?").replace(f"/{m['dbname']}", "/") if m['dbname'] else m["uri"]
        
        last_remote_path = ""
        for idx, db_name in enumerate(db_names):
            # Reset the stop_event before each database so the previous iteration's
            # finally-block stop_event.set() doesn't cancel this one immediately.
            if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()

            write_log(log_path, f"INFO  [Cluster Backup] Backing up database {idx+1}/{len(db_names)}: {db_name}")
            file_remote_name = f"{remote_name}{db_name}.archive.{ext}"
            
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
            last_remote_path = stream_backup_to_storage(cmd, env, storage, file_remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)
            
        return remote_name # return the folder path

    cmd = [
        "mongodump",
        f"--uri={m['uri']}",
        f"--db={dbname}",
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
    
    backup_mongo_indexes(m["uri"], dbname, collection, storage, remote_name + ".indexes.json", log_path)
    
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)


# ─── MONGODB RESTORE ─────────────────────────────────────────────────────────

def run_mongo_restore(db: dict, collection: str, storage: dict, remote_name: str, log_path: str, drop_existing: bool = True, compression: str = None, source_dbname: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", target_dbname: str = None):
    m = parse_mongo_uri(db)
    dbname = target_dbname or db.get("database_name") or m["dbname"]
    if not dbname: raise ValueError("Database name is required for restore.")

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
                # Try with .indexes.json extension if the main file was a standard backup
                indexes_metadata = download_metadata_file(storage, remote_name + ".indexes.json")
                
            if not indexes_metadata:
                raise RuntimeError("Index metadata file not found on storage")
                
            indexes_data = json.loads(indexes_metadata)
            
            # Check if it is the new format:
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
                # Old format: { collection_name: index_spec }
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

    if indexing_mode == "without_index" or use_no_index_restore:
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

    if use_no_index_restore and indexes_metadata:
        try:
            import pymongo
            import json
            all_indexes = json.loads(indexes_metadata)
            write_log(log_path, f"INFO  Starting sequential index creation for database: {dbname}")
            
            client = pymongo.MongoClient(m["uri"])
            for src_db_name, collections_data in all_indexes.items():
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
                            
            client.close()
            write_log(log_path, "INFO  Sequential index creation completed successfully.")
        except Exception as e:
            write_log(log_path, f"WARN  Error during sequential index restoration: {e}")


_pg_options_support_cache = {}

def check_pg_options_supported(db: dict, test_options: str = "-c statement_timeout=0", dbname: str = None) -> bool:
    """
    Check if PGOPTIONS can be passed safely to PostgreSQL.
    Connection poolers like PgBouncer (commonly on port 6432 or proxies) reject startup parameters
    in PGOPTIONS with 'FATAL: unsupported startup parameter in options: statement_timeout'.
    """
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
            err = (res.stderr or "").lower()
            if "unsupported startup parameter in options" in err or "pgbouncer" in err:
                _pg_options_support_cache[cache_key] = False
                return False
        _pg_options_support_cache[cache_key] = True
        return True
    except Exception:
        _pg_options_support_cache[cache_key] = False
        return False


# ─── POSTGRESQL BACKUP ───────────────────────────────────────────────────────

def run_pg_backup(db: dict, backup_info: dict, storage: dict, remote_name: str, log_path: str, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", is_cluster: bool = False) -> str:
    dbname = (backup_info.get("database_name") or db.get("database_name") or "").strip()
    collection = backup_info.get("collection", "full")
    backup_method = backup_info.get("backup_method", "full")
    
    if is_cluster or dbname == "__all__":
        write_log(log_path, "INFO  All Databases requested. Enumerating accessible databases...")
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

        write_log(log_path, f"INFO  Found {len(db_names)} databases to backup: {', '.join(db_names)}")

        comp_info = get_compressor_info(speed_profile, log_path)
        ext = comp_info["ext"] if comp_info else "gz"
        compression_cmd = comp_info["cmd"] if comp_info else None

        folder_remote_name = remote_name if remote_name.endswith("/") else f"{remote_name}/"
        total_uploaded_bytes = 0
        success_count = 0

        for idx, single_db in enumerate(db_names):
            # Reset the stop_event before each database so the previous iteration's
            # finally-block stop_event.set() doesn't cancel this one immediately.
            if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()

            write_log(log_path, f"INFO  [Instance Backup] Backing up database {idx+1}/{len(db_names)}: {single_db}")
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
            pg_opts = None
            if speed_profile == "extreme":
                pg_opts = "-c statement_timeout=0 -c work_mem=128MB -c maintenance_work_mem=1GB -c max_parallel_workers_per_gather=4 -c effective_io_concurrency=32"
            elif speed_profile == "balanced":
                pg_opts = "-c statement_timeout=0 -c work_mem=32MB -c maintenance_work_mem=128MB -c max_parallel_workers_per_gather=2 -c effective_io_concurrency=4"
            else:
                pg_opts = "-c statement_timeout=0 -c work_mem=16MB -c maintenance_work_mem=64MB -c max_parallel_workers_per_gather=0 -c effective_io_concurrency=1"

            if pg_opts and check_pg_options_supported(db, pg_opts, single_db):
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
    pg_opts = None
    if speed_profile == "extreme":
        pg_opts = "-c statement_timeout=0 -c work_mem=128MB -c maintenance_work_mem=1GB -c max_parallel_workers_per_gather=4 -c effective_io_concurrency=32"
    elif speed_profile == "balanced":
        pg_opts = "-c statement_timeout=0 -c work_mem=32MB -c maintenance_work_mem=128MB -c max_parallel_workers_per_gather=2 -c effective_io_concurrency=4"
    else:
        pg_opts = "-c statement_timeout=0 -c work_mem=16MB -c maintenance_work_mem=64MB -c max_parallel_workers_per_gather=0 -c effective_io_concurrency=1"

    if pg_opts:
        if check_pg_options_supported(db, pg_opts, dbname):
            env["PGOPTIONS"] = pg_opts
        else:
            write_log(log_path, "INFO  Connection pooler / proxy detected (or startup parameters unsupported). Skipping PGOPTIONS for connection compatibility.")
        
    return stream_backup_to_storage(cmd, env, storage, remote_name, log_path, compression_cmd=compression_cmd, tracker=tracker, speed_profile=speed_profile)


# ─── POSTGRESQL RESTORE ──────────────────────────────────────────────────────

def run_pg_restore(db: dict, storage: dict, remote_name: str, log_path: str, new_database: bool = False, compression: str = None, indexing_mode: str = "with_index", tracker: ProgressTracker = None, speed_profile: str = "default", collection: str = None):
    dbname = (db.get("database_name") or "").strip()
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
        
    if collection and collection != "full":
        cmd += ["-t", collection]
        
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
    
    # Resource-optimized PostgreSQL restore settings based on profile
    sync_commit = "on"
    work_mem = "32MB"
    maint_work_mem = "128MB"
    
    if speed_profile == "extreme":
        sync_commit = "off"
        work_mem = "64MB"
        maint_work_mem = "512MB"
        cmd += ["--disable-triggers"]
    elif speed_profile == "balanced":
        sync_commit = "on"
        work_mem = "32MB"
        maint_work_mem = "128MB"
    elif speed_profile == "safe":
        sync_commit = "on"
        work_mem = "16MB"
        maint_work_mem = "64MB"
        
    pg_opts = f"-c statement_timeout=0 -c work_mem={work_mem} -c maintenance_work_mem={maint_work_mem} -c synchronous_commit={sync_commit}"
    if check_pg_options_supported(db, pg_opts, dbname):
        env["PGOPTIONS"] = pg_opts
    else:
        write_log(log_path, "INFO  Connection pooler / proxy detected. Skipping PGOPTIONS for restore compatibility.")
        
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
    
    # We do NOT set tracker.total_size based on Solr index size because the JSON dump stream size 
    # has no predictable relationship with the Lucene index binary size. Instead, the progress 
    # will be parsed directly from the percentage outputted by solr_helper.py.
    
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
        cancel_job(backup_id)
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
        is_cluster = False
        if target_db_name == "__all__":
            is_cluster = True
            base_name = f"{safe_db_name}_all"
            remote_name = f"{base_name}_{date_str}/"
        elif db["type"] == "mongodb" and collection == "full" and backup.get("is_scheduled") and not target_db_name:
            is_cluster = True
            base_name = "Mongdbbackup"
            remote_name = f"{base_name}_{date_str}/"
        elif collection and collection != "full":
            base_name = collection.strip().replace(" ", "_")
            remote_name = f"{base_name}-{date_str}/backup.archive.{ext}"
        else:
            base_name = safe_db_name
            remote_name = f"{base_name}-{date_str}/backup.archive.{ext}"

        # Prepend folder name if specified on backup or default on storage
        folder = (backup.get("folder_name") or storage.get("folder_name") or "").strip().strip("/")
        while "//" in folder:
            folder = folder.replace("//", "/")
        if folder:
            remote_name = f"{folder}/{remote_name}"

        # Initialize progress tracker
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

        # Fetch actual uploaded size from storage (set by stream_backup_to_storage)
        raw_bytes = getattr(stream_backup_to_storage, "last_size_bytes", 0) or 0
        if raw_bytes == 0:
            return fail(f"Backup verification failed: Uploaded backup on storage '{storage.get('name')}' is 0 bytes or was not saved.")
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
            completed_at=datetime.utcnow().isoformat() + "Z",
            duration_seconds=duration
        )

        # Check schedule retention policy if this was a scheduled backup
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
        cancel_job(backup_id)
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

        # Override dbname if new database or if explicit target database is specified
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

        # Resolve remote object size if total_size is 0 for exact progress calculation
        if total_size <= 0 and not remote_name.endswith("/"):
            total_size = get_storage_object_size(storage, remote_name)
            if total_size > 0:
                write_log(log_path, f"INFO  Remote backup archive size: {round(total_size / (1024*1024), 2)} MB")

        if total_size > 0:
            update_restore(restore_id, total_mb=round(total_size / (1024 * 1024), 2))

        # Initialize progress tracker
        tracker = ProgressTracker(restore_id, is_restore=True, total_size=total_size, log_path=log_path)
        indexing_mode = restore.get("indexing_mode", "with_index")

        if target_db["type"] == "mongodb":
            if remote_name.endswith("/") or "Mongdbbackup" in remote_name:
                write_log(log_path, "INFO  Cluster restore detected. Discovering databases...")
                objects = [b for b in list_storage_objects(storage, remote_name) if ".archive." in b]
                
                write_log(log_path, f"INFO  Found {len(objects)} databases to restore.")
                for obj in objects:
                    db_name = obj.split("/")[-1].split(".archive.")[0]
                    write_log(log_path, f"INFO  [Cluster Restore] Restoring database: {db_name}")
                    obj_compression = "zstd" if ".zst" in obj else ("gzip" if ".gz" in obj else compression)
                    if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                        ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()
                    run_mongo_restore(target_db, "full", storage, obj, log_path, drop_existing=not restore.get("new_database", False), compression=obj_compression, source_dbname=db_name, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), target_dbname=db_name)
            else:
                run_mongo_restore(target_db, collection, storage, remote_name, log_path, drop_existing=not restore.get("new_database", False), compression=compression, source_dbname=source_dbname, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), target_dbname=target_db.get("database_name"))
        elif target_db["type"] == "postgresql":
            if remote_name.endswith("/"):
                # Instance backup — folder contains individual database archives
                write_log(log_path, "INFO  PostgreSQL instance restore detected. Discovering database archives...")
                objects = [b for b in list_storage_objects(storage, remote_name) if ".archive." in b]

                if not objects:
                    return fail(f"No database archives found in instance backup folder: {remote_name}")

                write_log(log_path, f"INFO  Found {len(objects)} database archives to restore.")

                for idx_obj, obj in enumerate(objects):
                    db_file = obj.split("/")[-1]
                    db_name = db_file.split(".archive.")[0]
                    write_log(log_path, f"INFO  [Instance Restore] Restoring database {idx_obj+1}/{len(objects)}: {db_name}")

                    # Detect compression from filename
                    obj_compression = compression
                    if ".zst" in obj:
                        obj_compression = "zstd"
                    elif ".gz" in obj:
                        obj_compression = "gzip"

                    # Reset stop_event for each database restore iteration
                    if tracker and tracker.job_id in ACTIVE_STOP_EVENTS:
                        ACTIVE_STOP_EVENTS[tracker.job_id] = threading.Event()

                    restore_db = {**target_db, "database_name": db_name}
                    run_pg_restore(restore_db, storage, obj, log_path, new_database=restore.get("new_database", False), compression=obj_compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), collection=collection)

                write_log(log_path, f"INFO  Instance restore completed: {len(objects)} databases restored.")
            else:
                run_pg_restore(target_db, storage, remote_name, log_path, new_database=restore.get("new_database", False), compression=compression, indexing_mode=indexing_mode, tracker=tracker, speed_profile=restore.get("speed_profile", "default"), collection=collection)
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
        cancel_job(restore_id)
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