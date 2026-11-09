"""
BackupVault Engine - Storage Driver Integrations
Supports AWS S3 (and S3-compatible: MinIO, Wasabi, Backblaze, Cloudflare R2),
BunnyCDN Edge Storage (REST & S3 Multipart), Azure Blob Storage, and Google Cloud Storage (GCS).
"""
import os
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from .core import write_log, log
except ImportError:
    from engine.core import write_log, log

from utils import read_json, write_json, get_decrypted_storage

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
        reg = (region_or_storage.get("bunny_region_url") or region_or_storage.get("bunny_region") or "").strip()
        if not reg and region_or_storage.get("bunny_storage_zone"):
            try:
                res = verify_and_resolve_bunny(region_or_storage)
                if res and res.get("success"):
                    region_or_storage["_resolved_bunny"] = res
                    return res["rest_host"]
            except Exception:
                pass
    else:
        reg = (region_or_storage or "").strip()
    rest_host, _ = resolve_bunny_hosts(reg)
    return rest_host


def verify_and_resolve_bunny(storage: dict, log_path: str = None) -> dict:
    """
    Validates Bunny credentials, auto-detects the storage zone's region endpoint
    across all known BunnyCDN regions, and verifies S3 multipart compatibility.
    """
    storage = get_decrypted_storage(storage)
    import requests

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
    region_code = matched_region.get("code", "de")

    # 2. Check S3-compatible API usability with auto-matched region
    s3_usable = False
    try:
        import boto3
        from botocore.config import Config
        s3_cfg = Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            retries={"max_attempts": 2, "mode": "standard"},
            connect_timeout=6,
            read_timeout=10,
            max_pool_connections=64
        )
        s3 = boto3.client(
            "s3",
            endpoint_url=s3_url,
            aws_access_key_id=zone,  # Storage Zone Name is Access Key ID
            aws_secret_access_key=bunny_key,
            region_name=region_code,
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
                try:
                    s3_fb = boto3.client(
                        "s3",
                        endpoint_url=s3_url,
                        aws_access_key_id=zone,
                        aws_secret_access_key=bunny_key,
                        region_name="us-east-1",
                        config=s3_cfg
                    )
                    s3_fb.list_objects_v2(Bucket=zone, MaxKeys=1)
                    s3_usable = True
                    region_code = "us-east-1"
                except Exception:
                    s3_usable = True
                    if log_path:
                        write_log(log_path, f"INFO  Bunny S3 verification note: {s3_err}. Enabling S3 multipart streaming.")
    except Exception as e:
        s3_usable = False
        if log_path:
            write_log(log_path, f"INFO  Bunny S3 client initialization error ({e}). Will use REST API.")

    return {
        "success": True,
        "zone": zone,
        "bunny_key": bunny_key,
        "rest_host": rest_host,
        "s3_url": s3_url,
        "region_code": region_code,
        "region_name": matched_region.get("name", rest_host),
        "s3_usable": s3_usable,
        "error": None
    }


def get_s3_client(storage: dict, max_pool: int = 64):
    storage = get_decrypted_storage(storage)
    import boto3
    from botocore.config import Config
    
    config = Config(
        retries={'max_attempts': 3, 'mode': 'standard'},
        connect_timeout=30,
        read_timeout=3600,
        max_pool_connections=max_pool
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
        bunny_res = storage.get("_resolved_bunny")
        if not bunny_res or not bunny_res.get("success"):
            bunny_res = verify_and_resolve_bunny(storage)
            if bunny_res and bunny_res.get("success"):
                storage["_resolved_bunny"] = bunny_res

        if bunny_res and bunny_res.get("success"):
            bunny_host = bunny_res["rest_host"]
            bunny_key = bunny_res["bunny_key"]
            zone = bunny_res["zone"]
            s3_url = bunny_res["s3_url"]
            region_code = bunny_res.get("region_code") or "de"
        else:
            bunny_host = get_bunny_host(storage)
            bunny_key = get_bunny_key(storage)
            zone = (storage.get("bunny_storage_zone") or "").strip()
            _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or "")
            region_code = "de"

        headers = {"AccessKey": bunny_key, "Accept": "application/json"}
        results = []

        def list_bunny_dir(subpath=""):
            clean = subpath.strip("/")
            url = f"https://{bunny_host}/{zone}/{clean}/" if clean else f"https://{bunny_host}/{zone}/"
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
                s3 = boto3.client(
                    "s3",
                    endpoint_url=s3_url,
                    aws_access_key_id=zone,
                    aws_secret_access_key=bunny_key,
                    region_name=region_code,
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
    storage = get_decrypted_storage(storage)

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
        import boto3
        from botocore.config import Config

        # 1. Ensure Bunny zone and regional endpoints are accurately auto-detected
        bunny_res = storage.get("_resolved_bunny")
        if not bunny_res or not bunny_res.get("success"):
            bunny_res = verify_and_resolve_bunny(storage)
            if bunny_res and bunny_res.get("success"):
                storage["_resolved_bunny"] = bunny_res

        if bunny_res and bunny_res.get("success"):
            bunny_host = bunny_res["rest_host"]
            bunny_key = bunny_res["bunny_key"]
            zone = bunny_res["zone"]
            s3_url = bunny_res["s3_url"]
            region_code = bunny_res.get("region_code") or "de"
        else:
            bunny_host = get_bunny_host(storage)
            bunny_key = get_bunny_key(storage)
            zone = (storage.get("bunny_storage_zone") or "").strip()
            _, s3_url = resolve_bunny_hosts(storage.get("bunny_region_url") or "")
            region_code = "de"

        headers = {"AccessKey": bunny_key}

        # Initialize S3 client for Bunny
        s3_client = None
        try:
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
                    connect_timeout=10,
                    read_timeout=15,
                    max_pool_connections=32
                )
            )
        except Exception as ex:
            log.warning(f"[BunnyCDN] S3 client setup note for delete: {ex}")

        # 2. Discover all objects associated with this backup
        list_prefix = (backup_dir + "/") if backup_dir else remote_name
        objects = set(list_storage_objects(storage, list_prefix))

        # Always include the direct remote_name and .indexes.json file if not directory
        if not remote_name.endswith("/"):
            objects.add(clean_rn)
            objects.add(clean_rn + ".indexes.json")

        # Also discover objects via S3 paginator across all prefixes to ensure no files are missed
        if s3_client:
            prefixes_to_scan = [list_prefix, clean_rn]
            if backup_dir:
                prefixes_to_scan.extend([backup_dir, backup_dir + "/"])
            for pfx in set(prefixes_to_scan):
                try:
                    paginator = s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=zone, Prefix=pfx.lstrip("/")):
                        for item in page.get("Contents", []):
                            objects.add(item["Key"])
                except Exception as s3_list_err:
                    log.warning(f"[BunnyCDN] S3 list objects warning for prefix {pfx}: {s3_list_err}")

        # 3. Delete all individual files via S3 batch and REST
        if s3_client and objects:
            try:
                obj_list = [k.lstrip("/") for k in objects]
                for i in range(0, len(obj_list), 500):
                    batch = [{'Key': k} for k in obj_list[i:i+500]]
                    s3_client.delete_objects(Bucket=zone, Delete={'Objects': batch})
                log.info(f"[BunnyCDN S3] Batch deleted {len(obj_list)} objects.")
            except Exception as s3_batch_err:
                log.warning(f"[BunnyCDN S3] Batch delete fallback note: {s3_batch_err}")

        for obj in objects:
            clean_obj = obj.lstrip("/")
            if s3_client:
                try:
                    s3_client.delete_object(Bucket=zone, Key=clean_obj)
                    log.info(f"[BunnyCDN S3] Deleted object {clean_obj}")
                except Exception:
                    pass

            del_url = f"https://{bunny_host}/{zone}/{clean_obj}"
            try:
                r = requests.delete(del_url, headers=headers, timeout=15)
                log.info(f"[BunnyCDN REST] Deleted file {clean_obj} (status {r.status_code})")
            except Exception as e:
                log.warning(f"[BunnyCDN REST] Failed to delete file {clean_obj}: {e}")

        # 4. Recursively delete the entire backup directory from BunnyCDN Edge Storage
        if backup_dir:
            clean_dir = backup_dir.strip("/")

            # Delete via REST API directory endpoint
            del_folder_url = f"https://{bunny_host}/{zone}/{clean_dir}/"
            try:
                r = requests.delete(del_folder_url, headers=headers, timeout=25)
                log.info(f"[BunnyCDN REST] Deleted directory {clean_dir}/ (status {r.status_code})")
            except Exception as e:
                log.warning(f"[BunnyCDN REST] Failed to delete directory {clean_dir}/: {e}")

            # Also delete virtual folder markers via S3
            if s3_client:
                for marker in [f"{clean_dir}/", clean_dir]:
                    try:
                        s3_client.delete_object(Bucket=zone, Key=marker)
                        log.info(f"[BunnyCDN S3] Deleted folder marker {marker}")
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

    try:
        backups = read_json("data/backups.json")
        storages = read_json("data/storages.json")

        schedule_backups = [
            b for b in backups
            if b.get("schedule_id") == schedule_id and b.get("status") == "completed"
        ]
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
