#!/usr/bin/env python3
"""
BackupVault Automation CLI
Enables triggering backups and restores from Terraform, CI/CD pipelines, Ansible, and interactive shell sessions.
Supports interactive selection, automatic storage/database resolution, and automatic new database creation.
Works with or without third-party libraries (built-in urllib fallback).
"""

import os
import sys
import time
import json
import argparse
import urllib.request
import urllib.error

try:
    import requests
except ImportError:
    requests = None

DEFAULT_URL = os.getenv("BACKUPVAULT_URL", "https://autobackup.dev.travelswitch.com")

class HTTPResponse:
    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text
        self.ok = 200 <= status_code < 300

    def json(self):
        try:
            return json.loads(self.text)
        except Exception:
            return {}

def http_get(url: str, headers: dict = None, timeout: int = 15) -> HTTPResponse:
    if requests:
        try:
            r = requests.get(url, headers=headers or {}, timeout=timeout)
            return HTTPResponse(r.status_code, r.text)
        except Exception as e:
            return HTTPResponse(0, str(e))
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return HTTPResponse(resp.status, resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return HTTPResponse(e.code, e.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        return HTTPResponse(0, str(e))

def http_post(url: str, json_data: dict = None, headers: dict = None, timeout: int = 15) -> HTTPResponse:
    hdrs = dict(headers or {})
    if requests:
        try:
            r = requests.post(url, json=json_data, headers=hdrs, timeout=timeout)
            return HTTPResponse(r.status_code, r.text)
        except Exception as e:
            return HTTPResponse(0, str(e))
    hdrs["Content-Type"] = "application/json"
    data_bytes = json.dumps(json_data).encode("utf-8") if json_data is not None else None
    req = urllib.request.Request(url, data=data_bytes, headers=hdrs, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return HTTPResponse(resp.status, resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return HTTPResponse(e.code, e.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        return HTTPResponse(0, str(e))

def get_headers(url: str, api_key: str = None, email: str = None, password: str = None):
    if api_key:
        return {"X-API-Key": api_key.strip()}
    elif email and password:
        login_res = http_post(f"{url.rstrip('/')}/api/auth/login", json_data={"email": email, "password": password})
        if not login_res.ok:
            print(f"[!] Authentication failed: {login_res.text}")
            sys.exit(1)
        d = login_res.json()
        token = d.get("token") or d.get("access_token")
        return {"Authorization": f"Bearer {token}"}
    return {}

def resolve_or_prompt_db(url: str, headers: dict, db_identifier: str = None):
    res = http_get(f"{url}/api/databases/", headers=headers)
    if not res.ok:
        print(f"[!] Failed to fetch databases ({res.status_code}): {res.text}")
        sys.exit(1)
    dbs = res.json()
    if not dbs:
        print("[!] No database connection profiles found. Please add a database first in BackupVault.")
        sys.exit(1)

    if db_identifier:
        ident_lower = db_identifier.strip().lower()
        matched = next((d for d in dbs if d["id"] == db_identifier), None)
        if not matched:
            matched = next((d for d in dbs if d.get("name", "").lower() == ident_lower), None)
        if not matched:
            matched = next((d for d in dbs if (d.get("database_name") or "").lower() == ident_lower), None)
        if not matched:
            matched = next((d for d in dbs if d.get("type", "").lower() == ident_lower), None)
        if matched:
            return matched

    if len(dbs) == 1:
        return dbs[0]

    # Interactive prompt if terminal is attached
    if sys.stdin.isatty():
        print("\nAvailable Database Connections:")
        for idx, d in enumerate(dbs, 1):
            db_spec = d.get("database_name") or "all"
            print(f"  [{idx}] {d.get('name')} (Engine: {d.get('type')}, DB: {db_spec}) - ID: {d['id']}")
        while True:
            choice = input(f"\nSelect database [1-{len(dbs)}] (Default: 1): ").strip()
            if not choice:
                return dbs[0]
            if choice.isdigit() and 1 <= int(choice) <= len(dbs):
                return dbs[int(choice) - 1]
            print("[!] Invalid selection. Please try again.")

    return dbs[0]

def resolve_or_prompt_storage(url: str, headers: dict, storage_identifier: str = None):
    res = http_get(f"{url}/api/storage/", headers=headers)
    if not res.ok:
        print(f"[!] Failed to fetch storage destinations ({res.status_code}): {res.text}")
        sys.exit(1)
    storages = res.json()
    if not storages:
        print("[!] No storage destinations found. Please configure a storage provider first in BackupVault.")
        sys.exit(1)

    if storage_identifier:
        ident_lower = storage_identifier.strip().lower()
        matched = next((s for s in storages if s["id"] == storage_identifier), None)
        if not matched:
            matched = next((s for s in storages if s.get("name", "").lower() == ident_lower), None)
        if not matched:
            matched = next((s for s in storages if s.get("type", "").lower() == ident_lower), None)
        if matched:
            return matched

    if len(storages) == 1:
        return storages[0]

    if sys.stdin.isatty():
        print("\nAvailable Storage Destinations:")
        for idx, s in enumerate(storages, 1):
            target = s.get("azure_container") or s.get("s3_bucket") or s.get("bunny_storage_zone") or s.get("gcs_bucket") or "local"
            print(f"  [{idx}] {s.get('name')} (Provider: {s.get('type').upper()}, Target: {target}) - ID: {s['id']}")
        while True:
            choice = input(f"\nSelect storage destination [1-{len(storages)}] (Default: 1): ").strip()
            if not choice:
                return storages[0]
            if choice.isdigit() and 1 <= int(choice) <= len(storages):
                return storages[int(choice) - 1]
            print("[!] Invalid selection. Please try again.")

    return storages[0]

def cmd_backup(args):
    url = args.url.rstrip("/")
    headers = get_headers(url, args.api_key, args.email, args.password)

    db = resolve_or_prompt_db(url, headers, args.db_id or args.db)
    storage = resolve_or_prompt_storage(url, headers, args.storage_id or args.storage)

    target_dbname = args.db_name or db.get("database_name")
    print("\n" + "=" * 60)
    print("                TRIGGERING DATABASE BACKUP")
    print("=" * 60)
    print(f"Database   : {db.get('name')} ({db.get('type')}) - DB: {target_dbname or '__all__'}")
    print(f"Destination: {storage.get('name')} ({storage.get('type').upper()})")
    print(f"Profile    : {args.speed_profile.upper()} | Method: {args.method.upper()}")
    print("=" * 60)

    payload = {
        "database_id": db["id"],
        "storage_id": storage["id"],
        "backup_method": args.method,
        "speed_profile": args.speed_profile,
        "indexing_mode": args.indexing_mode,
        "label": args.label or f"backup-{db.get('name', 'db')}-{int(time.time())}",
        "archive_mode": args.archive_mode
    }
    if args.db_name:
        payload["database_name"] = args.db_name

    res = http_post(f"{url}/api/backups/", json_data=payload, headers=headers)
    if not res.ok:
        print(f"\n[!] Backup trigger failed ({res.status_code}): {res.text}")
        sys.exit(1)

    backup = res.json()
    backup_id = backup["id"]
    print(f"[+] Backup job started successfully! ID: {backup_id}")

    if not args.wait:
        print(f"[*] Exiting. Monitor with: curl {url}/api/backups/{backup_id}/logs")
        return

    print(f"[*] Polling progress (timeout: {args.timeout}s)...")
    start = time.time()
    while time.time() - start < args.timeout:
        poll_res = http_get(f"{url}/api/backups/", headers=headers)
        if poll_res.ok:
            for b in poll_res.json():
                if b["id"] == backup_id:
                    status = b.get("status")
                    pct = b.get("progress_percentage", 0)
                    speed = b.get("speed_mbps", 0)
                    proc = b.get("processed_mb", 0)
                    print(f"[{time.strftime('%H:%M:%S')}] Status: {status.upper()} | Progress: {pct}% | Processed: {proc} MB | Speed: {speed} MB/s")
                    if status == "completed":
                        duration = int(time.time() - start)
                        print("\n" + "=" * 60)
                        print(f"[SUCCESS] Backup finished in {duration}s!")
                        print(f"Storage File: {b.get('remote_name') or b.get('remote_path')}")
                        print("=" * 60)
                        sys.exit(0)
                    elif status == "failed":
                        print(f"\n[FAILED] Backup failed: {b.get('error')}")
                        sys.exit(1)
                    break
        time.sleep(args.interval)

    print(f"[TIMEOUT] Backup did not complete within {args.timeout}s.")
    sys.exit(2)

def cmd_restore(args):
    url = args.url.rstrip("/")
    headers = get_headers(url, args.api_key, args.email, args.password)

    backup_id = args.backup_id or args.backup
    if not backup_id:
        poll_res = http_get(f"{url}/api/backups/", headers=headers)
        if poll_res.ok:
            completed_backups = [b for b in poll_res.json() if b.get("status") == "completed"]
            if not completed_backups:
                print("[!] No completed backups found to restore.")
                sys.exit(1)
            if sys.stdin.isatty():
                print("\nRecent Completed Backups:")
                for idx, b in enumerate(completed_backups[:10], 1):
                    created = b.get("created_at", "")[:19].replace("T", " ")
                    print(f"  [{idx}] {b.get('label')} ({b.get('database_name') or b.get('connection_name')}) - {b.get('size_mb', 0)} MB [{created}] - ID: {b['id']}")
                choice = input(f"\nSelect backup to restore [1-{min(10, len(completed_backups))}] (Default: 1): ").strip()
                selected_idx = int(choice) - 1 if (choice.isdigit() and 1 <= int(choice) <= len(completed_backups)) else 0
                backup_id = completed_backups[selected_idx]["id"]
            else:
                backup_id = completed_backups[0]["id"]
        else:
            print("[!] Could not fetch backups list.")
            sys.exit(1)

    target_db = resolve_or_prompt_db(url, headers, args.target_db_id or args.target_db)
    target_name = args.target_name or args.db_name

    if not target_name:
        if sys.stdin.isatty():
            target_name = input(f"Enter target database name [{target_db.get('database_name') or 'restored_db'}]: ").strip() or (target_db.get("database_name") or "restored_db")
        else:
            target_name = target_db.get("database_name") or "restored_db"

    print("\n" + "=" * 60)
    print("                TRIGGERING DATABASE RESTORE")
    print("=" * 60)
    print(f"Backup ID   : {backup_id}")
    print(f"Target Host : {target_db.get('name')} ({target_db.get('type')})")
    print(f"Target DB   : {target_name} (Auto-creates if missing on server)")
    print(f"Profile     : {args.speed_profile.upper()}")
    print("=" * 60)

    payload = {
        "backup_id": backup_id,
        "target_database_id": target_db["id"],
        "target_database_name": target_name,
        "new_database": True,
        "indexing_mode": args.indexing_mode,
        "changes_only": args.changes_only,
        "speed_profile": args.speed_profile
    }

    res = http_post(f"{url}/api/backups/restore", json_data=payload, headers=headers)
    if not res.ok:
        print(f"\n[!] Restore trigger failed ({res.status_code}): {res.text}")
        sys.exit(1)

    restore = res.json()
    restore_id = restore["id"]
    print(f"[+] Restore job started successfully! ID: {restore_id}")

    if not args.wait:
        print(f"[*] Exiting. Monitor with: curl {url}/api/backups/restores/{restore_id}/logs")
        return

    print(f"[*] Polling progress (timeout: {args.timeout}s)...")
    start = time.time()
    while time.time() - start < args.timeout:
        poll_res = http_get(f"{url}/api/backups/restores", headers=headers)
        if poll_res.ok:
            for r in poll_res.json():
                if r["id"] == restore_id:
                    status = r.get("status")
                    pct = r.get("progress_percentage", 0)
                    speed = r.get("speed_mbps", 0)
                    proc = r.get("processed_mb", 0)
                    print(f"[{time.strftime('%H:%M:%S')}] Status: {status.upper()} | Progress: {pct}% | Processed: {proc} MB | Speed: {speed} MB/s")
                    if status == "completed":
                        duration = int(time.time() - start)
                        print("\n" + "=" * 60)
                        print(f"[SUCCESS] Restore into database '{target_name}' completed in {duration}s!")
                        print("=" * 60)
                        sys.exit(0)
                    elif status == "failed":
                        print(f"\n[FAILED] Restore failed: {r.get('error')}")
                        sys.exit(1)
                    break
        time.sleep(args.interval)

    print(f"[TIMEOUT] Restore did not complete within {args.timeout}s.")
    sys.exit(2)

def cmd_list_dbs(args):
    url = args.url.rstrip("/")
    headers = get_headers(url, args.api_key, args.email, args.password)
    res = http_get(f"{url}/api/databases/", headers=headers)
    if not res.ok:
        print(f"[!] Error: {res.text}")
        sys.exit(1)
    dbs = res.json()
    print(f"\nConfigured Databases ({len(dbs)}):")
    for d in dbs:
        print(f"  - [{d.get('type')}] {d.get('name')}: database='{d.get('database_name')}' | ID: {d['id']}")

def cmd_list_storages(args):
    url = args.url.rstrip("/")
    headers = get_headers(url, args.api_key, args.email, args.password)
    res = http_get(f"{url}/api/storage/", headers=headers)
    if not res.ok:
        print(f"[!] Error: {res.text}")
        sys.exit(1)
    storages = res.json()
    print(f"\nConfigured Storage Destinations ({len(storages)}):")
    for s in storages:
        target = s.get("azure_container") or s.get("s3_bucket") or s.get("bunny_storage_zone") or s.get("gcs_bucket") or "local"
        print(f"  - [{s.get('type').upper()}] {s.get('name')}: target='{target}' | ID: {s['id']}")

def cmd_list_backups(args):
    url = args.url.rstrip("/")
    headers = get_headers(url, args.api_key, args.email, args.password)
    res = http_get(f"{url}/api/backups/", headers=headers)
    if not res.ok:
        print(f"[!] Error: {res.text}")
        sys.exit(1)
    backups = res.json()
    print(f"\nBackup History ({len(backups)}):")
    for b in backups[:15]:
        created = b.get("created_at", "")[:19].replace("T", " ")
        print(f"  - [{b.get('status').upper()}] {b.get('label') or b.get('database_name')} | Size: {b.get('size_mb', 0)} MB | {created} | ID: {b['id']}")

def main():
    parser = argparse.ArgumentParser(description="BackupVault Automation CLI for Terraform, CI/CD & Shell")
    parser.add_argument("--url", default=DEFAULT_URL, help="BackupVault API base URL")
    parser.add_argument("--api-key", default=os.getenv("BACKUPVAULT_API_KEY"), help="API Key (X-API-Key: bv_live_...)")
    parser.add_argument("--email", default=os.getenv("BACKUPVAULT_EMAIL"), help="User email")
    parser.add_argument("--password", default=os.getenv("BACKUPVAULT_PASSWORD"), help="User password")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: backup
    p_b = subparsers.add_parser("backup", help="Trigger a database backup")
    p_b.add_argument("--db-id", "--db", default=None, help="Database connection ID or name (optional, prompts if omitted)")
    p_b.add_argument("--storage-id", "--storage", default=None, help="Storage destination ID, name, or type (optional, prompts if omitted)")
    p_b.add_argument("--db-name", default=None, help="Specific database name to dump (optional)")
    p_b.add_argument("--method", default="full", choices=["full", "incremental"], help="Backup method")
    p_b.add_argument("--indexing-mode", default="with_index", choices=["with_index", "without_index", "only_index"])
    p_b.add_argument("--speed-profile", default="extreme", choices=["default", "balanced", "extreme", "safe"])
    p_b.add_argument("--archive-mode", default="single", choices=["single", "multiple"])
    p_b.add_argument("--label", default=None, help="Custom label for backup")
    p_b.add_argument("--wait", action="store_true", help="Wait for backup to complete before exiting")
    p_b.add_argument("--timeout", type=int, default=1800, help="Wait timeout in seconds")
    p_b.add_argument("--interval", type=int, default=3, help="Polling interval in seconds")

    # Subcommand: restore
    p_r = subparsers.add_parser("restore", help="Trigger a database restore")
    p_r.add_argument("--backup-id", "--backup", default=None, help="Backup ID to restore (optional, prompts if omitted)")
    p_r.add_argument("--target-db-id", "--target-db", default=None, help="Target database connection ID or name (optional, prompts if omitted)")
    p_r.add_argument("--target-name", "--db-name", default=None, help="Target database name (auto-created on server if missing)")
    p_r.add_argument("--indexing-mode", default="with_index", choices=["with_index", "without_index", "only_index"])
    p_r.add_argument("--changes-only", action="store_true", help="Enable Smart Sync / Diff Mode")
    p_r.add_argument("--speed-profile", default="extreme", choices=["default", "balanced", "extreme", "safe"])
    p_r.add_argument("--wait", action="store_true", help="Wait for restore to complete before exiting")
    p_r.add_argument("--timeout", type=int, default=3600, help="Wait timeout in seconds")
    p_r.add_argument("--interval", type=int, default=3, help="Polling interval in seconds")

    # Listing helpers
    subparsers.add_parser("list-dbs", help="List all configured databases")
    subparsers.add_parser("list-storages", help="List all configured storage destinations")
    subparsers.add_parser("list-backups", help="List recent backup history")

    args = parser.parse_args()
    if args.command == "backup":
        cmd_backup(args)
    elif args.command == "restore":
        cmd_restore(args)
    elif args.command == "list-dbs":
        cmd_list_dbs(args)
    elif args.command == "list-storages":
        cmd_list_storages(args)
    elif args.command == "list-backups":
        cmd_list_backups(args)

if __name__ == "__main__":
    main()
