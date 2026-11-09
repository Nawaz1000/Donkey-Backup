#!/usr/bin/env python3
"""
BackupVault Automation CLI
Enables triggering backups and restores from Terraform, CI/CD pipelines, Ansible, and Python scripts.
Supports both persistent API Key (X-API-Key) and JWT credentials.
"""

import os
import sys
import time
import argparse
import requests

def get_headers(url: str, api_key: str = None, email: str = None, password: str = None):
    if api_key:
        return {"X-API-Key": api_key.strip()}
    elif email and password:
        login_res = requests.post(f"{url.rstrip('/')}/api/auth/login", json={"email": email, "password": password})
        login_res.raise_for_status()
        token = login_res.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}
    else:
        raise ValueError("Must provide either --api-key or both --email and --password")

def cmd_backup(args):
    url = args.url.rstrip("/")
    headers = get_headers(url, args.api_key, args.email, args.password)

    payload = {
        "database_id": args.db_id,
        "storage_id": args.storage_id,
        "backup_method": args.method,
        "speed_profile": args.speed_profile,
        "indexing_mode": args.indexing_mode,
        "label": args.label or f"automated-backup-{int(time.time())}",
        "archive_mode": args.archive_mode
    }
    if args.db_name:
        payload["database_name"] = args.db_name

    print(f"[*] Triggering backup on {url} for database_id: {args.db_id}...")
    res = requests.post(f"{url}/api/backups/", json=payload, headers=headers)
    if not res.ok:
        print(f"[!] Backup trigger failed ({res.status_code}): {res.text}")
        sys.exit(1)

    backup = res.json()
    backup_id = backup["id"]
    print(f"[+] Backup job started successfully! ID: {backup_id}")

    if not args.wait:
        print(f"[*] --wait not specified. Exiting with backup_id: {backup_id}")
        return

    print(f"[*] Polling progress (timeout: {args.timeout}s)...")
    start = time.time()
    while time.time() - start < args.timeout:
        poll_res = requests.get(f"{url}/api/backups/", headers=headers)
        if poll_res.ok:
            for b in poll_res.json():
                if b["id"] == backup_id:
                    status = b.get("status")
                    pct = b.get("progress_percentage", 0)
                    speed = b.get("speed_mbps", 0)
                    print(f"[{time.strftime('%H:%M:%S')}] Status: {status.upper()} | Progress: {pct}% | Speed: {speed} MB/s")
                    if status == "completed":
                        duration = int(time.time() - start)
                        print(f"[SUCCESS] Backup finished in {duration}s! Remote: {b.get('remote_name')}")
                        sys.exit(0)
                    elif status == "failed":
                        print(f"[FAILED] Backup failed: {b.get('error')}")
                        sys.exit(1)
                    break
        time.sleep(args.interval)

    print(f"[TIMEOUT] Backup did not complete within {args.timeout}s.")
    sys.exit(2)

def cmd_restore(args):
    url = args.url.rstrip("/")
    headers = get_headers(url, args.api_key, args.email, args.password)

    payload = {
        "backup_id": args.backup_id,
        "target_database_id": args.target_db_id,
        "target_database_name": args.target_name,
        "indexing_mode": args.indexing_mode,
        "changes_only": args.changes_only,
        "speed_profile": args.speed_profile
    }

    print(f"[*] Triggering restore of backup {args.backup_id} into target database: {args.target_name}...")
    res = requests.post(f"{url}/api/backups/restore", json=payload, headers=headers)
    if not res.ok:
        print(f"[!] Restore trigger failed ({res.status_code}): {res.text}")
        sys.exit(1)

    restore = res.json()
    restore_id = restore["id"]
    print(f"[+] Restore job started successfully! ID: {restore_id}")

    if not args.wait:
        print(f"[*] --wait not specified. Exiting with restore_id: {restore_id}")
        return

    print(f"[*] Polling progress (timeout: {args.timeout}s)...")
    start = time.time()
    while time.time() - start < args.timeout:
        poll_res = requests.get(f"{url}/api/backups/restores", headers=headers)
        if poll_res.ok:
            for r in poll_res.json():
                if r["id"] == restore_id:
                    status = r.get("status")
                    pct = r.get("progress_percentage", 0)
                    speed = r.get("speed_mbps", 0)
                    print(f"[{time.strftime('%H:%M:%S')}] Status: {status.upper()} | Progress: {pct}% | Speed: {speed} MB/s")
                    if status == "completed":
                        duration = int(time.time() - start)
                        print(f"[SUCCESS] Restore finished in {duration}s!")
                        sys.exit(0)
                    elif status == "failed":
                        print(f"[FAILED] Restore failed: {r.get('error')}")
                        sys.exit(1)
                    break
        time.sleep(args.interval)

    print(f"[TIMEOUT] Restore did not complete within {args.timeout}s.")
    sys.exit(2)

def main():
    parser = argparse.ArgumentParser(description="BackupVault Automation CLI for Terraform & CI/CD")
    parser.add_argument("--url", default=os.getenv("BACKUPVAULT_URL", "http://localhost:8000"), help="BackupVault API base URL")
    parser.add_argument("--api-key", default=os.getenv("BACKUPVAULT_API_KEY"), help="API Key (X-API-Key: bv_live_...)")
    parser.add_argument("--email", default=os.getenv("BACKUPVAULT_EMAIL"), help="User email")
    parser.add_argument("--password", default=os.getenv("BACKUPVAULT_PASSWORD"), help="User password")
    
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: backup
    p_b = subparsers.add_parser("backup", help="Trigger a database backup")
    p_b.add_argument("--db-id", required=True, help="Database connection ID")
    p_b.add_argument("--storage-id", required=True, help="Storage destination ID")
    p_b.add_argument("--db-name", default=None, help="Specific database name (optional)")
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
    p_r.add_argument("--backup-id", required=True, help="Backup ID to restore")
    p_r.add_argument("--target-db-id", required=True, help="Target database connection ID")
    p_r.add_argument("--target-name", required=True, help="Target database name")
    p_r.add_argument("--indexing-mode", default="with_index", choices=["with_index", "without_index", "only_index"])
    p_r.add_argument("--changes-only", action="store_true", help="Enable Smart Sync / Diff Mode")
    p_r.add_argument("--speed-profile", default="extreme", choices=["default", "balanced", "extreme", "safe"])
    p_r.add_argument("--wait", action="store_true", help="Wait for restore to complete before exiting")
    p_r.add_argument("--timeout", type=int, default=3600, help="Wait timeout in seconds")
    p_r.add_argument("--interval", type=int, default=3, help="Polling interval in seconds")

    args = parser.parse_args()
    if args.command == "backup":
        cmd_backup(args)
    elif args.command == "restore":
        cmd_restore(args)

if __name__ == "__main__":
    main()

