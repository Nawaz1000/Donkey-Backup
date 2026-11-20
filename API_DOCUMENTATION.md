# BackupVault API Documentation (Open Direct URL Access)

Complete guide for triggering automated backups and restores directly via URL on `https://autobackup.dev.travelswitch.com`.

---

## 1. Direct URL Access (No API Key or Token Required)

The API is fully open for direct automation. You **do NOT need any headers, tokens, or API keys** to access your databases, storages, backups, or triggers.

- **Base URL:** `https://autobackup.dev.travelswitch.com`
- **Authentication:** None required (simply make standard HTTP GET/POST calls).

```bash
# Example test - directly query your databases with zero headers:
curl -s https://autobackup.dev.travelswitch.com/api/databases/
```

---

## 2. Step 1: List Databases & Storage Destinations

### List Databases:
```bash
curl -s https://autobackup.dev.travelswitch.com/api/databases/
```

**Output:**
```json
[
  {
    "id": "e9e4dee6-8a0e-45c0-8267-83edafc73368",
    "name": "MongoDB-Production",
    "type": "mongodb",
    "database_name": "HotelStaticData"
  },
  {
    "id": "8a7c2b1d-9e0f-4a3b-2c1d-0e9f8a7b6c5d",
    "name": "PostgreSQL-Production",
    "type": "postgresql",
    "database_name": "postgres"
  }
]
```
> You can refer to any database by its **`id`** (UUID) or by its **`name`** (e.g. `"PostgreSQL-Production"`).

### List Storage Destinations:
```bash
curl -s https://autobackup.dev.travelswitch.com/api/storage/
```

**Output:**
```json
[
  {
    "id": "7b8c9d0e-1f2a-3b4c-5d6e-7f8a9b0c1d2e",
    "name": "Azure-Blob-Storage",
    "type": "azure",
    "azure_container": "backups"
  },
  {
    "id": "3a2f1e0d-5b4c-4e3f-1a2b-9c8d7e6f5a4b",
    "name": "AWS-S3-Backups",
    "type": "s3",
    "s3_bucket": "my-database-backups"
  }
]
```
> You can refer to storage by its **`id`**, by its **`name`**, by its **`type`** (`"azure"`, `"s3"`, `"bunny"`, `"gcs"`), or **omit it entirely** to automatically use your default storage destination.

---

## 3. Step 2: Trigger a Backup via API (cURL)

Trigger a backup by sending a `POST` request to `/api/backups/`.

### Flexible Options:
- **`database_id`**: Database UUID or database connection name (e.g. `"PostgreSQL-Production"` or `"e9e4dee6..."`).
- **`database_name`** *(Optional)*: Specific database name to back up (e.g. `"HotelStaticData"`).
- **`storage_id`** *(Optional)*: Target storage destination UUID, name (e.g. `"AWS-S3-Backups"`), or provider type (`"s3"`, `"azure"`, `"bunny"`). If omitted, automatically uses your default storage.
- **`backup_method`**: `"full"` or `"incremental"`.
- **`speed_profile`**: `"extreme"`, `"balanced"`, or `"safe"`.

```bash
# Minimal trigger using friendly names:
curl -s -X POST "https://autobackup.dev.travelswitch.com/api/backups/" \
  -H "Content-Type: application/json" \
  -d '{
    "database_id": "PostgreSQL-Production",
    "database_name": "HotelStaticData",
    "storage_id": "s3",
    "speed_profile": "extreme"
  }'
```

### Response (`200 OK`):
```json
{
  "id": "b74900a2-3f12-4d2c-8a19-b2f9104ac715",
  "database_id": "8a7c2b1d-9e0f-4a3b-2c1d-0e9f8a7b6c5d",
  "storage_id": "3a2f1e0d-5b4c-4e3f-1a2b-9c8d7e6f5a4b",
  "status": "running",
  "created_at": "2026-11-20T12:00:00Z"
}
```
The returned `"id"` is your `backup_id`.

---

## 4. Step 3: Check / Monitor Live Backup Status

```bash
curl -s https://autobackup.dev.travelswitch.com/api/backups/
```

Find the entry matching your `backup_id`:
```json
{
  "id": "b74900a2-3f12-4d2c-8a19-b2f9104ac715",
  "status": "running",
  "progress_percentage": 78,
  "speed_mbps": 154.2,
  "processed_mb": 425.8,
  "remote_name": "backup-Postgres-20261120-120000.archive.zst"
}
```

To view live terminal execution logs of the backup:
```bash
curl -s https://autobackup.dev.travelswitch.com/api/backups/YOUR_BACKUP_ID/logs
```

---

## 5. Step 4: Trigger a Restore (With Automatic Database Creation)

To restore a backup into a database, send a `POST` request to `/api/backups/restore`.

### Automatic Database Creation:
If the database specified in **`target_database_name`** does not exist on your PostgreSQL or MongoDB server, **BackupVault will automatically create it** (`CREATE DATABASE "..."`) and register the new database profile in BackupVault.

```bash
curl -s -X POST "https://autobackup.dev.travelswitch.com/api/backups/restore" \
  -H "Content-Type: application/json" \
  -d '{
    "backup_id": "YOUR_BACKUP_ID",
    "target_database_id": "PostgreSQL-Production",
    "target_database_name": "my_new_restored_db",
    "speed_profile": "extreme",
    "indexing_mode": "with_index"
  }'
```

### Parameters:
| Field | Type | Description |
|---|---|---|
| `backup_id` | string *(Required)* | ID of the completed backup to restore. |
| `target_database_id` | string *(Optional)* | Target database connection ID, name, or type (`"postgres"`, `"mongodb"`). If omitted, defaults to the source connection profile of the backup. |
| `target_database_name` | string *(Optional)* | Target database name. **If it doesn't exist on the server, BackupVault automatically creates it.** |
| `speed_profile` | string *(Optional)* | `"extreme"`, `"balanced"`, or `"safe"`. |
| `indexing_mode` | string *(Optional)* | `"with_index"`, `"without_index"`, or `"only_index"`. |
| `changes_only` | boolean *(Optional)* | If true, applies diffs while preserving existing schemas and tables. |

To check live restore progress and terminal logs:
```bash
curl -s https://autobackup.dev.travelswitch.com/api/backups/restores/YOUR_RESTORE_ID/logs
```

---

## 6. One-Line Command via Python CLI Script

Use [`scripts/backup_cli.py`](file:///c:/Users/nawaz/OneDrive/Desktop/IH-Projects/Auto-Backup/scripts/backup_cli.py) directly from shell, CI/CD, or Ansible.

### Quick Listing Helpers:
```bash
python scripts/backup_cli.py list-dbs
python scripts/backup_cli.py list-storages
python scripts/backup_cli.py list-backups
```

### Interactive Backup (Prompts for Destination & Database):
```bash
python scripts/backup_cli.py backup
```
The CLI will interactively display all available databases and storage destinations (Azure, AWS S3, BunnyCDN, GCS) for you to select!

### Direct Backup with Waiting:
```bash
python scripts/backup_cli.py backup \
  --db "PostgreSQL-Production" \
  --storage "s3" \
  --db-name "HotelStaticData" \
  --speed-profile extreme \
  --wait
```

### Direct Restore (With Auto-Creation of New Database):
```bash
python scripts/backup_cli.py restore \
  --backup-id "YOUR_BACKUP_ID" \
  --target-db "PostgreSQL-Production" \
  --target-name "my_new_restored_db" \
  --speed-profile extreme \
  --wait
```
> If `my_new_restored_db` does not exist on your PostgreSQL server, BackupVault automatically creates the database and restores all tables, data, and indexes into it.

---

## 7. Python Script Integration Example

```python
import time
import requests

API_URL = "https://autobackup.dev.travelswitch.com"

def run_backup_and_restore(db_name: str, target_db_name: str, storage_type: str = "s3"):
    # 1. Trigger backup (no auth headers required!)
    payload = {
        "database_id": "PostgreSQL-Production",
        "database_name": db_name,
        "storage_id": storage_type,
        "speed_profile": "extreme"
    }
    res = requests.post(f"{API_URL}/api/backups/", json=payload)
    res.raise_for_status()
    backup_id = res.json()["id"]
    print(f"[*] Backup started! ID: {backup_id}")

    # 2. Wait for backup completion
    while True:
        poll = requests.get(f"{API_URL}/api/backups/").json()
        job = next((b for b in poll if b["id"] == backup_id), None)
        if job:
            status = job.get("status")
            pct = job.get("progress_percentage", 0)
            spd = job.get("speed_mbps", 0)
            print(f" -> Backup: {status.upper()} | {pct}% | {spd} MB/s")
            if status == "completed":
                print(f"[SUCCESS] Backup finished! Remote: {job.get('remote_name')}")
                break
            elif status == "failed":
                raise RuntimeError(f"Backup failed: {job.get('error')}")
        time.sleep(3)

    # 3. Trigger restore into a new database (auto-created if missing)
    restore_payload = {
        "backup_id": backup_id,
        "target_database_id": "PostgreSQL-Production",
        "target_database_name": target_db_name,
        "speed_profile": "extreme"
    }
    r_res = requests.post(f"{API_URL}/api/backups/restore", json=restore_payload)
    r_res.raise_for_status()
    restore_id = r_res.json()["id"]
    print(f"[*] Restore started into '{target_db_name}'! ID: {restore_id}")

    # 4. Wait for restore completion
    while True:
        poll_r = requests.get(f"{API_URL}/api/backups/restores").json()
        r_job = next((r for r in poll_r if r["id"] == restore_id), None)
        if r_job:
            status = r_job.get("status")
            pct = r_job.get("progress_percentage", 0)
            print(f" -> Restore: {status.upper()} | {pct}%")
            if status == "completed":
                print(f"[SUCCESS] Database '{target_db_name}' restored successfully!")
                return
            elif status == "failed":
                raise RuntimeError(f"Restore failed: {r_job.get('error')}")
        time.sleep(3)

if __name__ == "__main__":
    run_backup_and_restore("HotelStaticData", "HotelStaticData_Restored", storage_type="s3")
```
