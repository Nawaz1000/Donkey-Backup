# BackupVault API Documentation

Complete guide for triggering automated backups and restores via API using your persistent API Key on `https://autobackup.dev.travelswitch.com`.

---

## 1. Authentication & Base URL

- **Base URL:** `https://autobackup.dev.travelswitch.com`
- **Header:** `X-API-Key: bv_live_YOUR_KEY`

All API endpoints accept the persistent key via the `X-API-Key` HTTP header.

```bash
# Example curl test
curl -H "X-API-Key: bv_live_YOUR_KEY" https://autobackup.dev.travelswitch.com/api/databases/
```

---

## 2. Step 1: Retrieve Database and Storage IDs

Before triggering a backup, you need the unique ID of the database you want to back up and the ID of the storage destination where the archive will be saved.

### List Databases
```bash
curl -s -X GET "https://autobackup.dev.travelswitch.com/api/databases/" \
  -H "X-API-Key: bv_live_YOUR_KEY"
```

**Sample Output:**
```json
[
  {
    "id": "a1b2c3d4-db01",
    "name": "Production MongoDB",
    "type": "mongodb",
    "database_name": "HotelStaticData"
  }
]
```
> Note the `"id"` value (e.g. `a1b2c3d4-db01`).

### List Storage Destinations
```bash
curl -s -X GET "https://autobackup.dev.travelswitch.com/api/storage/" \
  -H "X-API-Key: bv_live_YOUR_KEY"
```

**Sample Output:**
```json
[
  {
    "id": "e5f6g7h8-st01",
    "name": "Azure Blob Storage",
    "type": "azure",
    "azure_container": "backups"
  }
]
```
> Note the `"id"` value (e.g. `e5f6g7h8-st01`).

---

## 3. Step 2: Trigger a Database Backup

### API Endpoint
- **Method:** `POST`
- **Path:** `/api/backups/`
- **Headers:**
  - `X-API-Key: bv_live_YOUR_KEY`
  - `Content-Type: application/json`

### cURL Example
```bash
curl -s -X POST "https://autobackup.dev.travelswitch.com/api/backups/" \
  -H "X-API-Key: bv_live_YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "database_id": "YOUR_DATABASE_ID",
    "storage_id": "YOUR_STORAGE_ID",
    "backup_method": "full",
    "speed_profile": "extreme",
    "indexing_mode": "with_index",
    "label": "daily-prod-backup"
  }'
```

### Response (`200 OK`)
```json
{
  "id": "9c8b7a6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d",
  "database_id": "YOUR_DATABASE_ID",
  "storage_id": "YOUR_STORAGE_ID",
  "status": "pending",
  "created_at": "2026-11-18T12:00:00Z"
}
```
Save the returned `"id"` (this is the `backup_id`).

### Request Parameters Reference

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `database_id` | string | **Yes** | — | ID of the target database connection |
| `storage_id` | string | **Yes** | — | ID of the storage destination (Azure, S3, Bunny, GCS) |
| `database_name` | string | No | `null` | Target a specific database if cluster connection |
| `collection` | string | No | `""` | Single table/collection name (`""` or `"full"` for entire DB) |
| `backup_method` | string | No | `"full"` | `"full"`, `"incremental"`, or `"differential"` |
| `speed_profile` | string | No | `"extreme"` | `"extreme"` (fastest, maximum concurrency), `"balanced"`, `"default"` |
| `indexing_mode` | string | No | `"with_index"` | `"with_index"` (data + separate index metadata), `"without_index"`, `"only_index"` |
| `label` | string | No | `""` | Human-readable tag or CI/CD job reference |
| `archive_mode` | string | No | `"single"` | `"single"` (unified archive) or `"multiple"` (parallel cluster format) |

---

## 4. Step 3: Check / Monitor Backup Status

To check the progress, throughput speed, and completion status of your backup:

### Poll All Backups
```bash
curl -s -X GET "https://autobackup.dev.travelswitch.com/api/backups/" \
  -H "X-API-Key: bv_live_YOUR_KEY"
```

Find the entry matching your `backup_id`:

```json
{
  "id": "9c8b7a6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d",
  "status": "in_progress",
  "progress_percentage": 68,
  "speed_mbps": 142.5,
  "size_mb": 420.15,
  "remote_name": "ts-prod-mongodb-2026-11-18_12-00-00/backup.archive.zst"
}
```

### Possible Status Values
- `pending`: Job queued, preparing streaming pipeline.
- `in_progress`: Actively streaming compressed data to cloud storage.
- `completed`: Successfully uploaded and validated.
- `failed`: An error occurred (check `"error"` field for details).

### View Live Logs
```bash
curl -s -X GET "https://autobackup.dev.travelswitch.com/api/backups/YOUR_BACKUP_ID/logs" \
  -H "X-API-Key: bv_live_YOUR_KEY"
```

---

## 5. Step 4: Trigger a Restore via API

To restore an existing backup into a target database:

### API Endpoint
- **Method:** `POST`
- **Path:** `/api/backups/restore`
- **Headers:**
  - `X-API-Key: bv_live_YOUR_KEY`
  - `Content-Type: application/json`

### cURL Example
```bash
curl -s -X POST "https://autobackup.dev.travelswitch.com/api/backups/restore" \
  -H "X-API-Key: bv_live_YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "backup_id": "YOUR_BACKUP_ID",
    "target_database_id": "TARGET_DATABASE_ID",
    "target_database_name": "HotelStaticData_Restored",
    "speed_profile": "extreme",
    "indexing_mode": "with_index"
  }'
```

### Restore Request Parameters Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `backup_id` | string | **Yes** | ID of the backup record to restore |
| `target_database_id` | string | **Yes** | ID of destination database connection |
| `target_database_name` | string | No | Restore into a custom database name |
| `selected_database` | string | No | Source database to extract if restoring from a cluster archive |
| `speed_profile` | string | No | `"extreme"` (default), `"balanced"`, `"safe"` |
| `indexing_mode` | string | No | `"with_index"` (restores tables first, then builds indexes in parallel) |
| `changes_only` | boolean | No | `true` for Smart Sync / Diff Mode (`--mode=upsert`) |

---

## 6. One-Command Automation via CLI Script

Your repo contains [`scripts/backup_cli.py`](file:///c:/Users/nawaz/OneDrive/Desktop/IH-Projects/Auto-Backup/scripts/backup_cli.py), which handles triggering, live progress polling, and exit codes in a single command.

### Trigger Backup & Wait for Completion:
```bash
python scripts/backup_cli.py backup \
  --api-key "bv_live_YOUR_KEY" \
  --db-id "YOUR_DATABASE_ID" \
  --storage-id "YOUR_STORAGE_ID" \
  --speed-profile extreme \
  --wait
```

### Trigger Restore & Wait for Completion:
```bash
python scripts/backup_cli.py restore \
  --api-key "bv_live_YOUR_KEY" \
  --backup-id "YOUR_BACKUP_ID" \
  --target-db-id "TARGET_DATABASE_ID" \
  --target-name "HotelStaticData" \
  --speed-profile extreme \
  --wait
```

---

## 7. Python Script Integration Example

Here is a lightweight Python script using `requests` you can copy directly into your automation pipelines:

```python
import time
import requests

API_URL = "https://autobackup.dev.travelswitch.com"
API_KEY = "bv_live_YOUR_KEY"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

def trigger_backup(db_id: str, storage_id: str, label: str = "automated-backup"):
    payload = {
        "database_id": db_id,
        "storage_id": storage_id,
        "backup_method": "full",
        "speed_profile": "extreme",
        "indexing_mode": "with_index",
        "label": label
    }
    
    # 1. Trigger backup
    print(f"[*] Triggering backup for DB: {db_id}...")
    res = requests.post(f"{API_URL}/api/backups/", json=payload, headers=HEADERS)
    res.raise_for_status()
    backup_id = res.json()["id"]
    print(f"[+] Backup job started! ID: {backup_id}")

    # 2. Poll until completed or failed
    print("[*] Waiting for completion...")
    while True:
        poll = requests.get(f"{API_URL}/api/backups/", headers=HEADERS).json()
        job = next((b for b in poll if b["id"] == backup_id), None)
        if not job:
            time.sleep(3)
            continue
        
        status = job.get("status")
        pct = job.get("progress_percentage", 0)
        speed = job.get("speed_mbps", 0)
        print(f" -> Status: {status.upper()} | Progress: {pct}% | Speed: {speed} MB/s")

        if status == "completed":
            print(f"[SUCCESS] Backup finished! Remote archive: {job.get('remote_name')}")
            return job
        elif status == "failed":
            raise RuntimeError(f"Backup failed: {job.get('error')}")

        time.sleep(3)

if __name__ == "__main__":
    trigger_backup(
        db_id="YOUR_DATABASE_ID",
        storage_id="YOUR_STORAGE_ID",
        label="pre-deployment-backup"
    )
```

---

## 8. Terraform Integration Example

In your Terraform root or module:

```hcl
variable "backupvault_api_key" {
  type        = string
  description = "BackupVault persistent API key"
  sensitive   = true
}

resource "null_resource" "pre_release_backup" {
  # Trigger before database migrations or infrastructure changes
  provisioner "local-exec" {
    command = <<EOT
      python scripts/backup_cli.py backup \
        --api-key "${var.backupvault_api_key}" \
        --db-id "YOUR_DATABASE_ID" \
        --storage-id "YOUR_STORAGE_ID" \
        --speed-profile extreme \
        --wait --timeout 1800
    EOT
  }
}
```
If the backup fails or times out, Terraform will automatically halt and fail the deployment.
