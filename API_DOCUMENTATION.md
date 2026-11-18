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

## 2. Step 1: Get Your Database ID & Storage ID

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
  }
]
```
> Copy the `"id"` value for your target database.

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
  }
]
```
> Copy the `"id"` value for your target storage.

---

## 3. Step 2: Trigger a Backup via API (cURL)

Trigger a backup by sending a `POST` request to `/api/backups/`:

```bash
curl -s -X POST "https://autobackup.dev.travelswitch.com/api/backups/" \
  -H "Content-Type: application/json" \
  -d '{
    "database_id": "YOUR_DATABASE_ID",
    "storage_id": "YOUR_STORAGE_ID",
    "backup_method": "full",
    "speed_profile": "extreme",
    "indexing_mode": "with_index",
    "label": "automated-backup"
  }'
```

### Response (`200 OK`):
```json
{
  "id": "b74900a2-3f12-4d2c-8a19-b2f9104ac715",
  "database_id": "YOUR_DATABASE_ID",
  "storage_id": "YOUR_STORAGE_ID",
  "status": "pending",
  "created_at": "2026-11-18T12:00:00Z"
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
  "status": "in_progress",
  "progress_percentage": 78,
  "speed_mbps": 154.2,
  "size_mb": 425.8,
  "remote_name": "ts-prod-mongodb-2026-11-18/backup.archive.zst"
}
```

To view live terminal logs of the backup:
```bash
curl -s https://autobackup.dev.travelswitch.com/api/backups/YOUR_BACKUP_ID/logs
```

---

## 5. Step 4: Trigger a Restore via API

To restore a backup into a database:

```bash
curl -s -X POST "https://autobackup.dev.travelswitch.com/api/backups/restore" \
  -H "Content-Type: application/json" \
  -d '{
    "backup_id": "YOUR_BACKUP_ID",
    "target_database_id": "TARGET_DATABASE_ID",
    "target_database_name": "HotelStaticData",
    "speed_profile": "extreme",
    "indexing_mode": "with_index"
  }'
```

---

## 6. One-Line Command via Python CLI Script

Use [`scripts/backup_cli.py`](file:///c:/Users/nawaz/OneDrive/Desktop/IH-Projects/Auto-Backup/scripts/backup_cli.py) directly without any API keys or credentials:

### Trigger Backup & Wait for Completion:
```bash
python scripts/backup_cli.py backup \
  --db-id "YOUR_DATABASE_ID" \
  --storage-id "YOUR_STORAGE_ID" \
  --speed-profile extreme \
  --wait
```

### Trigger Restore & Wait for Completion:
```bash
python scripts/backup_cli.py restore \
  --backup-id "YOUR_BACKUP_ID" \
  --target-db-id "TARGET_DATABASE_ID" \
  --target-name "HotelStaticData" \
  --speed-profile extreme \
  --wait
```

---

## 7. Python Script Integration Example

```python
import time
import requests

API_URL = "https://autobackup.dev.travelswitch.com"

def run_backup(db_id: str, storage_id: str):
    # 1. Trigger backup (no auth headers required!)
    payload = {
        "database_id": db_id,
        "storage_id": storage_id,
        "backup_method": "full",
        "speed_profile": "extreme",
        "indexing_mode": "with_index"
    }
    res = requests.post(f"{API_URL}/api/backups/", json=payload)
    res.raise_for_status()
    backup_id = res.json()["id"]
    print(f"[*] Backup triggered! ID: {backup_id}")

    # 2. Wait for completion
    while True:
        poll = requests.get(f"{API_URL}/api/backups/").json()
        job = next((b for b in poll if b["id"] == backup_id), None)
        if job:
            status = job.get("status")
            pct = job.get("progress_percentage", 0)
            spd = job.get("speed_mbps", 0)
            print(f" -> Status: {status.upper()} | Progress: {pct}% | Speed: {spd} MB/s")
            
            if status == "completed":
                print(f"[SUCCESS] Done! Remote file: {job.get('remote_name')}")
                return job
            elif status == "failed":
                raise RuntimeError(f"Backup failed: {job.get('error')}")
        time.sleep(3)

if __name__ == "__main__":
    run_backup("YOUR_DATABASE_ID", "YOUR_STORAGE_ID")
```

---

## 8. Terraform Integration Example

```hcl
resource "null_resource" "trigger_backup" {
  provisioner "local-exec" {
    command = <<EOT
      python scripts/backup_cli.py backup \
        --db-id "YOUR_DATABASE_ID" \
        --storage-id "YOUR_STORAGE_ID" \
        --speed-profile extreme \
        --wait --timeout 1800
    EOT
  }
}
```
