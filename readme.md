# BackupVault

A self-hosted database backup manager for **PostgreSQL** and **MongoDB**, with support for **Azure Blob Storage** and **Google Cloud Storage**.

## Features
- 🔐 User authentication (JWT)
- 🗃️ Manage PostgreSQL & MongoDB connections
- ☁️ Azure Blob Storage & Google Cloud Storage destinations
- 💾 Manual & scheduled backups
- 📊 Dashboard with stats
- 🐳 Docker-ready

## Quick Start

### Option 1: Docker Compose (Recommended)
```bash
docker-compose up -d
```
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000/docs

### Option 2: Run locally

**Backend:**
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

**Frontend:**
Open `frontend/index.html` in your browser (or serve with any static server).
> ⚠️ If opening directly, change `API` in index.html to `http://localhost:8000/api`

## Configuration

| Variable | Default | Description |
|---|---|---|
| `SECRET_KEY` | `backupvault-secret` | JWT signing key — **change in production!** |

## Storage Setup

### Azure Blob Storage
1. Create a Storage Account in Azure Portal
2. Create a container for backups
3. Copy Account Name + Account Key from "Access Keys"

### Google Cloud Storage
1. Create a GCS bucket
2. Create a Service Account with `Storage Object Creator` role
3. Download the JSON key file and paste its contents

## Project Structure
```
backupvault/
├── backend/
│   ├── main.py          # FastAPI app
│   ├── utils.py         # JWT + file helpers
│   ├── routers/
│   │   ├── auth.py
│   │   ├── databases.py
│   │   ├── storage.py
│   │   ├── backups.py
│   │   └── schedules.py
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   └── index.html       # Single-page app
└── docker-compose.yml
```

## Tech Stack
- **Backend:** Python, FastAPI, JWT, bcrypt
- **Frontend:** Vanilla JS, CSS (no framework needed)
- **Storage:** Azure Blob Storage SDK, Google Cloud Storage SDK
- **Auth:** JWT tokens, bcrypt password hashing

## Recent Optimizations & Advanced Features
- **Advanced Automated Scheduling:**
  - Implemented an `asyncio`-based background scheduler in FastAPI that automatically manages and triggers backups based on user-defined intervals (hourly, daily, weekly, monthly).
- **Incremental Backups (MongoDB):**
  - Added support for True Incremental Backups using MongoDB's `--query` flag. Users can specify an "Incremental Field" (e.g., `updated_at`) and the engine will only back up records newer than the last successful backup timestamp.
- **Premium Glassmorphism UI:**
  - Redesigned the user interface using modern web aesthetics, including glassmorphism effects (`backdrop-filter`), smooth gradients, and advanced layout structures.
- **Direct Cloud Streaming (Zero Disk IO):**
  - Completely removed the intermediate step of writing backups to the local container disk.
  - Now, `pg_dump` and `mongodump` output streams (`stdout`) are directly piped into the Azure Blob Storage and Google Cloud Storage SDKs.
  - This results in zero extra disk space required on the container during backups and halves the total backup duration since local disk writes/reads are bypassed.
- **MongoDB Backup & Restore:** 
  - Eliminated disk-heavy intermediate files and CPU-intensive double-compression (`tar -czf`) by streaming `mongodump` directly into a single gzip archive using the `--archive` flag.
  - Reduced `--numParallelCollections` to 1, drastically reducing memory consumption and CPU spikes during the backup process.
- **PostgreSQL Backup:** 
  - Adjusted `pg_dump` compression level from 4 to 1 (`--compress=1`), prioritizing maximum backup speed and lowering CPU overhead without significantly impacting storage space.
