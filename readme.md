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
- **High-Performance 100GB Daily Backup Engine:**
  - Zero-copy streaming architecture designed to scale database backups/restores up to 100GB+ under 30 minutes with extremely low CPU and memory footprints.
- **High-Performance Zero-Copy `QueueReader`:**
  - Optimized queue and buffer management using memory pointer offsets and Python `memoryview` stream wrappers.
  - Completely eliminates array slicing memory copy overhead, reducing Python process CPU utilization to near 0% and maximizing upload throughput.
- **Resource Limits & CPU Throttling (Capped under 500m CPU):**
  - Restricts Zstandard (`zstd`) and parallel gzip (`pigz`) compression pipelines to **1 thread** (`--threads=1`, `-p 1`). This caps system resource footprints under 0.5 core to prevent host system thrashing.
- **Actual Progress Tracking (Bar, Percentages, and Stderr Parsing):**
  - Parses real-time `mongodump` and `mongorestore` logs using background regex parsers to fetch progress percentages.
  - Queries Postgres database size using SQL `pg_database_size` and tracks bytes streamed via a background `pipe_and_count` thread to compute exact progress.
  - Wraps GCS/Azure download streams with a `ProgressWriter` to monitor restore progress.
- **Database Client Performance Tuning:**
  - Configures `PGOPTIONS="-c statement_timeout=0 -c work_mem=256MB -c maintenance_work_mem=512MB"` for PostgreSQL to accelerate index creation and constraint checking.
  - Sets `--numInsertionWorkersPerCollection=4` and `--numParallelCollections=4` on MongoDB restores to parallelize write insertions.
- **Incremental Backups (MongoDB):**
  - True Incremental Backups filter queries dynamically using the last successful backup date.
- **User Interface Enhancements:**
  - Support for deleting restore history from the UI.
  - Active Loading indicator spinners on manual refresh actions.
  - UTC timezone rendering with local browser conversion for database connections and history tables.

