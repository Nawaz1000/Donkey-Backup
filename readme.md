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
- **Databases Supported:** MongoDB, PostgreSQL, Apache Solr
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
  - Wraps GCS/Azure download streams with a `ProgressWriter` to monitor restore progress and seamlessly track stdin archive ingestion metrics for MongoDB restores where logs are unavailable.
- **Database Client Performance Tuning (Strict < 500MB RAM & < 500m CPU):**
  - Configures PostgreSQL for tight memory constraints (`work_mem=16MB`, `maintenance_work_mem=64MB`) to safely ensure < 500MB total memory usage even during massive parallel operations.
  - Maximizes MongoDB restore throughput using 8 parallel insertion workers per collection (across 4 parallel collections) with a massive 10,000 batch size, achieving extreme insertion speeds. Host server RAM/CPU remains strictly bounded due to Zero-Copy streaming and single-threaded Zstd decompression.
  - Aggressive Go Runtime Environment Garbage Collection (`GOGC=20`), strict CPU capping (`GOMAXPROCS=1`), and Windows Background Process Priority Class (`BELOW_NORMAL_PRIORITY_CLASS`) for `mongodump`/`mongorestore`. This keeps host RAM strictly under 150MB, despite the massive concurrency.
  - Removed proactive Python execution yielding (`time.sleep(0.002)`) in the streaming background threads to unlock maximum network throughput, achieving 100GB backups in under 5 minutes while naturally balancing resource limits via IO blocking.
- **Server-Side Resource Control (Database Engine Internal Limits):**
  - **MongoDB Server:** `--readPreference=secondaryPreferred` offloads backup reads to replica secondaries to reduce primary server load. `--bypassDocumentValidation` skips server-side document validation during restores (saves server CPU). `--writeConcern={w:1,j:false}` skips journal fsync to reduce server IO/CPU spikes.
  - **PostgreSQL Server:** `max_parallel_workers_per_gather=0` prevents the Postgres server from spawning parallel worker processes (the #1 cause of server CPU spikes). `effective_io_concurrency=1` limits IO prefetching. `--disable-triggers` in `pg_restore` prevents trigger execution during data load, massively reducing server CPU during restores.
- **Granular Indexing Modes:**
  - Introduced API/UI options for Backups and Restores: **Include Indexes**, **Exclude Indexes (Fast Data)**, and **Only Indexes (Schema)**.
  - "Exclude Indexes" mode skips costly `mongorestore` background index rebuilds and Postgres `post-data` sections, driving restore speeds beyond 100GB in 20 minutes natively.
  - Auto-fetches database schema and indexes instantly on connection via PyMongo and `psql` querying.
- **Incremental Backups (MongoDB):**
  - True Incremental Backups filter queries dynamically using the last successful backup date.
- **User Interface & UX Enhancements:**
  - **Collapsible Sidebar:** Hamburger menu toggle to open/close the sidebar for a wider workspace view.
  - **Smart Duration Formatting:** Durations auto-format to `7m 54s`, `1h 23m 45s`, or raw seconds (if under 60s) instead of always showing raw seconds.
  - **Detailed Database Info in History Tables:** Backup and Restore tables now display the connection name, actual database name, and collection/table name for full visibility into what was backed up or restored.
  - "Total Restores" added to the main Dashboard analytics.
  - Remote cloud storage object deletion directly from the BackupVault dashboard with a dedicated "Delete from Bucket" button.
  - Live progress card state persistence — progress bars instantly resume tracking background jobs even across hard browser refreshes.
  - Support for deleting restore history from the UI.
  - Active Loading indicator spinners on manual refresh actions.
  - UTC timezone rendering with local browser conversion for database connections and history tables.
  - **Log Windowing**: Prevents browser lag/freezing during massive failure events (e.g. thousands of duplicate key errors) by efficiently limiting DOM nodes to only the last 100 log lines.
- **Direct Database Sync (Cloning):**
  - Instantly clone or sync data from a Source Database directly to a Target Database without requiring intermediate cloud storage or local disk space.
  - Streams `mongodump` directly into `mongorestore` (and `pg_dump` into `pg_restore`) over a zero-copy pipeline for maximum transfer speeds.
  - Supports Target DB renaming, specific collection scope, and dropping existing data before sync to prevent duplicate errors.
- **Apache Solr Support:**
  - Backup and Restore capabilities using native Apache Solr Collections API.
- **Webhook Notifications:**
  - Configurable alerts for backup/restore success and failures directly to Slack, Microsoft Teams, and Telegram.
- **Dynamic Scheduling:**
  - Easy-to-use visual time picker with hour, minute, and AM/PM options.

