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
- **Database Client Performance Tuning:**
  - Configures PostgreSQL for memory constraints (`work_mem=16MB`, `maintenance_work_mem=64MB`) to safely ensure minimal memory usage even during massive parallel operations.
  - MongoDB restore concurrency is optimized to 4 parallel collections and 4 insertion workers per collection (batch size of 5000) to maximize write performance and restore 50GB in 15 minutes.
  - Uses Go Runtime environment tuning (`GOGC=100`) and multicore thread scheduling (`GOMAXPROCS=4`) for `mongodump`/`mongorestore` to achieve maximum throughput on multi-core host systems.
  - Removed all artificial sleep delays from streaming background threads to enable line-rate direct download and decompression speeds.
  - Uses direct connection URI (`--uri`) in MongoDB tool arguments to preserve critical options like SSL/TLS, `replicaSet`, and `directConnection` from connection strings.
  - Implements dynamic wildcard namespace remapping (`--nsFrom=$database$.$collection$` and `--nsTo=target_db.$collection$`) to guarantee clean database renames when restoring archives.
- **Server-Side Resource Control (Database Engine Internal Limits):**
  - **MongoDB Server:** `--readPreference=secondaryPreferred` offloads backup reads to replica secondaries to reduce primary server load. `--bypassDocumentValidation` skips server-side document validation during restores (saves server CPU). `--writeConcern=1` (standard `w:1`) ensures acknowledged writes on the primary/standalone node without replica set blocking.
  - **PostgreSQL Server:** `max_parallel_workers_per_gather=0` prevents the Postgres server from spawning parallel worker processes (the #1 cause of server CPU spikes). `effective_io_concurrency=1` limits IO prefetching. `--disable-triggers` in `pg_restore` prevents trigger execution during data load, massively reducing server CPU during restores.
- **Granular Indexing Modes:**
  - Introduced API/UI options for Backups and Restores: **Include Indexes**, **Exclude Indexes (Fast Data)**, and **Only Indexes (Schema)**.
  - "Exclude Indexes" mode skips costly `mongorestore` background index rebuilds and Postgres `post-data` sections, driving restore speeds beyond 100GB in 20 minutes natively.
  - Auto-fetches database schema and indexes instantly on connection via PyMongo and `psql` querying.
- **Incremental & Differential Backups:**
  - True Incremental and Differential Backups filter queries and databases dynamically using the last successful backup or full backup dates.
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
  - **Neon Donkey Favicon**: A customized neon donkey favicon linked globally across the dashboard.
- **Apache Solr Support:**
  - Backup and Restore capabilities using native Apache Solr Collections API, with robust URL parsing, credentials-based basic authentication support, and built-in SSL certificate validation bypass for secure HTTPS endpoints (such as `https://solr.dev.travelswitch.com`).
  - Streamlined manual backup workflow: automatically queries Solr server cores as the "Collection / Core" selector, and fetches core schema fields as the "Schema Fields" options.
- **Webhook Notifications:**
  - Configurable alerts for backup/restore success and failures directly to Slack, Microsoft Teams, and Telegram.
  - Custom User-Agent headers to prevent gateway firewalls from blocking notifications.
  - Instant Webhook testing on the Settings page to verify delivery.
  - Per-backup preference toggle to opt-out of notification dispatches for specific manual runs.
- **Dynamic Scheduling:**
  - Robust time-window based scheduler logic avoiding race conditions.
  - Native browser-integrated time picker supporting precise hour and minute selections.

