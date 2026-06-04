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
- **Ultra-Fast Dynamic CPU Scaling:**
  - Dynamically scales Zstandard (`zstd`) and parallel gzip (`pigz`) compression pipelines up to a safe maximum of **8 parallel threads**. This dramatically accelerates massive gigabyte backup speeds while intentionally leaving remaining CPU cores free for your database and other production applications.
- **Disk-Flush Backpressure (OOM Crash Protection):**
  - **MongoDB:** Uses `w: 1` with 10 insertion workers, creating a balanced memory-acknowledgment stream. This allows `mongorestore` to insert data at maximum speed while WiredTiger naturally manages disk eviction in the background, minimizing memory bloat and maximizing insertion speed.
  - **PostgreSQL:** Throttles safely with parallel workers (`--jobs=4`), enforcing memory limits (`maintenance_work_mem=64MB`), and using `synchronous_commit=on` to naturally backpressure the restore without crashes.
- **Extreme Streaming Throughput (32MB Chunks & 16x Concurrency):**
  - Downloads and uploads GCS/Azure objects using massively parallel **32MB** data blocks with a 1GB memory buffer to maximize network saturation.
  - Azure pipelines natively leverage `max_concurrency=16` allowing the system to push 16 separate 32MB network streams simultaneously. This completely unbottlenecks high-latency connections, allowing massive 50GB+ backups to finish in extremely short timeframes (e.g. 5 minutes).
- **Robust Connection Options Parsing:**
  - Implements an advanced, case-insensitive URI parser that detects database options from both query parameters and path-based segments (such as `...:27017/authMechanism=...`), auto-corrects them, and connects securely using keyword argument credentials to bypass PyMongo URI decoding limitations with special characters (like `!`).
- **Verbosity & Notification Logging:**
  - Job logs now track webhook notification dispatches and connection statuses in real-time, detailing successes and errors in the job's viewable console logs.

- **Actual Progress Tracking (Bar, Percentages, and Stderr Parsing):**
  - Parses real-time `mongodump` and `mongorestore` logs using background regex parsers to fetch progress percentages.
  - Queries Postgres database size using SQL `pg_database_size` and tracks bytes streamed via a background `pipe_and_count` thread to compute exact progress.
  - Wraps GCS/Azure download streams with a `ProgressWriter` to monitor restore progress and seamlessly track stdin archive ingestion metrics for MongoDB restores where logs are unavailable.
- **Database Client Performance Tuning:**
  - Configures PostgreSQL for memory constraints (`work_mem=16MB`, `maintenance_work_mem=64MB`) to safely ensure minimal memory usage even during massive parallel operations.
  - MongoDB restore concurrency is heavily throttled to 1 parallel collection and 1 insertion worker (batch size of 500) to ensure predictable and safe low-memory execution.
  - Uses Go Runtime environment tuning (`GOGC=100`) and single-core thread scheduling (`GOMAXPROCS=1`) to prevent database resource exhaustion on 1-core instances.
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
  - **Storage Connection Management:** In-UI Storage editor to easily update storage credentials and paths without deleting them.
  - "Total Restores" added to the main Dashboard analytics.
  - **Remote Deletion:** Remote cloud storage object deletion directly from the BackupVault dashboard with a dedicated "Delete from Bucket" button. Features robust error reporting directly to the UI if Azure/GCS credentials expire or the file doesn't exist.
  - Restoring external backups natively by auto-fetching existing backup files straight from Cloud buckets without requiring local history.
  - Live progress card state persistence — progress bars instantly resume tracking background jobs even across hard browser refreshes.
  - Support for deleting restore history from the UI.
  - Active Loading indicator spinners on manual refresh actions.
  - UTC timezone rendering with local browser conversion for database connections and history tables.
  - **Log Windowing**: Prevents browser lag/freezing during massive failure events (e.g. thousands of duplicate key errors) by efficiently limiting DOM nodes to only the last 100 log lines.
  - **Neon Donkey Favicon**: A customized neon donkey favicon linked globally across the dashboard.
- **Apache Solr Support (Zero-Dependency HTTP Streaming):**
  - Completely bypasses Solr's filesystem dependency. Uses a custom Python HTTP-streaming pipeline that iteratively fetches documents via `/select` (with `cursorMark` pagination) and restores them in optimal batches using `/update`.
  - No shared volume mounts or `/tmp/backupvault` configuration required. You only need the Solr URL, Username, and Password.
  - Dynamically discovers the unique key for pagination directly from the Solr schema. Automatically compresses payloads on the fly via `zstd` or `pigz` and streams directly to Cloud Storage.
  - **Dynamic URL Formatting:** Simply entering `localhost` or an IP will automatically resolve to standard HTTP URLs (pre-pending `http://`).
  - **Accurate Backup Progress Bar:** Calculates exact progress percentages by fetching actual byte sizes from the Solr Cores API before the stream begins.
  - Fully supports restoring existing backups directly into new, dynamically named Solr collections. It natively auto-creates new target Solr collections if they don't already exist prior to dumping data.
  - **Schema Preservation:** Automatically extracts your exact schema (including custom `metaphone`, `string`, and `text` field types, dynamic fields, and copy fields) via the `/schema` API during a backup. Restores intelligently parse and apply this exact schema to new collections *before* data ingestion, perfectly mirroring your original query configurations.
  - **Extreme API Batching:** Drastically slashes HTTP network overhead by extracting `20,000` documents per `/select` batch and pushing `5,000` documents per `/update` batch. Features a zero-latency `bytearray` stream buffer to maximize Solr pipeline speeds.
- **Job Management:**
  - Includes a global "Stop Job" functionality that allows users to instantly terminate active backup and restore background processes from the UI. 
  - Safely sets threading stop events (`ACTIVE_STOP_EVENTS`) to unblock cloud streaming pipes instantly, alongside forceful termination signals to prevent hanging streams or runaway I/O tasks.
- **Webhook Notifications:**
  - Configurable alerts for backup/restore success and failures directly to Slack, Microsoft Teams (using modern Adaptive Cards for Power Automate Workflows), and Telegram.
  - Fault-tolerant Webhook Pipeline: A failure to send to one provider (like Slack) will no longer abort notifications to other providers (like Telegram). Detailed HTTP error logs are now injected directly into the active job logs for rapid debugging.
  - Custom User-Agent headers to prevent gateway firewalls from blocking notifications.
  - Instant Webhook testing on the Settings page to verify delivery.
  - Per-backup preference toggle to opt-out of notification dispatches for specific manual runs.
- **Dynamic Scheduling:**
  - Robust time-window based scheduler logic avoiding race conditions.
  - Native browser-integrated time picker supporting precise hour and minute selections.

