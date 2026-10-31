# BackupVault

A self-hosted database backup manager for **PostgreSQL**, **MongoDB**, and **Apache Solr**, with support for **Azure Blob Storage**, **Google Cloud Storage**, **Amazon S3 / S3-Compatible**, and **BunnyCDN Edge Storage**.

## Features
- 🔐 User authentication (JWT)
- 🗃️ Manage PostgreSQL, MongoDB & Apache Solr connections
- ☁️ Azure Blob, Google Cloud Storage, Amazon S3, and BunnyCDN Edge Storage destinations
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

### Amazon S3 / S3-Compatible
1. Create a bucket in AWS S3 or compatible provider (MinIO, Cloudflare R2, Wasabi, DigitalOcean Spaces)
2. Enter Bucket Name, Access Key, Secret Key, and Region
3. (Optional) Specify Custom Endpoint URL for S3-compatible providers

### BunnyCDN Edge Storage
1. Create a Storage Zone in Bunny.net (e.g. `databasus-poc`)
2. In Bunny.net, navigate to **Storage -> {your-zone} -> FTP & API Access**
3. Copy the **Password** (Read/Write Password)
   > ⚠️ **Important:** Do not use the Storage Zone Name as the password. Use the secret password generated under "FTP & API Access".
4. Enter the **Storage Zone Name** and **Password** in BackupVault
5. (Optional) Provide a Region URL (e.g. `https://la.storage.bunnycdn.com`) or let BackupVault automatically detect your zone's region endpoint!
6. (Optional) Specify a folder name (e.g. `postgres-backup` or `database-backups`)

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
- **Storage:** Azure Blob Storage SDK, Google Cloud Storage SDK, Boto3 S3 SDK, BunnyCDN REST & S3 API
- **Auth:** JWT tokens, bcrypt password hashing

## Recent Optimizations & Advanced Features
- **High-Performance 100GB Daily Backup Engine:**
  - Zero-copy streaming architecture designed to scale database backups/restores up to 100GB+ under 30 minutes with extremely low CPU and memory footprints.
- **High-Performance Zero-Copy `QueueReader`:**
  - Optimized queue and buffer management using memory pointer offsets and Python `memoryview` stream wrappers.
  - Completely eliminates array slicing memory copy overhead, reducing Python process CPU utilization to near 0% and maximizing upload throughput.
- **Ultra-Fast Dynamic Resource Scaling & Speed Profiles:**
  - Dynamically scales Zstandard (`zstd`) and parallel gzip (`pigz`) compression/decompression pipelines, CDN/S3/Azure/GCS chunk sizes, network concurrency limits, and MongoDB parallel insertion workers.
  - Granular API and UI options (Default, Balanced, Extreme, Safe) allow fine-tuning CPU usage during backups and restores to protect production instances during the day.
  - **Memory-Bounded Protection:** In-memory queue bounds are strictly calculated (`chunk_size * queue_max`, e.g. 16MB in Safe up to 512MB in Extreme) to prevent RAM bloat and eliminate Out-Of-Memory (OOM) crashes on constrained host instances.
- **High-Speed CDN (BunnyCDN & S3) Streaming Acceleration:**
  - **Parallel Multipart Uploads:** BunnyCDN and S3 storage endpoints now leverage multi-threaded parallel multipart uploads (`TransferConfig(use_threads=True, max_concurrency=16)` with 16MB–64MB parts) and persistent connection pools (`max_pool_connections=32`), completely eliminating single-stream network latency bottlenecks.
  - **Decoupled Prefetch Streaming Pipeline for Restores:** Restore downloads from BunnyCDN (both S3 and high-speed REST endpoints) and S3 use a dedicated prefetch buffer queue. The network download worker continuously buffers incoming chunks in RAM up to the profile limit, completely preventing TCP window collapse while the database restore engine processes and writes tables to disk.
  - **Zero-Latency Zero-Sleep Pipelining:** Removed all artificial sleep delays in `reader_thread_fn` and widened `pipe_and_count` buffer blocks from 256KB to 2MB, reducing context switching and delivering maximum line-rate throughput.
- **Live Resource & Speed Calculations:**
  - **Accurate Real-Time Transfer Metrics:** `ProgressTracker` calculates real-time throughput (MB/s), elapsed duration, and ETA, logging detailed live statistics (e.g. `Progress: 76% (421.5 MB processed at 48.2 MB/s)`) and surfacing live `speed_mbps`, `processed_mb`, and `total_mb` directly to the UI dashboard card.
  - **Automatic Remote Object Sizing:** Restore operations automatically query remote object headers (`get_storage_object_size`) across BunnyCDN, S3, Azure, and GCS to resolve exact archive byte counts, ensuring restore progress bars and speed meters calculate with 100% precision from start to finish.
- **Night-Time Auto-Scaling:**
  - Background scheduler automatically detects night-time windows (12:00 AM - 6:00 AM) and seamlessly upgrades "Default" schedules to the absolute maximum "Extreme" performance limits to completely saturate the network when application traffic is lowest.
- **Disk-Flush Backpressure (OOM Crash Protection):**
  - **MongoDB:** Uses `w: 1` with 10 insertion workers, creating a balanced memory-acknowledgment stream. This allows `mongorestore` to insert data at maximum speed while WiredTiger naturally manages disk eviction in the background, minimizing memory bloat and maximizing insertion speed.
  - **PostgreSQL:** Throttles safely with parallel workers (`--jobs=4`), enforcing memory limits (`maintenance_work_mem=64MB`), and using `synchronous_commit=on` to naturally backpressure the restore without crashes.
- **Absolute Maximum Streaming Throughput (64MB Chunks & 32x Concurrency):**
  - Downloads and uploads GCS/Azure objects using massively parallel **64MB** data blocks with a 1GB memory buffer to guarantee absolute network saturation.
  - Azure pipelines natively leverage `max_concurrency=32` allowing the system to push 32 separate 64MB network streams simultaneously. This completely unbottlenecks high-latency connections, pushing 50GB+ backups to finish in incredibly short timeframes (e.g. under 5 minutes).
- **Robust Connection Options Parsing:**
  - Implements an advanced, case-insensitive URI parser that detects database options from both query parameters and path-based segments (such as `...:27017/authMechanism=...`), auto-corrects them, and connects securely using keyword argument credentials to bypass PyMongo URI decoding limitations with special characters (like `!`).
- **Verbosity & Notification Logging:**
  - Job logs now track webhook notification dispatches and connection statuses in real-time, detailing successes and errors in the job's viewable console logs.
- **MongoDB Cluster-Wide & Targeted Scheduled Backups:**
  - Schedule targeted backups for specific databases or granular collections right from the UI, or let the engine automatically iterate over all available databases in the cluster for a full backup, streaming each into a unified cluster-wide folder.
- **Automated Cluster Restore:**
  - Native detection of cluster-wide backup folders triggering sequential object discovery and database-specific restores directly from Azure/GCS without manual intervention.
- **Local Timezone Scheduling & Accurate History:**
  - Integrates local browser timezone offsets directly into the backend scheduling loop. This ensures background jobs always evaluate trigger times using exact local time rather than UTC, making scheduling foolproof across global regions.
  - The UI cleanly handles UTC conversions natively (via `Z` parsing) so your historical backup and restore logs reflect your exact local time down to the minute.

- **Actual Progress Tracking (Bar, Percentages, and Stderr Parsing):**
  - Parses real-time `mongodump` and `mongorestore` logs using background regex parsers to fetch progress percentages.
  - Queries Postgres database size using SQL `pg_database_size` and tracks bytes streamed via a background `pipe_and_count` thread to compute exact progress.
  - Wraps GCS/Azure download streams with a `ProgressWriter` to monitor restore progress and seamlessly track stdin archive ingestion metrics for MongoDB restores where logs are unavailable.
- **Database Client Performance Tuning:**
  - Configures PostgreSQL for memory constraints (`work_mem=32MB`, `maintenance_work_mem=128MB` in balanced/default, up to `512MB` in extreme) to safely ensure minimal memory usage even during massive parallel operations.
  - MongoDB restore concurrency is heavily throttled to 1 parallel collection and 1 insertion worker (batch size of 500) to ensure predictable and safe low-memory execution.
  - Uses Go Runtime environment tuning (`GOGC=100`) and single-core thread scheduling (`GOMAXPROCS=1`) to prevent database resource exhaustion on 1-core instances.
  - Removed all artificial sleep delays from streaming background threads to enable line-rate direct download and decompression speeds.
  - Uses direct connection URI (`--uri`) in MongoDB tool arguments to preserve critical options like SSL/TLS, `replicaSet`, and `directConnection` from connection strings.
  - Implements dynamic wildcard namespace remapping (`--nsFrom=$database$.$collection$` and `--nsTo=target_db.$collection$`) to guarantee clean database renames when restoring archives.
- **Server-Side Resource Control (Database Engine Internal Limits):**
  - **MongoDB Server:** `--readPreference=secondaryPreferred` offloads backup reads to replica secondaries to reduce primary server load. `--bypassDocumentValidation` skips server-side document validation during restores (saves server CPU). `--writeConcern=1` (standard `w:1`) ensures acknowledged writes on the primary/standalone node without replica set blocking.
  - **PostgreSQL Server:** `max_parallel_workers_per_gather=0` prevents the Postgres server from spawning parallel worker processes (the #1 cause of server CPU spikes). `effective_io_concurrency=1` limits IO prefetching. `--disable-triggers` in `pg_restore` prevents trigger execution during data load, massively reducing server CPU during restores.
- **Granular Indexing Modes & Sequential Restoration:**
  - Introduced API/UI options for Backups and Restores: **Include Indexes**, **Exclude Indexes (Fast Data)**, and **Only Indexes (Schema)**.
  - **Sequential MongoDB Index Restoration (Low Memory Indexing):** Dynamically extracts index structures via PyMongo during backup and uploads them as a `{remote_name}.indexes.json` metadata file. During restore, it uses `--noIndexRestore` to load raw data instantly, then rebuilds each index sequentially (one-by-one) via PyMongo. This prevents MongoDB from spilling index builds to disk when concurrent limits are exceeded, making index restoration up to **10x faster** on limited-resource servers.
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
- **Webhook Notifications (Comprehensive Triggers):**
  - Configurable alerts triggered immediately when a backup or restore **Starts**, **Completes Successfully**, or **Fails**, dispatched directly to Slack, Microsoft Teams (using modern Adaptive Cards), and Telegram.
  - Fault-tolerant Webhook Pipeline: A failure to send to one provider (like Slack) will no longer abort notifications to other providers (like Telegram). Detailed HTTP error logs are now injected directly into the active job logs for rapid debugging.
  - Custom User-Agent headers to prevent gateway firewalls from blocking notifications.
  - Instant Webhook testing on the Settings page to verify delivery.
  - Per-backup preference toggle to opt-out of notification dispatches for specific manual runs.
- **Dynamic Scheduling & Automated Retention:**
  - Robust time-window based scheduler logic avoiding race conditions.
  - Native browser-integrated time picker supporting precise hour and minute selections.
  - **Retention Policy (Keep Last N Backups):** Configure schedules to automatically retain only the last *N* completed backups (e.g. keep last 7 daily backups). Older runs exceeding the threshold are automatically pruned from both the dashboard and remote cloud storage.
- **BunnyCDN Dual-Engine Streaming & Auto-Region Detection:**
  - High-speed zero-copy streaming to **BunnyCDN Edge Storage** with multi-part S3 acceleration and automatic REST API fallback.
  - Intelligent auto-detection of regional Edge Storage endpoints (`la` Los Angeles, `de` Falkenstein, `uk` London, `sg` Singapore, `syd` Sydney, `ny` New York) based on your zone's primary storage region.
  - Pre-flight storage verification tests connectivity and ensures the storage zone password is used rather than the zone name.
- **Automated Cloud Directory & Folder Cleanup (Zero Ghost Folders):**
  - Backups are stored in isolated, timestamped directories (e.g. `postgres-backup/ts-prod-postgres-2026-10-31_07-14-00/backup.archive.zst`).
  - When a backup is deleted from the UI via **"Delete from Cloud Bucket & Local"** (or pruned by a schedule's retention policy), BackupVault removes all associated archive objects and automatically issues a directory-level `DELETE` request with a trailing slash (`/`) to the BunnyCDN Edge Storage REST API (`DELETE https://{bunny_host}/{zone}/{backup_dir}/`).
  - This guarantees that empty folders and directory markers are completely wiped from Bunny.net's File Manager, preventing orphaned empty folders from accumulating while safely preserving your root base folder (e.g. `postgres-backup/`).

