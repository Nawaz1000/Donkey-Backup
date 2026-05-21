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
  - Architectural and code-level changes designed to scale database backups/restores up to 100GB+ daily with extremely low CPU and memory footprints.
- **Thread-safe `QueueReader` (Parallel Cloud Uploads/Downloads):**
  - Added a `threading.Lock` to synchronize buffer and queue access, preventing race conditions and buffer corruption under concurrency.
  - Replaced slow byte concatenation (`+=`) with a `collections.deque` queue to prevent repeated memory reallocation and copying.
  - Optimized streaming chunk size to `1MB` and queue capacity to `32` chunks to support high network throughput with capped client-side memory.
- **Multi-Threaded Parallel Compression Pipeline:**
  - Auto-detects system-level availability of `zstd` (Zstandard), `pigz` (Parallel Gzip), and standard `gzip` to optimize compression speed.
  - Pipes database dump outputs directly to `zstd -1 --threads=0` (if available) or `pigz -1` (if available) to leverage all available CPU cores with minimal overhead.
  - Database native compression is disabled (`mongodump` `--gzip` and `pg_dump` `--compress=0` when external tools are present) to offload compression processing, freeing up database client resources.
- **Dynamic Decompression Pipeline on Restore:**
  - Backups record the compression mechanism used (`zstd`, `pigz`, `gzip`, or `native`) in the metadata.
  - Restore engine reads the metadata and dynamically routes the download stream through the correct decompression pipeline (`zstd -d -c`, `pigz -d -c`, etc.) on the fly.
  - Built with fallback mechanisms to ensure complete backwards-compatibility with older backups using native gzip/custom pg_dump compression formats.
- **PostgreSQL Client-Side Database Client Tuning:**
  - Configured `PGOPTIONS="-c statement_timeout=0 -c work_mem=128MB"` for `pg_dump` and `pg_restore` to optimize PostgreSQL memory sorting and execution times.
- **Direct Cloud Streaming (Zero Disk IO & Low Memory)**:
  - Completely removed intermediate disk writes. Dump output is piped directly into Azure/GCS.
- **Cloud Upload/Download Tuning**:
  - Configured GCS uploads with an explicit `chunk_size` of 16MB to enable fast resumable uploads and prevent Google Cloud Client Library from loading the entire backup stream into RAM.
  - Configured Azure uploads and downloads with `max_concurrency=4` and `max_block_size=4MB` to process blocks in parallel, maximizing network throughput while strictly capping client-side memory overhead under 16MB.
- **Advanced Automated Scheduling:**
  - Implemented an `asyncio`-based background scheduler in FastAPI that automatically manages and triggers backups based on user-defined intervals (hourly, daily, weekly, monthly).
- **Incremental Backups (MongoDB):**
  - Added support for True Incremental Backups using MongoDB's `--query` flag. Users can specify an "Incremental Field" (e.g., `updated_at`) and the engine will only back up records newer than the last successful backup timestamp.
- **Premium Glassmorphism UI:**
  - Redesigned the user interface using modern web aesthetics, including glassmorphism effects (`backdrop-filter`), smooth gradients, and advanced layout structures.

