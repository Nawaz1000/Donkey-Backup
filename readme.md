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
- **Direct Cloud Streaming with Bounded Buffering (Zero Disk IO & Low Memory)**:
  - Completely removed intermediate disk writes. Dump output is piped directly into Azure/GCS.
  - Implemented a decoupled **Producer-Consumer stream buffer (`QueueReader`)** using a bounded memory queue (max 8MB). This applies backpressure to the dump tools when the network is slow, eliminating OS pipe-blocking context switches and preventing OOM kills on large databases (20GB+).
- **CPU-Optimized MongoDB Backup**:
  - Automatically pipes `mongodump` through system `gzip -1` (fastest compression level) instead of native `--gzip` (default level 6). This keeps CPU usage well under 1.5 cores for large databases while preserving a low storage footprint. Falls back to native `--gzip` if `gzip` is not in the system PATH.
  - Set `--numParallelCollections=1` to minimize resource contention.
- **Resource-Optimized PostgreSQL Backup**:
  - Configured `pg_dump` with client compression level 1 (`--compress=1`) to achieve the fastest compression and lowest CPU overhead.
- **Cloud Upload Tuning**:
  - Configured GCS uploads with an explicit `chunk_size` of 8MB to enable resumable uploads and prevent Google Cloud Client Library from loading the entire backup stream into RAM.
  - Configured Azure uploads and downloads with `max_concurrency=1` to process chunks sequentially and strictly cap memory consumption.
