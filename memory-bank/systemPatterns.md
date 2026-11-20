# System Patterns: BackupVault Architecture

## 1. High-Level Architecture
BackupVault is structured as a containerized, asynchronous web application and streaming engine. It bundles an Nginx reverse proxy, a FastAPI Python backend managed by Uvicorn, and a static single-page frontend inside a lightweight Linux container supervised by `supervisord`.

```
                    ┌────────────────────────────────────────────────────────┐
                    │               Client (Browser / CLI / cURL)           │
                    └──────────────────────────┬─────────────────────────────┘
                                               │ HTTP (Port 80 / 8000)
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ Docker Container (BackupVault)                                                              │
│                                                                                             │
│   ┌─────────────────────┐              ┌────────────────────────────────────────────────┐   │
│   │   Nginx (Port 80)   │───/api/*────►│ FastAPI Application (Uvicorn on :8000)         │   │
│   │   Serves Static UI  │              │ (backend/main.py)                              │   │
│   └─────────────────────┘              └───────┬────────────────────────────────────────┘   │
│                                                │                                            │
│                      ┌─────────────────────────┴────────────────────────┐                   │
│                      ▼                                                  ▼                   │
│         ┌─────────────────────────┐                        ┌─────────────────────────┐      │
│         │ SQLite (WAL mode)       │                        │ Engine Background Tasks │      │
│         │ data/backupvault.db     │                        │ (backend/engine/)       │      │
│         └─────────────────────────┘                        └────────────┬────────────┘      │
│                                                                         │                   │
│                         ┌───────────────────────────────────────────────┴─────────────┐     │
│                         ▼                                                             ▼     │
│         ┌───────────────────────────────┐                             ┌───────────────────┐ │
│         │ Native DB Client Subprocesses │                             │ Cloud Storage SDKs│ │
│         │ (mongodump, pg_dump, etc.)    │                             │ (Azure, S3, GCS,  │ │
│         └───────────────┬───────────────┘                             │  BunnyCDN, Local) │ │
│                         │                                             └─────────▲─────────┘ │
│                         │ stdout pipe                                           │           │
│                         ▼                                                       │           │
│         ┌───────────────────────────────┐         QueueReader (RawIOBase)       │           │
│         │ Compression (zstd / pigz)     │───────────────────────────────────────┘           │
│         │ In-Memory Bounded Queue       │   (Zero local disk write, zero intermediate file) │
│         └───────────────────────────────┘                                                   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

## 2. Component Breakdown

### 2.1 Backend Modules (`backend/`)
- **`main.py`**:
  - FastAPI application entry point, CORS middleware, lifespan events.
  - Hosts the background asynchronous `scheduler_loop()` polling schedules and triggering `do_backup()` in separate threads.
- **`database.py`**:
  - Singleton `DatabaseManager` providing SQLite access with WAL journal mode (`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;`).
  - Implements document collections (`users`, `databases`, `storages`, `backups`, `schedules`, `restores`, `settings`, `api_keys`).
  - Auto-migrates legacy `data/*.json` files on initial startup.
- **`utils.py`**:
  - Symmetric Fernet encryption/decryption (`encrypt_secret`, `decrypt_secret`) using `SECRET_KEY`.
  - Authentication helpers: JWT encoding/decoding, Bcrypt password hashing.
  - Request user extractor `get_current_user`: Supports `X-API-Key`, `Authorization: Bearer <token>`, and automatic fallback to default admin for open direct URL access.
  - JSON file serialization and backward-compatible wrappers for `db_manager`.
  - MongoDB URI parser and query sanitization (`strip_mongo_uri_database`).
- **`routers/`**:
  - `auth.py`: Authentication endpoints (`/api/auth/login`, `/api/auth/me`). Public registration is permanently disabled.
  - `databases.py`: Connection profile CRUD, connection testing, schema inspection.
  - `storage.py`: Storage provider CRUD, bucket listing, connection validation.
  - `backups.py`: Backup job creation, status polling, live logs inspection, restore triggers, job cancellation, archive deletion.
  - `schedules.py`: Scheduled job management and next-run calculations.
  - `settings.py`: System configuration, email alert settings, API key generation and revocation.
  - `syncs.py`: Database-to-database live sync operations.

### 2.2 Streaming Engine (`backend/engine/`)
- **`core.py`**:
  - Process lifecycle management (`ACTIVE_PROCESSES`, `ACTIVE_STOP_EVENTS`, `ACTIVE_TRACKERS`).
  - `ProgressTracker`: Real-time tracking of bytes processed, transfer speed (MB/s), duration, and ETA.
  - Logging infrastructure (`write_log`, `read_log`, log path resolution, and log truncation).
  - Pipe optimization (`optimize_pipe_fd`) to adjust pipe buffer sizes on supported Linux platforms.
- **`compressors.py`**:
  - Speed profile mapping (`extreme`, `balanced`, `safe`, `default`) configuring thread allocations, chunk sizes, and compression levels for `zstd` and `pigz`.
- **`pipeline.py`**:
  - **`QueueReader(io.RawIOBase)`**: Custom memory-efficient stream class wrapping a Python `queue.Queue`.
  - Utilizes `deque` and `memoryview` to slice chunks without allocating duplicate string/byte arrays, yielding near-zero CPU overhead in the Python runtime.
  - Streams directly into `boto3` multipart uploaders, Azure blob append/block streams, GCS upload writers, and BunnyCDN HTTP streams.
- **`storage.py`**:
  - Cloud storage driver abstraction: `get_s3_client`, `list_storage_objects`, `get_storage_object_size`, `delete_remote_backup`, and `prune_schedule_backups`.
- **`tasks.py`**:
  - Coordinates high-level workflows: `do_backup()`, `do_restore()`, and `do_sync()`.
  - Handles exception teardown, remote incomplete archive cleanup on failure, and notification dispatch.
- **`drivers/`**:
  - `mongo.py`: MongoDB database engine driver using `mongodump` and `mongorestore`. Strips target database from URI to prevent CLI flag collision.
  - `postgres.py`: PostgreSQL driver using `pg_dump` and `pg_restore` (v18 client compatible).
  - `solr.py`: Apache Solr core backup and restore driver via Solr HTTP Core API.

### 2.3 Frontend Architecture (`frontend/index.html`)
- **Single Page Application (SPA):** Lightweight vanilla JavaScript without frontend build pipelines or heavy frameworks.
- **State Management:** In-memory objects (`allHistoryItems`, `filteredHistoryItems`, `expandedHistoryRows = new Set()`) synchronized with backend polling intervals.
- **Lucide Icons:** Client-side vector icons initialized via `lucide.createIcons()`.
- **Modular Sections:** Page switching via CSS visibility (`.page.active`). Includes Dashboard, Databases, Storage, Backups, History, Schedules, Restores, and Settings.

## 3. Key Design Patterns & Engineering Decisions

1. **Zero-Disk Pipelining Pattern:**
   - Instead of `DB Dump -> Local File -> Compress -> Local File -> Cloud Upload`, the architecture executes `DB Dump Process -> Stdout Pipe -> Compressor Subprocess -> Stdout Pipe -> QueueReader -> Cloud Multipart Stream`.
   - Result: 0 bytes of disk space utilized for intermediate storage.

2. **Memory-Bounded Queue Pattern:**
   - The queue maximum size is strictly calculated as `chunk_size * queue_max` (16MB in Safe mode up to 512MB in Extreme mode).
   - If the network slows down, the dump/compression subprocess blocks on write backpressure rather than ballooning RAM.

3. **Dual Data Persistence & Graceful Migration:**
   - The application transitioned from flat JSON files (`data/*.json`) to an ACID SQLite database (`backupvault.db`) with WAL mode.
   - `DatabaseManager` automatically detects unmigrated JSON files and imports them to SQLite on startup, preserving seamless compatibility with existing deployments.

4. **Transparent Secrets Encryption Pattern:**
   - All connection strings, passwords, and API keys are stored with an `enc:` prefix denoting Fernet AES-128-CBC + HMAC encryption.
   - The decrypt function handles unencrypted legacy strings transparently without breaking older configurations.

5. **Headless Automation Fallback:**
   - `get_current_user` extracts authentication from headers (`X-API-Key`, `Authorization: Bearer`), but automatically defaults to the system admin user when headers are omitted, supporting frictionless direct URL automation (`https://autobackup.dev.travelswitch.com/api/...`).
