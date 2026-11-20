# Technical Context: BackupVault

## 1. Technology Stack

### Backend
- **Language / Runtime:** Python 3.11 (Python 3.11-slim base image)
- **Web Framework:** FastAPI (v0.111.0)
- **ASGI Server:** Uvicorn (v0.29.0, standard extras)
- **Data Validation & Schemas:** Pydantic (v2.7.1)
- **Database & Persistence:**
  - SQLite 3 (WAL journal mode, multithreaded connection manager)
  - `data/backupvault.db`
- **Security & Cryptography:**
  - `bcrypt` (v4.1.3): Password hashing
  - `PyJWT` (v2.8.0): JWT creation and signature verification
  - `cryptography` (>=42.0.0): Fernet symmetric encryption for credentials at rest
- **Storage & Cloud Provider SDKs:**
  - `boto3` (v1.34.106): Amazon S3 & S3-compatible providers (MinIO, R2, Wasabi, BunnyCDN S3)
  - `azure-storage-blob` (v12.19.1): Azure Blob Storage SDK
  - `google-cloud-storage` (v2.16.0): Google Cloud Storage Client SDK
  - `requests` (v2.31.0): BunnyCDN REST storage APIs and Solr Core HTTP APIs
  - `pymongo` (v4.7.2): MongoDB Python driver for metadata and collection inspection

### Native System Binaries & Utilities
- **`postgresql-client-18`**: Official PostgreSQL client utilities (`pg_dump`, `pg_restore`, `psql`) installed from the official PGDG repository.
- **`mongodb-database-tools` (v100.10.0)**: Official MongoDB tools package (`mongodump`, `mongorestore`, `bsondump`).
- **Compression Engines:**
  - `zstd`: Zstandard multithreaded compression utility.
  - `pigz`: Parallel implementation of gzip for multicore compression.

### Frontend
- **Stack:** Vanilla JavaScript (ES6+), Semantic HTML5, CSS3 with CSS Custom Properties.
- **Dependencies / Libraries:**
  - Lucide Icons (CDN delivery via `lucide.min.js`)
  - No build pipeline, node modules, or bundler required (pure static files).

### Containerization & Deployment
- **Container Base:** Multi-stage `Dockerfile` based on `python:3.11-slim`.
- **Process Manager:** `supervisord` running both Nginx and Uvicorn within a single container.
- **Reverse Proxy:** Nginx serving static assets and proxying `/api/` traffic to `127.0.0.1:8000`.
- **Docker Compose:** `docker-compose.yml` exposing port 80 (frontend/reverse proxy) and port 8000 (direct API), mapping `./data:/app/data`.

## 2. Environment Variables & Configuration

| Environment Variable | Default Value | Description |
|---|---|---|
| `SECRET_KEY` | `backupvault-secret-change-in-production` | Secret seed used for JWT signing and Fernet symmetric key derivation. |
| `SQLITE_DB_PATH` | `data/backupvault.db` | Absolute or relative path to the persistent SQLite database. |
| `ADMIN_USERNAME` | `admin` | Default system administrator username for authentication. |
| `ADMIN_PASSWORD` | `KXbTv6n7d6` | Default system administrator password for authentication. |
| `BACKUPVAULT_URL` | `https://autobackup.dev.travelswitch.com` | Base URL used by CLI scripts and remote automation. |
| `PYTHONUNBUFFERED` | `1` | Forces unbuffered stdout and stderr streams for accurate real-time logging. |

## 3. Storage Destination Configuration Parameters

### Azure Blob Storage
- **Account Name:** Storage account identifier.
- **Account Key:** Primary or secondary access key.
- **Container Name:** Target container for backup archives.

### Google Cloud Storage (GCS)
- **Bucket Name:** Target GCS bucket.
- **Credentials JSON:** Service account JSON with `Storage Object Admin` or `Storage Object Creator` roles.

### Amazon S3 & S3-Compatible
- **Bucket Name:** S3 bucket identifier.
- **Access Key ID & Secret Access Key:** AWS IAM or provider credentials.
- **Region:** AWS region string (e.g., `us-east-1`, `eu-west-1`).
- **Endpoint URL (Optional):** Custom endpoint for MinIO, Cloudflare R2, Wasabi, or DigitalOcean Spaces.

### BunnyCDN Edge Storage
- **Storage Zone Name:** Bunny storage zone identifier.
- **Password / Access Key:** Read/Write password from "FTP & API Access" (not the zone name).
- **Region Host (Optional):** Regional endpoint URL (e.g., `https://la.storage.bunnycdn.com`) or automatic regional endpoint discovery.
- **Folder Name (Optional):** Subfolder prefix within the zone.

## 4. Development Environment & Tooling Constraints
- **Platform / OS:** Deployed in production on Linux containers; locally developed on Windows environment (`c:\Users\nawaz\OneDrive\Desktop\IH-Projects\Auto-Backup`).
- **Git Remotes:**
  - `github`: `git@github.com:Nawaz1000/Donkey-Backup.git`
  - `origin`: `https://gitea.futuretravelplatform.com/FTP_ORG/Auto-Backup.git`
- **Active Branch:** `ftp_dev`
- **Testing & Verification:**
  - Backend syntax checks: `python -m py_compile ...`
  - Frontend script checks: `node --check ...`
  - CLI operations: `python scripts/backup_cli.py ...`
