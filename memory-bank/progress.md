# Progress: BackupVault

## 1. What Works (Completed Features)

### 1.1 Database Drivers & Operations
- [x] **MongoDB Support:**
  - Automated `mongodump` streaming with Zstandard (`zstd`) / `pigz` compression.
  - Collection-level filtering and continuous oplog backup support.
  - Sanitization of connection URIs via `strip_mongo_uri_database` to eliminate `--db` parameter collisions.
  - Disk-flush backpressure with WiredTiger engine during restore operations.
- [x] **PostgreSQL Support:**
  - `pg_dump` and `pg_restore` integration using PostgreSQL v18 client binaries.
  - Automatic detection and creation of non-existent target databases on restore (`ensure_pg_database_exists`).
  - Automatic registration of restored new databases into BackupVault's database collection.
  - Safe binary custom archive (`-Fc`) streaming with native section restoration and error diagnostics.
  - Support for custom format (`-Fc`), plain SQL, and directory formats.
  - Multithreaded dump/restore workers (`--jobs`).
  - Table selection and schema-only / data-only modes.
- [x] **Apache Solr Support:**
  - Core-level snapshot and backup/restore via Solr Core HTTP APIs.
- [x] **Cross-Database Direct Sync:**
  - Direct live sync between databases without intermediate storage files.

### 1.2 Storage Integrations
- [x] **Azure Blob Storage:** Block and append streaming upload with SAS and access key authentication.
- [x] **Google Cloud Storage (GCS):** Streaming upload via service account JSON credentials.
- [x] **Amazon S3 & S3-Compatible:** Multipart parallel streaming (`boto3`) with custom endpoint support (MinIO, Wasabi, Cloudflare R2, DigitalOcean Spaces).
- [x] **BunnyCDN Edge Storage:** High-speed REST and S3-compatible streaming with automated regional endpoint detection.
- [x] **Local Filesystem:** Local directory storage support.

### 1.3 Streaming Engine & Optimization
- [x] **Zero-Disk Pipelining:** Direct pipeline from database subprocess stdout into compression into cloud storage multipart stream.
- [x] **Zero-Copy `QueueReader`:** Memory pointer offsets and Python `memoryview` stream wrappers eliminating array slice copying.
- [x] **Speed Profiles:** Configurable throughput modes (`extreme`, `balanced`, `safe`, `default`) with bounded RAM protection (16MB to 512MB max queue memory).
- [x] **Night-Time Auto-Scaling:** Automated elevation to "Extreme" profile between 12:00 AM and 6:00 AM.
- [x] **Live Progress & Metrics:** Real-time calculation of transfer speed (`speed_mbps`), processed bytes (`processed_mb`), total archive size (`total_mb`), and duration.

### 1.4 Persistence & Security
- [x] **SQLite Document Store:** ACID-compliant `data/backupvault.db` in WAL mode replacing flat JSON files with auto-migration.
- [x] **Credential Encryption:** Fernet (AES-128-CBC + HMAC) encryption for all database passwords, URIs, and cloud access keys.
- [x] **Authentication Hardening:** JWT token auth, disabled public registration, removed login page credential leaks.
- [x] **Dual Authentication Mode:** Persistent API keys (`X-API-Key`) and open direct URL access fallback for headless cURL and automation.

### 1.5 User Interface & Management
- [x] **Dark-Themed SPA Dashboard:** Overview of backup metrics, active connections, storage quotas, and recent operations.
- [x] **Database & Storage Management:** Interactive connection modals with immediate "Test Connection" verification.
- [x] **Automated Scheduler:** Recurring schedules (Hourly, Daily, Weekly, Monthly, Weekly Mixed) with retention pruning.
- [x] **Expandable History Drawer:** Accordion row expansion showing backup strategy, storage destination with 1-click path copy, database details, performance metrics, and collapsible execution logs.

### 1.6 Tooling & Automation
- [x] **Command-Line Interface:** `scripts/backup_cli.py` for automated backup and restore workflows with polling and exit codes.
- [x] **API Key Generator:** `scripts/create_api_key.py` for headless credential generation.
- [x] **API Documentation:** Comprehensive documentation in `API_DOCUMENTATION.md`.

---

## 2. What's Left to Build / Pending Tasks
- [ ] **MySQL / MariaDB Support (TBD):** Driver implementation using `mysqldump` / `mariadb-dump` streaming.
- [ ] **Direct Cloud-to-Cloud Storage Sync (TBD):** Synchronizing backup archives between different cloud storage providers (e.g. S3 to Azure Blob).
- [ ] **Automated Test Restores (TBD):** Scheduled sandbox restore testing to verify archive recoverability.
- [ ] **Notification Webhooks (TBD):** Webhook channels for Slack, Discord, Microsoft Teams, and custom HTTP endpoints.

---

## 3. Known Issues & Operational Considerations
- **Gitea Push Authentication:** Pushing to `origin` requires manual terminal authentication due to remote server credentials.
- **Open API vs. Strict Security:** Direct URL access currently defaults to admin access without credentials to support frictionless cURL scripts. A future toggle in Settings could enforce strict API key validation if the user desires zero-trust restriction.
- **Large Solr Cores:** Solr backup endpoints rely on core replication handlers; extremely large index cores may require tuned HTTP timeouts on slow disks.
