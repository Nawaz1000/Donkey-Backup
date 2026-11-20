# Product Context: BackupVault

## 1. Problem Statement & Why This Project Exists
Managing database backups in production environments frequently encounters critical bottlenecks:
1. **Disk Space Exhaustion:** Standard backup tools create full dump archives locally on the host server before uploading them to cloud storage. On large databases (50GB to 100GB+), this requires equal or greater free disk space on the primary disk volume, often causing full-disk outages or crashed services.
2. **High CPU & Memory Spikes:** Compression utilities and multi-threaded copy processes can saturate host resources, degrading production database performance during work hours or causing Out-Of-Memory (OOM) killer crashes on containerized hosts.
3. **Complex Vendor Lock-In:** SaaS database backup tools are often costly, require external agents, or demand broad third-party network access to private database clusters.
4. **Slow Restores:** Restoring large databases traditionally involves downloading massive archives to disk, unpacking them, and executing restore binaries, consuming double the time and disk footprint.

**BackupVault** solves these issues by acting as a self-hosted, lightweight streaming bridge between database clusters and cloud object stores, operating entirely in memory with strictly bounded queues and zero local dump file creation.

## 2. Target Users & Personas
- **DevOps & Platform Engineers:** Setting up resilient, automated backup pipelines, managing storage quotas across multiple cloud providers, and embedding backup steps into CI/CD or Terraform pipelines.
- **Database Administrators (DBAs):** Managing backup schedules, testing cross-environment database restores (e.g., prod-to-staging), verifying index integrity, and reviewing execution logs.
- **System Administrators:** Running self-hosted Docker infrastructure needing a lightweight web UI to monitor backup health without maintaining brittle shell scripts and cron jobs.

## 3. User Experience Goals & Key Workflows

### Workflow 1: Database & Storage Configuration
- Users navigate to **Databases** or **Storage** in the web dashboard.
- Users input connection parameters (PostgreSQL host/credentials, MongoDB connection URI, or Solr base URL; Azure container keys, GCS service account JSON, S3 credentials, or BunnyCDN passwords).
- A single-click "Test Connection" validates reachability and authentication immediately.
- Credentials are automatically encrypted using Fernet upon storage.

### Workflow 2: On-Demand Manual Backup
- Users click "New Backup" from the Dashboard or Backups tab.
- They select the source database, cloud destination, backup method (Full, Incremental, Differential, Cluster), speed profile (Safe, Balanced, Extreme), and indexing mode (Preserve Indexes vs. Skip).
- The backup starts immediately: the dashboard surfaces live metrics including data processed (MB/GB), transfer speed (MB/s), duration, and an animated progress bar.
- Output files are streamed directly into the destination storage folder.

### Workflow 3: Automated Recurring Schedules
- Users create schedules specifying frequency (Hourly, Daily, Weekly, Monthly, or Weekly Mixed).
- Schedules automatically calculate `next_run` timestamps adjusted for local timezone offsets.
- During night-time windows (12:00 AM – 6:00 AM), the engine automatically elevates default jobs to "Extreme" throughput settings to leverage off-peak network bandwidth.
- Automated retention limits prune older backups to preserve cloud storage costs.

### Workflow 4: Direct Cloud-to-DB Restore
- Users initiate a restore from the Restores page or directly from an item in the History drawer.
- The engine queries cloud object metadata to determine archive size, streams the compressed stream down through memory buffers, and pipes it straight into the target database restore binary (`mongorestore` or `pg_restore`).
- No local intermediate files are downloaded to disk.

### Workflow 5: History Auditing & Deep Diagnostics
- The **History** view allows expanding any past backup or restore record via an interactive accordion drawer.
- The drawer displays a 4-quadrant overview: Backup Type & Strategy, Destination & Cloud Storage (with 1-click path copy), Database Details, and Performance & Metrics.
- Users can view real-time or historical execution logs inline to troubleshoot warnings, table skips, or syntax errors.

### Workflow 6: Headless API & CLI Operations
- DevOps pipelines can trigger backups using `curl` or the Python utility (`scripts/backup_cli.py`).
- No complicated handshake or OAuth flow is mandatory: open direct URL endpoints enable simple HTTP calls, with optional persistent API keys for environments requiring authenticated headers.
