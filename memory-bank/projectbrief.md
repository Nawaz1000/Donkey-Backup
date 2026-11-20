# Project Brief: BackupVault

## 1. Project Purpose & Vision
**BackupVault** (also developed under *Auto-Backup* / *Donkey-Backup*) is a self-hosted, enterprise-grade, lightweight database backup and disaster recovery manager. It provides high-performance, automated, zero-disk streaming backups, scheduled archival, and cross-database restores for **PostgreSQL**, **MongoDB**, and **Apache Solr** directly to cloud object storage providers.

## 2. Core Objectives
- **Zero-Disk Streaming Architecture:** Eliminate disk space exhaustion during large database backups (100GB+ scale) by streaming data directly from database dump utilities through memory buffers to remote cloud storage without creating intermediate local dump files.
- **Multi-Cloud Storage Flexibility:** Support first-class integration with Azure Blob Storage, AWS S3 (and S3-compatible endpoints such as MinIO, Wasabi, Cloudflare R2), Google Cloud Storage, BunnyCDN Edge Storage, and Local Disks.
- **Low Overhead & Memory-Bounded Safety:** Protect host systems from memory bloat and Out-Of-Memory (OOM) crashes via bounded in-memory queues and selectable speed profiles (Safe, Balanced, Extreme).
- **Comprehensive Administration:** Offer a modern, dark-themed Single Page Application (SPA) dashboard for monitoring, manual operations, schedule management, and history exploration.
- **Headless Automation Ready:** Enable frictionless DevOps and CI/CD automation through direct URL REST API endpoints and a dedicated command-line utility (`scripts/backup_cli.py`).
- **Security-at-Rest:** Encrypt all sensitive connection strings, database passwords, and cloud storage access keys at rest using symmetric Fernet encryption.

## 3. Core Features
- **Database Engine Support:**
  - **MongoDB:** `mongodump` / `mongorestore` database tools integration, collection filtering, and URI query parameter sanitization.
  - **PostgreSQL:** `pg_dump` / `pg_restore` v18 client integration with directory and custom archive formats, multi-threaded worker options, and table filtering.
  - **Apache Solr:** Core API backup and restore handlers.
- **Storage Integrations:**
  - Azure Blob Storage (via `azure-storage-blob`).
  - Google Cloud Storage (via `google-cloud-storage`).
  - AWS S3 & S3-compatible providers (via `boto3`).
  - BunnyCDN Edge Storage (via REST and S3-compatible regional endpoints).
  - Local filesystem storage directory.
- **Pipeline & Performance Engine:**
  - In-memory `QueueReader` implementing `io.RawIOBase` using `memoryview` wrappers to eliminate array slicing memory copies.
  - Dynamic compression tuning for Zstandard (`zstd`) and parallel gzip (`pigz`).
  - Night-time auto-acceleration to maximize bandwidth usage during off-peak windows (12:00 AM – 6:00 AM).
  - Disk-flush backpressure handling for WiredTiger (MongoDB) and PostgreSQL during restores.
- **Scheduling & Retention:**
  - Flexible frequencies: Hourly, Daily, Weekly, Monthly, and Weekly Mixed (Full backup on Sunday, incremental backups on weekdays).
  - Automated retention pruning to discard stale archives and enforce storage quotas.
- **User Interface & Observability:**
  - Responsive dark-themed SPA (Vanilla JavaScript, HTML5, CSS3, Lucide icons).
  - Real-time progress tracking displaying throughput (MB/s), duration, percentage, and ETA.
  - Interactive History table with expandable detail drawers displaying backup strategies, storage paths, telemetry, and live execution logs.
- **Authentication & Security:**
  - JWT token-based authentication.
  - Persistent API Key management.
  - Open Direct URL access mode for headless script integration.
  - Fernet encryption for credentials stored in SQLite.

## 4. Project Scope & Boundaries
- **In Scope:**
  - Full, collection-level, and incremental database dumps.
  - Direct streaming to and from cloud object storage.
  - Direct DB-to-DB migration and synchronization.
  - Web UI management, scheduled job execution, and logging.
  - Headless API access and CLI operations.
- **Out of Scope:**
  - Change Data Capture (CDC) continuous real-time replication (e.g., Debezium-style stream processing).
  - Non-supported database engines (e.g., MySQL, Redis, Cassandra — marked as TBD for future extension).
  - Multi-tenant cloud SaaS billing or subscription metering.
