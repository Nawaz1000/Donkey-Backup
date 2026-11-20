# Active Context: BackupVault

## 1. Current Development Focus
The project is focused on performance optimization, user experience refinement, and headless automation of database operations.

Recent sessions accomplished:
1. **Target Database Auto-Creation & Auto-Registration on Restore:**
   - Implemented `ensure_pg_database_exists()` in `backend/engine/drivers/postgres.py` to automatically detect if target databases exist on PostgreSQL instances and execute `CREATE DATABASE` if missing.
   - Enhanced `do_restore` in `backend/engine/tasks.py` to auto-create and auto-register newly named databases directly into BackupVault's database collection (`databases.json` / SQLite) so they immediately appear in the UI.
   - Fixed archive type detection in `run_pg_restore` so custom format (`-Fc`) archives always use `pg_restore` with native section flags, preventing binary stream corruption from text-based `filter_worker`.
   - Prevented fatal connection errors (missing database, connection refused) from being masked as harmless warnings in `backend/engine/pipeline.py`.
2. **Flexible API Storage & Database Resolution:**
   - Updated `backend/routers/backups.py` so `storage_id` is optional in `BackupCreate` (auto-defaults to user's default storage) and accepts either UUID, name, or provider type (`azure`, `s3`, `bunny`, `gcs`).
   - Allowed `database_id` and `target_database_id` to resolve flexibly by UUID, name, or engine type, and seamlessly support restoring into new databases by name.
3. **Interactive & Zero-Dependency CLI Script (`scripts/backup_cli.py`):**
   - Upgraded `backup_cli.py` with interactive prompts for database and storage destinations when arguments are omitted.
   - Added zero-dependency standard library `urllib` fallback so the CLI runs on any Python 3 environment without needing `pip install requests`.
   - Added listing subcommands (`list-dbs`, `list-storages`, `list-backups`).
4. **Interactive Backup History Accordion Drawer (`frontend/index.html`):**
   - Implemented an expandable drawer below each history table row.
   - Surfaced a 4-quadrant metadata grid: Backup Strategy, Cloud Storage Destination (with 1-click clipboard copy), Database Profile, and Performance Metrics.
   - Pushed under commit `0fb95b1` to `github ftp_dev`.
2. **MongoDB Database Name Conflict Resolution (`backend/engine/drivers/mongo.py` & `backend/utils.py`):**
   - Resolved exit code 1 in `mongodump` by implementing `strip_mongo_uri_database()` to prevent conflict between database in the URI and the `--db` CLI parameter.
   - Verified on live instance with 41.19 GB database backup completing successfully.
   - Pushed under commit `823d48d`.
3. **Authentication Security Hardening:**
   - Removed credentials display from the login UI.
   - Removed the quick sign-in bypass button.
   - Disabled public user registration (`POST /api/auth/register` returns 403 Forbidden).
   - Pushed under commit `941f009`.
4. **Headless Automation & Open Direct URL Access:**
   - Configured `get_current_user` in `backend/utils.py` to allow direct unauthenticated access defaulting to the admin profile for seamless cURL and CI/CD script execution.
   - Authored complete documentation in `API_DOCUMENTATION.md` and CLI script in `scripts/backup_cli.py`.

## 2. Active Branch & Version Control State
- **Active Branch:** `ftp_dev`
- **Upstream Remotes:**
  - `github`: `git@github.com:Nawaz1000/Donkey-Backup.git` (synchronized with commit `0fb95b1`)
  - `origin`: `https://gitea.futuretravelplatform.com/FTP_ORG/Auto-Backup.git` (requires manual CLI authentication by user for push)
- **Working Tree:** Clean; application source files are untouched during Memory Bank initialization.

## 3. Recent Decisions & Architectural Principles
- **Memory Bank Compliance (`.agents/rules/devops.md`):**
  - All subsequent development sessions must read all core files in `memory-bank/` prior to proposing or implementing code changes.
  - After completing significant tasks or when instructed with **update memory bank**, all files must be kept synchronized.
- **Zero Local Disk Writes:**
  - Avoid creating intermediate dump files on the container filesystem. Maintain the streaming pipeline (`QueueReader`, pipes, and in-memory buffers).
- **Graceful Security Fallbacks:**
  - Keep the dual-mode API accessible: direct URL requests work out-of-the-box, while secure headers (`X-API-Key` or `Authorization: Bearer`) are respected when provided.

## 4. Next Steps & Upcoming Priorities
- Keep Memory Bank documents up to date after any feature or bug fix.
- Synchronize `ftp_dev` to Gitea remote when user provides credentials.
- Monitor long-running backups for memory stability and line-rate saturation.
- TBD: Explore additional database drivers (e.g., MySQL / MariaDB) if requested.
