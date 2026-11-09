import os
import json
import sqlite3
import hashlib
import secrets
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional

DB_PATH = os.getenv("SQLITE_DB_PATH", "data/backupvault.db")

COLLECTION_MAP = {
    "users": "users",
    "users.json": "users",
    "databases": "databases",
    "databases.json": "databases",
    "backups": "backups",
    "backups.json": "backups",
    "storages": "storages",
    "storages.json": "storages",
    "schedules": "schedules",
    "schedules.json": "schedules",
    "syncs": "syncs",
    "syncs.json": "syncs",
    "restores": "restores",
    "restores.json": "restores",
    "settings": "settings",
    "settings.json": "settings",
}

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()
    _local = threading.local()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db_path: str = DB_PATH):
        if self._initialized:
            return
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._init_sqlite()
        self._migrate_existing_json_files()
        self._initialized = True

    def get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 10000;")
            self._local.conn = conn
        return conn

    def _init_sqlite(self):
        conn = self.get_conn()
        with conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS _schema_meta (
                key TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
            """)
            conn.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                collection TEXT NOT NULL,
                item_id TEXT NOT NULL,
                user_id TEXT,
                data_json TEXT NOT NULL,
                created_at TEXT,
                PRIMARY KEY (collection, item_id)
            );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_coll_user ON collections(collection, user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_coll_created ON collections(collection, created_at);")

            conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key_hash TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                prefix TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used_at TEXT
            );
            """)

    def _migrate_existing_json_files(self):
        """Auto-migrates existing data/*.json files into SQLite on first run."""
        conn = self.get_conn()
        cursor = conn.execute("SELECT applied_at FROM _schema_meta WHERE key = 'json_migration_v1';")
        if cursor.fetchone():
            return

        migration_targets = [
            ("users", ["data/users.json"]),
            ("databases", ["data/databases.json"]),
            ("backups", ["data/backups.json"]),
            ("storages", ["data/storages.json"]),
            ("schedules", ["data/schedules.json"]),
            ("syncs", ["data/syncs.json", "data/syncs.json.bak"]),
            ("restores", ["data/restores.json"]),
            ("settings", ["data/settings.json", "backend/data/settings.json", "backend/data/settings.json.bak"]),
        ]

        for coll, paths in migration_targets:
            for p in paths:
                if os.path.exists(p):
                    try:
                        content = None
                        for enc in ("utf-8", "utf-8-sig", "utf-16", "latin1"):
                            try:
                                with open(p, "r", encoding=enc) as f:
                                    content = f.read().strip()
                                break
                            except Exception:
                                continue
                        if not content:
                            continue
                        parsed = json.loads(content)
                        if parsed:
                            existing = self.read_collection(coll)
                            if not existing:
                                print(f"[DatabaseManager] Migrating existing {p} to SQLite table '{coll}'...")
                                self.write_collection(coll, parsed)
                    except Exception as e:
                        print(f"[DatabaseManager] Warning: failed migrating {p}: {e}")

        with conn:
            conn.execute("INSERT OR REPLACE INTO _schema_meta (key, applied_at) VALUES ('json_migration_v1', ?);", (datetime.utcnow().isoformat() + "Z",))

    def read_collection(self, collection_name: str) -> Any:
        clean_name = COLLECTION_MAP.get(collection_name.lower().split("/")[-1], collection_name)
        conn = self.get_conn()

        if clean_name == "settings":
            cursor = conn.execute("SELECT key, value_json FROM settings;")
            res = {}
            for row in cursor.fetchall():
                try:
                    res[row["key"]] = json.loads(row["value_json"])
                except Exception:
                    res[row["key"]] = row["value_json"]
            return res

        cursor = conn.execute(
            "SELECT data_json FROM collections WHERE collection = ? ORDER BY created_at DESC;",
            (clean_name,)
        )
        items = []
        for row in cursor.fetchall():
            try:
                items.append(json.loads(row["data_json"]))
            except Exception:
                pass
        return items

    def write_collection(self, collection_name: str, data: Any):
        clean_name = COLLECTION_MAP.get(collection_name.lower().split("/")[-1], collection_name)
        conn = self.get_conn()

        with conn:
            if clean_name == "settings":
                if isinstance(data, dict):
                    for k, v in data.items():
                        v_json = json.dumps(v, default=str)
                        conn.execute(
                            "INSERT INTO settings (key, value_json) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json;",
                            (k, v_json)
                        )
                return

            if not isinstance(data, list):
                return

            existing_ids = set()
            for item in data:
                if not isinstance(item, dict):
                    continue
                item_id = str(item.get("id") or "")
                if not item_id:
                    continue
                existing_ids.add(item_id)
                user_id = str(item.get("user_id") or item.get("sub") or "")
                created_at = str(item.get("created_at") or item.get("started_at") or "")
                data_json = json.dumps(item, default=str)
                conn.execute("""
                    INSERT INTO collections (collection, item_id, user_id, data_json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(collection, item_id) DO UPDATE SET
                        user_id = excluded.user_id,
                        data_json = excluded.data_json,
                        created_at = COALESCE(excluded.created_at, collections.created_at);
                """, (clean_name, item_id, user_id, data_json, created_at))

            if existing_ids:
                placeholders = ",".join(["?"] * len(existing_ids))
                conn.execute(
                    f"DELETE FROM collections WHERE collection = ? AND item_id NOT IN ({placeholders});",
                    [clean_name] + list(existing_ids)
                )
            else:
                conn.execute("DELETE FROM collections WHERE collection = ?;", (clean_name,))

    def update_item_atomic(self, collection_name: str, item_id: str, **fields):
        """Atomically updates specific fields of a single record in SQLite without rewriting the whole array."""
        clean_name = COLLECTION_MAP.get(collection_name.lower().split("/")[-1], collection_name)
        conn = self.get_conn()
        with conn:
            cursor = conn.execute(
                "SELECT data_json FROM collections WHERE collection = ? AND item_id = ?;",
                (clean_name, str(item_id))
            )
            row = cursor.fetchone()
            if not row:
                return False
            try:
                item = json.loads(row["data_json"])
            except Exception:
                item = {}
            item.update(fields)
            data_json = json.dumps(item, default=str)
            user_id = str(item.get("user_id") or item.get("sub") or "")
            conn.execute(
                "UPDATE collections SET data_json = ?, user_id = ? WHERE collection = ? AND item_id = ?;",
                (data_json, user_id, clean_name, str(item_id))
            )
            return True

    # ─── API KEY AUTHENTICATION (For Terraform, Python Scripts, CI/CD) ──────────

    def create_api_key(self, user_id: str, name: str = "Default API Key") -> tuple:
        """Generates a secure API key with prefix 'bv_live_' and stores its hash."""
        raw_key = f"bv_live_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        prefix = raw_key[:12] + "..."
        now = datetime.utcnow().isoformat() + "Z"

        conn = self.get_conn()
        with conn:
            conn.execute("""
                INSERT INTO api_keys (key_hash, user_id, name, prefix, created_at, last_used_at)
                VALUES (?, ?, ?, ?, ?, ?);
            """, (key_hash, user_id, name, prefix, now, None))
        return raw_key, prefix

    def verify_api_key(self, raw_key: str) -> Optional[dict]:
        """Validates raw API key and returns the associated user dict if valid."""
        if not raw_key or not raw_key.startswith("bv_live_"):
            return None
        key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        conn = self.get_conn()
        cursor = conn.execute(
            "SELECT user_id FROM api_keys WHERE key_hash = ?;",
            (key_hash,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        user_id = row["user_id"]
        # Update last_used_at
        with conn:
            conn.execute(
                "UPDATE api_keys SET last_used_at = ? WHERE key_hash = ?;",
                (datetime.utcnow().isoformat() + "Z", key_hash)
            )

        # Retrieve user record
        users = self.read_collection("users")
        user = next((u for u in users if u["id"] == user_id), None)
        return user

    def list_api_keys(self, user_id: str) -> List[dict]:
        conn = self.get_conn()
        cursor = conn.execute(
            "SELECT prefix, name, created_at, last_used_at FROM api_keys WHERE user_id = ? ORDER BY created_at DESC;",
            (user_id,)
        )
        return [dict(r) for r in cursor.fetchall()]

    def delete_api_key(self, user_id: str, prefix: str):
        conn = self.get_conn()
        with conn:
            conn.execute(
                "DELETE FROM api_keys WHERE user_id = ? AND prefix = ?;",
                (user_id, prefix)
            )


# Global singleton database manager instance
db_manager = DatabaseManager()
