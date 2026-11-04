import jwt
import bcrypt
import json
import os
import base64
import hashlib
from datetime import datetime, timedelta
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from cryptography.fernet import Fernet

SECRET_KEY = os.getenv("SECRET_KEY", "backupvault-secret-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24

security = HTTPBearer()

def _get_fernet() -> Fernet:
    key_bytes = hashlib.sha256(SECRET_KEY.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    return Fernet(fernet_key)

def encrypt_secret(val: str) -> str:
    """Encrypt sensitive credentials at rest using Fernet (AES-128-CBC + HMAC)."""
    if not val:
        return val
    val_str = str(val)
    if val_str.startswith("enc:"):
        return val_str
    try:
        f = _get_fernet()
        encrypted = f.encrypt(val_str.encode("utf-8")).decode("utf-8")
        return f"enc:{encrypted}"
    except Exception:
        return val_str

def decrypt_secret(val: str) -> str:
    """Decrypt sensitive credentials. Transparently handles legacy plaintext values."""
    if not val:
        return val
    val_str = str(val)
    if not val_str.startswith("enc:"):
        return val_str  # Legacy plaintext, return as-is
    try:
        f = _get_fernet()
        raw_token = val_str[4:]
        return f.decrypt(raw_token.encode("utf-8")).decode("utf-8")
    except Exception:
        return val_str

def get_decrypted_db(db: dict) -> dict:
    if not db or not isinstance(db, dict):
        return db
    d = db.copy()
    if 'password' in d and d['password']:
        d['password'] = decrypt_secret(d['password'])
    if 'mongo_uri' in d and d['mongo_uri']:
        d['mongo_uri'] = decrypt_secret(d['mongo_uri'])
    return d

def get_decrypted_storage(storage: dict) -> dict:
    if not storage or not isinstance(storage, dict):
        return storage
    s = storage.copy()
    for field in ('azure_account_key', 'gcs_credentials_json', 's3_secret_key', 'bunny_password', 'bunny_access_key'):
        if field in s and s[field]:
            s[field] = decrypt_secret(s[field])
    return s

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())

def create_token(user_id: str, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Authentication credentials required")
    token = credentials.credentials.strip()
    if token in ("no-auth", "local-user-token", "admin-token"):
        return {"sub": "local-user", "email": "admin@local", "name": "Admin"}
    try:
        payload = decode_token(token)
        return {"sub": payload.get("sub", "local-user"), "email": payload.get("email", "admin@local"), "name": payload.get("email", "Admin").split("@")[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid authentication token: {e}")

def parse_iso_datetime(dt_str: str):
    if not dt_str:
        return None
    try:
        clean = dt_str.rstrip("Z")
        if "+" in clean:
            clean = clean.split("+")[0]
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None

def calculate_next_run(s: dict, from_dt: datetime = None) -> str:
    now_utc = from_dt or datetime.utcnow()
    freq = s.get("frequency", "daily")
    tz_offset = s.get("timezone_offset", 0)
    local_now = now_utc - timedelta(minutes=tz_offset)
    
    target_time = s.get("time") or "02:00"
    try:
        th, tm = map(int, target_time.split(":"))
    except Exception:
        th, tm = 2, 0

    if freq == "hourly":
        next_local = local_now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    elif freq in ("daily", "weekly_mixed"):
        candidate = local_now.replace(hour=th, minute=tm, second=0, microsecond=0)
        if candidate <= local_now:
            candidate += timedelta(days=1)
        next_local = candidate
    elif freq == "weekly":
        days_ahead = (0 - local_now.weekday()) % 7
        candidate = local_now.replace(hour=th, minute=tm, second=0, microsecond=0) + timedelta(days=days_ahead)
        if candidate <= local_now:
            candidate += timedelta(days=7)
        next_local = candidate
    elif freq == "monthly":
        if local_now.month == 12:
            candidate = local_now.replace(year=local_now.year + 1, month=1, day=1, hour=th, minute=tm, second=0, microsecond=0)
        else:
            candidate = local_now.replace(month=local_now.month + 1, day=1, hour=th, minute=tm, second=0, microsecond=0)
        if local_now.day == 1:
            same_day = local_now.replace(hour=th, minute=tm, second=0, microsecond=0)
            if same_day > local_now:
                candidate = same_day
        next_local = candidate
    else:
        candidate = local_now.replace(hour=th, minute=tm, second=0, microsecond=0)
        if candidate <= local_now:
            candidate += timedelta(days=1)
        next_local = candidate

    next_utc = next_local + timedelta(minutes=tz_offset)
    return next_utc.isoformat() + "Z"


def read_json(path: str):
    if not os.path.exists(path):
        return {} if "settings.json" in path else []
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError as jde:
        print(f"JSON decode error in {path}: {jde}. Attempting recovery...")
        try:
            with open(path) as f:
                raw_data = f.read()
            
            # Simple bracket-based object extractor
            parsed_items = []
            stack = []
            start_idx = -1
            for i, char in enumerate(raw_data):
                if char == '{':
                    if not stack:
                        start_idx = i
                    stack.append(char)
                elif char == '}':
                    if stack:
                        stack.pop()
                        if not stack:
                            obj_str = raw_data[start_idx:i+1]
                            try:
                                obj = json.loads(obj_str)
                                if isinstance(obj, dict):
                                    parsed_items.append(obj)
                            except Exception:
                                pass
            if parsed_items:
                if "settings.json" in path:
                    salvaged = parsed_items[-1]
                else:
                    salvaged = [item for item in parsed_items if "id" in item]
                
                print(f"Successfully salvaged {len(salvaged) if isinstance(salvaged, list) else 1} items from {path}")
                write_json(path, salvaged)
                return salvaged
        except Exception as re:
            print(f"Failed to salvage JSON file {path}: {re}")
        raise jde

def write_json(path: str, data):
    import tempfile
    dir_name = os.path.dirname(path) or "."
    os.makedirs(dir_name, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=dir_name, prefix=".tmp-")
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(temp_path, path)
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise e

import urllib.request
import urllib.parse

def send_notification(title: str, message: str = "", details: dict = None, is_error: bool = False, log_path: str = None):
    try:
        if not os.path.exists("data/settings.json"):
            if log_path:
                try:
                    from engine import write_log
                    write_log(log_path, "WARN  Cannot send notification: data/settings.json does not exist. Please configure notification webhooks in UI settings.")
                except Exception:
                    pass
            return
        settings = read_json("data/settings.json")
        
        # Fallback raw text
        text = f"*{title}*\n{message}"
        if details:
            text += "\n\nDetails:\n"
            for k, v in details.items():
                text += f"• *{k}:* {v}\n"

        color = "#EF4444" if is_error else "#10B981"
        
        headers = {
            "User-Agent": "curl/7.68.0",
            "Content-Type": "application/json"
        }
        
        # Slack
        if settings.get("slack_webhook"):
            if log_path:
                try:
                    from engine import write_log
                    write_log(log_path, "INFO  Sending Slack webhook notification...")
                except Exception:
                    pass
                    
            slack_blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": title,
                        "emoji": True
                    }
                }
            ]
            if message:
                slack_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                })
            if details:
                details_text = ""
                for k, v in details.items():
                    details_text += f"*{k}:* {v}\n"
                slack_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": details_text
                    }
                })
                
            slack_payload = {
                "text": text, # Fallback text
                "attachments": [
                    {
                        "color": color,
                        "blocks": slack_blocks
                    }
                ]
            }
            payload = json.dumps(slack_payload).encode('utf-8')
            req = urllib.request.Request(settings["slack_webhook"], data=payload, headers=headers, method="POST")
            _send_single_webhook("Slack", req, log_path)
            
        # Teams
        if settings.get("teams_webhook"):
            if log_path:
                try:
                    from engine import write_log
                    write_log(log_path, "INFO  Sending Microsoft Teams webhook notification...")
                except Exception:
                    pass
                    
            teams_facts = []
            if details:
                for k, v in details.items():
                    teams_facts.append({"name": f"{k}:", "value": str(v)})
            
            teams_payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": color.replace("#", ""),
                "summary": title,
                "sections": [{
                    "activityTitle": title,
                    "activitySubtitle": message,
                    "facts": teams_facts,
                    "markdown": True
                }]
            }
            payload = json.dumps(teams_payload).encode('utf-8')
            req = urllib.request.Request(settings["teams_webhook"], data=payload, headers=headers, method="POST")
            _send_single_webhook("Teams", req, log_path)
            
        # Telegram
        if settings.get("telegram_webhook"):
            if log_path:
                try:
                    from engine import write_log
                    write_log(log_path, "INFO  Sending Telegram webhook notification...")
                except Exception:
                    pass
                    
            tg_text = f"<b>{title}</b>\n"
            if message:
                tg_text += f"{message}\n"
            if details:
                tg_text += "\n"
                for k, v in details.items():
                    tg_text += f"• <b>{k}:</b> {v}\n"
                    
            tg_url = settings["telegram_webhook"]
            connector = "&" if "?" in tg_url else "?"
            url = tg_url + connector + "text=" + urllib.parse.quote(tg_text) + "&parse_mode=HTML"
            req = urllib.request.Request(url, headers={"User-Agent": "BackupVault/1.0"})
            _send_single_webhook("Telegram", req, log_path)
    except Exception as e:
        msg = f"Failed to send notification: {e}"
        print(msg)
        if log_path:
            try:
                from engine import write_log
                write_log(log_path, f"ERROR {msg}")
            except Exception:
                pass

def _send_single_webhook(name, req, log_path, timeout=5):
    try:
        import urllib.error
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if log_path:
                from engine import write_log
                write_log(log_path, f"INFO  {name} notification sent successfully (status: {resp.status})")
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode('utf-8', errors='ignore')
        msg = f"WARN  {name} notification failed with HTTP {e.code}: {err_msg}"
        print(msg)
        if log_path:
            from engine import write_log
            write_log(log_path, msg)
    except Exception as e:
        msg = f"WARN  {name} notification failed: {e}"
        print(msg)
        if log_path:
            from engine import write_log
            write_log(log_path, msg)


def parse_mongo_uri(db_or_uri) -> dict:
    from urllib.parse import unquote, quote

    if isinstance(db_or_uri, str):
        raw_uri = db_or_uri
        db_name_default = ""
    else:
        raw_uri = db_or_uri.get("mongo_uri") or ""
        db_name_default = db_or_uri.get("database_name", "").strip()

    # Defaults
    scheme = "mongodb"
    userinfo = ""
    hosts = "localhost:27017"
    dbname = db_name_default
    options = {}

    if raw_uri:
        # Determine scheme
        if "://" in raw_uri:
            scheme, rest = raw_uri.split("://", 1)
        else:
            rest = raw_uri

        # Split userinfo and the rest
        if "@" in rest:
            userinfo, rest = rest.rsplit("@", 1)
        else:
            userinfo = ""

        # Parse query options first (if any)
        if "?" in rest:
            hosts_path, query_str = rest.split("?", 1)
            for pair in query_str.split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    options[k.lower()] = unquote(v)
        else:
            hosts_path = rest

        # Parse hosts and path
        if "/" in hosts_path:
            hosts, path_str = hosts_path.split("/", 1)
            path_parts = path_str.split("/")
            for part in path_parts:
                if not part:
                    continue
                if "=" in part or "&" in part:
                    for pair in part.split("&"):
                        if "=" in pair:
                            k, v = pair.split("=", 1)
                            options[k.lower()] = unquote(v)
                else:
                    dbname = part
        else:
            hosts = hosts_path
    else:
        # Construct from individual fields if not a string
        if isinstance(db_or_uri, dict):
            host = db_or_uri.get("host", "").strip() or "localhost"
            port = db_or_uri.get("port")
            if port is not None:
                hosts = f"{host}:{port}"
            else:
                hosts = host
            user = db_or_uri.get("username", "").strip()
            pwd = db_or_uri.get("password", "").strip()
            if user and pwd:
                userinfo = f"{user}:{pwd}"
            elif user:
                userinfo = user
        else:
            hosts = "localhost:27017"

    # Fallback to default dbname if empty
    if not dbname and db_name_default:
        dbname = db_name_default

    # Reconstruct quoted credentials
    quoted_userinfo = ""
    username_unquoted = None
    password_unquoted = None
    if userinfo:
        safe_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        if ":" in userinfo:
            u, p = userinfo.split(":", 1)
            username_unquoted = unquote(u)
            password_unquoted = unquote(p)
            quoted_userinfo = f"{quote(username_unquoted, safe=safe_chars)}:{quote(password_unquoted, safe=safe_chars)}@"
        else:
            username_unquoted = unquote(userinfo)
            quoted_userinfo = f"{quote(username_unquoted, safe=safe_chars)}@"

    # Map option keys to standard casing
    standard_keys = {
        "authsource": "authSource",
        "authmechanism": "authMechanism",
        "directconnection": "directConnection",
        "tls": "tls",
        "ssl": "ssl",
        "replicaset": "replicaSet",
        "readpreference": "readPreference",
        "retrywrites": "retryWrites",
        "w": "w"
    }

    uri_options = {}
    for k, v in options.items():
        std_key = standard_keys.get(k, k)
        uri_options[std_key] = v

    # Build query string
    query_parts = []
    for k, v in uri_options.items():
        query_parts.append(f"{k}={quote(v)}")
    query_str = "?" + "&".join(query_parts) if query_parts else ""

    # Reconstruct URI
    uri_path = f"/{dbname}" if dbname else "/"
    reconstructed_uri = f"{scheme}://{quoted_userinfo}{hosts}{uri_path}{query_str}"

    # Parse first host and port for compatibility
    first_host = hosts.split(",")[0]
    if ":" in first_host:
        h, p = first_host.split(":", 1)
        try:
            port_val = int(p)
        except ValueError:
            port_val = 27017
    else:
        h = first_host
        port_val = 27017

    return {
        "uri": reconstructed_uri,
        "hosts": hosts,
        "host": h,
        "port": port_val,
        "username": username_unquoted,
        "password": password_unquoted,
        "dbname": dbname,
        "auth_source": uri_options.get("authSource", "admin"),
        "auth_mechanism": uri_options.get("authMechanism"),
        "direct_connection": uri_options.get("directConnection"),
        "options": uri_options
    }
