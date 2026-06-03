import jwt
import bcrypt
import json
import os
from datetime import datetime, timedelta
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

SECRET_KEY = os.getenv("SECRET_KEY", "backupvault-secret-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24

security = HTTPBearer()

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
    return {"sub": "local-user", "email": "admin@local"}

def read_json(path: str):
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

def send_notification(title: str, message: str, is_error: bool = False, log_path: str = None):
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
        
        text = f"*{title}*\n{message}"
        color = "EF4444" if is_error else "10B981"
        
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
            payload = json.dumps({"text": text}).encode('utf-8')
            req = urllib.request.Request(settings["slack_webhook"], data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if log_path:
                    try:
                        from engine import write_log
                        write_log(log_path, f"INFO  Slack notification sent successfully (status: {resp.status})")
                    except Exception:
                        pass
            
        # Teams
        if settings.get("teams_webhook"):
            if log_path:
                try:
                    from engine import write_log
                    write_log(log_path, "INFO  Sending Microsoft Teams webhook notification...")
                except Exception:
                    pass
            teams_payload = {
                "text": text
            }
            payload = json.dumps(teams_payload).encode('utf-8')
            req = urllib.request.Request(settings["teams_webhook"], data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if log_path:
                    try:
                        from engine import write_log
                        write_log(log_path, f"INFO  Teams notification sent successfully (status: {resp.status})")
                    except Exception:
                        pass
            
        # Telegram
        if settings.get("telegram_webhook"):
            if log_path:
                try:
                    from engine import write_log
                    write_log(log_path, "INFO  Sending Telegram webhook notification...")
                except Exception:
                    pass
            tg_url = settings["telegram_webhook"]
            connector = "&" if "?" in tg_url else "?"
            url = tg_url + connector + "text=" + urllib.parse.quote(text)
            req = urllib.request.Request(url, headers={"User-Agent": "BackupVault/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                if log_path:
                    try:
                        from engine import write_log
                        write_log(log_path, f"INFO  Telegram notification sent successfully (status: {resp.status})")
                    except Exception:
                        pass
    except Exception as e:
        msg = f"Failed to send notification: {e}"
        print(msg)
        if log_path:
            try:
                from engine import write_log
                write_log(log_path, f"ERROR {msg}")
            except Exception:
                pass


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