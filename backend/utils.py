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

def send_notification(title: str, message: str, is_error: bool = False):
    try:
        if not os.path.exists("data/settings.json"):
            return
        settings = read_json("data/settings.json")
        
        text = f"*{title}*\n{message}"
        color = "EF4444" if is_error else "10B981"
        
        # Slack
        if settings.get("slack_webhook"):
            req = urllib.request.Request(settings["slack_webhook"], json.dumps({"text": text}).encode('utf-8'), {"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
            
        # Teams
        if settings.get("teams_webhook"):
            payload = {"title": title, "text": message, "themeColor": color}
            req = urllib.request.Request(settings["teams_webhook"], json.dumps(payload).encode('utf-8'), {"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
            
        # Telegram
        if settings.get("telegram_webhook"):
            url = settings["telegram_webhook"] + "&text=" + urllib.parse.quote(text)
            urllib.request.urlopen(url, timeout=5)
    except Exception as e:
        print(f"Failed to send notification: {e}")