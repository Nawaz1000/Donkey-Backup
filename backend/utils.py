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
    with open(path) as f:
        return json.load(f)

def write_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

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