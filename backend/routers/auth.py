from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
import uuid
import os
from utils import hash_password, verify_password, create_token, read_json, write_json, get_current_user

router = APIRouter()

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "KXbTv6n7d6")

def ensure_admin_user():
    """Ensure the default administrator account exists with configured credentials."""
    try:
        users = read_json("data/users.json")
        admin = next(
            (u for u in users if u.get("username", "").lower() == ADMIN_USERNAME.lower() or u.get("email", "").lower() in (ADMIN_USERNAME.lower(), f"{ADMIN_USERNAME.lower()}@local")),
            None
        )
        if not admin:
            admin = {
                "id": str(uuid.uuid4()),
                "name": "Administrator",
                "username": ADMIN_USERNAME,
                "email": ADMIN_USERNAME,
                "password": hash_password(ADMIN_PASSWORD),
            }
            users.append(admin)
            write_json("data/users.json", users)
        else:
            admin["password"] = hash_password(ADMIN_PASSWORD)
            admin["username"] = ADMIN_USERNAME
            write_json("data/users.json", users)
        return admin
    except Exception as e:
        print(f"Error ensuring admin user: {e}")
        return None

class RegisterRequest(BaseModel):
    name: str
    email: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str

@router.post("/register")
def register(req: RegisterRequest):
    users = read_json("data/users.json")
    if any(u["email"].lower() == req.email.lower() or u.get("username", "").lower() == req.email.lower() for u in users):
        raise HTTPException(400, "Email or username already registered")
    user = {
        "id": str(uuid.uuid4()),
        "name": req.name,
        "email": req.email,
        "username": req.email.split("@")[0] if "@" in req.email else req.email,
        "password": hash_password(req.password),
    }
    users.append(user)
    write_json("data/users.json", users)
    token = create_token(user["id"], user["email"])
    return {"token": token, "user": {"id": user["id"], "name": user["name"], "email": user["email"], "username": user.get("username", user["email"])}}

@router.post("/login")
def login(req: LoginRequest):
    login_id = req.email.strip().lower()

    # Direct match for configured admin credentials
    if login_id in (ADMIN_USERNAME.lower(), f"{ADMIN_USERNAME.lower()}@local", f"{ADMIN_USERNAME.lower()}@autobackup.local") and req.password == ADMIN_PASSWORD:
        admin = ensure_admin_user()
        if admin:
            token = create_token(admin["id"], admin.get("email", ADMIN_USERNAME))
            return {
                "token": token,
                "user": {
                    "id": admin["id"],
                    "name": admin.get("name", "Administrator"),
                    "email": admin.get("email", ADMIN_USERNAME),
                    "username": ADMIN_USERNAME
                }
            }

    users = read_json("data/users.json")
    user = next((u for u in users if u.get("email", "").lower() == login_id or u.get("username", "").lower() == login_id), None)
    if not user or not verify_password(req.password, user["password"]):
        raise HTTPException(401, "Invalid credentials")
    token = create_token(user["id"], user.get("email", login_id))
    return {
        "token": token,
        "user": {
            "id": user["id"],
            "name": user.get("name", "User"),
            "email": user.get("email", login_id),
            "username": user.get("username", login_id)
        }
    }

@router.get("/me")
def me(user=Depends(get_current_user)):
    return {"user": user}
