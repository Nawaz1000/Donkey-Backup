from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
import uuid
from utils import hash_password, verify_password, create_token, read_json, write_json

router = APIRouter()

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
    if any(u["email"] == req.email for u in users):
        raise HTTPException(400, "Email already registered")
    user = {
        "id": str(uuid.uuid4()),
        "name": req.name,
        "email": req.email,
        "password": hash_password(req.password),
    }
    users.append(user)
    write_json("data/users.json", users)
    token = create_token(user["id"], user["email"])
    return {"token": token, "user": {"id": user["id"], "name": user["name"], "email": user["email"]}}

@router.post("/login")
def login(req: LoginRequest):
    users = read_json("data/users.json")
    user = next((u for u in users if u["email"] == req.email), None)
    if not user or not verify_password(req.password, user["password"]):
        raise HTTPException(401, "Invalid credentials")
    token = create_token(user["id"], user["email"])
    return {"token": token, "user": {"id": user["id"], "name": user["name"], "email": user["email"]}}

@router.get("/me")
def me(credentials=None):
    from fastapi import Depends
    from utils import get_current_user
    # handled via dependency in routes that need it
    pass
