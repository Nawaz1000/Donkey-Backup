from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import asynccontextmanager
import uvicorn
import json
import os

from routers import auth, databases, backups, storage, schedules

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize data files if not exist
    for f, default in [
        ("data/users.json", []),
        ("data/databases.json", []),
        ("data/backups.json", []),
        ("data/storages.json", []),
        ("data/schedules.json", []),
    ]:
        os.makedirs("data", exist_ok=True)
        if not os.path.exists(f):
            with open(f, "w") as fp:
                json.dump(default, fp)
    yield

app = FastAPI(title="BackupVault API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(databases.router, prefix="/api/databases", tags=["databases"])
app.include_router(backups.router, prefix="/api/backups", tags=["backups"])
app.include_router(storage.router, prefix="/api/storage", tags=["storage"])
app.include_router(schedules.router, prefix="/api/schedules", tags=["schedules"])

@app.get("/api/health")
def health():
    return {"status": "ok", "app": "BackupVault"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
