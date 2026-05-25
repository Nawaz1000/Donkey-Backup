from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional
import os
from utils import get_current_user, read_json, write_json

router = APIRouter()

class SettingsUpdate(BaseModel):
    slack_webhook: Optional[str] = ""
    teams_webhook: Optional[str] = ""
    telegram_webhook: Optional[str] = ""

@router.get("/")
def get_settings(user=Depends(get_current_user)):
    if not os.path.exists("data/settings.json"):
        return {}
    return read_json("data/settings.json")

@router.post("/")
def update_settings(req: SettingsUpdate, user=Depends(get_current_user)):
    data = req.dict()
    write_json("data/settings.json", data)
    return {"message": "Settings updated successfully"}
