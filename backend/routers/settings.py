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

@router.post("/test")
def test_notifications(req: SettingsUpdate, user=Depends(get_current_user)):
    import urllib.request
    import urllib.parse
    import json
    
    title = "Test Connection Notification 🔔"
    message = "This is a test notification from BackupVault. Your webhook channel is working correctly!"
    
    headers = {
        "User-Agent": "BackupVault/1.0",
        "Content-Type": "application/json"
    }
    
    errors = []
    successes = []
    
    if req.slack_webhook:
        try:
            payload = json.dumps({"text": f"*{title}*\n{message}"}).encode('utf-8')
            request = urllib.request.Request(req.slack_webhook, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(request, timeout=10) as resp:
                if resp.status not in (200, 201, 204):
                    raise Exception(f"HTTP Status {resp.status}")
            successes.append("Slack")
        except Exception as e:
            errors.append(f"Slack failed: {str(e)}")
            
    if req.teams_webhook:
        try:
            adaptive_card = {
                "type": "message",
                "attachments": [
                    {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.2",
                            "body": [
                                {
                                    "type": "TextBlock",
                                    "text": title,
                                    "weight": "Bolder",
                                    "size": "Medium",
                                    "color": "Good"
                                },
                                {
                                    "type": "TextBlock",
                                    "text": message,
                                    "wrap": True
                                }
                            ]
                        }
                    }
                ]
            }
            payload = json.dumps(adaptive_card).encode('utf-8')
            request = urllib.request.Request(req.teams_webhook, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(request, timeout=10) as resp:
                if resp.status not in (200, 201, 204):
                    raise Exception(f"HTTP Status {resp.status}")
            successes.append("Teams")
        except Exception as e:
            errors.append(f"Teams failed: {str(e)}")
            
    if req.telegram_webhook:
        try:
            text = f"*{title}*\n{message}"
            url = req.telegram_webhook + "&text=" + urllib.parse.quote(text)
            request = urllib.request.Request(url, headers={"User-Agent": "BackupVault/1.0"})
            with urllib.request.urlopen(request, timeout=10) as resp:
                if resp.status not in (200, 201, 204):
                    raise Exception(f"HTTP Status {resp.status}")
            successes.append("Telegram")
        except Exception as e:
            errors.append(f"Telegram failed: {str(e)}")
            
    if not req.slack_webhook and not req.teams_webhook and not req.telegram_webhook:
        return {"success": False, "message": "No webhook URLs provided to test."}
        
    if errors:
        return {
            "success": False,
            "message": f"Test completed with errors. Success: {', '.join(successes) or 'None'}. Errors: {'; '.join(errors)}"
        }
        
    return {"success": True, "message": f"Test notification sent successfully to: {', '.join(successes)}!"}
