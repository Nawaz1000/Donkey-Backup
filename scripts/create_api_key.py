#!/usr/bin/env python3
"""
BackupVault - API Key Generator Script
Generates a persistent API Key for Terraform, Python scripts, and CI/CD pipelines.
"""

import sys
import os
import getpass
import requests

DEFAULT_URL = os.getenv("BACKUPVAULT_URL", "https://autobackup.dev.travelswitch.com")

def main():
    print("=" * 60)
    print("       BackupVault - Generate Automation API Key")
    print("=" * 60)
    
    url = input(f"BackupVault URL [{DEFAULT_URL}]: ").strip() or DEFAULT_URL
    url = url.rstrip("/")

    email = input("Admin Email: ").strip()
    if not email:
        print("[!] Email is required.")
        sys.exit(1)

    password = getpass.getpass("Admin Password: ")
    if not password:
        print("[!] Password is required.")
        sys.exit(1)

    key_name = input("Key Description / Name [Automation Key]: ").strip() or "Automation Key"

    print(f"\n[*] Authenticating with {url}...")
    try:
        login_res = requests.post(
            f"{url}/api/auth/login",
            json={"email": email, "password": password},
            timeout=15
        )
    except Exception as e:
        print(f"[!] Network error connecting to {url}: {e}")
        sys.exit(1)

    if not login_res.ok:
        print(f"[!] Authentication failed ({login_res.status_code}): {login_res.text}")
        sys.exit(1)

    data = login_res.json()
    token = data.get("token") or data.get("access_token")
    if not token:
        print(f"[!] Could not extract token from response: {data}")
        sys.exit(1)

    print("[*] Creating persistent API key...")
    try:
        key_res = requests.post(
            f"{url}/api/settings/api-keys",
            headers={"Authorization": f"Bearer {token}"},
            json={"name": key_name},
            timeout=15
        )
    except Exception as e:
        print(f"[!] Network error creating key: {e}")
        sys.exit(1)

    if not key_res.ok:
        print(f"[!] Failed to generate API key ({key_res.status_code}): {key_res.text}")
        sys.exit(1)

    key_data = key_res.json()
    api_key = key_data["api_key"]

    print("\n" + "=" * 60)
    print("   SUCCESS! YOUR API KEY HAS BEEN CREATED")
    print("=" * 60)
    print(f"Key Name : {key_data.get('name')}")
    print(f"API Key  : {api_key}")
    print("=" * 60)
    print("[!] Important: Copy this key now! It will not be shown again.")
    print("Use this key in your scripts/Terraform with the header:")
    print(f"    X-API-Key: {api_key}")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
