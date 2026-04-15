
import os
import json
import urllib.request
from base64 import b64encode
from dotenv import load_dotenv

load_dotenv(dotenv_path="backend/.env")

def get_zoom_token():
    client_id = os.getenv("ZOOM_CLIENT_ID")
    client_secret = os.getenv("ZOOM_CLIENT_SECRET")
    account_id = os.getenv("ZOOM_ACCOUNT_ID")
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={account_id}"
    auth = b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"}
    req = urllib.request.Request(url, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode()).get("access_token")
    except Exception as e:
        print(f"❌ TOKEN ERROR: {e}")
        return None

def scan_account_for_sms_license(token):
    print("📋 Scanning Zoom Account for API Entitlements...")
    
    # 1. Check Account Settings (The "Master" switches)
    url = "https://api.zoom.us/v2/phone/settings"
    headers = {"Authorization": f"Bearer {token}"}
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            print("✅ Account Settings Found.")
            # Looking for 'sms' or 'api' toggles at the top level
            print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"❌ FAILED TO GET SETTINGS: {e}")

    # 2. Check calling plans (To see if SMS is included in the $28 plan)
    print("\n📋 Checking Calling Plans...")
    url = "https://api.zoom.us/v2/phone/calling_plans"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            plans = data.get("calling_plans", [])
            if plans:
                for p in plans:
                    print(f"   - Plan: {p.get('name')} | Type: {p.get('type')}")
            else:
                print("🚨 No calling plans found. This is very strange for a paid account.")
    except Exception as e:
        print(f"❌ FAILED TO GET PLANS: {e}")

if __name__ == "__main__":
    token = get_zoom_token()
    if token:
        scan_account_for_sms_license(token)
