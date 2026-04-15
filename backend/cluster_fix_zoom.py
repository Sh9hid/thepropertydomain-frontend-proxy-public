
import os
import json
import urllib.request
from base64 import b64encode
from dotenv import load_dotenv

load_dotenv(dotenv_path="backend/.env")

def global_user_sms_hail_mary():
    client_id = os.getenv("ZOOM_CLIENT_ID")
    client_secret = os.getenv("ZOOM_CLIENT_SECRET")
    account_id = os.getenv("ZOOM_ACCOUNT_ID")
    host = "https://us01pbx.zoom.us" # Using the verified cluster host
    
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={account_id}"
    auth = b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"}
    
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers, method="POST")) as response:
            token = json.loads(response.read().decode()).get("access_token")
            
            # This is the "Gold Standard" endpoint for User-based SMS in 2026
            sms_url = f"{host}/v2/phone/users/me/sms"
            print(f"🚀 FINAL HAIL MARY: {sms_url}")
            
            payload = {
                "from": "+61485857881",
                "to": "+61485857881", 
                "message": "ZOOM FINAL FIX: Woonona Hub Global User SMS Verified."
            }
            
            req = urllib.request.Request(
                sms_url, 
                data=json.dumps(payload).encode(), 
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req) as res:
                print("✅ SMS SUCCESS!")
                print(json.loads(res.read().decode()))

    except urllib.error.HTTPError as e:
        print(f"❌ API ERROR ({e.code}): {e.read().decode()}")

if __name__ == "__main__":
    global_user_sms_hail_mary()
