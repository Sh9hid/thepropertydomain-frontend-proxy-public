
import os
import json
import urllib.request
from base64 import b64encode
from dotenv import load_dotenv

load_dotenv(dotenv_path="backend/.env")

def deep_debug_zoom_sms():
    client_id = os.getenv("ZOOM_CLIENT_ID")
    client_secret = os.getenv("ZOOM_CLIENT_SECRET")
    account_id = os.getenv("ZOOM_ACCOUNT_ID")
    
    # 1. Get Token and Check Scopes AGAIN
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={account_id}"
    auth = b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"}
    
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers, method="POST")) as response:
            token_data = json.loads(response.read().decode())
            token = token_data.get("access_token")
            scopes = token_data.get("scope", "")
            print(f"📜 Scopes in NEW token: {scopes}")
            
            if "phone:write:sms" not in scopes:
                print("❌ ERROR: 'phone:write:sms' is STILL MISSING from the token. This means you must click 'Add to Account' in the 'Activation' tab of the Zoom Marketplace.")
                return

            # 2. Try the "me" User endpoint
            # This endpoint is the most common for direct user SMS
            sms_url = "https://api.zoom.us/v2/phone/users/me/messages"
            print(f"🚀 Testing 'me' endpoint: {sms_url}")
            
            sms_payload = {
                "from": "+61485857881",
                "to": "+61485857881", # Texting self for test
                "message": "DEEP DEBUG: Woonona Intelligence Hub final check."
            }
            
            sms_headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            req = urllib.request.Request(sms_url, data=json.dumps(sms_payload).encode(), headers=sms_headers, method="POST")
            with urllib.request.urlopen(req) as sms_res:
                print("✅ BOOM! SMS Success via 'me' endpoint.")
                print(json.loads(sms_res.read().decode()))
                
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"❌ API FAILURE ({e.code}): {body}")
        if "10DLC" in body:
            print("💡 REASON: Your account is blocked until A2P 10DLC registration is finished in the Zoom Web Portal.")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")

if __name__ == "__main__":
    deep_debug_zoom_sms()
