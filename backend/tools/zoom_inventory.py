import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "configured"
    return f"{value[:4]}...{value[-4:]}"


def _load_env() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(env_path)


def _env(name: str) -> str:
    return (os.getenv(name) or os.getenv(f"\ufeff{name}") or "").strip()


def _token_request() -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    client_id = _env("ZOOM_CLIENT_ID")
    client_secret = _env("ZOOM_CLIENT_SECRET")
    account_id = _env("ZOOM_ACCOUNT_ID")
    token_url = _env("ZOOM_TOKEN_URL") or "https://zoom.us/oauth/token"

    if not client_id or not client_secret or not account_id:
        return None, {
            "ok": False,
            "stage": "config",
            "error": "Missing one or more required env vars: ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET, ZOOM_ACCOUNT_ID",
        }

    query = urllib.parse.urlencode(
        {"grant_type": "account_credentials", "account_id": account_id}
    )
    auth = b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        f"{token_url}?{query}",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8") or "{}")
            return payload, None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(detail) if detail else {}
        except json.JSONDecodeError:
            parsed = {"raw": detail}
        return None, {
            "ok": False,
            "stage": "token",
            "status": exc.code,
            "error": parsed,
            "client_id": _mask(client_id),
            "account_id": _mask(account_id),
        }
    except urllib.error.URLError as exc:
        return None, {
            "ok": False,
            "stage": "token",
            "status": 0,
            "error": str(exc.reason),
            "client_id": _mask(client_id),
            "account_id": _mask(account_id),
        }


def _request(token: str, url: str) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8", errors="ignore")
            parsed = json.loads(raw) if raw else {}
            return {
                "ok": True,
                "status": response.status,
                "keys": list(parsed.keys()) if isinstance(parsed, dict) else [],
                "counts": {
                    key: len(value)
                    for key, value in (parsed.items() if isinstance(parsed, dict) else [])
                    if isinstance(value, list)
                },
                "sample": parsed,
            }
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = {"raw": raw}
        return {"ok": False, "status": exc.code, "error": parsed}
    except urllib.error.URLError as exc:
        return {"ok": False, "status": 0, "error": str(exc.reason)}


def _safe_excerpt(value: Any) -> Any:
    if isinstance(value, dict):
        trimmed: Dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= 12:
                break
            if isinstance(item, list):
                trimmed[key] = item[:2]
            else:
                trimmed[key] = item
        return trimmed
    if isinstance(value, list):
        return value[:2]
    return value


def _endpoints(date_from: str, date_to: str) -> List[Tuple[str, str]]:
    return [
        ("account", "https://api.zoom.us/v2/accounts/me"),
        ("users_me", "https://api.zoom.us/v2/users/me"),
        ("users", "https://api.zoom.us/v2/users?page_size=10"),
        ("meetings", "https://api.zoom.us/v2/users/me/meetings?page_size=10&type=scheduled"),
        ("webinars", "https://api.zoom.us/v2/users/me/webinars?page_size=10"),
        (
            "cloud_recordings_user",
            f"https://api.zoom.us/v2/users/me/recordings?page_size=10&from={date_from}&to={date_to}",
        ),
        (
            "cloud_recordings_account",
            f"https://api.zoom.us/v2/accounts/me/recordings?page_size=10&from={date_from}&to={date_to}",
        ),
        ("phone_settings", "https://api.zoom.us/v2/phone/settings"),
        ("phone_users", "https://api.zoom.us/v2/phone/users?page_size=10"),
        ("phone_numbers", "https://api.zoom.us/v2/phone/numbers?page_size=10"),
        ("calling_plans", "https://api.zoom.us/v2/phone/calling_plans"),
        ("call_logs", "https://api.zoom.us/v2/phone/call_logs?page_size=10"),
        (
            "sms_sessions",
            f"https://api.zoom.us/v2/phone/sms/sessions?page_size=10&from={date_from}&to={date_to}",
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Zoom API inventory probe.")
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD. Defaults to 30 days ago.")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--json", action="store_true", help="Print full JSON output.")
    args = parser.parse_args()

    _load_env()

    today = date.today()
    date_to = args.date_to or today.isoformat()
    date_from = args.date_from or (today - timedelta(days=30)).isoformat()

    token_payload, token_error = _token_request()
    if token_error:
        print(json.dumps({"token": token_error}, indent=2))
        return 1

    token = str((token_payload or {}).get("access_token") or "")
    results: Dict[str, Any] = {
        "token": {
            "ok": True,
            "token_type": token_payload.get("token_type"),
            "expires_in": token_payload.get("expires_in"),
            "scope": token_payload.get("scope"),
        },
        "date_range": {"from": date_from, "to": date_to},
        "endpoints": {},
    }

    for name, url in _endpoints(date_from, date_to):
        results["endpoints"][name] = _request(token, url)

    call_logs = (
        results["endpoints"].get("call_logs", {}).get("sample", {}).get("call_logs", [])
        if results["endpoints"].get("call_logs", {}).get("ok")
        else []
    )
    if call_logs:
        first_call_log_id = str(call_logs[0].get("id") or "").strip()
        if first_call_log_id:
            results["endpoints"]["call_log_detail"] = _request(
                token, f"https://api.zoom.us/v2/phone/call_logs/{first_call_log_id}"
            )
            results["endpoints"]["call_log_recording"] = _request(
                token, f"https://api.zoom.us/v2/phone/call_logs/{first_call_log_id}/recordings"
            )

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    print("Zoom API inventory")
    print(f"Token: ok | type={results['token'].get('token_type')} | expires_in={results['token'].get('expires_in')}")
    print(f"Scope: {results['token'].get('scope') or '(empty)'}")
    print(f"Date range: {date_from} -> {date_to}")
    print("")
    for name, result in results["endpoints"].items():
        if result.get("ok"):
            counts = result.get("counts") or {}
            counts_text = ", ".join(f"{key}={value}" for key, value in counts.items()) if counts else "no list counts"
            print(f"[OK]   {name}: HTTP {result.get('status')} | {counts_text}")
            sample = _safe_excerpt(result.get("sample"))
            print(json.dumps(sample, indent=2))
        else:
            print(f"[FAIL] {name}: HTTP {result.get('status')}")
            print(json.dumps(result.get("error"), indent=2))
        print("")
    return 0


if __name__ == "__main__":
    sys.exit(main())
