import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from sqlalchemy import text

from core.config import RECORDINGS_ROOT
from core.database import _async_session_factory
from services.integrations import _zoom_token

RECORDINGS_DIR = RECORDINGS_ROOT
RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)

async def get_zoom_token():
    """Get OAuth token for Zoom API."""
    account_id = os.getenv("ZOOM_ACCOUNT_ID", "")
    client_id = os.getenv("ZOOM_CLIENT_ID", "")
    client_secret = os.getenv("ZOOM_CLIENT_SECRET", "")
    if not account_id or not client_id or not client_secret:
        return None
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={account_id}"
    auth = (client_id, client_secret)
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, auth=auth)
        if resp.status_code == 200:
            return resp.json().get("access_token")
    return None

async def download_recording(recording_url: str, call_id: str, account: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Download recording from Zoom and save to local storage."""
    token = None
    if account:
        try:
            token = str((_zoom_token(account) or {}).get("access_token") or "").strip() or None
        except Exception as exc:
            logger.warning("Failed to resolve Zoom token from account for recording download: %s", exc)
    if not token:
        token = await get_zoom_token()
    if not token:
        logger.error("Failed to get Zoom token for recording download")
        return None

    file_path = RECORDINGS_DIR / f"call_{call_id}.mp3"
    legacy_path = RECORDINGS_DIR / f"{call_id}.mp3"
    if file_path.exists():
        return str(file_path)
    if legacy_path.exists():
        return str(legacy_path)

    async with httpx.AsyncClient() as client:
        # Note: Zoom recordings often need the access token in the header
        resp = await client.get(recording_url, headers={"Authorization": f"Bearer {token}"}, follow_redirects=True)
        if resp.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(resp.content)
            logger.info(f"Saved recording for call {call_id} to {file_path}")
            return str(file_path)
        else:
            logger.error(f"Failed to download recording for {call_id}: {resp.status_code}")
    
    return None


async def fetch_and_attach_recording(call_id: str) -> dict:
    """Best-effort fetch hook for downstream transcript pipelines."""
    async with _async_session_factory() as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT id, recording_url
                    FROM call_log
                    WHERE id = :call_id OR provider_call_id = :call_id
                    ORDER BY CASE WHEN id = :call_id THEN 0 ELSE 1 END
                    LIMIT 1
                    """
                ),
                {"call_id": call_id},
            )
        ).mappings().first()

    if not row:
        return {"attached": False, "reason": "call_not_found"}

    recording_url = str(row.get("recording_url") or "").strip()
    if not recording_url:
        return {"attached": False, "reason": "missing_recording_url", "call_id": str(row["id"])}

    local_path = await download_recording(recording_url, str(row["id"]))
    if not local_path:
        return {"attached": False, "reason": "download_failed", "call_id": str(row["id"])}

    return {"attached": True, "call_id": str(row["id"]), "local_path": local_path}
