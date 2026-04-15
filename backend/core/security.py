import os
from typing import Optional

from fastapi import HTTPException, Query, Request, Security, WebSocket

from core.admin_auth import session_cookie_settings, session_from_cookie
from core.config import API_KEY, APP_ENV, DEFAULT_API_KEY, api_key_header


def _has_valid_api_key(value: Optional[str]) -> bool:
    if not value:
        return False
    if bool(API_KEY) and value == API_KEY:
        return True
    # Local/dev compatibility: allow legacy local key aliases so frontend and
    # backend defaults do not silently drift and hide lead data.
    if APP_ENV != "production":
        legacy_aliases = {
            DEFAULT_API_KEY,
            os.getenv("LEGACY_LOCAL_API_KEY", "").strip(),
        }
        if value in {k for k in legacy_aliases if k}:
            return True
    return False


def get_authenticated_user(request: Request) -> Optional[dict]:
    cookie_name = session_cookie_settings()["key"]
    return session_from_cookie(request.cookies.get(cookie_name))


def get_authenticated_admin(request: Request) -> Optional[dict]:
    user = get_authenticated_user(request)
    if user and user.get("role") == "admin":
        return user
    return None


async def get_api_key(
    request: Request,
    header_val: Optional[str] = Security(api_key_header),
    query_val: Optional[str] = Query(None, alias="api_key"),
):
    if _has_valid_api_key(header_val) or _has_valid_api_key(query_val):
        return API_KEY

    if get_authenticated_user(request):
        return "session"

    raise HTTPException(status_code=403, detail="Could not validate credentials")


async def get_optional_api_key(
    request: Request,
    header_val: Optional[str] = Security(api_key_header),
    query_val: Optional[str] = Query(None, alias="api_key"),
):
    if _has_valid_api_key(header_val) or _has_valid_api_key(query_val):
        return API_KEY
    if get_authenticated_user(request):
        return "session"
    return None


async def require_admin_access(
    request: Request,
    header_val: Optional[str] = Security(api_key_header),
    query_val: Optional[str] = Query(None, alias="api_key"),
):
    if _has_valid_api_key(header_val) or _has_valid_api_key(query_val):
        return {"identifier": "api_key", "role": "admin"}

    user = get_authenticated_user(request)
    if user and user.get("role") == "admin":
        return user
    if user:
        raise HTTPException(status_code=403, detail="Admin access required")
    raise HTTPException(status_code=403, detail="Could not validate credentials")


def authorize_websocket_connection(websocket: WebSocket) -> bool:
    query_val = websocket.query_params.get("api_key")
    header_val = websocket.headers.get("x-api-key")
    if _has_valid_api_key(query_val) or _has_valid_api_key(header_val):
        return True

    cookie_name = session_cookie_settings()["key"]
    return session_from_cookie(websocket.cookies.get(cookie_name)) is not None
