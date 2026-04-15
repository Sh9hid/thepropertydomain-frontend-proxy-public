from __future__ import annotations

import os

from fastapi import Request
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles

from core.admin_auth import session_cookie_settings, session_from_cookie
from core.config import API_KEY


def _static_auth_required() -> bool:
    return os.getenv("STATIC_FILES_REQUIRE_AUTH", "false").lower() == "true"


class ProtectedStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        if not _static_auth_required():
            return await super().get_response(path, scope)

        request = Request(scope)
        header_value = request.headers.get("x-api-key")
        query_value = request.query_params.get("api_key")
        cookie_name = session_cookie_settings()["key"]
        session = session_from_cookie(request.cookies.get(cookie_name))
        if header_value != API_KEY and query_value != API_KEY and not session:
            return PlainTextResponse("Forbidden", status_code=403)
        return await super().get_response(path, scope)
