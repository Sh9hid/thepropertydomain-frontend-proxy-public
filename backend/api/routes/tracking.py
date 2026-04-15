"""Email tracking pixel and click endpoints — no auth, must be fast."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response

from api.routes._deps import SessionDep

router = APIRouter()

# 1x1 transparent GIF
_PIXEL = (
    b"\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00"
    b"\xff\xff\xff\x00\x00\x00\x21\xf9\x04\x00\x00\x00\x00"
    b"\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"\x44\x01\x00\x3b"
)


@router.get("/api/t/o/{tracking_id}")
async def track_open(tracking_id: str, request: Request, session: SessionDep = None):
    """Track email open via invisible pixel."""
    try:
        from services.email_tracking import record_open
        ip = request.client.host if request.client else None
        ua = request.headers.get("user-agent")
        await record_open(session, tracking_id, ip=ip, user_agent=ua)
    except Exception:
        pass  # Never fail — this is a tracking endpoint
    return Response(content=_PIXEL, media_type="image/gif", headers={
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
    })


@router.get("/api/t/c/{tracking_id}")
async def track_click(tracking_id: str, u: str = "", request: Request = None, session: SessionDep = None):
    """Track link click and redirect."""
    try:
        from services.email_tracking import record_click
        ip = request.client.host if request.client else None
        ua = request.headers.get("user-agent")
        await record_click(session, tracking_id, link_url=u, ip=ip, user_agent=ua)
    except Exception:
        pass
    target = u or "/"
    return RedirectResponse(url=target, status_code=302)
