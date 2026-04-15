from __future__ import annotations

import datetime
import os
import sys
from pathlib import Path

import core.config as core_config
from app_factory import create_app
from core.config import ALLOWED_ORIGIN_REGEX, ALLOWED_ORIGINS, GENERATED_REPORTS_ROOT, LISTING_PHOTOS_ROOT, STOCK_ROOT, STREETVIEW_IMAGE_ROOT, SYDNEY_TZ
from core.events import event_manager
from core.protected_static import ProtectedStaticFiles
from core.security import authorize_websocket_connection
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from runtime.health import _check_database_health, _check_redis_health
from runtime.routes import register_routers
from scalar_fastapi import get_scalar_api_reference

sys.setrecursionlimit(3000)

async def websocket_endpoint(websocket: WebSocket):
    if not authorize_websocket_connection(websocket):
        await websocket.close(code=4401)
        return
    await event_manager.connect(websocket)
    try:
        await websocket.send_json(
            {
                "type": "SYSTEM_HEALTH",
                "data": {
                    "status": "connected",
                    "database": "online",
                    "timestamp": datetime.datetime.now(SYDNEY_TZ).isoformat(),
                },
            }
        )
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        event_manager.disconnect(websocket)


async def scalar_html(request: Request):
    return get_scalar_api_reference(
        openapi_url=request.app.openapi_url,
        title=request.app.title + " - API Reference",
    )


async def healthcheck(request: Request):
    database_check = getattr(request.app.state, "database_health_check", _check_database_health)
    redis_check = getattr(request.app.state, "redis_health_check", _check_redis_health)
    database_status, database_error = await database_check()
    redis_status, redis_error = await redis_check()
    payload = {
        "status": "ok" if database_status == "ok" and redis_status == "ok" else "error",
        "app_env": core_config.APP_ENV,
        "services": {"database": database_status, "redis": redis_status},
    }
    if database_error:
        payload["database_error"] = database_error
    if redis_error:
        payload["redis_error"] = redis_error
    status_code = 200 if payload["status"] == "ok" else 503
    return JSONResponse(content=payload, status_code=status_code)


async def livecheck():
    return {"status": "ok"}


def _mount_static(app: FastAPI) -> None:
    if Path(STOCK_ROOT).exists():
        app.mount("/stock-images", ProtectedStaticFiles(directory=STOCK_ROOT), name="stock-images")

    STREETVIEW_IMAGE_ROOT.mkdir(parents=True, exist_ok=True)
    app.mount("/streetview_images", ProtectedStaticFiles(directory=str(STREETVIEW_IMAGE_ROOT)), name="streetview-images")

    if os.path.exists(STOCK_ROOT):
        app.mount("/stock_photos", ProtectedStaticFiles(directory=str(STOCK_ROOT)), name="stock-photos")

    GENERATED_REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    app.mount("/api/forms/download", ProtectedStaticFiles(directory=str(GENERATED_REPORTS_ROOT)), name="forms-download")

    LISTING_PHOTOS_ROOT.mkdir(parents=True, exist_ok=True)
    app.mount("/listing_photos", ProtectedStaticFiles(directory=str(LISTING_PHOTOS_ROOT)), name="listing-photos")


def create_web_app() -> FastAPI:
    app = create_app(
        runtime_role=os.getenv("RUNTIME_ROLE", "web"),
        title="Property Intelligence Core",
        docs_url=None,
        redoc_url=None,
    )
    app.state.database_health_check = _check_database_health
    app.state.redis_health_check = _check_redis_health
    app.add_api_websocket_route("/ws/events", websocket_endpoint)
    app.add_api_route("/docs", scalar_html, include_in_schema=False)
    app.add_api_route("/health", healthcheck, include_in_schema=False)
    app.add_api_route("/livez", livecheck, include_in_schema=False)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_origin_regex=ALLOWED_ORIGIN_REGEX,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _mount_static(app)
    register_routers(app)
    return app


app = create_web_app()
