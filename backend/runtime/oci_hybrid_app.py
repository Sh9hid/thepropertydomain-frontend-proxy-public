from __future__ import annotations

import os
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

from runtime.oci_core_api import router as oci_core_router


HOP_BY_HOP_HEADERS = {
    "connection",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _frontend_dist_dir() -> Path:
    explicit = (os.getenv("HYBRID_FRONTEND_DIST") or "").strip()
    if explicit:
        return Path(explicit).expanduser()
    return _project_root() / "frontend" / "dist"


def _upstream_base_url() -> str:
    return (os.getenv("HYBRID_UPSTREAM_URL") or "https://ai-rea-vcij.onrender.com").rstrip("/")


def _core_mode() -> str:
    return (os.getenv("HYBRID_CORE_MODE") or "sqlite").strip().lower()


def _resolve_static_candidate(path: str) -> Path:
    dist_dir = _frontend_dist_dir().resolve()
    requested = (path or "").lstrip("/") or "index.html"
    candidate = (dist_dir / requested).resolve()
    if dist_dir not in candidate.parents and candidate != dist_dir:
        return dist_dir / "index.html"
    if candidate.exists() and candidate.is_file():
        return candidate
    return dist_dir / "index.html"


async def proxy_to_upstream(request: Request, upstream_path: str) -> Response:
    upstream_url = f"{_upstream_base_url()}{upstream_path}"
    if request.url.query:
        upstream_url = f"{upstream_url}?{request.url.query}"

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() != "host"
    }
    body = await request.body()

    async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
        upstream_response = await client.request(
            request.method,
            upstream_url,
            headers=headers,
            content=body,
        )

    response_headers = {
        key: value
        for key, value in upstream_response.headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }
    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=response_headers,
    )


def create_hybrid_app() -> FastAPI:
    app = FastAPI(title="The Property Domain OCI Hybrid")
    if _core_mode() != "proxy_only":
        app.include_router(oci_core_router)

    @app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
    async def proxy_api(path: str, request: Request):
        return await proxy_to_upstream(request, f"/api/{path}")

    @app.api_route("/listing_photos/{path:path}", methods=["GET", "HEAD"])
    async def proxy_listing_photos(path: str, request: Request):
        return await proxy_to_upstream(request, f"/listing_photos/{path}")

    @app.api_route("/stock-images/{path:path}", methods=["GET", "HEAD"])
    async def proxy_stock_images(path: str, request: Request):
        return await proxy_to_upstream(request, f"/stock-images/{path}")

    @app.api_route("/stock_photos/{path:path}", methods=["GET", "HEAD"])
    async def proxy_stock_photos(path: str, request: Request):
        return await proxy_to_upstream(request, f"/stock_photos/{path}")

    @app.api_route("/streetview_images/{path:path}", methods=["GET", "HEAD"])
    async def proxy_streetview_images(path: str, request: Request):
        return await proxy_to_upstream(request, f"/streetview_images/{path}")

    @app.get("/{path:path}")
    async def frontend(path: str):
        dist_dir = _frontend_dist_dir()
        if not dist_dir.exists():
            raise HTTPException(status_code=503, detail=f"Frontend dist not found at {dist_dir}")
        return FileResponse(_resolve_static_candidate(path))

    return app


app = create_hybrid_app()
