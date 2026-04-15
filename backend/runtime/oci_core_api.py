from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Optional, Union
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, HTTPException, Request, Response, WebSocket
from pydantic import BaseModel

from core.admin_auth import (
    admin_login_ready,
    authenticate_user,
    build_session_token,
    session_cookie_settings,
    session_from_cookie,
)


router = APIRouter()

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
DEFAULT_API_KEY = "HILLS_SECURE_2026_CORE"
JSON_COLUMNS = {
    "potential_contacts",
    "contact_emails",
    "contact_phones",
    "key_details",
    "features",
    "summary_points",
    "next_actions",
    "source_evidence",
    "linked_files",
    "stage_note_history",
    "activity_log",
    "property_images",
    "source_tags",
    "risk_flags",
    "alternate_phones",
    "alternate_emails",
    "contactability_reasons",
    "sale_history",
    "listing_status_history",
    "nearby_sales",
    "deterministic_tags",
    "seller_intent_signals",
    "refinance_signals",
    "source_provenance",
}
LEAD_SEARCH_COLUMNS = (
    "address",
    "owner_name",
    "suburb",
    "canonical_address",
    "postcode",
    "trigger_type",
    "notes",
    "source",
    "contact_phones",
    "contact_emails",
    "source_tags",
)


class LoginRequest(BaseModel):
    identifier: str
    password: str


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _backend_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_sqlite_path() -> Path:
    explicit = (os.getenv("TPD_SQLITE_PATH") or "").strip()
    if explicit:
        return Path(explicit).expanduser()

    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if database_url.lower().startswith("sqlite"):
        raw = database_url.split("sqlite+aiosqlite:///", 1)[-1].split("sqlite:///", 1)[-1].strip()
        if raw:
            return Path(raw).expanduser()

    candidates = (
        _project_root() / "databases" / "leads.db",
        _project_root() / "leads.db",
        _backend_root() / "leads.db",
    )
    for candidate in candidates:
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    return candidates[0]


def _sqlite_connect() -> sqlite3.Connection:
    connection = sqlite3.connect(str(_resolve_sqlite_path()))
    connection.row_factory = sqlite3.Row
    return connection


def _now_sydney() -> dt.datetime:
    return dt.datetime.now(SYDNEY_TZ).replace(microsecond=0)


def _parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value in (None, "", 0, 0.0):
            return None
        parsed = float(value)
        return parsed if parsed else None
    except (TypeError, ValueError):
        return None


def _build_osm_map_tile_url(lat: Any, lng: Any, zoom: int = 16) -> str:
    lat_num = _coerce_float(lat)
    lng_num = _coerce_float(lng)
    if lat_num is None or lng_num is None:
        return ""

    import math

    tiles = 2 ** zoom
    x_tile = int((lng_num + 180.0) / 360.0 * tiles)
    y_tile = int((1.0 - math.asinh(math.tan(math.radians(lat_num))) / math.pi) / 2.0 * tiles)
    return f"https://tile.openstreetmap.org/{zoom}/{x_tile}/{y_tile}.png"


def _hydrate_lead_row(row: Union[sqlite3.Row, dict[str, Any]]) -> dict[str, Any]:
    lead = dict(row)
    for column in JSON_COLUMNS:
        lead[column] = _parse_json_list(lead.get(column))

    main_image = str(lead.get("main_image") or "").strip()
    property_images = [str(item).strip() for item in lead.get("property_images", []) if str(item).strip()]
    primary_visual = main_image or (property_images[0] if property_images else "")
    if primary_visual:
        lead["visual_url"] = primary_visual
        lead["visual_source"] = "listing_photo"
        lead["visual_label"] = "Main listing photo"
        lead["visual_is_fallback"] = False
    else:
        tile = _build_osm_map_tile_url(lead.get("lat"), lead.get("lng"))
        lead["visual_url"] = tile
        lead["visual_source"] = "osm_map_tile" if tile else ""
        lead["visual_label"] = "Map tile fallback" if tile else ""
        lead["visual_is_fallback"] = bool(tile)
    lead.setdefault("street_view_embed_url", "")
    return lead


def _valid_api_key(value: Optional[str]) -> bool:
    candidate = str(value or "").strip()
    if not candidate:
        return False
    live_key = (os.getenv("API_KEY") or DEFAULT_API_KEY).strip()
    return candidate == live_key


def _request_session(request: Request) -> Optional[dict[str, Any]]:
    cookie_name = session_cookie_settings()["key"]
    return session_from_cookie(request.cookies.get(cookie_name))


def _request_has_access(request: Request) -> bool:
    return bool(
        _valid_api_key(request.headers.get("x-api-key"))
        or _valid_api_key(request.query_params.get("api_key"))
        or _request_session(request)
    )


def _websocket_has_access(websocket: WebSocket) -> bool:
    cookie_name = session_cookie_settings()["key"]
    return bool(
        _valid_api_key(websocket.headers.get("x-api-key"))
        or _valid_api_key(websocket.query_params.get("api_key"))
        or session_from_cookie(websocket.cookies.get(cookie_name))
    )


def _require_access(request: Request) -> None:
    if not _request_has_access(request):
        raise HTTPException(status_code=403, detail="Could not validate credentials")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]).lower() for row in rows}


def _count_expression(column: str) -> str:
    return f"COALESCE(CAST({column} AS TEXT), '')"


def _sqlite_like_leads(
    *,
    limit: int,
    offset: int,
    search: Optional[str],
    is_fresh: bool,
    signal_status: Optional[str],
    min_dom: Optional[int],
) -> dict[str, Any]:
    with _sqlite_connect() as conn:
        columns = _table_columns(conn, "leads")
        if not columns:
            return {"leads": [], "total": 0}

        where_parts: list[str] = []
        params: list[Any] = []

        if is_fresh and "created_at" in columns:
            where_parts.append("created_at >= ?")
            params.append((_now_sydney() - dt.timedelta(hours=24)).isoformat())

        if min_dom is not None and "days_on_market" in columns:
            where_parts.append("days_on_market >= ?")
            params.append(int(min_dom))

        normalized_signal_status = str(signal_status or "").strip().upper()
        if normalized_signal_status and "signal_status" in columns:
            where_parts.append("UPPER(COALESCE(signal_status, '')) = ?")
            params.append(normalized_signal_status)

        normalized_search = str(search or "").strip().lower()
        digit_token = "".join(ch for ch in normalized_search if ch.isdigit())
        if normalized_search:
            searchable = [name for name in LEAD_SEARCH_COLUMNS if name in columns]
            if searchable:
                search_fragments = [f"LOWER(COALESCE(CAST({name} AS TEXT), '')) LIKE ?" for name in searchable]
                search_values: list[Any] = [f"%{normalized_search}%"] * len(searchable)
                if digit_token:
                    digit_columns = [name for name in ("address", "canonical_address", "postcode", "contact_phones", "contact_emails") if name in columns]
                    if digit_columns:
                        digit_haystack = " || ' ' || ".join(_count_expression(name) for name in digit_columns)
                        digit_expr = (
                            "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
                            f"LOWER({digit_haystack}), ' ', ''), '-', ''), '(', ''), ')', ''), '+', '') LIKE ?"
                        )
                        search_fragments.append(digit_expr)
                        search_values.append(f"%{digit_token}%")
                where_parts.append("(" + " OR ".join(search_fragments) + ")")
                params.extend(search_values)

        where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        order_parts = [part for part in ("updated_at DESC" if "updated_at" in columns else "", "created_at DESC" if "created_at" in columns else "", "id ASC" if "id" in columns else "") if part]
        order_sql = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""

        total = int(conn.execute(f"SELECT COUNT(*) AS total FROM leads{where_sql}", params).fetchone()[0])
        rows = conn.execute(
            f"SELECT * FROM leads{where_sql}{order_sql} LIMIT ? OFFSET ?",
            [*params, max(1, min(int(limit), 1000)), max(0, int(offset))],
        ).fetchall()
        return {"leads": [_hydrate_lead_row(row) for row in rows], "total": total}


def _get_lead_by_id(lead_id: str) -> Optional[dict[str, Any]]:
    with _sqlite_connect() as conn:
        row = conn.execute("SELECT * FROM leads WHERE id = ? LIMIT 1", (lead_id,)).fetchone()
        return _hydrate_lead_row(row) if row else None


def _compute_analytics() -> dict[str, Any]:
    with _sqlite_connect() as conn:
        columns = _table_columns(conn, "leads")
        if not columns:
            return {
                "total_crm_value": 0,
                "active_leads": 0,
                "avg_heat": 0,
                "withdrawn_count": 0,
                "delta_count": 0,
                "mortgage_cliff_count": 0,
            }

        total_leads = int(conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0])
        active_leads = total_leads
        if "status" in columns:
            active_leads = int(
                conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE LOWER(COALESCE(status, '')) NOT IN ('dropped', 'converted')"
                ).fetchone()[0]
            )

        total_crm_value = 0
        if "estimated_value_mid" in columns:
            total_crm_value = int(conn.execute("SELECT COALESCE(SUM(estimated_value_mid), 0) FROM leads").fetchone()[0] or 0)
        elif "est_value" in columns:
            total_crm_value = int(conn.execute("SELECT COALESCE(SUM(est_value), 0) FROM leads").fetchone()[0] or 0)

        avg_heat = 0
        if "heat_score" in columns:
            avg_heat = float(conn.execute("SELECT COALESCE(AVG(heat_score), 0) FROM leads").fetchone()[0] or 0)

        withdrawn_count = 0
        delta_count = 0
        if "signal_status" in columns:
            withdrawn_count = int(
                conn.execute("SELECT COUNT(*) FROM leads WHERE UPPER(COALESCE(signal_status, '')) = 'WITHDRAWN'").fetchone()[0]
            )
            delta_count = int(
                conn.execute("SELECT COUNT(*) FROM leads WHERE UPPER(COALESCE(signal_status, '')) = 'DELTA'").fetchone()[0]
            )

        mortgage_cliff_count = 0
        if "trigger_type" in columns:
            mortgage_cliff_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM leads
                    WHERE LOWER(COALESCE(trigger_type, '')) LIKE '%mortgage%'
                       OR LOWER(COALESCE(trigger_type, '')) LIKE '%cliff%'
                    """
                ).fetchone()[0]
            )

        return {
            "total_crm_value": total_crm_value,
            "active_leads": active_leads,
            "avg_heat": round(avg_heat, 2),
            "withdrawn_count": withdrawn_count,
            "delta_count": delta_count,
            "mortgage_cliff_count": mortgage_cliff_count,
        }


def _compute_pulse() -> dict[str, Any]:
    with _sqlite_connect() as conn:
        columns = _table_columns(conn, "leads")
        if not columns:
            return {
                "lead_count": 0,
                "with_images": 0,
                "with_phone": 0,
                "with_email": 0,
                "feed_health": "no_data",
                "runtime": "oci_hybrid",
                "database": "sqlite",
            }

        lead_count = int(conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0])
        with_images = 0
        if "main_image" in columns or "property_images" in columns:
            image_checks: list[str] = []
            if "main_image" in columns:
                image_checks.append("COALESCE(main_image, '') != ''")
            if "property_images" in columns:
                image_checks.append("COALESCE(CAST(property_images AS TEXT), '') NOT IN ('', '[]')")
            with_images = int(conn.execute(f"SELECT COUNT(*) FROM leads WHERE {' OR '.join(image_checks)}").fetchone()[0])

        with_phone = 0
        if "contact_phones" in columns:
            with_phone = int(
                conn.execute("SELECT COUNT(*) FROM leads WHERE COALESCE(CAST(contact_phones AS TEXT), '') NOT IN ('', '[]')").fetchone()[0]
            )

        with_email = 0
        if "contact_emails" in columns:
            with_email = int(
                conn.execute("SELECT COUNT(*) FROM leads WHERE COALESCE(CAST(contact_emails AS TEXT), '') NOT IN ('', '[]')").fetchone()[0]
            )

        return {
            "lead_count": lead_count,
            "with_images": with_images,
            "with_phone": with_phone,
            "with_email": with_email,
            "feed_health": "ok" if lead_count else "no_data",
            "last_scan": _now_sydney().isoformat(),
            "runtime": "oci_hybrid",
            "database": "sqlite",
            "database_path": str(_resolve_sqlite_path()),
        }


@router.get("/health", include_in_schema=False)
async def healthcheck():
    try:
        payload = await asyncio.to_thread(_compute_pulse)
        return {
            "status": "ok",
            "app_env": (os.getenv("APP_ENV") or "development").strip().lower(),
            "services": {
                "database": "ok" if payload["lead_count"] >= 0 else "error",
                "redis": "ok",
            },
        }
    except Exception as exc:  # pragma: no cover
        return {
            "status": "error",
            "services": {"database": "error", "redis": "ok"},
            "database_error": str(exc),
        }


@router.get("/livez", include_in_schema=False)
async def livecheck():
    return {"status": "ok"}


@router.get("/api/auth/session")
async def get_session(request: Request):
    user = _request_session(request)
    if not user:
        return {"authenticated": False}
    return {"authenticated": True, "identifier": user["identifier"], "role": user["role"]}


@router.post("/api/auth/login")
async def login(body: LoginRequest, response: Response):
    if not admin_login_ready():
        raise HTTPException(status_code=503, detail="Login is not configured")

    user = authenticate_user(body.identifier, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    response.set_cookie(
        value=build_session_token(user["identifier"], user["role"]),
        **session_cookie_settings(),
    )
    return {"authenticated": True, "identifier": user["identifier"], "role": user["role"]}


@router.post("/api/auth/logout")
async def logout(response: Response):
    settings = session_cookie_settings()
    response.delete_cookie(
        settings["key"],
        path="/",
        secure=settings["secure"],
        samesite=settings["samesite"],
    )
    return {"authenticated": False}


@router.get("/api/leads")
async def list_leads(
    request: Request,
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None,
    is_fresh: bool = False,
    signal_status: Optional[str] = None,
    min_dom: Optional[int] = None,
):
    _require_access(request)
    return await asyncio.to_thread(
        _sqlite_like_leads,
        limit=limit,
        offset=offset,
        search=search,
        is_fresh=is_fresh,
        signal_status=signal_status,
        min_dom=min_dom,
    )


@router.get("/api/leads/search")
async def search_leads(request: Request, q: str, limit: int = 50, signal_status: Optional[str] = None):
    _require_access(request)
    payload = await asyncio.to_thread(
        _sqlite_like_leads,
        limit=limit,
        offset=0,
        search=q,
        is_fresh=False,
        signal_status=signal_status,
        min_dom=None,
    )
    payload["mode"] = "sqlite_like"
    payload["vector_enabled"] = False
    return payload


@router.get("/api/leads/{lead_id}")
async def get_lead_detail(request: Request, lead_id: str):
    _require_access(request)
    lead = await asyncio.to_thread(_get_lead_by_id, lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return lead


@router.get("/api/analytics")
async def analytics(request: Request):
    _require_access(request)
    return await asyncio.to_thread(_compute_analytics)


@router.get("/api/analytics/events")
async def analytics_events(request: Request, hours: int = 168, limit: int = 40):
    _require_access(request)
    since = (_now_sydney() - dt.timedelta(hours=max(1, int(hours)))).isoformat()
    return {"events": [], "total": 0, "since": since, "limit": max(1, int(limit))}


@router.get("/api/system/pulse")
async def system_pulse(request: Request):
    _require_access(request)
    return await asyncio.to_thread(_compute_pulse)


@router.websocket("/ws/events")
async def events_ws(websocket: WebSocket):
    if not _websocket_has_access(websocket):
        await websocket.close(code=4401)
        return

    await websocket.accept()
    await websocket.send_json(
        {
            "type": "SYSTEM_HEALTH",
            "data": {
                "status": "connected",
                "database": "online",
                "timestamp": _now_sydney().isoformat(),
            },
        }
    )
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                await websocket.send_json(
                    {
                        "type": "SYSTEM_HEALTH",
                        "data": {
                            "status": "connected",
                            "database": "online",
                            "timestamp": _now_sydney().isoformat(),
                        },
                    }
                )
    except Exception:
        return


def create_oci_core_app() -> FastAPI:
    app = FastAPI(title="The Property Domain OCI Core")
    app.include_router(router)
    return app


app = create_oci_core_app()
