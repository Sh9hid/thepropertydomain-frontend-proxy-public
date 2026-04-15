"""
Property visual helpers.

Keeps real listing photos separate from generated fallbacks so the UI can
label synthetic visuals honestly.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
GOOGLE_MAPS_EMBED_API_KEY = os.getenv("GOOGLE_MAPS_EMBED_API_KEY", "") or GOOGLE_MAPS_API_KEY


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value in (None, "", 0, 0.0):
            return None
        numeric = float(value)
        return numeric if numeric else None
    except (TypeError, ValueError):
        return None


def has_coords(lat: Any, lng: Any) -> bool:
    return _coerce_float(lat) is not None and _coerce_float(lng) is not None


def build_street_view_embed_url(lat: Any, lng: Any) -> Optional[str]:
    if not GOOGLE_MAPS_EMBED_API_KEY or not has_coords(lat, lng):
        return None
    lat_num = _coerce_float(lat)
    lng_num = _coerce_float(lng)
    return (
        "https://www.google.com/maps/embed/v1/streetview"
        f"?key={GOOGLE_MAPS_EMBED_API_KEY}"
        f"&location={lat_num},{lng_num}"
        "&fov=80&pitch=5&source=outdoor"
    )


def build_osm_map_tile_url(lat: Any, lng: Any, zoom: int = 16) -> Optional[str]:
    lat_num = _coerce_float(lat)
    lng_num = _coerce_float(lng)
    if lat_num is None or lng_num is None:
        return None
    n = 2 ** zoom
    xtile = int((lng_num + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(math.radians(lat_num))) / math.pi) / 2.0 * n)
    return f"https://tile.openstreetmap.org/{zoom}/{xtile}/{ytile}.png"


def build_property_visuals(lead: dict[str, Any]) -> dict[str, Any]:
    main_image = str(lead.get("main_image") or "").strip()
    property_images = [str(img).strip() for img in (lead.get("property_images") or []) if str(img).strip()]
    primary_photo = main_image or (property_images[0] if property_images else "")
    lat = lead.get("lat")
    lng = lead.get("lng")

    visual_url = primary_photo
    visual_source = "listing_photo" if primary_photo else ""
    visual_label = "Main listing photo" if primary_photo else ""
    visual_is_fallback = False

    if not primary_photo:
        map_tile_url = build_osm_map_tile_url(lat, lng) or ""
        if map_tile_url:
            visual_url = map_tile_url
            visual_source = "osm_map_tile"
            visual_label = "Map tile fallback"
            visual_is_fallback = True

    street_view_embed_url = ""
    if not primary_photo:
        street_view_embed_url = build_street_view_embed_url(lat, lng) or ""

    return {
        "visual_url": visual_url,
        "visual_source": visual_source,
        "visual_label": visual_label,
        "visual_is_fallback": visual_is_fallback,
        "street_view_embed_url": street_view_embed_url,
    }


async def fetch_street_view_metadata(lat: Any, lng: Any) -> dict[str, Any]:
    """
    Verify Street View availability without triggering billed image requests.

    If only an embed key is configured, return UNVERIFIED and let the browser
    attempt the iframe directly.
    """
    if not has_coords(lat, lng):
        return {"ok": False, "status": "NO_COORDS"}
    if not GOOGLE_MAPS_API_KEY:
        return {"ok": None, "status": "UNVERIFIED"}

    lat_num = _coerce_float(lat)
    lng_num = _coerce_float(lng)
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                "https://maps.googleapis.com/maps/api/streetview/metadata",
                params={
                    "location": f"{lat_num},{lng_num}",
                    "source": "outdoor",
                    "key": GOOGLE_MAPS_API_KEY,
                },
            )
        if resp.status_code != 200:
            return {"ok": False, "status": f"HTTP_{resp.status_code}"}
        payload = resp.json()
    except Exception as exc:
        logger.warning("Street View metadata lookup failed: %s", exc)
        return {"ok": False, "status": "ERROR"}

    status = payload.get("status") or "UNKNOWN"
    return {
        "ok": status == "OK",
        "status": status,
        "date": payload.get("date"),
        "copyright": payload.get("copyright"),
        "location": payload.get("location") or {},
        "pano_id": payload.get("pano_id") or "",
    }
