"""
NSW Property Intelligence Service

Aggregates free government and open data for a single property:
  1. NSW Planning Portal DA history - all applications since 2019
  2. OSM Overpass - nearby schools, transport, shops within 1km
  3. Property visuals - Google Street View embed when configured, otherwise OSM tile

All fields are nullable. Every external call fails silently and returns empty
data. No call crashes the intel endpoint.
"""

import asyncio
import logging
import os
from typing import Optional

import httpx

from core.config import NSW_EPLANNING_DA_URL
from services.property_visuals import (
    build_osm_map_tile_url,
    build_street_view_embed_url,
    fetch_street_view_metadata,
)

logger = logging.getLogger(__name__)

NSW_EPLANNING_KEY = os.getenv("NSW_EPLANNING_KEY", "")
OSM_OVERPASS_URL = "https://overpass-api.de/api/interpreter"

DA_STATUS_COLOURS = {
    "approved": "green",
    "determined": "green",
    "submitted": "amber",
    "under assessment": "amber",
    "referred": "amber",
    "refused": "red",
    "withdrawn": "grey",
    "lapsed": "grey",
}


def _da_colour(status: str) -> str:
    s = (status or "").lower()
    for key, colour in DA_STATUS_COLOURS.items():
        if key in s:
            return colour
    return "grey"


async def _get_das(address: str, suburb: str) -> list[dict]:
    """
    Query NSW ePlanning DA tracker for development applications at this address.
    Returns [] if NSW_EPLANNING_KEY not set or request fails.
    """
    if not NSW_EPLANNING_KEY:
        logger.debug("NSW_EPLANNING_KEY not set - skipping DA lookup")
        return []

    params = {
        "suburb": suburb.upper(),
        "pageSize": "50",
        "pageNumber": "1",
    }
    headers = {"subscription-key": NSW_EPLANNING_KEY}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(NSW_EPLANNING_DA_URL, params=params, headers=headers)
        if resp.status_code == 401:
            logger.warning("NSW ePlanning: invalid subscription key")
            return []
        if resp.status_code != 200:
            logger.warning("NSW ePlanning: %s", resp.status_code)
            return []
        data = resp.json()
        items = data.get("Application", data.get("items", data if isinstance(data, list) else []))
        if not isinstance(items, list):
            return []

        addr_upper = address.upper().split(",")[0].strip()
        das = []
        for item in items:
            full_addr = (item.get("FullAddress") or item.get("address") or "").upper()
            if addr_upper and addr_upper not in full_addr:
                continue
            das.append({
                "app_number": item.get("PlanningPortalApplicationNumber") or item.get("id") or "",
                "type": item.get("DevelopmentType") or item.get("type") or "DA",
                "status": item.get("ApplicationStatus") or item.get("status") or "Unknown",
                "colour": _da_colour(item.get("ApplicationStatus") or item.get("status") or ""),
                "lodgement_date": (item.get("LodgementDate") or item.get("lodgement_date") or "")[:10],
                "description": (item.get("DevelopmentDescription") or "")[:120],
            })
        return das[:10]
    except Exception as exc:
        logger.warning("NSW ePlanning error: %s", exc)
        return []


async def _get_osm_amenities(lat: float, lng: float, radius_m: int = 1000) -> dict:
    """
    Query OSM Overpass for nearby amenities within radius_m metres.
    Returns dict with schools, stations, shops (top 3 each by distance proxy).
    """
    query = f"""
[out:json][timeout:8];
(
  node["amenity"="school"](around:{radius_m},{lat},{lng});
  node["railway"="station"](around:{radius_m},{lat},{lng});
  node["shop"="supermarket"](around:{radius_m},{lat},{lng});
  node["shop"="shopping_centre"](around:{radius_m},{lat},{lng});
);
out center 20;
"""
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.post(OSM_OVERPASS_URL, data={"data": query})
        if resp.status_code != 200:
            return {}
        elements = resp.json().get("elements", [])
        schools, stations, shops = [], [], []
        for el in elements:
            tags = el.get("tags", {})
            name = tags.get("name", "")
            if not name:
                continue
            if tags.get("amenity") == "school":
                schools.append(name)
            elif tags.get("railway") == "station":
                stations.append(name)
            elif tags.get("shop") in ("supermarket", "shopping_centre"):
                shops.append(name)
        return {
            "schools": schools[:3],
            "stations": stations[:2],
            "shops": shops[:2],
        }
    except Exception as exc:
        logger.warning("OSM Overpass error: %s", exc)
        return {}


async def _geocode_address(address: str, suburb: str, postcode: str) -> Optional[tuple[float, float]]:
    """
    Geocode an address via OSM Nominatim (free, no key).
    Returns (lat, lng) or None.
    """
    query = f"{address}, {suburb}, NSW {postcode}, Australia"
    try:
        async with httpx.AsyncClient(
            timeout=8,
            headers={"User-Agent": "woonona-lead-machine/1.0 (property-intelligence)"},
        ) as client:
            resp = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": "1", "countrycodes": "au"},
            )
        if resp.status_code != 200:
            return None
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
        return None
    except Exception as exc:
        logger.debug("Nominatim geocode error: %s", exc)
        return None


async def get_property_intel(
    lead_id: str,
    address: str,
    suburb: str,
    postcode: str,
    lat: Optional[float],
    lng: Optional[float],
) -> dict:
    """
    Aggregate property intelligence from free sources for a single property.
    """
    geocoded = False
    if not lat or not lng:
        coords = await _geocode_address(address, suburb, postcode or "")
        if coords:
            lat, lng = coords
            geocoded = True

    has_coords = bool(lat and lng)
    tasks = [
        _get_das(address, suburb),
        _get_osm_amenities(lat, lng) if has_coords else asyncio.sleep(0, result={}),
    ]
    das, amenities = await asyncio.gather(*tasks, return_exceptions=False)

    map_tile = build_osm_map_tile_url(lat, lng) if has_coords else None
    street_view_embed = build_street_view_embed_url(lat, lng) if has_coords else None
    street_view_meta = {"ok": False, "status": "NO_COORDS"}
    if has_coords and street_view_embed:
        street_view_meta = await fetch_street_view_metadata(lat, lng)
        if street_view_meta.get("ok") is False:
            street_view_embed = None

    return {
        "lead_id": lead_id,
        "das": das if isinstance(das, list) else [],
        "da_count": len(das) if isinstance(das, list) else 0,
        "nearby_amenities": amenities if isinstance(amenities, dict) else {},
        "street_view_url": None,
        "street_view_embed_url": street_view_embed,
        "street_view_available": bool(street_view_embed),
        "street_view_status": street_view_meta.get("status"),
        "street_view_date": street_view_meta.get("date"),
        "street_view_copyright": street_view_meta.get("copyright"),
        "map_tile_url": map_tile,
        "lat": lat,
        "lng": lng,
        "geocoded": geocoded,
    }
