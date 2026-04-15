"""
Lead geocoding via OSM Nominatim.
"""

from __future__ import annotations

import asyncio
import os
import re
from typing import Any, Dict

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from services.cadastral_resolver import cadastral_resolver

USER_AGENT = "PropertyIntelligenceMachine/1.0 (contact@lsre.com.au)"


async def _geocode_nominatim(client: httpx.AsyncClient, query: str) -> tuple[float, float] | None:
    resp = await client.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": query, "format": "json", "limit": 1},
    )
    data = resp.json()
    if not data:
        return None
    return float(data[0]["lat"]), float(data[0]["lon"])


def _looks_like_lot_dp(query: str) -> bool:
    return bool(re.search(r"\bLot\s+\d+[A-Za-z]?\s*(?:,|\s)+DP\s*\d+\b", query or "", flags=re.I))


async def _geocode_google(client: httpx.AsyncClient, query: str) -> tuple[float, float] | None:
    api_key = (os.getenv("GOOGLE_MAPS_GEOCODING_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not api_key:
        return None
    resp = await client.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"address": query, "key": api_key},
    )
    payload = resp.json()
    results = payload.get("results") or []
    if not results:
        return None
    loc = (results[0].get("geometry") or {}).get("location") or {}
    lat = float(loc.get("lat")) if loc.get("lat") is not None else None
    lng = float(loc.get("lng")) if loc.get("lng") is not None else None
    if lat is None or lng is None:
        return None
    return lat, lng


async def run_geocoding_batch(session: AsyncSession, limit: int = 100) -> Dict[str, Any]:
    """
    Bulk geocode leads with missing coordinates.

    Nominatim requires low-volume usage, so requests are throttled to roughly
    one per second.
    """
    res = await session.execute(
        text(
            """
            SELECT id, address, suburb, postcode
            FROM leads
            WHERE status != 'dropped'
              AND (
                lat IS NULL OR lat = 0
                OR lng IS NULL OR lng = 0
              )
            ORDER BY heat_score DESC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    )
    rows = res.mappings().all()
    geocoded = 0
    failed = 0
    nominatim_hits = 0
    cadastral_hits = 0
    google_hits = 0

    async with httpx.AsyncClient(
        timeout=10,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        for row in rows:
            query = f"{row['address']}, {row['suburb']} NSW {row['postcode'] or ''}, Australia"
            try:
                coords = await _geocode_nominatim(client, query)
                if coords:
                    lat, lng = coords
                    await session.execute(
                        text("UPDATE leads SET lat = :lat, lng = :lng, updated_at = :updated_at WHERE id = :id"),
                        {"lat": lat, "lng": lng, "updated_at": now_iso(), "id": row["id"]},
                    )
                    geocoded += 1
                    nominatim_hits += 1
                else:
                    cadastral_coords: tuple[float, float] | None = None
                    if _looks_like_lot_dp(query):
                        resolved = await cadastral_resolver.resolve(query)
                        if resolved and resolved.get("lat") and resolved.get("lng"):
                            cadastral_coords = (float(resolved["lat"]), float(resolved["lng"]))
                    if cadastral_coords:
                        await session.execute(
                            text("UPDATE leads SET lat = :lat, lng = :lng, updated_at = :updated_at WHERE id = :id"),
                            {"lat": cadastral_coords[0], "lng": cadastral_coords[1], "updated_at": now_iso(), "id": row["id"]},
                        )
                        geocoded += 1
                        cadastral_hits += 1
                    else:
                        google_coords = await _geocode_google(client, query)
                        if google_coords:
                            await session.execute(
                                text("UPDATE leads SET lat = :lat, lng = :lng, updated_at = :updated_at WHERE id = :id"),
                                {"lat": google_coords[0], "lng": google_coords[1], "updated_at": now_iso(), "id": row["id"]},
                            )
                            geocoded += 1
                            google_hits += 1
                        else:
                            failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(1.1)

    await session.commit()
    return {
        "geocoded": geocoded,
        "failed": failed,
        "attempted": len(rows),
        "nominatim_hits": nominatim_hits,
        "cadastral_hits": cadastral_hits,
        "google_hits": google_hits,
    }
