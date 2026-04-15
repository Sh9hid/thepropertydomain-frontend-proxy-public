"""
Spatial Deduplication via H3 hexagonal indexing.
Resolution 12 ≈ 0.0003 km² — matches a single subdivided parcel footprint.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

H3_RESOLUTION = 12

try:
    import h3 as h3lib
    HAS_H3 = True
except ImportError:
    h3lib = None
    HAS_H3 = False
    logger.warning("[H3] h3 library not installed — spatial dedup disabled")


def get_h3_index(lat: float, lng: float) -> Optional[str]:
    """Return H3 Resolution-12 index string for a lat/lng pair."""
    if not HAS_H3:
        return None
    if lat == 0.0 and lng == 0.0:
        return None
    try:
        return h3lib.geo_to_h3(lat, lng, H3_RESOLUTION)
    except Exception as e:
        logger.warning(f"[H3] geo_to_h3 error: {e}")
        return None


async def is_spatial_duplicate(
    db,
    lat: float,
    lng: float,
    exclude_id: Optional[str] = None,
) -> Optional[str]:
    """
    Query intelligence.property for an existing record at the same H3 hex.
    Returns the existing property_id if a duplicate is found, else None.
    Gracefully returns None if h3 or Postgres is unavailable.
    """
    h3idx = get_h3_index(lat, lng)
    if not h3idx:
        return None

    try:
        from sqlalchemy import text
        sql = "SELECT id FROM intelligence.property WHERE h3index = :h3idx"
        params: dict = {"h3idx": h3idx}
        if exclude_id:
            sql += " AND id != :exclude_id"
            params["exclude_id"] = exclude_id
        sql += " LIMIT 1"
        result = await db.execute(text(sql), params)
        row = result.fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"[H3] Duplicate check error: {e}")
        return None
