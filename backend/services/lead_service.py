"""
Lead persistence service.
Extracted from main.py to break the circular dependency from scraper.py.
"""
from typing import Optional

from core.events import event_manager
from services.dedup_spatial import get_h3_index


async def save_lead(lead, db=None) -> None:
    """
    Save a lead to Postgres (if enabled) and broadcast via WebSocket.
    Populates h3index for spatial deduplication on every save.
    Used by scrapers and ingest tools.
    """
    from core.config import USE_POSTGRES

    lead_data = lead.model_dump() if hasattr(lead, "model_dump") else dict(lead)

    # Populate H3 index if coordinates are available
    lat = lead_data.get("lat") or 0.0
    lng = lead_data.get("lng") or 0.0
    if not lead_data.get("h3index"):
        h3idx = get_h3_index(float(lat), float(lng))
        if h3idx:
            lead_data["h3index"] = h3idx
            if hasattr(lead, "h3index"):
                lead.h3index = h3idx

    if USE_POSTGRES and db:
        try:
            await db.merge(lead)
            await db.commit()
        except Exception as exc:
            await event_manager.broadcast_log(f"Postgres Save Error: {exc}", level="ERROR")

    await event_manager.broadcast({"type": "NEW_LEAD", "data": lead_data})
