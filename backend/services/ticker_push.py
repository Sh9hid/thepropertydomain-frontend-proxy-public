"""
Ticker Push — inserts a TickerEvent row and broadcasts over WebSocket.

Usage from any service:
    from services.ticker_push import push_ticker_event
    await push_ticker_event(session, event_type="WITHDRAWAL", source="domain_withdrawn",
                            address="12 Oak St", suburb="Windsor", postcode="2756",
                            heat_score=85, lead_id="abc123",
                            headline="Withdrawn after 47 days on market")
"""

import json
import uuid
import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.events import event_manager

_TYPE_META = {
    "WITHDRAWAL": {"icon": "⚠", "color": "#ff453a"},
    "PROBATE":    {"icon": "⚖", "color": "#ff9f0a"},
    "DA_FILED":   {"icon": "📋", "color": "#30d158"},
    "OBITUARY":   {"icon": "🕯", "color": "#bf5af2"},
    "INSOLVENCY": {"icon": "⚡", "color": "#ff9f0a"},
    "FIRE":       {"icon": "🔥", "color": "#ff453a"},
    "NEWS":       {"icon": "📰", "color": "#0a84ff"},
    "MARKET_SIGNAL": {"icon": "◈", "color": "#D6A84F"},
}


async def push_ticker_event(
    session: AsyncSession,
    event_type: str,
    source: str,
    address: str = "",
    suburb: str = "",
    postcode: str = "",
    owner_name: str = "",
    heat_score: int = 0,
    lead_id: str = "",
    headline: str = "",
    extra: Optional[dict] = None,
) -> str:
    """
    Insert a TickerEvent row and immediately broadcast over WebSocket.
    Returns the generated event id.
    """
    meta = _TYPE_META.get(event_type, {"icon": "●", "color": "rgba(255,255,255,0.5)"})
    event_id = str(uuid.uuid4())
    detected_at = datetime.datetime.utcnow().isoformat()

    await session.execute(
        text("""
            INSERT INTO ticker_events
                (id, event_type, source, address, suburb, postcode, owner_name,
                 heat_score, lead_id, icon, color, headline, extra, detected_at)
            VALUES
                (:id, :event_type, :source, :address, :suburb, :postcode, :owner_name,
                 :heat_score, :lead_id, :icon, :color, :headline, :extra, :detected_at)
            ON CONFLICT (id) DO NOTHING
        """),
        {
            "id": event_id,
            "event_type": event_type,
            "source": source,
            "address": address,
            "suburb": suburb,
            "postcode": postcode,
            "owner_name": owner_name,
            "heat_score": heat_score,
            "lead_id": lead_id,
            "icon": meta["icon"],
            "color": meta["color"],
            "headline": headline or f"{event_type} · {address or suburb}",
            "extra": json.dumps(extra or {}),
            "detected_at": detected_at,
        },
    )

    payload = {
        "id": event_id,
        "type": event_type,
        "source": source,
        "address": address,
        "suburb": suburb,
        "owner_name": owner_name,
        "heat_score": heat_score,
        "lead_id": lead_id,
        "icon": meta["icon"],
        "color": meta["color"],
        "headline": headline or f"{event_type} · {address or suburb}",
        "detected_at": detected_at,
    }
    await event_manager.broadcast_ticker_event(payload)
    return event_id
