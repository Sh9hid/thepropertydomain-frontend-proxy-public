"""
Timeline Service — unified lead touch history.

Aggregates calls, SMS, emails, status changes, and notes into a
single chronological timeline for each lead.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def get_lead_timeline(lead_id: str, session: AsyncSession, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Build a unified timeline for a lead from all touch sources.
    Returns list of events sorted by timestamp descending.
    """
    events: List[Dict[str, Any]] = []

    # 1. Call log entries
    try:
        rows = (await session.execute(text("""
            SELECT
                'call' as event_type,
                logged_at as ts,
                outcome,
                note,
                duration_seconds,
                NULL as channel,
                NULL as content
            FROM call_log
            WHERE lead_id = :lead_id
            ORDER BY logged_at DESC
            LIMIT :limit
        """), {"lead_id": lead_id, "limit": limit})).mappings().all()

        for row in rows:
            r = dict(row)
            events.append({
                "event_type": "call",
                "ts": r.get("ts", ""),
                "label": f"Call \u2014 {(r.get('outcome') or 'logged').replace('_', ' ')}",
                "detail": r.get("note") or "",
                "meta": {
                    "outcome": r.get("outcome"),
                    "duration_seconds": r.get("duration_seconds"),
                },
                "color": _call_color(r.get("outcome")),
            })
    except Exception as exc:
        logger.debug(f"[Timeline] call_log query failed: {exc}")

    # 2. Activity log from lead record (JSON array)
    try:
        row = (await session.execute(
            text("SELECT activity_log FROM leads WHERE id = :id"),
            {"id": lead_id}
        )).mappings().first()

        if row and row.get("activity_log"):
            activity = row["activity_log"]
            if isinstance(activity, str):
                try:
                    activity = json.loads(activity)
                except Exception:
                    activity = []
            if isinstance(activity, list):
                for entry in activity[-limit:]:
                    if not isinstance(entry, dict):
                        continue
                    ts = entry.get("at") or entry.get("created_at") or entry.get("ts") or entry.get("timestamp") or ""
                    action = entry.get("action") or entry.get("type") or entry.get("event") or "activity"
                    events.append({
                        "event_type": "activity",
                        "ts": ts,
                        "label": str(action).replace("_", " ").title(),
                        "detail": entry.get("note") or entry.get("message") or "",
                        "meta": {k: v for k, v in entry.items() if k not in ("at", "ts", "created_at", "timestamp")},
                        "color": "#636366",
                    })
    except Exception as exc:
        logger.debug(f"[Timeline] activity_log query failed: {exc}")

    # 3. Communications (SMS/email) if comm table exists
    try:
        rows = (await session.execute(text("""
            SELECT
                channel as event_type,
                sent_at as ts,
                status,
                body as content,
                recipient
            FROM communications
            WHERE lead_id = :lead_id
            ORDER BY sent_at DESC
            LIMIT :limit
        """), {"lead_id": lead_id, "limit": limit})).mappings().all()

        for row in rows:
            r = dict(row)
            channel = r.get("event_type", "message")
            events.append({
                "event_type": channel,
                "ts": r.get("ts", ""),
                "label": f"{channel.upper()} \u2014 {r.get('status', 'sent')}",
                "detail": (r.get("content") or "")[:120],
                "meta": {"recipient": r.get("recipient"), "status": r.get("status")},
                "color": "#0a84ff",
            })
    except Exception as exc:
        logger.debug(f"[Timeline] communications query failed: {exc}")

    # Sort by timestamp descending, handle missing timestamps
    events.sort(key=lambda e: e.get("ts") or "0000", reverse=True)
    return events[:limit]


def _call_color(outcome: Optional[str]) -> str:
    if not outcome:
        return "#636366"
    outcome = outcome.lower()
    if "interested" in outcome or "booked" in outcome or "qualified" in outcome:
        return "#30d158"
    if "no_answer" in outcome or "voicemail" in outcome:
        return "#ff9f0a"
    if "not_interested" in outcome or "do_not_call" in outcome:
        return "#ff453a"
    return "#5e5ce6"
