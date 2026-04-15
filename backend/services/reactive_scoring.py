"""
Reactive Scoring — adjusts call_today_score based on live events.

Runs every 30 minutes. Boosts leads that have:
  1. Open CRITICAL/HIGH Nyla tickets
  2. Unresponded REA enquiries (last 72h)
  3. Overdue follow-ups
  4. Recently contacted (decay to avoid re-calling)

Does NOT replace base scoring — adds a transient boost on top.
"""
from __future__ import annotations

import logging
from typing import Dict, Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import USE_POSTGRES

log = logging.getLogger("reactive_scoring")

# Dialect helpers
_3d_ago = "NOW() - INTERVAL '3 days'" if USE_POSTGRES else "datetime('now', '-3 days')"
_2d_ago = "NOW() - INTERVAL '2 days'" if USE_POSTGRES else "datetime('now', '-2 days')"


async def run_reactive_scoring(session: AsyncSession) -> Dict[str, Any]:
    """Boost call_today_score for leads that need urgent attention."""
    boosted = 0

    # 1. Leads with open CRITICAL/HIGH tickets from Nyla/Rex → +30
    try:
        result = await session.execute(text("""
            UPDATE leads SET call_today_score = LEAST(call_today_score + 30, 100)
            WHERE id IN (
                SELECT DISTINCT related_lead_id FROM tickets
                WHERE related_lead_id IS NOT NULL
                  AND status IN ('open', 'accepted')
                  AND severity IN ('critical', 'high')
                  AND created_by_type = 'agent'
            )
            AND status NOT IN ('converted', 'dropped')
            AND call_today_score < 90
        """))
        boosted += result.rowcount or 0
    except Exception as exc:
        log.debug("Ticket boost failed: %s", exc)

    # 2. Leads with unresponded REA enquiries (last 72h) → +25
    try:
        result = await session.execute(text(f"""
            UPDATE leads SET call_today_score = LEAST(call_today_score + 25, 100)
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND COALESCE(rea_enquiries, 0) > 0
              AND rea_last_enquiry_at >= {_3d_ago}
              AND (last_contacted_at IS NULL OR last_contacted_at < rea_last_enquiry_at)
              AND status NOT IN ('converted', 'dropped')
              AND call_today_score < 90
        """))
        boosted += result.rowcount or 0
    except Exception as exc:
        log.debug("Enquiry boost failed: %s", exc)

    # 3. Leads with overdue follow-ups → +15
    try:
        now_iso = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat()
        result = await session.execute(text("""
            UPDATE leads SET call_today_score = LEAST(call_today_score + 15, 100)
            WHERE follow_up_due_at IS NOT NULL
              AND follow_up_due_at != ''
              AND follow_up_due_at < :now
              AND status NOT IN ('converted', 'dropped')
              AND call_today_score < 85
        """), {"now": now_iso})
        boosted += result.rowcount or 0
    except Exception as exc:
        log.debug("Overdue boost failed: %s", exc)

    # 4. Decay recently contacted leads (last 48h) → -10 to avoid re-calling
    try:
        result = await session.execute(text(f"""
            UPDATE leads SET call_today_score = GREATEST(call_today_score - 10, 0)
            WHERE last_contacted_at >= {_2d_ago}
              AND status NOT IN ('converted', 'dropped')
              AND call_today_score > 20
        """))
    except Exception as exc:
        log.debug("Contact decay failed: %s", exc)

    await session.commit()

    if boosted > 0:
        log.info("[reactive_scoring] Boosted %d leads", boosted)
    return {"boosted": boosted}
