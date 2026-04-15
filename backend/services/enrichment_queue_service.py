"""
Enrichment Queue Service

Manages a priority-based queue for Domain API enrichment.
High-value leads (withdrawn, probate, high heat) get enriched first.
Prevents API rate limit exhaustion on low-value leads.

Daily budget: 490 API calls (Domain API limit).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Priority scores — higher = enrich first
_SIGNAL_PRIORITY: Dict[str, int] = {
    "PROBATE": 100,
    "WITHDRAWN": 90,
    "EXPIRED": 80,
    "STALE": 60,
    "OFF-MARKET": 70,
    "LIVE": 40,
    "DELTA": 30,
    "SOLD": 10,
}

_DAILY_BUDGET = 490  # Domain API daily call limit


def _compute_priority(lead: Dict[str, Any]) -> int:
    """Compute enrichment priority score for a lead."""
    score = _SIGNAL_PRIORITY.get((lead.get("signal_status") or "").upper(), 20)

    # Boost for high heat score
    heat = lead.get("heat_score") or 0
    if heat >= 80:
        score += 20
    elif heat >= 60:
        score += 10

    # Boost if no contact info (enrichment will help)
    phones = lead.get("contact_phones") or []
    if not phones or phones == "[]":
        score += 15

    # Penalty if recently enriched
    last_enriched = lead.get("id4me_enriched_at")
    if last_enriched:
        score -= 20

    return min(100, max(0, score))


async def ensure_queue_table(session: AsyncSession) -> None:
    """Create enrichment_queue table if not exists."""
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS enrichment_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id TEXT NOT NULL,
            priority_score INTEGER NOT NULL DEFAULT 50,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT,
            processed_at TEXT,
            error TEXT
        )
    """))
    await session.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_enrich_queue_status ON enrichment_queue(status, priority_score DESC)"
    ))
    await session.commit()


async def rebuild_queue(session: AsyncSession, limit: int = _DAILY_BUDGET) -> int:
    """
    Rebuild the pending enrichment queue from leads that need enrichment.
    Clears existing pending items and re-populates by priority.
    Returns count of items queued.
    """
    # Clear stale pending items
    await session.execute(text(
        "DELETE FROM enrichment_queue WHERE status = 'pending'"
    ))

    # Find leads that need enrichment (no Domain listing ID yet, or no images)
    rows = (await session.execute(text("""
        SELECT id, signal_status, heat_score, contact_phones, id4me_enriched_at,
               domain_listing_id, property_images
        FROM leads
        WHERE status NOT IN ('converted', 'dropped')
        AND (domain_listing_id IS NULL OR property_images IS NULL OR property_images = '[]')
        ORDER BY heat_score DESC
        LIMIT 2000
    """))).mappings().all()

    if not rows:
        return 0

    # Score and sort
    scored = []
    for row in rows:
        lead = dict(row)
        priority = _compute_priority(lead)
        scored.append((lead["id"], priority))

    scored.sort(key=lambda x: x[1], reverse=True)
    top = scored[:limit]

    now = datetime.now(timezone.utc).isoformat()
    for lead_id, priority in top:
        await session.execute(text("""
            INSERT INTO enrichment_queue (lead_id, priority_score, status, created_at)
            VALUES (:lead_id, :priority, 'pending', :now)
        """), {"lead_id": lead_id, "priority": priority, "now": now})

    await session.commit()
    logger.info("[EnrichQueue] Rebuilt queue with %d leads", len(top))
    return len(top)


async def get_next_batch(session: AsyncSession, batch_size: int = 10) -> List[str]:
    """Get next batch of lead IDs to enrich, ordered by priority."""
    rows = (await session.execute(text("""
        SELECT lead_id FROM enrichment_queue
        WHERE status = 'pending'
        ORDER BY priority_score DESC
        LIMIT :batch_size
    """), {"batch_size": batch_size})).mappings().all()

    return [row["lead_id"] for row in rows]


async def mark_processed(lead_id: str, session: AsyncSession, error: Optional[str] = None) -> None:
    """Mark a queued lead as processed (success or failed)."""
    now = datetime.now(timezone.utc).isoformat()
    status = "failed" if error else "done"
    await session.execute(text("""
        UPDATE enrichment_queue
        SET status = :status, processed_at = :now, error = :error
        WHERE lead_id = :lead_id AND status = 'pending'
    """), {"status": status, "now": now, "error": error, "lead_id": lead_id})
    await session.commit()


async def get_queue_stats(session: AsyncSession) -> Dict[str, Any]:
    """Return current queue statistics."""
    try:
        row = (await session.execute(text("""
            SELECT
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
                COUNT(CASE WHEN status = 'done' THEN 1 END) as done,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                MAX(CASE WHEN status = 'pending' THEN priority_score END) as top_priority
            FROM enrichment_queue
        """))).mappings().first()
        return dict(row) if row else {}
    except Exception:
        return {}
