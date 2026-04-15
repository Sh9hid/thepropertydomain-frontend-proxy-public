"""
HERMES Lead Signal Worker

When HERMES finds a relevant market signal, this worker cross-references
affected suburbs with leads and invalidates their cached briefs so they
regenerate with fresh market context.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def run(
    finding: Dict[str, Any],
    session: AsyncSession,
) -> Dict[str, Any]:
    """
    Cross-reference a HERMES finding with leads.
    Invalidates lead briefs for leads in the affected suburb.

    Args:
        finding: HermesFinding dict with keys: topic, signal_type, summary, suburb (optional)
        session: DB session

    Returns:
        dict with affected_leads count and lead_ids list
    """
    topic = finding.get("topic", "")
    signal_type = finding.get("signal_type", "")
    summary = finding.get("summary", "")

    # Extract suburb from topic or summary
    suburb = _extract_suburb(topic, summary, finding.get("suburb"))
    if not suburb:
        return {"affected_leads": 0, "lead_ids": [], "reason": "no_suburb_detected"}

    # Find leads in this suburb
    try:
        rows = (await session.execute(
            text("""
                SELECT id FROM leads
                WHERE LOWER(suburb) = LOWER(:suburb)
                AND status NOT IN ('converted', 'dropped')
                AND contact_phones IS NOT NULL AND contact_phones != '[]'
                LIMIT 100
            """),
            {"suburb": suburb},
        )).mappings().all()

        lead_ids = [row["id"] for row in rows]

        if not lead_ids:
            return {"affected_leads": 0, "lead_ids": [], "suburb": suburb, "reason": "no_leads_in_suburb"}

        # Invalidate briefs (sync cache eviction — no session needed)
        from services.underwriter_service import invalidate_brief
        invalidate_brief(lead_ids)

        logger.info(
            f"[HermesLeadSignal] Signal '{signal_type}' in suburb '{suburb}' "
            f"→ invalidated {len(lead_ids)} lead briefs"
        )

        return {
            "affected_leads": len(lead_ids),
            "lead_ids": lead_ids,
            "suburb": suburb,
            "signal_type": signal_type,
        }

    except Exception as exc:
        logger.warning(f"[HermesLeadSignal] Failed to process finding: {exc}")
        return {"affected_leads": 0, "lead_ids": [], "error": str(exc)}


def _extract_suburb(topic: str, summary: str, explicit_suburb: Optional[str] = None) -> Optional[str]:
    """Extract suburb name from topic/summary text."""
    if explicit_suburb:
        return explicit_suburb.strip()

    # Common NSW suburbs in our target area
    _TARGET_SUBURBS = [
        "Windsor", "Oakville", "Vineyard", "Riverstone", "Schofields",
        "Rouse Hill", "Box Hill", "Nelson", "Pitt Town", "McGraths Hill",
        "South Windsor", "Wilberforce", "Bligh Park", "Mulgrave",
        "Woonona", "Bulli", "Thirroul", "Corrimal", "Towradgi",
        "Fairy Meadow", "North Wollongong", "Wollongong",
    ]

    combined = f"{topic} {summary}".lower()
    for suburb in _TARGET_SUBURBS:
        if suburb.lower() in combined:
            return suburb

    return None
