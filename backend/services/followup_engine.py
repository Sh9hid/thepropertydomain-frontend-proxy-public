from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import SYDNEY_TZ
from core.logic import _hydrate_lead


FOLLOW_UP_RULES: Dict[str, Dict[str, Any]] = {
    "MISSED_BOOKING": {
        "reason_label": "Missed Booking",
        "action_type": "CALL_NOW",
        "suggested_action": "Call now and close",
        "delay_hours": 1,
        "title": "Recover missed booking window",
        "reason_text": "High intent was logged without a booking attempt.",
        "revenue_signal": "High-intent conversation ended without an appointment ask.",
        "ignore_risk": "Booking momentum cools fastest in the first 24 hours.",
    },
    "NO_FOLLOW_UP": {
        "reason_label": "No Follow Up",
        "action_type": "SET_FOLLOW_UP",
        "suggested_action": "Schedule follow-up within 24h",
        "delay_hours": 24,
        "title": "Lock the promised follow-up",
        "reason_text": "A next step was discussed but nothing was scheduled.",
        "revenue_signal": "A seller agreed to a next step but no callback is queued.",
        "ignore_risk": "Trust and response rate drop when promised follow-ups slip.",
    },
    "STALE_HIGH_INTENT": {
        "reason_label": "Stale High Intent",
        "action_type": "REENGAGE",
        "suggested_action": "Re-engage: high probability",
        "delay_hours": 4,
        "title": "Re-engage stale high-intent lead",
        "reason_text": "High-intent activity is stale beyond the 48 hour window.",
        "revenue_signal": "Proven intent is cooling without recent outreach.",
        "ignore_risk": "Hot leads decay into silent leads when the window is missed.",
    },
    "PRICE_DROP_OPPORTUNITY": {
        "reason_label": "Price Drop Opportunity",
        "action_type": "PRICE_DROP_REACHOUT",
        "suggested_action": "Call: price drop signal",
        "delay_hours": 2,
        "title": "Reach out on price-drop trigger",
        "reason_text": "A price-drop signal exists and no recent call was made.",
        "revenue_signal": "A repriced seller is showing urgency without recent contact.",
        "ignore_risk": "Repriced sellers usually speak first to the fastest agent.",
    },
}


def _resolve_now(now: Optional[datetime] = None) -> datetime:
    resolved = now or datetime.now(SYDNEY_TZ)
    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=SYDNEY_TZ)
    return resolved.astimezone(SYDNEY_TZ).replace(microsecond=0)


def get_follow_up_rule(reason: str) -> Dict[str, Any]:
    rule = FOLLOW_UP_RULES.get(reason)
    if not rule:
        raise ValueError(f"Unsupported missed-deal reason: {reason}")
    return rule


def generate_follow_up(
    lead: Dict[str, Any],
    reason: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    rule = get_follow_up_rule(reason)
    resolved_now = _resolve_now(now)
    follow_up_due_at = (resolved_now + timedelta(hours=int(rule["delay_hours"]))).isoformat()
    lead_id = str(lead.get("id") or lead.get("lead_id") or "")

    return {
        "lead_id": lead_id,
        "reason": reason,
        "reason_label": rule["reason_label"],
        "action_type": rule["action_type"],
        "suggested_action": rule["suggested_action"],
        "follow_up_due_at": follow_up_due_at,
        "next_action_at": follow_up_due_at,
        "next_action_type": "follow_up",
        "next_action_channel": "phone",
        "next_action_title": rule["title"],
        "next_action_reason": rule["reason_text"],
        "last_activity_type": "follow_up",
        "updated_at": resolved_now.isoformat(),
        "revenue_signal": rule["revenue_signal"],
        "ignore_risk": rule["ignore_risk"],
        "removed_from_queue": True,
    }


async def apply_follow_up(
    session: AsyncSession,
    lead_id: str,
    reason: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    row = (
        await session.execute(
            text(
                """
                SELECT *
                FROM leads
                WHERE id = :id
                LIMIT 1
                """
            ),
            {"id": lead_id},
        )
    ).mappings().first()
    if not row:
        raise LookupError(f"Lead not found: {lead_id}")

    payload = generate_follow_up(dict(row), reason, now=now)
    await session.execute(
        text(
            """
            UPDATE leads
            SET follow_up_due_at = :follow_up_due_at,
                next_action_at = :next_action_at,
                next_action_type = :next_action_type,
                next_action_channel = :next_action_channel,
                next_action_title = :next_action_title,
                next_action_reason = :next_action_reason,
                last_activity_type = :last_activity_type,
                updated_at = :updated_at
            WHERE id = :lead_id
            """
        ),
        payload,
    )
    await session.commit()

    updated_row = (
        await session.execute(
            text("SELECT * FROM leads WHERE id = :id LIMIT 1"),
            {"id": lead_id},
        )
    ).mappings().first()
    hydrated_lead = _hydrate_lead(dict(updated_row)) if updated_row else None
    return {**payload, "lead": hydrated_lead}
