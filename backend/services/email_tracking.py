"""
Email Tracking — pixel opens, click redirects, and behavioral scoring.

Provides tracking ID generation, HTML wrapping with pixel + click redirects,
and event recording that feeds back into lead behavioral intelligence.
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from models.sql_models import EmailEvent, Lead

logger = logging.getLogger(__name__)


# ─── Tracking ID ──────────────────────────────────────────────────────────────


def generate_tracking_id() -> str:
    """Generate a UUID-based tracking ID for an outbound email."""
    return str(uuid.uuid4())


# ─── HTML wrapping ────────────────────────────────────────────────────────────


def wrap_email_with_tracking(
    html_body: str,
    tracking_id: str,
    backend_url: str,
) -> str:
    """Append a tracking pixel and replace links with click-tracking redirects.

    Args:
        html_body: The original HTML email body.
        tracking_id: Unique tracking ID for this send.
        backend_url: Base URL of the backend (e.g. https://api.example.com).

    Returns:
        Modified HTML with tracking pixel and click-wrapped links.
    """
    base = backend_url.rstrip("/")
    pixel_url = f"{base}/api/track/open/{tracking_id}"
    pixel_tag = (
        f'<img src="{pixel_url}" width="1" height="1" '
        f'alt="" style="display:none;border:0;" />'
    )

    def _wrap_link(match: re.Match) -> str:
        href = match.group(1)
        # Don't wrap mailto: or tel: links
        if href.startswith(("mailto:", "tel:", "#")):
            return match.group(0)
        redirect_url = (
            f"{base}/api/track/click/{tracking_id}"
            f"?url={quote(href, safe='')}"
        )
        return match.group(0).replace(href, redirect_url)

    # Replace href="..." links
    wrapped = re.sub(
        r'href=["\']([^"\']+)["\']',
        _wrap_link,
        html_body,
    )

    # Append pixel before closing </body> if present, otherwise at the end
    if "</body>" in wrapped.lower():
        wrapped = re.sub(
            r"(</body>)",
            f"{pixel_tag}\\1",
            wrapped,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        wrapped += pixel_tag

    return wrapped


# ─── Event recording ─────────────────────────────────────────────────────────


async def record_open(
    session: AsyncSession,
    tracking_id: str,
    ip: str = None,
    user_agent: str = None,
) -> None:
    """Record an email open event and update lead behavioral fields.

    Creates an EmailEvent row and increments Lead.email_open_count.
    """
    now = now_iso()

    # Find the original send event to get lead_id
    lead_id = await _resolve_lead_id(session, tracking_id)

    event = EmailEvent(
        id=str(uuid.uuid4()),
        tracking_id=tracking_id,
        lead_id=lead_id,
        event_type="open",
        opened_at=now,
        ip_address=ip,
        user_agent=user_agent,
        created_at=now,
    )
    session.add(event)

    # Update lead if known
    if lead_id:
        lead_result = await session.execute(
            select(Lead).where(Lead.id == lead_id)
        )
        lead = lead_result.scalars().first()
        if lead:
            lead.email_open_count = (lead.email_open_count or 0) + 1
            lead.last_email_opened_at = now
            lead.updated_at = now

    await session.commit()
    logger.info("Recorded open for tracking_id=%s lead=%s", tracking_id, lead_id)


async def record_click(
    session: AsyncSession,
    tracking_id: str,
    link_url: str,
    ip: str = None,
    user_agent: str = None,
) -> None:
    """Record a click event for a tracked link."""
    now = now_iso()
    lead_id = await _resolve_lead_id(session, tracking_id)

    event = EmailEvent(
        id=str(uuid.uuid4()),
        tracking_id=tracking_id,
        lead_id=lead_id,
        event_type="click",
        link_url=link_url,
        opened_at=now,
        ip_address=ip,
        user_agent=user_agent,
        created_at=now,
    )
    session.add(event)

    # Update lead click count
    if lead_id:
        lead_result = await session.execute(
            select(Lead).where(Lead.id == lead_id)
        )
        lead = lead_result.scalars().first()
        if lead:
            lead.email_click_count = (lead.email_click_count or 0) + 1
            lead.updated_at = now

    await session.commit()
    logger.info(
        "Recorded click for tracking_id=%s lead=%s url=%s",
        tracking_id, lead_id, link_url,
    )


# ─── Behavioral scoring ──────────────────────────────────────────────────────


async def update_behavioral_scores(
    session: AsyncSession, lead_id: str
) -> None:
    """Recompute best_open_hour, email_engagement_score, and channel_preference
    from EmailEvent history for a given lead.
    """
    if not lead_id:
        return

    lead_result = await session.execute(
        select(Lead).where(Lead.id == lead_id)
    )
    lead = lead_result.scalars().first()
    if not lead:
        return

    # Gather all events for this lead
    events_result = await session.execute(
        select(EmailEvent)
        .where(EmailEvent.lead_id == lead_id)
        .order_by(EmailEvent.created_at)
    )
    events = list(events_result.scalars().all())
    if not events:
        return

    # --- best_open_hour: hour of day with most opens ---
    open_hours: Dict[int, int] = {}
    opens = 0
    clicks = 0
    for ev in events:
        if ev.event_type == "open":
            opens += 1
            if ev.opened_at:
                try:
                    from datetime import datetime as _dt
                    ts = _dt.fromisoformat(ev.opened_at)
                    hour = ts.hour
                    open_hours[hour] = open_hours.get(hour, 0) + 1
                except (ValueError, TypeError):
                    pass
        elif ev.event_type == "click":
            clicks += 1

    if open_hours:
        lead.best_open_hour = max(open_hours, key=open_hours.get)

    # --- email_engagement_score: 0-100 based on open/click ratios ---
    total_sends = max(lead.email_open_count or 0, opens, 1)
    open_rate = min(opens / total_sends, 1.0)
    click_rate = min(clicks / max(opens, 1), 1.0)
    engagement = round(open_rate * 60 + click_rate * 40, 1)
    lead.email_engagement_score = min(100.0, engagement)

    # --- channel_preference: simple heuristic ---
    if engagement >= 40:
        lead.channel_preference = "email"
    elif (lead.email_click_count or 0) > 0:
        lead.channel_preference = "email"
    else:
        # Don't override existing preference if email engagement is low
        if not lead.channel_preference:
            lead.channel_preference = "sms"

    lead.updated_at = now_iso()
    await session.commit()
    logger.info(
        "Updated behavioral scores for lead %s: engagement=%.1f, best_hour=%s, pref=%s",
        lead_id, lead.email_engagement_score, lead.best_open_hour, lead.channel_preference,
    )


# ─── Internal helpers ─────────────────────────────────────────────────────────


async def _resolve_lead_id(
    session: AsyncSession, tracking_id: str
) -> Optional[str]:
    """Resolve the lead_id associated with a tracking_id by looking up
    the first EmailEvent (the 'send' event) for that tracking_id.
    """
    result = await session.execute(
        select(EmailEvent.lead_id)
        .where(EmailEvent.tracking_id == tracking_id)
        .limit(1)
    )
    row = result.first()
    return row[0] if row and row[0] else None
