"""
Outreach Sender Daemon — dispatches approved hermes_campaigns.

Runs every 5 minutes. Picks up campaigns with status='approved',
sends via Twilio (SMS) or MS Graph (email), updates status to 'sent'.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = logging.getLogger("outreach_sender")


async def run_outreach_sender(session: AsyncSession) -> Dict[str, Any]:
    """Process approved campaigns. Returns summary."""
    sent_count = 0
    failed_count = 0

    rows = (await session.execute(text("""
        SELECT c.id, c.channel, c.subject, c.message, c.related_lead_id,
               l.contact_phones, l.contact_emails, l.address, l.suburb
        FROM hermes_campaigns c
        LEFT JOIN leads l ON l.id = c.related_lead_id
        WHERE c.status = 'approved'
        ORDER BY c.created_at ASC
        LIMIT 10
    """))).mappings().all()

    if not rows:
        return {"sent": 0, "failed": 0}

    for row in rows:
        campaign_id = row["id"]
        channel = (row["channel"] or "").lower()
        try:
            if channel == "sms":
                result = await _send_sms(row)
            elif channel in ("email", "email_html"):
                result = await _send_email(row)
            else:
                log.warning("[outreach_sender] Unknown channel '%s' for campaign %s", channel, campaign_id)
                result = {"ok": False, "error": f"unknown_channel:{channel}"}

            if result.get("ok"):
                await session.execute(text("""
                    UPDATE hermes_campaigns SET status = 'sent', sent_at = :now
                    WHERE id = :id
                """), {"id": campaign_id, "now": datetime.now(timezone.utc).isoformat()})
                sent_count += 1
                log.info("[outreach_sender] Sent %s to %s (%s)", channel, row.get("address", "?"), campaign_id[:8])
            else:
                await session.execute(text("""
                    UPDATE hermes_campaigns SET status = 'send_failed'
                    WHERE id = :id
                """), {"id": campaign_id})
                failed_count += 1
                log.warning("[outreach_sender] Failed %s for %s: %s", channel, campaign_id[:8], result.get("error"))

        except Exception as exc:
            log.warning("[outreach_sender] Exception sending %s: %s", campaign_id[:8], exc)
            failed_count += 1

    await session.commit()
    return {"sent": sent_count, "failed": failed_count}


async def _send_sms(row) -> Dict[str, Any]:
    """Send SMS via Twilio."""
    phones_raw = row.get("contact_phones") or ""
    if isinstance(phones_raw, list):
        phone = phones_raw[0] if phones_raw else ""
    else:
        phone = str(phones_raw).split(",")[0].strip().strip("[]\"' ")

    if not phone:
        return {"ok": False, "error": "no_phone"}

    from services.sms_service import SMSService
    sms = SMSService()
    return await sms.send_sms(phone, row["message"], lead_id=row.get("related_lead_id"))


async def _send_email(row) -> Dict[str, Any]:
    """Send email via MS Graph / SMTP."""
    emails_raw = row.get("contact_emails") or ""
    if isinstance(emails_raw, list):
        email = emails_raw[0] if emails_raw else ""
    else:
        email = str(emails_raw).split(",")[0].strip().strip("[]\"' ")

    if not email or "@" not in email:
        return {"ok": False, "error": "no_email"}

    from services.integrations import send_email_service

    class _EmailBody:
        recipient = email
        subject = row.get("subject") or "Follow up from Laing+Simmons Oakville"
        body = row["message"]
        plain_text = False
        attachment_paths = None

    result = await asyncio.to_thread(send_email_service, None, _EmailBody())
    return result
