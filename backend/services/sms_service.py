"""
SMS / WhatsApp delivery service via Twilio.
Replaces the localhost:3000 Hermes Bridge.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional
from sqlalchemy import text

logger = logging.getLogger(__name__)

TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM = os.getenv("TWILIO_FROM_NUMBER", "")
TWILIO_WA_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "")


class SMSService:
    """
    Twilio-backed SMS and WhatsApp sender with activity logging.
    Falls back gracefully when credentials are absent.
    """

    def _configured(self) -> bool:
        return bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM)

    async def send_sms(self, to: str, message: str, lead_id: Optional[str] = None) -> Dict[str, Any]:
        if not self._configured():
            logger.warning("[SMS] Twilio not configured — SMS not sent.")
            return {"ok": False, "error": "twilio_not_configured", "lead_id": lead_id}
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            msg = client.messages.create(body=message, from_=TWILIO_FROM, to=to)
            await self._log_delivery(lead_id, "sms", to, message, msg.sid, "sent")
            return {"ok": True, "sid": msg.sid, "status": msg.status}
        except Exception as exc:
            logger.error(f"[SMS] Twilio error: {exc}")
            return {"ok": False, "error": str(exc)}

    async def send_whatsapp(self, to: str, message: str, lead_id: Optional[str] = None) -> Dict[str, Any]:
        wa_from = TWILIO_WA_FROM or f"whatsapp:{TWILIO_FROM}"
        wa_to = to if to.startswith("whatsapp:") else f"whatsapp:{to}"
        if not self._configured():
            return {"ok": False, "error": "twilio_not_configured"}
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            msg = client.messages.create(body=message, from_=wa_from, to=wa_to)
            await self._log_delivery(lead_id, "whatsapp", to, message, msg.sid, "sent")
            return {"ok": True, "sid": msg.sid, "status": msg.status}
        except Exception as exc:
            logger.error(f"[WhatsApp] Twilio error: {exc}")
            return {"ok": False, "error": str(exc)}

    async def _log_delivery(
        self,
        lead_id: Optional[str],
        channel: str,
        recipient: str,
        message: str,
        provider_id: str,
        status: str,
    ) -> None:
        if not lead_id:
            return
        try:
            from core.database import async_engine
            from core.utils import now_iso
            async with async_engine.begin() as conn:
                await conn.execute(
                    text("""
                    INSERT INTO outreach_log
                        (lead_id, channel, provider, recipient, subject, sent_at, status, provider_message_id)
                    VALUES (:lead_id, :channel, 'twilio', :recipient, :subject, :sent_at, :status, :provider_id)
                    ON CONFLICT DO NOTHING
                    """),
                    {
                        "lead_id": lead_id,
                        "channel": channel,
                        "recipient": recipient,
                        "subject": message[:160],
                        "sent_at": now_iso(),
                        "status": status,
                        "provider_id": provider_id
                    }
                )
        except Exception:
            pass  # outreach_log table may not exist yet


# Module-level singleton
sms_service = SMSService()
