"""Waitlist / demo-request capture endpoint.

Stores inbound interest from the public marketing page.
No auth required - public endpoint.
"""
import asyncio
import datetime
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, field_validator
from sqlalchemy import text

import core.database as db_module
from core.security import get_api_key
from models.schemas import SendEmailRequest
from services.integrations import send_email_service

router = APIRouter()
log = logging.getLogger(__name__)

WAITLIST_GUIDE_FILES = [
    Path("backend/assets/waitlist_guides/01-buyer-opportunity-guide.txt"),
    Path("backend/assets/waitlist_guides/02-seller-timing-guide.txt"),
    Path("backend/assets/waitlist_guides/03-mortgage-readiness-guide.txt"),
]
ALLOWED_WAITLIST_OFFERS = {"buyer_guide", "seller_guide"}


class DemoRequest(BaseModel):
    name: Optional[str] = None
    email: str
    suburb_interest: Optional[str] = None
    offer_code: Optional[str] = None
    phone: Optional[str] = None
    agency: Optional[str] = None
    team_size: Optional[str] = None
    message: Optional[str] = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if "@" not in normalized or "." not in normalized.split("@", 1)[-1]:
            raise ValueError("A valid email address is required")
        return normalized


class DemoRequestResponse(BaseModel):
    ok: bool
    message: str


def _resolve_waitlist_guide_attachments() -> list[str]:
    attachments: list[str] = []
    for path in WAITLIST_GUIDE_FILES:
        resolved = path.resolve()
        if resolved.exists() and resolved.is_file():
            attachments.append(str(resolved))
    return attachments


def _build_waitlist_email_html(name: str, suburb: str, offer_code: str) -> str:
    safe_name = name or "there"
    safe_suburb = suburb or "your selected area"
    intent_label = "buying opportunities" if offer_code == "buyer_guide" else "seller timing"
    piece_one = f"{safe_suburb} Demand Pulse: the streets and price bands where inquiry depth is shifting first this month."
    piece_two = f"{safe_suburb} Value Window: where sellers are over/under pricing and how that changes negotiating leverage."
    piece_three = f"{safe_suburb} Finance Timing Map: practical rate-move scenarios and what they mean for action in the next 90 days."
    return f"""
<html>
  <body style="margin:0;padding:0;background:#f8f8f8;font-family:'Segoe UI',Arial,sans-serif;color:#101010;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="padding:28px 0;">
      <tr>
        <td align="center">
          <table role="presentation" width="620" cellpadding="0" cellspacing="0" style="max-width:620px;background:#ffffff;border-radius:18px;border:1px solid #ececec;overflow:hidden;">
            <tr>
              <td style="padding:26px 26px 10px;">
                <div style="font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#7a7a7a;">The Property Domain</div>
                <h1 style="margin:10px 0 8px;font-size:30px;line-height:1.15;">Your guides are inside</h1>
                <p style="margin:0 0 16px;font-size:15px;line-height:1.7;color:#333333;">
                  Hi {safe_name}, thanks for requesting our {intent_label} pack for <strong>{safe_suburb}</strong>.
                  We attached the three guides so you can review them now.
                </p>
                <p style="margin:0 0 14px;font-size:14px;line-height:1.7;color:#444444;">
                  If you want, reply to this email and we can prepare a tailored market snapshot next.
                </p>
                <div style="margin:16px 0 0;display:grid;gap:10px;">
                  <div style="padding:12px 14px;border:1px solid #e8e8e8;border-radius:12px;background:#fafafa;">
                    <div style="font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#666;">Piece 01</div>
                    <div style="margin-top:5px;font-size:14px;line-height:1.6;color:#242424;">{piece_one}</div>
                  </div>
                  <div style="padding:12px 14px;border:1px solid #e8e8e8;border-radius:12px;background:#fafafa;">
                    <div style="font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#666;">Piece 02</div>
                    <div style="margin-top:5px;font-size:14px;line-height:1.6;color:#242424;">{piece_two}</div>
                  </div>
                  <div style="padding:12px 14px;border:1px solid #e8e8e8;border-radius:12px;background:#fafafa;">
                    <div style="font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#666;">Piece 03</div>
                    <div style="margin-top:5px;font-size:14px;line-height:1.6;color:#242424;">{piece_three}</div>
                  </div>
                </div>
                <div style="margin-top:22px;padding-top:16px;border-top:1px solid #efefef;">
                  <div style="font-size:24px;line-height:1.1;color:#111111;font-family:'Segoe Script','Brush Script MT',cursive;">Thank you</div>
                  <div style="font-size:13px;color:#666666;margin-top:6px;">From The Property Domain</div>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()


async def _send_waitlist_autoreply(recipient: str, name: str, suburb: str, offer_code: str) -> None:
    attachment_paths = _resolve_waitlist_guide_attachments()
    subject = f"The Property Domain guides for {suburb or 'your suburb'}"
    email = SendEmailRequest(
        account_id="waitlist-auto",
        recipient=recipient,
        subject=subject,
        body=_build_waitlist_email_html(name=name, suburb=suburb, offer_code=offer_code),
        plain_text=False,
        attachment_paths=attachment_paths,
    )
    try:
        await asyncio.to_thread(send_email_service, None, email)
    except Exception:
        log.exception("Waitlist auto-email failed for recipient=%s", recipient)


@router.post("/api/waitlist", response_model=DemoRequestResponse)
async def submit_demo_request(body: DemoRequest):
    """Accept a demo/waitlist request from the public landing page."""
    display_name = str(body.name or "").strip() or body.email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
    offer_code = body.offer_code if body.offer_code in ALLOWED_WAITLIST_OFFERS else "buyer_guide"
    async with db_module._async_session_factory() as session:
        await session.execute(
            text(
                """
                INSERT INTO propella_waitlist
                    (name, email, suburb_interest, offer_code, phone, agency, team_size, message, submitted_at)
                VALUES
                    (:name, :email, :suburb_interest, :offer_code, :phone, :agency, :team_size, :message, :submitted_at)
                """
            ),
            {
                "name": display_name,
                "email": body.email,
                "suburb_interest": body.suburb_interest,
                "offer_code": offer_code,
                "phone": body.phone,
                "agency": body.agency,
                "team_size": body.team_size,
                "message": body.message,
                "submitted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
        )
        await session.commit()

    await _send_waitlist_autoreply(
        recipient=body.email,
        name=display_name,
        suburb=body.suburb_interest or "",
        offer_code=offer_code,
    )

    return DemoRequestResponse(ok=True, message="Request received. We'll be in touch within 24 hours.")


@router.get("/api/waitlist", include_in_schema=True, dependencies=[Depends(get_api_key)])
async def list_demo_requests():
    """Return all demo requests (admin use - protected via API key at router level)."""
    async with db_module._async_session_factory() as session:
        result = await session.execute(text("SELECT * FROM propella_waitlist ORDER BY submitted_at DESC LIMIT 200"))
        rows = result.mappings().all()
    return {"requests": [dict(r) for r in rows]}
