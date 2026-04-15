import datetime
import html
import asyncio
import hmac
import hashlib
import json
import os
import re
import smtplib
from base64 import b64encode
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Security, Request, BackgroundTasks, File, UploadFile, Form
from pydantic import BaseModel
from zoneinfo import ZoneInfo
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import (
    API_KEY, api_key_header, APP_TITLE, SYDNEY_TZ, STOCK_ROOT, 
    PROJECT_ROOT, PROJECT_LOG_PATH, BRAND_NAME, BRAND_AREA, BRAND_LOGO_URL, 
    PRINCIPAL_NAME, PRINCIPAL_EMAIL, PRINCIPAL_PHONE, PROJECT_MEMORY_RULE, 
    BACKGROUND_SEND_POLL_SECONDS, PRIMARY_STRIKE_SUBURB, SECONDARY_STRIKE_SUBURBS,
    SMS_BRIDGE_URL, USE_POSTGRES, build_public_url
)
from core.utils import (
    now_sydney, now_iso, format_sydney, parse_client_datetime, 
    _first_non_empty, _safe_int, _format_moneyish, _parse_json_list, 
    _encode_value, _decode_row, _dedupe_text_list, _normalize_phone, 
    _dedupe_by_phone, _parse_iso_datetime, _parse_calendar_date, 
    _month_range_from_date, _bool_db
)
from services.scoring import _trigger_bonus, _status_penalty, _score_lead
from models.schemas import *
from core.logic import *

from core.database import get_session
from services.automations import _schedule_task, _refresh_lead_next_action
from core.security import get_api_key

router = APIRouter()

@router.get("/api/intelligence/property")
async def get_property_intel(address: str, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    # Assuming cotality get_property_intelligence handles everything internally 
    return {"status": "ok", "address": address, "data": "Needs implementation in cotality"}


@router.post("/api/outreach/pre-call")
async def send_pre_call_warmup(lead_id: str, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    lead = res.mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    phones = json.loads(lead['contact_phones'] or "[]")
    if not phones:
        raise HTTPException(status_code=400, detail="No phone number available")
    
    # Construct the Warmup Message — identity based on lead queue
    from core.config import PRINCIPAL_NAME, BRAND_NAME, OWNIT1ST_OPERATOR_NAME, OWNIT1ST_BRAND_NAME
    address_short = lead['address'].split(',')[0]
    route_queue = (lead.get('route_queue') or '').upper()
    if route_queue == 'MORTGAGE' or lead.get('route_queue') == 'mortgage_ownit1st':
        sender_name = OWNIT1ST_OPERATOR_NAME
        sender_brand = OWNIT1ST_BRAND_NAME
    else:
        sender_name = PRINCIPAL_NAME
        sender_brand = BRAND_NAME
    message = f"Hi {lead['owner_name'].split(' ')[0]}, it's {sender_name} from {sender_brand}. Just about to give you a call regarding {address_short}—I've got some interesting market data to share. Speak soon!"
    
    # Try Twilio via sms_service, fall back to Hermes Bridge (port 3000)
    sms_sent = False
    try:
        from services.sms_service import sms_service
        result = await sms_service.send_sms(phones[0], message, lead_id)
        sms_sent = result.get("ok", False)
    except Exception:
        pass

    if not sms_sent:
        try:
            if SMS_BRIDGE_URL:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(f"{SMS_BRIDGE_URL}/send", json={"to": phones[0], "message": message})
                sms_sent = True
        except Exception:
            pass

    if not sms_sent:
        return {
            "status": "sms_bridge_unavailable",
            "note": "Twilio not configured and no reachable SMS bridge is configured. Set TWILIO_* or SMS_BRIDGE_URL.",
            "message": message,
        }

    try:
        await session.execute(
            text("INSERT INTO notes (lead_id, note_type, content) VALUES (:lead_id, 'sms_sent', :content)"),
            {"lead_id": lead_id, "content": f"Pre-call Warmup Sent: {message}"}
        )
        await session.commit()
    except Exception:
        pass
    return {"status": "warmup_sent", "message": message}


@router.get("/api/outreach/assets")
async def get_outreach_assets(api_key: str = Depends(get_api_key)):
    from core.config import STOCK_ROOT
    assets_dir = Path(STOCK_ROOT) / "Important documents"
    if not assets_dir.exists():
        return []

    assets = []
    for p in assets_dir.glob("*.pdf"):
        assets.append({
            "name": p.name,
            "path": str(p),
            "url": build_public_url(f"/stock-images/Important documents/{p.name}")
        })
    return assets


@router.post("/api/outreach/draft-email")
async def draft_email_for_lead(
    lead_id: str,
    brand: str = "ls",
    archetype: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    AI-draft a personalized outreach email for a lead.
    brand: 'ls' (Nitin Puri / L+S) or 'ownit1st' (Shahid / Ownit1st Loans)
    archetype: override detected archetype (optional)
    Returns {subject, body, tier_used} — body is plain text, ready for the operator to review.
    """
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead = dict(row)
    from services.ai_router import draft_outreach_email
    result = await draft_outreach_email(lead, archetype=archetype, brand=brand)
    return result


@router.get("/api/outreach/why-now/{lead_id}")
async def why_now_for_lead(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    Returns a one-sentence 'why now' urgency note and archetype classification for a lead.
    Used in the call script and lead detail panel.
    """
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead = dict(row)
    from services.ai_router import classify_lead, score_note
    classification = await classify_lead(lead)
    note = await score_note(lead)
    return {
        "archetype": classification.get("archetype"),
        "why_now": classification.get("why_now"),
        "score_note": note,
        "heat_score": lead.get("heat_score"),
        "address": lead.get("address"),
    }


@router.post("/api/outreach/cma/{lead_id}")
async def generate_cma_for_lead(
    lead_id: str,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    Generate a 1-page CMA (Comparative Market Analysis) for a lead via Gemini.
    Combines suburb report data, opportunity insights, and AI-drafted talking points.
    Returns structured JSON: headline, market_paragraph, talking_points, sms_text, email_subject.
    Falls back to rule-based output if AI is unavailable.
    """
    from services.cma_service import generate_cma
    return await generate_cma(session, lead_id)


@router.post("/api/enrich/domain")
async def trigger_domain_enrichment(
    max_calls: int = 20,
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    On-demand trigger for Domain API enrichment batch.
    Enriches up to `max_calls` leads that have not been Domain-enriched yet.
    Default 20 (well within the 500/day free-tier cap).
    """
    from services.domain_enrichment import run_enrichment_batch
    result = await run_enrichment_batch(session, max_calls=max_calls)
    return result


def _fmt_currency(value: Any, fallback: str = "") -> str:
    """Format a numeric value as AUD currency string, e.g. $1,250,000."""
    if value in (None, "", 0, "0"):
        return fallback
    try:
        return f"${float(str(value).replace(',', '')):,.0f}"
    except (TypeError, ValueError):
        return fallback


def _doorknock_email_html(lead: dict) -> str:
    """Render the doorknock follow-up HTML email for a single lead."""
    owner_name = (lead.get("owner_name") or "").strip()
    owner_first = owner_name.split()[0].strip() if owner_name else "there"

    address = lead.get("address") or "your property"
    suburb = lead.get("suburb") or "this area"

    val_low = _fmt_currency(lead.get("estimated_value_low"), "available on request")
    val_high = _fmt_currency(lead.get("estimated_value_high"), "")
    estimated_range = f"{val_low} – {val_high}" if val_high else val_low

    ownership_years = lead.get("ownership_duration_years")
    if ownership_years is not None:
        try:
            ownership_years_str = str(int(float(ownership_years)))
        except (TypeError, ValueError):
            ownership_years_str = "several"
    else:
        ownership_years_str = "several"

    suburb_median_raw = lead.get("suburb_median")
    suburb_median = _fmt_currency(suburb_median_raw, "contact us for details")

    return f"""<div style="max-width:600px;margin:0 auto;font-family:Georgia,'Times New Roman',serif;background:#fffdf7;padding:32px 28px;border:1px solid #e8e0d0;border-radius:8px;">
  <p style="color:#2d2a26;font-size:17px;line-height:1.7;margin:0 0 18px;">
    Hi {owner_first},
  </p>
  <p style="color:#2d2a26;font-size:17px;line-height:1.7;margin:0 0 18px;">
    Thanks for taking a moment to chat earlier — I really enjoyed learning a bit about {address} and the area.
  </p>
  <p style="color:#2d2a26;font-size:17px;line-height:1.7;margin:0 0 18px;">
    As I mentioned, I've been working closely with owners in {suburb} and happy to share anything useful about the local market. Here's a quick snapshot:
  </p>
  <div style="background:#faf6ef;border-left:3px solid #c9a84c;padding:16px 20px;margin:0 0 18px;border-radius:0 6px 6px 0;">
    <p style="margin:0 0 6px;color:#5a5347;font-size:15px;"><strong>Estimated range:</strong> {estimated_range}</p>
    <p style="margin:0 0 6px;color:#5a5347;font-size:15px;"><strong>Owned approximately:</strong> {ownership_years_str} years</p>
    <p style="margin:0;color:#5a5347;font-size:15px;"><strong>Suburb median:</strong> {suburb_median}</p>
  </div>
  <p style="color:#2d2a26;font-size:17px;line-height:1.7;margin:0 0 18px;">
    No pressure at all — just wanted to follow through on what we discussed. If you ever want a more detailed look, I'm a phone call away.
  </p>
  <p style="color:#2d2a26;font-size:17px;line-height:1.7;margin:0 0 6px;">
    Warm regards,
  </p>
  <p style="color:#2d2a26;font-size:17px;line-height:1.7;margin:0 0 2px;font-weight:600;">
    Nitin Puri
  </p>
  <p style="color:#8a8070;font-size:14px;line-height:1.5;margin:0;">
    Laing+Simmons Oakville | Windsor<br>
    04 85 85 7881 &nbsp;|&nbsp; oakville@lsre.com.au
  </p>
</div>"""


@router.get("/api/leads/doorknock-emails")
async def get_doorknock_emails(
    api_key: str = Depends(get_api_key),
    session: AsyncSession = Depends(get_session),
):
    """
    Returns pre-generated HTML follow-up emails for all door-knock leads.
    Matches leads where trigger_type or source contains 'door' followed by 'knock'
    (with or without separator). Operator can copy each email_html directly.
    """
    res = await session.execute(
        text(
            """
            SELECT
                id, owner_name, address, suburb,
                estimated_value_low, estimated_value_high,
                ownership_duration_years, suburb_median,
                contact_emails
            FROM leads
            WHERE
                trigger_type ILIKE '%door%knock%'
                OR trigger_type ILIKE '%doorknock%'
                OR source ILIKE '%door%knock%'
                OR source ILIKE '%doorknock%'
            ORDER BY created_at DESC
            """
        )
    )
    rows = res.mappings().all()

    results = []
    for row in rows:
        lead = dict(row)
        address = lead.get("address") or "your property"
        subject = f"Great to meet you today — {address}"

        raw_emails = lead.get("contact_emails")
        if isinstance(raw_emails, str):
            try:
                contact_emails = json.loads(raw_emails)
            except Exception:
                contact_emails = []
        elif isinstance(raw_emails, list):
            contact_emails = raw_emails
        else:
            contact_emails = []

        results.append({
            "lead_id": lead.get("id"),
            "owner_name": lead.get("owner_name") or "",
            "address": address,
            "suburb": lead.get("suburb") or "",
            "subject": subject,
            "email_html": _doorknock_email_html(lead),
            "contact_emails": contact_emails,
        })

    return results
