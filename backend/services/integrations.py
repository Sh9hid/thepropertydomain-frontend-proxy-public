import datetime
import html
import asyncio
import hmac
import hashlib
import json
import mimetypes
import os
import re
import smtplib
import sqlite3
import socket
import uuid
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

from core.config import (
    API_KEY, api_key_header, APP_TITLE, DB_PATH, SYDNEY_TZ, STOCK_ROOT, 
    PROJECT_ROOT, PROJECT_LOG_PATH, BRAND_NAME, BRAND_AREA, BRAND_LOGO_URL, 
    PRINCIPAL_NAME, PRINCIPAL_EMAIL, PRINCIPAL_PHONE, PROJECT_MEMORY_RULE, 
    BACKGROUND_SEND_POLL_SECONDS, PRIMARY_STRIKE_SUBURB, SECONDARY_STRIKE_SUBURBS,
    build_public_url,
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

from pdf_generator import html_to_pdf

try:
    import msal
except ImportError:  # pragma: no cover - optional dependency in minimal test envs
    msal = None

_DEFAULT_SUPPRESSED_EMAIL_RECIPIENTS = {"operator@example.com"}


def _suppressed_email_recipients() -> set[str]:
    configured = (os.getenv("EMAIL_SUPPRESSION_RECIPIENTS") or "").strip()
    configured_set = {
        item.strip().lower()
        for item in configured.split(",")
        if item and item.strip()
    }
    return _DEFAULT_SUPPRESSED_EMAIL_RECIPIENTS | configured_set


def _is_suppressed_email_recipient(recipient: str) -> bool:
    normalized = str(recipient or "").strip().lower()
    return bool(normalized and normalized in _suppressed_email_recipients())

# ---------------------------------------------------------------------------
# Azure / Microsoft Graph email — client-credentials flow
# No browser login required. Uses MS_CLIENT_ID + MS_TENANT_ID + MS_CLIENT_SECRET
# from .env.  Azure app must have Application permission: Mail.Send (admin-consented).
# ---------------------------------------------------------------------------

def _get_ms_token() -> Optional[str]:
    """
    Acquire an app-level access token using MSAL ConfidentialClientApplication
    (client credentials grant).  Requires:
        MS_CLIENT_ID     — Azure app registration Application (client) ID
        MS_TENANT_ID     — Azure Directory (tenant) ID
        MS_CLIENT_SECRET — Client secret value from Azure app registration
    Returns the access_token string, or None if credentials are missing.
    """
    client_id     = os.getenv("MS_CLIENT_ID")
    tenant_id     = os.getenv("MS_TENANT_ID")
    client_secret = os.getenv("MS_CLIENT_SECRET")
    if msal is None or not client_id or not tenant_id or not client_secret:
        return None

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )

    # .default scope = all Application permissions granted in Azure portal
    token_response = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )

    if token_response and "access_token" in token_response:
        return token_response["access_token"]

    import logging
    logging.getLogger(__name__).warning(
        "MS Graph token acquisition failed: %s",
        token_response.get("error_description") if token_response else "no response"
    )
    return None


def _send_email_graph(
    recipient: str,
    subject: str,
    body: str,
    plain_text: bool = False,
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> bool:
    """Send via Microsoft Graph API. Called inside asyncio.to_thread — blocking is fine."""
    token = _get_ms_token()
    if not token:
        return False

    sender_upn = os.getenv("SMTP_USER", "nitin.puri@lsre.com.au")
    from_email  = os.getenv("SMTP_FROM", "oakville@lsre.com.au")
    reply_to_email = (os.getenv("OUTBOUND_REPLY_TO") or "info@thepropertydomain.com.au").strip()
    url = f"https://graph.microsoft.com/v1.0/users/{sender_upn}/sendMail"

    content_type = "Text" if plain_text else "HTML"
    from_email = os.getenv("SMTP_FROM", "oakville@lsre.com.au")
    list_unsub_mailto = f"<mailto:{from_email}?subject=unsubscribe>"
    payload = {
        "message": {
            "subject": subject,
            "from": {"emailAddress": {"address": from_email}},
            "replyTo": [{"emailAddress": {"address": reply_to_email}}],
            "body": {"contentType": content_type, "content": body},
            "toRecipients": [{"emailAddress": {"address": recipient}}],
            "internetMessageHeaders": [
                {"name": "List-Unsubscribe", "value": list_unsub_mailto},
                {"name": "List-Unsubscribe-Post", "value": "List-Unsubscribe=One-Click"},
            ],
        },
        "saveToSentItems": "true",
    }
    if attachments:
        payload["message"]["attachments"] = [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": item["name"],
                "contentType": item["content_type"],
                "contentBytes": item["content_b64"],
            }
            for item in attachments
        ]

    import urllib.request as _urllib_req
    data = json.dumps(payload).encode()
    req = _urllib_req.Request(
        url,
        data=data,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with _urllib_req.urlopen(req, timeout=20) as resp:
            return resp.status == 202
    except _urllib_req.HTTPError as exc:
        import logging
        error_body = exc.read().decode("utf-8", errors="ignore")
        logging.getLogger(__name__).error("Graph sendMail HTTP %s: %s", exc.code, error_body)
        return False
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("Graph sendMail failed: %s", exc)
        return False

def send_email_service(account_data: Optional[Dict[str, Any]], body: "SendEmailRequest") -> Dict[str, Any]:
    """
    Send email. Called inside asyncio.to_thread so blocking I/O is safe.
    Priority order:
      1. Microsoft Graph (client-credentials) — if MS_CLIENT_ID / MS_CLIENT_SECRET set
      2. SMTP fallback — uses credentials from passed account_data or .env
    """
    import logging
    log = logging.getLogger(__name__)

    plain_text = getattr(body, "plain_text", False)
    if _is_suppressed_email_recipient(body.recipient):
        log.warning("Email suppressed for blocked recipient %s", body.recipient)
        return {
            "ok": False,
            "provider": "suppressed",
            "message_id": "",
            "recipient": body.recipient,
            "subject": body.subject,
            "suppressed": True,
        }

    attachment_paths = [path for path in (getattr(body, "attachment_paths", None) or []) if path]
    prepared_attachments: List[Dict[str, Any]] = []
    skipped_attachments: List[str] = []
    for path in attachment_paths:
        try:
            file_path = Path(path)
            content = file_path.read_bytes()
            content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
            prepared_attachments.append(
                {
                    "name": file_path.name,
                    "content_type": content_type,
                    "content_b64": b64encode(content).decode("ascii"),
                    "content_bytes": content,
                }
            )
        except Exception:
            skipped_attachments.append(path)

    if skipped_attachments:
        log.warning("Some email attachments could not be read and were skipped: %s", skipped_attachments)

    # 1. Try Microsoft Graph (Modern Auth)
    if os.getenv("MS_CLIENT_ID") and os.getenv("MS_CLIENT_SECRET"):
        try:
            if _send_email_graph(
                body.recipient,
                body.subject,
                body.body,
                plain_text=plain_text,
                attachments=prepared_attachments,
            ):
                log.info("Email sent via Microsoft Graph to %s", body.recipient)
                return {
                    "ok": True,
                    "provider": "graph",
                    "message_id": "",
                    "recipient": body.recipient,
                    "subject": body.subject,
                    "attachments_sent": len(prepared_attachments),
                }
        except Exception as exc:
            log.warning("Graph send failed, falling back to SMTP: %s", exc)

    # 2. SMTP fallback
    message = EmailMessage()
    message["Subject"] = body.subject
    
    # Resolve sender from account_data or env
    from_email = (account_data or {}).get("from_email") or os.getenv("SMTP_FROM") or os.getenv("SMTP_USER", "")
    reply_to_email = (os.getenv("OUTBOUND_REPLY_TO") or from_email or "info@thepropertydomain.com.au").strip()
    message["From"] = from_email
    message["Reply-To"] = reply_to_email
    message["To"] = body.recipient
    domain = (from_email.split("@", 1)[1] if "@" in from_email else "localhost").strip() or "localhost"
    message["Message-ID"] = f"<{uuid.uuid4().hex}@{domain}>"
    message["List-Unsubscribe"] = f"<mailto:{from_email}?subject=unsubscribe>"
    message["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
    message["X-TPD-Inbound-Aliases"] = (os.getenv("INBOUND_EMAIL_ALIASES") or reply_to_email)
    message["X-Mailer"] = f"woonona-lead-machine/{socket.gethostname()}"
    
    if plain_text:
        import re as _re
        plain_body = _re.sub(r'<[^>]+>', '', body.body)
        message.set_content(plain_body)
    else:
        # Check if body is HTML
        if "<html>" in body.body.lower() or "<p>" in body.body.lower():
            message.set_content(body.body, subtype="html")
        else:
            message.set_content(body.body)
    for item in prepared_attachments:
        maintype, subtype = item["content_type"].split("/", 1) if "/" in item["content_type"] else ("application", "octet-stream")
        message.add_attachment(
            item["content_bytes"],
            maintype=maintype,
            subtype=subtype,
            filename=item["name"],
        )

    host = (account_data or {}).get("smtp_host") or os.getenv("SMTP_HOST", "smtp-mail.outlook.com")
    port = int((account_data or {}).get("smtp_port") or os.getenv("SMTP_PORT", "587"))
    user = (account_data or {}).get("smtp_username") or os.getenv("SMTP_USER", "")
    pw   = (account_data or {}).get("smtp_password") or os.getenv("SMTP_PASS", "")

    if not user:
        log.error("SMTP user not configured and Graph credentials missing.")
        raise ValueError("Email credentials not configured (MS Graph or SMTP)")

    use_tls = (account_data or {}).get("use_tls", True) if account_data else os.getenv("SMTP_USE_TLS", "true").lower() == "true"

    if use_tls:
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.starttls()
            server.login(user, pw)
            server.send_message(message)
    else:
        with smtplib.SMTP_SSL(host, port, timeout=30) as server:
            server.login(user, pw)
            server.send_message(message)

    log.info("Email sent via SMTP to %s", body.recipient)
    return {
        "ok": True,
        "provider": "smtp",
        "message_id": str(message.get("Message-ID") or ""),
        "recipient": body.recipient,
        "subject": body.subject,
        "attachments_sent": len(prepared_attachments),
    }

def _normalize_cotality_params(lead: Dict[str, Any]) -> Dict[str, str]:
    return {
        "address": (lead.get("address") or "").strip(),
        "suburb": (lead.get("suburb") or "").strip(),
        "postcode": (lead.get("postcode") or "").strip(),
        "owner_name": (lead.get("owner_name") or "").strip(),
    }


def _build_cotality_url(account: Dict[str, Any], path_key: str, lead: Dict[str, Any]) -> str:
    params = _normalize_cotality_params(lead)
    raw_path = (account.get(path_key) or "").strip()
    if not raw_path:
        return ""
    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        base = raw_path
    else:
        base = f"{(account.get('api_base') or '').rstrip('/')}/{raw_path.lstrip('/')}"
    if "{" in base:
        return base.format(**params)
    query = urlencode({k: v for k, v in params.items() if v})
    return f"{base}{'&' if '?' in base else '?'}{query}" if query else base


def _get_cotality_account(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM cotality_accounts WHERE enabled = 1 ORDER BY updated_at DESC, created_at DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def _fetch_cotality_json(account: Dict[str, Any], url: str) -> Dict[str, Any]:
    req = urllib_request.Request(
        url,
        headers={
            "Authorization": f"Bearer {account.get('api_key', '')}",
            "x-api-key": account.get("api_key", ""),
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib_request.urlopen(req, timeout=30) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def _get_cotality_dataset(conn: sqlite3.Connection, lead: Dict[str, Any], data_type: str, path_key: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT * FROM cotality_cache
        WHERE lead_id = ? AND data_type = ?
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """,
        (lead["id"], data_type),
    ).fetchone()
    if row and _is_cache_fresh(data_type, row["expires_at"], row["updated_at"]):
        return {"payload": json.loads(row["payload"]), "source": "cache", "cached_at": row["updated_at"]}

    account = _get_cotality_account(conn)
    if not account or not account.get("api_base") or not account.get("api_key"):
        return {"payload": {}, "source": "unconfigured", "cached_at": None}

    url = _build_cotality_url(account, path_key, lead)
    if not url:
        return {"payload": {}, "source": "unconfigured", "cached_at": None}
    try:
        payload = _fetch_cotality_json(account, url)
    except (urllib_error.URLError, urllib_error.HTTPError, json.JSONDecodeError):
        if row:
            return {"payload": json.loads(row["payload"]), "source": "stale_cache", "cached_at": row["updated_at"]}
        return {"payload": {}, "source": "unavailable", "cached_at": None}

    now = now_iso()
    cache_id = hashlib.md5(f"{lead['id']}:{data_type}".encode()).hexdigest()
    conn.execute(
        """
        INSERT INTO cotality_cache (id, lead_id, data_type, payload, source, expires_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            payload=excluded.payload, source=excluded.source, expires_at=excluded.expires_at, updated_at=excluded.updated_at
        """,
        (cache_id, lead["id"], data_type, json.dumps(payload), "api", _compute_cache_expiry(data_type), now, now),
    )
    conn.commit()
    return {"payload": payload, "source": "api", "cached_at": now}


def _generate_cotality_report(conn: sqlite3.Connection, lead: Dict[str, Any], report_type: str) -> Dict[str, Any]:
    datasets = {
        "property_attributes": _get_cotality_dataset(conn, lead, "property_attributes", "property_path"),
        "valuation": _get_cotality_dataset(conn, lead, "valuation", "valuation_path"),
        "comparable_sales": _get_cotality_dataset(conn, lead, "comparable_sales", "comparables_path"),
        "suburb_statistics": _get_cotality_dataset(conn, lead, "suburb_statistics", "suburb_path"),
        "rental_estimate": _get_cotality_dataset(conn, lead, "rental_estimate", "rental_path"),
        "listing_history": _get_cotality_dataset(conn, lead, "listing_history", "listing_path"),
        "market_trends": _get_cotality_dataset(conn, lead, "market_trends", "market_path"),
    }
    property_payload = datasets["property_attributes"]["payload"] or {}
    valuation_payload = datasets["valuation"]["payload"] or {}
    suburb_payload = datasets["suburb_statistics"]["payload"] or {}
    comparables_payload = datasets["comparable_sales"]["payload"] or {}
    rental_payload = datasets["rental_estimate"]["payload"] or {}
    listing_payload = datasets["listing_history"]["payload"] or {}
    market_payload = datasets["market_trends"]["payload"] or {}

    sections = [
        {
            "title": "Property Snapshot",
            "items": [
                f"Owner record: {lead.get('owner_name') or 'Not recorded'}",
                f"Trigger: {lead.get('trigger_type') or 'Stock Intelligence'}",
                f"Beds/Baths/Cars: {lead.get('bedrooms') or '-'} / {lead.get('bathrooms') or '-'} / {lead.get('car_spaces') or '-'}",
                f"Land size: {lead.get('land_size_sqm') or 'Unknown'} sqm",
                f"Property attributes cache source: {datasets['property_attributes']['source']}",
            ],
        },
        {
            "title": "Valuation & Rental",
            "items": [
                f"Current value estimate: {valuation_payload.get('estimate') or lead.get('sale_price') or 'Not available'}",
                f"Rental estimate: {rental_payload.get('estimate') or 'Not available'}",
                f"Call priority: {lead.get('call_today_score') or 0}",
                f"Evidence score: {lead.get('evidence_score') or 0}",
            ],
        },
        {
            "title": "Market Context",
            "items": [
                f"Suburb median / trend: {suburb_payload.get('median_price') or 'Not available'} / {market_payload.get('growth_rate') or 'Not available'}",
                f"Comparable sales cached: {len(comparables_payload.get('results', comparables_payload if isinstance(comparables_payload, list) else []))}",
                f"Listing history source: {datasets['listing_history']['source']}",
                f"Recommended next step: {lead.get('recommended_next_step') or 'Review and contact'}",
            ],
        },
    ]
    if report_type == "seller_signal":
        sections.append(
            {
                "title": "Seller Signal",
                "items": [
                    f"Ownership duration signal: {property_payload.get('ownership_duration') or lead.get('ownership_tenure') or 'Not available'}",
                    f"Listing withdrawal / market movement: {listing_payload.get('recent_withdrawal') or 'Not available'}",
                    f"Why now: {lead.get('why_now') or lead.get('scenario') or 'Not available'}",
                ],
            }
        )
    if report_type == "investment_insights":
        sections.append(
            {
                "title": "Investment Lens",
                "items": [
                    f"Rental estimate: {rental_payload.get('estimate') or 'Not available'}",
                    f"Capital growth: {market_payload.get('capital_growth') or 'Not available'}",
                    f"Demand score: {suburb_payload.get('demand_score') or 'Not available'}",
                ],
            }
        )
    if report_type == "generic_seller_brief":
        sections.extend(
            [
                {
                    "title": "Seller Brief",
                    "items": [
                        f"Recommended appraisal path: {lead.get('recommended_next_step') or 'Book a short review appointment'}",
                        f"Primary contact surface: {(lead.get('contact_phones') or [''])[0] or (lead.get('contact_emails') or [''])[0] or 'Manual contact prep required'}",
                        f"Why now: {lead.get('why_now') or lead.get('scenario') or 'Live local signal on file'}",
                        f"Prepared by: {PRINCIPAL_NAME} | {PRINCIPAL_PHONE}",
                    ],
                },
                {
                    "title": "What We Would Cover In The Appraisal",
                    "items": [
                        "Likely value range in the current market",
                        "Buyer depth and current competition",
                        "Timing and presentation strategy",
                        "The strongest next step if selling is being considered",
                    ],
                },
            ]
        )
    if report_type == "ai_appraisal_brief":
        sections.extend(
            [
                {
                    "title": "AI Strategy Layer",
                    "items": [
                        f"Hero visual source: {'Property image on file' if _stock_public_url(lead.get('main_image')) else 'Hero image pending'}",
                        "This brief is designed to feel premium, visual, and easy for the owner to act on.",
                        f"Operator angle: {lead.get('what_to_say') or 'Lead with a concise appraisal invitation, not a hard pitch.'}",
                        f"Principal contact: {PRINCIPAL_NAME} | {PRINCIPAL_EMAIL}",
                    ],
                },
                {
                    "title": "Next 7 Days",
                    "items": [
                        "Day 0: call and send this brief",
                        "Day 3: follow up with a short pricing update",
                        "Day 7: revisit value, timing, and buyer demand",
                        "Move to booked appraisal before expanding the sequence",
                    ],
                },
            ]
        )
    title_map = {
        "property_intelligence": "Property Intelligence Brief",
        "suburb_analytics": "Suburb Market Report",
        "seller_signal": "Seller Signal Brief",
        "investment_insights": "Investment Positioning Brief",
        "generic_seller_brief": "Seller Appraisal Brief",
        "ai_appraisal_brief": "AI Appraisal Strategy Brief",
    }
    title = title_map.get(report_type, "Property Intelligence Brief")
    sources = [
        *(lead.get("linked_files") or []),
        *(lead.get("source_evidence") or []),
        *(datasets[key]["cached_at"] and [f"Cotality {key.replace('_', ' ')} cache: {datasets[key]['source']} @ {datasets[key]['cached_at']}"] or [] for key in datasets),
    ]
    flattened_sources: List[str] = []
    for item in sources:
        if isinstance(item, list):
            flattened_sources.extend(str(value) for value in item if value)
        elif item:
            flattened_sources.append(str(item))
    html_content = _render_report_html(lead, report_type, title, sections, flattened_sources[:18])
    payload = {
        "title": title,
        "report_type": report_type,
        "template_family": "premium_report",
        "template_version": "2026.03.13",
        "hero_image_url": _stock_public_url(lead.get("main_image")) or _stock_public_url((lead.get("property_images") or [""])[0]),
        "signer": {"name": PRINCIPAL_NAME, "email": PRINCIPAL_EMAIL, "phone": PRINCIPAL_PHONE},
        "sections": sections,
        "sources": flattened_sources[:18],
        "datasets": {name: data["source"] for name, data in datasets.items()},
    }
    report_id = hashlib.md5(f"{lead['id']}:{report_type}:{now_iso()}".encode()).hexdigest()
    now = now_iso()
    conn.execute(
        """
        INSERT INTO cotality_reports (id, lead_id, report_type, title, html_content, json_payload, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (report_id, lead["id"], report_type, title, html_content, json.dumps(payload), now, now),
    )
    conn.commit()
    return {"id": report_id, "lead_id": lead["id"], "title": title, "report_type": report_type, "html_content": html_content, "payload": payload, "created_at": now}


def _stock_public_url(path_value: Any) -> str:
    text = str(path_value or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    stock_root = str(STOCK_ROOT)
    normalized_root = stock_root.replace("/", "\\").lower()
    normalized_text = text.replace("/", "\\")
    if normalized_text.lower().startswith(normalized_root):
        relative = normalized_text[len(stock_root):].lstrip("\\/")
        encoded = urllib_parse.quote(relative.replace(chr(92), "/"))
        return build_public_url(f"/stock-images/{encoded}")
    return ""


def _report_bar_svg(metrics: List[Dict[str, Any]]) -> str:
    max_value = max([max(1, _safe_int(metric.get("value"), 0)) for metric in metrics], default=100)
    bars = []
    for index, metric in enumerate(metrics):
        value = _safe_int(metric.get("value"), 0)
        height = max(20, round((value / max_value) * 120))
        x = 30 + index * 92
        bars.append(
            f"""
            <g>
              <rect x="{x}" y="{160 - height}" width="56" height="{height}" rx="16" fill="{metric.get('color', '#0f6fff')}" opacity="0.92"></rect>
              <text x="{x + 28}" y="{180}" text-anchor="middle" fill="#cfd8e3" font-size="12" font-family="Segoe UI, sans-serif">{html.escape(str(metric.get('label', '')))}</text>
              <text x="{x + 28}" y="{152 - height}" text-anchor="middle" fill="#09111f" font-size="14" font-weight="700" font-family="Segoe UI, sans-serif">{value}</text>
            </g>
            """
        )
    return f"""
    <svg viewBox="0 0 400 205" role="img" aria-label="Signal strength">
      <rect width="400" height="205" rx="24" fill="#0f1728"></rect>
      <text x="24" y="34" fill="#f8fafc" font-size="18" font-weight="700" font-family="Segoe UI, sans-serif">Signal Strength Snapshot</text>
      <text x="24" y="56" fill="#94a3b8" font-size="12" font-family="Segoe UI, sans-serif">Evidence, confidence, and contactability in one view.</text>
      <line x1="24" y1="160" x2="376" y2="160" stroke="#223047" stroke-width="2"></line>
      {''.join(bars)}
    </svg>
    """


def _render_report_html(lead: Dict[str, Any], report_type: str, report_title: str, sections: List[Dict[str, Any]], sources: List[str]) -> str:
    accent = "#d6a84f" if report_type == "ai_appraisal_brief" else "#11b57c" if report_type == "investment_insights" else "#0f6fff"
    hero_image_url = _stock_public_url(lead.get("main_image")) or _stock_public_url((lead.get("property_images") or [""])[0])
    metric_svg = _report_bar_svg(
        [
            {"label": "Priority", "value": lead.get("call_today_score") or 0, "color": accent},
            {"label": "Evidence", "value": lead.get("evidence_score") or 0, "color": "#36c28f"},
            {"label": "Confidence", "value": lead.get("confidence_score") or 0, "color": "#79c6ff"},
            {"label": "Contact", "value": min(100, len(lead.get("contact_phones") or []) * 25 + len(lead.get("contact_emails") or []) * 15), "color": "#d6a84f"},
        ]
    )
    section_markup = "".join(
        f"""
        <section class="report-section{' page-break' if idx in (1, 3) else ''}">
          <div class="section-kicker">Section {idx + 1:02d}</div>
          <h2>{html.escape(str(section['title']))}</h2>
          <ul>{''.join(f'<li>{html.escape(str(item))}</li>' for item in section['items'])}</ul>
        </section>
        """
        for idx, section in enumerate(sections)
    )
    source_markup = "".join(f"<li>{html.escape(str(source))}</li>" for source in sources if source)
    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <title>{report_title}</title>
        <style>
          @page {{ size: A4; margin: 18mm; }}
          * {{ box-sizing: border-box; }}
          body {{ margin: 0; font-family: 'Segoe UI', Arial, sans-serif; background: #f4f6fb; color: #162032; }}
          .wrap {{ max-width: 1040px; margin: 0 auto; }}
          .hero {{ padding: 34px; border-radius: 28px; background: linear-gradient(145deg, #09111f, #152742 54%, {accent} 160%); color: #f8fafc; }}
          .brand {{ display:flex; justify-content: space-between; gap:18px; align-items:flex-start; margin-bottom:20px; }}
          .brand img {{ width: 188px; max-width: 42%; height: auto; object-fit: contain; background: rgba(255,255,255,0.96); border-radius: 18px; padding: 8px 12px; }}
          .brand-copy {{ display:flex; flex-direction:column; gap:6px; }}
          .brand-title {{ font-size: 14px; letter-spacing:.16em; text-transform: uppercase; color:#d5e7ff; }}
          .brand-area {{ font-size: 16px; font-weight: 700; letter-spacing:.08em; text-transform: uppercase; }}
          .eyebrow {{ color: #9cd2ff; text-transform: uppercase; letter-spacing: .18em; font-size: 12px; font-weight: 800; }}
          h1 {{ margin: 12px 0 8px; font-size: 36px; line-height: 1.1; }}
          .meta {{ color: #d6e4ff; margin-bottom: 6px; font-size: 14px; }}
          .hero-grid {{ display: grid; grid-template-columns: 1.18fr 0.82fr; gap: 24px; margin-top: 24px; }}
          .hero-copy {{ color: #dbe8ff; line-height: 1.62; }}
          .hero-media, .hero-placeholder {{ min-height: 320px; border-radius: 24px; overflow: hidden; background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.12); display: flex; flex-direction: column; justify-content: space-between; }}
          .hero-media img {{ width: 100%; height: 270px; object-fit: cover; display: block; }}
          .hero-media span, .hero-placeholder p {{ padding: 14px 16px 18px; margin: 0; color: #eef6ff; font-size: 13px; }}
          .hero-placeholder {{ padding: 24px; }}
          .hero-placeholder strong {{ display: inline-flex; align-self: flex-start; padding: 7px 12px; border-radius: 999px; background: rgba(255,255,255,0.12); font-size: 11px; letter-spacing: .12em; text-transform: uppercase; }}
          .summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 24px; }}
          .summary-card, .chart-card, .report-section, .sources, .cta {{ background: #ffffff; border-radius: 24px; padding: 24px; box-shadow: 0 18px 44px rgba(15, 23, 42, 0.08); }}
          .summary-card h2, .chart-card h2, .report-section h2, .sources h2, .cta h2 {{ margin: 0 0 12px; font-size: 24px; }}
          .summary-card p {{ margin: 0 0 14px; color: #445064; line-height: 1.62; }}
          .contact-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }}
          .contact-card {{ padding: 14px 16px; border-radius: 18px; background: #f3f6fb; }}
          .contact-card span {{ display: block; font-size: 11px; text-transform: uppercase; letter-spacing: .12em; color: #64748b; font-weight: 800; }}
          .contact-card strong {{ display: block; margin-top: 8px; font-size: 15px; color: #111827; }}
          .report-section {{ margin-top: 18px; }}
          .page-break {{ break-before: page; }}
          .section-kicker {{ font-size: 11px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; color: {accent}; }}
          ul {{ margin: 16px 0 0; padding-left: 20px; color: #1f2937; line-height: 1.6; }}
          li {{ margin: 8px 0; }}
          .sources {{ margin-top: 18px; background: #0b1320; color: #d5e1f4; }}
          .sources li {{ color: #d5e1f4; }}
          .cta {{ margin-top: 18px; background: linear-gradient(160deg, {accent} 0%, #09111f 120%); color: #ffffff; }}
          .cta p {{ color: rgba(255,255,255,0.92); line-height: 1.62; }}
          .footer {{ margin-top: 28px; color: #5b6576; font-size: 13px; display: flex; justify-content: space-between; gap: 16px; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <section class="hero">
            <div class="brand">
              <div class="brand-copy">
                <img src="{BRAND_LOGO_URL}" alt="{BRAND_NAME}" />
                <div class="brand-title">Laing+Simmons</div>
                <div class="brand-area">{BRAND_AREA}</div>
              </div>
            </div>
            <div class="eyebrow">{'AI Strategy Attachment' if report_type == 'ai_appraisal_brief' else 'Seller-Ready Brief'}</div>
            <h1>{html.escape(str(report_title))}</h1>
            <div class="meta">{html.escape(str(lead.get('address') or 'Address unavailable'))}</div>
            <div class="meta">{html.escape(' '.join(part for part in [str(lead.get('suburb') or ''), str(lead.get('postcode') or '')] if part).strip())}</div>
            <div class="meta">Prepared for {html.escape(str(lead.get('owner_name') or 'Homeowner'))} | Generated {format_sydney()}</div>
            <div class="hero-grid">
              <div class="hero-copy">
                <p>{html.escape(str(lead.get('why_now') or lead.get('scenario') or 'A timely, evidence-backed appraisal conversation is justified by the current signal on file.'))}</p>
                <div class="summary-grid">
                  <div class="summary-card">
                    <h2>Appraisal Summary</h2>
                    <p>{html.escape(str(lead.get('recommended_next_step') or 'Book a short appraisal meeting to discuss likely value, buyer depth, and timing.'))}</p>
                    <div class="contact-grid">
                      <div class="contact-card"><span>Principal</span><strong>{html.escape(PRINCIPAL_NAME)}</strong></div>
                      <div class="contact-card"><span>Email</span><strong>{html.escape(PRINCIPAL_EMAIL)}</strong></div>
                      <div class="contact-card"><span>Phone</span><strong>{html.escape(PRINCIPAL_PHONE)}</strong></div>
                    </div>
                  </div>
                  <div class="chart-card">
                    <h2>Readiness Snapshot</h2>
                    {metric_svg}
                  </div>
                </div>
              </div>
              {f'<div class="hero-media"><img src="{html.escape(hero_image_url)}" alt="Property visual" /><span>Property visual on file</span></div>' if hero_image_url else '<div class="hero-placeholder"><strong>Property Visual</strong><p>Subject property representation layer pending.</p></div>'}
            </div>
          </section>
          {section_markup}
          <section class="cta">
            <h2>Next Step</h2>
            <p>Book a 15-minute appraisal review with {html.escape(PRINCIPAL_NAME)} to tighten likely value, buyer depth, and timing for this property.</p>
            <p>{html.escape(f"{PRINCIPAL_PHONE} | {PRINCIPAL_EMAIL}")}</p>
          </section>
          <section class="sources">
            <h3>Sources</h3>
            <ul>{source_markup or '<li>Local CRM intelligence cache</li>'}</ul>
          </section>
          <div class="footer"><span>{BRAND_NAME}</span><span>Report type: {report_type.replace('_', ' ').title()}</span></div>
        </div>
      </body>
    </html>
    """


def _send_http_text(account: Dict[str, Any], recipient: str, message: str) -> Dict[str, Any]:
    url = f"{account['api_base'].rstrip('/')}/{(account.get('send_path') or '/phone/sms/messages').lstrip('/')}"
    req = urllib_request.Request(
        url,
        data=json.dumps({"toMembers": [{"phoneNumber": recipient}], "message": message}).encode("utf-8"),
        headers={"Authorization": f"Bearer {account.get('access_token', '')}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=30) as response:
            body = response.read().decode("utf-8", errors="ignore")
            try:
                parsed = json.loads(body) if body else {}
            except json.JSONDecodeError:
                parsed = {"raw": body}
            return {"status_code": response.status, "response": parsed}
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise HTTPException(status_code=400, detail=f"Text send failed: {detail or exc.reason}") from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Text send failed: {exc}") from exc


def _serialize_communication_account(row: sqlite3.Row) -> Dict[str, Any]:
    account = dict(row)
    for key in ("use_account_credentials", "send_enabled", "call_enabled", "text_enabled", "verify_ssl"):
        account[key] = _bool_db(account.get(key))
    if account.get("access_token"):
        account["access_token_masked"] = f"{account['access_token'][:6]}...{account['access_token'][-4:]}" if len(account["access_token"]) > 12 else "configured"
        account["access_token"] = ""
    if account.get("client_secret"):
        account["client_secret_masked"] = f"{account['client_secret'][:4]}...{account['client_secret'][-4:]}" if len(account["client_secret"]) > 8 else "configured"
        account["client_secret"] = ""
    if account.get("webhook_secret"):
        account["webhook_secret_masked"] = "configured"
        account["webhook_secret"] = ""
    return account


def _zoom_headers(bearer_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _zoom_token(account: Dict[str, Any]) -> Dict[str, Any]:
    if not _bool_db(account.get("use_account_credentials")) and account.get("access_token"):
        return {"access_token": account.get("access_token"), "token_type": "bearer", "source": "static"}
    client_id = (account.get("client_id") or "").strip()
    client_secret = (account.get("client_secret") or "").strip()
    account_id = (account.get("account_id") or "").strip()
    token_url = (account.get("token_url") or "https://zoom.us/oauth/token").strip()
    if not client_id or not client_secret or not account_id:
        raise HTTPException(status_code=400, detail="Zoom credentials are incomplete")
    query = urllib_parse.urlencode({"grant_type": "account_credentials", "account_id": account_id})
    auth = b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    req = urllib_request.Request(
        f"{token_url}?{query}",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8") or "{}")
            payload["source"] = "account_credentials"
            return payload
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise HTTPException(status_code=400, detail=f"Zoom token exchange failed: {detail or exc.reason}") from exc
    except urllib_error.URLError as exc:
        raise HTTPException(status_code=400, detail=f"Zoom token exchange failed: {exc.reason}") from exc


def _zoom_request(account: Dict[str, Any], method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    token = _zoom_token(account)
    url = path if path.startswith("http://") or path.startswith("https://") else f"{(account.get('api_base') or 'https://api.zoom.us/v2').rstrip('/')}/{path.lstrip('/')}"
    request_data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib_request.Request(url, data=request_data, headers=_zoom_headers(token["access_token"]), method=method)
    try:
        with urllib_request.urlopen(req, timeout=30) as response:
            body = response.read().decode("utf-8", errors="ignore")
            return {"ok": True, "status": response.status, "data": json.loads(body) if body else {}, "token_source": token.get("source")}
    except urllib_error.HTTPError as exc:
        return {"ok": False, "status": exc.code, "error": exc.read().decode("utf-8", errors="ignore") or exc.reason}
    except urllib_error.URLError as exc:
        return {"ok": False, "status": 0, "error": str(exc.reason)}


def _zoom_webhook_tokens(payload: Dict[str, Any], secret: str) -> Dict[str, str]:
    plain_token = str(payload.get("payload", {}).get("plainToken") or payload.get("plainToken") or "")
    if not plain_token:
        return {"plainToken": "", "encryptedToken": ""}
    return {
        "plainToken": plain_token,
        "encryptedToken": hmac.new(secret.encode("utf-8"), plain_token.encode("utf-8"), hashlib.sha256).hexdigest(),
    }


def _zoom_verify_account(account: Dict[str, Any], recipient: Optional[str] = None) -> Dict[str, Any]:
    checks = {
        "token": _zoom_token(account),
        "call_logs": _zoom_request(account, "GET", "/phone/call_logs?page_size=1"),
        "sms_dry_run": {
            "ok": True,
            "status": 200,
            "data": {
                "would_post_to": f"{(account.get('api_base') or 'https://api.zoom.us/v2').rstrip('/')}/{(account.get('send_path') or '/phone/sms/messages').lstrip('/')}",
                "from": account.get("from_number"),
                "to": recipient or "",
                "send_enabled": _bool_db(account.get("send_enabled")),
            },
        },
        "capabilities": {
            "text_enabled": _bool_db(account.get("text_enabled")),
            "call_enabled": _bool_db(account.get("call_enabled")),
            "send_enabled": _bool_db(account.get("send_enabled")),
            "webhook_configured": bool(account.get("webhook_secret")),
        },
    }
    return checks


async def _generate_existing_brief_artifacts(conn: sqlite3.Connection, lead: Dict[str, Any], destination_dir: Path) -> List[Dict[str, Any]]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    artifacts = []
    for index, report_type in enumerate(("generic_seller_brief", "ai_appraisal_brief"), start=1):
        report = _generate_cotality_report(conn, lead, report_type)
        filename = f"{index:02d}_{report_type}.pdf"
        file_path = destination_dir / filename
        await html_to_pdf(report["html_content"], str(file_path))
        payload = dict(report.get("payload") or {})
        payload["pdf_artifact"] = {
            "file_name": filename,
            "file_path": str(file_path),
            "generated_at": now_iso(),
        }
        conn.execute(
            "UPDATE cotality_reports SET json_payload = ?, updated_at = ? WHERE id = ?",
            (json.dumps(payload), now_iso(), report["id"]),
        )
        artifacts.append({"type": report_type, "filename": filename, "path": str(file_path), "report_id": report["id"]})
    conn.commit()
    return artifacts


try:
    from report_pack_engine import create_report_pack as _create_report_pack
except ImportError:
    _create_report_pack = None

async def _generate_report_pack_for_lead(
    conn: sqlite3.Connection,
    lead: Dict[str, Any],
    *,
    include_existing_briefs: bool = True,
    output_root: Optional[str] = None,
) -> Dict[str, Any]:
    if _create_report_pack is None:
        raise RuntimeError("report_pack_engine is not available in this environment")
    manifest = await _create_report_pack(
        conn,
        lead,
        stock_root=STOCK_ROOT,
        brand_name=BRAND_NAME,
        brand_area=BRAND_AREA,
        brand_logo_url=BRAND_LOGO_URL,
        principal_name=PRINCIPAL_NAME,
        principal_email=PRINCIPAL_EMAIL,
        principal_phone=PRINCIPAL_PHONE,
        html_to_pdf=html_to_pdf,
        output_root=Path(output_root) if output_root else None,
    )
    if include_existing_briefs:
        pack_root = Path(manifest["pack_root"])
        existing_artifacts = await _generate_existing_brief_artifacts(conn, lead, pack_root / "02_existing_briefs")
        manifest["existing_briefs"] = existing_artifacts
        manifest_path = pack_root / "00_manifest" / "pack_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
