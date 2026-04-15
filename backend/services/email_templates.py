"""
Archetype-aware email template engine.
Returns subject + HTML body keyed by lead_archetype and context.
"""
from __future__ import annotations

import os
from string import Template
from typing import Any, Dict, Optional

PRINCIPAL_NAME = os.getenv("BRAND_PRINCIPAL_NAME", "Nitin Puri")
BRAND_NAME = "Laing+Simmons Oakville | Windsor"
BRAND_PHONE = os.getenv("BRAND_PRINCIPAL_PHONE", "0430 042 041")
BRAND_EMAIL = os.getenv("BRAND_PRINCIPAL_EMAIL", "oakville@lsre.com.au")

OWNIT1ST_OPERATOR_NAME = os.getenv("OWNIT1ST_OPERATOR_NAME", "Shahid")
OWNIT1ST_BRAND_NAME = "Ownit1st Loans"
OWNIT1ST_PHONE = os.getenv("OWNIT1ST_PHONE", "04 85 85 7881")
OWNIT1ST_EMAIL = os.getenv("OWNIT1ST_EMAIL", "info@ownit1stloans.com.au")
OWNIT1ST_WEBSITE = os.getenv("OWNIT1ST_WEBSITE", "https://ownit1stloans.com.au/")
CALENDLY_LS = os.getenv("CALENDLY_LS_APPRAISAL", "https://calendly.com/nitin-puri-lsre/30min")
CALENDLY_OWNIT1ST = os.getenv("CALENDLY_OWNIT1ST_LOAN", "https://calendly.com/ownit1st-loans/consultation")

def _make_signature(brand: str = "ls", signoff: str = "Warm regards,") -> str:
    """Return HTML signature block. brand='ls' for L+S, brand='ownit1st' for Ownit1st Loans."""
    if brand == "ownit1st":
        return f"""
<p style="margin-top:24px;color:#555;font-size:13px;line-height:1.5;">
{signoff}<br>
<strong>{OWNIT1ST_OPERATOR_NAME}</strong><br>
{OWNIT1ST_BRAND_NAME}<br>
<a href="tel:{OWNIT1ST_PHONE}">{OWNIT1ST_PHONE}</a> &middot;
<a href="mailto:{OWNIT1ST_EMAIL}">{OWNIT1ST_EMAIL}</a>
</p>
<p style="margin-top:12px;">
<a href="{CALENDLY_OWNIT1ST}" style="display:inline-block;background:#1a56db;color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-size:13px;font-weight:600;">Book a Free Loan Consultation &rarr;</a>
</p>
"""
    return f"""
<p style="margin-top:24px;color:#555;font-size:13px;line-height:1.5;">
{signoff}<br>
<strong>{PRINCIPAL_NAME}</strong><br>
{BRAND_NAME}<br>
<a href="tel:{BRAND_PHONE}">{BRAND_PHONE}</a> &middot;
<a href="mailto:{BRAND_EMAIL}">{BRAND_EMAIL}</a>
</p>
<p style="margin-top:12px;">
<a href="{CALENDLY_LS}" style="display:inline-block;background:#0f4c81;color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-size:13px;font-weight:600;">Book a Free Appraisal &rarr;</a>
</p>
"""

_SIGNATURE = _make_signature("ls")
_SIGNATURE_TALK_SOON = _make_signature("ls", signoff="Talk soon,")

_TEMPLATES: Dict[str, Dict[str, str]] = {
    "probate": {
        "subject": "Property at $address — Estate Administration Note",
        "body": """<p>Dear $first_name,</p>
<p>I hope this message reaches you at an appropriate time. I understand that managing an estate involves
many responsibilities, and I wanted to make myself available should property decisions need to be made
regarding <strong>$address</strong>.</p>
<p>Our team has supported many families through this process in $suburb — from private appraisals through
to discreet sale management. There is absolutely no obligation; I simply wanted you to know we are here
if needed.</p>
<p>If you would like a confidential conversation, please reach me directly at $phone.</p>
""" + _SIGNATURE,
    },
    "mortgage_cliff": {
        "subject": "Your fixed rate on $address — important window closing",
        "body": """<p>Hi $first_name,</p>
<p>I'm reaching out because properties settled in the same period as <strong>$address</strong> are
now approaching the end of their fixed-rate terms — typically rolling to rates 1.5–2% higher.</p>
<p>Many owners in $suburb are using this moment to review their equity position and decide whether to
refinance, sell, or restructure. I can prepare a complimentary market update showing current value range
and likely buyer demand for your property.</p>
<p>Would a 10-minute call this week work? No obligation — just data you can use.</p>
""" + _SIGNATURE,
    },
    "withdrawn_listing": {
        "subject": "Your property at $address — I noticed it came off the market",
        "body": """<p>Hi $first_name,</p>
<p>I noticed that <strong>$address</strong> recently came off the market. That can happen for many
reasons, and I wanted to reach out to see if there's anything I could help with.</p>
<p>Sometimes a fresh strategy, updated pricing, or a different marketing approach makes all the
difference. I specialise in $suburb and have recent buyer enquiry I could match against your property.</p>
<p>Happy to have a quick no-obligation chat if timing suits.</p>
""" + _SIGNATURE,
    },
    "development_potential": {
        "subject": "Development opportunity — $address, $suburb",
        "body": """<p>Hi $first_name,</p>
<p>I've been analysing recent subdivision and development approvals in $suburb and your property at
<strong>$address</strong> stands out as having genuine development potential under current zoning.</p>
<p>Developer and investor buyer demand for sites like yours is strong. I can prepare a brief showing
comparable site sales, likely price range, and the development pathway — at no cost to you.</p>
<p>If this is of interest, I'd be happy to talk through the numbers.</p>
""" + _SIGNATURE,
    },
    "competitor_displacement": {
        "subject": "Your listing at $address — appraisal offer",
        "body": """<p>Hi $first_name,</p>
<p>I noticed your property at <strong>$address</strong> is coming to the end of its current listing
agreement. Before you make a decision about next steps, I'd love the opportunity to show you what
Laing+Simmons can offer — in terms of buyer reach, recent comparable sales, and marketing strategy.</p>
<p>I'll bring a full market analysis and a clear plan. No pressure, just a better-informed conversation.</p>
""" + _SIGNATURE,
    },
    "default": {
        "subject": "Market update for $suburb — $address",
        "body": """<p>Hi $first_name,</p>
<p>I wanted to send a quick update on the $suburb market as it relates to your property at
<strong>$address</strong>. Buyer demand has shifted recently and I have some data I think you'd
find useful.</p>
<p>Would a brief call this week work? I can cover value range, recent comparable sales, and likely
buyer profile — all in under 10 minutes.</p>
""" + _SIGNATURE,
    },
    # Door-knock follow-up: operator has already met the homeowner face-to-face.
    # Keeps Shahid's voice verbatim — grammar/phrasing preserved on purpose.
    # Uses "Talk soon," signoff instead of "Warm regards,".
    # $cotality_block is injected at render time (empty string if no estimate available).
    "doorknock_followup": {
        "subject": "Thanks for opening the door — $address",
        "body": """<p>Hey $first_name,</p>
<p>Really good seeing you at <strong>$address</strong>. Thanks for opening the door and having a chat, most people don't, so I appreciate it.</p>
<p>I've pulled a quick property report for you to scan through.</p>
<p>While, it is a helpful baseline, but it's also utterly and purely "AI slop". It really only sums up recent comparable sales, and to be honest, it would probably overvalue a cardboard box, if it lived in the right postcode.</p>
$cotality_block
<p>Until AI develops actual taste and can walk through a front door, I'd love to stop by and give you a much sharper valuation.</p>
""" + _SIGNATURE_TALK_SOON,
    },
    # Cold intro version: homeowner has NOT been visited yet.
    # Same voice/jokes, different opener that doesn't pretend we've met.
    "cold_avm_intro": {
        "subject": "Quick honest note on $address",
        "body": """<p>Hey $first_name,</p>
<p>I was running the numbers on $suburb this week and your property at <strong>$address</strong> kept coming back to the top of my list. Figured I'd drop you a line directly rather than let another generic appraisal letter land in your letterbox.</p>
<p>I've attached a quick property report for you to scan through.</p>
<p>While, it is a helpful baseline, but it's also utterly and purely "AI slop". It really only sums up recent comparable sales, and to be honest, it would probably overvalue a cardboard box, if it lived in the right postcode.</p>
$cotality_block
<p>Until AI develops actual taste and can walk through a front door, I'd love to stop by and give you a much sharper valuation in person.</p>
""" + _SIGNATURE_TALK_SOON,
    },
}


def _format_cotality_estimate(value: Any) -> Optional[str]:
    """Format a Cotality AVM estimate as a display string like '$1,280,000'."""
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
        if numeric <= 0:
            return None
        return f"${int(round(numeric)):,}"
    except (TypeError, ValueError):
        # Already a formatted string like "$1.28M" — pass through
        text = str(value).strip()
        return text or None


def _build_cotality_block(lead: Dict[str, Any]) -> str:
    """Return the optional 'For the record...' paragraph, or empty string if no estimate."""
    estimate = _format_cotality_estimate(lead.get("cotality_estimate"))
    if not estimate:
        return ""
    return (
        '<p>For the record, its number on your place is <strong>'
        + estimate
        + '</strong>. The real number depends entirely on things a spreadsheet cannot see, '
        'and what your block is doing to buyer emotion right now.</p>'
    )


def render_template(
    archetype: Optional[str],
    lead: Dict[str, Any],
    override_subject: Optional[str] = None,
    override_body: Optional[str] = None,
    brand: str = "ls",
) -> Dict[str, str]:
    """
    Render an email template for the given lead archetype.
    brand='ls' uses L+S / Nitin Puri identity.
    brand='ownit1st' uses Ownit1st / Shahid identity.
    Returns {"subject": ..., "body": ...} with HTML body.
    """
    key = (archetype or "default").lower().replace(" ", "_").replace("-", "_")
    tmpl = _TEMPLATES.get(key, _TEMPLATES["default"])

    first_name = (lead.get("owner_name") or "there").split()[0]
    address = lead.get("address") or "your property"
    suburb = lead.get("suburb") or "the area"
    default_phone = OWNIT1ST_PHONE if brand == "ownit1st" else BRAND_PHONE
    phone = lead.get("contact_phones", [default_phone])[0] if lead.get("contact_phones") else default_phone

    ctx = {
        "first_name": first_name,
        "address": address,
        "suburb": suburb,
        "phone": phone,
        "cotality_block": _build_cotality_block(lead),
    }

    subject = override_subject or Template(tmpl["subject"]).safe_substitute(ctx)
    raw_body = override_body or tmpl["body"]
    # Swap in the correct signature for each supported signoff
    sig_default = _make_signature(brand)
    sig_talk_soon = _make_signature(brand, signoff="Talk soon,")
    body = raw_body.replace(_SIGNATURE, sig_default).replace(_SIGNATURE_TALK_SOON, sig_talk_soon)
    body = Template(body).safe_substitute(ctx)

    return {"subject": subject, "body": body}
