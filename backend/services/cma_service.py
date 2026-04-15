"""
CMA Service — 1-page Comparative Market Analysis via Gemini.

Builds on the property terminal (suburb report + withdrawn analysis + opportunity
insights) and feeds that structured context into Gemini to produce operator-ready
talking points, a pre-drafted SMS, and a market paragraph — all specific to this
address.

The output is a JSON payload the frontend can render directly or store against
the lead.  No PDF is generated here; if the operator wants a PDF, they send the
CMA JSON to the velvet_engine separately.

Route: POST /api/outreach/cma/{lead_id}
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import BRAND_NAME, PRINCIPAL_NAME, PRINCIPAL_PHONE
from services.ai_router import ask
from services.property_terminal_service import get_property_terminal

logger = logging.getLogger(__name__)


def _fmt(value: Any, suffix: str = "") -> str:
    if value is None:
        return "–"
    try:
        return f"{int(round(float(value))):,}{suffix}"
    except (TypeError, ValueError):
        return str(value)


def _build_cma_prompt(lead: Dict[str, Any], terminal: Dict[str, Any]) -> str:
    mc = terminal.get("market_context", {})
    oi = terminal.get("opportunity_insights", [])
    wa = terminal.get("withdrawn_analysis", {})

    address = lead.get("address", "the property")
    suburb = lead.get("suburb", "the suburb")
    owner = (lead.get("owner_name") or "the owner").split(" ")[0].title()
    archetype = lead.get("archetype") or lead.get("trigger_type") or "off_market"
    why_now = lead.get("why_now") or wa.get("headline") or ""
    est_value = lead.get("est_value")
    bedrooms = lead.get("bedrooms")
    bathrooms = lead.get("bathrooms")
    land_sqm = lead.get("land_size_sqm")

    property_summary = (
        f"Address: {address}\n"
        f"Suburb: {suburb}\n"
        f"Owner first name: {owner}\n"
        f"Signal / archetype: {archetype}\n"
        f"Why-now note: {why_now}\n"
        f"Estimated value: {_fmt(est_value, '') if est_value else 'unknown'}\n"
        f"Bedrooms: {_fmt(bedrooms)}, Bathrooms: {_fmt(bathrooms)}, "
        f"Land: {_fmt(land_sqm, ' sqm')}\n"
    )

    market_summary = (
        f"Suburb median sale price: {mc.get('suburb_median_sale_label', '–')}\n"
        f"Suburb record count in report: {_fmt(mc.get('suburb_record_count'))}\n"
        f"Match type against suburb report: {mc.get('match_type', 'unknown')}\n"
        f"Same-street records: {_fmt(mc.get('same_street_count'))}\n"
        f"Suburb median land size: {mc.get('suburb_median_land_label', '–')}\n"
        f"Suburb median bedrooms: {mc.get('suburb_median_bedrooms_label', '–')}\n"
    )

    insights_block = "\n".join(f"- {line}" for line in oi) if oi else "- No specific insights computed."

    brand_context = (
        f"Agent: {PRINCIPAL_NAME}\n"
        f"Agency: {BRAND_NAME}\n"
        f"Phone: {PRINCIPAL_PHONE}\n"
    )

    return f"""You are a senior real estate analyst writing a 1-page Comparative Market Analysis (CMA) briefing for an agent about to call {owner} regarding {address}.

PROPERTY DATA:
{property_summary}
SUBURB MARKET DATA:
{market_summary}
PRE-COMPUTED OPPORTUNITY INSIGHTS (use these to anchor your talking points):
{insights_block}
AGENT IDENTITY:
{brand_context}

YOUR TASK:
Return ONLY a valid JSON object with exactly these fields:

{{
  "headline": "<One punchy sentence stating the key market position of this property — no agent names, no fluff>",
  "market_paragraph": "<2-3 sentences describing what the suburb data shows and how this property fits in. Specific numbers only. No generic phrases.>",
  "talking_points": [
    "<Talking point 1 — a specific, evidence-anchored observation the agent can use on the call>",
    "<Talking point 2>",
    "<Talking point 3>"
  ],
  "sms_text": "<Single SMS under 155 chars. From {PRINCIPAL_NAME}. Must reference the address or suburb. Natural, not salesy. End with a question to invite a reply.>",
  "email_subject": "<Subject line for a follow-up email — concise, property-specific>"
}}

Rules:
- Use only the data provided. Do not invent sale prices, dates, or comparable properties.
- talking_points must be usable verbatim on a call — no filler.
- sms_text must not contain exclamation marks, emojis, or generic phrases like "great opportunity".
- Return raw JSON only. No markdown fences. No preamble."""


def _extract_json(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    # Strip any accidental markdown fences
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find the first { ... } block
        m = re.search(r"\{[\s\S]+\}", cleaned)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return None


def _fallback_cma(lead: Dict[str, Any], terminal: Dict[str, Any]) -> Dict[str, Any]:
    """Rule-based fallback when AI is unavailable or returns unparseable output."""
    mc = terminal.get("market_context", {})
    oi = terminal.get("opportunity_insights", [])
    address = lead.get("address", "the property")
    suburb = lead.get("suburb", "this suburb")
    owner = (lead.get("owner_name") or "there").split(" ")[0].title()
    median = mc.get("suburb_median_sale_label", "–")

    return {
        "headline": f"{address} — positioned against a {median} suburb median in {suburb}.",
        "market_paragraph": (
            f"The suburb report contains {_fmt(mc.get('suburb_record_count'))} records for {suburb}. "
            f"The median sale price is {median}. "
            f"{oi[0] if oi else 'No additional market context is available at this time.'}"
        ),
        "talking_points": [i for i in oi[:3]] if oi else [
            "Review the suburb report for comparable sales before calling.",
            "Confirm the estimated value against Domain AVM data.",
            "Ask if they have spoken to any other agents recently.",
        ],
        "sms_text": (
            f"Hi {owner}, it's {PRINCIPAL_NAME} from {BRAND_NAME}. "
            f"I've been looking at recent activity around {address.split(',')[0]} — "
            f"would you be open to a quick chat about the market?"
        )[:155],
        "email_subject": f"Quick market update for {address.split(',')[0]}",
        "_ai_used": False,
        "_fallback_reason": "AI tier unavailable or returned unparseable output.",
    }


async def generate_cma(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    """
    Primary entry point.  Returns a structured CMA dict.
    Raises HTTPException 404 if lead not found (propagated from get_property_terminal).
    """
    # 1. Terminal gives us suburb report + opportunity insights for free
    terminal_payload = await get_property_terminal(session, lead_id)
    lead = terminal_payload["lead"]
    terminal = terminal_payload["terminal"]

    # 2. Build prompt and call AI
    prompt = _build_cma_prompt(lead, terminal)
    raw = await ask(task="report_content", prompt=prompt)

    # 3. Parse AI response
    ai_data = _extract_json(raw)

    if ai_data and all(k in ai_data for k in ("headline", "talking_points", "sms_text")):
        cma = {
            **ai_data,
            "_ai_used": True,
            "_ai_raw_length": len(raw) if raw else 0,
        }
    else:
        logger.warning("[CMA] AI parse failed for lead %s — using fallback", lead_id)
        cma = _fallback_cma(lead, terminal)

    return {
        "lead_id": lead_id,
        "address": lead.get("address"),
        "suburb": lead.get("suburb"),
        "est_value": lead.get("est_value"),
        "heat_score": lead.get("heat_score"),
        "archetype": lead.get("archetype") or lead.get("trigger_type"),
        "market_context": terminal.get("market_context"),
        "cma": cma,
    }
