"""
CMA Generator — Comparative Market Analysis via Gemini.

Data pipeline before any AI call:
  1. Pull recent comparable sales from Domain API (last 90 days, same suburb)
     — filtered to matching property type where possible
  2. Pull suburb median price / composition from local Cotality xlsx (suburb_intel_service)
  3. Inject both as concrete facts into the Gemini prompt

If Domain API is in sandbox (or credentials missing), comparables come from Cotality only.
If Cotality xlsx is missing, Domain comparables carry the analysis.
Gemini is always told exactly what data it has and must work from it — never invent.

Public interface:
    generate_cma(lead: dict, session: AsyncSession) -> dict
    generate_cma_for_lead_id(session: AsyncSession, lead_id: str) -> dict
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import BASE_URL, PRINCIPAL_NAME, BRAND_NAME
from core.utils import now_iso
from services.ai_router import _call_gemini
from services.suburb_intel_service import get_suburb_intel

logger = logging.getLogger(__name__)


# ─── Property type normalisation ─────────────────────────────────────────────

_TYPE_GROUPS: dict[str, list[str]] = {
    "house":     ["house", "villa", "townhouse", "semi", "terrace"],
    "unit":      ["unit", "apartment", "flat", "studio"],
    "land":      ["land", "lot", "vacant"],
    "rural":     ["rural", "acreage", "farm"],
}


def _normalise_prop_type(raw: str) -> str:
    """Return 'house' | 'unit' | 'land' | 'rural' | 'unknown'."""
    if not raw:
        return "unknown"
    low = raw.lower()
    for group, keywords in _TYPE_GROUPS.items():
        if any(k in low for k in keywords):
            return group
    return "unknown"


def _filter_comps_by_type(comps: list[dict], target_type: str) -> list[dict]:
    """Return comps matching target_type; fall back to all if none match."""
    if target_type == "unknown" or not comps:
        return comps
    matched = [c for c in comps if _normalise_prop_type(c.get("property_type", "")) == target_type]
    return matched if matched else comps


def _fmt_price(p: Optional[int]) -> str:
    if not p:
        return "unknown"
    if p >= 1_000_000:
        return f"${p / 1_000_000:.2f}M"
    return f"${p:,}"


# ─── Prompt builder ──────────────────────────────────────────────────────────

def _build_cma_prompt(
    lead: dict,
    comparables: list[dict],
    suburb_intel: dict,
) -> str:
    address = lead.get("address") or "the property"
    suburb = lead.get("suburb") or "the suburb"
    owner_name = lead.get("owner_name") or "the owner"
    zoning = lead.get("zoning_type") or lead.get("development_zone") or "residential"
    lot_sqm = lead.get("land_size_sqm") or 0
    beds = lead.get("bedrooms")
    baths = lead.get("bathrooms")
    settlement_date = lead.get("settlement_date") or lead.get("last_settlement_date") or ""
    trigger_type = lead.get("trigger_type") or ""
    est_value = lead.get("est_value") or 0
    prop_type_raw = lead.get("property_type") or lead.get("zoning_type") or ""

    # ── Property line ─────────────────────────────────────────────────────────
    prop_lines = [f"- Address: {address}", f"- Suburb: {suburb}, NSW, Australia", f"- Owner: {owner_name}"]
    if prop_type_raw:
        prop_lines.append(f"- Property type: {prop_type_raw}")
    if beds is not None:
        prop_lines.append(f"- Bedrooms: {int(beds)}")
    if baths is not None:
        prop_lines.append(f"- Bathrooms: {int(baths)}")
    if lot_sqm:
        prop_lines.append(f"- Lot size: {lot_sqm:.0f} sqm")
    if zoning:
        prop_lines.append(f"- Zoning: {zoning}")
    if settlement_date:
        prop_lines.append(f"- Settlement date: {settlement_date}")
    if trigger_type:
        prop_lines.append(f"- Trigger signal: {trigger_type}")
    if est_value and est_value > 0:
        prop_lines.append(f"- Domain estimated value: {_fmt_price(est_value)}")

    # ── Suburb data from Cotality xlsx ────────────────────────────────────────
    suburb_block = ""
    if suburb_intel:
        lines = [f"SUBURB MARKET DATA — {suburb.upper()} (Source: Cotality/CoreLogic):"]
        median_recent = suburb_intel.get("median_price_recent")
        median_all = suburb_intel.get("median_price")
        if median_recent:
            lines.append(f"  Median sale price (recent 3y): {_fmt_price(median_recent)}")
        elif median_all:
            lines.append(f"  Median sale price (all time): {_fmt_price(median_all)}")
        if suburb_intel.get("median_land_size"):
            lines.append(f"  Median land size: {suburb_intel['median_land_size']} sqm")
        house_pct = suburb_intel.get("house_pct")
        unit_pct = suburb_intel.get("unit_pct")
        if house_pct is not None:
            lines.append(f"  Property mix: {house_pct}% houses, {unit_pct or 0}% units")
        if suburb_intel.get("top_zone"):
            lines.append(f"  Dominant zone: {suburb_intel['top_zone']}")
        recent_count = suburb_intel.get("recent_5y_count")
        if recent_count:
            lines.append(f"  Sales in past 5 years: {recent_count}")
        suburb_block = "\n".join(lines)
    else:
        suburb_block = f"SUBURB MARKET DATA: No Cotality data available for {suburb}."

    # ── Comparable sales ──────────────────────────────────────────────────────
    if comparables:
        comp_lines = ["RECENT COMPARABLE SALES (last 90 days, same suburb — use these directly):"]
        for c in comparables:
            beds_str = f"{c['bedrooms']}bd " if c.get("bedrooms") else ""
            type_str = f"{c.get('property_type', '')} " if c.get("property_type") else ""
            comp_lines.append(
                f"  • {c['address']} — {type_str}{beds_str}"
                f"sold {_fmt_price(c['sold_price'])} on {c['sold_date']}"
            )
        comps_block = "\n".join(comp_lines)
    else:
        comps_block = "RECENT COMPARABLE SALES: Domain API unavailable — base value_range on Cotality median only."

    # ── Value instruction ─────────────────────────────────────────────────────
    if est_value and est_value > 0:
        value_instruction = (
            f"Domain estimates {_fmt_price(est_value)}. "
            "Cross-reference against the comparable sales above and suburb median to derive a realistic value_range (±8–12% band). "
            "If comparables support a different range, explain briefly in market_position."
        )
    elif suburb_intel.get("median_price_recent"):
        value_instruction = (
            f"No Domain estimate available. Suburb median is {_fmt_price(suburb_intel['median_price_recent'])}. "
            "Use this plus comparable sales to estimate value_range. "
            "If insufficient data, set value_range to 'market appraisal required'."
        )
    else:
        value_instruction = "No value data available — set value_range to 'market appraisal required'."

    prompt = f"""You are preparing a Comparative Market Analysis (CMA) for a property owner outreach by {PRINCIPAL_NAME}, {BRAND_NAME}.

PROPERTY DETAILS:
{chr(10).join(prop_lines)}

{suburb_block}

{comps_block}

VALUE GUIDANCE:
{value_instruction}

RULES:
- Use ONLY the data provided above — work from the comparable sales and suburb stats given.
- If data is provided, reference it. If not, say so plainly.
- Keep copy professional and in keeping with the Laing+Simmons brand voice.
- Do NOT mention Ownit1st, Shahid, Hills Intelligence Hub, or any internal system name.
- call_script_opening is for {PRINCIPAL_NAME} to use verbatim — factual, warm, no roleplay instructions.
- next_step_cta is a short sentence for the homeowner encouraging them to engage.
- market_position should reference 1–2 specific comparable sales if available.

Return ONLY a JSON object with exactly these keys:
{{
  "headline": "Short compelling headline about this specific property opportunity",
  "market_position": "2-3 sentences on current market conditions in {suburb}, referencing comparable sales if available",
  "why_now": "1-2 sentences on why NOW is a good time to consider selling, specific to this lead",
  "value_range": "estimated market value range as a string e.g. $1.1M–$1.3M",
  "next_step_cta": "Short call to action for the homeowner",
  "call_script_opening": "First 2 sentences for {PRINCIPAL_NAME} to open the call — reference address and one market fact"
}}

Output raw JSON only — no markdown, no preamble."""

    return prompt


# ─── SMS draft builder ───────────────────────────────────────────────────────

def _build_sms_draft(lead: dict, cma_link: str) -> str:
    owner_name = (lead.get("owner_name") or "").split()[0] if lead.get("owner_name") else ""
    address = lead.get("address") or "your property"
    first_name = owner_name if owner_name else "there"
    principal_first = (PRINCIPAL_NAME or "").split()[0] or "Nitin"

    return (
        f"Hi {first_name}, {principal_first} from Laing+Simmons Oakville here. "
        f"I've put together a quick market update for your property at {address} — "
        f"worth 2 mins if you're curious what's happening nearby: {cma_link}. "
        f"Happy to chat anytime."
    )


# ─── Core generator ──────────────────────────────────────────────────────────

async def generate_cma(lead: dict, session: Optional[AsyncSession] = None) -> dict:
    """
    Generate CMA content for a lead using Gemini.

    Data gathering order:
      1. Fetch Domain comparables (suburb + postcode)
      2. Read Cotality xlsx suburb stats (local, free, instant)
      3. Build enriched prompt
      4. Call Gemini

    Returns structured dict with source="gemini" or source="fallback".
    """
    lead_id = lead.get("id") or ""
    address = lead.get("address") or "the property"
    suburb = lead.get("suburb") or ""
    postcode = lead.get("postcode") or ""
    owner_name = lead.get("owner_name") or ""
    prop_type_raw = lead.get("property_type") or lead.get("zoning_type") or ""
    prop_type = _normalise_prop_type(prop_type_raw)

    cma_link = f"{BASE_URL}/cma/{lead_id}" if lead_id else f"{BASE_URL}/cma/view"
    sms_draft = _build_sms_draft(lead, cma_link)

    # ── 1. Comparable sales from Domain API ───────────────────────────────────
    comparables: list[dict] = []
    if suburb:
        try:
            from services.domain_enrichment import get_suburb_comparables
            raw_comps = await get_suburb_comparables(suburb, postcode)
            # Filter to matching property type; fall back to all if no match
            comparables = _filter_comps_by_type(raw_comps, prop_type)
            logger.info(
                "[CMA] lead=%s suburb=%s comparables=%d (type=%s)",
                lead_id, suburb, len(comparables), prop_type,
            )
        except Exception as exc:
            logger.warning("[CMA] Domain comparables fetch failed: %s", exc)

    # ── 2. Cotality xlsx suburb stats ─────────────────────────────────────────
    suburb_intel: dict = {}
    if suburb:
        try:
            suburb_intel = get_suburb_intel(suburb) or {}
        except Exception as exc:
            logger.warning("[CMA] suburb_intel fetch failed: %s", exc)

    # ── 3. Build prompt with real data ────────────────────────────────────────
    prompt = _build_cma_prompt(lead, comparables, suburb_intel)
    system = (
        f"You are an AI assistant for {BRAND_NAME}. "
        f"The principal is {PRINCIPAL_NAME}. "
        "Be concise, factual, and professional. "
        "Work strictly from the data provided. "
        "Output raw JSON only."
    )

    # ── 4. Call Gemini ────────────────────────────────────────────────────────
    raw = await _call_gemini(prompt, system)

    if not raw:
        logger.warning("[CMA] Gemini returned empty response for lead=%s (key missing or rate limited)", lead_id)
        return {
            "id": str(uuid.uuid4()),
            "headline": f"Market analysis — {address}",
            "market_position": "",
            "why_now": "",
            "value_range": "market appraisal required",
            "next_step_cta": "",
            "call_script_opening": "",
            "sms_draft": sms_draft,
            "comparables": comparables,
            "suburb_stats": suburb_intel,
            "source": "fallback",
            "lead_id": lead_id,
            "address": address,
            "owner_name": owner_name,
            "cma_link": cma_link,
            "generated_at": now_iso(),
        }

    # Strip markdown fences if Gemini wraps output despite instruction
    clean = raw.strip()
    if clean.startswith("```"):
        clean = clean.lstrip("`").lstrip("json").strip().rstrip("`").strip()

    try:
        parsed: dict = json.loads(clean)
    except Exception as exc:
        logger.warning("[CMA] JSON parse failed for lead=%s: %s — raw=%s", lead_id, exc, raw[:200])
        return {
            "id": str(uuid.uuid4()),
            "headline": f"Market analysis — {address}",
            "market_position": raw[:300] if raw else "",
            "why_now": "",
            "value_range": "market appraisal required",
            "next_step_cta": "",
            "call_script_opening": "",
            "sms_draft": sms_draft,
            "comparables": comparables,
            "suburb_stats": suburb_intel,
            "source": "gemini_parse_error",
            "lead_id": lead_id,
            "address": address,
            "owner_name": owner_name,
            "cma_link": cma_link,
            "generated_at": now_iso(),
        }

    cma: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "headline": parsed.get("headline", ""),
        "market_position": parsed.get("market_position", ""),
        "why_now": parsed.get("why_now", ""),
        "value_range": parsed.get("value_range", ""),
        "next_step_cta": parsed.get("next_step_cta", ""),
        "call_script_opening": parsed.get("call_script_opening", ""),
        "sms_draft": sms_draft,
        "comparables": comparables,
        "suburb_stats": suburb_intel,
        "source": "gemini",
        "lead_id": lead_id,
        "address": address,
        "owner_name": owner_name,
        "cma_link": cma_link,
        "generated_at": now_iso(),
    }
    return cma


# ─── DB-aware entry point ────────────────────────────────────────────────────

async def generate_cma_for_lead_id(session: AsyncSession, lead_id: str) -> dict:
    """
    Fetch lead from DB, run generate_cma (with real comparables + Cotality data),
    store result in leads.stage_note as JSON, and return the CMA dict.
    """
    from fastapi import HTTPException

    row = (
        await session.execute(
            text("SELECT * FROM leads WHERE id = :id"),
            {"id": lead_id},
        )
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Lead {lead_id} not found")

    lead = dict(row)
    cma = await generate_cma(lead, session=session)

    # Append to any existing reports list in stage_note
    existing_raw = lead.get("stage_note") or "[]"
    try:
        existing = json.loads(existing_raw)
        if not isinstance(existing, list):
            existing = []
    except Exception:
        existing = []

    # Keep last 5 reports
    existing.append(cma)
    existing = existing[-5:]

    await session.execute(
        text(
            "UPDATE leads SET stage_note = :stage_note, updated_at = :updated_at WHERE id = :id"
        ),
        {"stage_note": json.dumps(existing), "updated_at": now_iso(), "id": lead_id},
    )
    await session.commit()

    return cma
