"""
Underwriter Service — AI-backed lead brief generation.

Produces a LeadBrief: structured decision-ready output for the operator
covering why this lead matters, what to say, evidence bullets, risk flags,
and outreach drafts.

Uses ai_router.ask() for generation. Falls back to rule-based snapshot
if all AI tiers are unavailable. Caches results in-process for 24h TTL
(1h for WITHDRAWN/PROBATE signals).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from .urgency_helper import calculate_lead_urgency

logger = logging.getLogger(__name__)

# ─── In-process brief cache ─────────────────────────────────────────────────

_BRIEF_CACHE: dict[str, dict] = {}     # lead_id → {brief, expires_at}
_TTL_NORMAL = 86400        # 24h
_TTL_HOT    = 3600         # 1h for WITHDRAWN/PROBATE signals


def _cache_ttl(status: str) -> int:
    return _TTL_HOT if status.upper() in {"WITHDRAWN", "PROBATE", "EXPIRED"} else _TTL_NORMAL


def _cached_brief(lead_id: str) -> Optional[dict]:
    entry = _BRIEF_CACHE.get(lead_id)
    if not entry:
        return None
    if time.time() > entry["expires_at"]:
        del _BRIEF_CACHE[lead_id]
        return None
    return entry["brief"]


def _cache_brief(lead_id: str, brief: dict, status: str) -> None:
    _BRIEF_CACHE[lead_id] = {
        "brief": brief,
        "expires_at": time.time() + _cache_ttl(status),
    }


def invalidate_brief(lead_ids: list[str]) -> None:
    """Called by HERMES or lead-update webhook to force re-generation."""
    for lid in lead_ids:
        _BRIEF_CACHE.pop(lid, None)


# ─── Formatting helpers ──────────────────────────────────────────────────────

def _format_currency(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError:
            return None
    else:
        numeric = float(value)
    return f"${int(numeric):,}"


def _collect_signals(lead: Mapping[str, Any], value_label: str | None, status: str, signal_date: str | None) -> list[str]:
    entries: list[str] = []
    if status:
        entries.append(f"Signal status: {status}")
    if trigger := lead.get("trigger_type"):
        entries.append(f"Trigger: {trigger}")
    insignal = signal_date or lead.get("created_at")
    if insignal:
        entries.append(f"Signal recorded: {insignal}")
    if days_on_market := lead.get("days_on_market"):
        entries.append(f"Days on market: {int(days_on_market)}d")
    if value_label:
        entries.append(f"Estimated value: {value_label}")
    if lead.get("last_contacted_at"):
        entries.append(f"Last contacted: {lead['last_contacted_at']}")
    if lead.get("last_outcome"):
        entries.append(f"Last outcome: {lead['last_outcome']}")
    if lead.get("suburb"):
        entries.append(f"Suburb: {lead['suburb']}")
    if beds := lead.get("bedrooms"):
        baths = lead.get("bathrooms")
        cars = lead.get("car_spaces")
        spec_parts = [f"{int(beds)} bed" if isinstance(beds, (int, float)) else f"{beds} bed"]
        if baths:
            spec_parts.append(f"{int(baths)} bath")
        if cars:
            spec_parts.append(f"{int(cars)} car")
        entries.append(" / ".join(spec_parts))
    if not entries:
        entries.append("Signal metadata pending")
    return entries


def _collect_risks(status: str, dom: Any, days_since_contact: int | None) -> list[str]:
    risks: list[str] = []
    if dom is not None:
        try:
            dom_val = int(dom)
        except (TypeError, ValueError):
            dom_val = None
        if dom_val is not None:
            if dom_val > 60:
                risks.append("Long DOM suggests pricing mismatch or listing fatigue.")
            elif dom_val > 30:
                risks.append("Extended DOM may mean buyer interest is cooling.")
    if days_since_contact is not None and days_since_contact > 14:
        risks.append(f"No contact in {days_since_contact} days risks the lead cooling off.")
    if status in {"WITHDRAWN", "EXPIRED"}:
        risks.append("Seller withdrew or expired — there may be agent friction or pricing trouble.")
    return risks


# ─── Rule-based snapshot (AI fallback) ──────────────────────────────────────

def generate_underwriter_snapshot(lead: Mapping[str, Any]) -> dict[str, Any]:
    urgency_ctx = calculate_lead_urgency(lead)
    status = urgency_ctx.get("effective_status") or ""
    time_context = urgency_ctx.get("time_context", "")
    reason = lead.get("why_now") or urgency_ctx.get("fallback_reason")
    value_label = _format_currency(lead.get("estimated_value") or lead.get("est_value"))
    address = lead.get("address") or lead.get("suburb") or "Lead"
    status_phrase = status.lower() if status else "recent"
    time_phrase = time_context.lower() if time_context else "recently"
    summary = (
        f"{address} has a {status_phrase} signal {time_phrase} "
        f"with an estimated value of {value_label or 'unconfirmed value'}."
    )
    why_it_matters = reason or f"{status or 'Signal'} captured {time_phrase}."
    signal_date = lead.get("signal_date") or lead.get("created_at")
    key_signals = _collect_signals(lead, value_label, status, signal_date)
    risks = _collect_risks(status, lead.get("days_on_market"), urgency_ctx.get("days_since_contact"))
    urgency = urgency_ctx.get("urgency", "LOW")
    recommended_action = (
        "Call today to re-open the conversation while this signal is fresh."
        if urgency == "HIGH"
        else "Schedule a follow-up within the next week to keep momentum moving."
        if urgency == "MEDIUM"
        else "Monitor for new signals before reaching back out."
    )
    call_angle = reason or f"Open by referencing the {status or 'recent'} signal and ask what has changed."

    return {
        "summary": summary,
        "why_it_matters": why_it_matters,
        "urgency": urgency,
        "key_signals": key_signals,
        "risks": risks,
        "recommended_action": recommended_action,
        "call_angle": call_angle,
    }


# ─── Prompt builder ──────────────────────────────────────────────────────────

def _build_brief_prompt(lead: dict[str, Any]) -> str:
    addr = lead.get("address") or lead.get("suburb") or "Unknown address"
    suburb = lead.get("suburb", "")
    owner = lead.get("owner_name") or lead.get("contact_name") or "Unknown owner"
    phone = lead.get("phone") or lead.get("mobile") or "No phone"
    status = lead.get("status") or lead.get("trigger_type") or "Unknown"
    dom = lead.get("days_on_market")
    est_val = _format_currency(lead.get("estimated_value") or lead.get("est_value"))
    last_contact = lead.get("last_contacted_at", "Never")
    last_outcome = lead.get("last_outcome", "None")
    why_now_existing = lead.get("why_now", "")
    heat = lead.get("heat_score") or lead.get("call_today_score", 0)

    context_lines = [
        f"Address: {addr}",
        f"Suburb: {suburb}",
        f"Owner: {owner}",
        f"Phone: {phone}",
        f"Signal/Status: {status}",
        f"Estimated value: {est_val or 'unknown'}",
        f"Days on market: {dom or 'unknown'}",
        f"Last contacted: {last_contact}",
        f"Last outcome: {last_outcome}",
        f"Heat score: {heat}",
    ]
    if why_now_existing:
        context_lines.append(f"Existing why_now note: {why_now_existing}")

    context = "\n".join(context_lines)

    return f"""You are the Underwriter for Laing+Simmons Oakville | Windsor. Your job is to produce a decision brief for operator Shahid before he calls this lead.

LEAD DATA:
{context}

Return ONLY valid JSON (no markdown, no prose). Use this exact structure:
{{
  "operator_brief": "2-3 sentences: who this is, why they matter today, what angle to use",
  "call_opening": "First sentence to say on the phone. Specific, warm, not generic.",
  "objection_handling": "Most likely objection + how to handle it in one sentence",
  "urgency_reason": "Why THIS lead TODAY and not tomorrow. One sentence.",
  "evidence_bullets": ["Max 4 plain-English facts that support the call angle"],
  "risk_flags": ["What might go wrong — empty contact, wrong owner, seller friction etc"],
  "confidence": "high | medium | low",
  "missing_data": ["What data would improve this brief"],
  "sms_draft": "Ready-to-send SMS. Under 160 chars. From Shahid at L+S.",
  "next_action": "Specific action: 'Call today before 2pm' not 'Follow up'",
  "next_action_channel": "call | sms | email"
}}

Rules:
- operator_brief must name the address and the specific signal (WITHDRAWN, EXPIRED, PROBATE, etc)
- call_opening must NOT start with "Hi, I'm calling about your property"
- sms_draft must NOT contain the owner name if it's unknown
- confidence=high only if phone + owner name + clear signal all present
- Keep everything factual. Do not invent data not in the lead."""


# ─── AI-backed generation ────────────────────────────────────────────────────

def _parse_brief_response(raw: str, lead: dict) -> dict | None:
    """Extract JSON from AI response. Returns None if unparseable."""
    raw = raw.strip()
    # Strip markdown code blocks if present
    if raw.startswith("```"):
        lines = raw.splitlines()
        raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    try:
        parsed = json.loads(raw)
        # Must have at minimum operator_brief
        if isinstance(parsed, dict) and parsed.get("operator_brief"):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return None


async def _generate_ai_brief(lead: dict) -> dict | None:
    """Call ai_router to generate a structured brief. Returns None on failure."""
    try:
        from services.ai_router import ask
        prompt = _build_brief_prompt(lead)
        raw = await ask(task="operator_brief", prompt=prompt, lead=lead)
        if not raw:
            return None
        return _parse_brief_response(raw, lead)
    except Exception as exc:
        logger.warning(f"[Underwriter] AI generation failed: {exc}")
        return None


def _snapshot_to_brief(snapshot: dict) -> dict:
    """Convert rule-based snapshot to LeadBrief-compatible format."""
    return {
        "operator_brief": snapshot["summary"],
        "call_opening": snapshot["call_angle"],
        "objection_handling": "",
        "urgency_reason": snapshot["why_it_matters"],
        "evidence_bullets": snapshot["key_signals"][:4],
        "risk_flags": snapshot["risks"],
        "confidence": "low",
        "missing_data": ["AI generation unavailable — using rule-based fallback"],
        "sms_draft": "",
        "next_action": snapshot["recommended_action"],
        "next_action_channel": "call",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_used": "rule_based",
    }


async def get_or_generate_brief(lead_id: str, lead: dict, session: Any = None) -> dict | None:
    """
    Return a cached LeadBrief or generate a new one.

    Order:
    1. Check in-memory cache (hit → return immediately)
    2. Try AI generation via ai_router
    3. Fall back to rule-based snapshot if AI unavailable
    4. Cache result and return
    """
    cached = _cached_brief(lead_id)
    if cached:
        return cached

    status = (lead.get("status") or lead.get("trigger_type") or "").upper()

    # Try AI generation first
    brief = await _generate_ai_brief(lead)

    if brief is None:
        # Fall back to deterministic snapshot
        snapshot = generate_underwriter_snapshot(lead)
        brief = _snapshot_to_brief(snapshot)
    else:
        brief.setdefault("generated_at", datetime.now(timezone.utc).isoformat())
        brief.setdefault("model_used", "ai_router")

    brief["lead_id"] = lead_id
    _cache_brief(lead_id, brief, status)
    return brief
