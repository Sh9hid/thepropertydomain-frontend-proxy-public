"""
Missed Deal Detector — deterministic rules engine for surfacing neglected opportunities.

Detector keys and trigger conditions (all deterministic, no vibes):

  WARM_THEN_ABANDONED
    - At least 1 connected call exists for this lead
    - days_since_last_connected >= 5
    - status is not terminal (converted / dropped / dead)
    - no active next_action_at in the future OR next_action_at is already overdue

  OVERDUE_CALLBACK
    - next_action_at on lead is set AND next_action_at < now (Sydney)
    - OR most recent call_log row has next_action_due set AND that date < today

  LONG_TALK_NO_BOOKING
    - total connected duration across all calls >= 180 seconds
    - status is not appt_booked / mortgage_appt_booked / converted
    - no active appointment row in appointments table

  REPEATED_ATTEMPTS_NO_PROGRESS
    - total_attempts >= 3 from call_log
    - connected_calls == 0 (never picked up)
    - status still in (captured, qualified, outreach_ready)

  STALE_HOT_LEAD
    - heat_score >= 60 OR call_today_score >= 60
    - days_since_last_contact >= 14 OR last_contacted_at is null

  OBJECTION_NOT_REVISITED
    - objection_reason is non-empty on the lead
    - last connected call was >= 7 days ago (enough cooling-off time to try again)

  MARKET_SIGNAL_NO_ACTION
    - trigger_type in DISTRESS_TRIGGER_TYPES (withdrawn, probate, mortgage_cliff, etc.)
    - last_contacted_at is null OR days_since_last_contact >= 7

  PIPELINE_STALL
    - status in (contacted, qualified, outreach_ready) — intermediate, not terminal
    - updated_at is more than 14 days ago (lead stuck)

  HIGH_VALUE_NEGLECT
    - est_value >= 1_000_000 (confirmed property value signal)
    - touches_14d <= 1

  REPEATED_NO_ANSWER
    - total_attempts >= 4
    - connected_calls == 0
    - no diversification: all call_log rows share the same outcome / direction pattern

Urgency formula (deterministic — stored here, not computed at runtime):
  critical:
    (OVERDUE_CALLBACK AND overdue_days >= 7)
    OR (WARM_THEN_ABANDONED AND days_since_last_connected >= 14)
    OR (HIGH_VALUE_NEGLECT AND heat_score >= 60)
    OR (len(detector_reasons) >= 3)
  high:
    WARM_THEN_ABANDONED
    OR (OVERDUE_CALLBACK AND overdue_days >= 1)
    OR LONG_TALK_NO_BOOKING
    OR OBJECTION_NOT_REVISITED
  medium:
    any single detector not already critical/high

Opportunity score formula (0–100, for sort order):
  score = (
      severity_max * 30                           # max severity: 1→30, 2→60, 3→90
      + min(overdue_days, 14)                     # overdue penalty (max 14)
      + min(days_since_last_contact, 30) * 0.5    # staleness (max 15)
      + heat_score * 0.1                          # lead quality (max 10)
      + (10 if est_value >= 1_000_000 else 0)     # high value bonus
      + min(talk_time_total // 60, 10) * 0.5      # wasted talk time (max 5)
  )
  Capped at 100.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.config import SYDNEY_TZ
from models.sql_models import Appointment, CallLog, Lead
from models.opportunity_models import OpportunityAction


# ── Constants ────────────────────────────────────────────────────────────────

TERMINAL_STATUSES = {"converted", "dropped", "dead", "rescinded"}
INTERMEDIATE_STATUSES = {"contacted", "qualified", "outreach_ready"}
BOOKED_STATUSES = {"appt_booked", "mortgage_appt_booked", "converted"}

DISTRESS_TRIGGER_TYPES = {
    "withdrawn", "probate", "mortgage_cliff", "stale_active_listing",
    "da_feed", "delta_engine", "distress_intel",
}

# Minimum thresholds for each detector (documented, not magic numbers)
WARM_ABANDONED_MIN_DAYS = 5
WARM_ABANDONED_CRITICAL_DAYS = 14
STALE_HOT_MIN_DAYS = 14
MARKET_SIGNAL_MIN_DAYS = 7
OBJECTION_REVISIT_MIN_DAYS = 7
PIPELINE_STALL_DAYS = 14
LONG_TALK_MIN_SECONDS = 180
REPEATED_MIN_ATTEMPTS = 3
NO_ANSWER_MIN_ATTEMPTS = 4
HIGH_VALUE_THRESHOLD = 1_000_000


# ── Internal data structures ─────────────────────────────────────────────────

@dataclass
class _CallStats:
    total_attempts: int = 0
    connected_calls: int = 0
    talk_time_total: int = 0          # seconds (connected calls only)
    last_call_at: Optional[datetime] = None
    last_connected_at: Optional[datetime] = None
    last_call_note: str = ""
    last_call_outcome: str = ""
    last_call_next_action_due: Optional[str] = None
    unique_outcomes: set = field(default_factory=set)
    max_intent_signal: float = 0.0
    booking_attempted: bool = False
    next_step_detected: bool = False
    objection_tags: set = field(default_factory=set)


@dataclass
class _DetectorResult:
    key: str
    label: str
    confidence_basis: str
    severity: int  # 1=medium, 2=high, 3=critical


# ── Public output schema (used by routes) ─────────────────────────────────────

@dataclass
class OpportunityCard:
    lead_id: str
    lead_name: str
    address: str
    suburb: str
    postcode: str
    status: str
    urgency_level: str        # critical / high / medium
    detector_reasons: List[str]
    confidence_basis: str
    days_since_last_contact: Optional[int]
    talk_time_total: int      # seconds
    connected_calls: int
    total_attempts: int
    next_action_due: Optional[str]
    missed_value_reason: str
    recommended_action_type: str
    recommended_action_text: str
    recommended_contact_window: str
    evidence_summary: str
    call_brief: str
    suggested_opener: str
    last_call_summary: Optional[str]
    objection_summary: Optional[str]
    stale_days: Optional[int]
    overdue_days: Optional[int]
    heat_score: int
    evidence_score: int
    est_value: Optional[int]
    score: int                # 0–100, higher = more urgent


# ── Detector logic (pure functions — testable without DB) ─────────────────────

def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO datetime string, returning timezone-aware Sydney datetime or None."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(SYDNEY_TZ)
    except (ValueError, TypeError):
        return None


def _days_since(dt: Optional[datetime], now: datetime) -> Optional[int]:
    if dt is None:
        return None
    return max(0, (now - dt).days)


def run_detectors(
    lead: dict,
    stats: _CallStats,
    has_appointment: bool,
    now: datetime,
) -> List[_DetectorResult]:
    """
    Apply all detectors to a lead dict + its call stats.
    Returns list of triggered DetectorResult objects.
    Pure function — no DB access. Accepts plain dicts for testability.
    """
    results: List[_DetectorResult] = []
    status = lead.get("status") or ""
    next_action_at = _parse_iso(lead.get("next_action_at"))
    last_contacted_at = _parse_iso(lead.get("last_contacted_at"))
    updated_at = _parse_iso(lead.get("updated_at"))
    objection_reason = (lead.get("objection_reason") or "").strip()
    trigger_type = lead.get("trigger_type") or ""
    heat_score = lead.get("heat_score") or 0
    call_today_score = lead.get("call_today_score") or 0
    est_value = lead.get("est_value") or 0
    touches_14d = lead.get("touches_14d") or 0

    days_since_contact = _days_since(last_contacted_at, now)
    days_since_connected = _days_since(stats.last_connected_at, now)
    overdue_days = max(0, (now - next_action_at).days) if next_action_at and next_action_at < now else 0

    if status in TERMINAL_STATUSES:
        return results  # nothing to surface for closed leads

    # ── 1. WARM_THEN_ABANDONED ──────────────────────────────────────────────
    if (
        stats.connected_calls >= 1
        and days_since_connected is not None
        and days_since_connected >= WARM_ABANDONED_MIN_DAYS
        and status not in BOOKED_STATUSES
        and (next_action_at is None or next_action_at < now)
    ):
        sev = 3 if days_since_connected >= WARM_ABANDONED_CRITICAL_DAYS else 2
        results.append(_DetectorResult(
            key="WARM_THEN_ABANDONED",
            label="Warm Then Abandoned",
            confidence_basis=(
                f"{stats.connected_calls} connected call(s), last connected "
                f"{days_since_connected}d ago, no active next action"
            ),
            severity=sev,
        ))

    # ── 2. OVERDUE_CALLBACK ─────────────────────────────────────────────────
    call_log_due = _parse_iso(stats.last_call_next_action_due)
    call_log_overdue = max(0, (now - call_log_due).days) if call_log_due and call_log_due < now else 0
    effective_overdue = max(overdue_days, call_log_overdue)
    if effective_overdue > 0:
        sev = 3 if effective_overdue >= 7 else 2
        results.append(_DetectorResult(
            key="OVERDUE_CALLBACK",
            label="Overdue Callback",
            confidence_basis=f"Next action was due {effective_overdue} day(s) ago",
            severity=sev,
        ))

    # ── 3. LONG_TALK_NO_BOOKING ─────────────────────────────────────────────
    if (
        stats.talk_time_total >= LONG_TALK_MIN_SECONDS
        and (
            stats.booking_attempted
            or stats.next_step_detected
            or stats.max_intent_signal >= 0.6
        )
        and status not in BOOKED_STATUSES
        and not has_appointment
    ):
        mins = stats.talk_time_total // 60
        results.append(_DetectorResult(
            key="LONG_TALK_NO_BOOKING",
            label="Long Talk, No Booking",
            confidence_basis=(
                f"{mins} min total talk time across {stats.connected_calls} connected call(s), "
                f"intent={stats.max_intent_signal:.2f}, booking_attempted={stats.booking_attempted}, "
                f"next_step_detected={stats.next_step_detected}"
            ),
            severity=2,
        ))

    # ── 4. REPEATED_ATTEMPTS_NO_PROGRESS ────────────────────────────────────
    if (
        stats.total_attempts >= REPEATED_MIN_ATTEMPTS
        and stats.connected_calls == 0
        and status in INTERMEDIATE_STATUSES | {"captured"}
    ):
        results.append(_DetectorResult(
            key="REPEATED_ATTEMPTS_NO_PROGRESS",
            label="Repeated Attempts, No Progress",
            confidence_basis=(
                f"{stats.total_attempts} attempts with zero connections, "
                f"status still '{status}'"
            ),
            severity=1,
        ))

    # ── 5. STALE_HOT_LEAD ───────────────────────────────────────────────────
    is_hot = heat_score >= 60 or call_today_score >= 60
    contact_stale = days_since_contact is None or days_since_contact >= STALE_HOT_MIN_DAYS
    if is_hot and contact_stale:
        score_basis = f"heat_score={heat_score}, call_today_score={call_today_score}"
        stale_basis = f"no contact in {days_since_contact}d" if days_since_contact else "never contacted"
        results.append(_DetectorResult(
            key="STALE_HOT_LEAD",
            label="Stale Hot Lead",
            confidence_basis=f"{score_basis} — {stale_basis}",
            severity=1,
        ))

    # ── 6. OBJECTION_NOT_REVISITED ──────────────────────────────────────────
    if (
        objection_reason
        and (
            days_since_connected is None
            or days_since_connected >= OBJECTION_REVISIT_MIN_DAYS
        )
        and status not in BOOKED_STATUSES
    ):
        revisit_basis = (
            f"last connected {days_since_connected}d ago"
            if days_since_connected
            else "never connected since objection"
        )
        results.append(_DetectorResult(
            key="OBJECTION_NOT_REVISITED",
            label="Objection Not Revisited",
            confidence_basis=f"objection_reason='{objection_reason[:60]}', {revisit_basis}",
            severity=2,
        ))

    # ── 7. MARKET_SIGNAL_NO_ACTION ──────────────────────────────────────────
    if trigger_type in DISTRESS_TRIGGER_TYPES:
        contact_absent = days_since_contact is None or days_since_contact >= MARKET_SIGNAL_MIN_DAYS
        if contact_absent:
            basis = (
                f"no contact in {days_since_contact}d"
                if days_since_contact
                else "never contacted"
            )
            results.append(_DetectorResult(
                key="MARKET_SIGNAL_NO_ACTION",
                label="Market Signal, No Action",
                confidence_basis=f"trigger_type='{trigger_type}', {basis}",
                severity=1,
            ))

    # ── 8. PIPELINE_STALL ───────────────────────────────────────────────────
    if status in INTERMEDIATE_STATUSES and updated_at is not None:
        stall_days = (now - updated_at).days
        if stall_days >= PIPELINE_STALL_DAYS:
            results.append(_DetectorResult(
                key="PIPELINE_STALL",
                label="Pipeline Stall",
                confidence_basis=f"status='{status}' for {stall_days} days without progression",
                severity=1,
            ))

    # ── 9. HIGH_VALUE_NEGLECT ───────────────────────────────────────────────
    if est_value >= HIGH_VALUE_THRESHOLD and touches_14d <= 1:
        sev = 3 if heat_score >= 60 else 1
        results.append(_DetectorResult(
            key="HIGH_VALUE_NEGLECT",
            label="High Value Neglect",
            confidence_basis=(
                f"est_value=${est_value:,}, only {touches_14d} touch(es) in last 14d"
            ),
            severity=sev,
        ))

    # ── 10. REPEATED_NO_ANSWER ──────────────────────────────────────────────
    if stats.total_attempts >= NO_ANSWER_MIN_ATTEMPTS and stats.connected_calls == 0:
        # Check if outcomes are uniform (no strategy change)
        diverse = len(stats.unique_outcomes) >= 2
        if not diverse:
            results.append(_DetectorResult(
                key="REPEATED_NO_ANSWER",
                label="Repeated No Answer",
                confidence_basis=(
                    f"{stats.total_attempts} unanswered attempts, "
                    f"all same outcome pattern — no channel/time change"
                ),
                severity=1,
            ))

    return results


def _compute_urgency(detectors: List[_DetectorResult], overdue_days: int, days_since_connected: Optional[int]) -> str:
    """Determine urgency from detector set — documented formula, not guesswork."""
    keys = {d.key for d in detectors}
    max_sev = max((d.severity for d in detectors), default=0)

    # Critical conditions (formula from module docstring)
    if (
        ("OVERDUE_CALLBACK" in keys and overdue_days >= 7)
        or ("WARM_THEN_ABANDONED" in keys and (days_since_connected or 0) >= 14)
        or (max_sev >= 3)
        or len(detectors) >= 3
    ):
        return "critical"

    # High conditions
    if (
        "WARM_THEN_ABANDONED" in keys
        or ("OVERDUE_CALLBACK" in keys and overdue_days >= 1)
        or "LONG_TALK_NO_BOOKING" in keys
        or "OBJECTION_NOT_REVISITED" in keys
    ):
        return "high"

    return "medium"


def _compute_score(
    detectors: List[_DetectorResult],
    overdue_days: int,
    days_since_contact: Optional[int],
    heat_score: int,
    est_value: Optional[int],
    talk_time_total: int,
) -> int:
    """
    Deterministic opportunity score 0–100.
    Formula (from module docstring):
      severity_max * 30
      + min(overdue_days, 14)
      + min(days_since_contact, 30) * 0.5
      + heat_score * 0.1
      + 10 if est_value >= 1_000_000
      + min(talk_time_total // 60, 10) * 0.5
    """
    if not detectors:
        return 0
    sev_max = max(d.severity for d in detectors)
    score = (
        sev_max * 30
        + min(overdue_days, 14)
        + min(days_since_contact or 0, 30) * 0.5
        + heat_score * 0.1
        + (10 if (est_value or 0) >= HIGH_VALUE_THRESHOLD else 0)
        + min(talk_time_total // 60, 10) * 0.5
    )
    return min(100, int(score))


def _generate_call_brief(
    lead: dict,
    stats: _CallStats,
    primary_key: str,
    overdue_days: int,
    days_since: Optional[int],
) -> Tuple[str, str]:
    """
    Returns (micro_brief, suggested_opener) based on primary detector.
    Fully deterministic — no AI required.
    """
    name = (lead.get("owner_name") or "the owner").split()[0]
    suburb = lead.get("suburb") or "the area"
    mins = stats.talk_time_total // 60

    if primary_key == "WARM_THEN_ABANDONED":
        brief = (
            f"You had a {mins}-min conversation with {name} "
            f"{days_since}d ago — they haven't heard back since."
        )
        opener = (
            f"Hi {name}, it's Shahid from Laing+Simmons Oakville. "
            f"I wanted to follow up from our chat about your property in {suburb} — "
            f"just checking in on where things are sitting for you."
        )

    elif primary_key == "OVERDUE_CALLBACK":
        brief = (
            f"A callback was scheduled {overdue_days} day(s) ago "
            f"and hasn't happened. Each day makes recovery harder."
        )
        opener = (
            f"Hi {name}, it's Shahid from Laing+Simmons Oakville. "
            f"I had a note to call you back — apologies for the delay. "
            f"I wanted to check in on where things stand with the property."
        )

    elif primary_key == "LONG_TALK_NO_BOOKING":
        brief = (
            f"{mins} minutes of real conversation happened, "
            f"but no appointment was ever booked. The interest is there."
        )
        opener = (
            f"Hi {name}, it's Shahid from Laing+Simmons. "
            f"We've spoken a bit about your property in {suburb} — "
            f"I'd like to lock in a quick appraisal walkthrough this week."
        )

    elif primary_key == "OVERDUE_CALLBACK":
        brief = f"Callback overdue by {overdue_days} day(s). Pipeline cools fast."
        opener = (
            f"Hi {name}, Shahid from Laing+Simmons — following up on my note to call you back."
        )

    elif primary_key == "OBJECTION_NOT_REVISITED":
        obj = (lead.get("objection_reason") or "their concern")[:50]
        brief = (
            f"{name} had a concern ({obj}) that was never addressed "
            f"after the cooling-off period."
        )
        opener = (
            f"Hi {name}, it's Shahid from Laing+Simmons. "
            f"I wanted to revisit some of the concerns you mentioned — "
            f"a lot has changed in {suburb} recently and I think the timing looks different now."
        )

    elif primary_key == "HIGH_VALUE_NEGLECT":
        val_str = f"${(lead.get('est_value') or 0):,}"
        brief = (
            f"This property is estimated at {val_str} "
            f"and has had minimal outreach — it deserves proper attention."
        )
        opener = (
            f"Hi {name}, Shahid from Laing+Simmons Oakville. "
            f"We've been tracking your property in {suburb} and I'd love to walk you through "
            f"what comparable sales have been achieving right now."
        )

    elif primary_key == "MARKET_SIGNAL_NO_ACTION":
        trigger = (lead.get("trigger_type") or "market activity").replace("_", " ")
        brief = (
            f"There's a {trigger} signal attached to this lead, "
            f"but no outreach has happened in {days_since or '?'}d."
        )
        opener = (
            f"Hi {name}, it's Shahid from Laing+Simmons — "
            f"I wanted to reach out because we've noticed some activity in your area "
            f"and thought it was worth a quick conversation."
        )

    elif primary_key == "STALE_HOT_LEAD":
        brief = (
            f"This lead scores highly but hasn't been contacted "
            f"in {days_since or '?'}d — the window is closing."
        )
        opener = (
            f"Hi {name}, Shahid from Laing+Simmons Oakville — "
            f"just wanted to reach out about your property in {suburb}. "
            f"It's been a while since we connected."
        )

    elif primary_key == "PIPELINE_STALL":
        brief = (
            f"Lead is stuck at '{lead.get('status')}' for {days_since or '?'} days "
            f"with no progression."
        )
        opener = (
            f"Hi {name}, Shahid from Laing+Simmons. "
            f"I wanted to check in — has anything changed with your situation in {suburb}?"
        )

    else:
        brief = (
            f"{stats.total_attempts} contact attempt(s), "
            f"{stats.connected_calls} connected — follow-up needed."
        )
        opener = (
            f"Hi {name}, Shahid from Laing+Simmons Oakville — "
            f"following up on your property in {suburb}."
        )

    return brief, opener


def _recommended_action(primary_key: str, stats: _CallStats, lead: dict) -> Tuple[str, str, str]:
    """Returns (action_type, action_text, contact_window)."""
    suburb = lead.get("suburb") or "the area"

    windows = {
        "morning": "9:00–11:30 AM AEST",
        "afternoon": "2:00–4:30 PM AEST",
        "evening": "6:00–7:30 PM AEST",
    }

    if primary_key in ("WARM_THEN_ABANDONED", "LONG_TALK_NO_BOOKING", "OVERDUE_CALLBACK"):
        return (
            "call",
            f"Call {lead.get('owner_name') or 'owner'} — reference previous conversation and propose a specific meeting time.",
            windows["morning"],
        )
    elif primary_key == "OBJECTION_NOT_REVISITED":
        return (
            "call",
            f"Call to address outstanding objection: '{(lead.get('objection_reason') or '')[:60]}'. "
            f"Lead new evidence about {suburb} if available.",
            windows["afternoon"],
        )
    elif primary_key == "HIGH_VALUE_NEGLECT":
        return (
            "call",
            f"Priority call — high-value property. Lead with recent comparable sales in {suburb}.",
            windows["morning"],
        )
    elif primary_key == "REPEATED_ATTEMPTS_NO_PROGRESS":
        return (
            "sms",
            "Switch channel — send SMS with a soft touch asking if they're still open to a chat.",
            windows["afternoon"],
        )
    elif primary_key == "REPEATED_NO_ANSWER":
        return (
            "sms",
            "Try SMS — multiple calls haven't landed. A brief text may get a response.",
            windows["evening"],
        )
    elif primary_key in ("MARKET_SIGNAL_NO_ACTION", "STALE_HOT_LEAD"):
        return (
            "call",
            f"First outreach — lead with the {lead.get('trigger_type', 'market').replace('_', ' ')} signal as context.",
            windows["morning"],
        )
    else:
        return (
            "call",
            "Follow up with a check-in call.",
            windows["morning"],
        )


def build_opportunity_card(
    lead: dict,
    stats: _CallStats,
    has_appointment: bool,
    last_call_summary: Optional[str],
    objection_summary: Optional[str],
    detectors: List[_DetectorResult],
    now: datetime,
) -> Optional[OpportunityCard]:
    """Convert raw detector results into a fully populated OpportunityCard."""
    if not detectors:
        return None

    next_action_at = _parse_iso(lead.get("next_action_at"))
    last_contacted_at = _parse_iso(lead.get("last_contacted_at"))
    days_since_contact = _days_since(last_contacted_at, now)
    days_since_connected = _days_since(stats.last_connected_at, now)
    overdue_days = max(0, (now - next_action_at).days) if next_action_at and next_action_at < now else 0

    urgency = _compute_urgency(detectors, overdue_days, days_since_connected)
    score = _compute_score(
        detectors,
        overdue_days,
        days_since_contact,
        lead.get("heat_score") or 0,
        lead.get("est_value"),
        stats.talk_time_total,
    )

    primary = max(detectors, key=lambda d: d.severity)
    action_type, action_text, contact_window = _recommended_action(primary.key, stats, lead)
    brief, opener = _generate_call_brief(lead, stats, primary.key, overdue_days, days_since_contact)

    # Evidence summary from lead fields
    ev_parts = []
    if lead.get("trigger_type"):
        ev_parts.append(f"Trigger: {lead['trigger_type'].replace('_', ' ')}")
    if lead.get("heat_score"):
        ev_parts.append(f"Heat: {lead['heat_score']}/100")
    if lead.get("est_value"):
        ev_parts.append(f"Est value: ${lead['est_value']:,}")
    if stats.talk_time_total:
        ev_parts.append(f"Talk time: {stats.talk_time_total // 60}m")
    if lead.get("objection_reason"):
        ev_parts.append(f"Objection on file: {lead['objection_reason'][:40]}")
    evidence_summary = " · ".join(ev_parts) if ev_parts else "No enrichment data"

    # Missed value reason
    missed_reason = primary.label + " — " + primary.confidence_basis[:80]

    return OpportunityCard(
        lead_id=lead["id"],
        lead_name=lead.get("owner_name") or "Unknown Owner",
        address=lead.get("address") or "",
        suburb=lead.get("suburb") or "",
        postcode=lead.get("postcode") or "",
        status=lead.get("status") or "captured",
        urgency_level=urgency,
        detector_reasons=[f"{d.key}: {d.confidence_basis}" for d in detectors],
        confidence_basis=primary.confidence_basis,
        days_since_last_contact=days_since_contact,
        talk_time_total=stats.talk_time_total,
        connected_calls=stats.connected_calls,
        total_attempts=stats.total_attempts,
        next_action_due=(
            lead.get("next_action_at")
            or stats.last_call_next_action_due
        ),
        missed_value_reason=missed_reason,
        recommended_action_type=action_type,
        recommended_action_text=action_text,
        recommended_contact_window=contact_window,
        evidence_summary=evidence_summary,
        call_brief=brief,
        suggested_opener=opener,
        last_call_summary=last_call_summary,
        objection_summary=objection_summary,
        stale_days=days_since_contact,
        overdue_days=overdue_days if overdue_days else None,
        heat_score=lead.get("heat_score") or 0,
        evidence_score=lead.get("evidence_score") or 0,
        est_value=lead.get("est_value"),
        score=score,
    )


# ── DB-backed public functions ────────────────────────────────────────────────

async def get_missed_deals(
    session: AsyncSession,
    urgency: Optional[str] = None,
    detector: Optional[str] = None,
    suburb: Optional[str] = None,
    sort_by: str = "score",
    limit: int = 50,
) -> List[OpportunityCard]:
    """
    Scan leads + call_log + appointments, apply detectors, return sorted opportunity cards.
    Respects dismiss/snooze actions stored in opportunity_actions table.
    """
    now = datetime.now(SYDNEY_TZ)

    # 1. Fetch leads (non-terminal, limit 250 to keep query fast)
    lead_stmt = (
        select(Lead)
        .where(Lead.status.notin_(TERMINAL_STATUSES))
        .where(Lead.record_type == "property_record")
        .order_by(Lead.call_today_score.desc())
        .limit(250)
    )
    if suburb:
        lead_stmt = lead_stmt.where(Lead.suburb.ilike(f"%{suburb}%"))
    lead_rows = (await session.execute(lead_stmt)).scalars().all()
    if not lead_rows:
        return []

    lead_ids = [l.id for l in lead_rows]
    lead_map: Dict[str, Lead] = {l.id: l for l in lead_rows}

    # 2. Fetch call_log rows for these leads
    call_stmt = select(CallLog).where(CallLog.lead_id.in_(lead_ids))
    call_rows = (await session.execute(call_stmt)).scalars().all()

    # Aggregate call stats per lead
    stats_map: Dict[str, _CallStats] = {lid: _CallStats() for lid in lead_ids}
    for c in call_rows:
        s = stats_map[c.lead_id]
        s.total_attempts += 1
        dur = max(c.duration_seconds or 0, c.call_duration_seconds or 0)
        s.max_intent_signal = max(s.max_intent_signal, float(getattr(c, "intent_signal", 0.0) or 0.0))
        s.booking_attempted = s.booking_attempted or bool(getattr(c, "booking_attempted", False))
        s.next_step_detected = s.next_step_detected or bool(getattr(c, "next_step_detected", False))
        raw_tags = getattr(c, "objection_tags", "[]")
        try:
            parsed_tags = json.loads(raw_tags) if isinstance(raw_tags, str) else list(raw_tags or [])
        except (json.JSONDecodeError, TypeError):
            parsed_tags = []
        s.objection_tags.update(str(tag) for tag in parsed_tags if str(tag).strip())
        if c.connected:
            s.connected_calls += 1
            s.talk_time_total += dur
        # Track last timestamps
        if c.timestamp:
            call_dt = _parse_iso(c.timestamp)
            if call_dt:
                if s.last_call_at is None or call_dt > s.last_call_at:
                    s.last_call_at = call_dt
                    s.last_call_note = c.note or ""
                    s.last_call_outcome = c.outcome or ""
                    s.last_call_next_action_due = c.next_action_due
                if c.connected and (s.last_connected_at is None or call_dt > s.last_connected_at):
                    s.last_connected_at = call_dt
        if c.outcome:
            s.unique_outcomes.add(c.outcome)

    # 3. Fetch active appointments
    appt_stmt = (
        select(Appointment)
        .where(Appointment.lead_id.in_(lead_ids))
        .where(Appointment.status.in_(["scheduled", "confirmed"]))
    )
    appt_rows = (await session.execute(appt_stmt)).scalars().all()
    leads_with_appt = {a.lead_id for a in appt_rows}

    # 4. Fetch dismissed/snoozed leads (exclude them)
    action_stmt = select(OpportunityAction).where(
        OpportunityAction.lead_id.in_(lead_ids)
    )
    action_rows = (await session.execute(action_stmt)).scalars().all()
    excluded_leads: set = set()
    for a in action_rows:
        if a.action == "dismiss":
            excluded_leads.add(a.lead_id)
        elif a.action == "snooze" and a.expires_at:
            expires = _parse_iso(a.expires_at)
            if expires and expires > now:
                excluded_leads.add(a.lead_id)

    # 5. Fetch call_analysis for optional enrichment (last summary per lead)
    analysis_map: Dict[str, dict] = {}
    if lead_ids:
        try:
            from sqlalchemy import bindparam as _bp
            stmt = text(
                "SELECT lead_id, objections, summary FROM call_analysis "
                "WHERE lead_id IN :ids ORDER BY analyzed_at DESC"
            ).bindparams(_bp("ids", expanding=True))
            analysis_rows = (await session.execute(stmt, {"ids": lead_ids})).fetchall()
            for row in analysis_rows:
                lid = row[0]
                if lid not in analysis_map:
                    analysis_map[lid] = {
                        "summary": row[2] or "",
                        "objections": row[1] or "[]",
                    }
        except Exception:
            pass  # call_analysis may not exist in all environments

    # 6. Run detectors and build cards
    cards: List[OpportunityCard] = []
    for lead in lead_rows:
        if lead.id in excluded_leads:
            continue
        stats = stats_map[lead.id]
        lead_dict = {
            "id": lead.id,
            "address": lead.address,
            "suburb": lead.suburb,
            "postcode": lead.postcode,
            "owner_name": lead.owner_name,
            "status": lead.status,
            "heat_score": lead.heat_score,
            "call_today_score": lead.call_today_score,
            "evidence_score": lead.evidence_score,
            "next_action_at": lead.next_action_at,
            "last_contacted_at": lead.last_contacted_at,
            "updated_at": lead.updated_at,
            "objection_reason": lead.objection_reason,
            "trigger_type": lead.trigger_type,
            "est_value": lead.est_value,
            "touches_14d": lead.touches_14d,
        }
        detectors = run_detectors(lead_dict, stats, lead.id in leads_with_appt, now)
        if not detectors:
            continue

        # Filter by detector key if requested
        if detector and not any(d.key == detector for d in detectors):
            continue

        # Filter by urgency
        urgency_val = _compute_urgency(
            detectors,
            max(0, (now - _parse_iso(lead.next_action_at)).days)
            if lead.next_action_at and _parse_iso(lead.next_action_at) and _parse_iso(lead.next_action_at) < now
            else 0,
            _days_since(stats.last_connected_at, now),
        )
        if urgency and urgency_val != urgency:
            continue

        # Optional AI enrichment from call_analysis
        ca = analysis_map.get(lead.id, {})
        last_call_summary = ca.get("summary") or None
        objection_summary = None
        if ca.get("objections"):
            try:
                objs = json.loads(ca["objections"])
                if objs:
                    objection_summary = "; ".join(str(o) for o in objs[:3])
            except (json.JSONDecodeError, TypeError):
                pass

        card = build_opportunity_card(
            lead_dict, stats, lead.id in leads_with_appt,
            last_call_summary, objection_summary, detectors, now
        )
        if card:
            cards.append(card)

    # 7. Sort
    if sort_by == "score":
        cards.sort(key=lambda c: c.score, reverse=True)
    elif sort_by == "overdue":
        cards.sort(key=lambda c: c.overdue_days or 0, reverse=True)
    elif sort_by == "talk_time":
        cards.sort(key=lambda c: c.talk_time_total, reverse=True)
    elif sort_by == "heat":
        cards.sort(key=lambda c: c.heat_score, reverse=True)

    return cards[:limit]


async def get_missed_deals_summary(session: AsyncSession) -> dict:
    """Aggregate stats for the summary widget."""
    cards = await get_missed_deals(session, limit=250)
    now = datetime.now(SYDNEY_TZ)
    today_str = now.strftime("%Y-%m-%d")

    detector_counts: Dict[str, int] = {}
    for card in cards:
        for reason in card.detector_reasons:
            key = reason.split(":")[0].strip()
            detector_counts[key] = detector_counts.get(key, 0) + 1

    return {
        "total_opportunities": len(cards),
        "critical_count": sum(1 for c in cards if c.urgency_level == "critical"),
        "high_count": sum(1 for c in cards if c.urgency_level == "high"),
        "overdue_callbacks": sum(1 for c in cards if c.overdue_days and c.overdue_days > 0),
        "warm_gone_cold": sum(
            1 for c in cards
            if any("WARM_THEN_ABANDONED" in r for r in c.detector_reasons)
        ),
        "high_value_neglected": sum(
            1 for c in cards
            if any("HIGH_VALUE_NEGLECT" in r for r in c.detector_reasons)
        ),
        "by_detector": detector_counts,
    }
