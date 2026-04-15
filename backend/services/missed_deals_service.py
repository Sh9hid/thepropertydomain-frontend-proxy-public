from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import SYDNEY_TZ
from services.followup_engine import FOLLOW_UP_RULES, generate_follow_up
from services.zoom_call_sync_service import ensure_call_log_schema

REASON_ORDER = (
    "MISSED_BOOKING",
    "NO_FOLLOW_UP",
    "STALE_HIGH_INTENT",
    "PRICE_DROP_OPPORTUNITY",
)

REASON_BASE_POINTS = {
    "MISSED_BOOKING": 58.0,
    "NO_FOLLOW_UP": 52.0,
    "STALE_HIGH_INTENT": 44.0,
    "PRICE_DROP_OPPORTUNITY": 40.0,
}

TERMINAL_STATUSES = {"converted", "dropped", "appt_booked", "mortgage_appt_booked"}


def _parse_iso(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SYDNEY_TZ)
    return parsed.astimezone(SYDNEY_TZ)


def _resolve_now(date_range: str | None) -> datetime:
    now = datetime.now(SYDNEY_TZ).replace(microsecond=0)
    if not date_range or date_range == "today":
        return now
    if date_range == "yesterday":
        return (now - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    try:
        parsed = datetime.fromisoformat(date_range)
    except ValueError:
        return now
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SYDNEY_TZ)
    return parsed.astimezone(SYDNEY_TZ).replace(microsecond=0)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _coerce_float(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value or 0.0)))
    except (TypeError, ValueError):
        return 0.0


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item or "").strip()]
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip().lower() for item in parsed if str(item or "").strip()]


def _hours_since(reference_at: Optional[datetime], now: datetime) -> Optional[float]:
    if not reference_at:
        return None
    return round(max(0.0, (now - reference_at).total_seconds() / 3600.0), 2)


def _freshness_points(
    *,
    reason: str,
    hours_since_contact: Optional[float],
    hours_since_last_call: Optional[float],
    price_drop_count: int,
) -> float:
    reference_hours = hours_since_contact if hours_since_contact is not None else hours_since_last_call
    if reference_hours is None:
        return 12.0 if reason == "PRICE_DROP_OPPORTUNITY" else 0.0

    if reason in {"MISSED_BOOKING", "NO_FOLLOW_UP"}:
        if reference_hours <= 6:
            return 22.0
        if reference_hours <= 24:
            return 18.0
        if reference_hours <= 48:
            return 12.0
        if reference_hours <= 72:
            return 8.0
        return 4.0

    if reason == "STALE_HIGH_INTENT":
        if reference_hours >= 120:
            return 20.0
        if reference_hours >= 96:
            return 17.0
        if reference_hours >= 72:
            return 14.0
        if reference_hours >= 48:
            return 10.0
        return 0.0

    if reason == "PRICE_DROP_OPPORTUNITY":
        if reference_hours >= 120:
            return 18.0 + min(price_drop_count, 3) * 2.0
        if reference_hours >= 96:
            return 15.0 + min(price_drop_count, 3) * 2.0
        if reference_hours >= 72:
            return 11.0 + min(price_drop_count, 3) * 2.0
        return min(price_drop_count, 3) * 2.0

    return 0.0


def _priority_score(
    *,
    reason: str,
    intent_signal: float,
    hours_since_contact: Optional[float],
    hours_since_last_call: Optional[float],
    price_drop_count: int,
    booking_attempted: bool,
    next_step_detected: bool,
) -> float:
    reason_points = REASON_BASE_POINTS[reason]
    intent_points = _coerce_float(intent_signal) * 24.0
    freshness_points = _freshness_points(
        reason=reason,
        hours_since_contact=hours_since_contact,
        hours_since_last_call=hours_since_last_call,
        price_drop_count=price_drop_count,
    )
    workflow_points = 0.0
    if reason == "MISSED_BOOKING" and not booking_attempted:
        workflow_points += 8.0
    if reason == "NO_FOLLOW_UP" and next_step_detected:
        workflow_points += 6.0
    if reason == "STALE_HIGH_INTENT" and (hours_since_contact or 0.0) >= 72:
        workflow_points += 6.0
    if reason == "PRICE_DROP_OPPORTUNITY":
        workflow_points += min(price_drop_count, 3) * 4.0
    return round(min(100.0, reason_points + intent_points + freshness_points + workflow_points), 3)


def _urgency_band(priority_score: float) -> str:
    if priority_score >= 80:
        return "NOW"
    if priority_score >= 65:
        return "TODAY"
    return "THIS_WEEK"


def _estimated_recovery_class(priority_score: float, est_value: int, intent_signal: float) -> str:
    if priority_score >= 80 or (est_value >= 1_000_000 and intent_signal >= 0.7):
        return "HIGH"
    if priority_score >= 65:
        return "MEDIUM"
    return "LOW"


def _reason_detail(
    *,
    reason: str,
    intent_signal: float,
    hours_since_contact: Optional[float],
    hours_since_last_call: Optional[float],
    price_drop_count: int,
    booking_attempted: bool,
    next_step_detected: bool,
) -> str:
    if reason == "MISSED_BOOKING":
        return (
            f"intent={intent_signal:.2f}|booking_attempted={'yes' if booking_attempted else 'no'}|"
            f"hours_since_contact={int(round(hours_since_contact or 0.0))}"
        )
    if reason == "NO_FOLLOW_UP":
        return (
            f"intent={intent_signal:.2f}|next_step={'yes' if next_step_detected else 'no'}|"
            "follow_up=missing"
        )
    if reason == "STALE_HIGH_INTENT":
        return (
            f"intent={intent_signal:.2f}|hours_since_contact={int(round(hours_since_contact or 0.0))}|"
            "reengage_window=open"
        )
    return (
        f"price_drops={price_drop_count}|hours_since_last_call={int(round(hours_since_last_call or 0.0))}|"
        "recent_call=no"
    )


def summarize_missed_deals(cards: List[Dict[str, Any]]) -> Dict[str, int]:
    return {
        "total_missed_deals": len(cards),
        "high_urgency_count": sum(1 for card in cards if card.get("urgency_band") == "NOW"),
        "no_follow_up_count": sum(1 for card in cards if card.get("reason") == "NO_FOLLOW_UP"),
        "stale_high_intent_count": sum(1 for card in cards if card.get("reason") == "STALE_HIGH_INTENT"),
        "price_drop_opportunity_count": sum(1 for card in cards if card.get("reason") == "PRICE_DROP_OPPORTUNITY"),
    }


async def get_missed_deals(
    session: AsyncSession,
    date_range: str = "today",
    user_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    await ensure_call_log_schema(session)
    now = _resolve_now(date_range)

    call_query = """
        SELECT
            id,
            lead_id,
            user_id,
            timestamp,
            logged_at,
            call_duration_seconds,
            duration_seconds,
            connected,
            outcome,
            transcript,
            summary,
            COALESCE(intent_signal, 0) AS intent_signal,
            COALESCE(booking_attempted, 0) AS booking_attempted,
            COALESCE(objection_tags, '[]') AS objection_tags,
            COALESCE(next_step_detected, 0) AS next_step_detected
        FROM call_log
        WHERE COALESCE(lead_id, '') != ''
    """
    params: Dict[str, Any] = {}
    if user_id:
        call_query += " AND user_id = :user_id"
        params["user_id"] = user_id
    call_query += " ORDER BY lead_id ASC, COALESCE(timestamp, logged_at) DESC, id DESC"
    call_rows = (await session.execute(text(call_query), params)).mappings().all()

    call_lead_ids = {str(row["lead_id"]) for row in call_rows if row.get("lead_id")}
    lead_query = """
        SELECT
            id,
            address,
            suburb,
            postcode,
            owner_name,
            status,
            last_contacted_at,
            follow_up_due_at,
            price_drop_count,
            last_activity_type,
            heat_score,
            evidence_score,
            est_value
        FROM leads
    """
    lead_params: Dict[str, Any] = {}
    if user_id and call_lead_ids:
        placeholders = ", ".join(f":lead_id_{idx}" for idx, _ in enumerate(sorted(call_lead_ids)))
        lead_query += f" WHERE id IN ({placeholders})"
        lead_params = {f"lead_id_{idx}": value for idx, value in enumerate(sorted(call_lead_ids))}
    elif user_id and not call_lead_ids:
        return []
    lead_query += " ORDER BY updated_at DESC, created_at DESC"
    lead_rows = (await session.execute(text(lead_query), lead_params)).mappings().all()

    calls_by_lead: Dict[str, List[Dict[str, Any]]] = {}
    for row in call_rows:
        calls_by_lead.setdefault(str(row["lead_id"]), []).append(dict(row))

    cards: List[Dict[str, Any]] = []
    for lead_row in lead_rows:
        lead = dict(lead_row)
        lead_id = str(lead["id"])
        lead_calls = calls_by_lead.get(lead_id, [])
        if (lead.get("status") or "") in TERMINAL_STATUSES:
            continue

        follow_up_due_at = _parse_iso(lead.get("follow_up_due_at"))
        if follow_up_due_at and follow_up_due_at > now:
            continue

        latest_call: Optional[Dict[str, Any]] = None
        latest_call_at: Optional[datetime] = None
        latest_call_age_hours: Optional[float] = None
        total_attempts = 0
        connected_calls = 0
        talk_time_total = 0
        missed_booking_call: Optional[Dict[str, Any]] = None
        no_follow_up_call: Optional[Dict[str, Any]] = None
        stale_high_intent_call: Optional[Dict[str, Any]] = None

        for call in lead_calls:
            total_attempts += 1
            call_time = _parse_iso(call.get("timestamp") or call.get("logged_at"))
            duration_seconds = _coerce_int(call.get("call_duration_seconds") or call.get("duration_seconds"))
            connected = _coerce_bool(call.get("connected"))
            intent_signal = _coerce_float(call.get("intent_signal"))
            objection_tags = _parse_json_list(call.get("objection_tags"))
            prepared_call = {
                **call,
                "_call_time": call_time,
                "_duration_seconds": duration_seconds,
                "_connected": connected,
                "_intent_signal": intent_signal,
                "_booking_attempted": _coerce_bool(call.get("booking_attempted")),
                "_next_step_detected": _coerce_bool(call.get("next_step_detected")),
                "_objection_tags": objection_tags,
            }
            if latest_call is None:
                latest_call = prepared_call
                latest_call_at = call_time
            if connected:
                connected_calls += 1
                talk_time_total += duration_seconds
            if intent_signal >= 0.7 and not prepared_call["_booking_attempted"] and missed_booking_call is None:
                missed_booking_call = prepared_call
            if (
                connected
                and prepared_call["_next_step_detected"]
                and not follow_up_due_at
                and no_follow_up_call is None
            ):
                no_follow_up_call = prepared_call
            if intent_signal >= 0.6 and stale_high_intent_call is None:
                stale_high_intent_call = prepared_call

        if latest_call_at:
            latest_call_age_hours = _hours_since(latest_call_at, now)

        last_contacted_at = _parse_iso(lead.get("last_contacted_at")) or latest_call_at
        hours_since_contact = _hours_since(last_contacted_at, now)

        reason: Optional[str] = None
        reference_call = latest_call or {}
        reference_intent = _coerce_float(reference_call.get("_intent_signal"))
        if missed_booking_call is not None:
            reason = "MISSED_BOOKING"
            reference_call = missed_booking_call
            reference_intent = _coerce_float(missed_booking_call.get("_intent_signal"))
        elif no_follow_up_call is not None:
            reason = "NO_FOLLOW_UP"
            reference_call = no_follow_up_call
            reference_intent = max(_coerce_float(no_follow_up_call.get("_intent_signal")), 0.65)
        elif stale_high_intent_call is not None and (hours_since_contact or 0.0) > 48:
            reason = "STALE_HIGH_INTENT"
            reference_call = stale_high_intent_call
            reference_intent = _coerce_float(stale_high_intent_call.get("_intent_signal"))
        elif _coerce_int(lead.get("price_drop_count")) >= 1 and (
            latest_call_age_hours is None or latest_call_age_hours > 72
        ):
            reason = "PRICE_DROP_OPPORTUNITY"
            reference_intent = max(reference_intent, 0.58)

        if not reason:
            continue

        preview_follow_up = generate_follow_up(lead, reason, now=now)
        price_drop_count = _coerce_int(lead.get("price_drop_count"))
        booking_attempted = _coerce_bool(reference_call.get("_booking_attempted"))
        next_step_detected = _coerce_bool(reference_call.get("_next_step_detected"))
        priority_score = _priority_score(
            reason=reason,
            intent_signal=reference_intent,
            hours_since_contact=hours_since_contact,
            hours_since_last_call=latest_call_age_hours,
            price_drop_count=price_drop_count,
            booking_attempted=booking_attempted,
            next_step_detected=next_step_detected,
        )
        urgency_band = _urgency_band(priority_score)
        recovery_class = _estimated_recovery_class(
            priority_score,
            _coerce_int(lead.get("est_value")),
            reference_intent,
        )
        follow_up_rule = FOLLOW_UP_RULES[reason]
        last_call_summary = str((latest_call or {}).get("summary") or "")
        last_call_outcome = str((latest_call or {}).get("outcome") or "")
        last_objection_tags = list(reference_call.get("_objection_tags") or [])

        cards.append(
            {
                "lead_id": lead_id,
                "lead_name": str(lead.get("owner_name") or lead.get("address") or "Unknown lead"),
                "name": str(lead.get("owner_name") or lead.get("address") or "Unknown lead"),
                "address": str(lead.get("address") or ""),
                "suburb": str(lead.get("suburb") or ""),
                "postcode": str(lead.get("postcode") or ""),
                "status": str(lead.get("status") or ""),
                "reason": reason,
                "reason_label": str(follow_up_rule["reason_label"]),
                "priority_score": priority_score,
                "last_contacted_at": last_contacted_at.isoformat() if last_contacted_at else None,
                "last_interaction_at": (
                    (latest_call or {}).get("timestamp")
                    or (latest_call or {}).get("logged_at")
                    or (last_contacted_at.isoformat() if last_contacted_at else None)
                ),
                "last_call_outcome": last_call_outcome,
                "last_call_summary": last_call_summary,
                "suggested_action": str(preview_follow_up["suggested_action"]),
                "follow_up_due_at": str(preview_follow_up["follow_up_due_at"]),
                "hours_since_contact": hours_since_contact,
                "intent_signal": round(reference_intent, 2),
                "urgency_band": urgency_band,
                "action_type": str(preview_follow_up["action_type"]),
                "estimated_recovery_class": recovery_class,
                "reason_detail": _reason_detail(
                    reason=reason,
                    intent_signal=reference_intent,
                    hours_since_contact=hours_since_contact,
                    hours_since_last_call=latest_call_age_hours,
                    price_drop_count=price_drop_count,
                    booking_attempted=booking_attempted,
                    next_step_detected=next_step_detected,
                ),
                "revenue_signal": str(preview_follow_up["revenue_signal"]),
                "ignore_risk": str(preview_follow_up["ignore_risk"]),
                "last_activity_type": str(lead.get("last_activity_type") or ""),
                "last_objection_tags": last_objection_tags,
                "booking_attempted": booking_attempted,
                "next_step_detected": next_step_detected,
                "talk_time_total": talk_time_total,
                "connected_calls": connected_calls,
                "total_attempts": total_attempts,
                "heat_score": _coerce_int(lead.get("heat_score")),
                "evidence_score": _coerce_int(lead.get("evidence_score")),
                "est_value": _coerce_int(lead.get("est_value")),
            }
        )

    cards.sort(key=lambda item: item["priority_score"], reverse=True)
    return cards
