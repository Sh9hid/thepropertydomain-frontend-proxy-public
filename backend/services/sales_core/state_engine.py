from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional


ATTEMPT_COOLDOWNS = {
    "connected_interested": timedelta(hours=48),
    "call_back": timedelta(hours=24),
    "voicemail_left": timedelta(hours=24),
    "no_answer": timedelta(hours=12),
    "wrong_number": timedelta(days=3650),
    "do_not_call": timedelta(days=3650),
}

CLOSED_STATUSES = {"converted", "dropped", "appt_booked", "mortgage_appt_booked", "closed", "booked"}


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", 0):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _normalize_phone(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not raw:
        return ""
    if raw.startswith("61"):
        return f"+{raw}"
    if raw.startswith("0") and len(raw) >= 10:
        return f"+61{raw[1:]}"
    if str(value or "").startswith("+"):
        return str(value).strip()
    return raw


def _has_valid_phone(value: Any) -> bool:
    phone = _normalize_phone(value)
    digits = "".join(ch for ch in phone if ch.isdigit())
    return len(digits) >= 9


def _fatigue_band(attempts_last_7d: int) -> str:
    if attempts_last_7d >= 6:
        return "high"
    if attempts_last_7d >= 2:
        return "medium"
    return "low"


def _best_contact_window(now: datetime) -> str:
    hour = now.astimezone(timezone.utc).hour
    if hour < 4:
        return "morning"
    if hour < 10:
        return "afternoon"
    return "evening"


def _queue_score(*, lead_est_value: int, lead_heat_score: int, lead_evidence_score: int, attempts_last_7d: int, needs_enrichment: bool, stale_enrichment: bool, callable_now: bool) -> float:
    score = 0.0
    score += min(80.0, max(0.0, lead_heat_score))
    score += min(60.0, max(0.0, lead_evidence_score))
    score += min(40.0, max(0.0, lead_est_value / 50000.0))
    if callable_now:
        score += 25.0
    if needs_enrichment:
        score += 20.0
    if stale_enrichment:
        score += 10.0
    score -= attempts_last_7d * 8.0
    return max(0.0, round(score, 2))


def build_lead_state_snapshot(raw: Dict[str, Any], *, now: Optional[datetime] = None) -> Dict[str, Any]:
    resolved_now = now or datetime.now(timezone.utc)
    attempts: Iterable[Dict[str, Any]] = list(raw.get("attempts") or [])
    normalized_attempts = []
    for attempt in attempts:
        attempted_at = _coerce_datetime(attempt.get("attempted_at"))
        if attempted_at is None:
            continue
        normalized_attempts.append(
            {
                **attempt,
                "attempted_at": attempted_at,
                "next_action_due_at": _coerce_datetime(attempt.get("next_action_due_at")),
            }
        )
    normalized_attempts.sort(key=lambda item: item["attempted_at"], reverse=True)

    last_attempt = normalized_attempts[0] if normalized_attempts else None
    attempts_last_7d = sum(1 for item in normalized_attempts if item["attempted_at"] >= resolved_now - timedelta(days=7))
    last_outcome = str((last_attempt or {}).get("outcome") or "").strip().lower()
    lead_status = str(raw.get("lead_status") or "").strip().lower()
    phone_verified = str(raw.get("phone_verified") or raw.get("phone_verification_status") or "").strip().lower() in {"verified", "true", "1", "yes"}
    has_phone = _has_valid_phone(raw.get("primary_phone"))
    do_not_call = bool(raw.get("do_not_call"))
    explicit_due = _coerce_datetime(raw.get("next_action_due_at"))
    attempt_due = (last_attempt or {}).get("next_action_due_at")
    next_action_due_at = attempt_due or explicit_due

    cooldown_until = None
    if last_attempt:
        cooldown_window = ATTEMPT_COOLDOWNS.get(last_outcome)
        if cooldown_window:
            cooldown_until = last_attempt["attempted_at"] + cooldown_window
        if next_action_due_at and (cooldown_until is None or next_action_due_at > cooldown_until):
            cooldown_until = next_action_due_at

    in_cooldown = cooldown_until is not None and cooldown_until > resolved_now
    missing_contactability = not has_phone and not str(raw.get("primary_email") or "").strip()
    stale_enrichment = str(raw.get("enrichment_status") or "").strip().lower() in {"stale", "failed", "retry"}
    needs_enrichment = missing_contactability and (
        int(raw.get("lead_heat_score") or 0) >= 70
        or int(raw.get("lead_evidence_score") or 0) >= 60
        or int(raw.get("lead_est_value") or 0) >= 900000
        or stale_enrichment
    )
    callable_now = (
        has_phone
        and not do_not_call
        and lead_status not in CLOSED_STATUSES
        and not in_cooldown
        and (next_action_due_at is None or next_action_due_at <= resolved_now)
        and last_outcome not in {"wrong_number", "do_not_call"}
    )

    if callable_now:
        next_action = "call"
    elif needs_enrichment:
        next_action = "enrich_first"
    elif do_not_call or last_outcome in {"wrong_number", "do_not_call"}:
        next_action = "skip"
    elif in_cooldown and next_action_due_at:
        next_action = "follow_up_due"
    elif in_cooldown:
        next_action = "cooldown"
    else:
        next_action = "review"

    queue_score = _queue_score(
        lead_est_value=int(raw.get("lead_est_value") or 0),
        lead_heat_score=int(raw.get("lead_heat_score") or 0),
        lead_evidence_score=int(raw.get("lead_evidence_score") or 0),
        attempts_last_7d=attempts_last_7d,
        needs_enrichment=needs_enrichment,
        stale_enrichment=stale_enrichment,
        callable_now=callable_now,
    )

    return {
        "lead_contact_id": raw.get("lead_contact_id"),
        "business_context_key": raw.get("business_context_key"),
        "total_attempts": len(normalized_attempts),
        "attempts_last_7d": attempts_last_7d,
        "last_attempt_at": last_attempt["attempted_at"].isoformat() if last_attempt else None,
        "last_attempt_outcome": last_outcome or None,
        "last_contact_at": last_attempt["attempted_at"].isoformat() if last_attempt and bool(last_attempt.get("connected")) else None,
        "last_response_at": last_attempt["attempted_at"].isoformat() if last_attempt and bool(last_attempt.get("connected")) else None,
        "best_contact_window": _best_contact_window(resolved_now),
        "fatigue_band": _fatigue_band(attempts_last_7d),
        "callable_now": callable_now,
        "next_action": next_action,
        "next_action_due_at": next_action_due_at.isoformat() if next_action_due_at else (cooldown_until.isoformat() if cooldown_until and next_action == "cooldown" else None),
        "queue_score": queue_score,
        "needs_enrichment": needs_enrichment,
        "stale_enrichment": stale_enrichment,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
        "phone_verified": phone_verified,
        "has_valid_phone": has_phone,
    }
