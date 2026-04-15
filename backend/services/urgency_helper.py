import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional

RULES_PATH = Path(__file__).resolve().parents[2] / "shared" / "urgency_rules.json"


def _load_rules() -> dict[str, Any]:
    with RULES_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


@lru_cache(maxsize=1)
def _rules() -> dict[str, Any]:
    return _load_rules()


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _days_since(value: Optional[str]) -> Optional[int]:
    parsed = _parse_datetime(value)
    if not parsed:
        return None
    now = datetime.now(timezone.utc)
    delta = now - parsed
    return max(0, int(delta.total_seconds() // 86400))


def _format_days_context(days: Optional[int]) -> str:
    if days is None:
        return ""
    if days == 0:
        return "Today"
    if days == 1:
        return "Yesterday"
    return f"{days}d ago"


def _normalize_status(value: Optional[str]) -> str:
    return (value or "").strip().upper()


def calculate_lead_urgency(lead: Mapping[str, Any]) -> dict[str, Any]:
    rules = _rules()
    trigger_map = {k: v for k, v in rules["triggerStatusMap"].items()}
    force_high = set(rules["forceHighStatuses"])
    withdrawn_statuses = set(rules["withdrawnStatuses"])
    stale_status = rules["staleStatus"]
    withdrawn_thresholds = rules["withdrawnThresholds"]
    stale_thresholds = rules["staleThresholds"]
    contact_threshold = rules["coldContactDays"]
    fallback_reasons = rules["fallbackReasons"]

    status = _normalize_status(lead.get("signal_status"))
    trigger_raw = (lead.get("trigger_type") or "").strip().lower()
    trigger_status = trigger_map.get(trigger_raw, "").upper()
    effective_status = status or trigger_status

    days_since_signal = _days_since(lead.get("signal_date") or lead.get("created_at"))
    days_since_contact = _days_since(lead.get("last_contacted_at"))

    urgency: str = "LOW"
    if effective_status in force_high:
        urgency = "HIGH"
    elif effective_status in withdrawn_statuses:
        days = days_since_signal
        for rule in withdrawn_thresholds:
            max_days = rule["maxDays"]
            if max_days is None or (days is not None and days <= max_days):
                urgency = rule["urgency"]
                break
    elif effective_status == stale_status:
        dom = int(lead.get("days_on_market") or 0)
        stale_dom = stale_thresholds["daysOnMarket"]
        stale_signal = stale_thresholds["daysSinceSignal"]
        urgency = "HIGH" if dom > stale_dom or (days_since_signal is not None and days_since_signal > stale_signal) else "MEDIUM"
    elif days_since_contact is not None and days_since_contact > contact_threshold:
        urgency = "MEDIUM"

    fallback_reason = _build_fallback_reason(
        effective_status,
        days_since_contact,
        days_since_signal,
        lead.get("days_on_market"),
        fallback_reasons,
        contact_threshold,
    )

    return {
        "effective_status": effective_status,
        "urgency": urgency,
        "days_since_signal": days_since_signal,
        "days_since_contact": days_since_contact,
        "time_context": _format_days_context(days_since_signal),
        "fallback_reason": fallback_reason,
    }


def _build_fallback_reason(
    effective_status: str,
    days_since_contact: Optional[int],
    days_since_signal: Optional[int],
    days_on_market: Any,
    fallback_reasons: Mapping[str, str],
    contact_threshold: int,
) -> Optional[str]:
    if reason := fallback_reasons.get(effective_status):
        if effective_status == "STALE":
            dom_label = days_on_market if days_on_market is not None else "Extended"
            return reason.replace("{daysOnMarket}", str(dom_label))
        return reason

    if days_since_contact is not None and days_since_contact > contact_threshold:
        cooldown = fallback_reasons.get("CONTACT_COOLDOWN")
        if cooldown:
            return cooldown.replace("{days}", str(days_since_contact))

    return None
