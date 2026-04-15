from typing import Any, Dict, Optional
import datetime as _dt

from core.utils import _dedupe_by_phone, _dedupe_text_list


def _parse_any_date(value: Optional[str]) -> Optional[_dt.datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"-", "n/a", "na", "none", "null"}:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y"):
        try:
            return _dt.datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt)
        except ValueError:
            continue
    try:
        return _dt.datetime.fromisoformat(text[:19])
    except (ValueError, TypeError):
        return None


def _ownership_years(lead: Dict[str, Any]) -> Optional[float]:
    for key in ("settlement_date", "sale_date", "last_settlement_date"):
        parsed = _parse_any_date(lead.get(key))
        if parsed:
            return (_dt.datetime.utcnow() - parsed).days / 365.25
    return None


def _signal_age_decay(signal_date: Optional[str], trigger_type: Optional[str], lead: Optional[Dict[str, Any]] = None) -> int:
    """
    Return a decay penalty (negative int) based on how old the signal is.
    Fresh signals get 0 penalty; stale signals lose up to 25 points.
    domain_withdrawn and marketing_list decay faster — their urgency is time-bound.
    """
    parsed = _parse_any_date(signal_date)
    if not parsed:
        return -5
    days_old = (_dt.datetime.utcnow() - parsed).days

    trigger = (trigger_type or "").lower()
    is_urgent = any(k in trigger for k in ("withdrawn", "marketing_list", "domain_withdrawn", "rescinded"))

    lead_data = lead or {}
    tenure_years = _ownership_years(lead_data)
    is_mortgage_cliff_profile = str(lead_data.get("lead_archetype") or "").strip().lower() == "mortgage_cliff" or str(
        lead_data.get("route_queue") or ""
    ).strip().upper() == "MORTGAGE"
    is_high_tenure_profile = tenure_years is not None and tenure_years >= 10.0

    if is_urgent:
        if "marketing_list" in trigger and days_old > 30 and (is_mortgage_cliff_profile or is_high_tenure_profile):
            if days_old <= 60:
                return -8
            if days_old <= 120:
                return -12
            return -15
        if days_old <= 3:
            return 0
        if days_old <= 14:
            return -5
        if days_old <= 30:
            return -15
        return -25

    if days_old <= 30:
        return 0
    if days_old <= 90:
        return -5
    if days_old <= 180:
        return -10
    return -15


def _trigger_bonus(trigger_type: Optional[str], stage: Optional[str]) -> int:
    trigger = (trigger_type or "").strip().lower()
    lifecycle = (stage or "").strip().lower()
    if "rescinded" in lifecycle or "rescinded" in trigger:
        return 34
    if "construction" in lifecycle:
        return 26
    if "withdrawn" in trigger:
        return 28
    if "marketing" in trigger:
        return 20
    if "contract" in trigger:
        return 24
    if "subdivision" in lifecycle or "lot" in trigger:
        return 18
    if "probate" in trigger:
        return 12
    if "mortgage" in trigger and "cliff" in trigger:
        return 22
    return 8 if trigger else 0


def _status_penalty(status: Optional[str]) -> int:
    penalties = {
        "captured": 0,
        "qualified": 0,
        "outreach_ready": 0,
        "contacted": -8,
        "appt_booked": -18,
        "mortgage_appt_booked": -18,
        "converted": -100,
        "dropped": -100,
    }
    return penalties.get((status or "captured").strip(), 0)


def _score_lead(lead: Dict[str, Any]) -> Dict[str, int]:
    evidence_items = _dedupe_text_list(lead.get("source_evidence"))
    linked_files = _dedupe_text_list(lead.get("linked_files"))
    summary_points = _dedupe_text_list(lead.get("summary_points"))
    key_details = _dedupe_text_list(lead.get("key_details"))
    images = _dedupe_text_list(lead.get("property_images")) or ([lead.get("main_image")] if lead.get("main_image") else [])
    phones = _dedupe_by_phone(lead.get("contact_phones"))
    emails = _dedupe_text_list(lead.get("contact_emails"))

    evidence_score = min(
        100,
        len(evidence_items) * 12
        + len(linked_files) * 12
        + len(summary_points) * 4
        + len(key_details) * 3
        + len(images) * 5
        + len(phones) * 7
        + len(emails) * 5,
    )

    base = (
        int(lead.get("heat_score") or 0) * 0.25
        + int(lead.get("confidence_score") or 0) * 0.18
        + int(lead.get("propensity_score") or 0) * 0.12
        + int(lead.get("readiness_score") or 0) * 0.12
        + int(lead.get("conversion_score") or 0) * 0.08
        + evidence_score * 0.25
    )
    contact_bonus = min(12, len(phones) * 5 + len(emails) * 2)
    contact_penalty = -20 if not phones and not emails else -8 if not phones else 0
    record_bonus = 8 if (lead.get("record_type") or "") == "marketing_contact" else 4 if (lead.get("record_type") or "") == "property_report" else 0

    cliff_urgency_bonus = 0
    trigger_lower = (lead.get("trigger_type") or "").lower()
    archetype_lower = (lead.get("lead_archetype") or "").strip().lower()
    route_queue = (lead.get("route_queue") or "").strip().upper()
    has_cliff_signal = ("mortgage" in trigger_lower and "cliff" in trigger_lower) or archetype_lower == "mortgage_cliff" or route_queue == "MORTGAGE"
    if has_cliff_signal:
        years_since = _ownership_years(lead)
        if years_since is not None and 2.5 <= years_since <= 3.5:
            cliff_urgency_bonus = 15

    age_decay = _signal_age_decay(
        lead.get("signal_date") or lead.get("date_found") or lead.get("created_at"),
        lead.get("trigger_type"),
        lead,
    )

    call_today_score = round(
        base
        + _trigger_bonus(lead.get("trigger_type"), lead.get("lifecycle_stage"))
        + contact_bonus
        + contact_penalty
        + record_bonus
        + cliff_urgency_bonus
        + age_decay
        + _status_penalty(lead.get("status"))
    )

    return {
        "evidence_score": max(0, min(100, evidence_score)),
        "call_today_score": max(0, min(100, call_today_score)),
    }


def compute_derived_scores(lead: Dict[str, Any]) -> Dict[str, int]:
    """
    Rule-based computation for the 4 previously-empty score fields.
    Called by batch_populate_scores() and at lead upsert time.

    confidence_score: data provenance quality
    propensity_score: owner likelihood to sell/refinance soon
    readiness_score: operational readiness for outreach
    conversion_score: expected conversion likelihood
    """
    trigger = (lead.get("trigger_type") or "").lower()
    owner_type = (lead.get("owner_type") or "").lower()
    record_type = (lead.get("record_type") or "").lower()
    status = (lead.get("status") or "captured").lower()
    phones = _dedupe_by_phone(lead.get("contact_phones"))
    emails = _dedupe_text_list(lead.get("contact_emails"))
    heat = int(lead.get("heat_score") or 0)

    if "cotality" in trigger or record_type == "property_report":
        confidence = 80
    elif "domain" in trigger:
        confidence = 70
    elif "marketing" in trigger or record_type == "marketing_contact":
        confidence = 50
    elif "probate" in trigger or "delta" in trigger:
        confidence = 65
    elif "reaxml" in trigger or "sitemap" in trigger:
        confidence = 60
    else:
        confidence = 45
    if phones:
        confidence = min(100, confidence + 10)
    if emails:
        confidence = min(100, confidence + 5)

    propensity = 40
    if "owner" in owner_type or "occupier" in owner_type:
        propensity += 15
    elif "investor" in owner_type:
        propensity += 8

    years_since = _ownership_years(lead)
    if years_since is not None:
        if 2.0 <= years_since <= 4.0:
            propensity += 20
        elif 4.0 < years_since <= 8.0:
            propensity += 10
        elif years_since > 8.0:
            propensity += 5

    if "withdrawn" in trigger:
        propensity += 20
    elif "mortgage" in trigger and "cliff" in trigger:
        propensity += 18
    elif "probate" in trigger:
        propensity += 12
    propensity = min(100, propensity)

    readiness = 30
    if phones:
        readiness += 25
    if heat >= 70:
        readiness += 20
    elif heat >= 50:
        readiness += 10

    last_contacted = lead.get("last_contacted_at")
    if last_contacted:
        parsed_last_contact = _parse_any_date(last_contacted)
        if parsed_last_contact:
            days_since = (_dt.datetime.utcnow() - parsed_last_contact).days
            if days_since > 60:
                readiness += 10
            elif days_since < 7:
                readiness -= 15
    else:
        readiness += 10

    if status in ("captured", "qualified", "outreach_ready"):
        readiness += 5
    elif status in ("contacted",):
        readiness -= 5
    elif status in ("appt_booked", "converted"):
        readiness = 0
    readiness = max(0, min(100, readiness))

    conversion = 20
    if heat >= 70:
        conversion += 15
    if "withdrawn" in trigger:
        conversion += 15
    elif "mortgage" in trigger and "cliff" in trigger:
        conversion += 12
    if phones:
        conversion += 10
    if propensity >= 60:
        conversion += 10
    if readiness >= 60:
        conversion += 8
    if status == "appt_booked":
        conversion = 90
    elif status == "converted":
        conversion = 100
    elif status in ("dropped",):
        conversion = 0
    conversion = max(0, min(100, conversion))

    return {
        "confidence_score": confidence,
        "propensity_score": propensity,
        "readiness_score": readiness,
        "conversion_score": conversion,
    }
