from core.config import *
from models.schemas import *
from core.utils import *
from services.scoring import _score_lead, _trigger_bonus, _status_penalty, compute_derived_scores
from services.property_visuals import build_property_visuals
from fastapi import HTTPException
import json, datetime, hashlib, os, re, asyncio
from typing import Any, Dict, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


def _project_log_header() -> str:
    return (
        "# Project Vision Log\n\n"
        f"**Brand**: {BRAND_NAME}\n\n"
        f"**Rule**: {PROJECT_MEMORY_RULE}\n\n"
        "## Project Summary\n"
        "- Real-estate interface uses Laing+Simmons Oakville | Windsor branding.\n"
        "- Ownit1st remains the future separate mortgage interface.\n"
        "- Local stock data from config.STOCK_ROOT is the primary intelligence layer.\n"
        "- Cotality is a report/enrichment reference layer, not the lead backbone.\n\n"
    )


def append_project_memory(prompt: str, intent: Optional[str] = None, source: str = "user") -> None:
    prompt = (prompt or "").strip()
    if not prompt:
        return
    PROJECT_ROOT.mkdir(parents=True, exist_ok=True)
    if not PROJECT_LOG_PATH.exists():
        PROJECT_LOG_PATH.write_text(_project_log_header(), encoding="utf-8")
    timestamp = format_sydney()
    entry_lines = [
        f"## {timestamp} Sydney",
        f"Source: {source}",
        f"Prompt: {prompt}",
    ]
    if intent:
        entry_lines.append(f"Intent: {intent}")
    entry_lines.append("")
    with PROJECT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(entry_lines))


def ensure_project_memory_file() -> None:
    if PROJECT_LOG_PATH.exists():
        return
    PROJECT_LOG_PATH.write_text(_project_log_header(), encoding="utf-8")
    seed_entries = [
        (
            "Project-wide directive: preserve the current beautiful UI, turn the CRM into a polished property intelligence and outreach system, and use config.STOCK_ROOT as the core data source.",
            "product_vision",
        ),
        (
            "Need every lead to own its own notes, timeline, emails, texts, evidence, filters, and actionable insights without centralizing notes.",
            "crm_behavior",
        ),
        (
            "Need generic and generated PDF attachments for cold outreach under Laing+Simmons Oakville | Windsor branding, inspired by Cotality report quality but using our own brand.",
            "reporting_branding",
        ),
        (
            "This behavior is non-negotiable for all AI on the project: save each prompt with timestamp in a separate project note so future agents understand the vision.",
            "ai_memory_rule",
        ),
    ]
    for prompt, intent in seed_entries:
        append_project_memory(prompt, intent=intent, source="seed")


def get_deterministic_id(address: str) -> str:
    return hashlib.md5(address.lower().replace(" ", "").strip().encode()).hexdigest()


def _append_stage_note(
    existing: Any,
    note: str,
    status: str,
    channel: str = "note",
    subject: Optional[str] = None,
    recipient: Optional[str] = None,
) -> List[Dict[str, Any]]:
    history = _parse_json_list(existing)
    history.append(
        {
            "note": note.strip(),
            "status": status,
            "channel": channel,
            "subject": subject or "",
            "recipient": recipient or "",
            "timestamp": now_iso(),
            "timestamp_sydney": format_sydney(),
        }
    )
    return history[-50:]


def _append_activity(existing: Any, activity: Dict[str, Any]) -> List[Dict[str, Any]]:
    history = _parse_json_list(existing)
    history.append(activity)
    return history[-120:]


def _build_activity_entry(
    activity_type: str,
    note: Optional[str] = None,
    status: Optional[str] = None,
    channel: Optional[str] = None,
    subject: Optional[str] = None,
    recipient: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "type": activity_type,
        "note": note or "",
        "status": status or "",
        "channel": channel or "",
        "subject": subject or "",
        "recipient": recipient or "",
        "timestamp": now_iso(),
        "timestamp_sydney": format_sydney(),
    }


def _recent_touch_count(history: Any, days: int) -> int:
    cutoff = now_sydney() - datetime.timedelta(days=days)
    count = 0
    for entry in _parse_json_list(history):
        if not isinstance(entry, dict):
            continue
        ts = _parse_iso_datetime(entry.get("timestamp"))
        if not ts:
            continue
        if ts.astimezone(SYDNEY_TZ) >= cutoff:
            count += 1
    return count


def _lead_has_phone(lead: Dict[str, Any]) -> bool:
    return bool(_dedupe_by_phone(lead.get("contact_phones")))


def _lead_has_sms_mobile(lead: Dict[str, Any]) -> bool:
    phones = _dedupe_by_phone(lead.get("contact_phones"))
    return any(_is_sms_mobile_au(phone) for phone in phones)


def _lead_has_email(lead: Dict[str, Any]) -> bool:
    return bool(_dedupe_text_list(lead.get("contact_emails")))


def _infer_contactability_status(lead: Dict[str, Any]) -> str:
    has_phone = _lead_has_phone(lead)
    has_email = _lead_has_email(lead)
    if _bool_db(lead.get("do_not_call")):
        return "suppressed"
    if has_phone and has_email:
        return "contact_ready"
    if has_phone:
        return "phone_ready"
    if has_email:
        return "email_ready"
    return "enrichment_needed"


def _tenure_bucket_from_years(years: Optional[float]) -> str:
    if years is None:
        return ""
    if years >= 10:
        return "10y_plus"
    if years >= 7:
        return "7_10y"
    if years >= 4:
        return "4_7y"
    if years >= 2:
        return "2_4y"
    return "under_2y"


def _compute_ownership_years(lead: Dict[str, Any]) -> Optional[float]:
    existing = lead.get("ownership_duration_years")
    if existing not in (None, ""):
        try:
            return round(float(existing), 1)
        except (TypeError, ValueError):
            pass
    ownership_date = lead.get("settlement_date") or lead.get("sale_date") or lead.get("last_settlement_date")
    dt = _parse_iso_datetime(ownership_date)
    if not dt and ownership_date:
        raw = str(ownership_date).strip()
        for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
            try:
                dt = datetime.datetime.strptime(raw, fmt).replace(tzinfo=SYDNEY_TZ)
                break
            except ValueError:
                continue
    if not dt:
        return None
    if isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
        dt = datetime.datetime(dt.year, dt.month, dt.day, tzinfo=SYDNEY_TZ)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SYDNEY_TZ)
    years = max(0.0, (now_sydney() - dt.astimezone(SYDNEY_TZ)).days / 365.25)
    return round(years, 1)


def _derive_contactability_tier(lead: Dict[str, Any]) -> tuple[str, List[str]]:
    reasons = _dedupe_text_list(lead.get("contactability_reasons"))
    if _bool_db(lead.get("do_not_call")):
        reasons = _dedupe_text_list([*reasons, "Do not call flag present"])
        return "blocked", reasons
    phones = _dedupe_by_phone([*(lead.get("contact_phones") or []), *(lead.get("alternate_phones") or [])])
    emails = _dedupe_text_list([*(lead.get("contact_emails") or []), *(lead.get("alternate_emails") or [])])
    phone_status = _normalize_token(lead.get("phone_status"))
    email_status = _normalize_token(lead.get("email_status"))
    if phones:
        reasons = _dedupe_text_list([*reasons, "Phone on file"])
    if phone_status in {"verified", "connected", "valid"}:
        reasons = _dedupe_text_list([*reasons, f"Phone status {phone_status}"])
    if emails:
        reasons = _dedupe_text_list([*reasons, "Email on file"])
    if email_status in {"deliverable", "verified", "valid"}:
        reasons = _dedupe_text_list([*reasons, f"Email status {email_status}"])
    if phones and phone_status in {"verified", "connected", "valid"}:
        return "high", reasons
    if phones or emails:
        return "medium", reasons
    return "low", _dedupe_text_list([*reasons, "No verified contact path"])


def _derive_seller_intent_signals(lead: Dict[str, Any]) -> List[Dict[str, Any]]:
    existing = _parse_json_list(lead.get("seller_intent_signals"))
    if existing:
        return existing
    signals: List[Dict[str, Any]] = []
    years = _compute_ownership_years(lead)
    if years is not None and years >= 7:
        anchor = lead.get("settlement_date") or lead.get("sale_date") or "recorded acquisition"
        signals.append({
            "key": "long_ownership",
            "label": "Long ownership",
            "evidence": f"Held since {anchor}",
            "strength": "high" if years >= 10 else "medium",
        })
    if _bool_db(lead.get("absentee_owner")) or (
        lead.get("mailing_address") and not _bool_db(lead.get("mailing_address_matches_property"))
    ):
        signals.append({
            "key": "absentee_owner",
            "label": "Absentee owner",
            "evidence": "Mailing address differs from the property address",
            "strength": "medium",
        })
    if _bool_db(lead.get("likely_landlord")) or _normalize_token(lead.get("owner_type")) in {"investor", "rented"}:
        signals.append({
            "key": "landlord_signal",
            "label": "Landlord signal",
            "evidence": "Investor / rental ownership pattern is present",
            "strength": "medium",
        })
    dom = _safe_int(lead.get("days_on_market"), 0)
    listing_status = _normalize_token(lead.get("last_listing_status"))
    if dom >= 45 or listing_status in {"withdrawn", "expired"}:
        evidence = f"Last known listing status is {listing_status}" if listing_status else f"Days on market reached {dom}"
        signals.append({
            "key": "stale_listing_pattern",
            "label": "Stale listing pattern",
            "evidence": evidence,
            "strength": "medium",
        })
    return signals


def _derive_refinance_signals(lead: Dict[str, Any]) -> List[Dict[str, Any]]:
    existing = _parse_json_list(lead.get("refinance_signals"))
    if existing:
        return existing
    signals: List[Dict[str, Any]] = []
    years = _compute_ownership_years(lead)
    last_sale = _safe_int(lead.get("sale_price"), 0)
    est_high = _safe_int(lead.get("estimated_value_high") or lead.get("est_value"), 0)
    if years is not None and 2 <= years <= 7:
        signals.append({
            "key": "refinance_window",
            "label": "Ownership age in refinance window",
            "evidence": f"Ownership duration is {years:.1f} years",
        })
    if last_sale and est_high and est_high > last_sale:
        uplift = est_high - last_sale
        signals.append({
            "key": "equity_position",
            "label": "Likely equity position",
            "evidence": f"Estimated value is ${est_high:,.0f} versus last recorded sale ${last_sale:,.0f} (+${uplift:,.0f})",
        })
    return signals


def _normalize_token(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _infer_strike_zone(lead: Dict[str, Any]) -> str:
    suburb = _normalize_token(lead.get("suburb"))
    if suburb == _normalize_token(PRIMARY_STRIKE_SUBURB):
        return "primary"
    if suburb in {_normalize_token(item) for item in SECONDARY_STRIKE_SUBURBS}:
        return "secondary"
    return "background"


def _infer_lead_archetype(lead: Dict[str, Any]) -> str:
    trigger = _normalize_token(lead.get("trigger_type"))
    lifecycle = _normalize_token(lead.get("lifecycle_stage"))
    owner_type = _normalize_token(lead.get("owner_type"))
    zone = _normalize_token(lead.get("development_zone"))
    land_size = float(lead.get("land_size_sqm") or 0)
    if "probate" in trigger or "estate" in lifecycle:
        return "documented_signal"
    if owner_type == "rented":
        return "investor_landlord"
    if land_size >= 650 and zone:
        return "development_candidate"
    if lead.get("sale_date") or lead.get("settlement_date"):
        return "equity_review"
    if lead.get("agency_name") or lead.get("agent_name"):
        return "competitor_displacement"
    return "direct_seller"


def _default_preferred_channel(lead: Dict[str, Any]) -> str:
    if _lead_has_phone(lead):
        return "call"
    if _lead_has_email(lead):
        return "email"
    return "enrichment"


def _owner_first_name(lead: Dict[str, Any]) -> str:
    owner = str(lead.get("owner_name") or "").strip()
    return owner.split()[0] if owner else "there"


def _value_hook(lead: Dict[str, Any]) -> str:
    suburb = lead.get("suburb") or "your area"
    address = lead.get("address") or suburb
    owner_type = _normalize_token(lead.get("owner_type"))
    zone = lead.get("development_zone")
    land_size = lead.get("land_size_sqm")
    if owner_type == "rented":
        return f"I can map the current rent-vs-sell position for {address} in {suburb}."
    if zone and land_size:
        return f"Your {land_size}sqm holding sits in {zone}, which changes the buyer profile and value conversation."
    if lead.get("settlement_date") or lead.get("sale_date"):
        date_hint = lead.get("settlement_date") or lead.get("sale_date")
        return f"A lot has changed in {suburb} since {date_hint}; I can give you a quick equity and pricing reset."
    if lead.get("sale_price"):
        return f"I can compare what {address} might command now against the last recorded ${lead.get('sale_price')} sale context."
    return f"I can send a concise sold snapshot for {suburb} and where your property likely sits today."


def _message_bundle(lead: Dict[str, Any], purpose: str) -> Dict[str, str]:
    import os as _os
    sender_sms = _os.getenv("OWNIT1ST_OPERATOR_NAME", "Shahid")
    first = _owner_first_name(lead)
    address = lead.get("address") or lead.get("suburb") or "your property"
    suburb = lead.get("suburb") or "the area"
    hook = _value_hook(lead)
    if purpose == "missed_call_sms":
        return {
            "subject": "",
            "body": f"Hi {first}, {sender_sms} from L+S Oakville — I just tried you re {address}. {hook} Worth a quick chat?",
        }
    if purpose == "market_email":
        return {
            "subject": f"Market update — {address}",
            "body": (
                f"Hi {first},\n\n"
                f"{hook}\n\n"
                "If helpful, I can walk you through value range, recent comparable sales, and likely buyer demand in under 10 minutes.\n\n"
                f"Regards,\n{PRINCIPAL_NAME}\n{BRAND_NAME}"
            ),
        }
    if purpose == "step_back_email":
        return {
            "subject": f"Stepping back — {address}",
            "body": (
                f"Hi {first},\n\n"
                f"I've made a few attempts to connect about {address} without luck, so I'll give you some space. "
                f"I'll keep an eye on the {suburb} market and can pick this up again whenever timing suits.\n\n"
                f"Regards,\n{PRINCIPAL_NAME}\n{BRAND_NAME}"
            ),
        }
    if purpose == "question_reply":
        return {
            "subject": f"Re: {address}",
            "body": f"Hi {first}, thanks for getting back to me. {hook}",
        }
    if purpose == "nurture_sms":
        return {
            "subject": "",
            "body": f"Hi {first}, {sender_sms} from L+S Oakville. {hook} Happy to send details if useful.",
        }
    if purpose == "appointment_confirmation":
        return {
            "subject": f"Appraisal confirmed — {address}",
            "body": (
                f"Hi {first}, {sender_sms} from L+S Oakville — your appraisal for {address} is confirmed. "
                "I'll have local solds, a value range, and next-step options ready."
            ),
        }
    if purpose == "appointment_reminder":
        return {
            "subject": f"Appraisal reminder — {address}",
            "body": f"Hi {first}, {sender_sms} from L+S Oakville — just a reminder about your appraisal at {address} today.",
        }
    return {
        "subject": f"Market update — {address}",
        "body": f"Hi {first}, {sender_sms} from L+S Oakville. {hook}",
    }


def _next_business_slot(days_offset: int = 0, hour: int = 10, minute: int = 0) -> datetime.datetime:
    candidate = now_sydney() + datetime.timedelta(days=days_offset)
    candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
    while candidate.weekday() >= 5:
        candidate += datetime.timedelta(days=1)
        candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate < now_sydney():
        candidate = now_sydney() + datetime.timedelta(minutes=10)
        candidate = candidate.replace(second=0, microsecond=0)
    return candidate


def _task_sort_key(task: Dict[str, Any]) -> tuple[int, str]:
    ranks = {
        "overdue": 0,
        "call_now": 1,
        "callback_due": 2,
        "send_now": 3,
        "enrichment": 4,
        "follow_up": 5,
        "nurture": 6,
    }
    return (ranks.get(task.get("priority_bucket") or "", 9), task.get("due_at") or "")


def _task_id(lead_id: str, cadence_name: str, cadence_step: int, task_type: str, due_at: str) -> str:
    return hashlib.md5(f"{lead_id}:{cadence_name}:{cadence_step}:{task_type}:{due_at}".encode()).hexdigest()


def _task_to_dict(row: Any) -> Dict[str, Any]:
    task = dict(row) if row else {}
    if task:
        task["auto_generated"] = _bool_db(task.get("auto_generated"))
    return task


def _appointment_to_dict(row: Any) -> Dict[str, Any]:
    appointment = dict(row) if row else {}
    if appointment:
        appointment["auto_generated"] = _bool_db(appointment.get("auto_generated"))
    return appointment


def _append_note_text(existing: str, extra: str) -> str:
    existing_text = (existing or "").strip()
    extra_text = (extra or "").strip()
    if existing_text and extra_text:
        return f"{existing_text}\n{extra_text}"
    return existing_text or extra_text


def _phase_label(cadence_name: str, cadence_step: Any) -> str:
    cadence = _normalize_token(cadence_name).replace("_", " ").strip()
    step = _safe_int(cadence_step, 0)
    if cadence:
        if step > 0:
            return f"{cadence.title()} · Day {step}"
        return cadence.title()
    return f"Day {step}" if step > 0 else "Manual"


def _operator_task_payload(row: Any) -> Dict[str, Any]:
    task = _task_to_dict(row)
    channel = task.get("channel") or task.get("task_type") or "manual"
    approval_status = task.get("approval_status") or "not_required"
    task["phase_label"] = _phase_label(task.get("cadence_name") or "", task.get("cadence_step"))
    task["channel_label"] = str(channel).upper()
    task["approval_required"] = channel in {"sms", "email"}
    task["scheduled_state"] = (
        "queued"
        if approval_status == "approved" and task["approval_required"]
        else "needs_approval"
        if approval_status == "pending" and task["approval_required"]
        else "manual"
    )
    task["send_target"] = (
        (_dedupe_by_phone(task.get("contact_phones"))[0] if _dedupe_by_phone(task.get("contact_phones")) else "")
        if channel == "sms"
        else (_dedupe_text_list(task.get("contact_emails"))[0] if _dedupe_text_list(task.get("contact_emails")) else "")
        if channel == "email"
        else ""
    )
    return task


def _normalize_address_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


async def _find_matching_leads_for_sold_event(session: AsyncSession, address: str, suburb: str) -> List[str]:
    address_key = _normalize_address_key(address)
    suburb_key = _normalize_token(suburb)
    matches: List[str] = []
    
    result = await session.execute(text("SELECT id, address, suburb FROM leads"))
    rows = result.mappings().all()
    
    for row in rows:
        if address_key and _normalize_address_key(row["address"]) == address_key:
            matches.append(row["id"])
            continue
        if suburb_key and suburb_key == _normalize_token(row["suburb"]) and address_key and address_key in _normalize_address_key(row["address"]):
            matches.append(row["id"])
    return matches


def _sold_event_to_dict(row: Any) -> Dict[str, Any]:
    event = dict(row) if row else {}
    if event:
        event["matched_lead_ids"] = _parse_json_list(event.get("matched_lead_ids"))
    return event


def _is_cache_fresh(data_type: str, expires_at: Optional[str], updated_at: Optional[str]) -> bool:
    ttl = COTALITY_TTLS.get(data_type)
    if ttl is None:
        return bool(updated_at)
    expiry = _parse_iso_datetime(expires_at)
    return bool(expiry and expiry > now_sydney())


def _compute_cache_expiry(data_type: str) -> Optional[str]:
    ttl = COTALITY_TTLS.get(data_type)
    return (now_sydney() + ttl).isoformat() if ttl else None


def get_call_angle(lead: Dict[str, Any]) -> str:
    stage = (lead.get("lifecycle_stage") or "").replace("_", " ").strip()
    trigger = (lead.get("trigger_type") or "property signal").strip()
    files = _dedupe_text_list(lead.get("linked_files")) or _dedupe_text_list(lead.get("source_evidence"))
    evidence_hint = Path(files[0]).name if files else "the verified file trail"
    if stage:
        return f"{stage.title()} backed by {evidence_hint}. Lead with the documented event and a short value-first next step."
    return f"{trigger} backed by {evidence_hint}. Open with the concrete file-backed signal, then move to a low-pressure next step."


def _queue_bucket_for_lead(lead: Dict[str, Any]) -> str:
    status = _normalize_token(lead.get("status"))
    outcome = _normalize_token(lead.get("last_outcome"))
    do_not_contact_until = _parse_iso_datetime(lead.get("do_not_contact_until"))
    if status in {"converted", "dropped"} or outcome == "hard_no":
        return "suppressed"
    if do_not_contact_until and do_not_contact_until.astimezone(SYDNEY_TZ) > now_sydney():
        return "callback_due" if outcome in {"not_now", "call_back"} else "nurture"
    contactability = _infer_contactability_status(lead)
    if contactability == "enrichment_needed":
        trigger = _normalize_token(lead.get("trigger_type"))
        lifecycle = _normalize_token(lead.get("lifecycle_stage"))
        return "enrichment" if lead.get("evidence_score", 0) >= 55 or "probate" in trigger or "documented_signal" in lifecycle else "background"
    if outcome == "soft_no":
        return "nurture"
    if outcome in {"wrong_person", "wrong_number"}:
        return "enrichment"
    if outcome in {"question", "send_info", "not_now", "call_back"}:
        return "callback_due"
    if status == "appt_booked" or outcome == "booked_appraisal":
        return "booked"
    return "active"


def _infer_gender(name: str) -> str:
    name = (name or "").strip().lower()
    if not name: return "unknown"
    # Basic heuristics for the demo/use-case
    males = {"mr", "mr.", "darren", "shahid", "nitin", "john", "paul", "david"}
    females = {"ms", "mrs", "ms.", "mrs.", "bryanna", "marie", "louise", "jane", "sarah"}
    
    tokens = name.split()
    for t in tokens:
        if t in males: return "male"
        if t in females: return "female"
    return "unknown"

def _derive_contacts(lead: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Map correct numbers to names and infer gender.
    Structure: { name: str, gender: str, phone: str, email: str }
    """
    contacts = _parse_json_list(lead.get("contacts"))
    if contacts:
        # If we already have structured contacts, just return them
        return contacts
        
    # Otherwise, attempt to derive from flat fields (legacy migration)
    owner_raw = str(lead.get("owner_name") or "").strip()
    phones = _dedupe_by_phone(lead.get("contact_phones"))
    emails = _dedupe_text_list(lead.get("contact_emails"))
    
    derived = []
    # If multiple owners joined by '&'
    names = [n.strip() for n in owner_raw.split('&')] if '&' in owner_raw else [owner_raw]
    
    for i, name in enumerate(names):
        if not name or name == "-": continue
        contact = {
            "name": name,
            "gender": _infer_gender(name),
            "phone": phones[i] if i < len(phones) else (phones[0] if phones else ""),
            "email": emails[i] if i < len(emails) else (emails[0] if emails else ""),
        }
        derived.append(contact)
        
    # If we have more phones than names, add them as generic contacts
    if len(phones) > len(derived):
        for i in range(len(derived), len(phones)):
            derived.append({
                "name": f"Contact {i+1}",
                "gender": "unknown",
                "phone": phones[i],
                "email": emails[i] if i < len(emails) else "",
            })
            
    return derived

def _derive_intelligence(lead: Dict[str, Any]) -> Dict[str, Any]:
    lead["contact_emails"] = _dedupe_text_list(lead.get("contact_emails"))
    lead["contact_phones"] = _dedupe_by_phone(lead.get("contact_phones"))
    lead["alternate_emails"] = _dedupe_text_list(lead.get("alternate_emails"))
    lead["alternate_phones"] = _dedupe_by_phone(lead.get("alternate_phones"))
    lead["contacts"] = _derive_contacts(lead)
    lead["linked_files"] = _dedupe_text_list(lead.get("linked_files"))
    lead["source_evidence"] = _dedupe_text_list(lead.get("source_evidence"))
    lead["property_images"] = _dedupe_text_list(lead.get("property_images"))
    if lead.get("main_image"):
        lead["property_images"] = _dedupe_text_list([lead["main_image"], *lead["property_images"]])
    if not lead.get("main_image") and lead["property_images"]:
        lead["main_image"] = lead["property_images"][0]
    lead["source_tags"] = _dedupe_text_list(lead.get("source_tags"))
    lead["risk_flags"] = _dedupe_text_list(lead.get("risk_flags"))
    lead["contactability_reasons"] = _dedupe_text_list(lead.get("contactability_reasons"))
    lead["deterministic_tags"] = _dedupe_text_list(lead.get("deterministic_tags"))
    lead["activity_log"] = _parse_json_list(lead.get("activity_log"))
    lead["stage_note_history"] = _parse_json_list(lead.get("stage_note_history"))
    lead["sale_history"] = _parse_json_list(lead.get("sale_history"))
    lead["listing_status_history"] = _parse_json_list(lead.get("listing_status_history"))
    lead["nearby_sales"] = _parse_json_list(lead.get("nearby_sales"))
    lead["seller_intent_signals"] = _parse_json_list(lead.get("seller_intent_signals"))
    lead["refinance_signals"] = _parse_json_list(lead.get("refinance_signals"))
    lead["source_provenance"] = _parse_json_list(lead.get("source_provenance"))
    lead["owner_verified"] = _bool_db(lead.get("owner_verified"))
    lead["do_not_call"] = _bool_db(lead.get("do_not_call"))
    lead["mailing_address_matches_property"] = _bool_db(lead.get("mailing_address_matches_property", True))
    lead["absentee_owner"] = _bool_db(lead.get("absentee_owner"))
    lead["likely_landlord"] = _bool_db(lead.get("likely_landlord"))
    lead["likely_owner_occupier"] = _bool_db(lead.get("likely_owner_occupier"))
    lead["property_type"] = lead.get("property_type") or ""
    lead["canonical_address"] = lead.get("canonical_address") or lead.get("address") or ""
    lead["owner_first_name"] = lead.get("owner_first_name") or _owner_first_name(lead)
    owner_name = str(lead.get("owner_name") or "").strip()
    if owner_name and not lead.get("owner_last_name"):
        parts = owner_name.split()
        lead["owner_last_name"] = parts[-1] if len(parts) > 1 else ""
    lead["ownership_duration_years"] = _compute_ownership_years(lead)
    lead["tenure_bucket"] = lead.get("tenure_bucket") or _tenure_bucket_from_years(lead.get("ownership_duration_years"))
    contactability_tier, contactability_reasons = _derive_contactability_tier(lead)
    lead["contactability_tier"] = lead.get("contactability_tier") or contactability_tier
    if not lead.get("contactability_reasons"):
        lead["contactability_reasons"] = contactability_reasons
    lead["seller_intent_signals"] = _derive_seller_intent_signals(lead)
    lead["refinance_signals"] = _derive_refinance_signals(lead)
    lead["deterministic_tags"] = _dedupe_text_list(
        [
            *lead.get("deterministic_tags", []),
            *[signal.get("key") for signal in lead["seller_intent_signals"] if isinstance(signal, dict) and signal.get("key")],
        ]
    )
    if not lead.get("owner_persona"):
        if lead["likely_landlord"]:
            lead["owner_persona"] = "investor_landlord"
        elif lead["absentee_owner"]:
            lead["owner_persona"] = "absentee_owner"
        elif lead["likely_owner_occupier"]:
            lead["owner_persona"] = "owner_occupier"

    # Populate derived scores (confidence/propensity/readiness/conversion) before
    # _score_lead so they contribute to call_today_score weighting.
    derived = compute_derived_scores(lead)
    for k, v in derived.items():
        if not lead.get(k):
            lead[k] = v

    scores = _score_lead(lead)
    lead["evidence_score"] = scores["evidence_score"]

    # Synthetic heat_score: if the DB has no heat (import gap), derive from evidence
    # and trigger signal so the lead is still rankable.
    if not int(lead.get("heat_score") or 0):
        lead["heat_score"] = min(
            100,
            _trigger_bonus(lead.get("trigger_type"), lead.get("lifecycle_stage"))
            + scores["evidence_score"] // 3
            + (10 if lead.get("contact_phones") else 0)
        )
        scores = _score_lead(lead)

    lead["call_today_score"] = scores["call_today_score"]

    # Auto-detect route_queue from trigger_type if not set in DB.
    if not lead.get("route_queue"):
        _tt = (lead.get("trigger_type") or "").lower()
        if "mortgage" in _tt or "refinanc" in _tt or "cliff" in _tt:
            lead["route_queue"] = "mortgage_ownit1st"
        elif "development" in _tt or "subdivision" in _tt or "da_" in _tt:
            lead["route_queue"] = "development_acquisition"
        else:
            lead["route_queue"] = "real_estate"

    owner = lead.get("owner_name") or "Owner"
    suburb = lead.get("suburb") or "the area"
    stage = (lead.get("lifecycle_stage") or lead.get("likely_scenario") or lead.get("trigger_type") or "property event").replace("_", " ").strip()
    primary_phone = lead["contact_phones"][0] if lead["contact_phones"] else ""
    primary_email = lead["contact_emails"][0] if lead["contact_emails"] else ""
    lead["who_to_call"] = lead.get("who_to_call") or (f"{owner} on {primary_phone}" if primary_phone else f"{owner} via {primary_email}" if primary_email else owner)
    lead["why_now"] = lead.get("why_now") or f"{stage.title()} is documented for {suburb}, with {max(1, len(lead['linked_files']) or len(lead['source_evidence']))} supporting evidence item(s)."
    _rq_db = (lead.get("route_queue") or "").lower()
    _tt_db = (lead.get("trigger_type") or "").lower()
    _is_mortgage = _rq_db == "mortgage_ownit1st" or "mortgage" in _tt_db or "refinanc" in _tt_db
    if _is_mortgage:
        from core.config import OWNIT1ST_OPERATOR_NAME, OWNIT1ST_BRAND_NAME
        _sender_name = OWNIT1ST_OPERATOR_NAME
        _sender_brand = OWNIT1ST_BRAND_NAME
    else:
        from core.config import PRINCIPAL_NAME, BRAND_NAME
        _sender_name = PRINCIPAL_NAME
        _sender_brand = BRAND_NAME
    lead["what_to_say"] = lead.get("what_to_say") or (
        f"Hi {owner}, it's {_sender_name} from {_sender_brand}. We've been tracking {stage} activity near {lead.get('address') or suburb} "
        "and I have some market data I think would be useful for you. Happy to run through it quickly."
    )
    lead["recommended_next_step"] = lead.get("recommended_next_step") or (
        "Call the primary number and log the outcome."
        if primary_phone
        else "Send a tailored re-engagement email and set a follow-up reminder."
        if primary_email
        else "Review evidence, enrich contact details, and prepare an address-led follow-up."
    )
    risk_flags = list(lead["risk_flags"])
    if not primary_phone:
        risk_flags.append("No phone on record")
    if not primary_email:
        risk_flags.append("No email on record")
    if not lead.get("main_image"):
        risk_flags.append("No property image attached")
    if not lead["linked_files"] and not lead["source_evidence"]:
        risk_flags.append("Missing linked evidence")
    if not lead.get("lat") or not lead.get("lng"):
        risk_flags.append("No map coordinates")
    lead["risk_flags"] = _dedupe_text_list(risk_flags)
    lead["contactability_status"] = lead.get("contactability_status") or _infer_contactability_status(lead)
    lead["lead_archetype"] = lead.get("lead_archetype") or _infer_lead_archetype(lead)
    lead["strike_zone"] = lead.get("strike_zone") or _infer_strike_zone(lead)
    lead["preferred_channel"] = lead.get("preferred_channel") or _default_preferred_channel(lead)
    lead["touches_14d"] = _recent_touch_count(lead.get("activity_log"), 14)
    lead["touches_30d"] = _recent_touch_count(lead.get("activity_log"), 30)
    lead["queue_bucket"] = lead.get("queue_bucket") or _queue_bucket_for_lead(lead)
    lead["best_call_angle"] = get_call_angle(lead)

    # Frontend type aliases
    lead["agency"] = lead.get("agency") or lead.get("agency_name") or ""
    lead["lot_dp"] = lead.get("lot_dp") or lead.get("parcel_details") or ""
    _rq_raw = lead.get("route_queue") or lead.get("queue_bucket") or "real_estate"
    _rq_map = {
        "mortgage_ownit1st": "MORTGAGE",
        "real_estate": "RE",
        "development_acquisition": "DEVELOPMENT",
        "MORTGAGE": "MORTGAGE",
        "RE": "RE",
        "DEVELOPMENT": "DEVELOPMENT",
    }
    lead["route_queue"] = _rq_map.get(_rq_raw, "RE")

    # signal_status: map DB status + trigger_type → frontend SignalStatus enum
    _status = (lead.get("status") or "captured").lower()
    _trigger = (lead.get("trigger_type") or "").lower()
    _archetype = (lead.get("lead_archetype") or "").lower()
    _phones = lead.get("contact_phones") or []
    if _status == "dropped":
        lead["signal_status"] = "SOLD"
    elif "withdrawn" in _trigger or "withdrawn" in _archetype:
        lead["signal_status"] = "WITHDRAWN"
    elif _trigger in ("delta_engine", "delta", "domain_withdrawn"):
        lead["signal_status"] = "DELTA"
    elif _trigger == "marketing_list" or _status in ("active", "contacted", "qualified", "outreach_ready"):
        # Marketing list leads have real phones — they are warm/live outreach targets
        lead["signal_status"] = "LIVE"
    elif _phones:
        # Any lead with a contact number is worth calling
        lead["signal_status"] = "LIVE"
    else:
        lead.setdefault("signal_status", "OFF-MARKET")

    return lead


def _to_url(path: str) -> str:
    if not path or path.startswith("http"):
        return path
    # Convert a local stock-file path to the backend's protected static URL.
    try:
        stock_root = str(STOCK_ROOT)
        rel = str(path).replace(stock_root, "").replace("\\", "/").lstrip("/")
        return build_public_url(f"/stock-images/{rel}")
    except:
        return path


def _compute_connection_score(lead: Dict[str, Any]) -> Dict[str, Any]:
    """Compute connection strength from engagement signals."""
    points = 0
    signals: list[str] = []

    # Email engagement (max 30 pts)
    opens = int(lead.get("email_open_count") or 0)
    clicks = int(lead.get("email_click_count") or 0)
    if clicks > 0:
        points += min(clicks * 10, 20)
        signals.append(f"{clicks} email click{'s' if clicks != 1 else ''}")
    if opens > 0:
        points += min(opens * 5, 10)
        signals.append(f"{opens} email open{'s' if opens != 1 else ''}")

    # Call engagement (max 40 pts)
    call_count = int(lead.get("call_count") or 0)
    last_outcome = (lead.get("last_outcome") or "").strip().lower()
    if last_outcome in {"interested", "send_info", "question", "call_back", "warm", "appt_booked"}:
        points += 25
        signals.append(f"positive call: {last_outcome}")
    elif last_outcome in {"no_answer", "voicemail"}:
        points += 5
        signals.append("attempted call")
    elif last_outcome in {"soft_no", "not_now"}:
        points += 10
        signals.append("soft objection (still reachable)")
    if call_count > 1:
        points += min((call_count - 1) * 5, 15)
        signals.append(f"{call_count} calls total")

    # Contact quality (max 20 pts)
    has_phone = bool(lead.get("contact_phones") and str(lead.get("contact_phones")) not in {"[]", "", "null"})
    has_email = bool(lead.get("contact_emails") and str(lead.get("contact_emails")) not in {"[]", "", "null"})
    if has_phone:
        points += 10
    if has_email:
        points += 10

    # Recency bonus (max 10 pts)
    last_contacted = lead.get("last_contacted_at") or lead.get("last_called_date")
    if last_contacted:
        try:
            from datetime import datetime, timedelta
            contacted_dt = datetime.fromisoformat(str(last_contacted)[:19])
            days_since = (datetime.utcnow() - contacted_dt).days
            if days_since <= 7:
                points += 10
            elif days_since <= 30:
                points += 5
        except (ValueError, TypeError):
            pass

    # Classify
    if points >= 50:
        strength = "strong"
    elif points >= 25:
        strength = "medium"
    else:
        strength = "weak"

    return {"strength": strength, "score": min(points, 100), "signals": signals}


def _hydrate_lead(row: Any) -> Dict[str, Any]:
    lead = _derive_intelligence(_decode_row(row))
    lead["main_image"] = _to_url(lead.get("main_image", ""))
    lead["property_images"] = [_to_url(img) for img in lead.get("property_images", [])]
    lead.update(build_property_visuals(lead))
    lead["lead_state"] = _derive_lead_state(lead)
    lead["next_action"] = _derive_next_action(lead)
    lead["script_hints"] = _build_script_hints(lead)
    lead["connection_score"] = _compute_connection_score(lead)
    return lead


def _derive_lead_state(lead: Dict[str, Any]) -> str:
    status = _normalize_token(lead.get("status"))
    outcome = _normalize_token(lead.get("last_outcome"))
    queue_bucket = _normalize_token(lead.get("queue_bucket")) or _queue_bucket_for_lead(lead)
    next_action_type = _normalize_token(lead.get("next_action_type"))

    if status in {"converted", "dropped"} or outcome == "hard_no":
        return "closed"
    if status in {"appt_booked", "mortgage_appt_booked"} or queue_bucket == "booked":
        return "booked"
    if queue_bucket == "enrichment" or next_action_type == "enrichment" or outcome in {"wrong_number", "wrong_person", "not_me"}:
        return "needs_enrichment"
    if queue_bucket in {"callback_due", "nurture"} or next_action_type in {"sms", "email", "appointment"} or outcome in {"no_answer", "send_info", "question", "not_now", "call_back", "soft_no"}:
        return "follow_up_pending"
    if _lead_has_phone(lead):
        return "ready_to_call"
    if _lead_has_email(lead):
        return "ready_to_message"
    return "needs_enrichment"


def _derive_next_action(lead: Dict[str, Any]) -> Dict[str, Any]:
    next_action_at = lead.get("next_action_at")
    next_action_type = (lead.get("next_action_type") or "").strip()
    next_action_channel = (lead.get("next_action_channel") or "").strip()
    next_action_title = (lead.get("next_action_title") or "").strip()
    next_action_reason = (lead.get("next_action_reason") or "").strip()
    lead_state = _derive_lead_state(lead)
    outcome = _normalize_token(lead.get("last_outcome"))

    if next_action_type or next_action_channel or next_action_title:
        return {
            "at": next_action_at,
            "type": next_action_type or next_action_channel or "follow_up",
            "channel": next_action_channel or next_action_type or "follow_up",
            "title": next_action_title or "Queued follow-up",
            "reason": next_action_reason or "A follow-up task is already queued for this lead.",
        }

    if lead_state == "needs_enrichment":
        return {
            "at": next_action_at,
            "type": "enrichment",
            "channel": "enrichment",
            "title": "Enrich contact details",
            "reason": "Resolve missing or invalid owner contact details before the next outreach attempt.",
        }

    if lead_state == "follow_up_pending":
        if outcome == "no_answer" and _lead_has_phone(lead):
            return {
                "at": next_action_at,
                "type": "sms",
                "channel": "sms",
                "title": "Missed-call SMS",
                "reason": "Follow up quickly after the missed call with a short, low-friction SMS.",
            }
        if outcome in {"send_info", "question"}:
            channel = "email" if _lead_has_email(lead) else "sms" if _lead_has_phone(lead) else "enrichment"
            return {
                "at": next_action_at,
                "type": channel,
                "channel": channel,
                "title": "Answer / send requested info",
                "reason": "The owner asked for information before the next conversation.",
            }
        if outcome in {"not_now", "call_back"}:
            return {
                "at": lead.get("do_not_contact_until") or next_action_at,
                "type": "call",
                "channel": "call",
                "title": "Callback",
                "reason": "The owner asked for a later follow-up time.",
            }
        if outcome == "soft_no":
            channel = "email" if _lead_has_email(lead) else "sms" if _lead_has_phone(lead) else "enrichment"
            return {
                "at": lead.get("do_not_contact_until") or next_action_at,
                "type": channel,
                "channel": channel,
                "title": "Nurture follow-up",
                "reason": "Keep the lead warm without pushing for an immediate appointment.",
            }

    if lead_state == "ready_to_call":
        return {
            "at": next_action_at,
            "type": "call",
            "channel": "call",
            "title": "Call owner",
            "reason": "The lead is contact-ready and no newer follow-up task is queued.",
        }

    if _lead_has_email(lead):
        return {
            "at": next_action_at,
            "type": "email",
            "channel": "email",
            "title": "Send introduction email",
            "reason": "Email is the best available channel for the next touch.",
        }

    return {
        "at": next_action_at,
        "type": "review",
        "channel": "review",
        "title": "Review lead",
        "reason": "Review the lead record and choose the next manual step.",
    }


def _build_script_hints(lead: Dict[str, Any]) -> Dict[str, str]:
    next_action = _derive_next_action(lead)
    return {
        "opener": lead.get("what_to_say") or "",
        "if_no_answer": "Acknowledge the miss, reference the property signal, and send a short follow-up that asks for a better time.",
        "if_objection": "Lower the pressure, anchor on the documented signal, and offer a brief market update instead of a hard ask.",
        "cta": lead.get("recommended_next_step") or next_action.get("title") or "Move the lead to the next concrete step.",
    }


async def _append_activity_and_commit(
    session: AsyncSession,
    lead_id: str,
    activity_type: str,
    note: str,
    channel: str = "",
    subject: str = "",
) -> Dict[str, Any]:
    """
    Append an activity entry to a lead's activity_log, commit, and return the
    hydrated lead. Used by task and appointment creation routes.
    """
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")
        
    lead = _decode_row(row)
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry(activity_type, note, lead.get("status"), channel, subject),
    )
    await session.execute(
        text("UPDATE leads SET activity_log = :act, updated_at = :upd WHERE id = :id"),
        {"act": json.dumps(activity_log), "upd": now_iso(), "id": lead_id}
    )
    await session.commit()
    
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = res.mappings().first()
    return _hydrate_lead(row)


def _validate_next_status(current: str, target: str) -> None:
    if target not in LEAD_STATUS_TRANSITIONS:
        raise HTTPException(status_code=400, detail=f"Invalid target status: {target}")
    if target == current:
        return
    allowed = LEAD_STATUS_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise HTTPException(status_code=400, detail=f"Cannot transition from {current} to {target}. Allowed: {sorted(allowed)}")


def _build_outreach_pack(lead: Dict[str, Any], tone: str) -> Dict[str, Any]:
    owner = lead.get("owner_name") or "Owner"
    return {
        "tone": tone,
        "call_opener": lead.get("what_to_say") or f"Hi {owner}, I wanted to give you a quick update on your property and the strongest next steps available right now.",
        "why_now": lead.get("why_now"),
        "best_call_angle": lead.get("best_call_angle"),
        "recommended_next_step": lead.get("recommended_next_step"),
        "followups": [
            {"day": "D0", "message": f"Hi {owner}, thanks for your time. I can prepare a short property update with value range and next-step options."},
            {"day": "D2", "message": "Quick follow-up: if useful, I can still map pricing, buyer demand, and timing in a short call."},
            {"day": "D7", "message": "Last check-in for now. If timing improves later, I can keep the brief ready so you can move quickly."},
        ],
    }


def _get_default_zoom_account() -> Dict[str, Any]:
    return {
        "client_id": os.getenv("ZOOM_CLIENT_ID"),
        "client_secret": os.getenv("ZOOM_CLIENT_SECRET"),
        "account_id": os.getenv("ZOOM_ACCOUNT_ID"),
        "token_url": "https://zoom.us/oauth/token",
        "api_base": "https://api.zoom.us/v2",
        "use_account_credentials": True
    }


def _has_zoom_credentials(account: Optional[Dict[str, Any]]) -> bool:
    if not account:
        return False
    return bool((account.get("client_id") or "").strip() and (account.get("client_secret") or "").strip() and (account.get("account_id") or "").strip())


async def _resolve_zoom_account(session: AsyncSession, preferred_id: Optional[str] = None) -> Dict[str, Any]:
    clauses = []
    params: Dict[str, Any] = {}
    if preferred_id:
        clauses.append("id = :id")
        params["id"] = preferred_id
    clauses.append("provider = 'zoom'")
    
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else "WHERE provider = 'zoom'"
    
    res = await session.execute(
        text(f"SELECT * FROM communication_accounts {where_clause} ORDER BY updated_at DESC, created_at DESC"),
        params
    )
    rows = res.mappings().all()
    for row in rows:
        account = dict(row)
        if _has_zoom_credentials(account):
            return account

    fallback = _get_default_zoom_account()
    if _has_zoom_credentials(fallback):
        return fallback
    raise HTTPException(status_code=400, detail="No valid Zoom account is configured")

__all__ = ['_project_log_header', 'append_project_memory', 'ensure_project_memory_file', 'get_deterministic_id', '_append_stage_note', '_append_activity', '_build_activity_entry', '_recent_touch_count', '_lead_has_phone', '_lead_has_email', '_infer_contactability_status', '_normalize_token', '_infer_strike_zone', '_infer_lead_archetype', '_default_preferred_channel', '_owner_first_name', '_value_hook', '_message_bundle', '_next_business_slot', '_task_sort_key', '_task_id', '_task_to_dict', '_appointment_to_dict', '_append_note_text', '_phase_label', '_operator_task_payload', '_normalize_address_key', '_find_matching_leads_for_sold_event', '_sold_event_to_dict', '_is_cache_fresh', '_compute_cache_expiry', 'get_call_angle', '_queue_bucket_for_lead', '_derive_intelligence', '_to_url', '_hydrate_lead', '_append_activity_and_commit', '_validate_next_status', '_build_outreach_pack', '_get_default_zoom_account', '_has_zoom_credentials', '_resolve_zoom_account']
