from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from hermes.models import (
    HermesCaseMemory,
    HermesContactCluster,
    HermesContactPlan,
    HermesDecisionLog,
)
from models.sales_core_models import BusinessContext, LeadContact, LeadState
from models.sql_models import Lead, Task
from services.sales_core.dialing_service import get_lead_context, sync_lead_state


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


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


def _display_phone(value: Any) -> Optional[str]:
    normalized = _normalize_phone(value)
    if not normalized:
        return None
    digits = "".join(ch for ch in normalized if ch.isdigit())
    if normalized.startswith("+61") and len(digits) == 11:
        local = f"0{digits[2:]}"
        if len(local) == 10:
            return f"{local[:4]} {local[4:7]} {local[7:]}"
    return str(value or normalized).strip() or None


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _dedupe(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for item in values:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _safe_json_loads(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    return []


def _latest_note_text(lead: Dict[str, Any]) -> str:
    stage_note = str(lead.get("stage_note") or "").strip()
    if stage_note:
        return stage_note
    history = _safe_list(lead.get("stage_note_history"))
    for entry in reversed(history):
        if isinstance(entry, dict):
            note = str(entry.get("note") or entry.get("message") or "").strip()
            if note:
                return note
    return str(lead.get("notes") or "").strip()


def _next_weekday(base: datetime, weekday: int, *, include_current: bool = False) -> datetime:
    days_ahead = weekday - base.weekday()
    if days_ahead < 0 or (days_ahead == 0 and not include_current):
        days_ahead += 7
    return base + timedelta(days=days_ahead)


def _time_for_period(text: str) -> tuple[int, int]:
    lowered = text.lower()
    if "morning" in lowered:
        return 10, 0
    if "afternoon" in lowered:
        return 15, 0
    if "evening" in lowered:
        return 18, 0
    return 11, 0


def _extract_callback_due_at(note_text: str) -> Optional[str]:
    lowered = note_text.lower()
    if not any(token in lowered for token in ("call me", "call back", "callback", "follow up")):
        return None
    base = _utcnow()
    hour, minute = _time_for_period(lowered)
    weekday_map = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    for name, weekday in weekday_map.items():
        if f"next {name}" in lowered:
            target = _next_weekday(base, weekday) + timedelta(days=7 if _next_weekday(base, weekday).date() <= base.date() else 0)
            return target.replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()
        if name in lowered:
            target = _next_weekday(base, weekday, include_current=False)
            return target.replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()
    if "tomorrow" in lowered:
        return (base + timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()
    if "next week" in lowered:
        return (base + timedelta(days=7)).replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()
    return None


def _extract_note_signals(lead: Dict[str, Any]) -> Dict[str, Any]:
    note_text = _latest_note_text(lead)
    lowered = note_text.lower()
    promised_actions: List[Dict[str, Any]] = []
    objection_tags: List[str] = []
    preferred_channel: Optional[str] = None
    callback_hint: Optional[str] = None

    if any(token in lowered for token in ("call me", "call back", "callback", "follow up")):
        callback_hint = note_text
        promised_actions.append({"type": "callback_requested", "source": "note", "detail": note_text})
    callback_due_at = _extract_callback_due_at(note_text)
    if any(token in lowered for token in ("email", "mail it", "send me an email")):
        preferred_channel = "email"
    elif any(token in lowered for token in ("sms", "text me", "send a text")):
        preferred_channel = "sms"
    elif any(token in lowered for token in ("call me", "call back", "ring me")):
        preferred_channel = "call"

    for trigger, tag in (
        ("price", "price"),
        ("too expensive", "price"),
        ("not interested", "not_interested"),
        ("probate", "probate_sensitive"),
        ("withdrawn", "withdrawn_listing"),
    ):
        if trigger in lowered:
            objection_tags.append(tag)

    for trigger, action_type in (
        ("send figures", "send_figures"),
        ("send info", "send_info"),
        ("send details", "send_info"),
        ("send cma", "send_cma"),
        ("send appraisal", "send_appraisal"),
        ("send report", "send_report"),
    ):
        if trigger in lowered:
            promised_actions.append({"type": action_type, "source": "note", "detail": note_text})

    return {
        "latest_note": note_text,
        "preferred_channel": preferred_channel,
        "callback_hint": callback_hint,
        "callback_due_at": callback_due_at,
        "objection_tags": _dedupe(objection_tags),
        "promised_actions": promised_actions,
    }


def _draft_task_copy(action_type: str, lead: Dict[str, Any], note_text: str) -> Dict[str, str]:
    address = str(lead.get("address") or "your property").strip()
    owner = str(lead.get("owner_name") or "").strip()
    first = owner.split()[0] if owner else ""
    intro = f"Hi {first}," if first else "Hi,"
    drafts = {
        "send_figures": {
            "subject": f"Property figures for {address}",
            "body": f"{intro}\n\nAs requested, I’m sending through the key figures for {address}. Let me know if you want a quick walkthrough before we speak.\n\nRegards,",
        },
        "send_info": {
            "subject": f"Requested information for {address}",
            "body": f"{intro}\n\nSending through the information you asked for on {address}. If it helps, I can also summarise the key points on a quick call.\n\nRegards,",
        },
        "send_cma": {
            "subject": f"CMA for {address}",
            "body": f"{intro}\n\nAs requested, I’m sending the CMA for {address}. Have a look when convenient and I can walk you through the numbers after.\n\nRegards,",
        },
        "send_appraisal": {
            "subject": f"Appraisal information for {address}",
            "body": f"{intro}\n\nHere’s the appraisal information for {address}. If you want, I can also outline the likely next steps on a quick call.\n\nRegards,",
        },
        "send_report": {
            "subject": f"Requested report for {address}",
            "body": f"{intro}\n\nSending through the report for {address}. Happy to talk through the key takeaways once you’ve had a look.\n\nRegards,",
        },
    }
    return drafts.get(action_type, {"subject": address, "body": note_text})


async def _queue_note_admin_tasks(
    session: AsyncSession,
    *,
    lead: Dict[str, Any],
    plan: HermesContactPlan,
    note_signals: Dict[str, Any],
) -> List[str]:
    latest_note = str(note_signals.get("latest_note") or "").strip()
    if not latest_note:
        return []
    created_ids: List[str] = []
    preferred_channel = str(note_signals.get("preferred_channel") or "").strip().lower()
    has_email = bool(_safe_list(lead.get("contact_emails")))
    default_channel = "email" if preferred_channel == "email" or has_email else "call"
    due_at = now_iso()

    for promised in note_signals.get("promised_actions") or []:
        action_type = str(promised.get("type") or "").strip()
        if action_type not in {"send_figures", "send_info", "send_cma", "send_appraisal", "send_report"}:
            continue
        channel = "email" if default_channel == "email" else "call"
        task_type = "email" if channel == "email" else "follow_up_call"
        title = {
            "send_figures": "Send figures",
            "send_info": "Send requested info",
            "send_cma": "Send CMA",
            "send_appraisal": "Send appraisal info",
            "send_report": "Send report",
        }[action_type]
        task_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"hermes-note-task:{lead.get('id')}:{action_type}:{channel}"))
        existing = await session.get(Task, task_id)
        if existing and str(existing.status or "").lower() in {"pending", "completed"}:
            continue
        task = existing or Task(id=task_id, lead_id=str(lead.get("id") or ""))
        task.title = title
        task.task_type = task_type
        task.action_type = action_type
        task.channel = channel
        task.due_at = due_at
        task.status = "pending"
        task.notes = latest_note
        task.approval_status = "pending" if channel == "email" else "not_required"
        draft = _draft_task_copy(action_type, lead, latest_note)
        task.message_subject = draft["subject"] if channel == "email" else ""
        task.message_preview = draft["body"] if channel == "email" else latest_note
        task.rewrite_reason = "Hermes derived an admin follow-up task from the latest lead note."
        task.superseded_by = ""
        task.cadence_name = "hermes_note_admin"
        task.cadence_step = 0
        task.auto_generated = 1
        task.priority_bucket = "send_now" if channel == "email" else "follow_up"
        task.payload_json = {
            "source": "hermes_note_parser",
            "action_type": action_type,
            "lead_contact_id": plan.lead_contact_id,
            "note": latest_note,
        }
        task.created_at = task.created_at or now_iso()
        task.updated_at = now_iso()
        session.add(task)
        created_ids.append(task_id)

    if created_ids:
        await session.commit()
    return created_ids


def _infer_business_context(lead: Dict[str, Any]) -> str:
    joined = " ".join(
        str(lead.get(field) or "")
        for field in ("trigger_type", "lead_archetype", "route_queue", "queue_bucket", "status")
    ).lower()
    if any(token in joined for token in ("mortgage", "refinance", "lender", "loan")):
        return "mortgage"
    return "real_estate"


def _contact_id(lead_id: str, business_context_key: str, surface_type: str, surface_value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{business_context_key}:{lead_id}:{surface_type}:{surface_value}"))


def _cluster_key(business_context_key: str, phone: str = "", email: str = "") -> str:
    if phone:
        return f"{business_context_key}:phone:{phone}"
    if email:
        return f"{business_context_key}:email:{email}"
    return f"{business_context_key}:unknown"


async def ensure_business_contexts(session: AsyncSession) -> None:
    existing = {
        item.key
        for item in (
            await session.execute(select(BusinessContext).where(BusinessContext.key.in_(["real_estate", "mortgage", "app_saas"])))
        ).scalars().all()
    }
    created = False
    for key, label in (
        ("real_estate", "Real Estate"),
        ("mortgage", "Mortgage"),
        ("app_saas", "App / SaaS"),
    ):
        if key in existing:
            continue
        session.add(BusinessContext(key=key, label=label))
        created = True
    if created:
        await session.commit()


async def sync_lead_contacts(
    session: AsyncSession,
    *,
    lead_ids: Optional[List[str]] = None,
    limit: int = 500,
) -> Dict[str, Any]:
    await ensure_business_contexts(session)
    query = select(Lead)
    if lead_ids:
        query = query.where(Lead.id.in_(lead_ids))
    else:
        query = query.order_by(Lead.updated_at.desc()).limit(max(1, min(limit, 5000)))
    leads = (await session.execute(query)).scalars().all()

    synced = 0
    created = 0
    touched_contexts: set[str] = set()
    for lead in leads:
        lead_dict = lead.model_dump()
        business_context_key = _infer_business_context(lead_dict)
        touched_contexts.add(business_context_key)
        phones = _dedupe(
            _normalize_phone(value)
            for value in [*_safe_list(getattr(lead, "contact_phones", [])), *_safe_list(getattr(lead, "alternate_phones", []))]
        )
        emails = _dedupe(_normalize_email(value) for value in _safe_list(getattr(lead, "contact_emails", [])))
        if not phones and not emails:
            continue

        surfaces: List[Dict[str, str]] = []
        for phone in phones:
            surfaces.append({"surface_type": "phone", "phone": phone, "email": emails[0] if emails else ""})
        if not surfaces:
            for email in emails:
                surfaces.append({"surface_type": "email", "phone": "", "email": email})

        for surface in surfaces:
            contact_id = _contact_id(
                str(lead.id),
                business_context_key,
                surface["surface_type"],
                surface["phone"] or surface["email"],
            )
            contact = await session.get(LeadContact, contact_id)
            metadata_json = {
                "address": getattr(lead, "address", "") or "",
                "suburb": getattr(lead, "suburb", "") or "",
                "postcode": getattr(lead, "postcode", "") or "",
                "owner_name": getattr(lead, "owner_name", "") or "",
                "signal_status": getattr(lead, "signal_status", "") or "",
                "trigger_type": getattr(lead, "trigger_type", "") or "",
                "lead_status": getattr(lead, "status", "") or "",
            }
            if contact is None:
                contact = LeadContact(
                    id=contact_id,
                    business_context_key=business_context_key,
                    lead_id=str(lead.id),
                    full_name=str(getattr(lead, "owner_name", "") or ""),
                    primary_phone=surface["phone"] or None,
                    primary_email=surface["email"] or None,
                    contact_role=str(getattr(lead, "contact_role", "") or "") or None,
                    source="lead_sync",
                    metadata_json=metadata_json,
                )
                created += 1
            else:
                contact.business_context_key = business_context_key
                contact.lead_id = str(lead.id)
                contact.full_name = str(getattr(lead, "owner_name", "") or "")
                contact.primary_phone = surface["phone"] or None
                contact.primary_email = surface["email"] or None
                contact.contact_role = str(getattr(lead, "contact_role", "") or "") or None
                contact.metadata_json = metadata_json
                contact.updated_at = _utcnow()
            session.add(contact)
            synced += 1
    await session.commit()

    for business_context_key in touched_contexts:
        await rebuild_contact_clusters(session, business_context_key=business_context_key)

    return {"synced_contacts": synced, "created_contacts": created, "business_contexts": sorted(touched_contexts)}


async def rebuild_contact_clusters(
    session: AsyncSession,
    *,
    business_context_key: Optional[str] = None,
) -> Dict[str, Any]:
    query = select(LeadContact)
    if business_context_key:
        query = query.where(LeadContact.business_context_key == business_context_key)
    contacts = (await session.execute(query)).scalars().all()
    lead_ids = {str(item.lead_id or "") for item in contacts if item.lead_id}
    leads = (
        await session.execute(select(Lead).where(Lead.id.in_(lead_ids)))
    ).scalars().all() if lead_ids else []
    lead_map = {str(item.id): item for item in leads}

    grouped: Dict[str, List[LeadContact]] = {}
    for contact in contacts:
        phone = _normalize_phone(contact.primary_phone)
        email = _normalize_email(contact.primary_email)
        if not phone and not email:
            continue
        key = _cluster_key(contact.business_context_key, phone=phone, email=email)
        grouped.setdefault(key, []).append(contact)

    updated = 0
    for cluster_key, members in grouped.items():
        ranked = sorted(
            members,
            key=lambda item: (
                float(getattr(lead_map.get(str(item.lead_id or "")), "heat_score", 0) or 0),
                float(getattr(lead_map.get(str(item.lead_id or "")), "evidence_score", 0) or 0),
                float(getattr(lead_map.get(str(item.lead_id or "")), "est_value", 0) or 0),
                str(item.updated_at),
            ),
            reverse=True,
        )
        primary = ranked[0] if ranked else None
        cluster = await session.get(HermesContactCluster, cluster_key)
        if cluster is None:
            cluster = HermesContactCluster(
                cluster_key=cluster_key,
                business_context_key=members[0].business_context_key,
                surface_type="phone" if _normalize_phone(members[0].primary_phone) else "email",
                surface_value=_normalize_phone(members[0].primary_phone) or _normalize_email(members[0].primary_email),
                created_at=now_iso(),
            )
        cluster.primary_lead_contact_id = primary.id if primary else None
        cluster.lead_contact_ids_json = [item.id for item in members]
        cluster.lead_ids_json = _dedupe(str(item.lead_id or "") for item in members if item.lead_id)
        cluster.duplicate_count = len(members)
        cluster.status = "review_required" if len(members) > 1 else "active"
        cluster.updated_at = now_iso()
        session.add(cluster)
        updated += 1
    await session.commit()
    return {"clusters_updated": updated}


def _risk_flags(lead: Dict[str, Any], cluster: Optional[HermesContactCluster], state: Optional[LeadState]) -> List[str]:
    flags: List[str] = []
    if cluster and int(cluster.duplicate_count or 0) > 1:
        flags.append("duplicate_contact_surface")
    if str(lead.get("do_not_contact_until") or "").strip():
        flags.append("cooldown_active")
    if state and getattr(state, "needs_enrichment", False):
        flags.append("needs_enrichment")
    if state and getattr(state, "stale_enrichment", False):
        flags.append("stale_enrichment")
    if str(lead.get("status") or "").strip().lower() in {"appt_booked", "mortgage_appt_booked", "converted", "dropped"}:
        flags.append("terminal_or_booked")
    return flags


def _build_case_summary(
    lead: Dict[str, Any],
    contact: Optional[LeadContact],
    state: Optional[LeadState],
    cluster: Optional[HermesContactCluster],
) -> Dict[str, Any]:
    note_signals = _extract_note_signals(lead)
    signals = _safe_list(lead.get("seller_intent_signals")) + _safe_list(lead.get("refinance_signals"))
    next_due = getattr(state, "next_action_due_at", None)
    return {
        "address": str(lead.get("address") or ""),
        "suburb": str(lead.get("suburb") or ""),
        "owner_name": str(lead.get("owner_name") or ""),
        "business_context_key": str(contact.business_context_key if contact else _infer_business_context(lead)),
        "last_outcome": str(lead.get("last_outcome") or ""),
        "why_now": str(lead.get("why_now") or ""),
        "recommended_next_step": str(lead.get("recommended_next_step") or ""),
        "signal_count": len(signals),
        "duplicate_cluster": int(cluster.duplicate_count or 0) if cluster else 0,
        "callable_now": bool(getattr(state, "callable_now", False)),
        "next_action_due_at": next_due.isoformat() if next_due else None,
        "do_not_contact_until": str(lead.get("do_not_contact_until") or ""),
        "contact_surface": {
            "phone": _normalize_phone(getattr(contact, "primary_phone", "")) if contact else "",
            "email": _normalize_email(getattr(contact, "primary_email", "")) if contact else "",
        },
        "latest_note": note_signals["latest_note"],
        "note_preferred_channel": note_signals["preferred_channel"],
        "callback_hint": note_signals["callback_hint"],
        "callback_due_at": note_signals["callback_due_at"],
    }


def _plan_lane(
    lead: Dict[str, Any],
    state: Optional[LeadState],
    cluster: Optional[HermesContactCluster],
    lead_contact_id: str,
) -> Dict[str, Any]:
    blocked_reasons: List[str] = []
    now_value = now_iso()
    now_dt = _utcnow()
    do_not_contact_until = _coerce_datetime(lead.get("do_not_contact_until"))
    note_signals = _extract_note_signals(lead)
    note_callback_due = _coerce_datetime(note_signals.get("callback_due_at"))

    if cluster and int(cluster.duplicate_count or 0) > 1 and cluster.primary_lead_contact_id != lead_contact_id:
        blocked_reasons.append("Another lead owns the primary path for this contact surface.")
        return {
            "lane": "review_required",
            "priority_score": 15.0,
            "next_action": {"type": "review", "title": "Resolve duplicate contact surface", "due_at": now_value, "channel": "review"},
            "blocked_reasons": blocked_reasons,
            "review_required": True,
            "rationale": "This phone or email already appears on another lead. Hermes holds secondary records to prevent duplicate outreach.",
            "confidence_score": 0.95,
        }

    if state is None:
        blocked_reasons.append("No contact state exists yet.")
        return {
            "lane": "review_required",
            "priority_score": 5.0,
            "next_action": {"type": "review", "title": "Review contact state", "due_at": now_value, "channel": "review"},
            "blocked_reasons": blocked_reasons,
            "review_required": True,
            "rationale": "Hermes cannot safely queue work until this contact has a deterministic state snapshot.",
            "confidence_score": 0.7,
        }

    next_due = getattr(state, "next_action_due_at", None)
    if next_due is None and note_callback_due is not None and note_callback_due > now_dt:
        next_due = note_callback_due
    next_due_iso = next_due.isoformat() if next_due else None
    next_action = getattr(state, "next_action", "") or "review"
    callable_now = bool(getattr(state, "callable_now", False))
    if do_not_contact_until:
        blocked_reasons.append(f"Do-not-contact-until is set to {do_not_contact_until.isoformat()}.")
    if do_not_contact_until and do_not_contact_until > now_dt:
        return {
            "lane": "hold",
            "priority_score": 0.0,
            "next_action": {"type": "hold", "title": "Respect contact cooldown", "due_at": do_not_contact_until.isoformat(), "channel": "review"},
            "blocked_reasons": blocked_reasons,
            "review_required": False,
            "rationale": "Hermes is holding this contact until the requested or required do-not-contact window expires.",
            "confidence_score": 0.99,
        }
    if next_action == "skip":
        blocked_reasons.append("Contact is marked do not call or wrong number.")
        return {
            "lane": "hold",
            "priority_score": 0.0,
            "next_action": {"type": "hold", "title": "Do not contact", "due_at": next_due_iso, "channel": "review"},
            "blocked_reasons": blocked_reasons,
            "review_required": False,
            "rationale": "Hermes will not queue outreach for contacts that are unsafe or invalid.",
            "confidence_score": 0.98,
        }
    if callable_now and next_action == "call":
        if note_callback_due is not None and note_callback_due > now_dt:
            return {
                "lane": "follow_up_later",
                "priority_score": float(getattr(state, "queue_score", 0) or 0) + 6.0,
                "next_action": {"type": "follow_up_call", "title": "Follow up at requested time", "due_at": note_callback_due.isoformat(), "channel": "call"},
                "blocked_reasons": blocked_reasons,
                "review_required": False,
                "rationale": "The latest note includes a callback request with a time window, so Hermes keeps this lead out of the queue until then.",
                "confidence_score": 0.9,
            }
        return {
            "lane": "call_now",
            "priority_score": float(getattr(state, "queue_score", 0) or 0) + 12.0,
            "next_action": {"type": "call", "title": "Call now", "due_at": now_value, "channel": "call"},
            "blocked_reasons": blocked_reasons,
            "review_required": False,
            "rationale": "This contact is callable now and not blocked by cooldown, suppression, or terminal status.",
            "confidence_score": 0.96,
        }
    if next_action == "follow_up_due":
        return {
            "lane": "follow_up_later" if next_due_iso else "review_required",
            "priority_score": float(getattr(state, "queue_score", 0) or 0) + 6.0,
            "next_action": {"type": "follow_up_call", "title": "Follow up at due time", "due_at": next_due_iso, "channel": "call"},
            "blocked_reasons": blocked_reasons,
            "review_required": False,
            "rationale": "A callback or cooling window is already set. Hermes keeps the contact out of the queue until the due time arrives.",
            "confidence_score": 0.94,
        }
    if next_action == "enrich_first":
        return {
            "lane": "enrich_first",
            "priority_score": float(getattr(state, "queue_score", 0) or 0) + 8.0,
            "next_action": {"type": "enrich", "title": "Enrich before outreach", "due_at": now_value, "channel": "enrichment"},
            "blocked_reasons": blocked_reasons,
            "review_required": False,
            "rationale": "This record looks commercially valuable but is missing enough contactability or freshness to justify immediate outreach.",
            "confidence_score": 0.9,
        }
    if next_action == "cooldown":
        return {
            "lane": "hold",
            "priority_score": 10.0,
            "next_action": {"type": "hold", "title": "Cooldown active", "due_at": next_due_iso, "channel": "review"},
            "blocked_reasons": blocked_reasons or ["Cooldown is active."],
            "review_required": False,
            "rationale": "Hermes is intentionally spacing follow-up to avoid calling too early or too often.",
            "confidence_score": 0.93,
        }
    return {
        "lane": "review_required",
        "priority_score": 20.0,
        "next_action": {"type": "review", "title": "Review next step", "due_at": next_due_iso or now_value, "channel": "review"},
        "blocked_reasons": blocked_reasons,
        "review_required": True,
        "rationale": "This contact does not fit a safe automatic path yet, so Hermes keeps it visible for operator review.",
        "confidence_score": 0.75,
    }


async def replan_contact(session: AsyncSession, lead_contact_id: str, *, actor: str = "hermes") -> Dict[str, Any]:
    state = await sync_lead_state(session, lead_contact_id, now=_utcnow())
    context = await get_lead_context(session, lead_contact_id)
    contact = context["contact"]
    lead_model = context["lead"]
    lead = lead_model.model_dump() if lead_model is not None else {}

    cluster = None
    cluster_key = _cluster_key(
        contact.business_context_key,
        phone=_normalize_phone(contact.primary_phone),
        email=_normalize_email(contact.primary_email),
    )
    if ":unknown" not in cluster_key:
        cluster = await session.get(HermesContactCluster, cluster_key)

    planned = _plan_lane(lead, state, cluster, lead_contact_id)
    plan = await session.get(HermesContactPlan, lead_contact_id)
    if plan is None:
        plan = HermesContactPlan(lead_contact_id=lead_contact_id, business_context_key=contact.business_context_key)
    plan.business_context_key = contact.business_context_key
    plan.lead_id = str(contact.lead_id or "") or None
    plan.cluster_key = cluster_key if cluster else None
    plan.lane = planned["lane"]
    plan.priority_score = float(planned["priority_score"] or 0)
    plan.next_action_json = planned["next_action"]
    plan.blocked_reasons_json = list(planned["blocked_reasons"])
    plan.review_required = bool(planned["review_required"])
    plan.rationale = str(planned["rationale"] or "")
    plan.confidence_score = float(planned["confidence_score"] or 0.5)
    plan.updated_at = now_iso()
    session.add(plan)

    if contact.lead_id:
        note_signals = _extract_note_signals(lead)
        memory = await session.get(HermesCaseMemory, str(contact.lead_id))
        if memory is None:
            memory = HermesCaseMemory(lead_id=str(contact.lead_id), business_context_key=contact.business_context_key)
        memory.business_context_key = contact.business_context_key
        memory.primary_lead_contact_id = contact.id
        memory.primary_cluster_key = cluster_key if cluster else None
        memory.summary_json = _build_case_summary(lead, contact, state, cluster)
        memory.signal_summary_json = _safe_list(lead.get("seller_intent_signals")) + _safe_list(lead.get("refinance_signals"))
        memory.objection_tags_json = _dedupe(
            value
            for value in [
                str(lead.get("objection_reason") or "").strip(),
                str(lead.get("last_outcome") or "").strip(),
                *note_signals["objection_tags"],
            ]
            if value
        )
        memory.promised_actions_json = [planned["next_action"], *note_signals["promised_actions"]]
        memory.risk_flags_json = _risk_flags(lead, cluster, state)
        memory.confidence_score = float(planned["confidence_score"] or 0.5)
        memory.updated_at = now_iso()
        session.add(memory)

    decision = HermesDecisionLog(
        business_context_key=contact.business_context_key,
        lead_id=str(contact.lead_id or "") or None,
        lead_contact_id=contact.id,
        cluster_key=cluster_key if cluster else None,
        decision_type="replan",
        summary=plan.rationale,
        payload_json={
            "lane": plan.lane,
            "next_action": plan.next_action_json,
            "blocked_reasons": plan.blocked_reasons_json,
            "review_required": plan.review_required,
            "actor": actor,
        },
        confidence_score=plan.confidence_score,
    )
    session.add(decision)
    await session.commit()
    if contact.lead_id:
        await _queue_note_admin_tasks(session, lead=lead, plan=plan, note_signals=note_signals)

    return {
        "contact": {
            "id": contact.id,
            "lead_id": str(contact.lead_id or "") or None,
            "business_context_key": contact.business_context_key,
            "full_name": contact.full_name,
            "primary_phone": contact.primary_phone,
            "primary_email": contact.primary_email,
        },
        "plan": {
            "lane": plan.lane,
            "priority_score": plan.priority_score,
            "next_action": plan.next_action_json,
            "blocked_reasons": plan.blocked_reasons_json,
            "review_required": plan.review_required,
            "rationale": plan.rationale,
            "confidence_score": plan.confidence_score,
        },
        "state": state.summary_json if state else {},
    }


async def replan_lead(session: AsyncSession, lead_id: str, *, actor: str = "hermes") -> Dict[str, Any]:
    contacts = (
        await session.execute(select(LeadContact).where(LeadContact.lead_id == lead_id).order_by(LeadContact.updated_at.desc()))
    ).scalars().all()
    if not contacts:
        sync_result = await sync_lead_contacts(session, lead_ids=[lead_id], limit=10)
        contacts = (
            await session.execute(select(LeadContact).where(LeadContact.lead_id == lead_id).order_by(LeadContact.updated_at.desc()))
        ).scalars().all()
        if not contacts:
            lead = await session.get(Lead, lead_id)
            if lead is None:
                raise ValueError("Lead not found")
            lead_dict = lead.model_dump()
            business_context_key = _infer_business_context(lead_dict)
            memory = HermesCaseMemory(
                lead_id=lead_id,
                business_context_key=business_context_key,
                summary_json=_build_case_summary(lead_dict, None, None, None),
                signal_summary_json=_safe_list(lead_dict.get("seller_intent_signals")) + _safe_list(lead_dict.get("refinance_signals")),
                risk_flags_json=["missing_contactability"],
                confidence_score=0.8,
                updated_at=now_iso(),
            )
            session.add(memory)
            session.add(
                HermesDecisionLog(
                    business_context_key=business_context_key,
                    lead_id=lead_id,
                    decision_type="replan",
                    summary="Lead has no callable contact surface yet. Hermes marked it for enrichment or review.",
                    payload_json={"risk_flags": ["missing_contactability"], "actor": actor, "sync_result": sync_result},
                    confidence_score=0.9,
                )
            )
            await session.commit()
            return {"lead_id": lead_id, "contacts": [], "sync_result": sync_result}

    replanned = []
    for contact in contacts:
        replanned.append(await replan_contact(session, contact.id, actor=actor))
    return {"lead_id": lead_id, "contacts": replanned}


async def refresh_hermes_portfolio(
    session: AsyncSession,
    *,
    lead_ids: Optional[List[str]] = None,
    limit: int = 250,
    actor: str = "hermes_refresh",
) -> Dict[str, Any]:
    sync_result = await sync_lead_contacts(session, lead_ids=lead_ids, limit=limit)
    contact_query = select(LeadContact).order_by(LeadContact.updated_at.desc())
    if lead_ids:
        contact_query = contact_query.where(LeadContact.lead_id.in_(lead_ids))
    else:
        contact_query = contact_query.limit(max(1, min(limit * 3, 5000)))
    contacts = (await session.execute(contact_query)).scalars().all()
    replanned = 0
    for contact in contacts:
        await replan_contact(session, contact.id, actor=actor)
        replanned += 1
    return {"sync": sync_result, "replanned_contacts": replanned}


async def refresh_hermes_for_lead(
    session: AsyncSession,
    lead_id: str,
    *,
    actor: str = "lead_event",
    sync_contacts_first: bool = True,
) -> Dict[str, Any]:
    lead = await session.get(Lead, lead_id)
    if lead is None:
        raise ValueError("Lead not found")
    sync_result: Dict[str, Any] | None = None
    if sync_contacts_first:
        sync_result = await sync_lead_contacts(session, lead_ids=[lead_id], limit=10)
    replanned = await replan_lead(session, lead_id, actor=actor)
    return {"lead_id": lead_id, "sync": sync_result, "replanned": replanned}


async def refresh_hermes_for_leads(
    session: AsyncSession,
    lead_ids: List[str],
    *,
    actor: str = "lead_event_batch",
    sync_contacts_first: bool = True,
) -> Dict[str, Any]:
    cleaned = _dedupe(str(item or "").strip() for item in lead_ids if str(item or "").strip())
    if not cleaned:
        return {"lead_ids": [], "updated": 0}
    sync_result: Dict[str, Any] | None = None
    if sync_contacts_first:
        sync_result = await sync_lead_contacts(session, lead_ids=cleaned, limit=max(10, len(cleaned)))
    updated = 0
    for lead_id in cleaned:
        try:
            await replan_lead(session, lead_id, actor=actor)
            updated += 1
        except ValueError:
            continue
    return {"lead_ids": cleaned, "updated": updated, "sync": sync_result}


async def get_hermes_lead(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    lead = await session.get(Lead, lead_id)
    if lead is None:
        raise ValueError("Lead not found")
    await replan_lead(session, lead_id, actor="lead_view")

    memory = await session.get(HermesCaseMemory, lead_id)
    contacts = (
        await session.execute(select(LeadContact).where(LeadContact.lead_id == lead_id).order_by(LeadContact.updated_at.desc()))
    ).scalars().all()
    plans = (
        await session.execute(select(HermesContactPlan).where(HermesContactPlan.lead_id == lead_id).order_by(HermesContactPlan.priority_score.desc()))
    ).scalars().all()
    contact_ids = [item.id for item in contacts]
    states = (
        await session.execute(select(LeadState).where(LeadState.lead_contact_id.in_(contact_ids)))
    ).scalars().all() if contact_ids else []
    decisions = (
        await session.execute(
            select(HermesDecisionLog).where(HermesDecisionLog.lead_id == lead_id).order_by(HermesDecisionLog.created_at.desc()).limit(20)
        )
    ).scalars().all()

    state_map = {item.lead_contact_id: item for item in states}
    top_recommendation: Dict[str, Any] | None = None
    if plans:
        top_plan = plans[0]
        top_contact = next((item for item in contacts if item.id == top_plan.lead_contact_id), None)
        top_state = state_map.get(top_plan.lead_contact_id)
        business_key = str(top_plan.business_context_key or _infer_business_context(lead.model_dump()))
        cotality_status = _cotality_status_for_lead(lead)
        recommended_channel = _recommended_channel_for_plan(top_plan, lead, top_contact, business_key)
        recommended_angle = _recommended_angle_for_plan(top_plan, lead, business_key)
        top_recommendation = {
            "lead_contact_id": top_plan.lead_contact_id,
            "lead_id": lead_id,
            "business_context_key": business_key,
            "lane": top_plan.lane,
            "priority_score": top_plan.priority_score,
            "review_required": top_plan.review_required,
            "rationale": top_plan.rationale,
            "next_action": top_plan.next_action_json,
            "blocked_reasons": top_plan.blocked_reasons_json,
            "priority_reason": _priority_reason_for_plan(top_plan, lead, top_state),
            "recommended_angle": recommended_angle,
            "recommended_channel": recommended_channel,
            "recommended_contact": _recommended_contact_payload(top_contact),
            "cotality_recommended": _cotality_recommended_for_lead(lead, business_key, cotality_status),
            "cotality_status": cotality_status,
            "next_best_action": _next_best_action_for_plan(top_plan, lead, recommended_channel, recommended_angle),
        }
    return {
        "lead_id": lead_id,
        "memory": memory.model_dump() if memory else None,
        "contacts": [
            {
                "contact": item.model_dump(),
                "state": state_map.get(item.id).model_dump() if state_map.get(item.id) else None,
            }
            for item in contacts
        ],
        "plans": [item.model_dump() for item in plans],
        "top_recommendation": top_recommendation,
        "decision_log": [item.model_dump() for item in decisions],
    }


async def get_hermes_contact(session: AsyncSession, lead_contact_id: str) -> Dict[str, Any]:
    await replan_contact(session, lead_contact_id, actor="contact_view")
    context = await get_lead_context(session, lead_contact_id)
    plan = await session.get(HermesContactPlan, lead_contact_id)
    contact = context["contact"]
    cluster = None
    cluster_key = _cluster_key(
        contact.business_context_key,
        phone=_normalize_phone(contact.primary_phone),
        email=_normalize_email(contact.primary_email),
    )
    if ":unknown" not in cluster_key:
        cluster = await session.get(HermesContactCluster, cluster_key)
    decisions = (
        await session.execute(
            select(HermesDecisionLog)
            .where(HermesDecisionLog.lead_contact_id == lead_contact_id)
            .order_by(HermesDecisionLog.created_at.desc())
            .limit(20)
        )
    ).scalars().all()
    return {
        "contact": contact.model_dump(),
        "lead": context["lead"].model_dump() if context["lead"] else None,
        "state": context["state"].model_dump() if context["state"] else None,
        "tasks": [item.model_dump() for item in context["tasks"]],
        "attempts": [item.model_dump() for item in context["attempts"]],
        "plan": plan.model_dump() if plan else None,
        "cluster": cluster.model_dump() if cluster else None,
        "decision_log": [item.model_dump() for item in decisions],
    }


def _lane_bucket(lane: str) -> int:
    order = {
        "call_now": 0,
        "follow_up_later": 1,
        "enrich_first": 2,
        "review_required": 3,
        "hold": 4,
    }
    return order.get(str(lane or ""), 9)


def _priority_reason_for_plan(plan: HermesContactPlan, lead: Optional[Lead], state: Optional[LeadState]) -> str:
    reasons: List[str] = []
    lane = str(plan.lane or "").strip()
    if lane == "call_now":
        reasons.append("Call now while the lead is still contactable.")
    elif lane == "follow_up_later":
        reasons.append("Follow-up timing is driving this lead.")
    elif lane == "enrich_first":
        reasons.append("Enrichment should sharpen the next move.")
    elif lane == "review_required":
        reasons.append("A manual review is needed before action.")

    if lead is not None:
        call_score = int(getattr(lead, "call_today_score", 0) or 0)
        heat_score = int(getattr(lead, "heat_score", 0) or 0)
        evidence_score = int(getattr(lead, "evidence_score", 0) or 0)
        if call_score >= 80:
            reasons.append(f"Call-today score is strong at {call_score}.")
        elif heat_score >= 80:
            reasons.append(f"Heat score is elevated at {heat_score}.")
        if evidence_score >= 70:
            reasons.append(f"Evidence score is solid at {evidence_score}.")
        why_now = str(getattr(lead, "why_now", "") or "").strip()
        if why_now:
            reasons.append(why_now)

    if state is not None and isinstance(state.summary_json, dict):
        last_outcome = str(state.summary_json.get("last_outcome") or "").strip()
        if last_outcome:
            reasons.append(f"Last outcome was {last_outcome.replace('_', ' ')}.")

    reasons.extend(str(item).strip() for item in (plan.blocked_reasons_json or []) if str(item).strip())
    if plan.rationale:
        reasons.append(str(plan.rationale).strip())
    return " ".join(_dedupe(reasons)[:3]).strip()


def _recommended_contact_payload(contact: Optional[LeadContact]) -> Dict[str, Any]:
    if contact is None:
        return {
            "name": "",
            "full_name": "",
            "phone": None,
            "primary_phone": None,
            "email": None,
            "primary_email": None,
            "contact_role": None,
        }
    return {
        "name": str(contact.full_name or "").strip(),
        "full_name": str(contact.full_name or "").strip(),
        "phone": _display_phone(contact.primary_phone),
        "primary_phone": str(contact.primary_phone or "").strip() or None,
        "email": str(contact.primary_email or "").strip() or None,
        "primary_email": str(contact.primary_email or "").strip() or None,
        "contact_role": str(contact.contact_role or "").strip() or None,
    }


def _recommended_channel_for_plan(
    plan: HermesContactPlan,
    lead: Optional[Lead],
    contact: Optional[LeadContact],
    business_context_key: str,
) -> str:
    next_action = plan.next_action_json if isinstance(plan.next_action_json, dict) else {}
    preferred = str(next_action.get("preferred_channel") or "").strip().lower()
    lead_preferred = str(getattr(lead, "preferred_contact_method", "") or "").strip().lower() if lead else ""
    next_type = str(next_action.get("type") or "").strip().lower()
    if preferred in {"call", "email", "sms"}:
        return preferred
    if lead_preferred in {"call", "email", "sms"}:
        return lead_preferred
    if next_type in {"send_figures", "send_info", "send_cma", "send_appraisal", "send_report"}:
        if contact and contact.primary_email:
            return "email"
    if business_context_key == "mortgage":
        if contact and contact.primary_phone:
            return "call"
        if contact and contact.primary_email:
            return "email"
    if contact and contact.primary_phone:
        return "call"
    if contact and contact.primary_email:
        return "email"
    if lead is not None and _safe_list(getattr(lead, "contact_emails", [])):
        return "email"
    return "review"


def _recommended_angle_for_plan(
    plan: HermesContactPlan,
    lead: Optional[Lead],
    business_context_key: str,
) -> str:
    if lead is None:
        return str(plan.rationale or "Review the lead and choose the cleanest angle.").strip()

    trigger = str(getattr(lead, "trigger_type", "") or "").lower()
    why_now = str(getattr(lead, "why_now", "") or "").strip()
    what_to_say = str(getattr(lead, "what_to_say", "") or "").strip()
    next_step = str(getattr(lead, "recommended_next_step", "") or "").strip()
    listing_status = str(getattr(lead, "last_listing_status", "") or "").lower()

    if business_context_key == "mortgage" or any(token in trigger for token in ("mortgage", "refinance", "lender", "loan")):
        if what_to_say:
            return what_to_say
        if "refinance" in next_step.lower():
            return next_step
        if why_now:
            return f"Lead with a refinance review angle: {why_now}"
        return "Lead with a refinance review and quantify likely savings before pitching the next step."

    joined = " ".join(part for part in [why_now, what_to_say, next_step, trigger, listing_status] if part).lower()
    if "stale" in joined or "withdraw" in joined or "expired" in joined:
        return what_to_say or "Use a pricing reset angle and show how to relaunch the property with stronger positioning."
    if "probate" in joined:
        return what_to_say or "Use a probate-sensitive angle: practical guidance first, pressure second."
    if next_step:
        return next_step
    if what_to_say:
        return what_to_say
    if why_now:
        return why_now
    return str(plan.rationale or "Open with the strongest property signal and move to a concrete next step.").strip()


def _cotality_status_for_lead(lead: Optional[Lead]) -> str:
    if lead is None:
        return "not_requested"
    status = str(getattr(lead, "enrichment_status", "") or "").strip().lower()
    if status in {"queued", "running", "review_required", "completed", "failed", "no_results", "login_required"}:
        return status
    return "not_requested"


def _cotality_recommended_for_lead(lead: Optional[Lead], business_context_key: str, cotality_status: str) -> bool:
    if lead is None:
        return False
    if cotality_status in {"queued", "running", "review_required"}:
        return False
    if not str(getattr(lead, "address", "") or "").strip():
        return False
    if business_context_key not in {"real_estate", "mortgage"}:
        return False
    call_score = int(getattr(lead, "call_today_score", 0) or 0)
    heat_score = int(getattr(lead, "heat_score", 0) or 0)
    evidence_score = int(getattr(lead, "evidence_score", 0) or 0)
    has_angle = bool(str(getattr(lead, "what_to_say", "") or "").strip() or str(getattr(lead, "why_now", "") or "").strip())
    return cotality_status in {"not_requested", "failed", "completed"} and (
        call_score >= 70 or heat_score >= 80 or evidence_score >= 70 or not has_angle
    )


def _next_best_action_for_plan(
    plan: HermesContactPlan,
    lead: Optional[Lead],
    recommended_channel: str,
    recommended_angle: str,
) -> str:
    next_action = plan.next_action_json if isinstance(plan.next_action_json, dict) else {}
    title = str(next_action.get("title") or "").strip()
    if title:
        return title
    owner = str(getattr(lead, "owner_name", "") or "").strip() if lead else ""
    target = owner or "the lead"
    if recommended_channel == "call":
        return f"Call {target} using this angle: {recommended_angle}"
    if recommended_channel == "email":
        return f"Draft an email to {target} around this angle: {recommended_angle}"
    if recommended_channel == "sms":
        return f"Send a short text to {target} based on this angle: {recommended_angle}"
    return str(plan.rationale or "Review the lead and choose the next step.").strip()


async def get_hermes_rep_brief(
    session: AsyncSession,
    rep_id: str,
    *,
    business_context: Optional[str] = None,
    target_date: Optional[str] = None,
) -> Dict[str, Any]:
    report_date = target_date or _utcnow().date().isoformat()
    metrics = {"dial_count": 0, "connect_count": 0, "booking_attempts": 0, "talk_time_seconds": 0}
    coaching_row: Dict[str, Any] = {}
    voice_row: Dict[str, Any] = {}

    try:
        metrics_row = (
            await session.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS dial_count,
                        COALESCE(SUM(CASE WHEN COALESCE(connected, FALSE) THEN 1 ELSE 0 END), 0) AS connect_count,
                        COALESCE(SUM(CASE WHEN booking_attempted = 1 THEN 1 ELSE 0 END), 0) AS booking_attempts,
                        COALESCE(SUM(COALESCE(call_duration_seconds, duration_seconds, 0)), 0) AS talk_time_seconds
                    FROM call_log
                    WHERE user_id = :rep_id
                      AND logged_date = :target_date
                    """
                ),
                {"rep_id": rep_id, "target_date": report_date},
            )
        ).mappings().first()
        if metrics_row:
            metrics = {
                "dial_count": int(metrics_row.get("dial_count") or 0),
                "connect_count": int(metrics_row.get("connect_count") or 0),
                "booking_attempts": int(metrics_row.get("booking_attempts") or 0),
                "talk_time_seconds": int(metrics_row.get("talk_time_seconds") or 0),
            }
    except Exception:
        pass

    try:
        coaching_row = dict((
            await session.execute(
                text(
                    """
                    SELECT brutal_summary, live_task, generated_at
                    FROM coaching_reports
                    WHERE rep_id = :rep_id
                    ORDER BY COALESCE(generated_at, created_at, updated_at, '') DESC
                    LIMIT 1
                    """
                ),
                {"rep_id": rep_id},
            )
        ).mappings().first() or {})
    except Exception:
        coaching_row = {}

    try:
        voice_row = dict((
            await session.execute(
                text(
                    """
                    SELECT filler_count, agent_talk_ratio, pace_wpm, issues_json, highlights_json, updated_at
                    FROM voice_trainer_reports
                    WHERE rep_id = :rep_id
                    ORDER BY COALESCE(updated_at, '') DESC
                    LIMIT 1
                    """
                ),
                {"rep_id": rep_id},
            )
        ).mappings().first() or {})
    except Exception:
        try:
            voice_row = dict((
                await session.execute(
                    text(
                        """
                        SELECT highlights_json, issues_json, updated_at
                        FROM voice_trainer_reports
                        WHERE rep_id = :rep_id
                        ORDER BY COALESCE(updated_at, created_at, '') DESC
                        LIMIT 1
                        """
                    ),
                    {"rep_id": rep_id},
                )
            ).mappings().first() or {})
        except Exception:
            voice_row = {}

    voice_issues = _safe_json_loads(voice_row.get("issues_json"), [])
    voice_highlights = _safe_json_loads(voice_row.get("highlights_json"), [])
    voice_focus = str(
        (voice_issues[0] if isinstance(voice_issues, list) and voice_issues else "")
        or (voice_highlights[0] if isinstance(voice_highlights, list) and voice_highlights else "")
        or ""
    ).strip()
    coaching_focus = str(coaching_row.get("brutal_summary") or voice_focus or "").strip()
    if not coaching_focus:
        coaching_focus = (
            "No calls logged for this rep yet. Start logging calls so Hermes can coach the next dial."
            if metrics["dial_count"] == 0
            else "Keep tightening the opener and next-step ask to make the next dial cleaner."
        )

    filler_count = int(voice_row.get("filler_count") or 0)
    talk_ratio = float(voice_row.get("agent_talk_ratio") or 0)
    pace = float(voice_row.get("pace_wpm") or 0)
    next_training_focus = str(coaching_row.get("live_task") or "").strip()
    if not next_training_focus:
        if filler_count >= 5:
            next_training_focus = "Reduce filler words in the opener and transition sentences."
        elif talk_ratio >= 0.68:
            next_training_focus = "Ask a tighter discovery question sooner and let the prospect talk more."
        elif pace >= 165:
            next_training_focus = "Slow the pace slightly so the delivery sounds calmer and clearer."
        else:
            next_training_focus = "Keep the opener concise and move to the next-step ask earlier."

    latest_generated_at = str(coaching_row.get("generated_at") or voice_row.get("updated_at") or "")

    return {
        "rep_id": rep_id,
        "business_context": business_context or "all",
        "date": report_date,
        "metrics": metrics,
        "summary": {
            "dial_count": metrics["dial_count"],
            "connect_count": metrics["connect_count"],
            "booking_attempts": metrics["booking_attempts"],
            "talk_time_seconds": metrics["talk_time_seconds"],
        },
        "coaching_focus": coaching_focus,
        "latest_coaching": {
            "summary": str(coaching_row.get("brutal_summary") or "").strip(),
            "live_task": str(coaching_row.get("live_task") or "").strip(),
            "generated_at": latest_generated_at,
        },
        "voice_focus": voice_focus,
        "next_training_focus": next_training_focus,
        "latest_voice_flags": {
            "filler_count": filler_count,
            "agent_talk_ratio": talk_ratio,
            "pace_wpm": pace,
        },
        "latest_generated_at": latest_generated_at,
    }


async def build_rep_performance_brief(
    session: AsyncSession,
    *,
    rep_id: str,
    business_context_key: Optional[str] = None,
    date: Optional[str] = None,
) -> Dict[str, Any]:
    return await get_hermes_rep_brief(
        session,
        rep_id,
        business_context=business_context_key,
        target_date=date,
    )


async def get_hermes_today(
    session: AsyncSession,
    *,
    business_context_key: Optional[str] = None,
    limit: int = 25,
    auto_refresh: bool = True,
) -> Dict[str, Any]:
    if auto_refresh:
        await refresh_hermes_portfolio(session, limit=max(limit * 4, 100), actor="today_refresh")

    query = select(HermesContactPlan)
    if business_context_key:
        query = query.where(HermesContactPlan.business_context_key == business_context_key)
    plans = (await session.execute(query)).scalars().all()
    plans = sorted(plans, key=lambda item: (_lane_bucket(item.lane), -float(item.priority_score or 0), item.updated_at))
    selected = plans[: max(1, min(limit, 100))]
    cluster_keys = [item.cluster_key for item in plans if item.cluster_key]
    clusters = (
        await session.execute(select(HermesContactCluster).where(HermesContactCluster.cluster_key.in_(cluster_keys)))
    ).scalars().all() if cluster_keys else []
    cluster_map = {item.cluster_key: item for item in clusters}

    contact_ids = [item.lead_contact_id for item in selected]
    contacts = (
        await session.execute(select(LeadContact).where(LeadContact.id.in_(contact_ids)))
    ).scalars().all() if contact_ids else []
    contact_map = {item.id: item for item in contacts}
    lead_ids = [item.lead_id for item in selected if item.lead_id]
    leads = (
        await session.execute(select(Lead).where(Lead.id.in_(lead_ids)))
    ).scalars().all() if lead_ids else []
    lead_map = {str(item.id): item for item in leads}

    top_actions = []
    blocked = []
    duplicates = 0
    review_required = 0
    for item in selected:
        contact = contact_map.get(item.lead_contact_id)
        lead = lead_map.get(str(item.lead_id or ""))
        cluster = cluster_map.get(item.cluster_key or "")
        payload = {
            "lead_contact_id": item.lead_contact_id,
            "lead_id": item.lead_id,
            "business_context_key": item.business_context_key,
            "lane": item.lane,
            "priority_score": item.priority_score,
            "review_required": item.review_required,
            "rationale": item.rationale,
            "next_action": item.next_action_json,
            "blocked_reasons": item.blocked_reasons_json,
            "contact": {
                "full_name": getattr(contact, "full_name", ""),
                "primary_phone": getattr(contact, "primary_phone", None),
                "primary_email": getattr(contact, "primary_email", None),
            },
            "cluster": {
                "cluster_key": getattr(cluster, "cluster_key", None),
                "duplicate_count": int(getattr(cluster, "duplicate_count", 0) or 0),
                "primary_lead_contact_id": getattr(cluster, "primary_lead_contact_id", None),
            },
            "lead": {
                "address": getattr(lead, "address", "") if lead else "",
                "suburb": getattr(lead, "suburb", "") if lead else "",
                "owner_name": getattr(lead, "owner_name", "") if lead else "",
                "call_today_score": int(getattr(lead, "call_today_score", 0) or 0) if lead else 0,
                "status": getattr(lead, "status", "") if lead else "",
            },
        }
        business_key = str(item.business_context_key or business_context_key or _infer_business_context(lead.model_dump() if lead else {}))
        cotality_status = _cotality_status_for_lead(lead)
        recommended_channel = _recommended_channel_for_plan(item, lead, contact, business_key)
        recommended_angle = _recommended_angle_for_plan(item, lead, business_key)
        payload.update(
            {
                "priority_reason": _priority_reason_for_plan(item, lead, None),
                "recommended_angle": recommended_angle,
                "recommended_channel": recommended_channel,
                "recommended_contact": _recommended_contact_payload(contact),
                "cotality_recommended": _cotality_recommended_for_lead(lead, business_key, cotality_status),
                "cotality_status": cotality_status,
                "next_best_action": _next_best_action_for_plan(item, lead, recommended_channel, recommended_angle),
            }
        )
        if cluster and int(cluster.duplicate_count or 0) > 1:
            duplicates += 1
        if item.review_required:
            review_required += 1
        if item.lane in {"hold", "review_required"}:
            blocked.append(payload)
        else:
            top_actions.append(payload)

    return {
        "generated_at": now_iso(),
        "business_context_key": business_context_key or "all",
        "summary": {
            "total_plans": len(plans),
            "call_now": sum(1 for item in plans if item.lane == "call_now"),
            "follow_up_later": sum(1 for item in plans if item.lane == "follow_up_later"),
            "enrich_first": sum(1 for item in plans if item.lane == "enrich_first"),
            "review_required": review_required,
            "duplicate_contact_surfaces": duplicates,
        },
        "top_actions": top_actions,
        "blocked": blocked[: max(1, min(limit, 50))],
    }


async def build_chat_context(
    session: AsyncSession,
    *,
    lead_id: Optional[str] = None,
    lead_contact_id: Optional[str] = None,
    business_context_key: Optional[str] = None,
) -> str:
    if lead_contact_id:
        payload = await get_hermes_contact(session, lead_contact_id)
        lead = payload.get("lead") or {}
        plan = payload.get("plan") or {}
        state = payload.get("state") or {}
        return (
            f"Lead contact context:\n"
            f"- Lead ID: {payload['contact'].get('lead_id') or ''}\n"
            f"- Address: {lead.get('address') or ''}\n"
            f"- Contact: {payload['contact'].get('full_name') or ''}\n"
            f"- Phone: {payload['contact'].get('primary_phone') or ''}\n"
            f"- Lane: {plan.get('lane') or ''}\n"
            f"- Next action: {(plan.get('next_action') or {}).get('title') or ''}\n"
            f"- Rationale: {plan.get('rationale') or ''}\n"
            f"- Callable now: {state.get('callable_now')}\n"
        )
    if lead_id:
        payload = await get_hermes_lead(session, lead_id)
        memory = payload.get("memory") or {}
        plans = payload.get("plans") or []
        top_plan = plans[0] if plans else {}
        summary = memory.get("summary_json") or {}
        return (
            f"Lead context:\n"
            f"- Lead ID: {lead_id}\n"
            f"- Address: {summary.get('address') or ''}\n"
            f"- Owner: {summary.get('owner_name') or ''}\n"
            f"- Why now: {summary.get('why_now') or ''}\n"
            f"- Recommended next step: {summary.get('recommended_next_step') or ''}\n"
            f"- Callable now: {summary.get('callable_now')}\n"
            f"- Top Hermes lane: {top_plan.get('lane') or ''}\n"
            f"- Top Hermes rationale: {top_plan.get('rationale') or ''}\n"
        )
    today = await get_hermes_today(session, business_context_key=business_context_key, limit=8, auto_refresh=True)
    return (
        f"Hermes today summary:\n"
        f"- Business context: {today.get('business_context_key')}\n"
        f"- Call now: {today['summary'].get('call_now', 0)}\n"
        f"- Follow up later: {today['summary'].get('follow_up_later', 0)}\n"
        f"- Enrich first: {today['summary'].get('enrich_first', 0)}\n"
        f"- Review required: {today['summary'].get('review_required', 0)}\n"
        f"- Duplicate contact surfaces: {today['summary'].get('duplicate_contact_surfaces', 0)}\n"
    )
