"""
Call brief service for phone-first daily calling workflows.
"""

import datetime
import json
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import BRAND_NAME, PRINCIPAL_NAME
from core.logic import _derive_intelligence, _hydrate_lead
from core.utils import _decode_row, now_iso
from services.daily_hit_list_service import enrich_leads_with_daily_hit_list
from services.metrics_service import build_call_log_row, insert_call_log_row
from services.pipeline_guard import assert_status_transition_allowed
from services.zoom_call_sync_service import ensure_call_log_schema

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

VALID_OUTCOMES = {
    "no_answer",
    "voicemail",
    "vm_sent",
    "connected",
    "wrong_number",
    "connected_interested",
    "connected_follow_up",
    "connected_not_interested",
    "connected_do_not_call",
    "spoke",
    "not_interested",
    "booked_appraisal",
    "booked_mortgage",
    "soft_no",
    "hard_no",
    "call_back",
    "send_info",
    "question",
}

# Map human-readable frontend labels → snake_case VALID_OUTCOMES
_OUTCOME_NORMALIZE: dict[str, str] = {
    "no answer": "no_answer",
    "left voicemail": "voicemail",
    "voicemail": "voicemail",
    "vm sent": "vm_sent",
    "vm_sent": "vm_sent",
    "connected": "connected",
    "wrong number": "wrong_number",
    "spoke \u2014 not interested": "connected_not_interested",
    "spoke \u2014 interested": "connected_interested",
    "booked appraisal": "booked_appraisal",
    "booked mortgage": "booked_mortgage",
    "soft no": "soft_no",
    "hard no": "hard_no",
    "call back": "call_back",
    "send info": "send_info",
    "question": "question",
}

STATUS_TRANSITIONS = {
    "booked_appraisal": "appt_booked",
    "booked_mortgage": "mortgage_appt_booked",
    "not_interested": "dropped",
    "connected_not_interested": "dropped",
    "connected_do_not_call": "dropped",
    "connected_interested": "contacted",
    "connected_follow_up": "contacted",
    "connected": "contacted",
    "spoke": "contacted",
}


def _select_call_list_fields(lead: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": lead.get("id"),
        "address": lead.get("address"),
        "suburb": lead.get("suburb"),
        "owner_name": lead.get("owner_name"),
        "contact_phones": lead.get("contact_phones") or [],
        "heat_score": int(lead.get("heat_score") or 0),
        "call_today_score": int(lead.get("call_today_score") or 0),
        "signal_status": lead.get("signal_status"),
        "trigger_type": lead.get("trigger_type"),
        "what_to_say": lead.get("what_to_say"),
        "why_now": lead.get("why_now"),
        "intent_score": int(lead.get("intent_score") or 0),
        "intent_summary": lead.get("intent_summary"),
        "daily_hit_list_rank": lead.get("daily_hit_list_rank"),
        "who_to_call": lead.get("who_to_call"),
        "recommended_next_step": lead.get("recommended_next_step"),
        "touches_14d": int(lead.get("touches_14d") or 0),
    }


def _load_activity_log(raw_value: Any) -> list[dict[str, Any]]:
    if isinstance(raw_value, list):
        return [entry for entry in raw_value if isinstance(entry, dict)]
    try:
        parsed = json.loads(raw_value or "[]")
        if isinstance(parsed, list):
            return [entry for entry in parsed if isinstance(entry, dict)]
    except Exception:
        return []
    return []


def _strip_call_greeting(text: str) -> str:
    message = (text or "").strip()
    for prefix in ("Hi ", "Hello "):
        if message.startswith(prefix):
            comma_index = message.find(",")
            if comma_index > -1:
                return message[comma_index + 1 :].strip()
    return message


def _safe_non_negative_int(value: Any, default: int) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _derive_manual_call_signals(
    normalized_outcome: str,
    note_text: str,
    next_action_due: str | None,
) -> dict[str, Any]:
    intent_map = {
        "booked_appraisal": 0.95,
        "booked_mortgage": 0.95,
        "connected_interested": 0.85,
        "connected_follow_up": 0.72,
        "question": 0.64,
        "call_back": 0.68,
        "send_info": 0.6,
        "connected": 0.55,
        "spoke": 0.55,
        "connected_not_interested": 0.2,
        "not_interested": 0.2,
        "soft_no": 0.24,
        "voicemail": 0.12,
        "vm_sent": 0.12,
        "no_answer": 0.05,
        "wrong_number": 0.0,
        "connected_do_not_call": 0.0,
        "hard_no": 0.0,
    }
    booking_attempted = normalized_outcome in {"booked_appraisal", "booked_mortgage"}
    next_step_detected = bool(next_action_due) or normalized_outcome in {
        "connected_follow_up",
        "booked_appraisal",
        "booked_mortgage",
        "call_back",
        "send_info",
        "question",
    }
    intent_signal = intent_map.get(normalized_outcome, 0.0)
    if "price" in note_text.lower() and intent_signal < 0.6:
        intent_signal = 0.6
    return {
        "intent_signal": round(intent_signal, 2),
        "booking_attempted": booking_attempted,
        "next_step_detected": next_step_detected,
    }


async def get_todays_call_list(session: AsyncSession, limit: int = 25) -> list[dict[str, Any]]:
    safe_limit = max(1, _safe_non_negative_int(limit or 25, 25))
    result = await session.execute(
        text(
            """
            SELECT *
            FROM leads
            WHERE COALESCE(status, 'captured') NOT IN (
                    'converted',
                    'dropped',
                    'appt_booked',
                    'mortgage_appt_booked'
                )
            ORDER BY COALESCE(call_today_score, 0) DESC, COALESCE(heat_score, 0) DESC
            LIMIT :limit
            """
        ),
        {"limit": 5000},
    )
    rows = result.mappings().all()
    leads: list[dict[str, Any]] = []
    for row in rows:
        leads.append(_derive_intelligence(_decode_row(dict(row))))
    ranked = enrich_leads_with_daily_hit_list(leads, limit=200)
    call_ready = [lead for lead in ranked if lead.get("contact_phones")]
    return [_select_call_list_fields(lead) for lead in call_ready[:safe_limit]]


async def log_call_attempt(
    session: AsyncSession,
    lead_id: str,
    outcome: str,
    note: str = "",
    duration_seconds: int = 0,
    user_id: str | None = None,
    next_action_due: str | None = None,
    recording_url: str | None = None,
) -> dict[str, Any]:
    raw_outcome = str(outcome or "").strip()
    normalized_outcome = _OUTCOME_NORMALIZE.get(raw_outcome.lower(), raw_outcome)
    if normalized_outcome not in VALID_OUTCOMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid outcome '{raw_outcome}'. Valid: {sorted(VALID_OUTCOMES)}",
        )

    result = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")

    row_dict = dict(row)
    lead = _decode_row(dict(row_dict))
    current_status = lead.get("status") or "captured"
    timestamp = now_iso()
    activity_log = _load_activity_log(row_dict.get("activity_log"))
    call_duration = _safe_non_negative_int(duration_seconds or 0, 0)
    note_text = str(note or "")
    follow_up_due_value = str(next_action_due or "").strip() or None
    call_signals = _derive_manual_call_signals(normalized_outcome, note_text, follow_up_due_value)

    activity_log.append(
        {
            "type": "call",
            "outcome": normalized_outcome,
            "note": note_text,
            "duration_seconds": call_duration,
            "ts": timestamp,
            "timestamp": timestamp,
            "channel": "phone",
        }
    )

    next_status = STATUS_TRANSITIONS.get(normalized_outcome, current_status)
    if next_status != current_status:
        assert_status_transition_allowed(
            lead,
            next_status,
            source="call_brief_log_call",
            appointment_at=follow_up_due_value,
        )
    if next_status != current_status:
        activity_log.append(
            {
                "type": "status_change",
                "from": current_status,
                "to": next_status,
                "ts": timestamp,
                "timestamp": timestamp,
                "channel": "system",
            }
        )

    sydney_date_str = datetime.datetime.now(SYDNEY_TZ).strftime("%Y-%m-%d")

    from core.logic import _append_stage_note
    stage_note_history = _append_stage_note(
        lead.get("stage_note_history"),
        note_text or f"Call outcome: {normalized_outcome}",
        next_status,
        "call",
        f"Call: {normalized_outcome}",
        "Operator"
    )

    await session.execute(
        text(
            """
            UPDATE leads
            SET activity_log = :activity_log,
                stage_note_history = :stage_note_history,
                status = :status,
                last_outcome = :last_outcome,
                last_outcome_at = :last_outcome_at,
                last_contacted_at = :last_contacted_at,
                last_called_date = :last_called_date,
                follow_up_due_at = COALESCE(:follow_up_due_at, follow_up_due_at),
                next_action_at = COALESCE(:next_action_at, next_action_at),
                next_action_type = CASE
                    WHEN :next_action_at IS NOT NULL THEN 'follow_up'
                    ELSE next_action_type
                END,
                next_action_channel = CASE
                    WHEN :next_action_at IS NOT NULL THEN 'phone'
                    ELSE next_action_channel
                END,
                next_action_title = CASE
                    WHEN :next_action_at IS NOT NULL THEN 'Scheduled follow-up'
                    ELSE next_action_title
                END,
                next_action_reason = CASE
                    WHEN :next_action_at IS NOT NULL THEN 'Operator logged a follow-up due time.'
                    ELSE next_action_reason
                END,
                last_activity_type = :last_activity_type,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {
            "activity_log": json.dumps(activity_log),
            "stage_note_history": json.dumps(stage_note_history),
            "status": next_status,
            "last_outcome": normalized_outcome,
            "last_outcome_at": timestamp,
            "last_contacted_at": timestamp,
            "last_called_date": sydney_date_str,
            "follow_up_due_at": follow_up_due_value,
            "next_action_at": follow_up_due_value,
            "last_activity_type": "call",
            "updated_at": timestamp,
            "id": lead_id,
        },
    )
    
    # Expand connected check to include sentiment variations
    await ensure_call_log_schema(session)
    call_log_row = build_call_log_row(
        lead_id=lead_id,
        lead_address=lead.get("address", ""),
        outcome=normalized_outcome,
        call_duration_seconds=call_duration,
        note=note_text,
        user_id=user_id,
        timestamp=timestamp,
        next_action_due=next_action_due,
        provider="manual",
        direction="outbound",
        from_number="",
        to_number="",
        raw_payload="{}",
        intent_signal=call_signals["intent_signal"],
        booking_attempted=call_signals["booking_attempted"],
        next_step_detected=call_signals["next_step_detected"],
    )
    await insert_call_log_row(session, call_log_row)

    await session.commit()

    updated_result = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    updated_row = updated_result.mappings().first()
    if not updated_row:
        raise HTTPException(status_code=404, detail="Lead not found after update")

    enriched = _derive_intelligence(_decode_row(dict(updated_row)))
    new_bucket = enriched.get("queue_bucket")
    if new_bucket:
        await session.execute(
            text("UPDATE leads SET queue_bucket = :bucket WHERE id = :id"),
            {"bucket": new_bucket, "id": lead_id},
        )
        await session.commit()

    # Auto-start nurture sequence for soft outcomes
    nurture_suggestion = None
    try:
        if normalized_outcome in {"soft_no", "not_now", "not_interested"}:
            from services.nurture_service import select_nurture_template, create_nurture_sequence
            objections = []
            if "price" in note_text.lower():
                objections.append("price")
            if "agent" in note_text.lower():
                objections.append("has_agent")
            if any(w in note_text.lower() for w in ("timing", "later", "not yet", "6 month", "next year")):
                objections.append("timing")
            template_key = select_nurture_template(objections, [], normalized_outcome)
            await create_nurture_sequence(
                session, "real_estate", lead_id, lead_id,
                template_key, reason=f"Auto-created after call outcome: {normalized_outcome}",
            )
            nurture_suggestion = {"template": template_key, "auto_started": True}
        elif normalized_outcome == "no_answer":
            nurture_suggestion = {"suggested_action": "sms_follow_up", "auto_started": False,
                                  "message": "Send a short SMS — lead didn't pick up"}
        elif normalized_outcome in {"send_info", "question"}:
            nurture_suggestion = {"suggested_action": "email_follow_up", "auto_started": False,
                                  "message": "Send info via email with market data"}
    except Exception:
        pass  # Nurture is best-effort, never block call logging

    final_result = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    final_row = final_result.mappings().first()
    if not final_row:
        raise HTTPException(status_code=404, detail="Lead not found after update")
    result = {"lead": _hydrate_lead(dict(final_row)), "call_log_id": call_log_row["id"]}
    if nurture_suggestion:
        result["nurture"] = nurture_suggestion
    if recording_url:
        schedule_call_postprocess(call_log_row["id"])
    return result


def schedule_call_postprocess(call_id: str) -> None:
    """Placeholder for async call postprocessing (transcription, AI summary).

    Future: enqueue a background job that transcribes the recording and
    generates call insights.
    """
    pass


async def get_operator_brief_text(session: AsyncSession) -> str:
    leads = await get_todays_call_list(session, limit=10)
    now_sydney = datetime.datetime.now(SYDNEY_TZ)
    date_text = now_sydney.strftime("%A %d %B %Y %H:%M AEST")

    lines = [
        f"DAILY CALL BRIEF \u2014 {date_text}",
        f"{BRAND_NAME} \u2014 {PRINCIPAL_NAME}",
        f"{len(leads)} leads ready to call today",
        "",
    ]

    for index, lead in enumerate(leads, start=1):
        phones = lead.get("contact_phones") or []
        primary_phone = phones[0] if phones else ""
        lines.extend(
            [
                f"{index}. {lead.get('owner_name') or 'Owner'}",
                f"   {lead.get('address') or ''}, {lead.get('suburb') or ''}",
                f"   Phone: {primary_phone}",
                (
                    f"   Signal: {lead.get('trigger_type') or ''} | "
                    f"Intent: {lead.get('intent_score') or 0} | "
                    f"Heat: {lead.get('heat_score') or 0} | "
                    f"Call: {lead.get('call_today_score') or 0}"
                ),
                f"   Why ranked: {lead.get('intent_summary') or lead.get('why_now') or ''}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


async def get_mortgage_market_brief_text(session: AsyncSession) -> str:
    rates_rows = (
        await session.execute(
            text(
                """
                SELECT brand, MIN(advertised_rate) AS best_rate
                FROM lender_products
                WHERE advertised_rate IS NOT NULL
                  AND LOWER(COALESCE(rate_type, '')) = 'variable'
                  AND LOWER(COALESCE(occupancy_target, '')) = 'owner_occupier'
                GROUP BY brand
                """
            )
        )
    ).mappings().all()
    rates = {str(row.get("brand") or ""): float(row.get("best_rate") or 0.0) for row in rates_rows}

    westpac_rate = rates.get("Westpac")
    st_george_rate = rates.get("St.George Bank")
    nab_rate = rates.get("NATIONAL AUSTRALIA BANK")
    cba_rate = rates.get("CommBank")

    delta_row = (
        await session.execute(
            text(
                """
                SELECT headline
                FROM lender_product_deltas
                ORDER BY detected_at DESC
                LIMIT 1
                """
            )
        )
    ).mappings().first()
    delta_headline = str((delta_row or {}).get("headline") or "").strip()

    line_1_parts: list[str] = []
    if westpac_rate:
        line_1_parts.append(f"Westpac {westpac_rate:.2f}%")
    if st_george_rate:
        line_1_parts.append(f"St.George {st_george_rate:.2f}%")
    if nab_rate:
        line_1_parts.append(f"NAB {nab_rate:.2f}%")
    if cba_rate:
        line_1_parts.append(f"CommBank {cba_rate:.2f}%")
    line_1_summary = ", ".join(line_1_parts) if line_1_parts else "No current public variable pricing loaded."

    lines = [
        f"1. Current owner-occupier variable anchors: {line_1_summary}",
        "2. Borrowers currently above Westpac and St.George",
        "Reason: those two lenders are now the sharper public comparison anchors in this group",
        "3. NAB and CommBank customers",
        "Reason: public rates in this cohort are still generally above the sharpest anchor range.",
    ]
    if delta_headline:
        lines.append(f"Latest market movement: {delta_headline}")
    return "\n".join(lines)
