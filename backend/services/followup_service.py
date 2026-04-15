from __future__ import annotations

import json
import uuid
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.logic import _append_activity, _build_activity_entry, _hydrate_lead
from services.automations import _refresh_lead_next_action
from services.lead_read_cache import invalidate_lead_read_models


FOLLOWUP_FREQUENCIES = {"none", "manual", "weekly", "monthly"}
FOLLOWUP_STATUSES = {"active", "paused", "unsubscribed", "closed"}
FOLLOWUP_EVENT_TYPES = {
    "call",
    "email",
    "sms",
    "note",
    "followup_scheduled",
    "followup_executed",
    "followup_paused",
    "followup_skipped",
    "preference_updated",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _normalize_iso(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _iso_now() -> str:
    return _utc_now().isoformat()


def _hydrate_followup_lead(row: Dict[str, Any]) -> Dict[str, Any]:
    lead = _hydrate_lead(row)
    lead["market_updates_opt_in"] = bool(lead.get("market_updates_opt_in"))
    return lead


def _add_month(dt: datetime) -> datetime:
    month = dt.month + 1
    year = dt.year
    if month > 12:
        month = 1
        year += 1
    day = min(dt.day, monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def _next_due_from_frequency(current_due_iso: str, frequency: str) -> Optional[str]:
    normalized = str(frequency or "none").strip().lower()
    if normalized == "weekly":
        dt = datetime.fromisoformat(current_due_iso.replace("Z", "+00:00"))
        return (dt + timedelta(days=7)).astimezone(timezone.utc).replace(microsecond=0).isoformat()
    if normalized == "monthly":
        dt = datetime.fromisoformat(current_due_iso.replace("Z", "+00:00"))
        return _add_month(dt.astimezone(timezone.utc).replace(microsecond=0)).isoformat()
    return None


async def _fetch_lead_row(session: AsyncSession, lead_id: str) -> Optional[Dict[str, Any]]:
    row = (await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})).mappings().first()
    return dict(row) if row else None


async def _record_interaction(
    session: AsyncSession,
    *,
    lead_id: str,
    event_type: str,
    summary: str,
    direction: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    actor: Optional[str] = None,
    source: Optional[str] = None,
    created_at: Optional[str] = None,
) -> str:
    if event_type not in FOLLOWUP_EVENT_TYPES:
        raise ValueError(f"Unsupported interaction type: {event_type}")
    interaction_id = str(uuid.uuid4())
    created = created_at or _iso_now()
    await session.execute(
        text(
            """
            INSERT INTO lead_interactions (
                id, lead_id, event_type, direction, summary, payload_json, actor, source, created_at
            ) VALUES (
                :id, :lead_id, :event_type, :direction, :summary, :payload_json, :actor, :source, :created_at
            )
            """
        ),
        {
            "id": interaction_id,
            "lead_id": lead_id,
            "event_type": event_type,
            "direction": direction,
            "summary": summary,
            "payload_json": json.dumps(payload or {}),
            "actor": actor or "system",
            "source": source or "followup_service",
            "created_at": created,
        },
    )
    return interaction_id


async def _append_lead_activity(
    session: AsyncSession,
    *,
    lead: Dict[str, Any],
    event_type: str,
    note: str,
    channel: str,
    recipient: Optional[str] = None,
    subject: Optional[str] = None,
) -> None:
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry(event_type, note, lead.get("status"), channel, subject, recipient),
    )
    await session.execute(
        text("UPDATE leads SET activity_log = :activity_log, updated_at = :updated_at WHERE id = :id"),
        {"activity_log": json.dumps(activity_log or []), "updated_at": _iso_now(), "id": lead["id"]},
    )


async def cancel_pending_followup_tasks(session: AsyncSession, lead_id: str, *, keep_task_id: Optional[str] = None) -> None:
    params = {"lead_id": lead_id, "updated_at": _iso_now()}
    sql = """
        UPDATE tasks
        SET status = 'cancelled',
            updated_at = :updated_at
        WHERE lead_id = :lead_id
          AND task_type = 'follow_up'
          AND status IN ('pending', 'running')
    """
    if keep_task_id:
        sql += " AND id != :keep_task_id"
        params["keep_task_id"] = keep_task_id
    await session.execute(text(sql), params)


async def schedule_followup_task(
    session: AsyncSession,
    lead_id: str,
    *,
    due_at: Optional[str],
    source: str = "followup_preferences",
    actor: str = "operator",
) -> Optional[Dict[str, Any]]:
    lead = await _fetch_lead_row(session, lead_id)
    if not lead:
        raise LookupError(f"Lead not found: {lead_id}")

    due_at_iso = _normalize_iso(due_at)
    followup_status = str(lead.get("followup_status") or "active").strip().lower()
    followup_frequency = str(lead.get("followup_frequency") or "none").strip().lower()
    preferred_method = str(
        lead.get("preferred_contact_method") or lead.get("preferred_channel") or "manual"
    ).strip().lower() or "manual"

    await cancel_pending_followup_tasks(session, lead_id)

    if not due_at_iso or followup_frequency == "none" or followup_status in {"paused", "unsubscribed", "closed"}:
        await session.execute(
            text(
                """
                UPDATE leads
                SET next_followup_at = :next_followup_at,
                    follow_up_due_at = :follow_up_due_at,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {
                "next_followup_at": due_at_iso,
                "follow_up_due_at": due_at_iso,
                "updated_at": _iso_now(),
                "id": lead_id,
            },
        )
        await _refresh_lead_next_action(session, lead_id)
        return None

    task_id = str(uuid.uuid4())
    now_iso = _iso_now()
    payload = {
        "lead_id": lead_id,
        "task_type": "followup",
        "preferred_contact_method": preferred_method,
        "followup_frequency": followup_frequency,
        "delivery_status": "drafted" if preferred_method in {"email", "sms"} else "manual_required",
    }
    title = f"{preferred_method.replace('_', ' ').title()} follow-up"
    notes = str(lead.get("followup_notes") or "").strip()
    await session.execute(
        text(
            """
            INSERT INTO tasks (
                id, lead_id, title, task_type, action_type, channel, due_at, status, notes,
                related_report_id, approval_status, message_subject, message_preview, rewrite_reason,
                superseded_by, cadence_name, cadence_step, auto_generated, priority_bucket, payload_json,
                attempt_count, last_error, completed_at, created_at, updated_at
            ) VALUES (
                :id, :lead_id, :title, 'follow_up', 'follow_up', :channel, :due_at, 'pending', :notes,
                '', 'not_required', '', '', '', '', 'lead_followup', 0, 1, 'follow_up', :payload_json,
                0, NULL, NULL, :created_at, :updated_at
            )
            """
        ),
        {
            "id": task_id,
            "lead_id": lead_id,
            "title": title,
            "channel": preferred_method,
            "due_at": due_at_iso,
            "notes": notes,
            "payload_json": json.dumps(payload),
            "created_at": now_iso,
            "updated_at": now_iso,
        },
    )
    await _record_interaction(
        session,
        lead_id=lead_id,
        event_type="followup_scheduled",
        summary=f"{title} scheduled for {due_at_iso}",
        payload={"task_id": task_id, **payload},
        actor=actor,
        source=source,
        created_at=now_iso,
    )
    await _refresh_lead_next_action(session, lead_id)
    row = (
        await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})
    ).mappings().first()
    return dict(row) if row else None


async def update_followup_preferences(
    session: AsyncSession,
    lead_id: str,
    payload: Dict[str, Any],
    *,
    actor: str = "operator",
    source: str = "api",
) -> Dict[str, Any]:
    lead = await _fetch_lead_row(session, lead_id)
    if not lead:
        raise LookupError(f"Lead not found: {lead_id}")

    preferred_contact_method = str(
        payload.get("preferred_contact_method") or lead.get("preferred_contact_method") or ""
    ).strip().lower()
    followup_frequency = str(payload.get("followup_frequency") or lead.get("followup_frequency") or "none").strip().lower()
    followup_status = str(payload.get("followup_status") or lead.get("followup_status") or "active").strip().lower()
    next_followup_at = _normalize_iso(payload.get("next_followup_at") if "next_followup_at" in payload else lead.get("next_followup_at"))
    market_updates_opt_in = bool(
        payload.get("market_updates_opt_in")
        if "market_updates_opt_in" in payload
        else lead.get("market_updates_opt_in")
    )
    followup_notes = payload.get("followup_notes") if "followup_notes" in payload else lead.get("followup_notes")

    if followup_frequency not in FOLLOWUP_FREQUENCIES:
        raise ValueError("followup_frequency must be one of: none, manual, weekly, monthly")
    if followup_status not in FOLLOWUP_STATUSES:
        raise ValueError("followup_status must be one of: active, paused, unsubscribed, closed")

    now_iso = _iso_now()
    await session.execute(
        text(
            """
            UPDATE leads
            SET preferred_contact_method = :preferred_contact_method,
                preferred_channel = CASE WHEN :preferred_contact_method != '' THEN :preferred_contact_method ELSE preferred_channel END,
                followup_frequency = :followup_frequency,
                market_updates_opt_in = :market_updates_opt_in,
                next_followup_at = :next_followup_at,
                follow_up_due_at = :next_followup_at,
                followup_status = :followup_status,
                followup_notes = :followup_notes,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {
            "preferred_contact_method": preferred_contact_method,
            "followup_frequency": followup_frequency,
            "market_updates_opt_in": 1 if market_updates_opt_in else 0,
            "next_followup_at": next_followup_at,
            "followup_status": followup_status,
            "followup_notes": followup_notes,
            "updated_at": now_iso,
            "id": lead_id,
        },
    )
    updated = await _fetch_lead_row(session, lead_id)
    if not updated:
        raise LookupError(f"Lead not found after update: {lead_id}")

    summary = (
        f"Preferences updated: {preferred_contact_method or 'unspecified'} / "
        f"{followup_frequency} / {followup_status}"
    )
    await _record_interaction(
        session,
        lead_id=lead_id,
        event_type="preference_updated",
        summary=summary,
        payload={
            "preferred_contact_method": preferred_contact_method,
            "followup_frequency": followup_frequency,
            "market_updates_opt_in": market_updates_opt_in,
            "next_followup_at": next_followup_at,
            "followup_status": followup_status,
            "followup_notes": followup_notes,
        },
        actor=actor,
        source=source,
        created_at=now_iso,
    )
    await _append_lead_activity(
        session,
        lead=updated,
        event_type="preference_updated",
        note=summary,
        channel="follow_up",
    )
    if followup_status == "paused":
        await _record_interaction(
            session,
            lead_id=lead_id,
            event_type="followup_paused",
            summary="Follow-up paused",
            payload={"reason": followup_notes or ""},
            actor=actor,
            source=source,
            created_at=now_iso,
        )
    task = await schedule_followup_task(session, lead_id, due_at=next_followup_at, source=source, actor=actor)
    await session.commit()
    invalidate_lead_read_models([lead_id])
    final_lead = await _fetch_lead_row(session, lead_id)
    return {"lead": _hydrate_followup_lead(final_lead or {}), "task": task}


async def get_followup_state(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    lead = await _fetch_lead_row(session, lead_id)
    if not lead:
        raise LookupError(f"Lead not found: {lead_id}")
    task = (
        await session.execute(
            text(
                """
                SELECT *
                FROM tasks
                WHERE lead_id = :lead_id
                  AND task_type = 'follow_up'
                  AND status IN ('pending', 'running')
                ORDER BY due_at ASC, created_at ASC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().first()
    return {"lead": _hydrate_followup_lead(lead), "task": dict(task) if task else None}


async def scan_and_schedule_due_followups(
    session: AsyncSession,
    *,
    now_iso: Optional[str] = None,
    limit: int = 100,
) -> int:
    now_value = _normalize_iso(now_iso) or _iso_now()
    rows = (
        await session.execute(
            text(
                """
                SELECT *
                FROM leads
                WHERE COALESCE(followup_status, 'active') = 'active'
                  AND COALESCE(followup_frequency, 'none') != 'none'
                  AND next_followup_at IS NOT NULL
                  AND next_followup_at <= :now_iso
                ORDER BY next_followup_at ASC
                LIMIT :limit
                """
            ),
            {"now_iso": now_value, "limit": limit},
        )
    ).mappings().all()
    scheduled = 0
    for row in rows:
        lead_id = str(row["id"])
        existing = (
            await session.execute(
                text(
                    """
                    SELECT id
                    FROM tasks
                    WHERE lead_id = :lead_id
                      AND task_type = 'follow_up'
                      AND status IN ('pending', 'running')
                    LIMIT 1
                    """
                ),
                {"lead_id": lead_id},
            )
        ).mappings().first()
        if existing:
            scheduled += 1
            continue
        task = await schedule_followup_task(session, lead_id, due_at=row.get("next_followup_at"), source="scheduler", actor="scheduler")
        if task:
            scheduled += 1
    await session.commit()
    return scheduled


async def execute_followup_task(
    session: AsyncSession,
    task_id: str,
    *,
    now_iso: Optional[str] = None,
    fail_on_note_contains: Optional[str] = None,
) -> bool:
    now_value = _normalize_iso(now_iso) or _iso_now()
    task = (
        await session.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})
    ).mappings().first()
    if not task:
        return False
    task_dict = dict(task)
    if task_dict.get("status") not in {"pending", "running"} or task_dict.get("task_type") != "follow_up":
        return False

    lead = await _fetch_lead_row(session, str(task_dict.get("lead_id") or ""))
    if not lead:
        await session.execute(
            text(
                """
                UPDATE tasks
                SET status = 'failed', attempt_count = COALESCE(attempt_count, 0) + 1,
                    last_error = :last_error, updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {"last_error": "Lead missing for follow-up task", "updated_at": now_value, "id": task_id},
        )
        return False

    lead_status = str(lead.get("followup_status") or "active").strip().lower()
    if lead_status in {"paused", "unsubscribed", "closed"}:
        await session.execute(
            text(
                """
                UPDATE tasks
                SET status = 'cancelled', last_error = NULL, updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {"updated_at": now_value, "id": task_id},
        )
        await _record_interaction(
            session,
            lead_id=lead["id"],
            event_type="followup_skipped",
            summary=f"Follow-up skipped because lead is {lead_status}",
            payload={"task_id": task_id, "followup_status": lead_status},
            actor="worker",
            source="followup_worker",
            created_at=now_value,
        )
        await _refresh_lead_next_action(session, lead["id"])
        return False

    if fail_on_note_contains and fail_on_note_contains in str(task_dict.get("notes") or ""):
        error_message = f"Simulated failure for note token: {fail_on_note_contains}"
        await session.execute(
            text(
                """
                UPDATE tasks
                SET status = 'failed',
                    attempt_count = COALESCE(attempt_count, 0) + 1,
                    last_error = :last_error,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {"last_error": error_message, "updated_at": now_value, "id": task_id},
        )
        return False

    await session.execute(
        text(
            """
            UPDATE tasks
            SET status = 'running',
                attempt_count = COALESCE(attempt_count, 0) + 1,
                last_error = NULL,
                updated_at = :updated_at
            WHERE id = :id AND status = 'pending'
            """
        ),
        {"updated_at": now_value, "id": task_id},
    )
    preferred_method = str(task_dict.get("channel") or lead.get("preferred_contact_method") or "manual").strip().lower()
    delivery_status = "drafted" if preferred_method in {"email", "sms"} else "manual_required"
    summary = f"{preferred_method or 'manual'} follow-up executed"
    payload = {
        "task_id": task_id,
        "preferred_contact_method": preferred_method,
        "delivery_status": delivery_status,
        "notes": task_dict.get("notes") or "",
    }
    await _record_interaction(
        session,
        lead_id=lead["id"],
        event_type="followup_executed",
        direction="outbound",
        summary=summary,
        payload=payload,
        actor="worker",
        source="followup_worker",
        created_at=now_value,
    )
    await _append_lead_activity(
        session,
        lead=lead,
        event_type="followup_executed",
        note=summary,
        channel=preferred_method or "follow_up",
    )
    next_due = _next_due_from_frequency(str(task_dict.get("due_at") or now_value), str(lead.get("followup_frequency") or "none"))
    await session.execute(
        text(
            """
            UPDATE tasks
            SET status = 'completed',
                completed_at = :completed_at,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {"completed_at": now_value, "updated_at": now_value, "id": task_id},
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET last_contacted_at = :last_contacted_at,
                last_activity_type = 'follow_up',
                next_followup_at = :next_followup_at,
                follow_up_due_at = :next_followup_at,
                updated_at = :updated_at
            WHERE id = :id
            """
        ),
        {
            "last_contacted_at": now_value,
            "next_followup_at": next_due,
            "updated_at": now_value,
            "id": lead["id"],
        },
    )
    if next_due:
        await schedule_followup_task(session, lead["id"], due_at=next_due, source="followup_worker", actor="worker")
    await _refresh_lead_next_action(session, lead["id"])
    return True


async def execute_due_followup_tasks(
    session: AsyncSession,
    *,
    now_iso: Optional[str] = None,
    limit: int = 20,
    fail_on_note_contains: Optional[str] = None,
) -> int:
    now_value = _normalize_iso(now_iso) or _iso_now()
    await scan_and_schedule_due_followups(session, now_iso=now_value, limit=limit)
    rows = (
        await session.execute(
            text(
                """
                SELECT id
                FROM tasks
                WHERE task_type = 'follow_up'
                  AND status = 'pending'
                  AND due_at IS NOT NULL
                  AND due_at <= :now_iso
                ORDER BY due_at ASC, created_at ASC
                LIMIT :limit
                """
            ),
            {"now_iso": now_value, "limit": limit},
        )
    ).mappings().all()
    completed = 0
    for row in rows:
        if await execute_followup_task(
            session,
            str(row["id"]),
            now_iso=now_value,
            fail_on_note_contains=fail_on_note_contains,
        ):
            completed += 1
    await session.commit()
    return completed
