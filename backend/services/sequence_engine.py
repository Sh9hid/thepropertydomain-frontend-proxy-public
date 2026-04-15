"""
Sequence Engine — autonomous contact sequence orchestration.

Determines the right outreach cadence for a lead based on score and archetype,
creates pending Task entries with template content, and advances through the
sequence as steps are completed.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from models.sql_models import Lead, Task
from services import template_library

logger = logging.getLogger(__name__)

# ─── Sequence definitions ─────────────────────────────────────────────────────

_SEQUENCES: Dict[str, List[Dict[str, Any]]] = {
    "hot": [
        {"channel": "call",  "wait_hours": 0,  "stage": "hot",       "conditional": None},
        {"channel": "sms",   "wait_hours": 2,  "stage": "hot",       "conditional": None},
        {"channel": "call",  "wait_hours": 24, "stage": "hot",       "conditional": None},
        {"channel": "email", "wait_hours": 48, "stage": "hot",       "conditional": None},
    ],
    "warm": [
        {"channel": "sms",     "wait_hours": 0,   "stage": "warm",    "conditional": None},
        {"channel": "call",    "wait_hours": 24,  "stage": "warm",    "conditional": None},
        {"channel": "email",   "wait_hours": 72,  "stage": "warm",    "conditional": None},
        {"channel": "email",   "wait_hours": 168, "stage": "nurture", "conditional": None},
    ],
    "doorknock": [
        {"channel": "email",  "wait_hours": 2,   "stage": "doorknock", "conditional": None},
        {"channel": "call",   "wait_hours": 48,  "stage": "doorknock", "conditional": None},
        {"channel": "sms",    "wait_hours": 168, "stage": "doorknock", "conditional": None},
        {"channel": "email",  "wait_hours": 2160, "stage": "nurture",  "conditional": None},  # ~90 days
    ],
    "cold": [
        {"channel": "email", "wait_hours": 0,   "stage": "cold", "conditional": None},
        {"channel": "sms",   "wait_hours": 72,  "stage": "cold", "conditional": "opened"},
        {"channel": "call",  "wait_hours": 120, "stage": "cold", "conditional": "clicked"},
        {"channel": "email", "wait_hours": 720, "stage": "nurture", "conditional": None},  # ~30 days
    ],
}


def determine_sequence(lead: dict) -> list[dict]:
    """Pure function: return the sequence steps for a lead based on archetype/score.

    Selection rules:
      - score >= 80 or lifecycle_stage == 'hot'       -> hot
      - 50 <= score < 80 or lifecycle_stage == 'warm'  -> warm
      - lifecycle_stage == 'doorknock'                  -> doorknock
      - score < 50 (default)                            -> cold
    """
    score = int(lead.get("call_today_score") or lead.get("heat_score") or 0)
    lifecycle = (lead.get("lifecycle_stage") or "").lower().strip()
    archetype = (lead.get("lead_archetype") or "").lower().strip()

    if lifecycle == "doorknock" or archetype == "doorknock":
        return list(_SEQUENCES["doorknock"])
    if score >= 80 or lifecycle == "hot":
        return list(_SEQUENCES["hot"])
    if score >= 50 or lifecycle == "warm":
        return list(_SEQUENCES["warm"])
    return list(_SEQUENCES["cold"])


def _sequence_key_for_lead(lead: dict) -> str:
    """Return the sequence name string (hot/warm/doorknock/cold)."""
    score = int(lead.get("call_today_score") or lead.get("heat_score") or 0)
    lifecycle = (lead.get("lifecycle_stage") or "").lower().strip()
    archetype = (lead.get("lead_archetype") or "").lower().strip()

    if lifecycle == "doorknock" or archetype == "doorknock":
        return "doorknock"
    if score >= 80 or lifecycle == "hot":
        return "hot"
    if score >= 50 or lifecycle == "warm":
        return "warm"
    return "cold"


# ─── Assign sequence ─────────────────────────────────────────────────────────


async def assign_sequence(session: AsyncSession, lead_id: str) -> dict:
    """Determine and create the full outreach sequence for a lead.

    Creates Task rows with approval_status='pending' for each step.
    Returns summary dict with sequence name and task count.
    """
    result = await session.execute(select(Lead).where(Lead.id == lead_id))
    lead_row = result.scalars().first()
    if not lead_row:
        logger.warning("assign_sequence: lead %s not found", lead_id)
        return {"error": "lead_not_found", "lead_id": lead_id}

    lead = _lead_to_dict(lead_row)
    seq_name = _sequence_key_for_lead(lead)
    steps = determine_sequence(lead)
    now = now_iso()
    base_time = datetime.fromisoformat(now)

    created_tasks: List[str] = []
    for idx, step in enumerate(steps):
        due_at = (base_time + timedelta(hours=step["wait_hours"])).isoformat()

        # Build message content from template library
        subject = ""
        body = ""
        if step["channel"] in ("sms", "email"):
            tpl = await template_library.select_best_template(
                session, channel=step["channel"], stage=step["stage"], lead=lead,
            )
            if tpl:
                filled = template_library.fill_template(
                    tpl["body"], lead, subject=tpl.get("subject"),
                )
                body = filled["body"]
                subject = filled["subject"]

        task = Task(
            id=str(uuid.uuid4()),
            lead_id=lead_id,
            title=f"{seq_name.upper()} seq step {idx + 1}: {step['channel']}",
            task_type="sequence_step",
            action_type=step["channel"],
            channel=step["channel"],
            due_at=due_at,
            status="pending",
            approval_status="pending",
            message_subject=subject,
            message_preview=body[:500] if body else "",
            cadence_name=seq_name,
            cadence_step=idx + 1,
            auto_generated=1,
            payload_json={
                "sequence": seq_name,
                "step_index": idx,
                "stage": step["stage"],
                "conditional": step.get("conditional"),
                "wait_hours": step["wait_hours"],
            },
            created_at=now,
            updated_at=now,
        )
        session.add(task)
        created_tasks.append(task.id)

    # Update lead cadence fields
    lead_row.cadence_name = seq_name
    lead_row.cadence_step = 1
    if steps:
        lead_row.next_action_type = steps[0]["channel"]
        lead_row.next_action_channel = steps[0]["channel"]
        lead_row.next_action_at = base_time.isoformat()
    lead_row.updated_at = now

    await session.commit()
    logger.info(
        "Assigned %s sequence to lead %s — %d tasks created",
        seq_name, lead_id, len(created_tasks),
    )
    return {
        "lead_id": lead_id,
        "sequence": seq_name,
        "steps": len(steps),
        "tasks_created": len(created_tasks),
        "task_ids": created_tasks,
    }


# ─── Advance sequence ────────────────────────────────────────────────────────


async def advance_sequence(session: AsyncSession, lead_id: str) -> dict:
    """After the current step is completed, advance to the next pending step.

    Marks the current step's task as 'completed' and activates the next one.
    Returns the new state or indicates the sequence is finished.
    """
    result = await session.execute(select(Lead).where(Lead.id == lead_id))
    lead_row = result.scalars().first()
    if not lead_row:
        return {"error": "lead_not_found"}

    cadence = lead_row.cadence_name or ""
    current_step = lead_row.cadence_step or 0

    # Find the current (completed) task
    current_q = (
        select(Task)
        .where(
            Task.lead_id == lead_id,
            Task.cadence_name == cadence,
            Task.cadence_step == current_step,
            Task.task_type == "sequence_step",
        )
    )
    current_result = await session.execute(current_q)
    current_task = current_result.scalars().first()
    if current_task and current_task.status != "completed":
        current_task.status = "completed"
        current_task.completed_at = now_iso()
        current_task.updated_at = now_iso()

    # Find the next pending task
    next_step = current_step + 1
    next_q = (
        select(Task)
        .where(
            Task.lead_id == lead_id,
            Task.cadence_name == cadence,
            Task.cadence_step == next_step,
            Task.task_type == "sequence_step",
        )
    )
    next_result = await session.execute(next_q)
    next_task = next_result.scalars().first()

    now = now_iso()
    if not next_task:
        # Sequence complete
        lead_row.cadence_step = 0
        lead_row.next_action_type = ""
        lead_row.next_action_channel = ""
        lead_row.next_action_at = None
        lead_row.updated_at = now
        await session.commit()
        return {
            "lead_id": lead_id,
            "sequence": cadence,
            "status": "sequence_complete",
            "step": current_step,
        }

    # Check conditional gate
    payload = next_task.payload_json or {}
    conditional = payload.get("conditional")
    if conditional:
        lead_dict = _lead_to_dict(lead_row)
        if conditional == "opened" and (lead_row.email_open_count or 0) == 0:
            # Skip this step — condition not met
            lead_row.cadence_step = next_step
            lead_row.updated_at = now
            await session.commit()
            return await advance_sequence(session, lead_id)
        if conditional == "clicked" and (lead_row.email_click_count or 0) == 0:
            lead_row.cadence_step = next_step
            lead_row.updated_at = now
            await session.commit()
            return await advance_sequence(session, lead_id)

    # Activate the next step
    lead_row.cadence_step = next_step
    lead_row.next_action_type = next_task.action_type or ""
    lead_row.next_action_channel = next_task.channel or ""
    lead_row.next_action_at = next_task.due_at
    lead_row.next_action_title = next_task.title or ""
    lead_row.updated_at = now

    await session.commit()
    logger.info(
        "Advanced lead %s to step %d of %s sequence", lead_id, next_step, cadence,
    )
    return {
        "lead_id": lead_id,
        "sequence": cadence,
        "status": "advanced",
        "step": next_step,
        "channel": next_task.channel,
        "due_at": next_task.due_at,
        "task_id": next_task.id,
    }


# ─── Query ────────────────────────────────────────────────────────────────────


async def get_sequence_state(session: AsyncSession, lead_id: str) -> dict:
    """Return the current position and all steps in a lead's active sequence."""
    result = await session.execute(select(Lead).where(Lead.id == lead_id))
    lead_row = result.scalars().first()
    if not lead_row:
        return {"error": "lead_not_found"}

    cadence = lead_row.cadence_name or ""
    if not cadence:
        return {
            "lead_id": lead_id,
            "sequence": None,
            "current_step": 0,
            "steps": [],
        }

    tasks_q = (
        select(Task)
        .where(
            Task.lead_id == lead_id,
            Task.cadence_name == cadence,
            Task.task_type == "sequence_step",
        )
        .order_by(Task.cadence_step)
    )
    tasks_result = await session.execute(tasks_q)
    tasks = tasks_result.scalars().all()

    return {
        "lead_id": lead_id,
        "sequence": cadence,
        "current_step": lead_row.cadence_step or 0,
        "next_action_at": lead_row.next_action_at,
        "next_action_channel": lead_row.next_action_channel,
        "steps": [
            {
                "step": t.cadence_step,
                "channel": t.channel,
                "status": t.status,
                "due_at": t.due_at,
                "completed_at": t.completed_at,
                "approval_status": t.approval_status,
                "task_id": t.id,
            }
            for t in tasks
        ],
    }


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _lead_to_dict(lead: Lead) -> dict:
    """Minimal lead-to-dict for template filling and sequence selection."""
    return {
        "id": lead.id,
        "address": lead.address or "",
        "suburb": lead.suburb or "",
        "owner_name": lead.owner_name or "",
        "owner_first_name": lead.owner_first_name or "",
        "trigger_type": lead.trigger_type or "",
        "heat_score": lead.heat_score or 0,
        "call_today_score": lead.call_today_score or 0,
        "lifecycle_stage": lead.lifecycle_stage or "",
        "lead_archetype": lead.lead_archetype or "",
        "email_open_count": lead.email_open_count or 0,
        "email_click_count": lead.email_click_count or 0,
        "status": lead.status or "captured",
    }
