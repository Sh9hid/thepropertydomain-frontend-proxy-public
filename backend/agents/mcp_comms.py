"""
MCP Tool Server: comms

Wraps outreach/SMS/email services for the Outreach Composer agent.
All outreach is QUEUED for operator approval — agents cannot send directly.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = logging.getLogger(__name__)

TOOLS: list[dict[str, Any]] = [
    {
        "name": "draft_sms",
        "description": (
            "Queue an SMS draft for operator approval. "
            "The message will NOT be sent until the operator approves it. "
            "SMS identity: from Shahid. Max 160 characters."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string", "description": "Target lead UUID"},
                "message": {"type": "string", "description": "SMS body (max 160 chars)"},
            },
            "required": ["lead_id", "message"],
        },
    },
    {
        "name": "draft_email",
        "description": (
            "Queue an email draft for operator approval. "
            "The email will NOT be sent until the operator approves it. "
            "Email identity: from Nitin Puri, oakville@lsre.com.au"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string", "description": "Target lead UUID"},
                "subject": {"type": "string", "description": "Email subject line"},
                "body": {"type": "string", "description": "Email body (plain text)"},
            },
            "required": ["lead_id", "subject", "body"],
        },
    },
    {
        "name": "read_outreach_history",
        "description": "Read outreach history (campaigns sent/drafted) for a lead.",
        "input_schema": {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string", "description": "Target lead UUID"},
            },
            "required": ["lead_id"],
        },
    },
    {
        "name": "read_campaign_drafts",
        "description": "List campaign drafts by status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter: 'pending_approval', 'approved', 'all' (default 'pending_approval')",
                    "default": "pending_approval",
                },
            },
        },
    },
    {
        "name": "approve_campaign",
        "description": (
            "Approve a pending campaign draft (operator action). "
            "Returns updated campaign status."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "string", "description": "Campaign UUID to approve"},
                "note": {"type": "string", "description": "Approval note", "default": ""},
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "read_templates",
        "description": "List available outreach message templates.",
        "input_schema": {"type": "object", "properties": {}},
    },
]


async def execute(
    tool_name: str,
    params: dict[str, Any],
    session: AsyncSession,
    agent_id: str = "outreach_composer",
) -> str:
    """Execute a comms tool and return JSON string result."""
    try:
        fn = _EXECUTORS.get(tool_name)
        if not fn:
            return json.dumps({"error": f"Unknown comms tool: {tool_name}"})
        result = await fn(session, params, agent_id)
        return json.dumps(result, default=str)
    except Exception as exc:
        log.warning("[mcp_comms] Tool %s failed: %s", tool_name, exc)
        return json.dumps({"error": str(exc)})


async def _draft_sms(session: AsyncSession, params: dict, agent_id: str) -> Any:
    lead_id = params["lead_id"]
    message = params["message"][:160]  # Enforce SMS limit
    campaign_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    await session.execute(text("""
        INSERT INTO hermes_campaigns
            (id, campaign_type, audience, channel, stage, subject, message, goal,
             status, related_lead_id, created_at)
        VALUES
            (:id, 'direct_outreach', 'lead', 'sms', 'first_touch', :subject,
             :message, 'Contact lead', 'pending_approval', :lead_id, :now)
    """), {
        "id": campaign_id, "subject": f"SMS to lead {lead_id[:8]}",
        "message": message, "lead_id": lead_id, "now": now,
    })
    await session.commit()

    return {
        "campaign_id": campaign_id,
        "channel": "sms",
        "status": "pending_approval",
        "message_preview": message[:80],
        "note": "Queued for operator approval. Will NOT be sent until approved.",
    }


async def _draft_email(session: AsyncSession, params: dict, agent_id: str) -> Any:
    lead_id = params["lead_id"]
    subject = params["subject"][:200]
    body = params["body"][:2000]
    campaign_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    await session.execute(text("""
        INSERT INTO hermes_campaigns
            (id, campaign_type, audience, channel, stage, subject, message, goal,
             status, related_lead_id, created_at)
        VALUES
            (:id, 'direct_outreach', 'lead', 'email', 'first_touch', :subject,
             :message, 'Contact lead', 'pending_approval', :lead_id, :now)
    """), {
        "id": campaign_id, "subject": subject,
        "message": body, "lead_id": lead_id, "now": now,
    })
    await session.commit()

    return {
        "campaign_id": campaign_id,
        "channel": "email",
        "status": "pending_approval",
        "subject": subject,
        "note": "Queued for operator approval. Will NOT be sent until approved.",
    }


async def _read_outreach_history(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    lead_id = params["lead_id"]
    rows = (await session.execute(
        text("""
            SELECT id, campaign_type, channel, stage, subject, message,
                   status, created_at, sent_at
            FROM hermes_campaigns
            WHERE related_lead_id = :lid
            ORDER BY created_at DESC LIMIT 20
        """),
        {"lid": lead_id},
    )).mappings().all()
    return [dict(r) for r in rows]


async def _read_campaign_drafts(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    status = params.get("status", "pending_approval")
    if status == "all":
        q = """
            SELECT id, campaign_type, channel, stage, subject, status,
                   related_lead_id, created_at
            FROM hermes_campaigns ORDER BY created_at DESC LIMIT 30
        """
        rows = (await session.execute(text(q))).mappings().all()
    else:
        q = """
            SELECT id, campaign_type, channel, stage, subject, status,
                   related_lead_id, created_at
            FROM hermes_campaigns WHERE status = :status
            ORDER BY created_at DESC LIMIT 30
        """
        rows = (await session.execute(text(q), {"status": status})).mappings().all()
    return [dict(r) for r in rows]


async def _approve_campaign(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    campaign_id = params["campaign_id"]
    note = params.get("note", "")
    try:
        from hermes.controller import get_controller
        result = await get_controller().approve_campaign(
            session, campaign_id, approved_by="operator", note=note
        )
        return result
    except Exception as exc:
        return {"error": str(exc)}


async def _read_templates(session: AsyncSession, params: dict, _agent_id: str) -> Any:
    try:
        rows = (await session.execute(text("""
            SELECT id, content_type, audience, hook, status, created_at
            FROM hermes_content
            WHERE status = 'approved'
            ORDER BY created_at DESC LIMIT 20
        """))).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        return []


_EXECUTORS = {
    "draft_sms": _draft_sms,
    "draft_email": _draft_email,
    "read_outreach_history": _read_outreach_history,
    "read_campaign_drafts": _read_campaign_drafts,
    "approve_campaign": _approve_campaign,
    "read_templates": _read_templates,
}
