"""
Chat Service — keyword-driven AI chat backend.

Provides a conversational interface for the operator to query leads,
get stats, draft messages, and access memories.  Uses keyword matching
(no LLM) for intent detection and query execution.
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from models.sql_models import (
    CallLog,
    ChatSession,
    Lead,
    LeadMemory,
    OutreachTemplate,
    ReaListingMetric,
    Task,
)
from services import lead_memory_service, template_library

logger = logging.getLogger(__name__)

# ─── Capability descriptions (shown in help) ─────────────────────────────────

_CAPABILITIES = (
    "I can help you with:\n"
    "  - \"Who should I call?\" — top leads by call_today_score\n"
    "  - \"How many leads / stats\" — counts and summaries\n"
    "  - \"Tell me about [address]\" — lead detail + memories\n"
    "  - \"Draft SMS/email for [address]\" — fill a template\n"
    "  - \"Listing stats\" — REA listing metrics summary\n"
    "\n"
    "Just ask in plain language."
)


# ─── Main handler ─────────────────────────────────────────────────────────────


async def handle_chat_message(
    session: AsyncSession,
    message: str,
    history: list[dict] = None,
    user_id: str = "operator",
) -> dict:
    """Main chat handler.  Parses the message for intent, executes queries,
    and returns a natural-language response.

    Returns: {"response": str, "tools_used": list[str]}
    """
    msg = (message or "").strip()
    if not msg:
        return {"response": "Send me a question and I'll look it up.", "tools_used": []}

    lower = msg.lower()

    # ── Intent: who to call / call list ──────────────────────────────
    if _matches_any(lower, ["who should i call", "who to call", "call list", "top leads"]):
        return await _handle_call_list(session)

    # ── Intent: listing / REA metrics ────────────────────────────────
    if _matches_any(lower, ["listing", "rea", "days on market", "listing stats"]):
        return await _handle_listing_stats(session)

    # ── Intent: draft / write message ────────────────────────────────
    if _matches_any(lower, ["draft", "write", "compose", "template"]):
        return await _handle_draft(session, msg)

    # ── Intent: stats / how many ─────────────────────────────────────
    if _matches_any(lower, ["how many", "stats", "count", "total", "summary"]):
        return await _handle_stats(session)

    # ── Intent: know about / address lookup ──────────────────────────
    if _matches_any(lower, ["know about", "tell me about", "details for", "info on"]):
        return await _handle_lead_lookup(session, msg)

    # Address-pattern detection (number + street name)
    address_match = re.search(r"\d+\s+[A-Za-z]+\s+(?:st|street|rd|road|ave|avenue|dr|drive|pl|place|ct|court|cr|cres|crescent|way|ln|lane)", lower)
    if address_match:
        return await _handle_lead_lookup(session, msg)

    # ── Default: help ────────────────────────────────────────────────
    return {"response": _CAPABILITIES, "tools_used": []}


# ─── Intent handlers ──────────────────────────────────────────────────────────


async def _handle_call_list(session: AsyncSession) -> dict:
    """Find top 3 leads by call_today_score and return briefs."""
    result = await session.execute(
        select(Lead)
        .where(Lead.status.notin_(["converted", "dropped"]))
        .order_by(Lead.call_today_score.desc())
        .limit(3)
    )
    leads = result.scalars().all()
    if not leads:
        return {"response": "No active leads found.", "tools_used": ["lead_search"]}

    lines: list[str] = ["Here are your top leads to call today:\n"]
    for i, lead in enumerate(leads, 1):
        name = lead.owner_name or lead.owner_first_name or "Unknown"
        phones = lead.contact_phones or []
        phone_str = phones[0] if phones else "no phone"
        why = lead.why_now or lead.trigger_type or "general outreach"
        lines.append(
            f"{i}. **{lead.address}** ({lead.suburb or '?'})\n"
            f"   Owner: {name} | Phone: {phone_str}\n"
            f"   Score: {lead.call_today_score} | Why: {why}\n"
        )
    return {"response": "\n".join(lines), "tools_used": ["lead_search"]}


async def _handle_listing_stats(session: AsyncSession) -> dict:
    """Query ReaListingMetric for summary stats."""
    try:
        count_result = await session.execute(
            select(func.count()).select_from(ReaListingMetric)
        )
        total = count_result.scalar() or 0

        if total == 0:
            return {
                "response": "No REA listing metrics recorded yet.",
                "tools_used": ["analytics"],
            }

        avg_views = await session.execute(
            select(func.avg(ReaListingMetric.views_7d)).select_from(ReaListingMetric)
        )
        avg_v = avg_views.scalar() or 0

        avg_days = await session.execute(
            select(func.avg(ReaListingMetric.days_listed)).select_from(ReaListingMetric)
        )
        avg_d = avg_days.scalar() or 0

        rotation_count = await session.execute(
            select(func.count())
            .select_from(ReaListingMetric)
            .where(ReaListingMetric.rotation_recommended == True)  # noqa: E712
        )
        rotations = rotation_count.scalar() or 0

        response = (
            f"REA Listing Metrics Summary:\n"
            f"  - Total snapshots: {total}\n"
            f"  - Avg 7-day views: {avg_v:.0f}\n"
            f"  - Avg days listed: {avg_d:.0f}\n"
            f"  - Rotation recommended: {rotations} listings\n"
        )
        return {"response": response, "tools_used": ["analytics"]}
    except Exception as exc:
        logger.warning("listing stats query failed: %s", exc)
        return {
            "response": "Could not retrieve listing stats — table may not be populated yet.",
            "tools_used": ["analytics"],
        }


async def _handle_draft(session: AsyncSession, msg: str) -> dict:
    """Select a template and fill it based on message context."""
    lower = msg.lower()

    # Detect channel
    channel = "sms" if "sms" in lower else "email"

    # Detect stage hint
    stage = "warm"
    if "hot" in lower:
        stage = "hot"
    elif "cold" in lower:
        stage = "cold"
    elif "nurture" in lower:
        stage = "nurture"
    elif "doorknock" in lower or "door" in lower:
        stage = "doorknock"

    # Try to find a lead from address in the message
    lead_dict: dict = {}
    address_match = re.search(
        r"\d+\s+[A-Za-z]+\s+(?:st|street|rd|road|ave|avenue|dr|drive|pl|place|ct|court|cr|cres|crescent|way|ln|lane)[^,]*",
        lower,
    )
    if address_match:
        addr_text = address_match.group(0).strip()
        lead_result = await session.execute(
            select(Lead).where(
                func.lower(Lead.address).contains(addr_text)
            ).limit(1)
        )
        lead_row = lead_result.scalars().first()
        if lead_row:
            lead_dict = _lead_to_chat_dict(lead_row)

    tpl = await template_library.select_best_template(
        session, channel=channel, stage=stage, lead=lead_dict,
    )
    if not tpl:
        return {
            "response": f"No {channel} template found for stage '{stage}'.",
            "tools_used": ["template"],
        }

    filled = template_library.fill_template(
        tpl["body"], lead_dict, subject=tpl.get("subject"),
    )
    parts = [f"**Template: {tpl['name']}** ({channel}, {stage})\n"]
    if filled.get("subject"):
        parts.append(f"Subject: {filled['subject']}\n")
    parts.append(f"\n{filled['body']}")

    return {"response": "\n".join(parts), "tools_used": ["template"]}


async def _handle_stats(session: AsyncSession) -> dict:
    """Count leads, tasks, call logs."""
    lead_count = (await session.execute(
        select(func.count()).select_from(Lead)
    )).scalar() or 0

    task_count = (await session.execute(
        select(func.count()).select_from(Task)
    )).scalar() or 0

    call_count = (await session.execute(
        select(func.count()).select_from(CallLog)
    )).scalar() or 0

    # Calls today
    today = now_iso()[:10]
    calls_today = 0
    try:
        calls_today_result = await session.execute(
            select(func.count())
            .select_from(CallLog)
            .where(CallLog.logged_date == today)
        )
        calls_today = calls_today_result.scalar() or 0
    except Exception:
        pass

    response = (
        f"Dashboard Stats:\n"
        f"  - Total leads: {lead_count}\n"
        f"  - Total tasks: {task_count}\n"
        f"  - Total calls logged: {call_count}\n"
        f"  - Calls today: {calls_today}\n"
    )
    return {"response": response, "tools_used": ["analytics"]}


async def _handle_lead_lookup(session: AsyncSession, msg: str) -> dict:
    """Search leads by name or address and return detail + memories."""
    # Extract the search term after common prefixes
    search = msg
    for prefix in ["know about", "tell me about", "details for", "info on", "about"]:
        idx = msg.lower().find(prefix)
        if idx >= 0:
            search = msg[idx + len(prefix):].strip()
            break

    # Strip leading/trailing punctuation
    search = search.strip("?!. ")
    if not search:
        return {"response": "What address or name should I look up?", "tools_used": []}

    # Search by address (primary) then owner_name
    result = await session.execute(
        select(Lead).where(
            func.lower(Lead.address).contains(search.lower())
        ).limit(3)
    )
    leads = list(result.scalars().all())

    if not leads:
        result = await session.execute(
            select(Lead).where(
                func.lower(Lead.owner_name).contains(search.lower())
            ).limit(3)
        )
        leads = list(result.scalars().all())

    if not leads:
        return {
            "response": f"No leads found matching \"{search}\".",
            "tools_used": ["lead_search"],
        }

    parts: list[str] = []
    tools = ["lead_search"]
    for lead in leads:
        phones = lead.contact_phones or []
        emails = lead.contact_emails or []
        detail = (
            f"**{lead.address}** ({lead.suburb or '?'})\n"
            f"  Owner: {lead.owner_name or 'Unknown'}\n"
            f"  Status: {lead.status} | Score: {lead.call_today_score}\n"
            f"  Trigger: {lead.trigger_type or 'none'}\n"
            f"  Phones: {', '.join(phones) if phones else 'none'}\n"
            f"  Emails: {', '.join(emails) if emails else 'none'}\n"
        )
        # Fetch memories
        memories = await lead_memory_service.get_memories(session, lead.id)
        if memories:
            tools.append("lead_memory") if "lead_memory" not in tools else None
            mem_lines = []
            for m in memories[:5]:
                mem_lines.append(f"    [{m['memory_type']}] {m['content']}")
            detail += "  Memories:\n" + "\n".join(mem_lines) + "\n"

        if lead.ai_context_summary:
            detail += f"  AI Summary: {lead.ai_context_summary}\n"

        parts.append(detail)

    return {"response": "\n".join(parts), "tools_used": tools}


# ─── Session management ──────────────────────────────────────────────────────


async def list_chat_sessions(
    session: AsyncSession, user_id: str = "operator"
) -> list[dict]:
    """List all chat sessions for a user, newest first."""
    result = await session.execute(
        select(ChatSession)
        .where(ChatSession.user_id == user_id)
        .order_by(ChatSession.updated_at.desc())
    )
    rows = result.scalars().all()
    return [
        {
            "id": r.id,
            "user_id": r.user_id,
            "title": r.title,
            "message_count": len(r.messages_json or []),
            "created_at": r.created_at,
            "updated_at": r.updated_at,
        }
        for r in rows
    ]


async def get_chat_session(
    session: AsyncSession, session_id: str
) -> dict | None:
    """Get a full chat session with messages."""
    result = await session.execute(
        select(ChatSession).where(ChatSession.id == session_id)
    )
    row = result.scalars().first()
    if not row:
        return None
    return {
        "id": row.id,
        "user_id": row.user_id,
        "title": row.title,
        "messages": row.messages_json or [],
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


async def save_chat_message(
    session: AsyncSession,
    session_id: str,
    role: str,
    content: str,
) -> None:
    """Append a message to a chat session.  Creates the session if it doesn't exist."""
    now = now_iso()

    result = await session.execute(
        select(ChatSession).where(ChatSession.id == session_id)
    )
    chat = result.scalars().first()

    if not chat:
        # Auto-create session
        chat = ChatSession(
            id=session_id,
            user_id="operator",
            title=content[:60] if role == "user" else "Chat",
            messages_json=[],
            created_at=now,
            updated_at=now,
        )
        session.add(chat)

    messages = list(chat.messages_json or [])
    messages.append({
        "role": role,
        "content": content,
        "timestamp": now,
    })
    chat.messages_json = messages
    chat.updated_at = now

    await session.commit()


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _matches_any(text: str, patterns: list[str]) -> bool:
    """Check if text contains any of the given keyword patterns."""
    return any(p in text for p in patterns)


def _lead_to_chat_dict(lead: Lead) -> dict:
    """Minimal lead dict for template filling in chat context."""
    return {
        "id": lead.id,
        "address": lead.address or "",
        "suburb": lead.suburb or "",
        "owner_name": lead.owner_name or "",
        "owner_first_name": lead.owner_first_name or "",
        "trigger_type": lead.trigger_type or "",
        "heat_score": lead.heat_score or 0,
        "call_today_score": lead.call_today_score or 0,
    }
