"""
Conversation service — persistent agent-to-agent discussion threads.

Design rules:
- Bounded rounds only (max_rounds default 6)
- At resolution: stores final_decision + action_plan + compact summary
- Prompts use rolling compact summary, NOT full message replay
- Every message write emits a WebSocket event
- Degradation: if LLM fails, message is stored with "LLM_UNAVAILABLE" content
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.events import event_manager
from models.org_models import Conversation, ConversationMessage, ConvStatus
from services.orchestration_engine import route_completion

log = logging.getLogger(__name__)

MAX_SUMMARY_CHARS = 1200   # rolling summary cap injected into prompts
MAX_CONTENT_STORE = 4000   # max chars stored per message


async def _emit(event_type: str, data: Dict):
    await event_manager.broadcast({"type": event_type, "data": {**data, "ts": datetime.utcnow().isoformat()}})


# ─── Start ────────────────────────────────────────────────────────────────────

async def start_conversation(
    session: AsyncSession,
    ticket_id: str,
    topic: str,
    started_by_agent_id: str,
    participants: List[str],
    max_rounds: int = 6,
) -> Conversation:
    conv = Conversation(
        ticket_id=ticket_id,
        topic=topic,
        status=ConvStatus.OPEN,
        started_by_agent_id=started_by_agent_id,
        participants=participants,
        max_rounds=max_rounds,
    )
    session.add(conv)
    await session.commit()
    await session.refresh(conv)
    await _emit("CONV_STARTED", {
        "conversation_id": conv.id,
        "ticket_id": ticket_id,
        "topic": topic,
        "participants": participants,
    })
    return conv


# ─── Add message ──────────────────────────────────────────────────────────────

async def add_message(
    session: AsyncSession,
    conversation_id: str,
    agent_id: str,
    message_type: str,
    content: str,
    evidence_json: Optional[Dict] = None,
) -> ConversationMessage:
    msg = ConversationMessage(
        conversation_id=conversation_id,
        agent_id=agent_id,
        message_type=message_type,
        content=content[:MAX_CONTENT_STORE],
        evidence_json=evidence_json or {},
    )
    session.add(msg)
    # bump conversation updated_at
    r = await session.execute(select(Conversation).where(Conversation.id == conversation_id))
    conv = r.scalars().first()
    if conv:
        conv.rounds_completed += 1
        conv.updated_at = datetime.utcnow()
    await session.commit()
    await session.refresh(msg)
    await _emit("CONV_MESSAGE", {
        "conversation_id": conversation_id,
        "agent_id": agent_id,
        "message_type": message_type,
        "content_preview": content[:120],
    })
    return msg


# ─── Generate agent response ──────────────────────────────────────────────────

async def generate_agent_turn(
    session: AsyncSession,
    conversation_id: str,
    responding_agent_role: str,
    responding_agent_system_prompt: str,
    ticket_context: Dict[str, Any],
) -> Optional[str]:
    """
    Fetch the rolling summary of the conversation, then generate a response.
    Does NOT replay full message history — uses compact summary only.
    """
    r = await session.execute(select(Conversation).where(Conversation.id == conversation_id))
    conv = r.scalars().first()
    if not conv:
        return None

    # Get last N messages for context (not all — keep tokens low)
    msgs_q = await session.execute(
        select(ConversationMessage)
        .where(ConversationMessage.conversation_id == conversation_id)
        .order_by(ConversationMessage.created_at.desc())
        .limit(4)  # last 4 messages only
    )
    recent_msgs = list(reversed(msgs_q.scalars().all()))

    # Build compact context
    msg_lines = "\n".join(
        f"[{m.agent_id} / {m.message_type}]: {m.content[:400]}"
        for m in recent_msgs
    )

    user_prompt = f"""## Conversation Topic
{conv.topic}

## Ticket Context
{json.dumps(ticket_context, indent=2)[:1200]}

## Recent Discussion (last 4 messages)
{msg_lines or '(no prior messages)'}

## Your Role
{responding_agent_role}

## Instructions
Respond to the discussion above. Be specific and grounded. Reference evidence where possible.
Stay within scope — no speculation without data.
Return a concise response (200 words max).
If you have reached a clear decision, start your response with DECISION:"""

    messages = [
        {"role": "system", "content": responding_agent_system_prompt[:600]},
        {"role": "user", "content": user_prompt},
    ]

    try:
        result = await route_completion(
            work_type="summarization",
            messages=messages,
            max_tokens=512,
            job_id=None,
            task_id=conversation_id,
        )
        return result.text
    except Exception as exc:
        log.warning("[conv] LLM call failed for agent %s: %s", responding_agent_role, exc)
        return f"[LLM_UNAVAILABLE: {str(exc)[:100]}]"


# ─── Resolve ──────────────────────────────────────────────────────────────────

async def resolve_conversation(
    session: AsyncSession,
    conversation_id: str,
    final_decision: str,
    action_plan: str,
) -> Optional[Conversation]:
    r = await session.execute(select(Conversation).where(Conversation.id == conversation_id))
    conv = r.scalars().first()
    if not conv:
        return None

    # Generate compact summary from all messages
    msgs_q = await session.execute(
        select(ConversationMessage)
        .where(ConversationMessage.conversation_id == conversation_id)
        .order_by(ConversationMessage.created_at)
    )
    all_msgs = msgs_q.scalars().all()

    raw_summary = "\n".join(
        f"[{m.agent_id}] {m.content[:200]}"
        for m in all_msgs
    )[:MAX_SUMMARY_CHARS]

    conv.status = ConvStatus.RESOLVED
    conv.final_decision = final_decision[:2000]
    conv.action_plan = action_plan[:2000]
    conv.summary = raw_summary
    conv.updated_at = datetime.utcnow()
    await session.commit()
    await session.refresh(conv)

    await _emit("CONV_RESOLVED", {
        "conversation_id": conversation_id,
        "ticket_id": conv.ticket_id,
        "final_decision": final_decision[:200],
    })
    return conv


# ─── Read helpers ─────────────────────────────────────────────────────────────

async def get_conversations_for_ticket(
    session: AsyncSession,
    ticket_id: str,
) -> List[Conversation]:
    q = select(Conversation).where(Conversation.ticket_id == ticket_id).order_by(Conversation.created_at)
    return (await session.execute(q)).scalars().all()


async def get_messages(
    session: AsyncSession,
    conversation_id: str,
    limit: int = 50,
) -> List[ConversationMessage]:
    q = (
        select(ConversationMessage)
        .where(ConversationMessage.conversation_id == conversation_id)
        .order_by(ConversationMessage.created_at)
        .limit(limit)
    )
    return (await session.execute(q)).scalars().all()


# ─── Auto-conversation for complex tickets ────────────────────────────────────

async def maybe_auto_converse(
    session: AsyncSession,
    ticket: Any,
    agent_defs: Dict[str, Any],
) -> Optional[Conversation]:
    """
    For HIGH/CRITICAL tickets with enough evidence, automatically kick off a
    bounded 2-agent conversation (planner + reviewer) to produce an action plan.
    """
    from models.org_models import TicketSeverity
    if ticket.severity not in (TicketSeverity.HIGH, TicketSeverity.CRITICAL):
        return None
    if ticket.kind not in ("bug", "feature", "anomaly"):
        return None

    planner = agent_defs.get("planner")
    reviewer = agent_defs.get("reviewer")
    if not planner or not reviewer:
        return None

    conv = await start_conversation(
        session,
        ticket_id=ticket.id,
        topic=f"Resolution plan: {ticket.title}",
        started_by_agent_id="planner",
        participants=["planner", "reviewer"],
        max_rounds=4,
    )

    ticket_ctx = {
        "title": ticket.title,
        "department": ticket.department,
        "kind": ticket.kind,
        "severity": ticket.severity,
        "description": (ticket.description or "")[:600],
        "evidence": ticket.evidence_json,
    }

    # Round 1 — planner proposes
    plan_response = await generate_agent_turn(
        session, conv.id, "planner", planner.system_prompt, ticket_ctx
    )
    if plan_response:
        await add_message(session, conv.id, "planner", "proposal", plan_response)

    # Round 2 — reviewer critiques / approves
    review_response = await generate_agent_turn(
        session, conv.id, "reviewer", reviewer.system_prompt, ticket_ctx
    )
    if review_response:
        await add_message(session, conv.id, "reviewer", "critique", review_response)

    # Resolve with the reviewer's output as decision
    decision = (review_response or "No decision reached")[:500]
    action = (plan_response or "No plan generated")[:500]
    if "DECISION:" in decision:
        decision = decision.split("DECISION:", 1)[1].strip()

    await resolve_conversation(session, conv.id, decision, action)
    return conv
