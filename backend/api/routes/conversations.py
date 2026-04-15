"""Conversations API."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.database import get_session
from models.org_models import Conversation, ConversationMessage
from services.conversation_service import (
    start_conversation, add_message, resolve_conversation,
    get_conversations_for_ticket, get_messages,
)

router = APIRouter(prefix="/conversations", tags=["conversations"])


def _conv_dict(c: Conversation) -> Dict:
    return {
        "id": c.id,
        "ticket_id": c.ticket_id,
        "topic": c.topic,
        "status": c.status,
        "started_by_agent_id": c.started_by_agent_id,
        "max_rounds": c.max_rounds,
        "rounds_completed": c.rounds_completed,
        "final_decision": c.final_decision,
        "action_plan": c.action_plan,
        "summary": c.summary,
        "participants": c.participants,
        "created_at": c.created_at.isoformat(),
        "updated_at": c.updated_at.isoformat(),
    }


def _msg_dict(m: ConversationMessage) -> Dict:
    return {
        "id": m.id,
        "conversation_id": m.conversation_id,
        "agent_id": m.agent_id,
        "message_type": m.message_type,
        "content": m.content,
        "evidence_json": m.evidence_json,
        "created_at": m.created_at.isoformat(),
    }


class StartConvRequest(BaseModel):
    ticket_id: str
    topic: str
    started_by_agent_id: str
    participants: List[str]
    max_rounds: int = 6


class AddMessageRequest(BaseModel):
    agent_id: str
    message_type: str
    content: str
    evidence_json: Dict[str, Any] = {}


class ResolveRequest(BaseModel):
    final_decision: str
    action_plan: str


@router.post("/start")
async def post_start(body: StartConvRequest, session: AsyncSession = Depends(get_session)):
    conv = await start_conversation(
        session, body.ticket_id, body.topic,
        body.started_by_agent_id, body.participants, body.max_rounds,
    )
    return _conv_dict(conv)


@router.get("/ticket/{ticket_id}")
async def get_by_ticket(ticket_id: str, session: AsyncSession = Depends(get_session)):
    convs = await get_conversations_for_ticket(session, ticket_id)
    return [_conv_dict(c) for c in convs]


@router.get("/id/{conversation_id}")
async def get_by_id(conversation_id: str, session: AsyncSession = Depends(get_session)):
    r = await session.execute(select(Conversation).where(Conversation.id == conversation_id))
    c = r.scalars().first()
    if not c:
        raise HTTPException(404, "Conversation not found")
    msgs = await get_messages(session, conversation_id)
    return {**_conv_dict(c), "messages": [_msg_dict(m) for m in msgs]}


@router.post("/{conversation_id}/messages")
async def post_message(
    conversation_id: str,
    body: AddMessageRequest,
    session: AsyncSession = Depends(get_session),
):
    msg = await add_message(
        session, conversation_id, body.agent_id,
        body.message_type, body.content, body.evidence_json,
    )
    return _msg_dict(msg)


@router.post("/{conversation_id}/resolve")
async def post_resolve(
    conversation_id: str,
    body: ResolveRequest,
    session: AsyncSession = Depends(get_session),
):
    conv = await resolve_conversation(session, conversation_id, body.final_decision, body.action_plan)
    if not conv:
        raise HTTPException(404, "Conversation not found")
    return _conv_dict(conv)
