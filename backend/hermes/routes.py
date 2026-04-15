from __future__ import annotations

import uuid
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from core.security import get_api_key
from core.utils import now_iso
from hermes.controller import get_controller
from hermes.departments import get_agent_for_query, get_department
from hermes.schemas import (
    HermesApproveCampaignRequest,
    HermesApproveContentRequest,
    HermesCommandRequest,
    HermesSourceCreateRequest,
    HermesSourcePatchRequest,
    HermesSyncSourcesRequest,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/hermes", tags=["hermes"])


class HermesChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    agent_id: Optional[str] = None  # force a specific agent; None = auto-route


class HermesChatResponse(BaseModel):
    session_id: str
    agent_id: Optional[str]
    agent_name: str
    response: str
    message_id: str


@router.post("/chat", dependencies=[Depends(get_api_key)])
async def hermes_chat(body: HermesChatRequest, session: AsyncSession = Depends(get_session)) -> HermesChatResponse:
    """
    Send a message to HERMES. Auto-routes to the best-fit agent or uses a general brain.
    When SDK agents are enabled, uses multi-turn sessions with tool access.
    Stores conversation history. Returns the agent response.
    """
    from hermes.models import ensure_hermes_schema

    await ensure_hermes_schema()

    session_id = body.session_id or str(uuid.uuid4())
    user_msg_id = str(uuid.uuid4())
    agent_msg_id = str(uuid.uuid4())

    # ── Try SDK agent path first (multi-turn with tool access) ──
    try:
        from agents.bridge import sdk_agents_enabled, run_sdk_chat
        if sdk_agents_enabled():
            log.info("[HermesChat] Routing through SDK agent session")
            sdk_result = await run_sdk_chat(
                message=body.message,
                session=session,
                session_id=body.session_id,
                agent_id=body.agent_id,
            )
            response_text = sdk_result.get("response", "")
            if sdk_result.get("error"):
                raise RuntimeError(sdk_result["error"])
            if not response_text:
                raise RuntimeError("SDK returned empty response")

            # Store conversation in hermes_chat for history
            sdk_session_id = sdk_result.get("session_id", session_id)
            sdk_agent_id = sdk_result.get("agent_id", "supervisor")
            sdk_agent_name = sdk_result.get("agent_name", "HERMES")
            try:
                await session.execute(text("""
                    INSERT INTO hermes_chat (id, session_id, role, agent_id, agent_name, message, created_at)
                    VALUES (:id, :session_id, 'user', NULL, NULL, :message, :now)
                """), {"id": user_msg_id, "session_id": sdk_session_id, "message": body.message, "now": now_iso()})
                await session.execute(text("""
                    INSERT INTO hermes_chat (id, session_id, role, agent_id, agent_name, message, created_at)
                    VALUES (:id, :session_id, 'agent', :agent_id, :agent_name, :message, :now)
                """), {
                    "id": agent_msg_id, "session_id": sdk_session_id,
                    "agent_id": sdk_agent_id, "agent_name": sdk_agent_name,
                    "message": response_text, "now": now_iso(),
                })
                await session.commit()
            except Exception as exc:
                log.warning("[HermesChat] Failed to store SDK chat messages: %s", exc)

            return HermesChatResponse(
                session_id=sdk_session_id,
                agent_id=sdk_agent_id,
                agent_name=sdk_agent_name,
                response=response_text,
                message_id=agent_msg_id,
            )
    except ImportError:
        pass  # agents package not available
    except Exception as sdk_exc:
        log.warning("[HermesChat] SDK chat failed, falling back to ai_ask: %s", sdk_exc)

    # ── Fallback: single-shot ai_ask path ──
    from services.ai_router import ask as ai_ask

    # Determine which agent handles this
    agent_id = body.agent_id or get_agent_for_query(body.message)
    dept = get_department(agent_id) if agent_id else None
    agent_name = dept["name"] if dept else "HERMES"
    persona = dept["persona"] if dept else (
        "You are HERMES, the intelligence director for Laing+Simmons Oakville | Windsor "
        "and the Propella proptech platform. You have 30 specialist agents under your command. "
        "You help the operator (Shahid) make better decisions about leads, outreach, market signals, "
        "and business strategy. You are direct, data-driven, and never generic. "
        "If a question is about a specific domain (leads, mortgage, software), "
        "route the answer through the relevant expert framing."
    )

    # Load recent chat history for context
    try:
        history_rows = (await session.execute(text("""
            SELECT role, agent_name, message FROM hermes_chat
            WHERE session_id = :sid
            ORDER BY created_at DESC LIMIT 10
        """), {"sid": session_id})).mappings().all()
        history = list(reversed(history_rows))
    except Exception:
        history = []

    history_text = ""
    if history:
        history_text = "\n\nRecent conversation:\n" + "\n".join(
            f"{'User' if r['role'] == 'user' else r['agent_name'] or 'HERMES'}: {r['message']}"
            for r in history[-6:]
        )

    full_prompt = f"{body.message}{history_text}"

    # Store user message
    try:
        await session.execute(text("""
            INSERT INTO hermes_chat (id, session_id, role, agent_id, agent_name, message, created_at)
            VALUES (:id, :session_id, 'user', NULL, NULL, :message, :now)
        """), {"id": user_msg_id, "session_id": session_id, "message": body.message, "now": now_iso()})
        await session.commit()
    except Exception as exc:
        log.warning(f"[HermesChat] Failed to store user message: {exc}")

    # Call AI
    try:
        response_text = await ai_ask(
            task="operator_brief",
            prompt=full_prompt,
            system_override=persona,
        )
    except Exception as exc:
        log.warning(f"[HermesChat] AI call failed: {exc}")
        response_text = "I'm unable to process that request right now. Please try again."

    if not response_text:
        response_text = "No response generated. Check AI provider configuration."

    # Store agent response
    try:
        await session.execute(text("""
            INSERT INTO hermes_chat (id, session_id, role, agent_id, agent_name, message, created_at)
            VALUES (:id, :session_id, 'agent', :agent_id, :agent_name, :message, :now)
        """), {
            "id": agent_msg_id,
            "session_id": session_id,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "message": response_text,
            "now": now_iso(),
        })
        await session.commit()
    except Exception as exc:
        log.warning(f"[HermesChat] Failed to store agent response: {exc}")

    return HermesChatResponse(
        session_id=session_id,
        agent_id=agent_id,
        agent_name=agent_name,
        response=response_text,
        message_id=agent_msg_id,
    )


@router.get("/chat/history", dependencies=[Depends(get_api_key)])
async def hermes_chat_history(
    session_id: str,
    limit: int = 50,
    session: AsyncSession = Depends(get_session),
):
    """Get chat history for a session."""
    from hermes.models import ensure_hermes_schema
    await ensure_hermes_schema()
    try:
        rows = (await session.execute(text("""
            SELECT id, session_id, role, agent_id, agent_name, message, created_at
            FROM hermes_chat
            WHERE session_id = :sid
            ORDER BY created_at ASC
            LIMIT :limit
        """), {"sid": session_id, "limit": limit})).mappings().all()
        return {"messages": [dict(r) for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/feed", dependencies=[Depends(get_api_key)])
async def get_hermes_feed(session: AsyncSession = Depends(get_session)):
    return await get_controller().get_feed(session)


@router.get("/activity", dependencies=[Depends(get_api_key)])
async def get_hermes_activity(session: AsyncSession = Depends(get_session)):
    return await get_controller().get_activity(session)


@router.get("/memory", dependencies=[Depends(get_api_key)])
async def get_hermes_memory(session: AsyncSession = Depends(get_session)):
    return await get_controller().get_memory(session)


@router.post("/command", dependencies=[Depends(get_api_key)])
async def post_hermes_command(body: HermesCommandRequest, session: AsyncSession = Depends(get_session)):
    try:
        return await get_controller().run_command(session, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/approve-content", dependencies=[Depends(get_api_key)])
async def approve_hermes_content(body: HermesApproveContentRequest, session: AsyncSession = Depends(get_session)):
    try:
        return await get_controller().approve_content(session, body.content_id, body.approved_by, body.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/approve-campaign", dependencies=[Depends(get_api_key)])
async def approve_hermes_campaign(body: HermesApproveCampaignRequest, session: AsyncSession = Depends(get_session)):
    try:
        return await get_controller().approve_campaign(session, body.campaign_id, body.approved_by, body.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/sources/sync", dependencies=[Depends(get_api_key)])
async def sync_hermes_sources(body: HermesSyncSourcesRequest, session: AsyncSession = Depends(get_session)):
    return await get_controller().sync_sources(session, source_ids=body.source_ids, force=body.force)


@router.get("/sources", dependencies=[Depends(get_api_key)])
async def get_hermes_sources(session: AsyncSession = Depends(get_session)):
    return await get_controller().list_sources(session)


@router.post("/sources", dependencies=[Depends(get_api_key)])
async def create_hermes_source(body: HermesSourceCreateRequest, session: AsyncSession = Depends(get_session)):
    return await get_controller().create_source(session, body)


@router.patch("/sources/{source_id}", dependencies=[Depends(get_api_key)])
async def patch_hermes_source(source_id: str, body: HermesSourcePatchRequest, session: AsyncSession = Depends(get_session)):
    try:
        return await get_controller().patch_source(session, source_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
