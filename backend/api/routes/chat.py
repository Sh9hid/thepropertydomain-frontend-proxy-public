"""AI Chat API routes."""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from api.routes._deps import APIKeyDep, SessionDep

router = APIRouter()


class ChatMessageRequest(BaseModel):
    message: str
    history: Optional[List[dict]] = None
    session_id: Optional[str] = None


@router.post("/api/chat")
async def chat(req: ChatMessageRequest, session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.chat_service import handle_chat_message, save_chat_message
    if req.session_id:
        await save_chat_message(session, req.session_id, "user", req.message)
    result = await handle_chat_message(session, req.message, history=req.history)
    if req.session_id:
        await save_chat_message(session, req.session_id, "assistant", result["response"])
    return result


@router.get("/api/chat/sessions")
async def list_sessions(session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.chat_service import list_chat_sessions
    return await list_chat_sessions(session)


@router.get("/api/chat/sessions/{session_id}")
async def get_session(session_id: str, session: SessionDep = None, api_key: APIKeyDep = ""):
    from services.chat_service import get_chat_session
    result = await get_chat_session(session, session_id)
    if not result:
        return {"error": "Session not found"}
    return result


@router.post("/api/chat/sessions")
async def create_session(session: SessionDep = None, api_key: APIKeyDep = ""):
    """Create a new chat session."""
    import uuid
    from core.utils import now_iso
    from models.sql_models import ChatSession
    new_session = ChatSession(
        id=str(uuid.uuid4()),
        user_id="operator",
        title="New conversation",
        messages_json=[],
        created_at=now_iso(),
        updated_at=now_iso(),
    )
    session.add(new_session)
    await session.commit()
    return {"id": new_session.id, "title": new_session.title}
