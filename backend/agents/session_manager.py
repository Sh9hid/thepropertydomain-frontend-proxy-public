"""
Session manager — create, resume, fork, and expire SDK agent sessions.

Sessions persist conversation context across multiple operator turns,
enabling multi-step workflows (daily calls, campaign builds, REA cycles).

Storage: in-memory with 24-hour TTL. Sessions are keyed by session_id.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

SESSION_TTL_HOURS = 24
MAX_SESSIONS = 50


@dataclass
class Message:
    role: str  # "user" or "assistant"
    content: Any  # str or list of content blocks


@dataclass
class AgentSession:
    session_id: str
    agent_id: str  # SDK agent that owns this session
    messages: list[dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    turn_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=SESSION_TTL_HOURS)
        return self.last_active < cutoff

    def add_user_message(self, content: str) -> None:
        self.messages.append({"role": "user", "content": content})
        self.last_active = datetime.now(timezone.utc)

    def add_assistant_message(self, content: Any) -> None:
        """Add assistant response (may be str or list of content blocks)."""
        self.messages.append({"role": "assistant", "content": content})
        self.last_active = datetime.now(timezone.utc)
        self.turn_count += 1

    def add_tool_result(self, tool_use_id: str, content: str, is_error: bool = False) -> None:
        """Add a tool result message."""
        self.messages.append({
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": content,
                    "is_error": is_error,
                }
            ],
        })
        self.last_active = datetime.now(timezone.utc)

    def record_tokens(self, input_tokens: int, output_tokens: int) -> None:
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "agent_id": self.agent_id,
            "turn_count": self.turn_count,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "created_at": self.created_at.isoformat(),
            "last_active": self.last_active.isoformat(),
            "message_count": len(self.messages),
        }


class SessionManager:
    """In-memory session store with TTL and fork support."""

    def __init__(self) -> None:
        self._sessions: dict[str, AgentSession] = {}

    def create(self, agent_id: str, session_id: str | None = None) -> AgentSession:
        self._evict_expired()
        sid = session_id or str(uuid.uuid4())
        session = AgentSession(session_id=sid, agent_id=agent_id)
        self._sessions[sid] = session
        log.info("[SessionMgr] Created session %s for agent %s", sid, agent_id)
        return session

    def get(self, session_id: str) -> AgentSession | None:
        session = self._sessions.get(session_id)
        if session and session.is_expired:
            log.info("[SessionMgr] Session %s expired — removing", session_id)
            del self._sessions[session_id]
            return None
        return session

    def get_or_create(self, session_id: str | None, agent_id: str) -> AgentSession:
        if session_id:
            existing = self.get(session_id)
            if existing:
                return existing
        return self.create(agent_id, session_id)

    def fork(self, session_id: str) -> AgentSession | None:
        """Create a copy of an existing session for exploring alternatives."""
        source = self.get(session_id)
        if not source:
            return None
        forked = AgentSession(
            session_id=str(uuid.uuid4()),
            agent_id=source.agent_id,
            messages=list(source.messages),
            metadata={**source.metadata, "forked_from": session_id},
        )
        self._sessions[forked.session_id] = forked
        log.info("[SessionMgr] Forked %s → %s", session_id, forked.session_id)
        return forked

    def delete(self, session_id: str) -> bool:
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False

    def list_sessions(self) -> list[dict[str, Any]]:
        self._evict_expired()
        return [s.to_dict() for s in self._sessions.values()]

    def _evict_expired(self) -> None:
        expired = [sid for sid, s in self._sessions.items() if s.is_expired]
        for sid in expired:
            del self._sessions[sid]
        if expired:
            log.info("[SessionMgr] Evicted %d expired sessions", len(expired))

        # Cap total sessions
        if len(self._sessions) > MAX_SESSIONS:
            oldest = sorted(
                self._sessions.items(),
                key=lambda kv: kv[1].last_active,
            )
            to_remove = oldest[: len(self._sessions) - MAX_SESSIONS]
            for sid, _ in to_remove:
                del self._sessions[sid]
            log.info("[SessionMgr] Evicted %d sessions (over cap)", len(to_remove))


# Module-level singleton
_manager: SessionManager | None = None


def get_session_manager() -> SessionManager:
    global _manager
    if _manager is None:
        _manager = SessionManager()
    return _manager
