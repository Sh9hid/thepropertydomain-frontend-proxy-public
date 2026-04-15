"""
Bridge — connects department_runner to SDK agent loop for Tier 1 departments.

When ENABLE_SDK_AGENTS=true and a department is in TIER1_AGENT_IDS,
the bridge routes the department cycle through the SDK agent runner
instead of the single-shot ai_ask() path.

Feature-flagged with try/except fallback to ai_ask() on any SDK failure.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

from sqlalchemy.ext.asyncio import AsyncSession

from agents import is_tier1_agent

log = logging.getLogger(__name__)


def sdk_agents_enabled() -> bool:
    """Check if SDK agent integration is enabled."""
    return os.getenv("ENABLE_SDK_AGENTS", "false").lower() == "true"


def should_use_sdk(dept_id: str) -> bool:
    """Check if a department should use the SDK agent loop."""
    return sdk_agents_enabled() and is_tier1_agent(dept_id)


async def run_department_via_sdk(
    dept_id: str,
    session: AsyncSession,
    extra_context: str = "",
) -> Dict[str, Any]:
    """
    Run a Tier 1 department cycle through the SDK agent runner.

    Instead of single-shot ai_ask(), this creates a session and runs
    the agent with full tool access so it can query the DB,
    reason over results, and produce evidence-backed output.

    Returns the same shape as department_runner.run_department_cycle().
    """
    from datetime import datetime, timezone

    from agents.sdk_agents import get_sdk_agent_for_dept
    from agents.runner import run_agent
    from hermes.departments import get_department

    dept = get_department(dept_id)
    if not dept:
        return {"error": f"Department '{dept_id}' not found"}

    agent_def = get_sdk_agent_for_dept(dept_id)
    if not agent_def:
        return {"error": f"No SDK agent mapped for '{dept_id}'"}

    # Build the cycle prompt as the user message
    cycle_prompt = dept["cycle_prompt"]
    user_message = cycle_prompt
    if extra_context:
        user_message += f"\n\nAdditional context:\n{extra_context}"

    messages = [{"role": "user", "content": user_message}]

    log.info("[Bridge] Running %s via SDK agent %s", dept_id, agent_def.agent_id)

    result = await run_agent(
        agent=agent_def,
        messages=messages,
        session=session,
        session_id=f"dept_cycle_{dept_id}",
    )

    if result.error:
        log.warning("[Bridge] SDK agent %s failed: %s", agent_def.agent_id, result.error)
        raise RuntimeError(result.error)

    # Store the output as a hermes finding (same as department_runner does)
    from hermes.department_runner import _store_department_finding
    finding = await _store_department_finding(dept_id, dept, result.response_text, session)

    return {
        "dept_id": dept_id,
        "name": dept["name"],
        "workspace": dept["workspace"],
        "status": "complete",
        "output_preview": result.response_text[:400],
        "finding_id": finding.get("id") if finding else None,
        "run_at": datetime.now(timezone.utc).isoformat(),
        "sdk_agent": agent_def.agent_id,
        "sdk_turns": result.turns,
        "sdk_tool_calls": len(result.tool_calls),
        "sdk_tokens": result.input_tokens + result.output_tokens,
    }


async def run_sdk_chat(
    message: str,
    session: AsyncSession,
    session_id: str | None = None,
    agent_id: str | None = None,
) -> Dict[str, Any]:
    """
    Run an SDK-powered chat session. Multi-turn with tool access.

    This replaces the single-shot ai_ask() in hermes_chat when SDK is enabled.

    Returns:
        {
            "session_id": str,
            "agent_id": str,
            "agent_name": str,
            "response": str,
            "tool_calls": [...],
            "tokens": int,
        }
    """
    from agents.sdk_agents import SUPERVISOR, get_sdk_agent, AGENT_REGISTRY
    from agents.session_manager import get_session_manager
    from agents.runner import run_agent

    mgr = get_session_manager()

    # Determine which agent to use
    if agent_id and agent_id in AGENT_REGISTRY:
        agent_def = AGENT_REGISTRY[agent_id]
    else:
        # Default to supervisor for routing
        agent_def = SUPERVISOR

    # Get or create session
    agent_session = mgr.get_or_create(session_id, agent_def.agent_id)

    # Add user message to session
    agent_session.add_user_message(message)

    # Run agent with full message history
    result = await run_agent(
        agent=agent_def,
        messages=agent_session.messages,
        session=session,
        session_id=agent_session.session_id,
    )

    # Record response in session
    agent_session.add_assistant_message(result.response_text)
    agent_session.record_tokens(result.input_tokens, result.output_tokens)

    return {
        "session_id": agent_session.session_id,
        "agent_id": agent_def.agent_id,
        "agent_name": agent_def.name,
        "response": result.response_text,
        "tool_calls": result.tool_calls,
        "tokens": result.input_tokens + result.output_tokens,
        "turns": result.turns,
        "error": result.error,
    }
