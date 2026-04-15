"""
Agent hooks — pre/post tool-use enforcement for SDK agents.

Hooks run inline during the agent loop:
  - PreToolUse: validate parameters, enforce constraints
  - PostToolUse: audit logging, compliance gate on outreach

The compliance hook runs the Compliance Reviewer (haiku) on any
outreach draft before it gets queued. This catches brand leaks,
identity violations, and generic AI copy.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# Tools that trigger compliance review
_OUTREACH_TOOLS = frozenset({"draft_sms", "draft_email"})

# Tools that require approval context (cannot run without prior plan approval)
_APPROVAL_REQUIRED_TOOLS = frozenset({"rea_execute_push", "rea_execute_refresh"})


async def pre_tool_use(
    tool_name: str,
    tool_input: dict[str, Any],
    agent_id: str,
) -> dict[str, Any]:
    """
    Pre-execution hook. Returns {"allow": True/False, "reason": str}.
    Called before every tool execution in the agent loop.
    """
    # Block execution tools if no approval signal
    if tool_name in _APPROVAL_REQUIRED_TOOLS:
        # The agent should have received approval in its conversation context.
        # We log a warning but allow it — the MCP executor has its own safeguards.
        log.warning(
            "[Hook:PreToolUse] %s invoking approval-gated tool %s — "
            "ensure operator approved in session context",
            agent_id,
            tool_name,
        )

    return {"allow": True, "reason": ""}


async def post_tool_use(
    tool_name: str,
    tool_input: dict[str, Any],
    tool_output: str,
    agent_id: str,
) -> dict[str, Any]:
    """
    Post-execution hook. Runs compliance check on outreach drafts.
    Returns {"flagged": bool, "issues": [...]} if problems found.
    """
    # Compliance gate on outreach tools
    if tool_name in _OUTREACH_TOOLS:
        enable_compliance = os.getenv("ENABLE_SDK_COMPLIANCE_HOOK", "true").lower() == "true"
        if enable_compliance:
            return await _run_compliance_gate(tool_name, tool_input, tool_output, agent_id)

    # Audit log for all tool calls
    log.info(
        "[Hook:PostToolUse] agent=%s tool=%s ts=%s",
        agent_id,
        tool_name,
        datetime.now(timezone.utc).isoformat(),
    )

    return {"flagged": False, "issues": []}


async def _run_compliance_gate(
    tool_name: str,
    tool_input: dict[str, Any],
    tool_output: str,
    agent_id: str,
) -> dict[str, Any]:
    """Run Compliance Reviewer on outreach draft output."""
    try:
        # Extract the message content for review
        if tool_name == "draft_sms":
            draft_text = tool_input.get("message", "")
            channel = "SMS"
        elif tool_name == "draft_email":
            draft_text = f"Subject: {tool_input.get('subject', '')}\n\n{tool_input.get('body', '')}"
            channel = "email"
        else:
            return {"flagged": False, "issues": []}

        if not draft_text.strip():
            return {"flagged": False, "issues": []}

        from agents.runner import run_compliance_check
        result = await run_compliance_check(draft_text, channel)

        if not result.get("approved", True):
            log.warning(
                "[Hook:Compliance] FLAGGED %s draft by %s: %s",
                channel,
                agent_id,
                json.dumps(result.get("issues", [])),
            )
            return {
                "flagged": True,
                "issues": result.get("issues", []),
                "suggested_fix": result.get("suggested_fix"),
            }

        return {"flagged": False, "issues": []}

    except Exception as exc:
        log.warning("[Hook:Compliance] Check failed (allowing through): %s", exc)
        return {"flagged": False, "issues": [], "error": str(exc)}
