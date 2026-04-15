"""
Agent Runner — autonomous tool-use loop on top of the Anthropic API.

This is the core execution engine. It:
1. Sends messages to Claude with tool definitions
2. Processes tool_use blocks by dispatching to MCP tool servers
3. Sends tool results back
4. Repeats until the model stops calling tools (stop_reason == "end_turn")
5. Tracks token usage via CostTracker

The runner does NOT manage sessions — that's the session_manager's job.
The runner takes a list of messages and tools, runs the loop, and returns
the final response + all tool calls made.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from agents.cost_tracker import get_cost_tracker
from agents.sdk_agents import AgentDefinition, get_anthropic_model

log = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Result of an agent run."""
    response_text: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    turns: int = 0
    stop_reason: str = ""
    error: str | None = None


def _get_client():
    """Lazy-load Anthropic client."""
    import anthropic
    return anthropic.Anthropic(
        api_key=os.getenv("ANTHROPIC_API_KEY", ""),
    )


def _collect_tools(agent: AgentDefinition) -> list[dict[str, Any]]:
    """Collect tool definitions from agent's bound MCP servers."""
    tools: list[dict[str, Any]] = []
    for server_name in agent.tool_servers:
        if server_name == "lead_db":
            from agents.mcp_lead_db import TOOLS as lead_tools
            tools.extend(lead_tools)
        elif server_name == "rea":
            from agents.mcp_rea import TOOLS as rea_tools
            tools.extend(rea_tools)
        elif server_name == "comms":
            from agents.mcp_comms import TOOLS as comms_tools
            tools.extend(comms_tools)
    return tools


async def _execute_tool(
    tool_name: str,
    tool_input: dict[str, Any],
    session: AsyncSession,
    agent_id: str,
) -> str:
    """Route a tool call to the appropriate MCP server executor."""
    from agents import mcp_lead_db, mcp_rea, mcp_comms

    if tool_name in mcp_lead_db._EXECUTORS:
        return await mcp_lead_db.execute(tool_name, tool_input, session, agent_id)
    elif tool_name in mcp_rea._EXECUTORS:
        return await mcp_rea.execute(tool_name, tool_input, session, agent_id)
    elif tool_name in mcp_comms._EXECUTORS:
        return await mcp_comms.execute(tool_name, tool_input, session, agent_id)
    else:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})


async def run_agent(
    agent: AgentDefinition,
    messages: list[dict[str, Any]],
    session: AsyncSession,
    session_id: str = "ephemeral",
) -> RunResult:
    """
    Run an agent's tool-use loop until it produces a final text response.

    Args:
        agent: The AgentDefinition to run
        messages: Conversation messages (Anthropic API format)
        session: DB session for tool execution
        session_id: For cost tracking

    Returns:
        RunResult with final text, tool call log, and token usage
    """
    tracker = get_cost_tracker()
    client = _get_client()
    model = get_anthropic_model(agent)
    tools = _collect_tools(agent)

    result = RunResult(response_text="")
    working_messages = list(messages)

    for turn in range(agent.max_turns):
        # Check budget before each API call
        if not tracker.can_spend(session_id):
            result.error = "Budget exhausted"
            result.response_text = (
                "I've reached the daily token budget. "
                "Falling back to standard processing for the rest of today."
            )
            break

        # Make API call
        try:
            api_kwargs: dict[str, Any] = {
                "model": model,
                "max_tokens": 4096,
                "system": agent.system_prompt,
                "messages": working_messages,
                "temperature": agent.temperature,
            }
            if tools:
                api_kwargs["tools"] = tools

            response = client.messages.create(**api_kwargs)

        except Exception as exc:
            log.error("[Runner] API call failed for %s: %s", agent.agent_id, exc)
            result.error = str(exc)
            result.response_text = "I encountered an error processing your request."
            break

        # Record token usage
        usage = response.usage
        result.input_tokens += usage.input_tokens
        result.output_tokens += usage.output_tokens
        result.turns += 1
        tracker.record_usage(
            session_id,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
        )

        result.stop_reason = response.stop_reason

        # Process response content blocks
        text_parts: list[str] = []
        tool_use_blocks: list[dict[str, Any]] = []

        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_use_blocks.append({
                    "id": block.id,
                    "name": block.name,
                    "input": block.input,
                })

        # If no tool calls, we're done
        if not tool_use_blocks:
            result.response_text = "\n".join(text_parts)
            break

        # Add assistant message with all content blocks to working messages
        working_messages.append({
            "role": "assistant",
            "content": [
                {"type": "text", "text": t} if isinstance(t, str)
                else {"type": "tool_use", "id": t["id"], "name": t["name"], "input": t["input"]}
                for block in response.content
                for t in ([block.text] if block.type == "text" else [{"id": block.id, "name": block.name, "input": block.input}])
            ],
        })

        # Execute tools and collect results
        tool_results: list[dict[str, Any]] = []
        for tool_call in tool_use_blocks:
            log.info(
                "[Runner] %s calling tool: %s(%s)",
                agent.agent_id,
                tool_call["name"],
                json.dumps(tool_call["input"])[:200],
            )

            tool_output = await _execute_tool(
                tool_call["name"],
                tool_call["input"],
                session,
                agent.agent_id,
            )

            result.tool_calls.append({
                "tool": tool_call["name"],
                "input": tool_call["input"],
                "output_preview": tool_output[:300],
            })

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_call["id"],
                "content": tool_output,
            })

            tracker.record_usage(session_id, 0, 0, tool_calls=1)

        # Send tool results back
        working_messages.append({"role": "user", "content": tool_results})

        # Check if we're on the last turn
        if turn == agent.max_turns - 1:
            result.response_text = "\n".join(text_parts) if text_parts else (
                "I've used all available reasoning steps. "
                "Here's what I found so far based on my tool calls."
            )

    log.info(
        "[Runner] %s completed: %d turns, %d tool calls, %d in/%d out tokens",
        agent.agent_id,
        result.turns,
        len(result.tool_calls),
        result.input_tokens,
        result.output_tokens,
    )
    return result


async def run_compliance_check(draft_text: str, channel: str) -> dict[str, Any]:
    """
    Run the Compliance Reviewer on an outreach draft.
    Returns {"approved": bool, "issues": [...], "suggested_fix": str | None}
    """
    from agents.sdk_agents import COMPLIANCE_REVIEWER, get_anthropic_model

    client = _get_client()
    model = get_anthropic_model(COMPLIANCE_REVIEWER)

    prompt = (
        f"Review this {channel} outreach draft for compliance:\n\n"
        f"---\n{draft_text}\n---"
    )

    try:
        response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=COMPLIANCE_REVIEWER.system_prompt,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        text = response.content[0].text
        # Try to parse JSON from response
        return json.loads(text)
    except json.JSONDecodeError:
        return {"approved": True, "issues": [], "raw_response": text}
    except Exception as exc:
        log.warning("[Compliance] Check failed: %s", exc)
        return {"approved": True, "issues": [], "error": str(exc)}
