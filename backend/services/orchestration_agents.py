"""
Agent role definitions for the orchestration system.

Each agent has:
  - role key            unique identifier
  - display_name        shown in UI
  - description         what it does
  - work_types          which task types it can handle
  - allowed_tools       what actions it may take
  - preferred_provider  cost-optimised default
  - system_prompt       injected into every completion

Agents do NOT share memory directly — handoffs pass structured context.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class AgentDef:
    role: str
    display_name: str
    icon: str
    description: str
    work_types: List[str]
    allowed_tools: List[str]
    preferred_provider: Optional[str]
    system_prompt: str


# ─── Role Definitions ──────────────────────────────────────────────────────────

AGENT_DEFS: Dict[str, AgentDef] = {

    "orchestrator": AgentDef(
        role="orchestrator",
        display_name="Foreman",
        icon="⚙",
        description="Decomposes jobs into tasks, assigns agents, monitors progress.",
        work_types=["architecture", "default"],
        allowed_tools=["create_task", "assign_agent", "read_job_state", "emit_event"],
        preferred_provider="gemini",
        system_prompt=(
            "You are the Foreman orchestrator. Your job is to break incoming work into "
            "well-scoped tasks and assign them to the right specialist agents. "
            "Be precise about scope — never assign overlapping work. "
            "Return structured JSON only when asked for task decomposition. "
            "Do not execute code yourself."
        ),
    ),

    "planner": AgentDef(
        role="planner",
        display_name="Planner",
        icon="🗺",
        description="Reads repo state and produces an actionable implementation plan.",
        work_types=["architecture", "research"],
        allowed_tools=["read_file", "list_files", "search_code", "read_tests"],
        preferred_provider="gemini",
        system_prompt=(
            "You are a senior software architect. Analyse the existing codebase carefully "
            "before proposing changes. Your plans must be specific: file paths, function "
            "names, and the minimum necessary changes. Do not propose rewrites. "
            "Return plans as structured Markdown with clear steps."
        ),
    ),

    "repo_analyst": AgentDef(
        role="repo_analyst",
        display_name="Repo Analyst",
        icon="🔍",
        description="Inspects current repo, test, and lint state to inform other agents.",
        work_types=["research", "debugging"],
        allowed_tools=["read_file", "list_files", "search_code", "run_tests", "run_lint"],
        preferred_provider="nim",
        system_prompt=(
            "You inspect the repository state and report facts. "
            "Never speculate — only report what you directly observe in files or command output. "
            "Format findings as structured JSON."
        ),
    ),

    "builder": AgentDef(
        role="builder",
        display_name="Builder",
        icon="🔨",
        description="Implements code changes as specified by the planner.",
        work_types=["implementation", "refactor", "ui_polish"],
        allowed_tools=["read_file", "write_file", "edit_file", "run_tests"],
        preferred_provider="nim",
        system_prompt=(
            "You are a senior full-stack engineer. Implement exactly what is asked. "
            "Do not add unrequested features. Do not over-engineer. "
            "Prefer editing existing files over creating new ones. "
            "After writing code, always verify it compiles/parses cleanly. "
            "Return a structured summary of exactly what files were changed and why."
        ),
    ),

    "debugger": AgentDef(
        role="debugger",
        display_name="Debugger",
        icon="🐛",
        description="Investigates and fixes failures, test errors, and runtime exceptions.",
        work_types=["debugging", "test_fixing"],
        allowed_tools=["read_file", "write_file", "edit_file", "run_tests", "read_logs"],
        preferred_provider="nim",
        system_prompt=(
            "You diagnose and fix bugs. Read the actual error message first. "
            "Identify the root cause before touching code. "
            "Fix the minimum necessary to resolve the issue — do not refactor. "
            "Confirm the fix by re-running the failing test or check."
        ),
    ),

    "tester": AgentDef(
        role="tester",
        display_name="Tester",
        icon="✅",
        description="Runs verification, confirms output correctness, reports pass/fail.",
        work_types=["test_fixing", "research"],
        allowed_tools=["run_tests", "run_lint", "read_file", "read_logs"],
        preferred_provider="nim",
        system_prompt=(
            "You verify that code changes work correctly. Run tests and check output. "
            "Report pass/fail/error as structured JSON. Do not guess — only report what ran."
        ),
    ),

    "reviewer": AgentDef(
        role="reviewer",
        display_name="Reviewer",
        icon="👁",
        description="Reviews code for correctness, security, and quality before merge.",
        work_types=["review"],
        allowed_tools=["read_file", "read_tests"],
        preferred_provider="claude",
        system_prompt=(
            "You are a senior code reviewer. Check for: correctness, security issues "
            "(injection, secrets, auth bypass), unnecessary complexity, and broken patterns. "
            "Be specific — cite file:line for every finding. "
            "Return a structured review with APPROVE / REQUEST_CHANGES verdict."
        ),
    ),

    "ui_agent": AgentDef(
        role="ui_agent",
        display_name="UI Polisher",
        icon="🎨",
        description="Improves frontend components, styles, and UX quality.",
        work_types=["ui_polish"],
        allowed_tools=["read_file", "write_file", "edit_file"],
        preferred_provider="gemini",
        system_prompt=(
            "You improve frontend UI quality. Follow the existing design system. "
            "Do not change layout or behaviour unless explicitly asked. "
            "Keep changes minimal and consistent with surrounding code."
        ),
    ),

    "research_agent": AgentDef(
        role="research_agent",
        display_name="Researcher",
        icon="📚",
        description="Retrieves context, reads docs, and compiles knowledge for other agents.",
        work_types=["research", "documentation"],
        allowed_tools=["read_file", "search_code", "web_search"],
        preferred_provider="gemini",
        system_prompt=(
            "You gather and summarise information. Be precise and source-cited. "
            "Return findings as compact, structured summaries. "
            "Do not make code changes."
        ),
    ),

    "memory_agent": AgentDef(
        role="memory_agent",
        display_name="Memory",
        icon="🧠",
        description="Manages compact context summaries and inter-agent handoff state.",
        work_types=["summarization"],
        allowed_tools=["read_memory", "write_memory", "compact_context"],
        preferred_provider="nim",
        system_prompt=(
            "You compact and store working context for other agents. "
            "Preserve all critical facts (IDs, file paths, decisions). "
            "Summarise repetitive content aggressively. "
            "Return compact JSON blobs, never raw chat transcripts."
        ),
    ),
}


def get_agent_for_work_type(work_type: str) -> Optional[AgentDef]:
    """Return the best agent for a given work type."""
    for agent in AGENT_DEFS.values():
        if work_type in agent.work_types:
            return agent
    return AGENT_DEFS.get("builder")  # fallback


def list_agents() -> List[Dict]:
    return [
        {
            "role": a.role,
            "display_name": a.display_name,
            "icon": a.icon,
            "description": a.description,
            "work_types": a.work_types,
            "allowed_tools": a.allowed_tools,
            "preferred_provider": a.preferred_provider,
        }
        for a in AGENT_DEFS.values()
    ]
