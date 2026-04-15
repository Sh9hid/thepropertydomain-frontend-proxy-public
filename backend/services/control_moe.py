from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional

from services.ai_router import get_control_model_alias, is_control_model_alias_available

# --- Hierarchical Organization ---

EXPERT_ROSTER: Dict[str, Dict[str, Any]] = {
    # LEVEL 1: Assistant (Chief of Staff)
    "turing": {
        "expert_key": "turing",
        "name": "Turing",
        "role": "Chief of Staff",
        "department": "Executive",
        "parent_key": None,
        "provider_alias": "openai_planner_high",
        "fallback_chain": ["openai_builder_medium"],
        "cost_band": "high",
        "purpose": "Primary operator interface, strategic routing, and mission synthesis.",
    },

    # LEVEL 2: Department Heads
    "hopper": {
        "expert_key": "hopper",
        "name": "Hopper",
        "role": "Head of Engineering",
        "department": "Engineering",
        "parent_key": "turing",
        "provider_alias": "openai_builder_medium",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
        "purpose": "Technical architecture, system integrity, and engineering team oversight.",
    },
    "ogilvy": {
        "expert_key": "ogilvy",
        "name": "Ogilvy",
        "role": "Head of Marketing",
        "department": "Marketing",
        "parent_key": "turing",
        "provider_alias": "claude_writer",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
        "purpose": "Brand positioning, outreach strategy, and content quality.",
    },
    "ziglar": {
        "expert_key": "ziglar",
        "name": "Ziglar",
        "role": "Head of Sales",
        "department": "Sales",
        "parent_key": "turing",
        "provider_alias": "claude_writer",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
        "purpose": "Lead conversion, script effectiveness, and revenue strategy.",
    },
    "curie": {
        "expert_key": "curie",
        "name": "Curie",
        "role": "Head of Research",
        "department": "Research",
        "parent_key": "turing",
        "provider_alias": "gemini_delegate_small",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "low",
        "purpose": "Data sourcing, dependency mapping, and factual verification.",
    },
    "bayes": {
        "expert_key": "bayes",
        "name": "Bayes",
        "role": "Head of Analytics",
        "department": "Analytics",
        "parent_key": "turing",
        "provider_alias": "openai_reviewer_medium",
        "fallback_chain": ["gemini_delegate_small"],
        "cost_band": "medium",
        "purpose": "Lead scoring models, market trends, and performance metrics.",
    },
    "cicero": {
        "expert_key": "cicero",
        "name": "Cicero",
        "role": "Head of Voice & Accent",
        "department": "Communications",
        "parent_key": "turing",
        "provider_alias": "claude_writer",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
        "purpose": "Communication tone, accent quality, and outreach clarity.",
    },

    # LEVEL 3: Specialist Geniuses
    "linus": {
        "expert_key": "linus",
        "name": "Linus",
        "role": "Kernel Specialist",
        "department": "Engineering",
        "parent_key": "hopper",
        "provider_alias": "openai_builder_medium",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
        "purpose": "Backend optimization, database migrations, and core infrastructure.",
    },
    "tesla": {
        "expert_key": "tesla",
        "name": "Tesla",
        "role": "Systems Visionary",
        "department": "Engineering",
        "parent_key": "hopper",
        "provider_alias": "openai_builder_medium",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
        "purpose": "Complex integrations, automation logic, and experimental features.",
    },
    "jobs": {
        "expert_key": "jobs",
        "name": "Jobs",
        "role": "Product Designer",
        "department": "Marketing",
        "parent_key": "ogilvy",
        "provider_alias": "claude_writer",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
        "purpose": "UI/UX fidelity, aesthetic consistency, and user emotion triggers.",
    },
    "aristotle": {
        "expert_key": "aristotle",
        "name": "Aristotle",
        "role": "Logic Architect",
        "department": "Research",
        "parent_key": "curie",
        "provider_alias": "gemini_delegate_small",
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "low",
        "purpose": "First-principles analysis and structural reasoning.",
    },
}

CODE_KEYWORDS = (
    "build",
    "code",
    "control center",
    "control plane",
    "debug",
    "diff",
    "feature",
    "fix",
    "implement",
    "integration",
    "patch",
    "refactor",
    "runtime",
    "ship",
    "system",
    "test",
    "ui",
)


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def is_code_mission(command: str, objective: str = "") -> bool:
    haystack = f"{command}\n{objective}".lower()
    return any(keyword in haystack for keyword in CODE_KEYWORDS)


def mission_complexity(command: str, objective: str = "") -> str:
    score = 0
    tokens = f"{command} {objective}".split()
    if len(tokens) >= 20:
        score += 1
    if len(tokens) >= 40:
        score += 1
    if is_code_mission(command, objective):
        score += 2
    lowered = f"{command}\n{objective}".lower()
    if any(keyword in lowered for keyword in ("multi-provider", "runtime", "database", "schema", "migration", "e2e", "control")):
        score += 1
    if score >= 3:
        return "high"
    if score >= 1:
        return "medium"
    return "low"


def _selected_expert_keys(command: str, objective: str = "") -> List[str]:
    # In V0, we select the assistant and all heads.
    # We can refine this to only select heads relevant to the complexity/keywords.
    base = ["turing", "hopper", "ogilvy", "ziglar", "curie", "bayes", "cicero"]
    if is_code_mission(command, objective):
        base.extend(["linus", "tesla", "jobs"])
    return list(dict.fromkeys(base))


def summarize_context(context: Dict[str, Any]) -> str:
    scope = str(context.get("scope") or "portfolio")
    scorecard = context.get("scorecard") or {}
    lines = [
        f"Scope: {scope}",
        f"Bookings 30d: {int(scorecard.get('bookings_30d') or 0)}",
        f"Projected bookings 90d: {int(scorecard.get('projected_bookings_90d') or 0)}",
        f"Callable coverage: {round(float(scorecard.get('callable_coverage') or 0) * 100, 1)}%",
        f"Evidence coverage: {round(float(scorecard.get('evidence_coverage') or 0) * 100, 1)}%",
        f"Feed health: {scorecard.get('feed_health') or 'unknown'}",
    ]
    if scope == "lead":
        lead = context.get("lead") or {}
        workflow = context.get("workflow") or {}
        lines.extend(
            [
                f"Lead address: {lead.get('address') or 'unknown'}",
                f"Lead owner: {lead.get('owner_name') or 'unknown'}",
                f"Lead suburb: {lead.get('suburb') or 'unknown'}",
                f"Call today score: {int(lead.get('call_today_score') or 0)}",
                f"Workflow stage: {workflow.get('stage') or 'not started'}",
            ]
        )
    else:
        portfolio = context.get("portfolio") or {}
        hot_leads = portfolio.get("hot_leads") or []
        hot_addresses = [str(item.get("address") or item.get("owner_name") or "lead") for item in hot_leads[:3]]
        lines.extend(
            [
                f"Total leads: {int(portfolio.get('total_leads') or 0)}",
                f"Open leads: {int(portfolio.get('open_leads') or 0)}",
                f"No contact leads: {int(portfolio.get('no_contact_leads') or 0)}",
                f"Pending tasks: {int(portfolio.get('pending_tasks') or 0)}",
                f"Hot cohort: {', '.join(hot_addresses) if hot_addresses else 'none'}",
            ]
        )
    return "\n".join(lines)


def build_control_preview(
    *,
    command: str,
    objective: str,
    target_type: str,
    target_id: Optional[str],
    target_label: str,
    autonomy_mode: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    complexity = mission_complexity(command, objective)
    expert_keys = _selected_expert_keys(command, objective)
    experts: List[Dict[str, Any]] = []
    warnings: List[str] = []
    downgrade_requests: List[Dict[str, Any]] = []
    cost_band_order = {"low": 0, "medium": 1, "high": 2}
    cost_band = "low"
    for expert_key in expert_keys:
        expert = EXPERT_ROSTER[expert_key]
        alias_config = get_control_model_alias(expert["provider_alias"])
        available = is_control_model_alias_available(expert["provider_alias"])
        fallback_chain = list(alias_config.get("fallback_chain") or expert.get("fallback_chain") or [])
        proposed_fallback = fallback_chain[0] if fallback_chain else None
        if not available:
            reason = alias_config.get("unavailable_reason") or "preferred alias is unavailable"
            if proposed_fallback:
                warnings.append(f"{expert['name']} cannot use {expert['provider_alias']} right now. Approval is required to downgrade to {proposed_fallback}.")
                downgrade_requests.append(
                    {
                        "expert_key": expert_key,
                        "expert_name": expert["name"],
                        "requested_alias": expert["provider_alias"],
                        "proposed_alias": proposed_fallback,
                        "reason": reason,
                        "fallback_chain": fallback_chain,
                    }
                )
            else:
                warnings.append(f"{expert['name']} is blocked because {expert['provider_alias']} is unavailable and no fallback is configured.")
        cost_band = max(cost_band, alias_config.get("cost_band") or expert["cost_band"], key=lambda item: cost_band_order.get(item, 0))
        experts.append(
            {
                "expert_key": expert_key,
                "name": expert["name"],
                "role": expert["role"],
                "department": expert["department"],
                "parent_key": expert["parent_key"],
                "purpose": expert["purpose"],
                "provider": alias_config.get("provider"),
                "provider_alias": expert["provider_alias"],
                "model_alias": expert["provider_alias"],
                "planned_model": alias_config.get("model"),
                "fallback_chain": fallback_chain,
                "cost_band": alias_config.get("cost_band") or expert["cost_band"],
                "available": available,
                "availability_reason": alias_config.get("unavailable_reason"),
            }
        )
    model_plan = {
        "complexity": complexity,
        "code_mission": is_code_mission(command, objective),
        "cost_band": cost_band,
        "experts": experts,
        "downgrade_requests": downgrade_requests,
        "approved_aliases": {},
    }
    preview_hash = _stable_hash(
        {
            "command": command,
            "objective": objective,
            "target_type": target_type,
            "target_id": target_id,
            "target_label": target_label,
            "autonomy_mode": autonomy_mode,
            "model_plan": model_plan,
        }
    )
    return {
        "command": command,
        "objective": objective,
        "target_type": target_type,
        "target_id": target_id,
        "target_label": target_label,
        "autonomy_mode": autonomy_mode,
        "complexity": complexity,
        "cost_band": cost_band,
        "preview_hash": preview_hash,
        "experts": experts,
        "model_plan": model_plan,
        "warnings": warnings,
        "downgrade_required": bool(downgrade_requests),
        "context_summary": summarize_context(context),
    }


def model_plan_from_context(context_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    return (context_snapshot or {}).get("model_plan") or {}


def preview_hash_from_context(context_snapshot: Dict[str, Any]) -> str:
    return str((context_snapshot or {}).get("preview_hash") or "")


def sanitize_preview_for_hash(preview: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(preview)
    payload.pop("warnings", None)
    payload.pop("context_summary", None)
    return payload


def preview_matches_hash(preview: Dict[str, Any], preview_hash: Optional[str]) -> bool:
    if not preview_hash:
        return False
    expected = _stable_hash(sanitize_preview_for_hash(preview))
    return expected == preview_hash


def planning_system_prompt(preview: Dict[str, Any]) -> str:
    expert_names = ", ".join(expert["name"] for expert in preview.get("experts") or [])
    return (
        "You are Turing, the Chief of Staff inside a hierarchical mixture-of-experts control plane. "
        "You are rigorous, concise, and operational. "
        "You oversee department heads (Hopper, Ogilvy, etc.) who in turn oversee specialist geniuses (Linus, Tesla, etc.). "
        "Only use the supplied context. Do not invent product facts or file changes. "
        f"The active expert roster is: {expert_names}. "
        "Return strict JSON only."
    )


def planning_user_prompt(command: str, objective: str, target_label: str, context_summary: str, preview: Dict[str, Any]) -> str:
    expert_names = [expert["name"] for expert in preview.get("experts") or []]
    return (
        "Plan this mission for the control plane.\n"
        f"Command: {command}\n"
        f"Objective: {objective or command}\n"
        f"Target: {target_label}\n"
        f"Mission complexity: {preview.get('complexity')}\n"
        f"Code mission: {'yes' if preview.get('model_plan', {}).get('code_mission') else 'no'}\n"
        f"Expert roster: {', '.join(expert_names)}\n"
        "Context:\n"
        f"{context_summary}\n\n"
        "Return JSON with this shape:\n"
        "{\n"
        '  "director_summary": "short paragraph",\n'
        '  "consensus_plan": "short paragraph",\n'
        '  "expert_statuses": [\n'
        "    {\n"
        '      "expert_key": "turing",\n'
        '      "summary": "what this expert owns",\n'
        '      "findings": ["fact 1", "fact 2"],\n'
        '      "status": "active"\n'
        "    }\n"
        "  ],\n"
        '  "recommended_steps": [\n'
        "    {\n"
        '      "title": "step title",\n'
        '      "owner": "one of the active expert names",\n'
        '      "department": "same as owner",\n'
        '      "reason": "why this lane exists",\n'
        '      "priority": "critical|high|normal|medium|low",\n'
        '      "approval_required": true\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "Use the named experts as owners. Keep recommended_steps short and execution oriented."
    )


def delegate_system_prompt() -> str:
    return (
        "You are Curie, the Head of Research. "
        "Map dependencies, call out missing context, and keep token use low. "
        "Respond in plain text."
    )


def delegate_user_prompt(command: str, context_summary: str) -> str:
    return (
        "Break this mission into the minimum useful dependency map.\n"
        f"Command: {command}\n"
        "Context:\n"
        f"{context_summary}\n\n"
        "Return 3 short bullets covering: critical dependencies, missing facts, and the cheapest first move."
    )


def critique_system_prompt() -> str:
    return (
        "You are Popper, the adversarial reviewer. "
        "Focus on regressions, verification gaps, and weak assumptions. "
        "Respond in plain text."
    )


def critique_user_prompt(command: str, recommended_steps: List[Dict[str, Any]], context_summary: str) -> str:
    step_titles = [str(item.get("title") or "") for item in recommended_steps[:5]]
    return (
        "Critique this mission plan and highlight the main verification risk.\n"
        f"Command: {command}\n"
        f"Planned lanes: {', '.join(step_titles) if step_titles else 'none'}\n"
        "Context:\n"
        f"{context_summary}\n\n"
        "Return a concise review note with the strongest objection and the test focus."
    )


def writer_system_prompt() -> str:
    return (
        "You are Woolf, the operator packet writer. "
        "Write clear approval-ready summaries without fluff. "
        "Respond in plain text."
    )


def writer_user_prompt(command: str, director_summary: str, consensus_plan: str, recommended_steps: List[Dict[str, Any]]) -> str:
    lines = [f"- {item.get('title')}: {item.get('reason')}" for item in recommended_steps[:5]]
    return (
        "Write the operator approval packet.\n"
        f"Command: {command}\n"
        f"Director summary: {director_summary}\n"
        f"Consensus plan: {consensus_plan}\n"
        "Execution lanes:\n"
        f"{chr(10).join(lines) if lines else '- none'}\n\n"
        "Return a tight operator-facing note that explains what will run, what still needs approval, and what outputs to inspect."
    )


def patch_system_prompt() -> str:
    return (
        "You are Linus, the Engineering Specialist. "
        "Produce a reviewable patch artifact. "
        "Return strict JSON only."
    )


def patch_user_prompt(command: str, work_item: Dict[str, Any], mission_summary: str, context_summary: str) -> str:
    return (
        "Generate a reviewable patch artifact for this engineering task.\n"
        f"Mission command: {command}\n"
        f"Mission summary: {mission_summary}\n"
        f"Work item title: {work_item.get('title')}\n"
        f"Work item description: {work_item.get('description')}\n"
        "Context:\n"
        f"{context_summary}\n\n"
        "Return JSON with this shape:\n"
        "{\n"
        '  "artifact_title": "short title",\n'
        '  "summary": "implementation summary",\n'
        '  "diff": "unified diff text only",\n'
        '  "files": ["relative/path.py"],\n'
        '  "verification_steps": ["step 1"],\n'
        '  "warnings": ["warning 1"]\n'
        "}\n"
        "If the mission lacks enough code context, still return a best-effort diff and state the gap in warnings."
    )


def review_system_prompt() -> str:
    return (
        "You are Popper, the patch reviewer. "
        "Review the patch for regressions and test gaps. "
        "Return strict JSON only."
    )


def review_user_prompt(command: str, patch_diff: str, patch_summary: str) -> str:
    return (
        "Review this patch artifact.\n"
        f"Mission command: {command}\n"
        f"Patch summary: {patch_summary}\n"
        "Patch diff:\n"
        f"{patch_diff[:12000]}\n\n"
        "Return JSON with this shape:\n"
        "{\n"
        '  "summary": "overall review",\n'
        '  "verification_state": "ready|needs_revision|blocked",\n'
        '  "findings": ["finding 1"],\n'
        '  "verification_steps": ["step 1"],\n'
        '  "apply_recommendation": "apply|hold"\n'
        "}"
    )


def _strip_code_fences(payload: str) -> str:
    text = payload.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def extract_json_object(payload: str) -> Optional[Dict[str, Any]]:
    cleaned = _strip_code_fences(payload)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(cleaned[start : end + 1])
            except json.JSONDecodeError:
                return None
    return None


def _normalize_priority(priority: Any) -> str:
    value = str(priority or "normal").lower()
    if value in {"critical", "high", "normal", "medium", "low"}:
        return value
    return "normal"


def _step_id(owner: str, title: str) -> str:
    return hashlib.md5(f"{owner}:{title}".encode("utf-8")).hexdigest()


def parse_planning_payload(raw_output: str, preview: Dict[str, Any]) -> Dict[str, Any]:
    parsed = extract_json_object(raw_output) or {}
    experts_by_key = {item["expert_key"]: item for item in preview.get("experts") or []}
    experts_by_name = {item["name"].lower(): item for item in preview.get("experts") or []}
    director_summary = str(parsed.get("director_summary") or "").strip() or raw_output.strip()[:800]
    consensus_plan = str(parsed.get("consensus_plan") or "").strip() or director_summary
    expert_statuses: List[Dict[str, Any]] = []
    for item in parsed.get("expert_statuses") or []:
        expert_key = str(item.get("expert_key") or "").lower()
        expert = experts_by_key.get(expert_key)
        if not expert:
            continue
        expert_statuses.append(
            {
                "department": expert["department"],
                "head": expert["role"],
                "specialists": [expert.get("planned_model") or expert.get("provider_alias")],
                "status": str(item.get("status") or "active"),
                "summary": str(item.get("summary") or expert.get("purpose") or ""),
                "findings": [str(finding) for finding in (item.get("findings") or [])[:4]],
                "recommended_steps": [],
            }
        )
    recommended_steps: List[Dict[str, Any]] = []
    for step in parsed.get("recommended_steps") or []:
        owner = str(step.get("owner") or "Turing").strip()
        expert = experts_by_name.get(owner.lower()) or experts_by_key.get(str(step.get("department") or "").lower())
        owner_name = expert["name"] if expert else owner
        recommended_steps.append(
            {
                "id": _step_id(owner_name, str(step.get("title") or "Work item")),
                "title": str(step.get("title") or "Work item"),
                "owner": owner_name,
                "department": expert["department"] if expert else owner_name,
                "reason": str(step.get("reason") or "Execute the mission lane."),
                "priority": _normalize_priority(step.get("priority")),
                "channel": None,
                "lead_id": None,
                "approval_required": bool(step.get("approval_required", True)),
            }
        )
    if not recommended_steps:
        default_owner = "Linus" if preview.get("model_plan", {}).get("code_mission") else "Turing"
        recommended_steps.append(
            {
                "id": _step_id(default_owner, "Primary mission lane"),
                "title": "Primary mission lane",
                "owner": default_owner,
                "department": default_owner,
                "reason": "Execute the highest-leverage lane from the approved mission plan.",
                "priority": "high",
                "channel": None,
                "lead_id": None,
                "approval_required": True,
            }
        )
    expert_statuses_by_name = {item["department"]: item for item in expert_statuses}
    for step in recommended_steps:
        target = expert_statuses_by_name.get(step["department"])
        if target is not None:
            target["recommended_steps"].append(step)
    if not expert_statuses:
        for expert in preview.get("experts") or []:
            expert_statuses.append(
                {
                    "department": expert["department"],
                    "head": expert["role"],
                    "specialists": [expert.get("planned_model") or expert.get("provider_alias")],
                    "status": "active",
                    "summary": expert.get("purpose") or "",
                    "findings": [],
                    "recommended_steps": [step for step in recommended_steps if step["department"] == expert["department"]],
                }
            )
    return {
        "director_summary": director_summary,
        "consensus_plan": consensus_plan,
        "department_statuses": expert_statuses,
        "recommended_steps": recommended_steps,
    }


def parse_patch_payload(raw_output: str) -> Dict[str, Any]:
    parsed = extract_json_object(raw_output) or {}
    diff_text = str(parsed.get("diff") or "").strip()
    if not diff_text:
        cleaned = _strip_code_fences(raw_output)
        if "diff --git" in cleaned or cleaned.startswith("--- "):
            diff_text = cleaned
    return {
        "artifact_title": str(parsed.get("artifact_title") or "Patch artifact"),
        "summary": str(parsed.get("summary") or "").strip(),
        "diff": diff_text,
        "files": [str(item) for item in (parsed.get("files") or []) if str(item).strip()],
        "verification_steps": [str(item) for item in (parsed.get("verification_steps") or []) if str(item).strip()],
        "warnings": [str(item) for item in (parsed.get("warnings") or []) if str(item).strip()],
    }


def parse_review_payload(raw_output: str) -> Dict[str, Any]:
    parsed = extract_json_object(raw_output) or {}
    return {
        "summary": str(parsed.get("summary") or raw_output).strip(),
        "verification_state": str(parsed.get("verification_state") or "needs_revision").strip().lower(),
        "findings": [str(item) for item in (parsed.get("findings") or []) if str(item).strip()],
        "verification_steps": [str(item) for item in (parsed.get("verification_steps") or []) if str(item).strip()],
        "apply_recommendation": str(parsed.get("apply_recommendation") or "hold").strip().lower(),
    }
