from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.database import _async_session_factory
from core.events import event_manager
from core.utils import now_iso, now_sydney
from models.control_models import (
    AgentHeartbeat,
    AgentNode,
    ControlTrigger,
    DebateSession,
    DebateTurn,
    ExecutionAttempt,
    FactPack,
    ImprovementCandidate,
    LearningEvaluation,
    Mission,
    MissionEvent,
    MissionRun,
    OrgRun,
    PolicyVersion,
    ReviewGate,
    RunArtifact,
    WorkItem,
)
from models.control_schemas import (
    AgentHeartbeatPayload,
    AgentNodePayload,
    ControlDepartmentStatus,
    ControlLiveAgentPayload,
    ControlLiveSnapshot,
    ControlMissionCommandRequest,
    ControlMissionDetail,
    ControlMissionEventPayload,
    ControlMissionListResponse,
    ControlMissionPreview,
    ControlDowngradeApproveRequest,
    ControlMissionRunPayload,
    ControlMissionSummary,
    ControlOrgRunDetail,
    ControlOrgRunPayload,
    ControlRecommendedStep,
    ControlRuntimeStatusPayload,
    ControlTimelineEntryPayload,
    ControlTriggerPayload,
    ControlWorkItemListResponse,
    DebateSessionPayload,
    DebateTurnPayload,
    ExecutionAttemptPayload,
    FactPackPayload,
    ImprovementCandidatePayload,
    LearningEvaluationPayload,
    PolicyVersionPayload,
    ReviewGatePayload,
    RunArtifactPayload,
    WorkItemPayload,
)
from services.ai_router import run_control_model_alias
from services.control_moe import (
    EXPERT_ROSTER,
    build_control_preview,
    critique_system_prompt,
    critique_user_prompt,
    delegate_system_prompt,
    delegate_user_prompt,
    model_plan_from_context,
    parse_patch_payload,
    parse_planning_payload,
    parse_review_payload,
    patch_system_prompt,
    patch_user_prompt,
    planning_system_prompt,
    planning_user_prompt,
    preview_hash_from_context,
    preview_matches_hash,
    review_system_prompt,
    review_user_prompt,
    summarize_context,
    writer_system_prompt,
    writer_user_prompt,
)
from models.sql_models import Lead

_PRIORITY_ORDER = {"critical": 0, "high": 1, "normal": 2, "medium": 3, "low": 4}
_ACTIVE_ORG_STATUSES = {"routing", "debating", "executing", "approved"}
_BUILD_KEYWORDS = ("build", "implement", "ship", "control", "agent", "workflow", "runtime", "project", "system", "org")
_REPO_ROOT = Path(__file__).resolve().parents[2]
_RUNTIME_LOOP_SECONDS = 2
_LOGGER = logging.getLogger(__name__)
_RUNTIME_STATE: Dict[str, Any] = {
    "status": "booting",
    "loop_interval_seconds": _RUNTIME_LOOP_SECONDS,
    "tick_count": 0,
    "failure_count": 0,
    "last_tick_at": None,
    "last_success_at": None,
    "last_error_at": None,
    "last_error": "",
}
_TRIGGER_COOLDOWNS = {
    "source_ingested": 24,
    "score_threshold": 24,
    "outcome_logged": 6,
    "stale_queue": 6,
    "funnel_regression": 24,
    "system_health_regression": 24,
    "operator_command": 0,
}
_TEAM_BLUEPRINTS: Dict[str, Dict[str, Any]] = {
    "Growth Team": {
        "head": "Head of Growth Systems",
        "specialists": ["Booking Forecast Analyst", "Queue Strategist"],
        "summary": "Push more booked appraisals without burning operator focus on low-yield work.",
    },
    "Data Quality Team": {
        "head": "Head of Data Quality",
        "specialists": ["Callable Coverage Analyst", "Evidence Coverage Analyst"],
        "summary": "Improve contactability and evidence quality on the cohort most likely to book.",
    },
    "Source Ops Team": {
        "head": "Head of Source Operations",
        "specialists": ["Feed Freshness Monitor", "Archive Coverage Monitor"],
        "summary": "Keep ingest, archive, and enrichment signals fresh enough to sustain bookings growth.",
    },
    "Workflow UX Team": {
        "head": "Head of Workflow UX",
        "specialists": ["Approval Friction Analyst", "Task Queue Analyst"],
        "summary": "Reduce queue drag, overdue work, and operator friction that blocks booked appointments.",
    },
    "Reliability Team": {
        "head": "Head of Reliability",
        "specialists": ["Sender Reliability Analyst", "Runtime Health Monitor"],
        "summary": "Protect the runtime from silent failures that erode bookings and trust.",
    },
    "Governance Team": {
        "head": "Head of Governance",
        "specialists": ["Policy Gatekeeper", "Rollback Steward"],
        "summary": "Promote only measurable improvements that stay inside the guardrails.",
    },
    "Engineering": {
        "head": "Head of Engineering",
        "specialists": ["Build Supervisor", "Patch Builder", "Test Reviewer"],
        "summary": "Package approved app improvements into reviewable engineering slices.",
    },
}

# --- Tiered Execution & Capability Routing ---

CAPABILITY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "cheap_small_text":      {"alias": "gemini_delegate_small", "max_retries": 2, "escalate_to": "fast_structured_output"},
    "fast_structured_output": {"alias": "nim_small",            "max_retries": 2, "escalate_to": "summarization"},
    "summarization":          {"alias": "gemini_delegate_small", "max_retries": 1, "escalate_to": "review_reasoning"},
    "classification":         {"alias": "nim_small",            "max_retries": 1, "escalate_to": "light_planning"},
    "light_planning":         {"alias": "openai_builder_medium", "max_retries": 2, "escalate_to": "code_heavy_patch"},
    "code_small_patch":       {"alias": "openai_builder_medium", "max_retries": 2, "escalate_to": "code_heavy_patch"},
    "code_heavy_patch":       {"alias": "openai_planner_high",   "max_retries": 3, "escalate_to": "debugging_complex"},
    "debugging_complex":      {"alias": "openai_planner_high",   "max_retries": 3, "escalate_to": None},
    "review_reasoning":       {"alias": "openai_reviewer_medium", "max_retries": 2, "escalate_to": "openai_planner_high"},
    "long_context":           {"alias": "claude_writer",         "max_retries": 1, "escalate_to": "openai_planner_high"},
}

TASK_CAPABILITY_MAP: Dict[str, str] = {
    "analyze_repo_context":   "long_context",
    "identify_target_files":  "light_planning",
    "summarize_relevant_code": "summarization",
    "propose_patch_plan":     "light_planning",
    "generate_small_patch":   "code_small_patch",
    "generate_test_cases":    "cheap_small_text",
    "review_patch":           "review_reasoning",
    "summarize_progress":     "cheap_small_text",
    "classify_task":          "classification",
    "extract_structured":     "fast_structured_output",
}


def _resolve_capability_for_step(step: Dict[str, Any]) -> str:
    title = str(step.get("title") or "").lower()
    reason = str(step.get("reason") or "").lower()
    
    if any(k in title for k in ("summarize", "summary")):
        return "summarization"
    if any(k in title for k in ("classify", "type")):
        return "classification"
    if any(k in title for k in ("extract", "structured", "json")):
        return "fast_structured_output"
    if any(k in title for k in ("review", "audit")):
        return "review_reasoning"
    if any(k in title for k in ("patch", "fix", "implement")):
        if "complex" in reason or "multi-file" in reason:
            return "code_heavy_patch"
        return "code_small_patch"
    if any(k in title for k in ("plan", "propose")):
        return "light_planning"
    if any(k in title for k in ("test", "pytest")):
        return "cheap_small_text"
        
    return "cheap_small_text"



def _safe_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return []


def _mission_title(body: ControlMissionCommandRequest, target_label: str) -> str:
    if body.title and body.title.strip():
        return body.title.strip()
    command = body.command.strip()
    if target_label:
        return f"{target_label} - {command[:72]}".strip()
    return command[:96] or "Control mission"


def _step(
    title: str,
    owner: str,
    department: str,
    reason: str,
    priority: str = "normal",
    channel: Optional[str] = None,
    lead_id: Optional[str] = None,
    approval_required: bool = True,
) -> Dict[str, Any]:
    return ControlRecommendedStep(
        id=hashlib.md5(f"{department}:{owner}:{title}:{lead_id or ''}".encode("utf-8")).hexdigest(),
        title=title,
        owner=owner,
        department=department,
        reason=reason,
        priority=priority,
        channel=channel,
        lead_id=lead_id,
        approval_required=approval_required,
    ).model_dump()


def _sort_steps(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(steps, key=lambda item: _PRIORITY_ORDER.get(str(item.get("priority", "normal")), 2))


async def _build_preview_for_body(
    session: AsyncSession,
    body: ControlMissionCommandRequest,
    *,
    context: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
    built_context = context or await _build_context(session, body)
    target_label = _mission_target_label(built_context)
    preview = build_control_preview(
        command=body.command.strip(),
        objective=(body.objective or body.command).strip(),
        target_type=str(body.target_type or "portfolio"),
        target_id=body.target_id,
        target_label=target_label,
        autonomy_mode=str(body.autonomy_mode or "research_only"),
        context=built_context,
    )
    return built_context, target_label, preview


def _mission_model_plan(mission: Mission) -> Dict[str, Any]:
    return model_plan_from_context(mission.context_snapshot or {})


def _expert_assignments(mission: Mission) -> Dict[str, Dict[str, Any]]:
    experts = (_mission_model_plan(mission).get("experts") or [])
    return {str(expert.get("expert_key") or "").lower(): expert for expert in experts}


def _approved_aliases(mission: Mission) -> Dict[str, str]:
    return dict(_mission_model_plan(mission).get("approved_aliases") or {})


def _planned_alias_for_expert(mission: Mission, expert_key: str) -> Optional[str]:
    expert = _expert_assignments(mission).get(expert_key.lower())
    if not expert:
        return None
    return str(expert.get("provider_alias") or expert.get("model_alias") or "")


def _selected_alias_for_expert(mission: Mission, expert_key: str) -> Optional[str]:
    approved = _approved_aliases(mission)
    if expert_key.lower() in approved:
        return approved[expert_key.lower()]
    return _planned_alias_for_expert(mission, expert_key)


def _fallback_chain_for_expert(mission: Mission, expert_key: str) -> List[str]:
    expert = _expert_assignments(mission).get(expert_key.lower()) or {}
    return _safe_list(expert.get("fallback_chain"))


def _context_summary(mission: Mission) -> str:
    return summarize_context(mission.context_snapshot or {})


def _find_agent_by_expert_key(agents: List[AgentNode], expert_key: str) -> Optional[AgentNode]:
    expert_key = expert_key.lower()
    for agent in agents:
        if str((agent.attributes or {}).get("expert_key") or "").lower() == expert_key:
            return agent
    return None


def _apply_approved_alias(mission: Mission, expert_key: str, alias: str) -> None:
    snapshot = dict(mission.context_snapshot or {})
    model_plan = dict(snapshot.get("model_plan") or {})
    approved_aliases = dict(model_plan.get("approved_aliases") or {})
    approved_aliases[expert_key.lower()] = alias
    model_plan["approved_aliases"] = approved_aliases
    snapshot["model_plan"] = model_plan
    mission.context_snapshot = snapshot


async def _ensure_downgrade_gate(
    session: AsyncSession,
    *,
    org_run_id: str,
    mission: Mission,
    work_item_id: Optional[str],
    expert_key: str,
    requested_alias: str,
    proposed_alias: Optional[str],
    reason: str,
    resume_status: Optional[str] = None,
) -> Optional[ReviewGate]:
    if not proposed_alias:
        return None
    existing = (
        await session.execute(
            select(ReviewGate).where(
                ReviewGate.org_run_id == org_run_id,
                ReviewGate.mission_id == mission.id,
                ReviewGate.work_item_id == work_item_id,
                ReviewGate.gate_type == "model_downgrade",
                ReviewGate.status == "pending",
            )
        )
    ).scalars().first()
    if existing:
        return existing
    expert = EXPERT_ROSTER.get(expert_key.lower(), {})
    gate = ReviewGate(
        id=uuid.uuid4().hex,
        org_run_id=org_run_id,
        mission_id=mission.id,
        work_item_id=work_item_id,
        gate_type="model_downgrade",
        title=f"Approve downgrade for {expert.get('name') or expert_key}",
        status="pending",
        requested_by="control_moe",
        rationale=reason,
        payload={
            "expert_key": expert_key.lower(),
            "expert_name": expert.get("name") or expert_key,
            "requested_alias": requested_alias,
            "proposed_alias": proposed_alias,
            "fallback_chain": _fallback_chain_for_expert(mission, expert_key),
            "resume_status": resume_status,
        },
        created_at=now_iso(),
        updated_at=now_iso(),
    )
    session.add(gate)
    return gate


async def _create_execution_attempt(
    session: AsyncSession,
    *,
    org_run_id: str,
    mission_id: str,
    work_item_id: Optional[str],
    agent_id: Optional[str],
    expert_key: str,
    execution_role: str,
    provider: str,
    model_alias: str,
    model_name: str,
    prompt_hash: str,
    cost_band: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> ExecutionAttempt:
    now = now_iso()
    attempt = ExecutionAttempt(
        id=uuid.uuid4().hex,
        org_run_id=org_run_id,
        mission_id=mission_id,
        work_item_id=work_item_id,
        agent_id=agent_id,
        expert_key=expert_key,
        execution_role=execution_role,
        provider=provider,
        model_alias=model_alias,
        model_name=model_name,
        status="running",
        prompt_hash=prompt_hash,
        cost_band=cost_band,
        execution_metadata=metadata or {},
        started_at=now,
        updated_at=now,
    )
    session.add(attempt)
    return attempt


async def _run_expert(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    mission: Mission,
    agents: List[AgentNode],
    expert_key: str,
    execution_role: str,
    prompt: str,
    system: str,
    work_item: Optional[WorkItem] = None,
    temperature: float = 0.2,
    max_output_tokens: int = 2048,
    resume_status: Optional[str] = None,
) -> Dict[str, Any]:
    selected_alias = _selected_alias_for_expert(mission, expert_key)
    if work_item and work_item.payload and work_item.payload.get("model_alias"):
        selected_alias = work_item.payload["model_alias"]

    if not selected_alias:
        return {"success": False, "status": "failed", "error": f"missing model alias for {expert_key}"}
    
    planned_alias = _planned_alias_for_expert(mission, expert_key) or selected_alias
    agent = _find_agent_by_expert_key(agents, expert_key)
    prompt_hash = hashlib.sha256(f"{system}\n{prompt}".encode("utf-8")).hexdigest()
    
    # Tiered Retry/Escalation Logic
    max_retries = 1
    if work_item and work_item.capability_requirement:
        cap_config = CAPABILITY_REGISTRY.get(work_item.capability_requirement, {})
        max_retries = cap_config.get("max_retries", 1)

    last_error = ""
    for attempt_no in range(max_retries + 1):
        if attempt_no > 0 and work_item:
            work_item.retry_count = attempt_no
            # Simple escalation: if we have an escalation target, use it on retry
            cap_config = CAPABILITY_REGISTRY.get(work_item.capability_requirement or "", {})
            esc_target = cap_config.get("escalate_to")
            if esc_target and esc_target in CAPABILITY_REGISTRY:
                selected_alias = CAPABILITY_REGISTRY[esc_target]["alias"]
                work_item.escalation_level += 1
                work_item.capability_requirement = esc_target
                _LOGGER.info(f"[Control] Escalating work_item {work_item.id} to {selected_alias} (attempt {attempt_no})")

        result = await run_control_model_alias(
            selected_alias,
            prompt=prompt,
            system=system,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        )
        
        attempt = await _create_execution_attempt(
            session,
            org_run_id=org_run.id,
            mission_id=mission.id,
            work_item_id=work_item.id if work_item else None,
            agent_id=agent.id if agent else None,
            expert_key=expert_key,
            execution_role=execution_role,
            provider=str(result.get("provider") or ""),
            model_alias=str(result.get("model_alias") or selected_alias),
            model_name=str(result.get("model") or ""),
            prompt_hash=prompt_hash,
            cost_band=str(result.get("cost_band") or "medium"),
            metadata={
                "fallback_chain": result.get("fallback_chain") or [],
                "planned_alias": planned_alias,
                "selected_alias": selected_alias,
                "attempt_no": attempt_no,
            },
        )
        
        if result.get("success"):
            attempt.status = "completed"
            attempt.output_content = str(result.get("output") or "")
            attempt.updated_at = now_iso()
            session.add(attempt)
            if work_item:
                work_item.output_summary = attempt.output_content[:200]
                session.add(work_item)
            return {**result, "attempt": attempt}
        
        last_error = str(result.get("error") or "Unknown error")
        attempt.status = "failed"
        attempt.error_message = last_error
        attempt.updated_at = now_iso()
        session.add(attempt)
        await session.flush() # ensure attempt is saved before next retry

    return {"success": False, "status": "failed", "error": f"Max retries exceeded. Last error: {last_error}"}

    if result.get("success"):
        usage = result.get("usage") or {}
        attempt.status = "completed"
        attempt.input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
        attempt.output_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
        attempt.completed_at = now_iso()
        attempt.updated_at = now_iso()
        session.add(attempt)
        return {
            "success": True,
            "status": "completed",
            "attempt": attempt,
            "output": str(result.get("output") or ""),
            "provider": result.get("provider"),
            "model_alias": result.get("model_alias"),
            "model": result.get("model"),
            "usage": usage,
        }

    fallback_chain = _fallback_chain_for_expert(mission, expert_key)
    if selected_alias == planned_alias and fallback_chain:
        proposed_alias = fallback_chain[0]
        gate = await _ensure_downgrade_gate(
            session,
            org_run_id=org_run.id,
            mission=mission,
            work_item_id=work_item.id if work_item else None,
            expert_key=expert_key,
            requested_alias=planned_alias,
            proposed_alias=proposed_alias,
            reason=str(result.get("error") or f"{planned_alias} unavailable"),
            resume_status=resume_status,
        )
        attempt.status = "awaiting_model_approval"
        attempt.execution_metadata = {**(attempt.execution_metadata or {}), "downgrade_gate_id": gate.id if gate else None}
        attempt.updated_at = now_iso()
        session.add(attempt)
        return {
            "success": False,
            "status": "awaiting_model_approval",
            "attempt": attempt,
            "gate": gate,
            "error": result.get("error") or "downgrade approval required",
        }

    attempt.status = "failed"
    attempt.execution_metadata = {**(attempt.execution_metadata or {}), "error": result.get("error")}
    attempt.completed_at = now_iso()
    attempt.updated_at = now_iso()
    session.add(attempt)
    return {
        "success": False,
        "status": "failed",
        "attempt": attempt,
        "error": result.get("error") or "expert execution failed",
    }


async def _upsert_artifact(
    session: AsyncSession,
    *,
    org_run_id: str,
    mission_id: str,
    artifact_type: str,
    title: str,
    content: str,
    attributes: Optional[Dict[str, Any]] = None,
) -> RunArtifact:
    artifact = (
        await session.execute(
            select(RunArtifact)
            .where(RunArtifact.org_run_id == org_run_id, RunArtifact.artifact_type == artifact_type)
            .order_by(RunArtifact.created_at.asc())
            .limit(1)
        )
    ).scalars().first()
    now = now_iso()
    if artifact:
        artifact.title = title
        artifact.content = content
        artifact.status = "ready"
        artifact.attributes = {**(artifact.attributes or {}), **(attributes or {})}
        artifact.updated_at = now
        session.add(artifact)
        return artifact
    artifact = RunArtifact(
        id=uuid.uuid4().hex,
        org_run_id=org_run_id,
        mission_id=mission_id,
        artifact_type=artifact_type,
        title=title,
        status="ready",
        content=content,
        attributes=attributes or {},
        created_at=now,
        updated_at=now,
    )
    session.add(artifact)
    return artifact


async def _execute_mission_planning(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    mission: Mission,
    run: MissionRun,
    agents: List[AgentNode],
) -> Dict[str, Any]:
    snapshot = dict(mission.context_snapshot or {})
    preview = {
        "experts": _mission_model_plan(mission).get("experts") or [],
        "model_plan": _mission_model_plan(mission),
        "complexity": snapshot.get("complexity") or "medium",
    }
    context_summary = _context_summary(mission)
    turing_result = await _run_expert(
        session,
        org_run=org_run,
        mission=mission,
        agents=agents,
        expert_key="turing",
        execution_role="planning",
        prompt=planning_user_prompt(mission.command_text, mission.objective, mission.target_label, context_summary, preview),
        system=planning_system_prompt(preview),
        max_output_tokens=2200,
        resume_status="routing",
    )
    if turing_result["status"] != "completed":
        return turing_result

    planning = parse_planning_payload(str(turing_result.get("output") or ""), preview)
    mission.director_summary = planning["director_summary"]
    mission.consensus_plan = planning["consensus_plan"]
    mission.department_statuses = planning["department_statuses"]
    mission.recommended_steps = _sort_steps(planning["recommended_steps"])
    mission.llm_call_count = int(mission.llm_call_count or 0) + 1
    run.director_summary = mission.director_summary
    run.consensus_plan = mission.consensus_plan
    run.recommended_steps = mission.recommended_steps
    run.context_snapshot = snapshot
    run.updated_at = now_iso()
    session.add(mission)
    session.add(run)

    delegate_result = await _run_expert(
        session,
        org_run=org_run,
        mission=mission,
        agents=agents,
        expert_key="shannon",
        execution_role="triage",
        prompt=delegate_user_prompt(mission.command_text, context_summary),
        system=delegate_system_prompt(),
        max_output_tokens=900,
        resume_status="routing",
    )
    if delegate_result["status"] == "awaiting_model_approval":
        return delegate_result

    critique_result = await _run_expert(
        session,
        org_run=org_run,
        mission=mission,
        agents=agents,
        expert_key="popper",
        execution_role="critique",
        prompt=critique_user_prompt(mission.command_text, mission.recommended_steps, context_summary),
        system=critique_system_prompt(),
        max_output_tokens=1200,
        resume_status="routing",
    )
    if critique_result["status"] == "awaiting_model_approval":
        return critique_result

    writer_result = await _run_expert(
        session,
        org_run=org_run,
        mission=mission,
        agents=agents,
        expert_key="woolf",
        execution_role="operator_packet",
        prompt=writer_user_prompt(mission.command_text, mission.director_summary or "", mission.consensus_plan or "", mission.recommended_steps),
        system=writer_system_prompt(),
        max_output_tokens=1200,
        resume_status="routing",
    )
    if writer_result["status"] == "awaiting_model_approval":
        return writer_result

    expert_outputs = {
        "turing": str(turing_result.get("output") or ""),
        "shannon": str(delegate_result.get("output") or ""),
        "popper": str(critique_result.get("output") or ""),
        "woolf": str(writer_result.get("output") or ""),
    }
    snapshot["expert_outputs"] = expert_outputs
    snapshot["complexity"] = snapshot.get("complexity") or preview.get("complexity") or "medium"
    mission.context_snapshot = snapshot
    run.context_snapshot = snapshot
    mission.llm_call_count = int(mission.llm_call_count or 0) + sum(
        1 for item in (delegate_result, critique_result, writer_result) if item.get("status") == "completed"
    )
    session.add(mission)
    session.add(run)

    await _upsert_artifact(
        session,
        org_run_id=org_run.id,
        mission_id=mission.id,
        artifact_type="mission_brief",
        title="Turing mission brief",
        content=expert_outputs["turing"],
        attributes={"expert_key": "turing", "provider_alias": _selected_alias_for_expert(mission, "turing"), "model_alias": _selected_alias_for_expert(mission, "turing"), "verification_state": "planned"},
    )
    if expert_outputs["shannon"]:
        await _upsert_artifact(
            session,
            org_run_id=org_run.id,
            mission_id=mission.id,
            artifact_type="triage_note",
            title="Shannon dependency map",
            content=expert_outputs["shannon"],
            attributes={"expert_key": "shannon", "provider_alias": _selected_alias_for_expert(mission, "shannon"), "model_alias": _selected_alias_for_expert(mission, "shannon"), "verification_state": "planned"},
        )
    if expert_outputs["popper"]:
        await _upsert_artifact(
            session,
            org_run_id=org_run.id,
            mission_id=mission.id,
            artifact_type="review_note",
            title="Popper planning critique",
            content=expert_outputs["popper"],
            attributes={"expert_key": "popper", "provider_alias": _selected_alias_for_expert(mission, "popper"), "model_alias": _selected_alias_for_expert(mission, "popper"), "verification_state": "reviewed"},
        )
    if expert_outputs["woolf"]:
        await _upsert_artifact(
            session,
            org_run_id=org_run.id,
            mission_id=mission.id,
            artifact_type="operator_packet",
            title="Woolf operator packet",
            content=expert_outputs["woolf"],
            attributes={"expert_key": "woolf", "provider_alias": _selected_alias_for_expert(mission, "woolf"), "model_alias": _selected_alias_for_expert(mission, "woolf"), "verification_state": "ready"},
        )
    return {"success": True, "status": "completed", "planning": planning, "expert_outputs": expert_outputs}


def _control_summary(mission: Mission) -> ControlMissionSummary:
    payload = mission.model_dump()
    payload["recommended_steps"] = [ControlRecommendedStep(**step) for step in payload.get("recommended_steps") or []]
    payload["department_statuses"] = [ControlDepartmentStatus(**department) for department in payload.get("department_statuses") or []]
    payload["preview_hash"] = preview_hash_from_context(mission.context_snapshot or {})
    payload["complexity"] = str((mission.context_snapshot or {}).get("complexity") or "medium")
    payload["model_plan"] = model_plan_from_context(mission.context_snapshot or {})
    payload["downgrade_required"] = bool(payload["model_plan"].get("downgrade_requests"))
    return ControlMissionSummary(**payload)


def _run_payload(run: MissionRun) -> ControlMissionRunPayload:
    payload = run.model_dump()
    payload["recommended_steps"] = [ControlRecommendedStep(**step) for step in payload.get("recommended_steps") or []]
    return ControlMissionRunPayload(**payload)


def _event_payload(event: MissionEvent) -> ControlMissionEventPayload:
    return ControlMissionEventPayload(**event.model_dump())


def _org_run_payload(org_run: OrgRun) -> ControlOrgRunPayload:
    return ControlOrgRunPayload(**org_run.model_dump())


def _agent_payload(agent: AgentNode) -> AgentNodePayload:
    payload = agent.model_dump()
    attrs = agent.attributes or {}
    payload["expert_key"] = attrs.get("expert_key")
    payload["execution_role"] = attrs.get("execution_role")
    payload["provider_alias"] = attrs.get("provider_alias")
    payload["model_alias"] = attrs.get("model_alias") or attrs.get("provider_alias")
    payload["fallback_chain"] = _safe_list(attrs.get("fallback_chain"))
    payload["cost_band"] = attrs.get("cost_band")
    return AgentNodePayload(**payload)


def _heartbeat_payload(heartbeat: AgentHeartbeat) -> AgentHeartbeatPayload:
    return AgentHeartbeatPayload(**heartbeat.model_dump())


def _work_item_payload(item: WorkItem) -> WorkItemPayload:
    payload = item.model_dump()
    meta = item.payload or {}
    payload["expert_key"] = meta.get("expert_key")
    payload["provider_alias"] = meta.get("provider_alias")
    payload["model_alias"] = meta.get("model_alias")
    payload["fallback_chain"] = _safe_list(meta.get("fallback_chain"))
    payload["verification_state"] = meta.get("verification_state")
    return WorkItemPayload(**payload)


def _debate_session_payload(session: DebateSession) -> DebateSessionPayload:
    return DebateSessionPayload(**session.model_dump())


def _debate_turn_payload(turn: DebateTurn) -> DebateTurnPayload:
    return DebateTurnPayload(**turn.model_dump())


def _review_gate_payload(gate: ReviewGate) -> ReviewGatePayload:
    return ReviewGatePayload(**gate.model_dump())


def _policy_payload(policy: PolicyVersion) -> PolicyVersionPayload:
    return PolicyVersionPayload(**policy.model_dump())


def _artifact_payload(artifact: RunArtifact) -> RunArtifactPayload:
    payload = artifact.model_dump()
    attrs = artifact.attributes or {}
    payload["expert_key"] = attrs.get("expert_key")
    payload["provider_alias"] = attrs.get("provider_alias")
    payload["model_alias"] = attrs.get("model_alias")
    payload["verification_state"] = attrs.get("verification_state")
    return RunArtifactPayload(**payload)


def _execution_attempt_payload(attempt: ExecutionAttempt) -> ExecutionAttemptPayload:
    payload = attempt.model_dump()
    payload["metadata"] = payload.pop("execution_metadata", {})
    return ExecutionAttemptPayload(**payload)


def _trigger_payload(trigger: ControlTrigger) -> ControlTriggerPayload:
    return ControlTriggerPayload(**trigger.model_dump())


def _fact_pack_payload(pack: FactPack) -> FactPackPayload:
    return FactPackPayload(**pack.model_dump())


def _improvement_payload(candidate: ImprovementCandidate) -> ImprovementCandidatePayload:
    return ImprovementCandidatePayload(**candidate.model_dump())


def _learning_payload(item: LearningEvaluation) -> LearningEvaluationPayload:
    return LearningEvaluationPayload(**item.model_dump())


def _runtime_payload(*, active_org_runs: int = 0, queued_triggers: int = 0, pending_reviews: int = 0) -> ControlRuntimeStatusPayload:
    return ControlRuntimeStatusPayload(
        status=str(_RUNTIME_STATE.get("status") or "idle"),
        loop_interval_seconds=int(_RUNTIME_STATE.get("loop_interval_seconds") or _RUNTIME_LOOP_SECONDS),
        tick_count=int(_RUNTIME_STATE.get("tick_count") or 0),
        failure_count=int(_RUNTIME_STATE.get("failure_count") or 0),
        last_tick_at=_RUNTIME_STATE.get("last_tick_at"),
        last_success_at=_RUNTIME_STATE.get("last_success_at"),
        last_error_at=_RUNTIME_STATE.get("last_error_at"),
        last_error=str(_RUNTIME_STATE.get("last_error") or ""),
        active_org_runs=active_org_runs,
        queued_triggers=queued_triggers,
        pending_reviews=pending_reviews,
    )


def _waiting_for_status(status: str, detail: str = "") -> str:
    normalized = str(status or "").lower()
    if normalized in {"waiting_review", "awaiting_review"}:
        return "Operator approval"
    if normalized == "queued":
        return "Director dispatch"
    if normalized in {"running", "routing", "debating", "executing"}:
        return "Active lane"
    if normalized == "approved":
        return "Release tick"
    if normalized == "completed":
        return "Next mission"
    if normalized in {"blocked", "failed", "rejected"}:
        return detail or "Dependency or rejection"
    return detail or "System"


def _timeline_ts(value: Optional[str]) -> datetime.datetime:
    if not value:
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


def _agent_step_lookup(recommended_steps: List[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    by_department: Dict[str, List[Dict[str, Any]]] = {}
    by_owner: Dict[str, List[Dict[str, Any]]] = {}
    for step in recommended_steps:
        department = str(step.get("department") or "").strip().lower()
        owner = str(step.get("owner") or "").strip().lower()
        if department:
            by_department.setdefault(department, []).append(step)
        if owner:
            by_owner.setdefault(owner, []).append(step)
    return by_department, by_owner


def _needs_engineering(command: str) -> bool:
    lower = command.lower()
    return any(keyword in lower for keyword in _BUILD_KEYWORDS)


def _department_specialists_map() -> Dict[str, List[str]]:
    return {team: list(spec.get("specialists") or []) for team, spec in _TEAM_BLUEPRINTS.items()}


async def _get_lead_context(session: AsyncSession, lead_id: str) -> Dict[str, Any]:
    lead = await session.get(Lead, lead_id)
    if not lead:
        raise ValueError("Lead not found")

    lead_payload = lead.model_dump()
    task_result = await session.execute(
        text(
            """
            SELECT
                COUNT(*) AS total_tasks,
                COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_tasks,
                COALESCE(SUM(CASE WHEN status = 'pending' AND channel = 'call' THEN 1 ELSE 0 END), 0) AS call_tasks,
                COALESCE(SUM(CASE WHEN status = 'pending' AND channel = 'email' THEN 1 ELSE 0 END), 0) AS email_tasks,
                COALESCE(SUM(CASE WHEN status = 'pending' AND channel = 'sms' THEN 1 ELSE 0 END), 0) AS sms_tasks
            FROM tasks
            WHERE lead_id = :lead_id
            """
        ),
        {"lead_id": lead_id},
    )
    task_summary = dict(task_result.mappings().first() or {})

    appt_result = await session.execute(
        text("SELECT COUNT(*) AS total_appointments FROM appointments WHERE lead_id = :lead_id"),
        {"lead_id": lead_id},
    )
    appointment_count = int((appt_result.mappings().first() or {}).get("total_appointments") or 0)

    evidence_result = await session.execute(
        text(
            """
            SELECT
                COUNT(*) AS linked_assets,
                COALESCE(SUM(CASE WHEN ma.is_sensitive THEN 1 ELSE 0 END), 0) AS restricted_assets
            FROM lead_evidence_links lel
            LEFT JOIN mirrored_assets ma ON ma.id = lel.asset_id
            WHERE lel.lead_id = :lead_id
            """
        ),
        {"lead_id": lead_id},
    )
    evidence_summary = dict(evidence_result.mappings().first() or {})

    recent_assets_result = await session.execute(
        text(
            """
            SELECT ma.relative_path
            FROM lead_evidence_links lel
            JOIN mirrored_assets ma ON ma.id = lel.asset_id
            WHERE lel.lead_id = :lead_id
            ORDER BY COALESCE(ma.uploaded_at, ma.updated_at, ma.created_at) DESC
            LIMIT 5
            """
        ),
        {"lead_id": lead_id},
    )
    recent_assets = [row["relative_path"] for row in recent_assets_result.mappings().all()]

    workflow_result = await session.execute(
        text(
            """
            SELECT stage, price_guidance_status, authority_pack_status, market_ready, lawyer_signoff_status
            FROM listing_workflows
            WHERE lead_id = :lead_id
            """
        ),
        {"lead_id": lead_id},
    )
    workflow = dict(workflow_result.mappings().first() or {})

    return {
        "scope": "lead",
        "lead": lead_payload,
        "tasks": task_summary,
        "appointments": {"total_appointments": appointment_count},
        "evidence": {
            **evidence_summary,
            "linked_files": _safe_list(lead.linked_files),
            "source_evidence": _safe_list(lead.source_evidence),
            "recent_assets": recent_assets,
        },
        "workflow": workflow,
    }


async def _get_portfolio_context(session: AsyncSession) -> Dict[str, Any]:
    lead_rows = (await session.execute(select(Lead))).scalars().all()
    total_leads = len(lead_rows)
    open_leads = 0
    no_contact_leads = 0
    leads_with_files = 0
    hot_leads: List[Dict[str, Any]] = []

    for lead in lead_rows:
        phones = _safe_list(lead.contact_phones)
        emails = _safe_list(lead.contact_emails)
        linked_files = _safe_list(lead.linked_files)
        if str(lead.status or "captured") not in {"converted", "dropped"}:
            open_leads += 1
        if not phones and not emails:
            no_contact_leads += 1
        if linked_files:
            leads_with_files += 1
        hot_leads.append(
            {
                "id": lead.id,
                "address": lead.address,
                "owner_name": lead.owner_name,
                "suburb": lead.suburb,
                "call_today_score": int(lead.call_today_score or 0),
                "evidence_score": int(lead.evidence_score or 0),
            }
        )

    task_counts_result = await session.execute(
        text(
            """
            SELECT
                COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_tasks,
                COALESCE(SUM(CASE WHEN status = 'pending' AND channel = 'call' THEN 1 ELSE 0 END), 0) AS call_tasks,
                COALESCE(SUM(CASE WHEN status = 'pending' AND channel IN ('sms', 'email') THEN 1 ELSE 0 END), 0) AS outbound_tasks
            FROM tasks
            """
        )
    )
    task_counts = dict(task_counts_result.mappings().first() or {})

    archive_result = await session.execute(
        text(
            """
            SELECT
                COUNT(*) AS mirrored_assets,
                COALESCE(SUM(CASE WHEN upload_status = 'completed' THEN 1 ELSE 0 END), 0) AS uploaded_assets,
                COALESCE(SUM(CASE WHEN is_sensitive THEN 1 ELSE 0 END), 0) AS restricted_assets
            FROM mirrored_assets
            """
        )
    )
    archive_counts = dict(archive_result.mappings().first() or {})

    hot_leads.sort(
        key=lambda item: (
            -int(item.get("call_today_score") or 0),
            -int(item.get("evidence_score") or 0),
            str(item.get("address") or ""),
        )
    )

    return {
        "scope": "portfolio",
        "portfolio": {
            "total_leads": total_leads,
            "open_leads": open_leads,
            "no_contact_leads": no_contact_leads,
            "leads_with_files": leads_with_files,
            **task_counts,
            **archive_counts,
            "hot_leads": hot_leads[:5],
        },
    }


def _hash_payload(value: Any) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _json_hash(value: Any) -> str:
    import json

    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _pct(numerator: Any, denominator: Any) -> float:
    base = float(denominator or 0)
    if base <= 0:
        return 0.0
    return round(float(numerator or 0) / base, 4)


async def _build_bookings_scorecard(session: AsyncSession) -> Dict[str, Any]:
    lead_metrics = dict(
        (
            await session.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS lead_count,
                        COALESCE(SUM(CASE WHEN status != 'dropped' THEN 1 ELSE 0 END), 0) AS active_leads,
                        COALESCE(SUM(CASE WHEN status = 'outreach_ready' THEN 1 ELSE 0 END), 0) AS ready_leads,
                        COALESCE(SUM(CASE WHEN contact_phones IS NOT NULL AND contact_phones != '[]' AND contact_phones != '' THEN 1 ELSE 0 END), 0) AS with_phone,
                        COALESCE(SUM(CASE WHEN linked_files != '[]' OR source_evidence != '[]' THEN 1 ELSE 0 END), 0) AS with_evidence,
                        COALESCE(SUM(CASE WHEN confidence_score > 0 THEN 1 ELSE 0 END), 0) AS with_confidence,
                        COALESCE(SUM(CASE WHEN readiness_score > 0 THEN 1 ELSE 0 END), 0) AS with_readiness,
                        COALESCE(SUM(CASE WHEN conversion_score > 0 THEN 1 ELSE 0 END), 0) AS with_conversion,
                        COALESCE(SUM(CASE WHEN call_today_score >= 80 THEN 1 ELSE 0 END), 0) AS hot_call_today
                    FROM leads
                    """
                )
            )
        ).mappings().first()
        or {}
    )
    pending_metrics = dict(
        (
            await session.execute(
                text(
                    """
                    SELECT
                        COALESCE(SUM(CASE WHEN status = 'pending' AND COALESCE(superseded_by, '') = '' THEN 1 ELSE 0 END), 0) AS pending_tasks,
                        COALESCE(SUM(CASE WHEN status = 'pending' AND COALESCE(superseded_by, '') = '' AND channel = 'call' THEN 1 ELSE 0 END), 0) AS ready_to_call,
                        COALESCE(SUM(CASE WHEN status = 'pending' AND COALESCE(superseded_by, '') = '' AND due_at < :now THEN 1 ELSE 0 END), 0) AS overdue_pending_tasks,
                        COALESCE(SUM(CASE WHEN approval_status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_sends
                    FROM tasks
                    """
                ),
                {"now": now_sydney().isoformat()},
            )
        ).mappings().first()
        or {}
    )
    call_cutoff = (now_sydney().date() - datetime.timedelta(days=30)).isoformat()
    call_metrics = dict(
        (
            await session.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS calls_30d,
                        COALESCE(SUM(CASE WHEN connected IN (1) THEN 1 ELSE 0 END), 0) AS connections_30d,
                        COALESCE(SUM(CASE WHEN outcome IN ('booked_appraisal', 'booked_mortgage') THEN 1 ELSE 0 END), 0) AS bookings_30d
                    FROM call_log
                    WHERE logged_date >= :cutoff
                    """
                ),
                {"cutoff": call_cutoff},
            )
        ).mappings().first()
        or {}
    )
    archive_metrics = dict(
        (
            await session.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS mirrored_assets,
                        COALESCE(SUM(CASE WHEN upload_status = 'completed' THEN 1 ELSE 0 END), 0) AS uploaded_assets
                    FROM mirrored_assets
                    """
                )
            )
        ).mappings().first()
        or {}
    )
    last_scan_raw = (
        await session.execute(text("SELECT MAX(created_at) FROM ingest_runs"))
    ).scalar_one_or_none()
    feed_health = "ok" if last_scan_raw else "no_data"
    if last_scan_raw:
        try:
            last_scan_dt = datetime.datetime.fromisoformat(str(last_scan_raw))
            if (now_sydney() - last_scan_dt).total_seconds() > 60 * 60 * 48:
                feed_health = "stale"
        except ValueError:
            pass

    calls_30d = int(call_metrics.get("calls_30d") or 0)
    bookings_30d = int(call_metrics.get("bookings_30d") or 0)
    connections_30d = int(call_metrics.get("connections_30d") or 0)
    booking_rate_30d = _pct(bookings_30d, calls_30d)
    connection_rate_30d = _pct(connections_30d, calls_30d)
    projected_bookings_90d = round((calls_30d / 30.0) * 90.0 * booking_rate_30d) if calls_30d else 0

    lead_count = int(lead_metrics.get("lead_count") or 0)
    return {
        "lead_count": lead_count,
        "active_leads": int(lead_metrics.get("active_leads") or 0),
        "ready_leads": int(lead_metrics.get("ready_leads") or 0),
        "hot_call_today": int(lead_metrics.get("hot_call_today") or 0),
        "with_phone": int(lead_metrics.get("with_phone") or 0),
        "with_evidence": int(lead_metrics.get("with_evidence") or 0),
        "with_confidence": int(lead_metrics.get("with_confidence") or 0),
        "with_readiness": int(lead_metrics.get("with_readiness") or 0),
        "with_conversion": int(lead_metrics.get("with_conversion") or 0),
        "callable_coverage": _pct(lead_metrics.get("with_phone"), lead_count),
        "evidence_coverage": _pct(lead_metrics.get("with_evidence"), lead_count),
        "score_health": {
            "confidence": _pct(lead_metrics.get("with_confidence"), lead_count),
            "readiness": _pct(lead_metrics.get("with_readiness"), lead_count),
            "conversion": _pct(lead_metrics.get("with_conversion"), lead_count),
        },
        "pending_tasks": int(pending_metrics.get("pending_tasks") or 0),
        "ready_to_call": int(pending_metrics.get("ready_to_call") or 0),
        "overdue_pending_tasks": int(pending_metrics.get("overdue_pending_tasks") or 0),
        "failed_sends": int(pending_metrics.get("failed_sends") or 0),
        "send_failure_rate": _pct(pending_metrics.get("failed_sends"), pending_metrics.get("pending_tasks")),
        "calls_30d": calls_30d,
        "connections_30d": connections_30d,
        "bookings_30d": bookings_30d,
        "connection_rate_30d": connection_rate_30d,
        "booking_rate_30d": booking_rate_30d,
        "projected_bookings_90d": projected_bookings_90d,
        "feed_health": feed_health,
        "last_scan": str(last_scan_raw or ""),
        "archive_assets": int(archive_metrics.get("mirrored_assets") or 0),
        "archive_uploaded_assets": int(archive_metrics.get("uploaded_assets") or 0),
        "archive_upload_gap": max(
            0,
            int(archive_metrics.get("mirrored_assets") or 0) - int(archive_metrics.get("uploaded_assets") or 0),
        ),
    }


def _team_facts_for_lead(context: Dict[str, Any]) -> Dict[str, Any]:
    lead = context.get("lead") or {}
    evidence = context.get("evidence") or {}
    tasks = context.get("tasks") or {}
    appointments = context.get("appointments") or {}
    workflow = context.get("workflow") or {}
    return {
        "lead_id": str(lead.get("id") or ""),
        "address": str(lead.get("address") or lead.get("suburb") or "Lead"),
        "has_phone": bool(_safe_list(lead.get("contact_phones"))),
        "has_email": bool(_safe_list(lead.get("contact_emails"))),
        "linked_assets": int(evidence.get("linked_assets") or 0),
        "pending_calls": int(tasks.get("call_tasks") or 0),
        "appointments": int(appointments.get("total_appointments") or 0),
        "call_today_score": int(lead.get("call_today_score") or 0),
        "last_outcome": str(lead.get("last_outcome") or ""),
        "workflow_stage": str(workflow.get("stage") or ""),
        "authority_pack_status": str(workflow.get("authority_pack_status") or ""),
    }


def _team_blueprint(team_name: str) -> Dict[str, Any]:
    spec = _TEAM_BLUEPRINTS[team_name]
    return {"department": team_name, "head": spec["head"], "specialists": list(spec["specialists"]), "summary": spec["summary"]}


def _department_from_team(
    team_name: str,
    *,
    status: str,
    summary: str,
    findings: List[str],
    recommended_steps: List[Dict[str, Any]],
    confidence: float,
    expected_booking_lift: float,
    execution_mode: str = "rules",
) -> Dict[str, Any]:
    spec = _team_blueprint(team_name)
    payload = ControlDepartmentStatus(
        department=spec["department"],
        head=spec["head"],
        specialists=spec["specialists"],
        status=status,
        summary=summary,
        findings=findings,
        recommended_steps=[ControlRecommendedStep(**step) for step in recommended_steps],
    ).model_dump()
    payload["confidence"] = round(confidence, 2)
    payload["expected_booking_lift"] = round(expected_booking_lift, 2)
    payload["execution_mode"] = execution_mode
    payload["metrics_used"] = ["bookings_30d", "projected_bookings_90d", "callable_coverage", "evidence_coverage"]
    return payload


def _pick_active_teams(command: str, context: Dict[str, Any]) -> List[str]:
    scorecard = context.get("scorecard") or {}
    trigger = context.get("control_trigger") or {}
    scope = context.get("scope")
    active: List[str] = ["Governance Team"]
    trigger_type = str(trigger.get("trigger_type") or "")
    trigger_source = str(trigger.get("trigger_source") or "")
    if trigger_type in {"operator_command", "score_threshold", "outcome_logged", "funnel_regression"} or int(scorecard.get("projected_bookings_90d") or 0) <= max(3, int(scorecard.get("ready_to_call") or 0) // 20):
        active.append("Growth Team")
    if (
        float(scorecard.get("callable_coverage") or 0) < 0.35
        or float(scorecard.get("evidence_coverage") or 0) < 0.2
        or (scope == "lead" and not _team_facts_for_lead(context).get("has_phone"))
    ):
        active.append("Data Quality Team")
    if trigger_type == "source_ingested" or trigger_source in {"da_feed", "withdrawn_feed", "probate_feed", "distress_feed"} or str(scorecard.get("feed_health") or "") != "ok":
        active.append("Source Ops Team")
    if trigger_type == "stale_queue" or int(scorecard.get("overdue_pending_tasks") or 0) > 0:
        active.append("Workflow UX Team")
    if trigger_type == "system_health_regression" or float(scorecard.get("send_failure_rate") or 0) >= 0.03:
        active.append("Reliability Team")
    if _needs_engineering(command):
        active.append("Engineering")

    ordered: List[str] = []
    for name in active:
        if name not in ordered:
            ordered.append(name)
    non_governance = [team for team in ordered if team != "Governance Team"]
    if len(non_governance) > 2 and "Engineering" not in non_governance[:2]:
        non_governance = non_governance[:2]
    capped = ["Governance Team"] + non_governance
    if "Engineering" in ordered and "Engineering" not in capped:
        capped[-1] = "Engineering"
    return capped[:3]


def _booking_lift_from_scorecard(scorecard: Dict[str, Any], multiplier: float) -> float:
    return round(float(scorecard.get("projected_bookings_90d") or 0) * multiplier, 2)


def _build_dynamic_departments(command: str, context: Dict[str, Any]) -> Tuple[str, str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    scorecard = context.get("scorecard") or {}
    trigger = context.get("control_trigger") or {}
    teams = _pick_active_teams(command, context)
    departments: List[Dict[str, Any]] = []
    lead_facts = _team_facts_for_lead(context) if context.get("scope") == "lead" else {}
    hot_leads = (context.get("portfolio") or {}).get("hot_leads") or []
    hot_targets = ", ".join(str(item.get("address") or item.get("owner_name") or "lead") for item in hot_leads[:3])

    for team_name in teams:
        findings: List[str] = []
        steps: List[Dict[str, Any]] = []
        confidence = 0.7
        expected_booking_lift = 0.0
        execution_mode = "rules"
        if team_name == "Growth Team":
            findings = [
                f"Bookings in last 30 days: {int(scorecard.get('bookings_30d') or 0)}.",
                f"Projected bookings in next 90 days: {int(scorecard.get('projected_bookings_90d') or 0)}.",
                f"Ready-to-call queue: {int(scorecard.get('ready_to_call') or 0)} live task(s).",
            ]
            if context.get("scope") == "lead":
                findings.append(
                    f"Lead score is {lead_facts.get('call_today_score', 0)} with last outcome {lead_facts.get('last_outcome') or 'none'}."
                )
                if lead_facts.get("pending_calls", 0) == 0 and lead_facts.get("appointments", 0) == 0:
                    steps.append(
                        _step(
                            "Release appraisal-ready outreach draft",
                            "Queue Strategist",
                            team_name,
                            "High-intent lead has no live call or booked appointment yet.",
                            "critical",
                            channel="call",
                            lead_id=lead_facts.get("lead_id"),
                        )
                    )
            else:
                if hot_targets:
                    steps.append(
                        _step(
                            "Promote top ready leads into today's call wave",
                            "Queue Strategist",
                            team_name,
                            f"Highest-value near-term booking opportunities are: {hot_targets}.",
                            "critical",
                        )
                    )
                steps.append(
                    _step(
                        "Refresh no-answer follow-up sequencing",
                        "Booking Forecast Analyst",
                        team_name,
                        "Bookings-first policy should bias toward faster second-touch follow-up on warm leads.",
                        "high",
                    )
                )
            expected_booking_lift = _booking_lift_from_scorecard(scorecard, 0.12)
        elif team_name == "Data Quality Team":
            findings = [
                f"Callable coverage is {round(float(scorecard.get('callable_coverage') or 0) * 100, 1)}%.",
                f"Evidence coverage is {round(float(scorecard.get('evidence_coverage') or 0) * 100, 1)}%.",
                f"Hot score-threshold leads: {int(scorecard.get('hot_call_today') or 0)}.",
            ]
            if context.get("scope") == "lead":
                if not lead_facts.get("has_phone") and not lead_facts.get("has_email"):
                    steps.append(
                        _step(
                            "Backfill callable channel for this lead",
                            "Callable Coverage Analyst",
                            team_name,
                            "This lead cannot be worked efficiently until a live contact path exists.",
                            "critical",
                            lead_id=lead_facts.get("lead_id"),
                        )
                    )
                if int(lead_facts.get("linked_assets") or 0) == 0:
                    steps.append(
                        _step(
                            "Attach source evidence to this lead",
                            "Evidence Coverage Analyst",
                            team_name,
                            "Booking-facing recommendations should be evidence-backed.",
                            "high",
                            lead_id=lead_facts.get("lead_id"),
                        )
                    )
            else:
                steps.append(
                    _step(
                        "Target enrichment on hot callable-gap cohort",
                        "Callable Coverage Analyst",
                        team_name,
                        "Improve bookings by fixing contactability on the highest call-today leads first.",
                        "high",
                    )
                )
                steps.append(
                    _step(
                        "Fill evidence gaps on the booking cohort",
                        "Evidence Coverage Analyst",
                        team_name,
                        "Low-evidence hot leads create weak scripts and lower conversion quality.",
                        "medium",
                    )
                )
            expected_booking_lift = _booking_lift_from_scorecard(scorecard, 0.08)
        elif team_name == "Source Ops Team":
            findings = [
                f"Feed health is {scorecard.get('feed_health') or 'unknown'}.",
                f"Archive upload gap is {int(scorecard.get('archive_upload_gap') or 0)} asset(s).",
                f"Trigger source is {trigger.get('trigger_source') or 'system'}.",
            ]
            steps.append(
                _step(
                    "Recover stale or underperforming signal sources",
                    "Feed Freshness Monitor",
                    team_name,
                    "Bookings-first routing depends on fresh source flow and uploaded archive coverage.",
                    "high",
                )
            )
            expected_booking_lift = _booking_lift_from_scorecard(scorecard, 0.05)
        elif team_name == "Workflow UX Team":
            findings = [
                f"Pending tasks: {int(scorecard.get('pending_tasks') or 0)}.",
                f"Overdue pending tasks: {int(scorecard.get('overdue_pending_tasks') or 0)}.",
                f"Ready-to-call backlog: {int(scorecard.get('ready_to_call') or 0)}.",
            ]
            steps.append(
                _step(
                    "Reduce overdue queue drag",
                    "Task Queue Analyst",
                    team_name,
                    "Bookings stall when the ready queue is buried under overdue or low-priority work.",
                    "high",
                )
            )
            expected_booking_lift = _booking_lift_from_scorecard(scorecard, 0.07)
        elif team_name == "Reliability Team":
            findings = [
                f"Send failure rate is {round(float(scorecard.get('send_failure_rate') or 0) * 100, 2)}%.",
                f"Feed health is {scorecard.get('feed_health') or 'unknown'}.",
                f"Last scan marker: {scorecard.get('last_scan') or 'none'}.",
            ]
            steps.append(
                _step(
                    "Stabilize runtime failure surfaces",
                    "Runtime Health Monitor",
                    team_name,
                    "Bookings-first automation cannot rely on fragile sender or ingest loops.",
                    "high",
                )
            )
            expected_booking_lift = _booking_lift_from_scorecard(scorecard, 0.04)
        elif team_name == "Engineering":
            findings = [
                f"Command scope references: {command.strip()}",
                "Engineering output stays review-gated; code patches never apply automatically.",
                f"Trigger reason: {trigger.get('reason') or 'operator requested product improvement'}.",
            ]
            execution_mode = "analysis"
            confidence = 0.78
            steps = [
                _step(
                    "Package the approved app improvement into an engineering brief",
                    "Build Supervisor",
                    team_name,
                    "Translate the candidate change into a bounded code/task slice with clear validation.",
                    "high",
                )
            ]
            expected_booking_lift = _booking_lift_from_scorecard(scorecard, 0.1)
        else:
            findings = [
                f"Primary objective remains bookings-first with {int(scorecard.get('bookings_30d') or 0)} bookings in the last 30 days.",
                f"Guardrails: callable coverage {round(float(scorecard.get('callable_coverage') or 0) * 100, 1)}%, evidence coverage {round(float(scorecard.get('evidence_coverage') or 0) * 100, 1)}%, feed health {scorecard.get('feed_health') or 'unknown'}.",
                "Only measurable policy changes should be promoted.",
            ]
            steps.append(
                _step(
                    "Review the top policy change candidate",
                    "Policy Gatekeeper",
                    team_name,
                    "Bookings-first changes should only ship when the guardrails remain intact.",
                    "normal",
                )
            )
            expected_booking_lift = _booking_lift_from_scorecard(scorecard, 0.03)
        departments.append(
            _department_from_team(
                team_name,
                status="active",
                summary=_TEAM_BLUEPRINTS[team_name]["summary"],
                findings=findings,
                recommended_steps=steps,
                confidence=confidence,
                expected_booking_lift=expected_booking_lift,
                execution_mode=execution_mode,
            )
        )

    director_summary = (
        f"Bookings-first director focus: protect conversion throughput while improving the app surfaces that determine "
        f"bookings. Current 30-day bookings: {int(scorecard.get('bookings_30d') or 0)}; projected 90-day bookings: "
        f"{int(scorecard.get('projected_bookings_90d') or 0)}."
    )
    consensus_plan = (
        "Consensus plan: prioritize the bottleneck most likely to raise booked appointments, keep outbound and code "
        "changes review-gated, and promote only changes that stay inside callable/evidence/feed guardrails."
    )
    recommended_steps = _sort_steps([step for department in departments for step in department.get("recommended_steps") or []])
    return director_summary, consensus_plan, departments, recommended_steps


def _guardrail_risk(scorecard: Dict[str, Any]) -> str:
    if str(scorecard.get("feed_health") or "") not in {"ok", ""}:
        return "high"
    if float(scorecard.get("callable_coverage") or 0) < 0.25 or float(scorecard.get("evidence_coverage") or 0) < 0.1:
        return "high"
    if float(scorecard.get("callable_coverage") or 0) < 0.35 or float(scorecard.get("evidence_coverage") or 0) < 0.2:
        return "medium"
    return "low"


async def _store_fact_pack(
    session: AsyncSession,
    *,
    entity_type: str,
    entity_id: str,
    payload: Dict[str, Any],
) -> Tuple[FactPack, bool]:
    fact_pack_hash = _json_hash(payload)
    existing = (
        await session.execute(
            select(FactPack)
            .where(
                FactPack.entity_type == entity_type,
                FactPack.entity_id == entity_id,
                FactPack.fact_pack_hash == fact_pack_hash,
            )
            .order_by(FactPack.created_at.desc())
            .limit(1)
        )
    ).scalars().first()
    if existing:
        existing.updated_at = now_iso()
        session.add(existing)
        return existing, True
    pack = FactPack(
        id=uuid.uuid4().hex,
        entity_type=entity_type,
        entity_id=entity_id,
        scope="control",
        fact_pack_hash=fact_pack_hash,
        payload=payload,
        source_updated_at=now_iso(),
        created_at=now_iso(),
        updated_at=now_iso(),
    )
    session.add(pack)
    return pack, False


async def queue_control_trigger(
    session: AsyncSession,
    *,
    trigger_type: str,
    trigger_source: str,
    entity_type: str,
    entity_id: str = "",
    priority: str = "normal",
    reason: str = "",
    payload: Optional[Dict[str, Any]] = None,
    cooldown_hours: Optional[int] = None,
) -> Optional[ControlTrigger]:
    now = now_sydney()
    window_hours = _TRIGGER_COOLDOWNS.get(trigger_type, 24) if cooldown_hours is None else cooldown_hours
    if window_hours > 0:
        cutoff = (now - datetime.timedelta(hours=window_hours)).isoformat()
        recent = (
            await session.execute(
                select(ControlTrigger)
                .where(
                    ControlTrigger.trigger_type == trigger_type,
                    ControlTrigger.trigger_source == trigger_source,
                    ControlTrigger.entity_type == entity_type,
                    ControlTrigger.entity_id == entity_id,
                    ControlTrigger.created_at >= cutoff,
                )
                .order_by(ControlTrigger.created_at.desc())
                .limit(1)
            )
        ).scalars().first()
        if recent:
            return None

    trigger = ControlTrigger(
        id=uuid.uuid4().hex,
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        entity_type=entity_type,
        entity_id=entity_id,
        status="queued",
        priority=priority,
        dedupe_key=uuid.uuid4().hex,
        reason=reason,
        payload=payload or {},
        cooldown_until=(now + datetime.timedelta(hours=max(window_hours, 0))).isoformat() if window_hours else None,
        created_at=now.isoformat(),
        updated_at=now.isoformat(),
    )
    session.add(trigger)
    return trigger


def _engineering_department(command: str, target_label: str, lead_id: Optional[str] = None) -> ControlDepartmentStatus:
    active = _needs_engineering(command)
    specialists = _department_specialists_map()["Engineering"]
    recommended_steps: List[ControlRecommendedStep] = []
    findings = [
        f"Command scope references: {command.strip()}",
        f"Target surface: {target_label}",
        "Engineering output stays review-gated; code patches never apply automatically.",
    ]
    status = "active" if active else "standby"
    if active:
        recommended_steps = [
            ControlRecommendedStep(
                **_step(
                    "Break the request into implementation slices",
                    "Engineering Director",
                    "Engineering",
                    "Complex build work should be decomposed into reviewable deliverables before code is touched.",
                    priority="high",
                    lead_id=lead_id,
                )
            ),
            ControlRecommendedStep(
                **_step(
                    "Spawn builder and reviewer lanes",
                    "Build Supervisor",
                    "Engineering",
                    "Patch builders and test reviewers should operate in parallel under explicit review gates.",
                    priority="high",
                    lead_id=lead_id,
                )
            ),
        ]
        findings.append("Builder sub-agents and reviewer sub-agents are expected for this command.")
    else:
        findings.append("No explicit build/programming scope detected, so Engineering stays in standby.")
    return ControlDepartmentStatus(
        department="Engineering",
        head="Head of Engineering",
        specialists=specialists,
        status=status,
        summary="Keep the org runtime and product backlog moving through reviewable implementation slices.",
        findings=findings,
        recommended_steps=recommended_steps,
    )


def _build_lead_departments(command: str, context: Dict[str, Any]) -> Tuple[str, str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    lead = context["lead"]
    tasks = context["tasks"]
    appointments = context["appointments"]
    evidence = context["evidence"]
    workflow = context["workflow"]
    lead_id = str(lead.get("id") or "")

    phones = _safe_list(lead.get("contact_phones"))
    emails = _safe_list(lead.get("contact_emails"))
    linked_files = evidence.get("linked_files") or []
    source_evidence = evidence.get("source_evidence") or []
    recent_assets = evidence.get("recent_assets") or []

    departments: List[ControlDepartmentStatus] = []

    acquisition_steps: List[Dict[str, Any]] = []
    if not phones and not emails:
        acquisition_steps.append(_step("Backfill contact channels", "Acquisition Head", "Acquisition", "Lead has no direct phone or email on file.", "critical", lead_id=lead_id))
    if int(evidence.get("linked_assets") or 0) == 0:
        acquisition_steps.append(_step("Search archive and attach source documents", "Archive Analyst", "Acquisition", "No mirrored evidence is linked to the target lead yet.", "high", lead_id=lead_id))
    departments.append(
        ControlDepartmentStatus(
            department="Acquisition",
            head="Head of Acquisition",
            specialists=_department_specialists_map()["Acquisition"],
            summary="Assess contactability and evidence coverage before pushing outreach.",
            findings=[
                f"{len(phones)} phone number(s) and {len(emails)} email(s) are on the lead.",
                f"{int(evidence.get('linked_assets') or 0)} mirrored asset(s) are already linked.",
                f"{len(linked_files)} linked file path(s) and {len(source_evidence)} evidence note(s) are stored on the lead.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in acquisition_steps],
        )
    )

    resolution_steps: List[Dict[str, Any]] = []
    if not lead.get("lat") or not lead.get("lng"):
        resolution_steps.append(_step("Resolve missing geo coordinates", "Resolution Lead", "Resolution", "Lead still has zeroed coordinates, reducing map and routing quality.", "medium", lead_id=lead_id))
    if not lead.get("owner_verified"):
        resolution_steps.append(_step("Confirm ownership/contact role", "Entity Resolver", "Resolution", "Owner verification is not complete on this lead.", "high", lead_id=lead_id))
    departments.append(
        ControlDepartmentStatus(
            department="Resolution",
            head="Head of Resolution",
            specialists=_department_specialists_map()["Resolution"],
            summary="Validate entity, address, and routing quality before deciding execution.",
            findings=[
                f"Address target is {lead.get('address') or 'unknown address'} in {lead.get('suburb') or 'unknown suburb'}.",
                f"Geo state: lat={lead.get('lat') or 0}, lng={lead.get('lng') or 0}, h3={lead.get('h3index') or 'missing'}.",
                f"Owner verified flag is {'set' if lead.get('owner_verified') else 'not set'} and contact role is {lead.get('contact_role') or 'unspecified'}.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in resolution_steps],
        )
    )

    market_steps: List[Dict[str, Any]] = []
    if workflow.get("stage") and workflow.get("authority_pack_status") not in ("ready", "sent", "signed"):
        market_steps.append(_step("Close authority-pack gaps", "Market Lead", "Market", "Listing workflow exists but the authority pack is not yet operational.", "high", lead_id=lead_id))
    if int(lead.get("call_today_score") or 0) >= 70:
        market_steps.append(_step("Prepare appraisal conversation brief", "Market Lead", "Market", "Lead scores high enough for immediate appraisal-oriented outreach.", "high", lead_id=lead_id))
    departments.append(
        ControlDepartmentStatus(
            department="Market",
            head="Head of Market Strategy",
            specialists=_department_specialists_map()["Market"],
            summary="Translate signal quality into appraisal strategy and pricing posture.",
            findings=[
                f"Trigger is {lead.get('trigger_type') or 'unspecified'} with call-today score {lead.get('call_today_score') or 0}.",
                f"Estimated value is {lead.get('est_value') or 0} and last outcome is {lead.get('last_outcome') or 'none recorded'}.",
                f"Listing workflow stage is {workflow.get('stage') or 'not started'}; authority pack status is {workflow.get('authority_pack_status') or 'n/a'}.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in market_steps],
        )
    )

    outreach_steps: List[Dict[str, Any]] = []
    if int(tasks.get("call_tasks") or 0) == 0 and int(appointments.get("total_appointments") or 0) == 0:
        outreach_steps.append(_step("Queue live call attempt", "Outreach Lead", "Outreach", "No pending call task and no appointment are attached to this lead.", "critical", channel="call", lead_id=lead_id))
    if not lead.get("next_action_title"):
        outreach_steps.append(_step("Define next operator action", "Outreach Lead", "Outreach", "Lead lacks a named next action in the workflow fields.", "medium", lead_id=lead_id))
    departments.append(
        ControlDepartmentStatus(
            department="Outreach",
            head="Head of Outreach",
            specialists=_department_specialists_map()["Outreach"],
            summary="Turn evidence and strategy into a concrete operator move.",
            findings=[
                f"{int(tasks.get('pending_tasks') or 0)} pending task(s), including {int(tasks.get('call_tasks') or 0)} call task(s).",
                f"{appointments.get('total_appointments') or 0} appointment(s) already exist for this lead.",
                f"Preferred channel is {lead.get('preferred_channel') or 'unset'} and next action is {lead.get('next_action_title') or 'not queued'}.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in outreach_steps],
        )
    )

    platform_steps: List[Dict[str, Any]] = []
    if int(evidence.get("restricted_assets") or 0) > 0:
        platform_steps.append(_step("Review restricted evidence access", "Platform Lead", "Platform", "Sensitive mirrored files are linked and should stay operator-gated.", "medium", lead_id=lead_id))
    platform_steps.append(_step("Prepare review gates for execution", "Workflow QA", "Platform", "Any outbound action or code application must pass an approval gate.", "normal", lead_id=lead_id))
    departments.append(
        ControlDepartmentStatus(
            department="Platform",
            head="Head of Platform",
            specialists=_department_specialists_map()["Platform"],
            summary="Protect evidence handling and execution gates while the plan is assembled.",
            findings=[
                f"Recent archive assets: {', '.join(recent_assets[:3]) if recent_assets else 'none linked yet'}.",
                f"Current command asks: {command.strip()}",
                "Mission persistence and approval gates are active; no outbound action executes automatically.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in platform_steps],
        )
    )

    departments.append(_engineering_department(command, str(lead.get("address") or "Lead"), lead_id=lead_id))
    all_steps = _sort_steps([step.model_dump() for department in departments for step in department.recommended_steps])
    director_summary = (
        f"Director focus: move {lead.get('address') or 'this lead'} from analysis into a defensible next step using "
        f"{len(phones)} contact channel(s), {int(evidence.get('linked_assets') or 0)} linked archive asset(s), and "
        f"{int(tasks.get('pending_tasks') or 0)} pending task(s)."
    )
    consensus_plan = (
        "Consensus plan: tighten evidence coverage first, resolve any owner or geo gaps that would weaken confidence, "
        "then convert the lead into one operator-owned next move. Approval gates remain in front of any outbound or code-affecting execution."
    )
    return director_summary, consensus_plan, [department.model_dump() for department in departments], all_steps


def _build_portfolio_departments(command: str, context: Dict[str, Any]) -> Tuple[str, str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    portfolio = context["portfolio"]
    hot_leads = portfolio.get("hot_leads") or []
    hot_addresses = [str(item.get("address") or item.get("owner_name") or "lead") for item in hot_leads[:3]]

    departments: List[ControlDepartmentStatus] = []

    acquisition_steps: List[Dict[str, Any]] = []
    if int(portfolio.get("no_contact_leads") or 0) > 0:
        acquisition_steps.append(_step("Backfill blind leads", "Acquisition Head", "Acquisition", "A segment of the portfolio has no contactable phone or email.", "critical"))
    if int(portfolio.get("uploaded_assets") or 0) < int(portfolio.get("mirrored_assets") or 0):
        acquisition_steps.append(_step("Close archive upload backlog", "Archive Analyst", "Acquisition", "Not all mirrored assets are uploaded and ready for search.", "high"))
    departments.append(
        ControlDepartmentStatus(
            department="Acquisition",
            head="Head of Acquisition",
            specialists=_department_specialists_map()["Acquisition"],
            summary="Assess the usable signal pool before expanding operator workload.",
            findings=[
                f"{int(portfolio.get('total_leads') or 0)} total lead(s), with {int(portfolio.get('open_leads') or 0)} still active.",
                f"{int(portfolio.get('no_contact_leads') or 0)} lead(s) still need contact backfill.",
                f"{int(portfolio.get('uploaded_assets') or 0)} of {int(portfolio.get('mirrored_assets') or 0)} mirrored asset(s) are fully uploaded.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in acquisition_steps],
        )
    )

    resolution_steps: List[Dict[str, Any]] = []
    if int(portfolio.get("leads_with_files") or 0) < int(portfolio.get("open_leads") or 0):
        resolution_steps.append(_step("Link archive evidence to open leads", "Resolution Lead", "Resolution", "Evidence coverage is uneven across the active book.", "high"))
    if hot_leads:
        resolution_steps.append(_step("Resolve top-lead identity gaps", "Entity Resolver", "Resolution", f"Highest-priority addresses need clean owner and address matching: {', '.join(hot_addresses)}.", "high"))
    departments.append(
        ControlDepartmentStatus(
            department="Resolution",
            head="Head of Resolution",
            specialists=_department_specialists_map()["Resolution"],
            summary="Reduce ambiguity so prioritization and outreach are tied to verified entities.",
            findings=[
                f"{int(portfolio.get('leads_with_files') or 0)} lead(s) currently have linked files.",
                f"Top ranked addresses right now: {', '.join(hot_addresses) if hot_addresses else 'none available yet'}.",
                "A portfolio mission should push verified evidence onto the hottest cohort first.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in resolution_steps],
        )
    )

    market_steps: List[Dict[str, Any]] = []
    if hot_leads:
        market_steps.append(_step("Draft appraisal ladder for hot cohort", "Market Lead", "Market", f"Top call-today candidates are ready for an appraisal-first plan: {', '.join(hot_addresses)}.", "critical"))
    if int(portfolio.get("open_leads") or 0) > 0:
        market_steps.append(_step("Segment portfolio by readiness", "Appraisal Planner", "Market", "Open leads should be split into appraisal-ready, nurture, and evidence-gap buckets.", "high"))
    departments.append(
        ControlDepartmentStatus(
            department="Market",
            head="Head of Market Strategy",
            specialists=_department_specialists_map()["Market"],
            summary="Convert the lead book into a ranked appraisal pipeline instead of a flat list.",
            findings=[
                f"{int(portfolio.get('pending_tasks') or 0)} pending task(s) are already sitting in the operator queue.",
                f"{int(portfolio.get('call_tasks') or 0)} pending call task(s) exist right now.",
                f"Current mission asks: {command.strip()}",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in market_steps],
        )
    )

    outreach_steps: List[Dict[str, Any]] = []
    if int(portfolio.get("call_tasks") or 0) < min(int(portfolio.get("open_leads") or 0), 10):
        outreach_steps.append(_step("Queue next call wave", "Outreach Lead", "Outreach", "The live call queue is smaller than the current active lead opportunity.", "critical", channel="call"))
    if int(portfolio.get("outbound_tasks") or 0) == 0:
        outreach_steps.append(_step("Prepare follow-up templates", "Follow-up Designer", "Outreach", "No pending SMS/email follow-up inventory is visible in the task queue.", "medium", channel="sms"))
    departments.append(
        ControlDepartmentStatus(
            department="Outreach",
            head="Head of Outreach",
            specialists=_department_specialists_map()["Outreach"],
            summary="Translate the ranked book into operator-owned actions and follow-up inventory.",
            findings=[
                f"{int(portfolio.get('call_tasks') or 0)} call task(s) and {int(portfolio.get('outbound_tasks') or 0)} outbound follow-up task(s) are queued.",
                f"Open portfolio size is {int(portfolio.get('open_leads') or 0)} lead(s).",
                "The control plane can recommend but does not auto-send or auto-dial.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in outreach_steps],
        )
    )

    platform_steps: List[Dict[str, Any]] = []
    if int(portfolio.get("restricted_assets") or 0) > 0:
        platform_steps.append(_step("Audit restricted archive access", "Platform Lead", "Platform", "Sensitive mirrored assets exist and need strict operator-only access.", "medium"))
    platform_steps.append(_step("Track mission approvals and outcomes", "Workflow QA", "Platform", "The portfolio plan needs a persisted record of approvals and operator execution.", "normal"))
    departments.append(
        ControlDepartmentStatus(
            department="Platform",
            head="Head of Platform",
            specialists=_department_specialists_map()["Platform"],
            summary="Keep mission outputs auditable, gated, and tied to real lead execution.",
            findings=[
                f"{int(portfolio.get('restricted_assets') or 0)} restricted asset(s) are present in the mirror.",
                "Mission persistence is enabled so debate, evidence, and approvals remain inspectable.",
                "No outbound or destructive action runs without explicit approval.",
            ],
            recommended_steps=[ControlRecommendedStep(**step) for step in platform_steps],
        )
    )

    departments.append(_engineering_department(command, "Oakville | Windsor Portfolio"))
    all_steps = _sort_steps([step.model_dump() for department in departments for step in department.recommended_steps])
    director_summary = (
        f"Director focus: convert {int(portfolio.get('open_leads') or 0)} open leads into a ranked appraisal machine, "
        f"using {int(portfolio.get('mirrored_assets') or 0)} mirrored assets and {int(portfolio.get('pending_tasks') or 0)} active tasks."
    )
    consensus_plan = (
        "Consensus plan: clean the reachable cohort, force evidence onto the highest-signal properties, "
        "then queue a disciplined call-first execution wave. Approval gates stay in front of any outbound or code-affecting move."
    )
    return director_summary, consensus_plan, [department.model_dump() for department in departments], all_steps


async def _build_context(session: AsyncSession, body: ControlMissionCommandRequest) -> Dict[str, Any]:
    requested_target = str(body.target_type or "portfolio").strip().lower()
    if requested_target == "lead":
        if not body.target_id:
            raise ValueError("Lead target requires target_id")
        context = await _get_lead_context(session, body.target_id)
    else:
        context = await _get_portfolio_context(session)
    context["scorecard"] = await _build_bookings_scorecard(session)
    return context


def _mission_target_label(context: Dict[str, Any]) -> str:
    if context.get("scope") == "lead":
        lead = context.get("lead") or {}
        address = str(lead.get("address") or "").strip()
        suburb = str(lead.get("suburb") or "").strip()
        if address and suburb and suburb.lower() not in address.lower():
            return f"{address}, {suburb}"
        return address or suburb or "Lead"
    return "Oakville | Windsor Portfolio"


def _build_blueprint(command: str, context: Dict[str, Any]) -> Tuple[str, str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    return _build_dynamic_departments(command, context)


async def _ensure_active_policy(session: AsyncSession) -> None:
    active = (
        await session.execute(
            select(PolicyVersion).where(PolicyVersion.active == True).order_by(PolicyVersion.version_no.desc()).limit(1)  # noqa: E712
        )
    ).scalars().first()
    if active:
        return
    now = now_iso()
    session.add(
        PolicyVersion(
            id=uuid.uuid4().hex,
            version_no=1,
            title="Control plane baseline policy",
            status="active",
            summary="Bookings-first baseline: autonomous research, deterministic routing, and human-gated outbound/code changes.",
            active=True,
            change_set={
                "autonomy_mode": "research_only",
                "primary_objective": "bookings_first",
                "guardrails": {
                    "callable_coverage_floor": 0.35,
                    "evidence_coverage_floor": 0.2,
                    "feed_health_required": "ok",
                },
            },
            created_at=now,
            approved_at=now,
        )
    )


async def _next_event_sequence(session: AsyncSession, run_id: str) -> int:
    last_event = (
        await session.execute(
            select(MissionEvent).where(MissionEvent.run_id == run_id).order_by(MissionEvent.sequence_no.desc()).limit(1)
        )
    ).scalars().first()
    return int(last_event.sequence_no or 0) + 1 if last_event else 1


async def _record_event(
    session: AsyncSession,
    *,
    mission_id: str,
    run_id: str,
    department: str,
    role: str,
    event_type: str,
    status: str,
    title: str,
    summary: str,
    detail: str = "",
    evidence_refs: Optional[List[str]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> MissionEvent:
    event = MissionEvent(
        id=uuid.uuid4().hex,
        mission_id=mission_id,
        run_id=run_id,
        sequence_no=await _next_event_sequence(session, run_id),
        department=department,
        role=role,
        event_type=event_type,
        status=status,
        title=title,
        summary=summary,
        detail=detail,
        evidence_refs=evidence_refs or [],
        payload=payload or {},
        created_at=now_iso(),
    )
    session.add(event)
    return event


async def _heartbeat_agent(
    session: AsyncSession,
    agent: AgentNode,
    *,
    status: str,
    current_task: str,
    detail: str,
) -> AgentHeartbeat:
    now = now_iso()
    agent.status = status
    agent.current_task = current_task
    agent.last_heartbeat_at = now
    agent.lease_expires_at = now
    agent.updated_at = now
    session.add(agent)
    heartbeat = AgentHeartbeat(
        id=uuid.uuid4().hex,
        org_run_id=agent.org_run_id,
        mission_id=agent.mission_id,
        agent_id=agent.id,
        status=status,
        queue_name=agent.queue_name,
        current_task=current_task,
        detail=detail,
        created_at=now,
    )
    session.add(heartbeat)
    return heartbeat


def _phase_instruction_for_agent(
    agent: AgentNode,
    phase: str,
    *,
    recommended_steps: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, str, str]:
    recommended_steps = recommended_steps or []
    by_department, by_owner = _agent_step_lookup(recommended_steps)
    agent_name = agent.name.strip().lower()
    department_name = agent.department.strip().lower()
    owned_steps = by_owner.get(agent_name) or []
    department_steps = by_department.get(department_name) or []
    primary_step = owned_steps[0] if owned_steps else (department_steps[0] if department_steps else None)
    primary_title = str((primary_step or {}).get("title") or "").strip()

    if phase == "routing":
        if agent.agent_type == "head":
            return ("running", f"Briefing {agent.department} lane", "Department head is assigning the mission brief.")
        return ("queued", "Waiting for department brief", "Specialist is standing by for the department brief.")

    if phase == "debating":
        if agent.agent_type == "head":
            return ("running", f"Moderating {agent.department} debate", "Department head is consolidating specialist arguments.")
        return ("running", f"Submitting {agent.department} analysis", "Specialist is contributing evidence and dissent to the debate.")

    if phase == "executing":
        if primary_title:
            return ("running", f"Packaging {primary_title}", "Agent is converting debated work into a reviewable execution packet.")
        if agent.agent_type == "head":
            return ("running", f"Packaging {agent.department} queue", "Department head is preparing execution lanes.")
        return ("queued", "Standing by for lane assignment", "No direct execution lane assigned yet.")

    if phase == "waiting_review":
        if primary_title:
            return ("waiting_review", f"Waiting on approval for {primary_title}", "Execution packet is ready and blocked on operator approval.")
        return ("completed", "Packet prepared", "No further action until the next mission or release.")

    if phase == "approved":
        if primary_title:
            return ("running", f"Releasing {primary_title}", "All blocking approvals cleared; implementation lane is being released.")
        return ("running", "Releasing approved queue", "Approvals cleared and the lane is being released.")

    if phase == "completed":
        if primary_title:
            return ("completed", f"Released {primary_title}", "Approved lane has been released and archived.")
        return ("completed", "Mission archived", "This agent has no further work on the closed mission.")

    return (agent.status or "queued", agent.current_task or "Standing by", "No phase instructions available.")


async def _heartbeat_phase_agents(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    phase: str,
    recommended_steps: Optional[List[Dict[str, Any]]] = None,
    include_director: bool = False,
) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []
    agents = await _load_agents_by_run(session, org_run.id)
    for agent in agents:
        if not include_director and agent.agent_type == "director":
            continue
        status, task, detail = _phase_instruction_for_agent(
            agent,
            phase,
            recommended_steps=recommended_steps,
        )
        heartbeat = await _heartbeat_agent(
            session,
            agent,
            status=status,
            current_task=task,
            detail=detail,
        )
        messages.append({"type": "AGENT_HEARTBEAT", "data": heartbeat.model_dump()})
    return messages


async def _broadcast_messages(messages: Iterable[Dict[str, Any]]) -> None:
    for message in messages:
        try:
            await event_manager.broadcast(message)
        except Exception:
            pass


async def _get_org_run_by_run_id(session: AsyncSession, run_id: str) -> Optional[OrgRun]:
    return (
        await session.execute(select(OrgRun).where(OrgRun.run_id == run_id).order_by(OrgRun.updated_at.desc()).limit(1))
    ).scalars().first()


def _dept_queue_name(department: str) -> str:
    return department.strip().lower().replace(" ", "_") or "control"


async def _load_agents_by_run(session: AsyncSession, org_run_id: str) -> List[AgentNode]:
    return (
        await session.execute(select(AgentNode).where(AgentNode.org_run_id == org_run_id).order_by(AgentNode.depth.asc(), AgentNode.created_at.asc()))
    ).scalars().all()


async def _load_org_run_detail(session: AsyncSession, org_run_id: str) -> ControlOrgRunDetail:
    org_run = await session.get(OrgRun, org_run_id)
    if not org_run:
        raise ValueError("Org run not found")
    mission = await session.get(Mission, org_run.mission_id)
    agent_nodes = await _load_agents_by_run(session, org_run_id)
    heartbeats = (
        await session.execute(
            select(AgentHeartbeat).where(AgentHeartbeat.org_run_id == org_run_id).order_by(AgentHeartbeat.created_at.desc()).limit(80)
        )
    ).scalars().all()
    work_items = (
        await session.execute(select(WorkItem).where(WorkItem.org_run_id == org_run_id).order_by(WorkItem.created_at.asc()))
    ).scalars().all()
    debate_sessions = (
        await session.execute(select(DebateSession).where(DebateSession.org_run_id == org_run_id).order_by(DebateSession.created_at.asc()))
    ).scalars().all()
    debate_turns = (
        await session.execute(select(DebateTurn).where(DebateTurn.org_run_id == org_run_id).order_by(DebateTurn.created_at.asc()))
    ).scalars().all()
    review_gates = (
        await session.execute(select(ReviewGate).where(ReviewGate.org_run_id == org_run_id).order_by(ReviewGate.created_at.asc()))
    ).scalars().all()
    policy_versions = (
        await session.execute(
            select(PolicyVersion)
            .where((PolicyVersion.org_run_id == org_run_id) | (PolicyVersion.active == True))  # noqa: E712
            .order_by(PolicyVersion.version_no.desc(), PolicyVersion.created_at.desc())
        )
    ).scalars().all()
    artifacts = (
        await session.execute(select(RunArtifact).where(RunArtifact.org_run_id == org_run_id).order_by(RunArtifact.created_at.asc()))
    ).scalars().all()
    execution_attempts = (
        await session.execute(
            select(ExecutionAttempt).where(ExecutionAttempt.org_run_id == org_run_id).order_by(ExecutionAttempt.started_at.desc(), ExecutionAttempt.updated_at.desc())
        )
    ).scalars().all()
    triggers = (
        await session.execute(
            select(ControlTrigger)
            .where((ControlTrigger.mission_id == org_run.mission_id) | (ControlTrigger.entity_type == (mission.target_type if mission else "portfolio")))
            .order_by(ControlTrigger.created_at.desc())
            .limit(20)
        )
    ).scalars().all()
    fact_packs = (
        await session.execute(
            select(FactPack)
            .where(
                FactPack.entity_type == (mission.target_type if mission else "portfolio"),
                FactPack.entity_id == (mission.target_id if mission and mission.target_id else ""),
            )
            .order_by(FactPack.created_at.desc())
            .limit(5)
        )
    ).scalars().all()
    improvement_candidates = (
        await session.execute(
            select(ImprovementCandidate)
            .where(ImprovementCandidate.org_run_id == org_run_id)
            .order_by(ImprovementCandidate.priority.asc(), ImprovementCandidate.created_at.asc())
        )
    ).scalars().all()
    learning_evaluations = (
        await session.execute(select(LearningEvaluation).order_by(LearningEvaluation.created_at.desc()).limit(5))
    ).scalars().all()

    return ControlOrgRunDetail(
        org_run=_org_run_payload(org_run),
        agent_nodes=[_agent_payload(agent) for agent in agent_nodes],
        heartbeats=[_heartbeat_payload(heartbeat) for heartbeat in heartbeats],
        work_items=[_work_item_payload(item) for item in work_items],
        debate_sessions=[_debate_session_payload(item) for item in debate_sessions],
        debate_turns=[_debate_turn_payload(item) for item in debate_turns],
        review_gates=[_review_gate_payload(item) for item in review_gates],
        policy_versions=[_policy_payload(item) for item in policy_versions],
        artifacts=[_artifact_payload(item) for item in artifacts],
        execution_attempts=[_execution_attempt_payload(item) for item in execution_attempts],
        triggers=[_trigger_payload(item) for item in triggers],
        fact_packs=[_fact_pack_payload(item) for item in fact_packs],
        improvement_candidates=[_improvement_payload(item) for item in improvement_candidates],
        learning_evaluations=[_learning_payload(item) for item in learning_evaluations],
    )


def _resolve_agent_for_step(agents: List[AgentNode], step: Dict[str, Any]) -> Optional[AgentNode]:
    owner = str(step.get("owner") or "").strip().lower()
    department = str(step.get("department") or "").strip().lower()
    for agent in agents:
        if agent.name.strip().lower() == owner:
            return agent
    for agent in agents:
        if agent.department.strip().lower() == department and agent.agent_type in {"head", "specialist"}:
            return agent
    return None


async def _seed_department_agents(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    mission: Mission,
    departments: List[Dict[str, Any]],
    director: AgentNode,
) -> List[AgentNode]:
    existing_agents = await _load_agents_by_run(session, org_run.id)
    existing_by_key = {str((agent.attributes or {}).get("expert_key") or "").lower(): agent for agent in existing_agents}
    created_agents: List[AgentNode] = []
    
    # Roster of experts to consider seeding
    experts_to_seed = _mission_model_plan(mission).get("experts") or []
    
    # We'll seed in passes to ensure parents exist before children
    # Level 0 (Director) already exists.
    # Level 1: Heads (parent_key == 'turing')
    # Level 2: Geniuses (parent_key == head_key)
    
    for depth in [1, 2]:
        for expert in experts_to_seed:
            expert_key = str(expert.get("expert_key") or "").lower()
            if expert_key in existing_by_key:
                continue
            
            parent_key = str(expert.get("parent_key") or "").lower()
            # If it's a head, its parent is turing. If it's a genius, its parent is a head.
            # Only seed if parent already exists in existing_by_key
            if parent_key not in existing_by_key:
                # If parent_key is None or empty, it might be a root agent (but turing is our only root)
                if not parent_key and depth == 1:
                     parent_key = "turing" # Default to turing for heads if not set
                else:
                     continue
            
            parent_node = existing_by_key[parent_key]
            expert_name = str(expert.get("name") or expert_key.title())
            
            node = AgentNode(
                id=uuid.uuid4().hex,
                org_run_id=org_run.id,
                mission_id=mission.id,
                parent_id=parent_node.id,
                name=expert_name,
                agent_type="head" if depth == 1 else "expert",
                department=str(expert.get("department") or expert_name),
                role=str(expert.get("role") or "expert"),
                model=str(expert.get("planned_model") or expert.get("provider_alias") or "unknown"),
                capability_tags=[expert_name.lower(), expert_key],
                status="queued",
                queue_name=_dept_queue_name(expert_name),
                depth=depth,
                attributes={
                    "expert_key": expert_key,
                    "execution_role": expert.get("role"),
                    "provider_alias": expert.get("provider_alias"),
                    "model_alias": expert.get("model_alias") or expert.get("provider_alias"),
                    "fallback_chain": expert.get("fallback_chain") or [],
                    "cost_band": expert.get("cost_band"),
                    "purpose": expert.get("purpose"),
                    "parent_key": parent_key,
                },
                created_at=now_iso(),
                updated_at=now_iso(),
            )
            session.add(node)
            created_agents.append(node)
            existing_by_key[expert_key] = node
            parent_node.spawned_children += 1
            session.add(parent_node)

    return created_agents


async def _ensure_debate_and_turns(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    mission: Mission,
    director_summary: str,
    consensus_plan: str,
    departments: List[Dict[str, Any]],
    context: Dict[str, Any],
) -> DebateSession:
    debate_session = (
        await session.execute(
            select(DebateSession).where(DebateSession.org_run_id == org_run.id).order_by(DebateSession.created_at.asc()).limit(1)
        )
    ).scalars().first()
    now = now_iso()
    if not debate_session:
        debate_session = DebateSession(
            id=uuid.uuid4().hex,
            org_run_id=org_run.id,
            mission_id=mission.id,
            title="Director debate",
            topic=mission.command_text,
            status="active",
            created_at=now,
            updated_at=now,
        )
        session.add(debate_session)

    existing_turns = (
        await session.execute(select(DebateTurn).where(DebateTurn.debate_id == debate_session.id))
    ).scalars().all()
    if existing_turns:
        return debate_session

    agents = await _load_agents_by_run(session, org_run.id)
    expert_outputs = (mission.context_snapshot or {}).get("expert_outputs") or {}
    turn_index = 1
    
    # Sort agents by depth so debate flows reasonably in the log
    sorted_agents = sorted(agents, key=lambda a: a.depth)
    
    for agent in sorted_agents:
        expert_key = str((agent.attributes or {}).get("expert_key") or "").lower()
        content = str(expert_outputs.get(expert_key) or "")
        
        # In V0, if we don't have explicit output for an expert yet, we might skip or use a placeholder
        # However, Turing (Director) always has the planning output.
        if not content and agent.agent_type != "director":
            continue
            
        role = agent.role or "expert"
        stance = "brief" if agent.agent_type == "director" else "position"
        claim_type = "proposal" if agent.agent_type == "director" else "fact"
        
        if expert_key == "popper":
            stance = "critique"
            
        session.add(
            DebateTurn(
                id=uuid.uuid4().hex,
                debate_id=debate_session.id,
                org_run_id=org_run.id,
                mission_id=mission.id,
                agent_id=agent.id,
                department=agent.department,
                role=role,
                stance=stance,
                claim_type=claim_type,
                content=content or director_summary,
                evidence_refs=_safe_list((context.get("evidence") or {}).get("recent_assets")),
                turn_index=turn_index,
                created_at=now,
            )
        )
        turn_index += 1

    # Add a final consensus turn from Turing
    director_agent = next((agent for agent in agents if agent.agent_type == "director"), None)
    if director_agent:
        session.add(
            DebateTurn(
                id=uuid.uuid4().hex,
                debate_id=debate_session.id,
                org_run_id=org_run.id,
                mission_id=mission.id,
                agent_id=director_agent.id,
                department="Turing",
                role="Mission architect",
                stance="consensus",
                claim_type="proposal",
                content=consensus_plan,
                evidence_refs=_safe_list((context.get("evidence") or {}).get("recent_assets")),
                turn_index=turn_index,
                created_at=now,
            )
        )
    debate_session.status = "completed"
    debate_session.consensus_summary = consensus_plan
    debate_session.dissent_summary = str(expert_outputs.get("popper") or "No blocking dissent recorded.")
    debate_session.updated_at = now
    debate_session.completed_at = now
    session.add(debate_session)
    return debate_session


async def _ensure_mission_reports(
    session: AsyncSession,
    *,
    mission: Mission,
    run: MissionRun,
    director_summary: str,
    consensus_plan: str,
    departments: List[Dict[str, Any]],
    recommended_steps: List[Dict[str, Any]],
    context: Dict[str, Any],
) -> None:
    existing_count = (
        await session.execute(
            select(MissionEvent).where(MissionEvent.run_id == run.id, MissionEvent.event_type == "department_report")
        )
    ).scalars().all()
    if existing_count:
        return
    await _record_event(
        session,
        mission_id=mission.id,
        run_id=run.id,
        department="Director",
        role="Director",
        event_type="mission_brief",
        status="completed",
        title="Mission decomposed",
        summary=director_summary,
        detail=consensus_plan,
        evidence_refs=_safe_list((context.get("evidence") or {}).get("recent_assets")),
        payload={"target_label": mission.target_label, "target_type": mission.target_type, "requested_by": mission.requested_by},
    )
    for department in departments:
        await _record_event(
            session,
            mission_id=mission.id,
            run_id=run.id,
            department=str(department.get("department") or "Department"),
            role=str(department.get("head") or "Department Head"),
            event_type="department_report",
            status=str(department.get("status") or "completed"),
            title=f"{department.get('department') or 'Department'} report",
            summary=str(department.get("summary") or ""),
            detail="\n".join(str(item) for item in (department.get("findings") or [])),
            payload=department,
        )
    if recommended_steps:
        await _record_event(
            session,
            mission_id=mission.id,
            run_id=run.id,
            department="Director",
            role="Director",
            event_type="approval_gate",
            status="awaiting_approval",
            title="Approval required before execution",
            summary=f"{len(recommended_steps)} recommended step(s) are staged for operator approval.",
            detail="The control system proposes actions, code lanes, and drafts, but it does not auto-send or auto-apply.",
            payload={"recommended_step_ids": [step.get("id") for step in recommended_steps]},
        )


async def _ensure_improvement_candidates(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    mission: Mission,
    departments: List[Dict[str, Any]],
) -> List[ImprovementCandidate]:
    existing = (
        await session.execute(select(ImprovementCandidate).where(ImprovementCandidate.org_run_id == org_run.id))
    ).scalars().all()
    if existing:
        return existing

    scorecard = (mission.context_snapshot or {}).get("scorecard") or {}
    now = now_iso()
    created: List[ImprovementCandidate] = []
    for department in departments:
        team_name = str(department.get("department") or "")
        if not team_name or team_name == "Governance Team":
            continue
        steps = department.get("recommended_steps") or []
        lead_step = steps[0] if steps else None
        candidate = ImprovementCandidate(
            id=uuid.uuid4().hex,
            org_run_id=org_run.id,
            mission_id=mission.id,
            team=team_name,
            title=str((lead_step or {}).get("title") or f"{team_name} improvement slate"),
            status="proposed",
            priority=str((lead_step or {}).get("priority") or "normal"),
            summary="\n".join(str(item) for item in (department.get("findings") or [])[:2]),
            expected_booking_lift=float(department.get("expected_booking_lift") or 0.0),
            confidence=float(department.get("confidence") or 0.0),
            guardrail_risk=_guardrail_risk(scorecard),
            payload={
                "recommended_steps": steps,
                "metrics_used": department.get("metrics_used") or [],
                "execution_mode": department.get("execution_mode") or "rules",
                "scorecard": scorecard,
            },
            created_at=now,
            updated_at=now,
        )
        session.add(candidate)
        created.append(candidate)
    return created


def _operator_packet_content(mission: Mission, candidates: List[ImprovementCandidate]) -> str:
    lines = [
        f"Objective: {mission.objective or mission.command_text}",
        f"Trigger source: {mission.trigger_source}",
        f"Budget class: {mission.budget_class}",
        "Top improvement candidates:",
    ]
    for candidate in candidates[:5]:
        lines.append(
            f"- {candidate.team}: {candidate.title} | lift={round(candidate.expected_booking_lift, 2)} | confidence={round(candidate.confidence, 2)} | risk={candidate.guardrail_risk}"
        )
    return "\n".join(lines)


def _engineering_brief_content(mission: Mission, departments: List[Dict[str, Any]]) -> str:
    engineering = next((item for item in departments if item.get("department") == "Engineering"), None)
    if not engineering:
        return "No engineering work was activated for this org run."
    lines = [
        f"Command: {mission.command_text}",
        "Engineering slices:",
    ]
    for step in engineering.get("recommended_steps") or []:
        lines.append(f"- {step.get('title')}: {step.get('reason')}")
    return "\n".join(lines)


def _learning_brief_content(scorecard: Dict[str, Any], candidates: List[ImprovementCandidate]) -> str:
    lines = [
        f"Bookings 30d: {int(scorecard.get('bookings_30d') or 0)}",
        f"Projected bookings 90d: {int(scorecard.get('projected_bookings_90d') or 0)}",
        f"Callable coverage: {round(float(scorecard.get('callable_coverage') or 0) * 100, 1)}%",
        f"Evidence coverage: {round(float(scorecard.get('evidence_coverage') or 0) * 100, 1)}%",
        "Candidate slate:",
    ]
    for candidate in candidates[:5]:
        lines.append(f"- {candidate.title} ({candidate.team})")
    return "\n".join(lines)


async def _ensure_policy_proposal(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    mission: Mission,
    candidates: List[ImprovementCandidate],
) -> Optional[PolicyVersion]:
    existing = (
        await session.execute(select(PolicyVersion).where(PolicyVersion.org_run_id == org_run.id).order_by(PolicyVersion.version_no.desc()).limit(1))
    ).scalars().first()
    if existing:
        return existing

    active_policy = (
        await session.execute(select(PolicyVersion).where(PolicyVersion.active == True).order_by(PolicyVersion.version_no.desc()).limit(1))  # noqa: E712
    ).scalars().first()
    next_version = int(active_policy.version_no if active_policy else 1) + 1
    scorecard = (mission.context_snapshot or {}).get("scorecard") or {}
    top_candidates = [
        {
            "team": candidate.team,
            "title": candidate.title,
            "expected_booking_lift": candidate.expected_booking_lift,
            "confidence": candidate.confidence,
        }
        for candidate in candidates[:3]
    ]
    policy = PolicyVersion(
        id=uuid.uuid4().hex,
        org_run_id=org_run.id,
        version_no=next_version,
        title=f"Bookings-first update for run {org_run.id[:8]}",
        status="proposed",
        summary=(
            f"Projected bookings_90d={int(scorecard.get('projected_bookings_90d') or 0)} with "
            f"callable coverage {round(float(scorecard.get('callable_coverage') or 0) * 100, 1)}%."
        ),
        active=False,
        change_set={
            "primary_objective": "bookings_first",
            "top_candidates": top_candidates,
            "guardrails": {
                "callable_coverage_floor": 0.35,
                "evidence_coverage_floor": 0.2,
                "feed_health_required": "ok",
            },
            "rollback": "Revert to previous active policy version if bookings or guardrails regress.",
        },
        created_at=now_iso(),
    )
    session.add(policy)
    return policy


async def _ensure_work_items_and_artifacts(
    session: AsyncSession,
    *,
    org_run: OrgRun,
    mission: Mission,
    recommended_steps: List[Dict[str, Any]],
    departments: List[Dict[str, Any]],
) -> Tuple[List[WorkItem], List[ReviewGate]]:
    existing_items = (
        await session.execute(select(WorkItem).where(WorkItem.org_run_id == org_run.id))
    ).scalars().all()
    if existing_items:
        existing_gates = (
            await session.execute(select(ReviewGate).where(ReviewGate.org_run_id == org_run.id))
        ).scalars().all()
        return existing_items, existing_gates

    agents = await _load_agents_by_run(session, org_run.id)
    now = now_iso()
    created_items: List[WorkItem] = []
    created_gates: List[ReviewGate] = []
    experts_by_name = {expert["name"]: expert for expert in (_mission_model_plan(mission).get("experts") or [])}
    patch_items: List[WorkItem] = []

    for step in recommended_steps:
        owner_name = str(step.get("owner") or step.get("department") or "Turing")
        expert = experts_by_name.get(owner_name) or experts_by_name.get(str(step.get("department") or ""))
        expert_key = str((expert or {}).get("expert_key") or owner_name.lower())
        assigned_agent = _find_agent_by_expert_key(agents, expert_key) or _resolve_agent_for_step(agents, step)
        work_type = "analysis"
        if expert_key == "hopper":
            work_type = "code_patch"
        elif expert_key == "popper":
            work_type = "review"
        elif step.get("channel") in {"call", "email", "sms"}:
            work_type = "outreach"
            
        capability = _resolve_capability_for_step(step)
        cap_config = CAPABILITY_REGISTRY.get(capability, CAPABILITY_REGISTRY["cheap_small_text"])
        model_alias = str(cap_config.get("alias") or (expert or {}).get("model_alias") or (expert or {}).get("provider_alias") or "gemini_delegate_small")

        item = WorkItem(
            id=uuid.uuid4().hex,
            org_run_id=org_run.id,
            mission_id=mission.id,
            assigned_agent_id=assigned_agent.id if assigned_agent else None,
            department=str(expert.get("name") if expert else owner_name),
            title=str(step.get("title") or "Work item"),
            description=str(step.get("reason") or ""),
            work_type=work_type,
            status="awaiting_review" if bool(step.get("approval_required")) else "prepared",
            priority=str(step.get("priority") or "normal"),
            queue_name=_dept_queue_name(str(expert.get("name") if expert else owner_name)),
            execution_mode="llm_artifact" if work_type in {"code_patch", "review", "analysis"} else "operator_action",
            confidence=0.82 if work_type == "code_patch" else 0.7,
            expected_booking_lift=0.0,
            capability_requirement=capability,
            escalation_level=0,
            retry_count=0,
            input_context_summary=str(mission.context_snapshot.get("context_summary") or "")[:500],
            approval_required=bool(step.get("approval_required")),
            payload={
                "step_id": step.get("id"),
                "owner": owner_name,
                "lead_id": step.get("lead_id"),
                "channel": step.get("channel"),
                "trigger_source": mission.trigger_source,
                "trigger_reason": mission.trigger_reason,
                "expert_key": expert_key,
                "provider_alias": (expert or {}).get("provider_alias"),
                "model_alias": model_alias,
                "fallback_chain": (expert or {}).get("fallback_chain") or [],
                "verification_state": "pending",
            },
            created_at=now,
            updated_at=now,
        )
        session.add(item)
        created_items.append(item)
        if work_type == "code_patch":
            patch_items.append(item)

    popper = experts_by_name.get("Popper")
    popper_agent = _find_agent_by_expert_key(agents, "popper")
    if patch_items and not any(item.work_type == "review" for item in created_items):
        review_item = WorkItem(
            id=uuid.uuid4().hex,
            org_run_id=org_run.id,
            mission_id=mission.id,
            assigned_agent_id=popper_agent.id if popper_agent else None,
            department="Popper",
            title="Review patch quality and verification coverage",
            description="Validate the generated patch artifact before any apply action is considered.",
            work_type="review",
            status="prepared",
            priority="high",
            queue_name="popper",
            execution_mode="llm_artifact",
            confidence=0.8,
            expected_booking_lift=0.0,
            approval_required=False,
            depends_on_ids=[item.id for item in patch_items],
            payload={
                "expert_key": "popper",
                "provider_alias": (popper or {}).get("provider_alias"),
                "model_alias": (popper or {}).get("model_alias") or (popper or {}).get("provider_alias"),
                "fallback_chain": (popper or {}).get("fallback_chain") or [],
                "verification_state": "pending",
            },
            created_at=now,
            updated_at=now,
        )
        session.add(review_item)
        created_items.append(review_item)

    for item in created_items:
        if not item.approval_required:
            continue
        gate = ReviewGate(
            id=uuid.uuid4().hex,
            org_run_id=org_run.id,
            mission_id=mission.id,
            work_item_id=item.id,
            gate_type="code_review" if item.work_type == "code_patch" else "execution_review",
            title=f"Approve: {item.title}",
            status="pending",
            requested_by="director",
            rationale=item.description,
            payload={"department": item.department, "work_type": item.work_type},
            created_at=now,
            updated_at=now,
        )
        session.add(gate)
        created_gates.append(gate)
        item.artifact_refs = [gate.id]
        session.add(item)

    return created_items, created_gates


async def _tick_org_run(session: AsyncSession, org_run: OrgRun) -> List[Dict[str, Any]]:
    mission = await session.get(Mission, org_run.mission_id)
    if not mission:
        return []
    run = await session.get(MissionRun, org_run.run_id)
    if not run:
        return []

    messages: List[Dict[str, Any]] = []
    now = now_iso()
    context = mission.context_snapshot or {}
    director_summary = mission.director_summary or "Planning is pending."
    consensus_plan = mission.consensus_plan or "Consensus is pending."
    departments = mission.department_statuses or []
    recommended_steps = mission.recommended_steps or []

    director = (
        await session.execute(select(AgentNode).where(AgentNode.org_run_id == org_run.id, AgentNode.agent_type == "director").limit(1))
    ).scalars().first()
    if not director:
        director = AgentNode(
            id=uuid.uuid4().hex,
            org_run_id=org_run.id,
            mission_id=mission.id,
            name="Turing",
            agent_type="director",
            department="Turing",
            role="Mission architect",
            model=_selected_alias_for_expert(mission, "turing") or "openai_planner_high",
            capability_tags=["routing", "synthesis", "approval_gates", "mission_architect"],
            status="queued",
            queue_name="director",
            depth=0,
            attributes={
                "expert_key": "turing",
                "execution_role": "planning",
                "provider_alias": _planned_alias_for_expert(mission, "turing"),
                "model_alias": _selected_alias_for_expert(mission, "turing"),
                "fallback_chain": _fallback_chain_for_expert(mission, "turing"),
                "cost_band": "high",
            },
            created_at=now,
            updated_at=now,
        )
        session.add(director)
        org_run.root_agent_id = director.id

    if org_run.status == "routing":
        mission.status = "routing"
        mission.updated_at = now
        run.status = "routing"
        run.started_at = run.started_at or now
        run.updated_at = now
        org_run.current_phase = "routing"
        org_run.summary = "Turing is allocating the expert roster and generating the mission brief."
        org_run.started_at = org_run.started_at or now
        org_run.heartbeat_at = now
        org_run.updated_at = now
        created_agents = await _seed_department_agents(session, org_run=org_run, mission=mission, departments=departments, director=director)
        await _heartbeat_agent(session, director, status="running", current_task="Planning mission with expert roster", detail="Turing is generating the mission brief and lane plan.")
        team_heartbeats = await _heartbeat_phase_agents(
            session,
            org_run=org_run,
            phase="debating",
            recommended_steps=[],
        )
        planning_result = await _execute_mission_planning(session, org_run=org_run, mission=mission, run=run, agents=[director, *created_agents, *[agent for agent in await _load_agents_by_run(session, org_run.id) if agent.id != director.id]])
        if planning_result.get("status") == "awaiting_model_approval":
            mission.status = "awaiting_model_approval"
            run.status = "awaiting_model_approval"
            org_run.status = "awaiting_model_approval"
            org_run.current_phase = "awaiting_model_approval"
            org_run.summary = "Preferred model is unavailable. Waiting for downgrade approval."
            org_run.heartbeat_at = now_iso()
            org_run.updated_at = now_iso()
            session.add(mission)
            session.add(run)
            session.add(org_run)
            gate = planning_result.get("gate")
            if gate:
                messages.append({"type": "REVIEW_GATE", "data": gate.model_dump()})
            messages.append({"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}})
            return messages
        if not planning_result.get("success"):
            mission.status = "failed"
            run.status = "failed"
            org_run.status = "failed"
            org_run.current_phase = "failed"
            org_run.summary = str(planning_result.get("error") or "Mission planning failed.")
            org_run.updated_at = now_iso()
            session.add(mission)
            session.add(run)
            session.add(org_run)
            return messages

        director_summary = mission.director_summary or director_summary
        consensus_plan = mission.consensus_plan or consensus_plan
        departments = mission.department_statuses or []
        recommended_steps = mission.recommended_steps or []
        org_run.status = "debating"
        org_run.current_phase = "debating"
        org_run.summary = "Expert debate is persisted and ready for review."
        debate_session = await _ensure_debate_and_turns(session, org_run=org_run, mission=mission, director_summary=director_summary, consensus_plan=consensus_plan, departments=departments, context=context)
        await _record_event(
            session,
            mission_id=mission.id,
            run_id=run.id,
            department="Turing",
            role="Mission architect",
            event_type="debate_opened",
            status="completed",
            title="Debate session opened",
            summary=debate_session.title,
            detail=debate_session.consensus_summary or "Experts are now debating the path forward.",
            payload={"debate_id": debate_session.id},
        )
        session.add(mission)
        session.add(run)
        session.add(org_run)
        messages.append({"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}})
        for agent in created_agents[:8]:
            messages.append(
                {
                    "type": "ORG_RUN",
                    "data": {"mission_id": mission.id, "run_id": run.id, "org_run_id": org_run.id, "agent_id": agent.id, "agent_status": agent.status, "department": agent.department},
                }
            )
        messages.extend(team_heartbeats)
        return messages

    if org_run.status == "debating":
        mission.status = "executing"
        mission.updated_at = now
        mission.director_summary = director_summary
        mission.consensus_plan = consensus_plan
        mission.department_statuses = departments
        mission.recommended_steps = recommended_steps
        run.status = "executing"
        run.updated_at = now
        run.director_summary = director_summary
        run.consensus_plan = consensus_plan
        run.recommended_steps = recommended_steps
        org_run.status = "executing"
        org_run.current_phase = "executing"
        org_run.summary = "Consensus is locked. Work items and approval gates are being assembled."
        org_run.metrics = {
            "experts": len((_mission_model_plan(mission).get("experts") or [])),
            "recommended_steps": len(recommended_steps),
            "engineering_active": _needs_engineering(mission.command_text),
        }
        org_run.heartbeat_at = now
        org_run.updated_at = now
        await _ensure_mission_reports(
            session,
            mission=mission,
            run=run,
            director_summary=director_summary,
            consensus_plan=consensus_plan,
            departments=departments,
            recommended_steps=recommended_steps,
            context=context,
        )
        await _heartbeat_agent(session, director, status="running", current_task="Synthesizing debate into execution lanes", detail="Director is converting debate output into work items and approvals.")
        team_heartbeats = await _heartbeat_phase_agents(
            session,
            org_run=org_run,
            phase="executing",
            recommended_steps=recommended_steps,
        )
        session.add(mission)
        session.add(run)
        session.add(org_run)
        messages.append({"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}})
        messages.extend(team_heartbeats)
        return messages

    if org_run.status == "executing":
        work_items, review_gates = await _ensure_work_items_and_artifacts(
            session,
            org_run=org_run,
            mission=mission,
            recommended_steps=recommended_steps,
            departments=departments,
        )
        mission.status = "waiting_review"
        mission.updated_at = now
        run.status = "waiting_review"
        run.updated_at = now
        org_run.status = "waiting_review"
        org_run.current_phase = "waiting_review"
        org_run.summary = "Execution packets are prepared. Review gates are waiting for operator approval."
        org_run.metrics = {**(org_run.metrics or {}), "review_gates": len(review_gates), "work_items": len(work_items)}
        org_run.heartbeat_at = now
        org_run.updated_at = now
        await _heartbeat_agent(session, director, status="waiting_review", current_task="Holding for approval", detail="Director is waiting for review gates to be approved.")
        team_heartbeats = await _heartbeat_phase_agents(
            session,
            org_run=org_run,
            phase="waiting_review",
            recommended_steps=recommended_steps,
        )
        await _record_event(
            session,
            mission_id=mission.id,
            run_id=run.id,
            department="Woolf",
            role="Operator packet writer",
            event_type="review_gates_opened",
            status="awaiting_approval",
            title="Review gates raised",
            summary=f"{len(review_gates)} review gate(s) and {len(work_items)} work item(s) are ready for approval.",
            detail="Nothing auto-applies and no fallback model is used without approval.",
            payload={"review_gate_ids": [gate.id for gate in review_gates], "work_item_ids": [item.id for item in work_items]},
        )
        session.add(mission)
        session.add(run)
        session.add(org_run)
        messages.extend(
            [
                {"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}},
                {"type": "APPROVAL_REQUIRED", "data": {"mission_id": mission.id, "org_run_id": org_run.id, "review_gate_count": len(review_gates), "work_item_count": len(work_items)}},
            ]
        )
        for gate in review_gates:
            messages.append({"type": "REVIEW_GATE", "data": gate.model_dump()})
        for item in work_items[:12]:
            messages.append({"type": "WORK_ITEM", "data": item.model_dump()})
        messages.extend(team_heartbeats)
        return messages

    if org_run.status == "approved":
        work_items = (
            await session.execute(select(WorkItem).where(WorkItem.org_run_id == org_run.id))
        ).scalars().all()
        for item in work_items:
            if item.status == "awaiting_review":
                item.status = "approved"
                item.updated_at = now
                session.add(item)
        mission.status = "completed"
        mission.updated_at = now
        run.status = "completed"
        run.completed_at = now
        run.updated_at = now
        org_run.status = "completed"
        org_run.current_phase = "completed"
        org_run.summary = "Mission completed. Generated artifacts remain reviewable and patch apply stays explicit."
        org_run.completed_at = now
        org_run.heartbeat_at = now
        org_run.updated_at = now
        await _heartbeat_agent(session, director, status="completed", current_task="Mission released", detail="Turing closed the run after approval.")
        team_heartbeats = await _heartbeat_phase_agents(
            session,
            org_run=org_run,
            phase="completed",
            recommended_steps=recommended_steps,
        )
        await _record_event(
            session,
            mission_id=mission.id,
            run_id=run.id,
            department="Turing",
            role="Mission architect",
            event_type="mission_completed",
            status="completed",
            title="Mission released to execution",
            summary=org_run.summary,
            detail="Prepared artifacts remain inspectable; patch apply stays explicit and separate from generation.",
        )
        session.add(mission)
        session.add(run)
        session.add(org_run)
        messages.append({"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}})
        messages.extend(team_heartbeats)
        return messages

    return messages


def _command_for_trigger(trigger: ControlTrigger) -> str:
    mapping = {
        "source_ingested": "Investigate the fresh source signal, rank booking potential, and propose app improvements that raise booked appraisals.",
        "score_threshold": "Review this high-priority lead, tighten routing and evidence coverage, and prepare bookings-first follow-up drafts.",
        "outcome_logged": "Learn from the latest outreach outcome, adjust the queue strategy, and propose improvements that increase booking rate.",
        "stale_queue": "Reduce queue drag, clear overdue tasks, and improve the app workflow for faster booked appointments.",
        "funnel_regression": "Investigate the bookings regression and propose guarded app improvements that restore throughput.",
        "system_health_regression": "Investigate the reliability regression and propose guarded improvements that protect bookings and feed health.",
        "operator_command": "Review the portfolio and propose the highest-leverage bookings-first improvement actions.",
    }
    return mapping.get(trigger.trigger_type, mapping["operator_command"])


async def _create_control_mission_record(
    session: AsyncSession,
    body: ControlMissionCommandRequest,
    *,
    trigger: Optional[ControlTrigger] = None,
) -> ControlMissionDetail:
    command = body.command.strip()
    if not command:
        raise ValueError("Command is required")

    await _ensure_active_policy(session)
    context = await _build_context(session, body)
    if trigger:
        context["control_trigger"] = {
            "id": trigger.id,
            "trigger_type": trigger.trigger_type,
            "trigger_source": trigger.trigger_source,
            "reason": trigger.reason,
            "payload": trigger.payload,
        }
    entity_type = "lead" if context.get("scope") == "lead" else "portfolio"
    entity_id = str(body.target_id or "")
    fact_payload = {
        "command": command,
        "objective": (body.objective or command).strip(),
        "target_type": entity_type,
        "target_id": entity_id,
        "context": context,
        "trigger": context.get("control_trigger") or {},
    }
    fact_pack, cache_hit = await _store_fact_pack(session, entity_type=entity_type, entity_id=entity_id, payload=fact_payload)
    context["fact_pack_hash"] = fact_pack.fact_pack_hash

    context, target_label, preview = await _build_preview_for_body(session, body, context=context)
    if body.preview_hash and not preview_matches_hash(preview, body.preview_hash):
        raise ValueError("Preview hash does not match the current model plan")
    context["preview_hash"] = preview["preview_hash"]
    context["model_plan"] = preview["model_plan"]
    context["complexity"] = preview["complexity"]
    context["context_summary"] = preview["context_summary"]
    mission_id = uuid.uuid4().hex
    run_id = uuid.uuid4().hex
    org_run_id = uuid.uuid4().hex
    director_id = uuid.uuid4().hex
    now = now_iso()

    mission = Mission(
        id=mission_id,
        title=_mission_title(body, target_label),
        command_text=command,
        objective=(body.objective or command).strip(),
        target_type=entity_type,
        target_id=body.target_id if entity_type == "lead" else None,
        target_label=target_label,
        requested_by=body.requested_by.strip() or "operator",
        trigger_source=str((trigger.trigger_source if trigger else "operator_command") or "operator_command"),
        trigger_reason=str((trigger.reason if trigger else "") or ""),
        status="routing",
        priority=body.priority,
        latest_run_id=run_id,
        fact_pack_hash=fact_pack.fact_pack_hash,
        budget_class=str(preview.get("cost_band") or "medium"),
        cache_hit=cache_hit,
        llm_call_count=0,
        director_summary="Mission accepted. Turing is planning the expert roster and debate brief.",
        consensus_plan="The expert roster is assembling debate lanes automatically.",
        recommended_steps=[],
        department_statuses=[],
        context_snapshot=context,
        created_at=now,
        updated_at=now,
    )
    run = MissionRun(
        id=run_id,
        mission_id=mission_id,
        run_number=1,
        status="routing",
        started_at=now,
        objective_snapshot=mission.objective,
        director_summary=mission.director_summary,
        consensus_plan=mission.consensus_plan,
        recommended_steps=[],
        context_snapshot=context,
        updated_at=now,
    )
    org_run = OrgRun(
        id=org_run_id,
        mission_id=mission_id,
        run_id=run_id,
        status="routing",
        current_phase="routing",
        autonomy_mode=(body.autonomy_mode or "research_only"),
        root_agent_id=director_id,
        summary="Mission accepted. Turing is routing the expert roster.",
        metrics={
            "target_type": mission.target_type,
            "priority": mission.priority,
            "trigger_source": mission.trigger_source,
            "cache_hit": cache_hit,
            "fact_pack_hash": fact_pack.fact_pack_hash,
            "preview_hash": preview["preview_hash"],
            "complexity": preview["complexity"],
            "cost_band": preview["cost_band"],
            "scorecard": context.get("scorecard") or {},
        },
        queued_at=now,
        started_at=now,
        heartbeat_at=now,
        updated_at=now,
    )
    director = AgentNode(
        id=director_id,
        org_run_id=org_run_id,
        mission_id=mission_id,
        name="Turing",
        agent_type="director",
        department="Turing",
        role="Mission architect",
        model=_selected_alias_for_expert(mission, "turing") or "openai_planner_high",
        capability_tags=["routing", "synthesis", "approval_gates", "bookings_first", "turing"],
        status="running",
        queue_name="director",
        depth=0,
        attributes={
            "expert_key": "turing",
            "execution_role": "planning",
            "provider_alias": _planned_alias_for_expert(mission, "turing") or "openai_planner_high",
            "model_alias": _planned_alias_for_expert(mission, "turing") or "openai_planner_high",
            "fallback_chain": _fallback_chain_for_expert(mission, "turing"),
            "cost_band": "high",
        },
        created_at=now,
        updated_at=now,
    )

    session.add(mission)
    session.add(run)
    session.add(org_run)
    session.add(director)
    await _record_event(
        session,
        mission_id=mission.id,
        run_id=run.id,
        department="Turing",
        role="Mission architect",
        event_type="mission_started",
        status="running",
        title="Mission accepted",
        summary="The expert roster and model plan were activated immediately.",
        detail=f"Target: {mission.target_label}",
        payload={
            "org_run_id": org_run.id,
            "trigger_source": mission.trigger_source,
            "cache_hit": cache_hit,
            "fact_pack_hash": fact_pack.fact_pack_hash,
            "preview_hash": preview["preview_hash"],
            "model_plan": preview["model_plan"],
        },
    )
    if trigger:
        trigger.status = "processed"
        trigger.processed_at = now
        trigger.mission_id = mission.id
        trigger.fact_pack_hash = fact_pack.fact_pack_hash
        trigger.updated_at = now
        session.add(trigger)
    await session.commit()
    await _broadcast_messages(
        [
            {"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}},
            {"type": "AGENT_HEARTBEAT", "data": {"org_run_id": org_run.id, "mission_id": mission.id, "agent_id": director.id, "status": director.status, "current_task": "Planning mission with expert roster", "queue_name": director.queue_name, "detail": "Turing accepted the mission and started planning."}},
            {"type": "ORG_TRIGGER", "data": {"mission_id": mission.id, "trigger_source": mission.trigger_source, "fact_pack_hash": fact_pack.fact_pack_hash, "cache_hit": cache_hit}},
        ]
    )
    return await get_control_mission_detail(session, mission_id)


async def preview_control_mission(session: AsyncSession, body: ControlMissionCommandRequest) -> ControlMissionPreview:
    command = body.command.strip()
    if not command:
        raise ValueError("Command is required")
    context, target_label, preview = await _build_preview_for_body(session, body)
    return ControlMissionPreview(
        **{
            **preview,
            "target_label": target_label,
            "target_id": body.target_id,
            "target_type": str(body.target_type or "portfolio"),
            "autonomy_mode": str(body.autonomy_mode or "research_only"),
            "objective": (body.objective or command).strip(),
            "command": command,
            "context_summary": preview.get("context_summary") or summarize_context(context),
        }
    )


async def _scan_runtime_triggers(session: AsyncSession) -> None:
    scorecard = await _build_bookings_scorecard(session)
    if int(scorecard.get("hot_call_today") or 0) > 0:
        hot_leads = (
            await session.execute(
                select(Lead)
                .where(Lead.call_today_score >= 80, Lead.status.notin_(["dropped", "converted"]))
                .order_by(Lead.call_today_score.desc(), Lead.updated_at.desc())
                .limit(3)
            )
        ).scalars().all()
        for lead in hot_leads:
            await queue_control_trigger(
                session,
                trigger_type="score_threshold",
                trigger_source="lead_scanner",
                entity_type="lead",
                entity_id=str(lead.id),
                priority="high",
                reason=f"Lead crossed the bookings-first threshold at score {int(lead.call_today_score or 0)}.",
                payload={"lead_id": lead.id, "call_today_score": int(lead.call_today_score or 0)},
            )

    recent_cutoff = (now_sydney() - datetime.timedelta(hours=6)).isoformat()
    recent_sources = (
        await session.execute(
            select(Lead)
            .where(Lead.created_at >= recent_cutoff, Lead.status.notin_(["dropped"]))
            .order_by(Lead.created_at.desc())
            .limit(5)
        )
    ).scalars().all()
    for lead in recent_sources:
        trigger_type = "source_ingested" if str(lead.trigger_type or "").strip() else ""
        if not trigger_type:
            continue
        await queue_control_trigger(
            session,
            trigger_type=trigger_type,
            trigger_source="lead_ingest_scanner",
            entity_type="lead",
            entity_id=str(lead.id),
            priority="normal",
            reason=f"Fresh lead/source activity detected for {lead.address or lead.id}.",
            payload={"lead_id": lead.id, "trigger_type": lead.trigger_type or ""},
        )

    if int(scorecard.get("overdue_pending_tasks") or 0) > 0:
        await queue_control_trigger(
            session,
            trigger_type="stale_queue",
            trigger_source="queue_monitor",
            entity_type="portfolio",
            priority="high",
            reason=f"Overdue pending tasks detected: {int(scorecard.get('overdue_pending_tasks') or 0)}.",
            payload={"scorecard": scorecard},
        )
    if str(scorecard.get("feed_health") or "") != "ok" or float(scorecard.get("send_failure_rate") or 0) >= 0.03:
        await queue_control_trigger(
            session,
            trigger_type="system_health_regression",
            trigger_source="health_monitor",
            entity_type="portfolio",
            priority="high",
            reason="Feed health or sender reliability regressed below the bookings-first guardrails.",
            payload={"scorecard": scorecard},
        )


async def _maybe_record_learning_evaluation(session: AsyncSession) -> None:
    latest = (
        await session.execute(select(LearningEvaluation).order_by(LearningEvaluation.created_at.desc()).limit(1))
    ).scalars().first()
    now_dt = now_sydney()
    if latest:
        try:
            latest_dt = datetime.datetime.fromisoformat(str(latest.created_at))
            if (now_dt - latest_dt).total_seconds() < 60 * 60 * 6:
                return
        except ValueError:
            pass

    scorecard = await _build_bookings_scorecard(session)
    deltas: Dict[str, Any] = {}
    if latest:
        previous = latest.scorecard or {}
        for key in ("bookings_30d", "projected_bookings_90d", "callable_coverage", "evidence_coverage", "ready_to_call", "overdue_pending_tasks"):
            deltas[key] = round(float(scorecard.get(key) or 0) - float(previous.get(key) or 0), 4)
    summary = (
        f"Bookings-first evaluation: bookings_30d={int(scorecard.get('bookings_30d') or 0)}, "
        f"projected_bookings_90d={int(scorecard.get('projected_bookings_90d') or 0)}, "
        f"callable_coverage={round(float(scorecard.get('callable_coverage') or 0) * 100, 1)}%."
    )
    session.add(
        LearningEvaluation(
            id=uuid.uuid4().hex,
            evaluation_type="bookings_first",
            window_start=(now_dt - datetime.timedelta(days=30)).date().isoformat(),
            window_end=now_dt.date().isoformat(),
            scorecard=scorecard,
            deltas=deltas,
            summary=summary,
            created_at=now_dt.isoformat(),
        )
    )
    if latest and float(deltas.get("bookings_30d") or 0) < 0:
        await queue_control_trigger(
            session,
            trigger_type="funnel_regression",
            trigger_source="learning_evaluator",
            entity_type="portfolio",
            priority="high",
            reason="Recent learning evaluation found a bookings regression versus the last window.",
            payload={"deltas": deltas, "scorecard": scorecard},
        )


async def _process_control_triggers(session: AsyncSession) -> None:
    queued = (
        await session.execute(
            select(ControlTrigger)
            .where(ControlTrigger.status == "queued")
            .order_by(ControlTrigger.created_at.asc())
            .limit(4)
        )
    ).scalars().all()
    for trigger in queued:
        body = ControlMissionCommandRequest(
            command=_command_for_trigger(trigger),
            objective=trigger.reason or _command_for_trigger(trigger),
            target_type="lead" if trigger.entity_type == "lead" else "portfolio",
            target_id=trigger.entity_id or None,
            requested_by="system",
            priority="high" if trigger.priority in {"critical", "high"} else "normal",
            autonomy_mode="research_only",
        )
        detail = await _create_control_mission_record(session, body, trigger=trigger)
        await approve_control_mission(session, detail.mission.id, approved_by="system")


async def _control_runtime_loop() -> None:
    while True:
        tick_started_at = now_iso()
        _RUNTIME_STATE["status"] = "running"
        _RUNTIME_STATE["last_tick_at"] = tick_started_at
        _RUNTIME_STATE["tick_count"] = int(_RUNTIME_STATE.get("tick_count") or 0) + 1
        try:
            async with _async_session_factory() as session:
                await _maybe_record_learning_evaluation(session)
                await _scan_runtime_triggers(session)
                await session.commit()
                await _process_control_triggers(session)
                queued_triggers = (
                    await session.execute(select(ControlTrigger).where(ControlTrigger.status == "queued"))
                ).scalars().all()
                pending_reviews = (
                    await session.execute(
                        select(WorkItem).where(
                            WorkItem.approval_required == True,  # noqa: E712
                            WorkItem.status.in_(["awaiting_review", "awaiting_model_approval"]),
                        )
                    )
                ).scalars().all()
                org_runs = (
                    await session.execute(
                        select(OrgRun)
                        .where(OrgRun.status.in_(_ACTIVE_ORG_STATUSES | {"awaiting_model_approval"}))
                        .order_by(OrgRun.updated_at.asc(), OrgRun.queued_at.asc())
                        .limit(4)
                    )
                ).scalars().all()
                messages: List[Dict[str, Any]] = []
                for org_run in org_runs:
                    messages.extend(await _tick_org_run(session, org_run))
                if org_runs or messages:
                    await session.commit()
                    await _broadcast_messages(messages)
                runtime_status = "healthy" if (org_runs or queued_triggers or pending_reviews) else "idle"
                _RUNTIME_STATE["status"] = runtime_status
                _RUNTIME_STATE["last_success_at"] = now_iso()
                _RUNTIME_STATE["last_error"] = ""
                await _broadcast_messages(
                    [
                        {
                            "type": "CONTROL_RUNTIME",
                            "data": _runtime_payload(
                                active_org_runs=len(org_runs),
                                queued_triggers=len(queued_triggers),
                                pending_reviews=len(pending_reviews),
                            ).model_dump(),
                        }
                    ]
                )
        except asyncio.CancelledError:
            _RUNTIME_STATE["status"] = "stopped"
            raise
        except Exception as exc:
            _RUNTIME_STATE["status"] = "error"
            _RUNTIME_STATE["failure_count"] = int(_RUNTIME_STATE.get("failure_count") or 0) + 1
            _RUNTIME_STATE["last_error"] = str(exc)
            _RUNTIME_STATE["last_error_at"] = now_iso()
            _LOGGER.exception("Control runtime loop failed")
            await _broadcast_messages(
                [
                    {
                        "type": "CONTROL_RUNTIME",
                        "data": _runtime_payload().model_dump(),
                    }
                ]
            )
        await asyncio.sleep(_RUNTIME_LOOP_SECONDS)


async def _find_patch_artifact_for_review(session: AsyncSession, item: WorkItem) -> Optional[RunArtifact]:
    for dependency_id in item.depends_on_ids or []:
        dependency = await session.get(WorkItem, dependency_id)
        if not dependency:
            continue
        for artifact_id in dependency.artifact_refs or []:
            artifact = await session.get(RunArtifact, artifact_id)
            if artifact and artifact.artifact_type == "patch_artifact":
                return artifact
    return None


async def _execute_review_item(
    session: AsyncSession,
    *,
    item: WorkItem,
    mission: Mission,
    org_run: OrgRun,
    agents: List[AgentNode],
    patch_artifact: RunArtifact,
) -> Dict[str, Any]:
    result = await _run_expert(
        session,
        org_run=org_run,
        mission=mission,
        agents=agents,
        expert_key="popper",
        execution_role="patch_review",
        prompt=review_user_prompt(mission.command_text, patch_artifact.content, str(patch_artifact.attributes.get("summary") or patch_artifact.title)),
        system=review_system_prompt(),
        work_item=item,
        max_output_tokens=1400,
        resume_status="waiting_review",
    )
    if result.get("status") == "awaiting_model_approval":
        item.status = "awaiting_model_approval"
        item.updated_at = now_iso()
        session.add(item)
        return result
    if not result.get("success"):
        item.status = "failed"
        item.updated_at = now_iso()
        session.add(item)
        return result

    review_payload = parse_review_payload(str(result.get("output") or ""))
    artifact = await _upsert_artifact(
        session,
        org_run_id=org_run.id,
        mission_id=mission.id,
        artifact_type="review_artifact",
        title=f"Popper review: {item.title}",
        content=review_payload["summary"] + ("\n\nFindings:\n- " + "\n- ".join(review_payload["findings"]) if review_payload["findings"] else ""),
        attributes={
            "expert_key": "popper",
            "provider_alias": _selected_alias_for_expert(mission, "popper"),
            "model_alias": _selected_alias_for_expert(mission, "popper"),
            "verification_state": review_payload["verification_state"],
            "findings": review_payload["findings"],
            "verification_steps": review_payload["verification_steps"],
            "apply_recommendation": review_payload["apply_recommendation"],
            "source_patch_artifact_id": patch_artifact.id,
        },
    )
    attempt = result.get("attempt")
    if attempt:
        attempt.output_artifact_id = artifact.id
        attempt.updated_at = now_iso()
        session.add(attempt)
    patch_artifact.attributes = {
        **(patch_artifact.attributes or {}),
        "verification_state": review_payload["verification_state"],
        "review_artifact_id": artifact.id,
        "findings": review_payload["findings"],
        "verification_steps": review_payload["verification_steps"],
    }
    patch_artifact.updated_at = now_iso()
    session.add(patch_artifact)
    item.artifact_refs = [*item.artifact_refs, artifact.id]
    item.payload = {**(item.payload or {}), "verification_state": review_payload["verification_state"]}
    item.status = "completed"
    item.completed_at = now_iso()
    item.updated_at = now_iso()
    session.add(item)
    return {"success": True, "artifact": artifact}


async def _execute_work_item(session: AsyncSession, item: WorkItem) -> WorkItemPayload:
    mission = await session.get(Mission, item.mission_id)
    if not mission:
        raise ValueError("Mission not found")
    org_run = await session.get(OrgRun, item.org_run_id)
    if not org_run:
        raise ValueError("Org run not found")
    agents = await _load_agents_by_run(session, org_run.id)
    expert_key = str((item.payload or {}).get("expert_key") or "turing").lower()
    context_summary = _context_summary(mission)

    if item.work_type == "code_patch":
        result = await _run_expert(
            session,
            org_run=org_run,
            mission=mission,
            agents=agents,
            expert_key="hopper",
            execution_role="patch_generation",
            prompt=patch_user_prompt(mission.command_text, item.model_dump(), mission.director_summary or mission.command_text, context_summary),
            system=patch_system_prompt(),
            work_item=item,
            temperature=0.15,
            max_output_tokens=2600,
            resume_status="waiting_review",
        )
        if result.get("status") == "awaiting_model_approval":
            item.status = "awaiting_model_approval"
            item.updated_at = now_iso()
            session.add(item)
            await session.commit()
            return _work_item_payload(item)
        if not result.get("success"):
            item.status = "failed"
            item.updated_at = now_iso()
            session.add(item)
            await session.commit()
            return _work_item_payload(item)

        patch_payload = parse_patch_payload(str(result.get("output") or ""))
        artifact = await _upsert_artifact(
            session,
            org_run_id=org_run.id,
            mission_id=mission.id,
            artifact_type="patch_artifact",
            title=patch_payload["artifact_title"] or item.title,
            content=patch_payload["diff"],
            attributes={
                "expert_key": "hopper",
                "provider_alias": _selected_alias_for_expert(mission, "hopper"),
                "model_alias": _selected_alias_for_expert(mission, "hopper"),
                "verification_state": "pending_review",
                "summary": patch_payload["summary"],
                "files": patch_payload["files"],
                "verification_steps": patch_payload["verification_steps"],
                "warnings": patch_payload["warnings"],
                "work_item_id": item.id,
            },
        )
        attempt = result.get("attempt")
        if attempt:
            attempt.output_artifact_id = artifact.id
            attempt.updated_at = now_iso()
            session.add(attempt)
        item.artifact_refs = [*item.artifact_refs, artifact.id]
        item.payload = {
            **(item.payload or {}),
            "verification_state": "pending_review",
            "generated_artifact_id": artifact.id,
        }
        item.status = "completed"
        item.completed_at = now_iso()
        item.updated_at = now_iso()
        session.add(item)

        dependent_reviews = (
            await session.execute(select(WorkItem).where(WorkItem.org_run_id == item.org_run_id, WorkItem.work_type == "review"))
        ).scalars().all()
        for review_item in dependent_reviews:
            if item.id in (review_item.depends_on_ids or []) and review_item.status == "prepared":
                await _execute_review_item(session, item=review_item, mission=mission, org_run=org_run, agents=agents, patch_artifact=artifact)

        await session.commit()
        await _broadcast_messages([{"type": "WORK_ITEM", "data": item.model_dump()}, {"type": "RUN_ARTIFACT", "data": artifact.model_dump()}])
        return _work_item_payload(item)

    if item.work_type == "review":
        patch_artifact = await _find_patch_artifact_for_review(session, item)
        if not patch_artifact:
            raise ValueError("Patch artifact not found for review")
        await _execute_review_item(session, item=item, mission=mission, org_run=org_run, agents=agents, patch_artifact=patch_artifact)
        await session.commit()
        return _work_item_payload(item)

    expert_to_use = expert_key if expert_key in {"turing", "shannon", "woolf"} else "woolf"
    generic_prompt = (
        f"Execute this approved work item as a concise artifact.\nMission: {mission.command_text}\n"
        f"Work item title: {item.title}\nWork item description: {item.description}\nContext:\n{context_summary}"
    )
    system_prompt = writer_system_prompt() if expert_to_use == "woolf" else delegate_system_prompt()
    result = await _run_expert(
        session,
        org_run=org_run,
        mission=mission,
        agents=agents,
        expert_key=expert_to_use,
        execution_role="work_item_execution",
        prompt=generic_prompt,
        system=system_prompt,
        work_item=item,
        max_output_tokens=1200,
        resume_status="waiting_review",
    )
    if result.get("status") == "awaiting_model_approval":
        item.status = "awaiting_model_approval"
        item.updated_at = now_iso()
        session.add(item)
        await session.commit()
        return _work_item_payload(item)
    if not result.get("success"):
        item.status = "failed"
        item.updated_at = now_iso()
        session.add(item)
        await session.commit()
        return _work_item_payload(item)

    artifact = await _upsert_artifact(
        session,
        org_run_id=org_run.id,
        mission_id=mission.id,
        artifact_type="execution_note",
        title=f"{item.department} output: {item.title}",
        content=str(result.get("output") or ""),
        attributes={
            "expert_key": expert_to_use,
            "provider_alias": _selected_alias_for_expert(mission, expert_to_use),
            "model_alias": _selected_alias_for_expert(mission, expert_to_use),
            "verification_state": "ready",
            "work_item_id": item.id,
        },
    )
    attempt = result.get("attempt")
    if attempt:
        attempt.output_artifact_id = artifact.id
        attempt.updated_at = now_iso()
        session.add(attempt)
    item.artifact_refs = [*item.artifact_refs, artifact.id]
    item.payload = {**(item.payload or {}), "verification_state": "ready"}
    item.status = "completed"
    item.completed_at = now_iso()
    item.updated_at = now_iso()
    session.add(item)
    await session.commit()
    return _work_item_payload(item)


async def create_control_mission(session: AsyncSession, body: ControlMissionCommandRequest) -> ControlMissionDetail:
    return await _create_control_mission_record(session, body)


async def restart_control_mission(session: AsyncSession, mission_id: str, restarted_by: str = "operator") -> ControlMissionDetail:
    mission = await session.get(Mission, mission_id)
    if not mission:
        raise ValueError("Mission not found")

    source_org_run = await _get_org_run_by_run_id(session, mission.latest_run_id or "")
    body = ControlMissionCommandRequest(
        command=mission.command_text,
        title=f"Restart - {mission.title}".strip()[:160],
        objective=mission.objective or mission.command_text,
        target_type=mission.target_type or "portfolio",
        target_id=mission.target_id,
        requested_by=restarted_by or "operator",
        priority=mission.priority or "high",
        autonomy_mode=(source_org_run.autonomy_mode if source_org_run else "approve_sends_code"),
    )
    detail = await _create_control_mission_record(session, body)

    restarted = await session.get(Mission, detail.mission.id)
    restarted_run = await session.get(MissionRun, restarted.latest_run_id) if restarted and restarted.latest_run_id else None
    if restarted and restarted_run:
        now = now_iso()
        snapshot = dict(restarted.context_snapshot or {})
        snapshot["restart"] = {
            "source_mission_id": mission.id,
            "source_run_id": mission.latest_run_id,
            "source_status": mission.status,
            "source_title": mission.title,
            "requested_by": restarted_by or "operator",
            "requested_at": now,
        }
        restarted.context_snapshot = snapshot
        restarted.updated_at = now
        session.add(restarted)
        await _record_event(
            session,
            mission_id=restarted.id,
            run_id=restarted_run.id,
            department="Governance",
            role="Mission restarter",
            event_type="mission_restarted",
            status="completed",
            title="Mission restarted from prior run",
            summary=f"Restarted from mission {mission.id[:8]} by {restarted_by or 'operator'}.",
            detail=f"Source status was {mission.status}. The new run inherits the same target and autonomy mode.",
            payload={
                "source_mission_id": mission.id,
                "source_run_id": mission.latest_run_id,
                "source_status": mission.status,
                "restarted_by": restarted_by or "operator",
            },
        )
        await session.commit()
    return await get_control_mission_detail(session, detail.mission.id)


async def list_control_missions(session: AsyncSession, limit: int = 20) -> ControlMissionListResponse:
    missions = (
        await session.execute(select(Mission).order_by(Mission.created_at.desc()).limit(limit))
    ).scalars().all()
    return ControlMissionListResponse(missions=[_control_summary(mission) for mission in missions])


async def get_control_mission_detail(session: AsyncSession, mission_id: str) -> ControlMissionDetail:
    mission = await session.get(Mission, mission_id)
    if not mission:
        raise ValueError("Mission not found")

    latest_run: Optional[MissionRun] = None
    org_run: Optional[OrgRun] = None
    if mission.latest_run_id:
        latest_run = await session.get(MissionRun, mission.latest_run_id)
        org_run = await _get_org_run_by_run_id(session, mission.latest_run_id)

    if latest_run:
        events = (
            await session.execute(
                select(MissionEvent)
                .where(MissionEvent.run_id == latest_run.id)
                .order_by(MissionEvent.sequence_no.asc(), MissionEvent.created_at.asc())
            )
        ).scalars().all()
    else:
        events = (
            await session.execute(
                select(MissionEvent)
                .where(MissionEvent.mission_id == mission_id)
                .order_by(MissionEvent.sequence_no.asc(), MissionEvent.created_at.asc())
            )
        ).scalars().all()

    org_detail = await _load_org_run_detail(session, org_run.id) if org_run else ControlOrgRunDetail()
    return ControlMissionDetail(
        mission=_control_summary(mission),
        latest_run=_run_payload(latest_run) if latest_run else None,
        org_run=org_detail.org_run,
        events=[_event_payload(event) for event in events],
        agent_nodes=org_detail.agent_nodes,
        heartbeats=org_detail.heartbeats,
        work_items=org_detail.work_items,
        debate_sessions=org_detail.debate_sessions,
        debate_turns=org_detail.debate_turns,
        review_gates=org_detail.review_gates,
        policy_versions=org_detail.policy_versions,
        artifacts=org_detail.artifacts,
        execution_attempts=org_detail.execution_attempts,
        triggers=org_detail.triggers,
        fact_packs=org_detail.fact_packs,
        improvement_candidates=org_detail.improvement_candidates,
        learning_evaluations=org_detail.learning_evaluations,
    )


async def get_control_org_run_detail(session: AsyncSession, run_id: str) -> ControlOrgRunDetail:
    org_run = await _get_org_run_by_run_id(session, run_id)
    if not org_run:
        raise ValueError("Org run not found")
    return await _load_org_run_detail(session, org_run.id)


async def list_control_work_items(session: AsyncSession, mission_id: Optional[str] = None, status: Optional[str] = None) -> ControlWorkItemListResponse:
    query = select(WorkItem).order_by(WorkItem.created_at.desc())
    if mission_id:
        query = query.where(WorkItem.mission_id == mission_id)
    if status:
        query = query.where(WorkItem.status == status)
    items = (await session.execute(query.limit(100))).scalars().all()
    return ControlWorkItemListResponse(work_items=[_work_item_payload(item) for item in items])


async def get_control_live_snapshot(session: AsyncSession, limit: int = 8) -> ControlLiveSnapshot:
    live_org_statuses = _ACTIVE_ORG_STATUSES | {"waiting_review", "awaiting_model_approval"}
    active_org_runs = (
        await session.execute(
            select(OrgRun)
            .where(OrgRun.status.in_(live_org_statuses))
            .order_by(OrgRun.updated_at.desc(), OrgRun.queued_at.desc())
            .limit(max(limit, 12))
        )
    ).scalars().all()
    recent_org_runs = (
        await session.execute(
            select(OrgRun)
            .order_by(OrgRun.updated_at.desc(), OrgRun.queued_at.desc())
            .limit(max(limit * 3, 18))
        )
    ).scalars().all()

    org_runs_by_id: Dict[str, OrgRun] = {}
    for org_run in [*active_org_runs, *recent_org_runs]:
        org_runs_by_id.setdefault(org_run.id, org_run)
    org_run_ids = list(org_runs_by_id.keys())

    recent_missions_raw = (
        await session.execute(
            select(Mission).order_by(Mission.updated_at.desc(), Mission.created_at.desc()).limit(max(limit * 2, 12))
        )
    ).scalars().all()
    mission_by_id: Dict[str, Mission] = {mission.id: mission for mission in recent_missions_raw}
    for org_run in org_runs_by_id.values():
        if org_run.mission_id in mission_by_id:
            continue
        mission = await session.get(Mission, org_run.mission_id)
        if mission:
            mission_by_id[mission.id] = mission

    pending_items = (
        await session.execute(
            select(WorkItem)
            .where(
                WorkItem.approval_required == True,  # noqa: E712
                WorkItem.status.in_(["awaiting_review", "awaiting_model_approval"]),
            )
            .order_by(WorkItem.updated_at.desc(), WorkItem.created_at.desc())
            .limit(max(limit * 5, 20))
        )
    ).scalars().all()

    queued_triggers = (
        await session.execute(
            select(ControlTrigger).where(ControlTrigger.status == "queued").order_by(ControlTrigger.created_at.desc())
        )
    ).scalars().all()
    recent_triggers = (
        await session.execute(select(ControlTrigger).order_by(ControlTrigger.created_at.desc()).limit(max(limit * 2, 10)))
    ).scalars().all()

    heartbeats: List[AgentHeartbeat] = []
    agents: List[AgentNode] = []
    debate_turns: List[DebateTurn] = []
    review_gates: List[ReviewGate] = []
    mission_events: List[MissionEvent] = []
    if org_run_ids:
        heartbeats = (
            await session.execute(
                select(AgentHeartbeat)
                .where(AgentHeartbeat.org_run_id.in_(org_run_ids))
                .order_by(AgentHeartbeat.created_at.desc())
                .limit(160)
            )
        ).scalars().all()
        agents = (
            await session.execute(
                select(AgentNode)
                .where(AgentNode.org_run_id.in_(org_run_ids))
                .order_by(AgentNode.updated_at.desc(), AgentNode.depth.asc(), AgentNode.created_at.asc())
                .limit(120)
            )
        ).scalars().all()
        debate_turns = (
            await session.execute(
                select(DebateTurn)
                .where(DebateTurn.org_run_id.in_(org_run_ids))
                .order_by(DebateTurn.created_at.desc())
                .limit(80)
            )
        ).scalars().all()
        review_gates = (
            await session.execute(
                select(ReviewGate)
                .where(ReviewGate.org_run_id.in_(org_run_ids))
                .order_by(ReviewGate.updated_at.desc(), ReviewGate.created_at.desc())
                .limit(80)
            )
        ).scalars().all()
        mission_events = (
            await session.execute(
                select(MissionEvent)
                .where(MissionEvent.mission_id.in_(list(mission_by_id.keys())))
                .order_by(MissionEvent.created_at.desc(), MissionEvent.sequence_no.desc())
                .limit(100)
            )
        ).scalars().all()

    latest_heartbeat_by_agent: Dict[str, AgentHeartbeat] = {}
    for heartbeat in heartbeats:
        latest_heartbeat_by_agent.setdefault(heartbeat.agent_id, heartbeat)
    agent_by_id = {agent.id: agent for agent in agents}
    org_run_by_run_id = {org_run.run_id: org_run for org_run in org_runs_by_id.values()}

    active_missions: List[ControlMissionSummary] = []
    for org_run in active_org_runs:
        mission = mission_by_id.get(org_run.mission_id)
        if mission:
            active_missions.append(_control_summary(mission))

    agent_status_order = {"running": 0, "waiting_review": 1, "awaiting_model_approval": 2, "approved": 3, "queued": 4, "completed": 5, "failed": 6, "blocked": 7}
    live_agents: List[ControlLiveAgentPayload] = []
    for agent in agents:
        heartbeat = latest_heartbeat_by_agent.get(agent.id)
        mission = mission_by_id.get(agent.mission_id)
        org_run = org_runs_by_id.get(agent.org_run_id)
        status = str((heartbeat.status if heartbeat else agent.status) or "queued")
        current_task = str((heartbeat.current_task if heartbeat and heartbeat.current_task else agent.current_task) or "")
        detail = str((heartbeat.detail if heartbeat else "") or "")
        last_update_at = heartbeat.created_at if heartbeat else (agent.last_heartbeat_at or agent.updated_at or agent.created_at)

        visual_state = "idle"
        if status in ("running", "executing"):
            visual_state = "working"
        elif status == "debating":
            visual_state = "discussing"
        elif status in ("blocked", "error", "failed"):
            visual_state = "blocked"
        elif status in ("waiting_review", "awaiting_model_approval", "awaiting_review"):
            visual_state = "waiting_approval"
        elif status == "routing":
            visual_state = "moving"

        live_agents.append(
            ControlLiveAgentPayload(
                agent_id=agent.id,
                org_run_id=agent.org_run_id,
                mission_id=agent.mission_id,
                mission_title=mission.title if mission else "",
                mission_status=mission.status if mission else "",
                org_phase=(org_run.current_phase if org_run else ""),
                name=agent.name,
                agent_type=agent.agent_type,
                department=agent.department,
                role=agent.role,
                status=status,
                queue_name=agent.queue_name,
                current_task=current_task,
                detail=detail,
                waiting_for=_waiting_for_status(status, detail),
                last_update_at=last_update_at,
                depth=agent.depth,
                visual_state=visual_state,
                current_zone=agent.department,
                target_zone="",
                interaction_partner_id="",
            )
        )
    live_agents.sort(key=lambda agent: _timeline_ts(agent.last_update_at), reverse=True)
    live_agents.sort(key=lambda agent: (agent_status_order.get(agent.status, 99), agent.depth))

    timeline: List[ControlTimelineEntryPayload] = []
    for event in mission_events:
        mission = mission_by_id.get(event.mission_id)
        org_run = org_run_by_run_id.get(event.run_id)
        timeline.append(
            ControlTimelineEntryPayload(
                id=f"event-{event.id}",
                source_type="event",
                mission_id=event.mission_id,
                org_run_id=org_run.id if org_run else None,
                mission_title=mission.title if mission else "",
                status=event.status,
                actor_name=event.role or event.department,
                actor_role=event.role,
                department=event.department,
                title=event.title,
                detail=event.summary or event.detail,
                created_at=event.created_at,
            )
        )
    for turn in debate_turns:
        mission = mission_by_id.get(turn.mission_id)
        agent = agent_by_id.get(turn.agent_id or "")
        timeline.append(
            ControlTimelineEntryPayload(
                id=f"debate-{turn.id}",
                source_type="debate",
                mission_id=turn.mission_id,
                org_run_id=turn.org_run_id,
                mission_title=mission.title if mission else "",
                status=turn.claim_type,
                actor_name=agent.name if agent else (turn.role or turn.department),
                actor_role=turn.role,
                department=turn.department,
                title=f"{turn.stance.title()} argument",
                detail=turn.content,
                created_at=turn.created_at,
            )
        )
    for heartbeat in heartbeats[:80]:
        mission = mission_by_id.get(heartbeat.mission_id)
        agent = agent_by_id.get(heartbeat.agent_id)
        timeline.append(
            ControlTimelineEntryPayload(
                id=f"heartbeat-{heartbeat.id}",
                source_type="heartbeat",
                mission_id=heartbeat.mission_id,
                org_run_id=heartbeat.org_run_id,
                mission_title=mission.title if mission else "",
                status=heartbeat.status,
                actor_name=agent.name if agent else heartbeat.queue_name,
                actor_role=agent.role if agent else "",
                department=agent.department if agent else "",
                title=heartbeat.current_task or heartbeat.status.replace("_", " ").title(),
                detail=heartbeat.detail,
                created_at=heartbeat.created_at,
            )
        )
    for gate in review_gates:
        mission = mission_by_id.get(gate.mission_id)
        actor_name = gate.approved_by or gate.rejected_by or gate.requested_by or "Review Gate"
        timeline.append(
            ControlTimelineEntryPayload(
                id=f"gate-{gate.id}",
                source_type="review_gate",
                mission_id=gate.mission_id,
                org_run_id=gate.org_run_id,
                mission_title=mission.title if mission else "",
                status=gate.status,
                actor_name=actor_name,
                actor_role="review_gate",
                department="Governance",
                title=gate.title,
                detail=gate.rationale or gate.status.replace("_", " "),
                created_at=gate.updated_at or gate.created_at,
            )
        )
    timeline.sort(key=lambda item: _timeline_ts(item.created_at), reverse=True)

    return ControlLiveSnapshot(
        runtime=_runtime_payload(
            active_org_runs=len(active_org_runs),
            queued_triggers=len(queued_triggers),
            pending_reviews=len(pending_items),
        ),
        active_missions=active_missions,
        recent_missions=[_control_summary(mission) for mission in recent_missions_raw[:limit]],
        pending_work_items=[_work_item_payload(item) for item in pending_items],
        agents=live_agents[: max(limit * 6, 24)],
        timeline=timeline[: max(limit * 10, 40)],
        triggers=[_trigger_payload(trigger) for trigger in recent_triggers],
    )


async def approve_control_mission(session: AsyncSession, mission_id: str, approved_by: str = "operator") -> ControlMissionDetail:
    mission = await session.get(Mission, mission_id)
    if not mission:
        raise ValueError("Mission not found")

    org_run = await _get_org_run_by_run_id(session, mission.latest_run_id or "")
    latest_run = await session.get(MissionRun, mission.latest_run_id) if mission.latest_run_id else None
    if not latest_run:
        raise ValueError("Mission run not found")

    now = now_iso()
    mission.approved_at = now
    mission.updated_at = now
    latest_run.updated_at = now
    pending_downgrades: List[ReviewGate] = []
    if org_run:
        for request in (_mission_model_plan(mission).get("downgrade_requests") or []):
            expert_key = str(request.get("expert_key") or "").lower()
            if expert_key in _approved_aliases(mission):
                continue
            gate = await _ensure_downgrade_gate(
                session,
                org_run_id=org_run.id,
                mission=mission,
                work_item_id=None,
                expert_key=expert_key,
                requested_alias=str(request.get("requested_alias") or ""),
                proposed_alias=str(request.get("proposed_alias") or ""),
                reason=str(request.get("reason") or "preferred model unavailable"),
                resume_status="routing",
            )
            if gate:
                pending_downgrades.append(gate)

        if pending_downgrades:
            mission.status = "awaiting_model_approval"
            latest_run.status = "awaiting_model_approval"
            org_run.status = "awaiting_model_approval"
            org_run.current_phase = "awaiting_model_approval"
            org_run.summary = "Preferred model is unavailable. Waiting for downgrade approval."
            org_run.heartbeat_at = now
            org_run.updated_at = now
        else:
            mission.status = "routing"
            latest_run.status = "routing"
            latest_run.started_at = latest_run.started_at or now
            org_run.status = "routing"
            org_run.current_phase = "routing"
            org_run.started_at = org_run.started_at or now
            org_run.heartbeat_at = now
            org_run.updated_at = now
            org_run.summary = "Mission approved. Turing can start planning."

    session.add(mission)
    session.add(latest_run)
    if org_run:
        session.add(org_run)

    await _record_event(
        session,
        mission_id=mission.id,
        run_id=latest_run.id,
        department="Turing",
        role="Operator Approval",
        event_type="mission_approved" if not pending_downgrades else "model_downgrade_required",
        status="completed" if not pending_downgrades else "awaiting_approval",
        title="Mission approved" if not pending_downgrades else "Downgrade approval required",
        summary=f"Approved by {approved_by}.",
        detail="The mission will start planning immediately." if not pending_downgrades else "Preferred model is unavailable and requires explicit downgrade approval before execution.",
        payload={"approved_by": approved_by, "org_run_id": org_run.id if org_run else None, "downgrade_gate_ids": [gate.id for gate in pending_downgrades]},
    )
    await session.commit()
    if org_run:
        messages = [
            {"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": latest_run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}},
        ]
        for gate in pending_downgrades:
            messages.append({"type": "REVIEW_GATE", "data": gate.model_dump()})
        await _broadcast_messages(messages)
    return await get_control_mission_detail(session, mission_id)


async def approve_work_item(session: AsyncSession, work_item_id: str, approved_by: str = "operator", rationale: str = "") -> WorkItemPayload:
    item = await session.get(WorkItem, work_item_id)
    if not item:
        raise ValueError("Work item not found")
    now = now_iso()
    item.status = "approved"
    item.updated_at = now
    session.add(item)
    gates = (
        await session.execute(select(ReviewGate).where(ReviewGate.work_item_id == item.id, ReviewGate.status == "pending"))
    ).scalars().all()
    for gate in gates:
        gate.status = "approved"
        gate.approved_by = approved_by
        gate.rationale = rationale or gate.rationale
        gate.approved_at = now
        gate.updated_at = now
        session.add(gate)
    return await _execute_work_item(session, item)


async def approve_control_downgrade(session: AsyncSession, mission_id: str, approved_by: str = "operator", selected_model_alias: Optional[str] = None) -> ControlMissionDetail:
    mission = await session.get(Mission, mission_id)
    if not mission:
        raise ValueError("Mission not found")
    org_run = await _get_org_run_by_run_id(session, mission.latest_run_id or "")
    latest_run = await session.get(MissionRun, mission.latest_run_id) if mission.latest_run_id else None
    if not org_run or not latest_run:
        raise ValueError("Mission run not found")

    pending_gates = (
        await session.execute(
            select(ReviewGate).where(
                ReviewGate.mission_id == mission.id,
                ReviewGate.gate_type == "model_downgrade",
                ReviewGate.status == "pending",
            )
        )
    ).scalars().all()
    if not pending_gates:
        raise ValueError("No pending downgrade approval found")

    resume_items: List[WorkItem] = []
    now = now_iso()
    for gate in pending_gates:
        proposed_alias = selected_model_alias or str((gate.payload or {}).get("proposed_alias") or "")
        expert_key = str((gate.payload or {}).get("expert_key") or "")
        if not proposed_alias or not expert_key:
            continue
        _apply_approved_alias(mission, expert_key, proposed_alias)
        gate.status = "approved"
        gate.approved_by = approved_by
        gate.approved_at = now
        gate.updated_at = now
        session.add(gate)
        if gate.work_item_id:
            item = await session.get(WorkItem, gate.work_item_id)
            if item:
                item.status = "approved"
                item.updated_at = now
                session.add(item)
                resume_items.append(item)

    if not resume_items:
        mission.status = "routing"
        latest_run.status = "routing"
        latest_run.updated_at = now
        org_run.status = "routing"
        org_run.current_phase = "routing"
        org_run.heartbeat_at = now
        org_run.updated_at = now
        org_run.summary = "Downgrade approved. Mission planning can resume."
        session.add(mission)
        session.add(latest_run)
        session.add(org_run)
        await session.commit()
        await _broadcast_messages([{"type": "ORG_RUN", "data": {"mission_id": mission.id, "run_id": latest_run.id, "org_run_id": org_run.id, "status": org_run.status, "phase": org_run.current_phase}}])
        return await get_control_mission_detail(session, mission_id)

    await session.commit()
    for item in resume_items:
        await _execute_work_item(session, item)
    return await get_control_mission_detail(session, mission_id)


async def apply_patch_artifact(session: AsyncSession, artifact_id: str, applied_by: str = "operator") -> RunArtifactPayload:
    artifact = await session.get(RunArtifact, artifact_id)
    if not artifact:
        raise ValueError("Artifact not found")
    if artifact.artifact_type != "patch_artifact":
        raise ValueError("Only patch artifacts can be applied")
    if not artifact.content.strip():
        raise ValueError("Patch artifact is empty")

    with tempfile.NamedTemporaryFile("w", suffix=".patch", delete=False, encoding="utf-8") as handle:
        handle.write(artifact.content)
        patch_path = handle.name

    try:
        check = subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "apply", "--check", patch_path],
            capture_output=True,
            text=True,
        )
        if check.returncode != 0:
            artifact.status = "blocked"
            artifact.attributes = {
                **(artifact.attributes or {}),
                "verification_state": "apply_failed",
                "apply_error": (check.stderr or check.stdout or "").strip(),
            }
            artifact.updated_at = now_iso()
            session.add(artifact)
            await session.commit()
            return _artifact_payload(artifact)

        apply_result = subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "apply", patch_path],
            capture_output=True,
            text=True,
        )
        if apply_result.returncode != 0:
            artifact.status = "blocked"
            artifact.attributes = {
                **(artifact.attributes or {}),
                "verification_state": "apply_failed",
                "apply_error": (apply_result.stderr or apply_result.stdout or "").strip(),
            }
            artifact.updated_at = now_iso()
            session.add(artifact)
            await session.commit()
            return _artifact_payload(artifact)

        artifact.status = "applied"
        artifact.attributes = {
            **(artifact.attributes or {}),
            "verification_state": "applied",
            "applied_by": applied_by,
            "applied_at": now_iso(),
        }
        artifact.updated_at = now_iso()
        session.add(artifact)
        await session.commit()
        return _artifact_payload(artifact)
    finally:
        try:
            Path(patch_path).unlink(missing_ok=True)
        except Exception:
            pass


async def reject_work_item(session: AsyncSession, work_item_id: str, approved_by: str = "operator", rationale: str = "") -> WorkItemPayload:
    item = await session.get(WorkItem, work_item_id)
    if not item:
        raise ValueError("Work item not found")
    now = now_iso()
    item.status = "blocked"
    item.updated_at = now
    session.add(item)
    gates = (
        await session.execute(select(ReviewGate).where(ReviewGate.work_item_id == item.id, ReviewGate.status == "pending"))
    ).scalars().all()
    for gate in gates:
        gate.status = "rejected"
        gate.rejected_by = approved_by
        gate.rationale = rationale or gate.rationale
        gate.rejected_at = now
        gate.updated_at = now
        session.add(gate)
    await session.commit()
    await _broadcast_messages([{"type": "WORK_ITEM", "data": item.model_dump()}])
    return _work_item_payload(item)
