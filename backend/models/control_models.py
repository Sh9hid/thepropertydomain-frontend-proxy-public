from typing import Any, Dict, List, Optional

from sqlalchemy import JSON as SAJSON
from sqlalchemy import Column as SAColumn
from sqlmodel import Field, SQLModel


class Mission(SQLModel, table=True):
    __tablename__ = "missions"

    id: str = Field(primary_key=True)
    title: str
    command_text: str
    objective: str = Field(default="")
    target_type: str = Field(default="portfolio", index=True)
    target_id: Optional[str] = Field(default=None, index=True)
    target_label: str = Field(default="")
    requested_by: str = Field(default="operator")
    trigger_source: str = Field(default="operator_command", index=True)
    trigger_reason: str = Field(default="")
    status: str = Field(default="draft", index=True)
    priority: str = Field(default="normal", index=True)
    latest_run_id: Optional[str] = Field(default=None, index=True)
    fact_pack_hash: str = Field(default="", index=True)
    budget_class: str = Field(default="heuristic_first", index=True)
    cache_hit: bool = Field(default=False, index=True)
    llm_call_count: int = Field(default=0)
    director_summary: Optional[str] = None
    consensus_plan: Optional[str] = None
    recommended_steps: List[Dict[str, Any]] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    department_statuses: List[Dict[str, Any]] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    context_snapshot: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None
    approved_at: Optional[str] = Field(default=None, index=True)


class MissionRun(SQLModel, table=True):
    __tablename__ = "mission_runs"

    id: str = Field(primary_key=True)
    mission_id: str = Field(index=True)
    run_number: int = Field(default=1)
    status: str = Field(default="running", index=True)
    started_at: Optional[str] = Field(default=None, index=True)
    completed_at: Optional[str] = Field(default=None, index=True)
    objective_snapshot: str = Field(default="")
    director_summary: Optional[str] = None
    consensus_plan: Optional[str] = None
    recommended_steps: List[Dict[str, Any]] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    context_snapshot: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    updated_at: Optional[str] = None


class MissionEvent(SQLModel, table=True):
    __tablename__ = "mission_events"

    id: str = Field(primary_key=True)
    mission_id: str = Field(index=True)
    run_id: str = Field(index=True)
    sequence_no: int = Field(default=0, index=True)
    department: str = Field(default="director", index=True)
    role: str = Field(default="director")
    event_type: str = Field(default="note", index=True)
    status: str = Field(default="completed", index=True)
    title: str
    summary: str = Field(default="")
    detail: str = Field(default="")
    evidence_refs: List[str] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    payload: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)


class OrgRun(SQLModel, table=True):
    __tablename__ = "org_runs"

    id: str = Field(primary_key=True)
    mission_id: str = Field(index=True)
    run_id: str = Field(index=True)
    status: str = Field(default="queued", index=True)
    current_phase: str = Field(default="queued", index=True)
    autonomy_mode: str = Field(default="approve_sends_code")
    root_agent_id: Optional[str] = Field(default=None, index=True)
    summary: str = Field(default="")
    metrics: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    queued_at: Optional[str] = Field(default=None, index=True)
    started_at: Optional[str] = Field(default=None, index=True)
    heartbeat_at: Optional[str] = Field(default=None, index=True)
    completed_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class AgentNode(SQLModel, table=True):
    __tablename__ = "agent_nodes"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    parent_id: Optional[str] = Field(default=None, index=True)
    name: str
    agent_type: str = Field(default="specialist", index=True)
    department: str = Field(default="Director", index=True)
    role: str = Field(default="agent")
    model: str = Field(default="heuristic")
    capability_tags: List[str] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    status: str = Field(default="queued", index=True)
    queue_name: str = Field(default="control")
    current_task: str = Field(default="")
    depth: int = Field(default=0, index=True)
    spawned_children: int = Field(default=0)
    lease_expires_at: Optional[str] = Field(default=None, index=True)
    last_heartbeat_at: Optional[str] = Field(default=None, index=True)
    attributes: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class AgentHeartbeat(SQLModel, table=True):
    __tablename__ = "agent_heartbeats"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    agent_id: str = Field(index=True)
    status: str = Field(default="running", index=True)
    queue_name: str = Field(default="control")
    current_task: str = Field(default="")
    detail: str = Field(default="")
    created_at: Optional[str] = Field(default=None, index=True)


class WorkItem(SQLModel, table=True):
    __tablename__ = "work_items"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    assigned_agent_id: Optional[str] = Field(default=None, index=True)
    department: str = Field(default="Director", index=True)
    title: str
    description: str = Field(default="")
    work_type: str = Field(default="analysis", index=True)
    status: str = Field(default="queued", index=True)
    priority: str = Field(default="normal", index=True)
    queue_name: str = Field(default="control")
    execution_mode: str = Field(default="rules", index=True)
    confidence: float = Field(default=0.0)
    expected_booking_lift: float = Field(default=0.0)
    approval_required: bool = Field(default=False, index=True)
    depends_on_ids: List[str] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    artifact_refs: List[str] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    payload: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    capability_requirement: str = Field(default="cheap_small_text", index=True)
    escalation_level: int = Field(default=0, index=True)
    retry_count: int = Field(default=0, index=True)
    input_context_summary: str = Field(default="")
    output_summary: str = Field(default="")
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None
    completed_at: Optional[str] = Field(default=None, index=True)


class DebateSession(SQLModel, table=True):
    __tablename__ = "debate_sessions"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    title: str
    topic: str = Field(default="")
    status: str = Field(default="queued", index=True)
    consensus_summary: str = Field(default="")
    dissent_summary: str = Field(default="")
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None
    completed_at: Optional[str] = Field(default=None, index=True)


class DebateTurn(SQLModel, table=True):
    __tablename__ = "debate_turns"

    id: str = Field(primary_key=True)
    debate_id: str = Field(index=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    agent_id: Optional[str] = Field(default=None, index=True)
    department: str = Field(default="Director", index=True)
    role: str = Field(default="agent")
    stance: str = Field(default="proposal")
    claim_type: str = Field(default="proposal", index=True)
    content: str = Field(default="")
    evidence_refs: List[str] = Field(default_factory=list, sa_column=SAColumn(SAJSON))
    turn_index: int = Field(default=0, index=True)
    created_at: Optional[str] = Field(default=None, index=True)


class ReviewGate(SQLModel, table=True):
    __tablename__ = "review_gates"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    work_item_id: Optional[str] = Field(default=None, index=True)
    gate_type: str = Field(default="execution_review", index=True)
    title: str
    status: str = Field(default="pending", index=True)
    requested_by: str = Field(default="system")
    approved_by: Optional[str] = Field(default=None)
    rejected_by: Optional[str] = Field(default=None)
    rationale: str = Field(default="")
    payload: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None
    approved_at: Optional[str] = Field(default=None, index=True)
    rejected_at: Optional[str] = Field(default=None, index=True)


class PolicyVersion(SQLModel, table=True):
    __tablename__ = "policy_versions"

    id: str = Field(primary_key=True)
    org_run_id: Optional[str] = Field(default=None, index=True)
    version_no: int = Field(default=1, index=True)
    title: str
    status: str = Field(default="proposed", index=True)
    summary: str = Field(default="")
    active: bool = Field(default=False, index=True)
    change_set: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    approved_at: Optional[str] = Field(default=None, index=True)


class RunArtifact(SQLModel, table=True):
    __tablename__ = "run_artifacts"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    artifact_type: str = Field(default="note", index=True)
    title: str
    status: str = Field(default="ready", index=True)
    content: str = Field(default="")
    attributes: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class ExecutionAttempt(SQLModel, table=True):
    __tablename__ = "execution_attempts"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    work_item_id: Optional[str] = Field(default=None, index=True)
    agent_id: Optional[str] = Field(default=None, index=True)
    expert_key: str = Field(default="", index=True)
    execution_role: str = Field(default="", index=True)
    provider: str = Field(default="", index=True)
    model_alias: str = Field(default="", index=True)
    model_name: str = Field(default="")
    status: str = Field(default="queued", index=True)
    prompt_hash: str = Field(default="", index=True)
    output_artifact_id: Optional[str] = Field(default=None, index=True)
    retry_count: int = Field(default=0)
    input_tokens: int = Field(default=0)
    output_tokens: int = Field(default=0)
    cost_band: str = Field(default="medium", index=True)
    execution_metadata: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn("metadata", SAJSON))
    started_at: Optional[str] = Field(default=None, index=True)
    completed_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class ControlTrigger(SQLModel, table=True):
    __tablename__ = "control_triggers"

    id: str = Field(primary_key=True)
    trigger_type: str = Field(default="operator_command", index=True)
    trigger_source: str = Field(default="operator", index=True)
    entity_type: str = Field(default="portfolio", index=True)
    entity_id: str = Field(default="", index=True)
    status: str = Field(default="queued", index=True)
    priority: str = Field(default="normal", index=True)
    dedupe_key: str = Field(default="", index=True)
    reason: str = Field(default="")
    payload: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    fact_pack_hash: str = Field(default="", index=True)
    mission_id: Optional[str] = Field(default=None, index=True)
    cooldown_until: Optional[str] = Field(default=None, index=True)
    created_at: Optional[str] = Field(default=None, index=True)
    processed_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class FactPack(SQLModel, table=True):
    __tablename__ = "fact_packs"

    id: str = Field(primary_key=True)
    entity_type: str = Field(default="portfolio", index=True)
    entity_id: str = Field(default="", index=True)
    scope: str = Field(default="control", index=True)
    fact_pack_hash: str = Field(default="", index=True)
    payload: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    source_updated_at: Optional[str] = Field(default=None, index=True)
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class ImprovementCandidate(SQLModel, table=True):
    __tablename__ = "improvement_candidates"

    id: str = Field(primary_key=True)
    org_run_id: str = Field(index=True)
    mission_id: str = Field(index=True)
    team: str = Field(default="Growth Team", index=True)
    title: str
    status: str = Field(default="proposed", index=True)
    priority: str = Field(default="normal", index=True)
    summary: str = Field(default="")
    expected_booking_lift: float = Field(default=0.0)
    confidence: float = Field(default=0.0)
    guardrail_risk: str = Field(default="low", index=True)
    payload: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None
    approved_at: Optional[str] = Field(default=None, index=True)


class LearningEvaluation(SQLModel, table=True):
    __tablename__ = "learning_evaluations"

    id: str = Field(primary_key=True)
    evaluation_type: str = Field(default="bookings_first", index=True)
    window_start: str = Field(index=True)
    window_end: str = Field(index=True)
    scorecard: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    deltas: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    summary: str = Field(default="")
    created_at: Optional[str] = Field(default=None, index=True)
