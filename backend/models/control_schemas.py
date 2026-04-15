from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ControlMissionCommandRequest(BaseModel):
    command: str
    title: Optional[str] = None
    objective: Optional[str] = None
    target_type: str = "portfolio"
    target_id: Optional[str] = None
    requested_by: str = "operator"
    priority: str = "high"
    autonomy_mode: str = "research_only"
    preview_hash: Optional[str] = None


class ControlMissionApproveRequest(BaseModel):
    approved_by: str = "operator"


class ControlMissionRestartRequest(BaseModel):
    restarted_by: str = "operator"


class ControlDowngradeApproveRequest(BaseModel):
    approved_by: str = "operator"
    selected_model_alias: Optional[str] = None


class ControlWorkItemDecisionRequest(BaseModel):
    approved_by: str = "operator"
    rationale: Optional[str] = None


class ControlArtifactApplyRequest(BaseModel):
    applied_by: str = "operator"


class ControlRecommendedStep(BaseModel):
    id: str
    title: str
    owner: str
    department: str
    reason: str
    priority: str = "normal"
    channel: Optional[str] = None
    lead_id: Optional[str] = None
    approval_required: bool = True


class ControlDepartmentStatus(BaseModel):
    department: str
    head: str
    specialists: List[str] = Field(default_factory=list)
    status: str = "completed"
    summary: str = ""
    findings: List[str] = Field(default_factory=list)
    recommended_steps: List[ControlRecommendedStep] = Field(default_factory=list)


class ControlExpertAssignmentPayload(BaseModel):
    expert_key: str
    name: str
    role: str
    purpose: str = ""
    provider: Optional[str] = None
    provider_alias: str
    model_alias: str
    planned_model: str = ""
    fallback_chain: List[str] = Field(default_factory=list)
    cost_band: str = "medium"
    available: bool = True
    availability_reason: Optional[str] = None


class ControlMissionPreview(BaseModel):
    command: str
    objective: str
    target_type: str
    target_id: Optional[str] = None
    target_label: str = ""
    autonomy_mode: str = "research_only"
    complexity: str = "medium"
    cost_band: str = "medium"
    preview_hash: str
    experts: List[ControlExpertAssignmentPayload] = Field(default_factory=list)
    model_plan: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    downgrade_required: bool = False
    context_summary: str = ""


class ControlMissionSummary(BaseModel):
    id: str
    title: str
    command_text: str
    objective: str = ""
    target_type: str
    target_id: Optional[str] = None
    target_label: str = ""
    requested_by: str = "operator"
    trigger_source: str = "operator_command"
    trigger_reason: str = ""
    status: str
    priority: str = "normal"
    latest_run_id: Optional[str] = None
    fact_pack_hash: str = ""
    budget_class: str = "heuristic_first"
    cache_hit: bool = False
    llm_call_count: int = 0
    preview_hash: str = ""
    complexity: str = "medium"
    model_plan: Dict[str, Any] = Field(default_factory=dict)
    downgrade_required: bool = False
    director_summary: Optional[str] = None
    consensus_plan: Optional[str] = None
    recommended_steps: List[ControlRecommendedStep] = Field(default_factory=list)
    department_statuses: List[ControlDepartmentStatus] = Field(default_factory=list)
    context_snapshot: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    approved_at: Optional[str] = None


class ControlMissionRunPayload(BaseModel):
    id: str
    mission_id: str
    run_number: int = 1
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    objective_snapshot: str = ""
    director_summary: Optional[str] = None
    consensus_plan: Optional[str] = None
    recommended_steps: List[ControlRecommendedStep] = Field(default_factory=list)
    context_snapshot: Dict[str, Any] = Field(default_factory=dict)
    updated_at: Optional[str] = None


class ControlMissionEventPayload(BaseModel):
    id: str
    mission_id: str
    run_id: str
    sequence_no: int = 0
    department: str
    role: str
    event_type: str
    status: str
    title: str
    summary: str = ""
    detail: str = ""
    evidence_refs: List[str] = Field(default_factory=list)
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None


class ControlOrgRunPayload(BaseModel):
    id: str
    mission_id: str
    run_id: str
    status: str
    current_phase: str
    autonomy_mode: str
    root_agent_id: Optional[str] = None
    summary: str = ""
    metrics: Dict[str, Any] = Field(default_factory=dict)
    queued_at: Optional[str] = None
    started_at: Optional[str] = None
    heartbeat_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: Optional[str] = None


class AgentNodePayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    parent_id: Optional[str] = None
    name: str
    agent_type: str
    department: str
    role: str
    model: str
    expert_key: Optional[str] = None
    execution_role: Optional[str] = None
    provider_alias: Optional[str] = None
    model_alias: Optional[str] = None
    fallback_chain: List[str] = Field(default_factory=list)
    cost_band: Optional[str] = None
    capability_tags: List[str] = Field(default_factory=list)
    status: str
    queue_name: str
    current_task: str = ""
    depth: int = 0
    spawned_children: int = 0
    lease_expires_at: Optional[str] = None
    last_heartbeat_at: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class AgentHeartbeatPayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    agent_id: str
    status: str
    queue_name: str
    current_task: str = ""
    detail: str = ""
    created_at: Optional[str] = None


class WorkItemPayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    assigned_agent_id: Optional[str] = None
    department: str
    title: str
    description: str = ""
    work_type: str
    status: str
    priority: str
    queue_name: str
    execution_mode: str
    confidence: float = 0.0
    expected_booking_lift: float = 0.0
    expert_key: Optional[str] = None
    provider_alias: Optional[str] = None
    model_alias: Optional[str] = None
    fallback_chain: List[str] = Field(default_factory=list)
    verification_state: Optional[str] = None
    approval_required: bool = False
    depends_on_ids: List[str] = Field(default_factory=list)
    artifact_refs: List[str] = Field(default_factory=list)
    payload: Dict[str, Any] = Field(default_factory=dict)
    capability_requirement: str = "cheap_small_text"
    escalation_level: int = 0
    retry_count: int = 0
    input_context_summary: str = ""
    output_summary: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None


class DebateSessionPayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    title: str
    topic: str
    status: str
    consensus_summary: str = ""
    dissent_summary: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None


class DebateTurnPayload(BaseModel):
    id: str
    debate_id: str
    org_run_id: str
    mission_id: str
    agent_id: Optional[str] = None
    department: str
    role: str
    stance: str
    claim_type: str
    content: str
    evidence_refs: List[str] = Field(default_factory=list)
    turn_index: int = 0
    created_at: Optional[str] = None


class ReviewGatePayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    work_item_id: Optional[str] = None
    gate_type: str
    title: str
    status: str
    requested_by: str
    approved_by: Optional[str] = None
    rejected_by: Optional[str] = None
    rationale: str = ""
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    approved_at: Optional[str] = None
    rejected_at: Optional[str] = None


class PolicyVersionPayload(BaseModel):
    id: str
    org_run_id: Optional[str] = None
    version_no: int
    title: str
    status: str
    summary: str = ""
    active: bool
    change_set: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    approved_at: Optional[str] = None


class RunArtifactPayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    artifact_type: str
    title: str
    status: str
    expert_key: Optional[str] = None
    provider_alias: Optional[str] = None
    model_alias: Optional[str] = None
    verification_state: Optional[str] = None
    content: str = ""
    attributes: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ExecutionAttemptPayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    work_item_id: Optional[str] = None
    agent_id: Optional[str] = None
    expert_key: str = ""
    execution_role: str = ""
    provider: str = ""
    model_alias: str = ""
    model_name: str = ""
    status: str
    prompt_hash: str = ""
    output_artifact_id: Optional[str] = None
    retry_count: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_band: str = "medium"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: Optional[str] = None


class ControlTriggerPayload(BaseModel):
    id: str
    trigger_type: str
    trigger_source: str
    entity_type: str
    entity_id: str
    status: str
    priority: str
    dedupe_key: str
    reason: str = ""
    payload: Dict[str, Any] = Field(default_factory=dict)
    fact_pack_hash: str = ""
    mission_id: Optional[str] = None
    cooldown_until: Optional[str] = None
    created_at: Optional[str] = None
    processed_at: Optional[str] = None
    updated_at: Optional[str] = None


class FactPackPayload(BaseModel):
    id: str
    entity_type: str
    entity_id: str
    scope: str
    fact_pack_hash: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    source_updated_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ImprovementCandidatePayload(BaseModel):
    id: str
    org_run_id: str
    mission_id: str
    team: str
    title: str
    status: str
    priority: str
    summary: str = ""
    expected_booking_lift: float = 0.0
    confidence: float = 0.0
    guardrail_risk: str = "low"
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    approved_at: Optional[str] = None


class LearningEvaluationPayload(BaseModel):
    id: str
    evaluation_type: str
    window_start: str
    window_end: str
    scorecard: Dict[str, Any] = Field(default_factory=dict)
    deltas: Dict[str, Any] = Field(default_factory=dict)
    summary: str = ""
    created_at: Optional[str] = None


class ControlRuntimeStatusPayload(BaseModel):
    status: str = "idle"
    loop_interval_seconds: int = 2
    tick_count: int = 0
    failure_count: int = 0
    last_tick_at: Optional[str] = None
    last_success_at: Optional[str] = None
    last_error_at: Optional[str] = None
    last_error: str = ""
    active_org_runs: int = 0
    queued_triggers: int = 0
    pending_reviews: int = 0


class ControlLiveAgentPayload(BaseModel):
    agent_id: str
    org_run_id: str
    mission_id: str
    mission_title: str = ""
    mission_status: str = ""
    org_phase: str = ""
    name: str
    agent_type: str
    department: str
    role: str
    status: str
    queue_name: str
    current_task: str = ""
    detail: str = ""
    waiting_for: str = ""
    last_update_at: Optional[str] = None
    depth: int = 0
    visual_state: str = "idle"
    current_zone: str = ""
    target_zone: str = ""
    interaction_partner_id: str = ""


class ControlTimelineEntryPayload(BaseModel):
    id: str
    source_type: str
    mission_id: Optional[str] = None
    org_run_id: Optional[str] = None
    mission_title: str = ""
    status: str = ""
    actor_name: str
    actor_role: str = ""
    department: str = ""
    title: str
    detail: str = ""
    created_at: Optional[str] = None


class ControlOrgRunDetail(BaseModel):
    org_run: Optional[ControlOrgRunPayload] = None
    agent_nodes: List[AgentNodePayload] = Field(default_factory=list)
    heartbeats: List[AgentHeartbeatPayload] = Field(default_factory=list)
    work_items: List[WorkItemPayload] = Field(default_factory=list)
    debate_sessions: List[DebateSessionPayload] = Field(default_factory=list)
    debate_turns: List[DebateTurnPayload] = Field(default_factory=list)
    review_gates: List[ReviewGatePayload] = Field(default_factory=list)
    policy_versions: List[PolicyVersionPayload] = Field(default_factory=list)
    artifacts: List[RunArtifactPayload] = Field(default_factory=list)
    execution_attempts: List[ExecutionAttemptPayload] = Field(default_factory=list)
    triggers: List[ControlTriggerPayload] = Field(default_factory=list)
    fact_packs: List[FactPackPayload] = Field(default_factory=list)
    improvement_candidates: List[ImprovementCandidatePayload] = Field(default_factory=list)
    learning_evaluations: List[LearningEvaluationPayload] = Field(default_factory=list)


class ControlMissionDetail(BaseModel):
    mission: ControlMissionSummary
    latest_run: Optional[ControlMissionRunPayload] = None
    org_run: Optional[ControlOrgRunPayload] = None
    events: List[ControlMissionEventPayload] = Field(default_factory=list)
    agent_nodes: List[AgentNodePayload] = Field(default_factory=list)
    heartbeats: List[AgentHeartbeatPayload] = Field(default_factory=list)
    work_items: List[WorkItemPayload] = Field(default_factory=list)
    debate_sessions: List[DebateSessionPayload] = Field(default_factory=list)
    debate_turns: List[DebateTurnPayload] = Field(default_factory=list)
    review_gates: List[ReviewGatePayload] = Field(default_factory=list)
    policy_versions: List[PolicyVersionPayload] = Field(default_factory=list)
    artifacts: List[RunArtifactPayload] = Field(default_factory=list)
    execution_attempts: List[ExecutionAttemptPayload] = Field(default_factory=list)
    triggers: List[ControlTriggerPayload] = Field(default_factory=list)
    fact_packs: List[FactPackPayload] = Field(default_factory=list)
    improvement_candidates: List[ImprovementCandidatePayload] = Field(default_factory=list)
    learning_evaluations: List[LearningEvaluationPayload] = Field(default_factory=list)


class ControlMissionListResponse(BaseModel):
    missions: List[ControlMissionSummary] = Field(default_factory=list)


class ControlWorkItemListResponse(BaseModel):
    work_items: List[WorkItemPayload] = Field(default_factory=list)


class ControlLiveSnapshot(BaseModel):
    runtime: ControlRuntimeStatusPayload = Field(default_factory=ControlRuntimeStatusPayload)
    active_missions: List[ControlMissionSummary] = Field(default_factory=list)
    recent_missions: List[ControlMissionSummary] = Field(default_factory=list)
    pending_work_items: List[WorkItemPayload] = Field(default_factory=list)
    agents: List[ControlLiveAgentPayload] = Field(default_factory=list)
    timeline: List[ControlTimelineEntryPayload] = Field(default_factory=list)
    triggers: List[ControlTriggerPayload] = Field(default_factory=list)
