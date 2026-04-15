"""
Orchestration models — jobs, tasks, agent runs, provider events.

Uses the same SQLModel/SQLAlchemy stack as the rest of the app.
Works with both SQLite (dev) and Postgres (prod).
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import Column as SAColumn, Text
from sqlalchemy.dialects.postgresql import JSONB

# Reuse JSON field pattern from sql_models.py
from sqlalchemy import JSON
JSON_FIELD_TYPE = JSON().with_variant(JSONB(), "postgresql")


# ─── Enums (stored as plain strings) ─────────────────────────────────────────

class JobStatus:
    QUEUED     = "queued"
    RUNNING    = "running"
    PAUSED     = "paused"
    WAITING    = "waiting_approval"
    DONE       = "done"
    FAILED     = "failed"
    CANCELLED  = "cancelled"

class TaskStatus:
    PENDING    = "pending"
    ASSIGNED   = "assigned"
    RUNNING    = "running"
    DONE       = "done"
    FAILED     = "failed"
    SKIPPED    = "skipped"
    ESCALATED  = "escalated"

class AgentStatus:
    IDLE       = "idle"
    THINKING   = "thinking"
    EXECUTING  = "executing"
    BLOCKED    = "blocked"
    FAILED     = "failed"
    DONE       = "done"

class ProviderStatus:
    HEALTHY    = "healthy"
    DEGRADED   = "degraded"
    RATE_LIMITED = "rate_limited"
    UNAVAILABLE = "unavailable"


# ─── SQL Models ───────────────────────────────────────────────────────────────

class OrchJob(SQLModel, table=True):
    """A top-level job submitted to the orchestration system."""
    __tablename__ = "orch_jobs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    title: str
    work_type: str = Field(index=True)  # e.g. "implementation", "debugging", "review"
    description: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    status: str = Field(default=JobStatus.QUEUED, index=True)
    priority: int = Field(default=5)  # 1=highest, 10=lowest
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    budget_tokens: Optional[int] = None  # hard cap
    tokens_used: int = Field(default=0)
    cost_usd: float = Field(default=0.0)
    retries: int = Field(default=0)
    max_retries: int = Field(default=3)
    failure_reason: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    # JSON fields
    context: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))
    tags: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE))
    stop_conditions: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))
    result_summary: Optional[str] = Field(default=None, sa_column=SAColumn(Text))


class OrchTask(SQLModel, table=True):
    """A unit of work within a job, assigned to a specific agent."""
    __tablename__ = "orch_tasks"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    job_id: str = Field(index=True, foreign_key="orch_jobs.id")
    parent_task_id: Optional[str] = Field(default=None, index=True)
    title: str
    work_type: str  # determines agent + provider selection
    status: str = Field(default=TaskStatus.PENDING, index=True)
    assigned_agent: Optional[str] = None  # agent role name
    provider: Optional[str] = None        # e.g. "nim", "gemini", "claude"
    model: Optional[str] = None
    prompt_template: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    latency_ms: Optional[int] = None
    input_tokens: int = Field(default=0)
    output_tokens: int = Field(default=0)
    cost_usd: float = Field(default=0.0)
    retries: int = Field(default=0)
    failure_reason: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    # JSON
    input_data: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))
    output_data: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))
    file_changes: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE))
    verification_result: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))


class OrchAgent(SQLModel, table=True):
    """Persistent state record for an agent role instance."""
    __tablename__ = "orch_agents"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    role: str = Field(index=True)        # e.g. "builder", "planner", "debugger"
    display_name: str
    status: str = Field(default=AgentStatus.IDLE, index=True)
    current_task_id: Optional[str] = None
    current_job_id: Optional[str] = None
    preferred_provider: Optional[str] = None
    last_active: datetime = Field(default_factory=datetime.utcnow)
    tasks_completed: int = Field(default=0)
    tasks_failed: int = Field(default=0)
    total_tokens: int = Field(default=0)
    total_cost_usd: float = Field(default=0.0)
    # JSON
    capabilities: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE))
    allowed_tools: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE))
    current_reasoning: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    memory_compact: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))


class OrchProviderState(SQLModel, table=True):
    """Live health + usage record for each provider."""
    __tablename__ = "orch_provider_states"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    provider_key: str = Field(unique=True, index=True)  # "nim", "gemini", "claude", "ollama"
    display_name: str
    status: str = Field(default=ProviderStatus.HEALTHY)
    last_checked: datetime = Field(default_factory=datetime.utcnow)
    requests_today: int = Field(default=0)
    requests_this_minute: int = Field(default=0)
    rpm_cap: int = Field(default=60)
    failures_recent: int = Field(default=0)
    circuit_open: bool = Field(default=False)
    circuit_open_until: Optional[datetime] = None
    total_tokens_today: int = Field(default=0)
    total_cost_today_usd: float = Field(default=0.0)
    # JSON
    models: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE))
    capabilities: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE))
    config: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))


class OrchEvent(SQLModel, table=True):
    """Append-only event log for full observability."""
    __tablename__ = "orch_events"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    job_id: Optional[str] = Field(default=None, index=True)
    task_id: Optional[str] = Field(default=None, index=True)
    agent_role: Optional[str] = None
    provider: Optional[str] = None
    event_type: str = Field(index=True)
    # e.g.: job_created, task_started, task_done, task_failed,
    #       provider_fallback, provider_rate_limited, circuit_opened,
    #       escalation, approval_requested, loop_iteration,
    #       agent_handoff, reasoning_step, file_changed, verification_passed
    level: str = Field(default="info")   # info, warn, error
    message: str = Field(sa_column=SAColumn(Text))
    ts: datetime = Field(default_factory=datetime.utcnow, index=True)
    data: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE))


class OrchMemory(SQLModel, table=True):
    """Bounded key/value memory store for agents."""
    __tablename__ = "orch_memory"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    scope: str = Field(index=True)  # "global", job_id, task_id, agent role
    key: str = Field(index=True)
    value: str = Field(sa_column=SAColumn(Text))
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    ttl_seconds: Optional[int] = None  # None = permanent
