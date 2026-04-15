"""
Org models — tickets, conversations, memory, research notes, voice training plans.

Designed to layer on top of the existing orchestration runtime.
All models use the same SQLModel/SQLAlchemy stack as the rest of the app.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import Column as SAColumn, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import JSON

JSON_FIELD = JSON().with_variant(JSONB(), "postgresql")


# ─── Ticket ───────────────────────────────────────────────────────────────────

class TicketDept:
    RESEARCH    = "research"
    REVENUE     = "revenue"
    ENGINEERING = "engineering"
    QA          = "qa"
    VOICE       = "voice"

class TicketKind:
    BUG        = "bug"
    FEATURE    = "feature"
    RESEARCH   = "research"
    FOLLOWUP   = "followup"
    ANOMALY    = "anomaly"
    TRAINING       = "training"
    INSIGHT        = "insight"
    OUTREACH_EMAIL = "outreach_email"
    OUTREACH_SMS   = "outreach_sms"

class TicketStatus:
    OPEN        = "open"
    ACCEPTED    = "accepted"
    REJECTED    = "rejected"
    IN_PROGRESS = "in_progress"
    BLOCKED     = "blocked"
    DONE        = "done"
    CANCELLED   = "cancelled"

class TicketSeverity:
    LOW     = "low"
    MEDIUM  = "medium"
    HIGH    = "high"
    CRITICAL = "critical"


class Ticket(SQLModel, table=True):
    __tablename__ = "org_tickets"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    title: str
    description: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    workspace_key: str = Field(default="real_estate", index=True)  # thepropertydomain, real_estate, mortgage
    department: str = Field(index=True)          # TicketDept
    kind: str = Field(index=True)                # TicketKind
    priority: int = Field(default=5)             # 1=highest
    severity: str = Field(default=TicketSeverity.MEDIUM)
    status: str = Field(default=TicketStatus.OPEN, index=True)

    created_by_type: str = Field(default="system")   # user, system, agent
    created_by_id: Optional[str] = None
    assigned_agent_id: Optional[str] = Field(default=None, index=True)
    parent_ticket_id: Optional[str] = Field(default=None, index=True)

    related_lead_id: Optional[str] = Field(default=None, index=True)
    related_job_id: Optional[str] = Field(default=None, index=True)

    acceptance_reason: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    rejection_reason: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    resolution_notes: Optional[str] = Field(default=None, sa_column=SAColumn(Text))

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None

    evidence_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD))
    metadata_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD))
    tags: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD))


# ─── Conversation ─────────────────────────────────────────────────────────────

class ConvStatus:
    OPEN       = "open"
    RESOLVED   = "resolved"
    ABANDONED  = "abandoned"


class Conversation(SQLModel, table=True):
    __tablename__ = "org_conversations"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    ticket_id: str = Field(index=True)
    topic: str
    status: str = Field(default=ConvStatus.OPEN, index=True)
    started_by_agent_id: Optional[str] = None
    max_rounds: int = Field(default=6)         # hard cap on back-and-forth
    rounds_completed: int = Field(default=0)
    final_decision: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    action_plan: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    summary: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    participants: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD))  # agent roles


class ConversationMessage(SQLModel, table=True):
    __tablename__ = "org_conv_messages"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    conversation_id: str = Field(index=True)
    agent_id: str        # agent role
    message_type: str    # proposal, critique, evidence, summary, decision
    content: str = Field(sa_column=SAColumn(Text))
    evidence_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD))
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ─── Memory ───────────────────────────────────────────────────────────────────

class MemType:
    FACT      = "fact"
    PREFERENCE = "preference"
    LESSON    = "lesson"
    PATTERN   = "pattern"
    SUMMARY   = "summary"


class AgentMemory(SQLModel, table=True):
    __tablename__ = "org_agent_memory"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    workspace_key: str = Field(default="real_estate", index=True)  # workspace isolation
    agent_id: str = Field(index=True)          # agent role
    memory_type: str = Field(index=True)       # MemType
    content: str = Field(sa_column=SAColumn(Text))
    source_type: str = Field(default="system") # ticket, conversation, transcript, metric, lead
    source_id: Optional[str] = None
    importance: int = Field(default=5)         # 1-10, used for retrieval ranking
    created_at: datetime = Field(default_factory=datetime.utcnow)


class OrgMemory(SQLModel, table=True):
    __tablename__ = "org_memory"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    workspace_key: str = Field(default="real_estate", index=True)  # workspace isolation
    topic: str = Field(index=True)
    department: Optional[str] = Field(default=None, index=True)
    business_context_key: Optional[str] = Field(default=None, index=True)
    memory_type: str                           # MemType
    content: str = Field(sa_column=SAColumn(Text))
    source_type: str = Field(default="system")
    source_id: Optional[str] = None
    importance: int = Field(default=5)
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ─── Research Note ────────────────────────────────────────────────────────────

class ResearchArea:
    SALES      = "sales"
    REAL_ESTATE = "real_estate"
    APP_TECH   = "app_tech"


class ResearchNote(SQLModel, table=True):
    __tablename__ = "org_research_notes"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    title: str
    area: str = Field(index=True)              # ResearchArea
    department: str = Field(default=TicketDept.RESEARCH)
    thesis: str = Field(sa_column=SAColumn(Text))
    evidence: str = Field(sa_column=SAColumn(Text))    # structured evidence summary
    recommendation: str = Field(sa_column=SAColumn(Text))
    confidence: str = Field(default="medium")  # low / medium / high
    produced_by_agent: str = Field(default="research_agent")
    ticket_raised_id: Optional[str] = None    # if a ticket was auto-raised from this
    run_id: Optional[str] = None              # links to ResearchRun
    created_at: datetime = Field(default_factory=datetime.utcnow)
    evidence_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD))


class ResearchRun(SQLModel, table=True):
    """Tracks each scheduled/on-demand research execution."""
    __tablename__ = "org_research_runs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    area: str = Field(index=True)
    status: str = Field(default="running")  # running, done, failed
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    notes_created: int = Field(default=0)
    tickets_raised: int = Field(default=0)
    provider_used: Optional[str] = None
    tokens_used: int = Field(default=0)
    cost_usd: float = Field(default=0.0)
    error: Optional[str] = None


# ─── Voice Training Plan ──────────────────────────────────────────────────────

class VoiceTrainingPlan(SQLModel, table=True):
    __tablename__ = "org_voice_plans"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    plan_date: str = Field(index=True)     # YYYY-MM-DD
    rep_id: str = Field(index=True, default="Shahid")
    calls_analysed: int = Field(default=0)
    source_call_ids: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD))
    status: str = Field(default="generated")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    # Plan content (structured JSON for UI rendering)
    mistakes: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD))
    # [{pattern, count, example_quote, recommendation}]
    drills: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD))
    # [{drill_name, instruction, example_before, example_after, duration_minutes}]
    improved_phrases: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD))
    # [{original, improved, context}]
    session_structure: str = Field(default="", sa_column=SAColumn(Text))  # 10-min plan as text
    overall_score: Optional[float] = None    # average from input reports
    key_focus: Optional[str] = None          # top priority for this session
    provider_used: Optional[str] = None
    tokens_used: int = Field(default=0)
