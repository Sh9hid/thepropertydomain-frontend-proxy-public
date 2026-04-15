from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import Column as SAColumn, JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel

from core.utils import now_iso


JSON_FIELD = JSON().with_variant(JSONB(), "postgresql")


class HermesSource(SQLModel, table=True):
    __tablename__ = "hermes_sources"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    name: str
    source_type: str = Field(index=True)
    base_url: str
    rss_url: Optional[str] = None
    enabled: bool = Field(default=True, index=True)
    fetch_frequency_minutes: int = Field(default=180)
    tags_json: List[str] = Field(default_factory=list, sa_column=SAColumn(JSON_FIELD))
    company_scope: str = Field(default="shared", index=True)
    credibility_score: float = Field(default=0.7)
    last_fetched_at: Optional[str] = Field(default=None, index=True)
    created_at: str = Field(default_factory=now_iso, index=True)
    updated_at: str = Field(default_factory=now_iso)


class HermesRun(SQLModel, table=True):
    __tablename__ = "hermes_runs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    job_type: str = Field(index=True)
    status: str = Field(default="running", index=True)
    started_at: str = Field(default_factory=now_iso, index=True)
    completed_at: Optional[str] = None
    input_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD))
    output_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD))
    error_text: Optional[str] = None


class HermesFinding(SQLModel, table=True):
    __tablename__ = "hermes_findings"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    source_id: str = Field(index=True)
    source_type: str = Field(index=True)
    source_name: str
    source_url: str
    dedupe_key: str = Field(index=True, unique=True)
    company_scope: str = Field(index=True)
    topic: str = Field(index=True)
    signal_type: str = Field(index=True)
    summary: str
    why_it_matters: str
    novelty_score: float = Field(default=0.5)
    confidence_score: float = Field(default=0.5)
    actionability_score: float = Field(default=0.5)
    proposed_actions_json: List[str] = Field(default_factory=list, sa_column=SAColumn(JSON_FIELD))
    published_at: Optional[str] = Field(default=None, index=True)
    created_at: str = Field(default_factory=now_iso, index=True)


class HermesMemoryEntry(SQLModel, table=True):
    __tablename__ = "hermes_memory"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    memory_type: str = Field(index=True)
    title: str = Field(index=True)
    body: str
    tags_json: List[str] = Field(default_factory=list, sa_column=SAColumn(JSON_FIELD))
    source_refs_json: List[str] = Field(default_factory=list, sa_column=SAColumn(JSON_FIELD))
    confidence_score: float = Field(default=0.5)
    created_at: str = Field(default_factory=now_iso, index=True)
    expires_at: Optional[str] = None


class HermesContent(SQLModel, table=True):
    __tablename__ = "hermes_content"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    content_type: str = Field(index=True)
    audience: str = Field(index=True)
    hook: str
    body: str
    cta: str
    status: str = Field(default="pending_approval", index=True)
    source_refs_json: List[str] = Field(default_factory=list, sa_column=SAColumn(JSON_FIELD))
    repurposable: bool = Field(default=True)
    scheduled_for: Optional[str] = None
    published_at: Optional[str] = None
    created_at: str = Field(default_factory=now_iso, index=True)
    updated_at: str = Field(default_factory=now_iso)


class HermesCampaign(SQLModel, table=True):
    __tablename__ = "hermes_campaigns"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    campaign_type: str = Field(index=True)
    audience: str = Field(index=True)
    channel: str = Field(index=True)
    stage: str = Field(index=True)
    subject: str
    message: str
    goal: str = ""
    status: str = Field(default="pending_approval", index=True)
    related_lead_id: Optional[str] = Field(default=None, index=True)
    created_at: str = Field(default_factory=now_iso, index=True)
    sent_at: Optional[str] = None


class HermesChatMessage(SQLModel, table=True):
    __tablename__ = "hermes_chat"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    session_id: str = Field(index=True)
    role: str = Field(index=True)          # "user" | "agent"
    agent_id: Optional[str] = Field(default=None, index=True)
    agent_name: Optional[str] = None
    message: str
    metadata_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD))
    created_at: str = Field(default_factory=now_iso, index=True)


class HermesCaseMemory(SQLModel, table=True):
    __tablename__ = "hermes_case_memory"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: str = Field(index=True)
    memory_type: str = Field(index=True)
    content: str
    source: str = ""
    importance: float = Field(default=0.5)
    created_at: str = Field(default_factory=now_iso, index=True)
    expires_at: Optional[str] = None


class HermesContactCluster(SQLModel, table=True):
    __tablename__ = "hermes_contact_clusters"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: str = Field(index=True)
    cluster_type: str = Field(index=True)
    contacts_json: List[str] = Field(default_factory=list, sa_column=SAColumn(JSON_FIELD))
    primary_lead_contact_id: Optional[str] = None
    score: float = Field(default=0.0)
    created_at: str = Field(default_factory=now_iso, index=True)


class HermesContactPlan(SQLModel, table=True):
    __tablename__ = "hermes_contact_plans"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: str = Field(index=True)
    channel: str = Field(index=True)
    planned_at: Optional[str] = None
    message_template: str = ""
    status: str = Field(default="pending", index=True)
    created_at: str = Field(default_factory=now_iso, index=True)


class HermesDecisionLog(SQLModel, table=True):
    __tablename__ = "hermes_decision_logs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: Optional[str] = Field(default=None, index=True)
    decision_type: str = Field(index=True)
    input_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD))
    output_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD))
    reasoning: str = ""
    created_at: str = Field(default_factory=now_iso, index=True)


_SCHEMA_READY = False
_SCHEMA_LOCK = asyncio.Lock()


async def ensure_hermes_schema(force: bool = False) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return

    async with _SCHEMA_LOCK:
        if _SCHEMA_READY and not force:
            return
        import core.database as db_module

        tables = [
            HermesSource.__table__,
            HermesRun.__table__,
            HermesFinding.__table__,
            HermesMemoryEntry.__table__,
            HermesContent.__table__,
            HermesCampaign.__table__,
            HermesChatMessage.__table__,
        ]
        async with db_module.async_engine.begin() as conn:
            await conn.run_sync(lambda sync_conn: SQLModel.metadata.create_all(sync_conn, tables=tables))
        _SCHEMA_READY = True


def reset_hermes_schema_state() -> None:
    global _SCHEMA_READY
    _SCHEMA_READY = False
