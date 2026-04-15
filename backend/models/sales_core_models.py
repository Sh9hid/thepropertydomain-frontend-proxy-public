from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Boolean, Column as SAColumn, DateTime, Float, JSON, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


JSON_FIELD_TYPE = JSON().with_variant(JSONB(), "postgresql")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class BusinessContext(SQLModel, table=True):
    __tablename__ = "business_context"

    key: str = Field(primary_key=True, max_length=64)
    label: str = Field(max_length=128)
    description: Optional[str] = Field(default=None, max_length=255)
    active: bool = Field(default=True, index=True)
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False),
    )


class LeadContact(SQLModel, table=True):
    __tablename__ = "lead_contact"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    business_context_key: str = Field(index=True, foreign_key="business_context.key")
    lead_id: Optional[str] = Field(default=None, index=True, foreign_key="leads.id")
    property_id: Optional[str] = Field(default=None, index=True)
    opportunity_id: Optional[str] = Field(default=None, index=True)
    full_name: str = Field(default="", max_length=255, index=True)
    primary_phone: Optional[str] = Field(default=None, max_length=64, index=True)
    secondary_phone: Optional[str] = Field(default=None, max_length=64)
    primary_email: Optional[str] = Field(default=None, max_length=255, index=True)
    contact_role: Optional[str] = Field(default=None, max_length=64, index=True)
    owner_type: Optional[str] = Field(default=None, max_length=64)
    source: Optional[str] = Field(default=None, max_length=64)
    phone_verification_status: Optional[str] = Field(default=None, max_length=32, index=True)
    email_verification_status: Optional[str] = Field(default=None, max_length=32)
    enrichment_source: Optional[str] = Field(default=None, max_length=64)
    metadata_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )


class ContactAttempt(SQLModel, table=True):
    __tablename__ = "contact_attempt"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    business_context_key: str = Field(index=True, foreign_key="business_context.key")
    lead_contact_id: str = Field(index=True, foreign_key="lead_contact.id")
    attempted_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )
    channel: str = Field(default="call", max_length=32, index=True)
    outcome: str = Field(default="unknown", max_length=64, index=True)
    connected: bool = Field(default=False, index=True)
    duration_seconds: Optional[int] = Field(default=None)
    voicemail_left: bool = Field(default=False)
    note: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    transcript_id: Optional[str] = Field(default=None, max_length=128)
    recording_id: Optional[str] = Field(default=None, max_length=128)
    recipient_email: Optional[str] = Field(default=None, max_length=255, index=True)
    email_subject: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    email_body: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    sequence_key: Optional[str] = Field(default=None, max_length=64, index=True)
    sequence_step: Optional[int] = Field(default=None, index=True)
    variant_key: Optional[str] = Field(default=None, max_length=64, index=True)
    parent_attempt_id: Optional[str] = Field(default=None, max_length=128, index=True)
    external_message_id: Optional[str] = Field(default=None, max_length=255, index=True)
    opened_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    replied_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    performance_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    next_action_due_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    created_by: Optional[str] = Field(default=None, max_length=64)
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )


class LeadState(SQLModel, table=True):
    __tablename__ = "lead_state"

    lead_contact_id: str = Field(primary_key=True, foreign_key="lead_contact.id")
    business_context_key: str = Field(index=True, foreign_key="business_context.key")
    total_attempts: int = Field(default=0)
    attempts_last_7d: int = Field(default=0, index=True)
    last_attempt_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    last_attempt_outcome: Optional[str] = Field(default=None, max_length=64)
    last_contact_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True)))
    last_response_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True)))
    best_contact_window: Optional[str] = Field(default=None, max_length=64)
    fatigue_band: str = Field(default="low", max_length=32, index=True)
    callable_now: bool = Field(default=False, index=True)
    next_action: str = Field(default="review", max_length=64, index=True)
    next_action_due_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    queue_score: float = Field(default=0.0, sa_column=SAColumn(Float, nullable=False, index=True))
    needs_enrichment: bool = Field(default=False, index=True)
    stale_enrichment: bool = Field(default=False, index=True)
    summary_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )


class TaskQueue(SQLModel, table=True):
    __tablename__ = "task_queue"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    business_context_key: str = Field(index=True, foreign_key="business_context.key")
    lead_contact_id: str = Field(index=True, foreign_key="lead_contact.id")
    task_type: str = Field(default="follow_up", max_length=64, index=True)
    due_at: datetime = Field(sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True))
    status: str = Field(default="pending", max_length=32, index=True)
    priority: int = Field(default=50, index=True)
    reason: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    payload_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    created_by: Optional[str] = Field(default=None, max_length=64)
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )


class EnrichmentState(SQLModel, table=True):
    __tablename__ = "enrichment_state"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    business_context_key: str = Field(index=True, foreign_key="business_context.key")
    lead_contact_id: Optional[str] = Field(default=None, index=True, foreign_key="lead_contact.id")
    target_type: str = Field(default="lead_contact", max_length=32, index=True)
    target_id: str = Field(index=True, max_length=128)
    source: str = Field(default="rp_data", max_length=64, index=True)
    status: str = Field(default="queued", max_length=32, index=True)
    attempt_count: int = Field(default=0)
    last_attempt_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    next_retry_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    cooldown_until: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    freshness_expires_at: Optional[datetime] = Field(default=None, sa_column=SAColumn(DateTime(timezone=True), index=True))
    checksum: Optional[str] = Field(default=None, max_length=128)
    version_tag: Optional[str] = Field(default=None, max_length=64)
    priority_score: int = Field(default=0, index=True)
    reason: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    payload_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )


class ProviderUsageLog(SQLModel, table=True):
    __tablename__ = "provider_usage_log"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    provider: str = Field(max_length=64, index=True)
    model: Optional[str] = Field(default=None, max_length=128)
    feature: str = Field(max_length=64, index=True)
    task_class: str = Field(default="cheap", max_length=32, index=True)
    status: str = Field(default="skipped", max_length=32, index=True)
    estimated_cost_usd: float = Field(default=0.0, sa_column=SAColumn(Float, nullable=False))
    usage_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    error_message: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )


class ContentAsset(SQLModel, table=True):
    __tablename__ = "content_asset"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    business_context_key: str = Field(index=True, foreign_key="business_context.key")
    asset_type: str = Field(default="linkedin_post", max_length=64, index=True)
    status: str = Field(default="draft", max_length=32, index=True)
    title: Optional[str] = Field(default=None, max_length=255)
    content_text: str = Field(default="", sa_column=SAColumn(Text))
    variant_key: Optional[str] = Field(default=None, max_length=64, index=True)
    source_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    performance_json: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(JSON_FIELD_TYPE))
    published: bool = Field(default=False, sa_column=SAColumn(Boolean, nullable=False, default=False))
    created_by: Optional[str] = Field(default=None, max_length=64)
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=SAColumn(DateTime(timezone=True), nullable=False, index=True),
    )
