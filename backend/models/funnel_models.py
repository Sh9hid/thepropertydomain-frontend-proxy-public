from typing import Any, Dict, Optional

from sqlalchemy import JSON as SAJSON
from sqlalchemy import Column as SAColumn
from sqlmodel import Field, SQLModel


class LeadChannelConsent(SQLModel, table=True):
    __tablename__ = "lead_channel_consents"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    channel: str = Field(index=True)
    purpose: str = Field(index=True)
    status: str = Field(default="unknown", index=True)
    basis: str = Field(default="")
    source: str = Field(default="operator")
    note: Optional[str] = None
    recipient: str = Field(default="")
    recorded_by: str = Field(default="operator")
    recorded_at: Optional[str] = Field(default=None, index=True)
    expires_at: Optional[str] = None
    updated_at: Optional[str] = None


class LeadSuppression(SQLModel, table=True):
    __tablename__ = "lead_suppressions"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    channel: str = Field(default="all", index=True)
    status: str = Field(default="active", index=True)
    reason: str = Field(default="")
    source: str = Field(default="operator")
    note: Optional[str] = None
    created_by: str = Field(default="operator")
    created_at: Optional[str] = Field(default=None, index=True)
    released_at: Optional[str] = None
    updated_at: Optional[str] = None


class LeadFunnel(SQLModel, table=True):
    __tablename__ = "lead_funnels"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    funnel_type: str = Field(index=True)
    stage: str = Field(default="lead_captured", index=True)
    status: str = Field(default="active", index=True)
    owner: str = Field(default="operator")
    summary: Optional[str] = None
    next_step_title: str = Field(default="")
    next_step_due_at: Optional[str] = None
    booked_at: Optional[str] = None
    completed_at: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class LeadFunnelEvent(SQLModel, table=True):
    __tablename__ = "lead_funnel_events"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    funnel_id: str = Field(index=True)
    funnel_type: str = Field(index=True)
    event_type: str = Field(default="note", index=True)
    title: str
    detail: str = Field(default="")
    payload: Dict[str, Any] = Field(default_factory=dict, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
