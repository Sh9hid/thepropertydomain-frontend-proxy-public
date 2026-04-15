import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import Column as SAColumn, JSON
from sqlmodel import Field, SQLModel


class DistressSource(SQLModel, table=True):
    __tablename__ = "distress_sources"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    source_key: str = Field(index=True, unique=True)
    label: str
    signal_type: str = Field(index=True)
    enabled: bool = Field(default=False, index=True)
    mode: str = Field(default="manual_feed")
    cadence_minutes: int = Field(default=1440)
    source_url: Optional[str] = None
    coverage_suburbs: List[str] = Field(default=[], sa_column=SAColumn(JSON))
    notes: str = Field(default="")
    last_run_at: Optional[str] = None
    last_success_at: Optional[str] = None
    last_error: Optional[str] = None
    metrics: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON))
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DistressRun(SQLModel, table=True):
    __tablename__ = "distress_runs"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    source_id: Optional[str] = Field(default=None, index=True)
    source_key: str = Field(index=True)
    requested_by: str = Field(default="scheduler")
    status: str = Field(default="queued", index=True)
    records_scanned: int = Field(default=0)
    records_created: int = Field(default=0)
    records_linked: int = Field(default=0)
    records_created_as_leads: int = Field(default=0)
    error_summary: Optional[str] = None
    metrics: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON))
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: Optional[str] = None


class DistressSignal(SQLModel, table=True):
    __tablename__ = "distress_signals"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    source_key: str = Field(index=True)
    signal_type: str = Field(index=True)
    external_ref: str = Field(index=True)
    title: str
    owner_name: Optional[str] = Field(default=None, index=True)
    address: Optional[str] = Field(default=None, index=True)
    suburb: Optional[str] = Field(default=None, index=True)
    postcode: Optional[str] = Field(default=None, index=True)
    description: str = Field(default="")
    occurred_at: Optional[str] = None
    source_name: str = Field(default="")
    source_url: Optional[str] = None
    confidence_score: float = Field(default=70)
    severity_score: int = Field(default=50)
    status: str = Field(default="captured", index=True)
    lead_ids: List[str] = Field(default=[], sa_column=SAColumn(JSON))
    inferred_owner_matches: List[str] = Field(default=[], sa_column=SAColumn(JSON))
    inferred_property_matches: List[str] = Field(default=[], sa_column=SAColumn(JSON))
    payload: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON))
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DistressSignalLink(SQLModel, table=True):
    __tablename__ = "distress_signal_links"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    signal_id: str = Field(index=True)
    lead_id: str = Field(index=True)
    link_type: str = Field(default="address_match")
    confidence_score: float = Field(default=70)
    rationale: str = Field(default="")
    created_at: Optional[str] = None
