"""
Deterministic intelligence models in the `intelligence` schema.

These tables extend the existing property/party/property_party core so the
repo can project raw lead imports into a normalized, explainable intelligence
layer without changing the current `leads` write path.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import Column as SAColumn, JSON, Text
from sqlmodel import Field, SQLModel

try:
    from pgvector.sqlalchemy import Vector

    def _vector_col():
        return SAColumn(Vector(512))

except ImportError:

    def _vector_col():
        return SAColumn(Text)


class IntelligenceProperty(SQLModel, table=True):
    """Canonical property_profile record. PK = md5(canonical address)."""

    __tablename__ = "property"
    __table_args__ = {"schema": "intelligence"}

    id: str = Field(primary_key=True)
    address: str = Field(sa_column=SAColumn(Text, unique=True, nullable=False))
    suburb: Optional[str] = None
    postcode: Optional[str] = None
    cadid: Optional[str] = None
    geometry_wkt: Optional[str] = Field(default=None, sa_column=SAColumn(Text))
    h3index: Optional[str] = None
    lat: float = Field(default=0.0)
    lng: float = Field(default=0.0)
    est_value: int = Field(default=0)
    zoning_type: Optional[str] = None
    parcel_details: Optional[str] = None
    property_type: Optional[str] = None
    land_size_sqm: Optional[float] = None
    last_sale_date: Optional[str] = None
    last_sale_price: Optional[int] = None
    estimated_value: Optional[int] = None
    last_settlement_date: Optional[str] = None
    trigger_type: Optional[str] = None
    status: str = Field(default="captured")
    route_queue: str = Field(default="")
    heat_score: int = Field(default=0)
    evidence_score: int = Field(default=0)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class IntelligenceParty(SQLModel, table=True):
    """Canonical person_profile record."""

    __tablename__ = "party"
    __table_args__ = {"schema": "intelligence"}

    id: str = Field(primary_key=True)
    full_name: str
    phone: Optional[str] = None
    owner_type: Optional[str] = None
    absentee_flag: Optional[bool] = None
    investor_flag: Optional[bool] = None
    source: str = Field(default="")
    cotality_contact_id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class IntelligencePropertyParty(SQLModel, table=True):
    """property_person_link many-to-many join."""

    __tablename__ = "property_party"
    __table_args__ = {"schema": "intelligence"}

    id: str = Field(primary_key=True)
    property_id: str = Field(index=True)
    party_id: str = Field(index=True)
    role: str = Field(default="")
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class IntelligenceAgentProfile(SQLModel, table=True):
    """Deterministic suburb-level agent_profile rollup."""

    __tablename__ = "agent_profile"
    __table_args__ = {"schema": "intelligence"}

    id: str = Field(primary_key=True)
    agent_name: str
    agency_name: Optional[str] = None
    suburb: Optional[str] = Field(default=None, index=True)
    suburb_activity_count: int = Field(default=0)
    last_updated: Optional[str] = None


class IntelligenceLeadIntelligence(SQLModel, table=True):
    """Per-property deterministic lead intelligence snapshot."""

    __tablename__ = "lead_intelligence"
    __table_args__ = {"schema": "intelligence"}

    property_id: str = Field(primary_key=True)
    intent_score: float = Field(default=0.0)
    contactability_score: float = Field(default=0.0)
    priority_rank: float = Field(default=0.0, index=True)
    tags_json: List[str] = Field(default=[], sa_column=SAColumn(JSON))
    reasons_json: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON))
    ownership_years: Optional[float] = None
    equity_proxy: Optional[int] = None
    absentee_owner: Optional[bool] = None
    company_owner: Optional[bool] = None
    investor_flag: Optional[bool] = None
    listing_failure_signal: Optional[float] = None
    same_owner_property_count: int = Field(default=0)
    nearby_sales_count: int = Field(default=0)
    agent_dominance_score: Optional[float] = None
    last_updated: Optional[str] = None


class IntelligenceEvent(SQLModel, table=True):
    __tablename__ = "event"
    __table_args__ = {"schema": "intelligence"}

    id: str = Field(primary_key=True)
    property_id: str = Field(index=True)
    event_type: str = Field(default="")
    source: str = Field(default="")
    raw_payload: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON))
    occurred_at: Optional[str] = None
    created_at: Optional[str] = None


class IntelligenceMedia(SQLModel, table=True):
    __tablename__ = "media"
    __table_args__ = {"schema": "intelligence"}

    id: str = Field(primary_key=True)
    property_id: str = Field(index=True)
    image_url: Optional[str] = None
    image_embedding: Optional[str] = Field(default=None, sa_column=_vector_col())
    perceptual_hash: Optional[str] = None
    created_at: Optional[str] = None


INTELLIGENCE_METADATA = SQLModel.metadata
