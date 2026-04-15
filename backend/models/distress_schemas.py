from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class DistressSourcePayload(BaseModel):
    id: str
    source_key: str
    label: str
    signal_type: str
    enabled: bool = False
    mode: str = "manual_feed"
    cadence_minutes: int = 1440
    source_url: Optional[str] = None
    coverage_suburbs: List[str] = Field(default_factory=list)
    notes: str = ""
    last_run_at: Optional[str] = None
    last_success_at: Optional[str] = None
    last_error: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DistressRunPayload(BaseModel):
    id: str
    source_id: Optional[str] = None
    source_key: str
    requested_by: str = "scheduler"
    status: str
    records_scanned: int = 0
    records_created: int = 0
    records_linked: int = 0
    records_created_as_leads: int = 0
    error_summary: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: Optional[str] = None


class DistressSignalPayload(BaseModel):
    id: str
    source_key: str
    signal_type: str
    external_ref: str
    title: str
    owner_name: Optional[str] = None
    address: Optional[str] = None
    suburb: Optional[str] = None
    postcode: Optional[str] = None
    description: str = ""
    occurred_at: Optional[str] = None
    source_name: str = ""
    source_url: Optional[str] = None
    confidence_score: float = 70
    severity_score: int = 50
    status: str = "captured"
    lead_ids: List[str] = Field(default_factory=list)
    inferred_owner_matches: List[str] = Field(default_factory=list)
    inferred_property_matches: List[str] = Field(default_factory=list)
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DistressSourceListResponse(BaseModel):
    sources: List[DistressSourcePayload] = Field(default_factory=list)


class DistressRunListResponse(BaseModel):
    runs: List[DistressRunPayload] = Field(default_factory=list)


class DistressSignalListResponse(BaseModel):
    signals: List[DistressSignalPayload] = Field(default_factory=list)
    total: int = 0


class DistressSourceRunRequest(BaseModel):
    requested_by: str = "operator"
    force: bool = True


class DistressManualSignalInput(BaseModel):
    external_ref: Optional[str] = None
    signal_type: Optional[str] = None
    title: str
    owner_name: Optional[str] = None
    address: Optional[str] = None
    suburb: Optional[str] = None
    postcode: Optional[str] = None
    description: str = ""
    occurred_at: Optional[str] = None
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    confidence_score: float = 70
    severity_score: int = 50
    payload: Dict[str, Any] = Field(default_factory=dict)


class DistressManualIngestRequest(BaseModel):
    source_key: str
    requested_by: str = "operator"
    signals: List[DistressManualSignalInput] = Field(default_factory=list)


class DistressLandlordCandidate(BaseModel):
    owner_name: str
    property_count: int = 0
    investor_records: int = 0
    newest_record_date: Optional[str] = None
    years_since_recorded_finance_event: Optional[float] = None
    inferred_no_recent_refi: bool = False
    delinquent_tax_signal_count: int = 0
    lien_signal_count: int = 0
    addresses: List[str] = Field(default_factory=list)
    lead_ids: List[str] = Field(default_factory=list)
    inference_note: str = ""


class DistressLandlordWatchResponse(BaseModel):
    landlords: List[DistressLandlordCandidate] = Field(default_factory=list)
