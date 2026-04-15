from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ConsentUpsertRequest(BaseModel):
    channel: str
    purpose: str
    status: str
    basis: str = "operator_confirmed"
    source: str = "operator"
    note: Optional[str] = None
    recipient: str = ""
    recorded_by: str = "operator"
    expires_at: Optional[str] = None


class SuppressionUpsertRequest(BaseModel):
    channel: str = "all"
    reason: str
    note: Optional[str] = None
    source: str = "operator"
    created_by: str = "operator"


class FunnelStageUpdateRequest(BaseModel):
    stage: str
    note: Optional[str] = None
    owner: str = "operator"
    next_step_title: Optional[str] = None
    next_step_due_at: Optional[str] = None


class FunnelBookingRequest(BaseModel):
    starts_at: str
    location: str = ""
    note: Optional[str] = None
    booked_by: str = "operator"


class FunnelOutreachTaskRequest(BaseModel):
    funnel_type: str
    channel: str
    due_at: str
    title: Optional[str] = None
    subject: Optional[str] = None
    message: Optional[str] = None
    recipient: Optional[str] = None
    note: Optional[str] = None
    requested_by: str = "operator"


class ConsentPayload(BaseModel):
    id: str
    lead_id: str
    channel: str
    purpose: str
    status: str
    basis: str
    source: str
    note: Optional[str] = None
    recipient: str = ""
    recorded_by: str
    recorded_at: Optional[str] = None
    expires_at: Optional[str] = None
    updated_at: Optional[str] = None


class SuppressionPayload(BaseModel):
    id: str
    lead_id: str
    channel: str
    status: str
    reason: str
    source: str
    note: Optional[str] = None
    created_by: str
    created_at: Optional[str] = None
    released_at: Optional[str] = None
    updated_at: Optional[str] = None


class FunnelPayload(BaseModel):
    id: str
    lead_id: str
    funnel_type: str
    stage: str
    status: str
    owner: str
    summary: Optional[str] = None
    next_step_title: str = ""
    next_step_due_at: Optional[str] = None
    booked_at: Optional[str] = None
    completed_at: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class FunnelEventPayload(BaseModel):
    id: str
    lead_id: str
    funnel_id: str
    funnel_type: str
    event_type: str
    title: str
    detail: str = ""
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None


class OutreachGuardPayload(BaseModel):
    channel: str
    purpose: str
    allowed: bool
    consent_status: str = "unknown"
    active_suppression: bool = False
    reasons: List[str] = Field(default_factory=list)


class FunnelTaskResponse(BaseModel):
    status: str
    guard: OutreachGuardPayload
    task: Dict[str, Any]
    funnel: FunnelPayload


class LeadFunnelsResponse(BaseModel):
    lead_id: str
    funnels: List[FunnelPayload] = Field(default_factory=list)
    consents: List[ConsentPayload] = Field(default_factory=list)
    suppressions: List[SuppressionPayload] = Field(default_factory=list)
    events: List[FunnelEventPayload] = Field(default_factory=list)
    guards: List[OutreachGuardPayload] = Field(default_factory=list)
