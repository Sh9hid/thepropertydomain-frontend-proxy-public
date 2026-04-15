from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class LeadContactPayload(BaseModel):
    id: str
    business_context_key: str
    lead_id: Optional[str] = None
    property_id: Optional[str] = None
    full_name: str = ""
    primary_phone: Optional[str] = None
    secondary_phone: Optional[str] = None
    primary_email: Optional[str] = None
    contact_role: Optional[str] = None
    owner_type: Optional[str] = None
    source: Optional[str] = None
    phone_verification_status: Optional[str] = None
    email_verification_status: Optional[str] = None
    enrichment_source: Optional[str] = None
    metadata_json: Dict[str, Any] = Field(default_factory=dict)


class ContactAttemptPayload(BaseModel):
    id: str
    business_context_key: str
    lead_contact_id: str
    attempted_at: datetime
    channel: str
    outcome: str
    connected: bool = False
    duration_seconds: Optional[int] = None
    voicemail_left: bool = False
    note: Optional[str] = None
    transcript_id: Optional[str] = None
    recording_id: Optional[str] = None
    recipient_email: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    sequence_key: Optional[str] = None
    sequence_step: Optional[int] = None
    variant_key: Optional[str] = None
    parent_attempt_id: Optional[str] = None
    external_message_id: Optional[str] = None
    opened_at: Optional[datetime] = None
    replied_at: Optional[datetime] = None
    performance_json: Dict[str, Any] = Field(default_factory=dict)
    next_action_due_at: Optional[datetime] = None
    created_by: Optional[str] = None


class LeadStatePayload(BaseModel):
    lead_contact_id: str
    business_context_key: str
    total_attempts: int = 0
    attempts_last_7d: int = 0
    last_attempt_at: Optional[datetime] = None
    last_attempt_outcome: Optional[str] = None
    fatigue_band: str = "low"
    callable_now: bool = False
    next_action: str = "review"
    next_action_due_at: Optional[datetime] = None
    queue_score: float = 0.0
    needs_enrichment: bool = False
    stale_enrichment: bool = False
    summary_json: Dict[str, Any] = Field(default_factory=dict)


class TaskQueuePayload(BaseModel):
    id: str
    business_context_key: str
    lead_contact_id: str
    task_type: str
    due_at: datetime
    status: str
    priority: int
    reason: Optional[str] = None
    payload_json: Dict[str, Any] = Field(default_factory=dict)
    created_by: Optional[str] = None


class EnrichmentQueueRequest(BaseModel):
    business_context_key: str
    lead_contact_id: Optional[str] = None
    target_type: str = "lead_contact"
    target_id: Optional[str] = None
    source: str = "rp_data"
    reason: str = "missing_contactability"


class LogContactAttemptRequest(BaseModel):
    business_context_key: str
    lead_contact_id: str
    channel: str = "call"
    outcome: str
    connected: bool = False
    duration_seconds: int = 0
    voicemail_left: bool = False
    note: str = ""
    transcript_id: Optional[str] = None
    recording_id: Optional[str] = None
    recipient_email: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    sequence_key: Optional[str] = None
    sequence_step: Optional[int] = None
    variant_key: Optional[str] = None
    parent_attempt_id: Optional[str] = None
    external_message_id: Optional[str] = None
    opened_at: Optional[datetime] = None
    replied_at: Optional[datetime] = None
    performance_json: Dict[str, Any] = Field(default_factory=dict)
    next_action_due_at: Optional[datetime] = None
    created_by: Optional[str] = None


class DialingContextResponse(BaseModel):
    contact: LeadContactPayload
    lead: Optional[Dict[str, Any]] = None
    state: Optional[LeadStatePayload] = None
    tasks: List[TaskQueuePayload] = Field(default_factory=list)
    attempts: List[ContactAttemptPayload] = Field(default_factory=list)


class LogContactAttemptResponse(BaseModel):
    attempt: ContactAttemptPayload
    state: LeadStatePayload
    task: Optional[TaskQueuePayload] = None
