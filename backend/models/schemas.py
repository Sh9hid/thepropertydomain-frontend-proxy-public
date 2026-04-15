from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from models.sql_models import COTALITY_PROPERTY_INTELLIGENCE_WORKFLOW_NAME

JSON_COLUMNS = {
    "potential_contacts",
    "contact_emails",
    "contact_phones",
    "key_details",
    "features",
    "summary_points",
    "next_actions",
    "source_evidence",
    "linked_files",
    "stage_note_history",
    "activity_log",
    "property_images",
    "source_tags",
    "risk_flags",
    "alternate_phones",
    "alternate_emails",
    "contactability_reasons",
    "sale_history",
    "listing_status_history",
    "nearby_sales",
    "deterministic_tags",
    "seller_intent_signals",
    "refinance_signals",
    "source_provenance",
}

LEAD_STATUS_ORDER = [
    "captured",
    "qualified",
    "outreach_ready",
    "contacted",
    "appt_booked",
    "mortgage_appt_booked",
    "converted",
    "dropped",
]

LEAD_STATUS_TRANSITIONS = {
    "captured": {"qualified", "dropped"},
    "qualified": {"outreach_ready", "dropped"},
    "outreach_ready": {"contacted", "dropped"},
    "contacted": {"appt_booked", "mortgage_appt_booked", "qualified", "dropped"},
    "appt_booked": {"converted", "contacted", "dropped"},
    "mortgage_appt_booked": {"converted", "contacted", "dropped"},
    "converted": set(),
    "dropped": set(),
}

LEAD_COLUMNS_SQL = {
    "id": "TEXT PRIMARY KEY",
    "address": "TEXT UNIQUE",
    "suburb": "TEXT",
    "postcode": "TEXT",
    "owner_name": "TEXT",
    "trigger_type": "TEXT",
    "record_type": "TEXT",
    "heat_score": "INTEGER",
    "scenario": "TEXT",
    "strategic_value": "TEXT",
    "contact_status": "TEXT",
    "ownership_tenure": "TEXT",
    "equity_estimate": "TEXT",
    "confidence_score": "INTEGER",
    "potential_contacts": "TEXT",
    "contact_emails": "TEXT",
    "contact_phones": "TEXT",
    "lat": "REAL",
    "lng": "REAL",
    "est_value": "INTEGER",
    "date_found": "TEXT",
    "signal_status": "TEXT",
    "key_details": "TEXT",
    "main_image": "TEXT",
    "property_images": "TEXT",
    "description_deep": "TEXT",
    "features": "TEXT",
    "conversion_strategy": "TEXT",
    "summary_points": "TEXT",
    "horizon": "TEXT",
    "last_checked": "TEXT",
    "exhaustive_summary": "TEXT",
    "likely_scenario": "TEXT",
    "strategic_why": "TEXT",
    "owner_age": "INTEGER",
    "suburb_average_tenure": "INTEGER",
    "propensity_score": "INTEGER",
    "recent_sales_velocity": "TEXT",
    "est_net_profit": "INTEGER",
    "local_dominance": "TEXT",
    "zoning_type": "TEXT",
    "status": "TEXT DEFAULT 'captured'",
    "conversion_score": "INTEGER DEFAULT 0",
    "compliance_score": "INTEGER DEFAULT 0",
    "readiness_score": "INTEGER DEFAULT 0",
    "call_today_score": "INTEGER DEFAULT 0",
    "evidence_score": "INTEGER DEFAULT 0",
    "lifecycle_stage": "TEXT",
    "who_to_call": "TEXT",
    "why_now": "TEXT",
    "what_to_say": "TEXT",
    "recommended_next_step": "TEXT",
    "risk_flags": "TEXT",
    "source_tags": "TEXT",
    "external_link": "TEXT",
    "next_actions": "TEXT",
    "source_evidence": "TEXT",
    "linked_files": "TEXT",
    "bedrooms": "REAL",
    "bathrooms": "REAL",
    "car_spaces": "REAL",
    "land_size_sqm": "REAL",
    "floor_size_sqm": "REAL",
    "year_built": "TEXT",
    "sale_price": "TEXT",
    "sale_date": "TEXT",
    "settlement_date": "TEXT",
    "agency_name": "TEXT",
    "agent_name": "TEXT",
    "owner_type": "TEXT",
    "land_use": "TEXT",
    "development_zone": "TEXT",
    "parcel_details": "TEXT",
    "canonical_address": "TEXT",
    "address_unit": "TEXT",
    "street_number": "TEXT",
    "street_name": "TEXT",
    "street_type": "TEXT",
    "state": "TEXT",
    "country_code": "TEXT",
    "mailing_address": "TEXT",
    "mailing_address_matches_property": "INTEGER DEFAULT 1",
    "absentee_owner": "INTEGER DEFAULT 0",
    "likely_landlord": "INTEGER DEFAULT 0",
    "likely_owner_occupier": "INTEGER DEFAULT 0",
    "owner_occupancy_status": "TEXT",
    "owner_first_name": "TEXT",
    "owner_last_name": "TEXT",
    "owner_persona": "TEXT",
    "alternate_phones": "TEXT",
    "alternate_emails": "TEXT",
    "phone_status": "TEXT",
    "phone_line_type": "TEXT",
    "email_status": "TEXT",
    "do_not_call": "INTEGER DEFAULT 0",
    "consent_status": "TEXT",
    "contactability_tier": "TEXT",
    "contactability_reasons": "TEXT",
    "property_type": "TEXT",
    "parcel_lot": "TEXT",
    "parcel_plan": "TEXT",
    "title_reference": "TEXT",
    "ownership_duration_years": "REAL",
    "tenure_bucket": "TEXT",
    "estimated_value_low": "INTEGER",
    "estimated_value_mid": "INTEGER",
    "estimated_value_high": "INTEGER",
    "valuation_confidence": "TEXT",
    "valuation_date": "TEXT",
    "rental_estimate_low": "INTEGER",
    "rental_estimate_high": "INTEGER",
    "yield_estimate": "REAL",
    "last_listing_status": "TEXT",
    "last_listing_date": "TEXT",
    "sale_history": "TEXT",
    "listing_status_history": "TEXT",
    "nearby_sales": "TEXT",
    "deterministic_tags": "TEXT",
    "seller_intent_signals": "TEXT",
    "refinance_signals": "TEXT",
    "ownership_notes": "TEXT",
    "source_provenance": "TEXT",
    "enrichment_status": "TEXT",
    "enrichment_last_synced_at": "TEXT",
    "research_status": "TEXT",
    "created_at": "TEXT",
    "updated_at": "TEXT",
    "last_contacted_at": "TEXT",
    "follow_up_due_at": "TEXT",
    "last_inbound_at": "TEXT",
    "last_outbound_at": "TEXT",
    "last_called_date": "TEXT",
    "price_drop_count": "INTEGER DEFAULT 0",
    "queue_bucket": "TEXT DEFAULT ''",
    "lead_archetype": "TEXT DEFAULT ''",
    "contactability_status": "TEXT DEFAULT ''",
    "owner_verified": "INTEGER DEFAULT 0",
    "contact_role": "TEXT DEFAULT ''",
    "cadence_name": "TEXT DEFAULT ''",
    "cadence_step": "INTEGER DEFAULT 0",
    "next_action_at": "TEXT",
    "next_action_type": "TEXT DEFAULT ''",
    "next_action_channel": "TEXT DEFAULT ''",
    "next_action_title": "TEXT DEFAULT ''",
    "next_action_reason": "TEXT DEFAULT ''",
    "next_message_template": "TEXT DEFAULT ''",
    "last_outcome": "TEXT DEFAULT ''",
    "last_outcome_at": "TEXT",
    "last_activity_type": "TEXT DEFAULT ''",
    "objection_reason": "TEXT DEFAULT ''",
    "preferred_channel": "TEXT DEFAULT ''",
    "strike_zone": "TEXT DEFAULT ''",
    "touches_14d": "INTEGER DEFAULT 0",
    "touches_30d": "INTEGER DEFAULT 0",
    "do_not_contact_until": "TEXT",
    "stage_note": "TEXT",
    "stage_note_history": "TEXT",
    "activity_log": "TEXT",
}

LEAD_COLUMNS = list(LEAD_COLUMNS_SQL.keys())

LEDGER_COLUMNS = [
    "id",
    "address",
    "suburb",
    "postcode",
    "owner_name",
    "trigger_type",
    "record_type",
    "status",
    "scenario",
    "main_image",
    "lat",
    "lng",
    "call_today_score",
    "evidence_score",
    "heat_score",
    "propensity_score",
    "readiness_score",
    "conversion_score",
    "confidence_score",
    "lifecycle_stage",
    "next_action_at",
    "next_action_title",
    "updated_at",
    "created_at",
]

COMMUNICATION_ACCOUNT_COLUMNS_SQL = {
    "id": "TEXT PRIMARY KEY",
    "label": "TEXT",
    "provider": "TEXT",
    "api_base": "TEXT",
    "access_token": "TEXT",
    "send_path": "TEXT",
    "from_number": "TEXT",
    "webhook_secret": "TEXT",
    "client_id": "TEXT",
    "client_secret": "TEXT",
    "account_id": "TEXT",
    "token_url": "TEXT",
    "webhook_url": "TEXT",
    "use_account_credentials": "INTEGER DEFAULT 1",
    "send_enabled": "INTEGER DEFAULT 0",
    "call_enabled": "INTEGER DEFAULT 1",
    "text_enabled": "INTEGER DEFAULT 1",
    "verify_ssl": "INTEGER DEFAULT 1",
    "created_at": "TEXT",
    "updated_at": "TEXT",
}

TASK_COLUMNS_SQL = {
    "id": "TEXT PRIMARY KEY",
    "lead_id": "TEXT",
    "title": "TEXT",
    "task_type": "TEXT",
    "action_type": "TEXT DEFAULT ''",
    "channel": "TEXT",
    "due_at": "TEXT",
    "status": "TEXT",
    "notes": "TEXT",
    "related_report_id": "TEXT",
    "approval_status": "TEXT DEFAULT 'not_required'",
    "message_subject": "TEXT DEFAULT ''",
    "message_preview": "TEXT DEFAULT ''",
    "rewrite_reason": "TEXT DEFAULT ''",
    "superseded_by": "TEXT DEFAULT ''",
    "cadence_name": "TEXT DEFAULT ''",
    "cadence_step": "INTEGER DEFAULT 0",
    "auto_generated": "INTEGER DEFAULT 0",
    "priority_bucket": "TEXT DEFAULT ''",
    "completed_at": "TEXT",
    "created_at": "TEXT",
    "updated_at": "TEXT",
}

APPOINTMENT_COLUMNS_SQL = {
    "id": "TEXT PRIMARY KEY",
    "lead_id": "TEXT",
    "title": "TEXT",
    "starts_at": "TEXT",
    "status": "TEXT",
    "location": "TEXT",
    "notes": "TEXT",
    "cadence_name": "TEXT DEFAULT ''",
    "auto_generated": "INTEGER DEFAULT 0",
    "created_at": "TEXT",
    "updated_at": "TEXT",
}

SOLD_EVENT_COLUMNS_SQL = {
    "id": "TEXT PRIMARY KEY",
    "address": "TEXT",
    "suburb": "TEXT",
    "postcode": "TEXT",
    "sale_date": "TEXT",
    "sale_price": "TEXT",
    "lat": "REAL",
    "lng": "REAL",
    "source_name": "TEXT",
    "source_url": "TEXT",
    "match_reason": "TEXT DEFAULT ''",
    "matched_lead_ids": "TEXT DEFAULT '[]'",
    "created_at": "TEXT",
    "updated_at": "TEXT",
}

class PotentialContact(BaseModel):
    type: str
    value: str
    probability: int
    source: str


class NextAction(BaseModel):
    title: str
    owner: str = "Shahid"
    due_at: Optional[str] = None
    channel: Optional[str] = None
    message_template_id: Optional[str] = None


class Lead(BaseModel):
    id: str
    address: str
    suburb: str = ""
    postcode: str = ""
    owner_name: str = ""
    trigger_type: str = ""
    record_type: str = "property_record"
    signal_status: str = ""
    heat_score: int = 0
    scenario: str = ""
    strategic_value: str = ""
    contact_status: str = ""
    ownership_tenure: str = ""
    equity_estimate: str = ""
    confidence_score: int = 0
    potential_contacts: List[PotentialContact] = []
    contact_emails: List[str] = []
    contact_phones: List[str] = []
    lat: float = 0
    lng: float = 0
    est_value: int = 0
    date_found: str = ""
    key_details: List[str] = []
    main_image: str = ""
    property_images: List[str] = []
    visual_url: str = ""
    visual_source: str = ""
    visual_label: str = ""
    visual_is_fallback: bool = False
    street_view_embed_url: str = ""
    description_deep: str = ""
    features: List[str] = []
    conversion_strategy: str = ""
    summary_points: List[str] = []
    horizon: str = ""
    last_checked: str = ""
    exhaustive_summary: str = ""
    likely_scenario: str = ""
    strategic_why: str = ""
    owner_age: Optional[int] = None
    suburb_average_tenure: Optional[int] = None
    propensity_score: Optional[int] = None
    recent_sales_velocity: Optional[str] = None
    est_net_profit: Optional[int] = None
    local_dominance: Optional[str] = None
    zoning_type: Optional[str] = None
    status: str = "captured"
    conversion_score: int = 0
    compliance_score: int = 0
    readiness_score: int = 0
    call_today_score: int = 0
    evidence_score: int = 0
    lifecycle_stage: Optional[str] = None
    who_to_call: str = ""
    why_now: str = ""
    what_to_say: str = ""
    recommended_next_step: str = ""
    risk_flags: List[str] = []
    source_tags: List[str] = []
    external_link: str = ""
    next_actions: List[NextAction] = []
    source_evidence: List[str] = []
    linked_files: List[str] = []
    bedrooms: Optional[float] = None
    bathrooms: Optional[float] = None
    car_spaces: Optional[float] = None
    land_size_sqm: Optional[float] = None
    floor_size_sqm: Optional[float] = None
    year_built: Optional[str] = None
    sale_price: Optional[str] = None
    sale_date: Optional[str] = None
    settlement_date: Optional[str] = None
    agency_name: Optional[str] = None
    agent_name: Optional[str] = None
    owner_type: Optional[str] = None
    land_use: Optional[str] = None
    development_zone: Optional[str] = None
    parcel_details: Optional[str] = None
    canonical_address: Optional[str] = None
    address_unit: Optional[str] = None
    street_number: Optional[str] = None
    street_name: Optional[str] = None
    street_type: Optional[str] = None
    state: Optional[str] = None
    country_code: Optional[str] = None
    mailing_address: Optional[str] = None
    mailing_address_matches_property: bool = True
    absentee_owner: bool = False
    likely_landlord: bool = False
    likely_owner_occupier: bool = False
    owner_occupancy_status: Optional[str] = None
    owner_first_name: Optional[str] = None
    owner_last_name: Optional[str] = None
    owner_persona: Optional[str] = None
    alternate_phones: List[str] = []
    alternate_emails: List[str] = []
    phone_status: Optional[str] = None
    phone_line_type: Optional[str] = None
    email_status: Optional[str] = None
    do_not_call: bool = False
    consent_status: Optional[str] = None
    contactability_tier: Optional[str] = None
    contactability_reasons: List[str] = []
    property_type: Optional[str] = None
    parcel_lot: Optional[str] = None
    parcel_plan: Optional[str] = None
    title_reference: Optional[str] = None
    ownership_duration_years: Optional[float] = None
    tenure_bucket: Optional[str] = None
    estimated_value_low: Optional[int] = None
    estimated_value_mid: Optional[int] = None
    estimated_value_high: Optional[int] = None
    valuation_confidence: Optional[str] = None
    valuation_date: Optional[str] = None
    rental_estimate_low: Optional[int] = None
    rental_estimate_high: Optional[int] = None
    yield_estimate: Optional[float] = None
    last_listing_status: Optional[str] = None
    last_listing_date: Optional[str] = None
    sale_history: List[Dict[str, Any]] = []
    listing_status_history: List[Dict[str, Any]] = []
    nearby_sales: List[Dict[str, Any]] = []
    deterministic_tags: List[str] = []
    seller_intent_signals: List[Dict[str, Any]] = []
    refinance_signals: List[Dict[str, Any]] = []
    ownership_notes: Optional[str] = None
    source_provenance: List[Dict[str, Any]] = []
    enrichment_status: Optional[str] = None
    enrichment_last_synced_at: Optional[str] = None
    research_status: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_contacted_at: Optional[str] = None
    follow_up_due_at: Optional[str] = None
    last_inbound_at: Optional[str] = None
    last_outbound_at: Optional[str] = None
    last_called_date: Optional[str] = None
    price_drop_count: int = 0
    relisted: bool = False
    list_date: Optional[str] = None
    queue_bucket: str = ""
    lead_archetype: str = ""
    contactability_status: str = ""
    owner_verified: bool = False
    contact_role: str = ""
    cadence_name: str = ""
    cadence_step: int = 0
    next_action_at: str = ""
    next_action_type: str = ""
    next_action_channel: str = ""
    next_action_title: str = ""
    next_action_reason: str = ""
    next_message_template: str = ""
    last_outcome: str = ""
    last_outcome_at: Optional[str] = None
    last_activity_type: str = ""
    objection_reason: str = ""
    preferred_channel: str = ""
    strike_zone: str = ""
    touches_14d: int = 0
    touches_30d: int = 0
    do_not_contact_until: Optional[str] = None
    stage_note: Optional[str] = None
    stage_note_history: List[Dict[str, Any]] = []
    activity_log: List[Dict[str, Any]] = []


class CallLogOut(BaseModel):
    id: str
    lead_id: str
    lead_address: str = ""
    user_id: str = "Shahid"
    outcome: str
    connected: bool = False
    timestamp: str = ""
    call_duration_seconds: int = 0
    duration_seconds: int = 0
    note: str = ""
    operator: str = "Shahid"
    logged_at: str = ""
    logged_date: str = ""
    transcript: Optional[str] = None
    summary: Optional[str] = None
    intent_signal: float = 0.0
    booking_attempted: bool = False
    objection_tags: str = "[]"
    next_step_detected: bool = False


class SpeechUploadResponse(BaseModel):
    call_id: str
    source: str
    status: str
    audio_uri: str
    pipeline: Dict[str, Any]


class CallScoreComponentPayload(BaseModel):
    score_name: str
    score_value: float
    raw_value: float
    normalized_value: float
    weight: float
    stable_flag: int = 0
    evidence_json: Optional[str] = None


class CallAnalysisResponse(BaseModel):
    call: Dict[str, Any]
    analysis: Dict[str, Any]
    scores: Dict[str, Any]


class CoachingReportResponse(BaseModel):
    id: str
    call_id: str
    rep_id: str = ""
    report_version: str = "v0"
    brutal_summary: str = ""
    detailed_breakdown: Dict[str, Any] = {}
    rewrite: Dict[str, Any] = {}
    drills: List[str] = []
    live_task: str = ""
    generated_at: Optional[str] = None


class RepScoreSummaryResponse(BaseModel):
    rep_id: str
    latest: Dict[str, Any] = {}
    moving_average: Dict[str, float] = {}
    snapshots: List[Dict[str, Any]] = []


class AgentUpdate(BaseModel):
    id: str
    status: Optional[str] = None
    activity: Optional[str] = None
    health: Optional[int] = None


class LeadAdvanceRequest(BaseModel):
    status: str
    note: Optional[str] = None
    last_contacted_at: Optional[str] = None


class OutreachPackRequest(BaseModel):
    tone: str = "professional"


class ActivityLogRequest(BaseModel):
    activity_type: str
    note: Optional[str] = None
    status: Optional[str] = None
    channel: Optional[str] = None
    subject: Optional[str] = None
    recipient: Optional[str] = None


class ManualLeadRequest(BaseModel):
    address: str
    suburb: str = ""
    postcode: str = ""
    owner_name: str = ""
    owner_type: str = ""
    trigger_type: str = "RP Data"
    lifecycle_stage: Optional[str] = "manual_entry"
    scenario: str = ""
    notes: str = ""
    description_deep: str = ""
    main_image: str = ""
    property_images: List[str] = []
    contacts: List[Dict[str, Any]] = []
    contact_emails: List[str] = []
    contact_phones: List[str] = []
    source_tags: List[str] = []
    source_evidence: List[str] = []
    linked_files: List[str] = []
    follow_up_due_at: Optional[str] = None
    last_activity_type: str = ""
    lat: float = 0
    lng: float = 0
    status: str = "captured"
    source: str = ""


class BulkCreateRequest(BaseModel):
    csv_data: str
    source: str = "rp_data"
    date_added: Optional[str] = None  # ISO date override for when leads were collected (e.g. door-knock date)


class EmailAccount(BaseModel):
    id: Optional[str] = None
    label: str
    smtp_host: str
    smtp_port: int = 587
    smtp_username: str
    smtp_password: str
    from_name: Optional[str] = None
    from_email: Optional[str] = None
    use_tls: bool = True
    daily_cap: int = 80
    is_warmup_mode: bool = False
    warmup_day: int = 0
    is_active: bool = True


class SendEmailRequest(BaseModel):
    account_id: str
    recipient: str
    subject: str
    body: str
    plain_text: bool = False
    attachment_paths: List[str] = []


class CommunicationAccount(BaseModel):
    id: Optional[str] = None
    label: str
    provider: str = "zoom"
    api_base: str = "https://api.zoom.us/v2"
    access_token: str = ""
    send_path: str = "/phone/sms/messages"
    from_number: str = ""
    webhook_secret: Optional[str] = None
    client_id: str = ""
    client_secret: str = ""
    account_id: str = ""
    token_url: str = "https://zoom.us/oauth/token"
    webhook_url: str = ""
    use_account_credentials: bool = True
    send_enabled: bool = False
    call_enabled: bool = True
    text_enabled: bool = True
    verify_ssl: bool = True


class SendTextRequest(BaseModel):
    account_id: str
    recipient: str
    message: str
    dry_run: bool = False


class ZoomVerificationRequest(BaseModel):
    account_id: str
    recipient: Optional[str] = None


class ZoomWebhookEnvelope(BaseModel):
    event: Optional[str] = None
    event_ts: Optional[int] = None
    payload: Dict[str, Any] = {}


class InboundCommunicationRequest(BaseModel):
    provider: str = "zoom"
    from_number: str
    to_number: str = ""
    message: str
    direction: str = "inbound"
    webhook_secret: Optional[str] = None


class CotalityAccount(BaseModel):
    id: Optional[str] = None
    label: str = "Primary Cotality"
    api_base: str = ""
    api_key: str = ""
    property_path: str = "/property"
    valuation_path: str = "/valuation"
    comparables_path: str = "/comparables"
    suburb_path: str = "/suburb"
    rental_path: str = "/rental"
    listing_path: str = "/listings"
    market_path: str = "/market"
    enabled: bool = True


class CotalityPropertyIntelligenceRawPayload(BaseModel):
    workflow_name: str = COTALITY_PROPERTY_INTELLIGENCE_WORKFLOW_NAME
    sections: Dict[str, Any] = Field(default_factory=dict)
    discovered_tabs: List[Any] = Field(default_factory=list)
    section_order: List[str] = Field(default_factory=list)
    suburb_market_stats: Dict[str, Any] = Field(default_factory=dict)


class CotalityPropertyIntelligenceStructuredUpdates(BaseModel):
    property_type: Optional[str] = None
    bedrooms: Optional[float] = None
    bathrooms: Optional[float] = None
    car_spaces: Optional[float] = None
    land_size_sqm: Optional[float] = None
    building_size_sqm: Optional[float] = None
    year_built: Optional[str] = None
    ownership_duration_years: Optional[float] = None
    tenure_bucket: Optional[str] = None
    owner_occupancy_status: Optional[str] = None
    absentee_owner: Optional[bool] = None
    likely_landlord: Optional[bool] = None
    likely_owner_occupier: Optional[bool] = None
    owner_type: Optional[str] = None
    estimated_value_low: Optional[int] = None
    estimated_value_mid: Optional[int] = None
    estimated_value_high: Optional[int] = None
    valuation_confidence: Optional[str] = None
    valuation_date: Optional[str] = None
    rental_estimate_low: Optional[int] = None
    rental_estimate_high: Optional[int] = None
    yield_estimate: Optional[float] = None
    last_sale_price: Optional[int] = None
    last_sale_date: Optional[str] = None
    sale_history: List[Dict[str, Any]] = Field(default_factory=list)
    last_listing_status: Optional[str] = None
    last_listing_date: Optional[str] = None
    listing_status_history: List[Dict[str, Any]] = Field(default_factory=list)
    nearby_sales: List[Dict[str, Any]] = Field(default_factory=list)
    ownership_notes: Optional[str] = None
    source_evidence: List[str] = Field(default_factory=list)
    summary_points: List[str] = Field(default_factory=list)
    key_details: List[str] = Field(default_factory=list)
    seller_intent_signals: List[Dict[str, Any]] = Field(default_factory=list)
    refinance_signals: List[Dict[str, Any]] = Field(default_factory=list)


class CotalityPropertyIntelligenceResult(BaseModel):
    matched_address: Optional[str] = None
    raw_payload_json: CotalityPropertyIntelligenceRawPayload = Field(
        default_factory=CotalityPropertyIntelligenceRawPayload
    )
    proposed_updates_json: CotalityPropertyIntelligenceStructuredUpdates = Field(
        default_factory=CotalityPropertyIntelligenceStructuredUpdates
    )
    confidence: Optional[float] = None
    screenshot_path: Optional[str] = None
    final_status: str = "review_required"
    status: Optional[str] = None
    error_message: Optional[str] = None


class CotalityReportRequest(BaseModel):
    report_type: str = "property_intelligence"
    report_id: Optional[str] = None


class ReportPackRequest(BaseModel):
    include_existing_briefs: bool = True
    output_root: Optional[str] = None


class ProjectMemoryEntry(BaseModel):
    prompt: str
    intent: Optional[str] = None
    source: str = "user"


class TaskRequest(BaseModel):
    id: Optional[str] = None
    title: str
    due_at: str
    task_type: str = "follow_up"
    channel: Optional[str] = None
    status: str = "pending"
    notes: Optional[str] = None
    related_report_id: Optional[str] = None


class LeadOutcomeRequest(BaseModel):
    outcome: str
    note: Optional[str] = None
    task_id: Optional[str] = None
    callback_at: Optional[str] = None
    appointment_at: Optional[str] = None
    appointment_location: Optional[str] = None
    preferred_channel: Optional[str] = None
    owner_verified: Optional[bool] = None
    objection_reason: Optional[str] = None
    user_id: Optional[str] = None


class QueueRebuildRequest(BaseModel):
    horizon_days: int = 60
    force: bool = False


class TaskExecuteRequest(BaseModel):
    email_account_id: Optional[str] = None
    text_account_id: Optional[str] = None
    recipient: Optional[str] = None
    subject: Optional[str] = None
    message: Optional[str] = None
    dry_run: bool = False


class TaskRescheduleRequest(BaseModel):
    due_at: str
    note: Optional[str] = None


class TaskCompletionRequest(BaseModel):
    note: Optional[str] = None


class TaskApprovalRequest(BaseModel):
    due_at: Optional[str] = None
    subject: Optional[str] = None
    message: Optional[str] = None
    note: Optional[str] = None


class TaskSkipRequest(BaseModel):
    note: Optional[str] = None


class SoldEventRequest(BaseModel):
    address: str
    suburb: str = ""
    postcode: str = ""
    sale_date: Optional[str] = None
    sale_price: Optional[str] = None
    lat: float = 0
    lng: float = 0
    source_name: str = ""
    source_url: str = ""


class AppointmentRequest(BaseModel):
    id: Optional[str] = None
    title: str
    starts_at: str
    status: str = "scheduled"
    location: Optional[str] = None
    notes: Optional[str] = None


class ListingWorkflowSummary(BaseModel):
    lead_id: str
    authority_type: str = "exclusive"
    stage: str = "documents"
    inspection_complete: bool = False
    price_guidance_status: str = "draft_missing"
    authority_pack_status: str = "draft_missing"
    market_ready: bool = False
    lawyer_signoff_status: str = "pending"
    marketing_payment_status: str = "not_requested"
    workflow_notes: Optional[str] = None
    pack_sent_at: Optional[str] = None
    pack_signed_at: Optional[str] = None
    market_ready_at: Optional[str] = None


class ListingDocumentPayload(BaseModel):
    id: str
    lead_id: str
    kind: str
    label: str
    original_name: str
    stored_name: str
    relative_path: str
    download_url: str = ""
    mime_type: Optional[str] = None
    version: int = 1
    source: str = "upload"
    generated: bool = False
    uploaded_by: str = "operator"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class InspectionReportRequest(BaseModel):
    inspected_by: str
    inspection_at: str
    occupancy: str = "owner_occupied"
    condition_rating: str = "sound"
    summary: str
    notes: Optional[str] = None


class InspectionReportPayload(BaseModel):
    id: str
    lead_id: str
    inspected_by: str
    inspection_at: str
    occupancy: str
    condition_rating: str
    summary: str
    notes: Optional[str] = None
    approved: bool = True
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ComparableSale(BaseModel):
    address: str
    suburb: str = ""
    sale_price: Optional[int] = None
    sale_date: Optional[str] = None
    source: str = ""
    distance_km: Optional[float] = None


class PriceGuidanceDraftRequest(BaseModel):
    low: Optional[int] = None
    high: Optional[int] = None
    rationale: Optional[str] = None


class PriceGuidanceUpdateRequest(BaseModel):
    estimate_low: int
    estimate_high: int
    rationale: str = ""
    comparables: List[ComparableSale] = []


class PriceGuidanceRecord(BaseModel):
    id: str
    lead_id: str
    kind: str
    status: str
    version: int
    estimate_low: Optional[int] = None
    estimate_high: Optional[int] = None
    rationale: Optional[str] = None
    comparables: List[ComparableSale] = []
    quoted_channel: Optional[str] = None
    quoted_to: Optional[str] = None
    quoted_at: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ListingSendRequest(BaseModel):
    recipient_email: str
    recipient_name: Optional[str] = None
    message: Optional[str] = None


class MarketingStatusRequest(BaseModel):
    status: str
    note: Optional[str] = None


class LawyerSignoffRequest(BaseModel):
    status: str


class MarketReadyRequest(BaseModel):
    market_ready: bool = True


class OfferEventRequest(BaseModel):
    amount: int
    buyer_name: Optional[str] = None
    conditions: Optional[str] = None
    channel: str = "manual"
    status: str = "received"
    received_at: str
    notes: Optional[str] = None


class OfferEventPayload(BaseModel):
    id: str
    lead_id: str
    amount: int
    buyer_name: Optional[str] = None
    conditions: Optional[str] = None
    channel: str = "manual"
    status: str = "received"
    received_at: str
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class SigningSessionPayload(BaseModel):
    id: str
    lead_id: str
    token: str
    status: str
    authority_pack_document_id: Optional[str] = None
    sent_to: Optional[str] = None
    signer_name: Optional[str] = None
    signer_email: Optional[str] = None
    signer_ip: Optional[str] = None
    signer_user_agent: Optional[str] = None
    sent_at: Optional[str] = None
    viewed_at: Optional[str] = None
    signed_at: Optional[str] = None
    serviced_at: Optional[str] = None
    archive_path: Optional[str] = None
    signing_url: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ListingWorkflowResponse(BaseModel):
    workflow: ListingWorkflowSummary
    documents: List[ListingDocumentPayload] = []
    inspection_report: Optional[InspectionReportPayload] = None
    approved_price_guidance: Optional[PriceGuidanceRecord] = None
    draft_price_guidance: Optional[PriceGuidanceRecord] = None
    price_guidance_history: List[PriceGuidanceRecord] = []
    offer_events: List[OfferEventPayload] = []
    latest_signing_session: Optional[SigningSessionPayload] = None
    required_document_kinds: List[str] = []
    can_send_authority_pack: bool = False
    can_mark_market_ready: bool = False
