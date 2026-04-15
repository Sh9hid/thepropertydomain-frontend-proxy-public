import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime
from sqlmodel import SQLModel, Field, Column, JSON, String, Float, Integer, Boolean, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column as SAColumn

JSON_FIELD_TYPE = JSON().with_variant(JSONB(), "postgresql")

COTALITY_PROPERTY_INTELLIGENCE_WORKFLOW_NAME = "cotality_full_enrich"
COTALITY_PROPERTY_INTELLIGENCE_ALLOWED_FIELDS = [
    "property_type",
    "bedrooms",
    "bathrooms",
    "car_spaces",
    "land_size_sqm",
    "building_size_sqm",
    "year_built",
    "ownership_duration_years",
    "tenure_bucket",
    "owner_occupancy_status",
    "absentee_owner",
    "likely_landlord",
    "likely_owner_occupier",
    "owner_type",
    "estimated_value_low",
    "estimated_value_mid",
    "estimated_value_high",
    "valuation_confidence",
    "valuation_date",
    "rental_estimate_low",
    "rental_estimate_high",
    "yield_estimate",
    "last_sale_price",
    "last_sale_date",
    "sale_history",
    "last_listing_status",
    "last_listing_date",
    "listing_status_history",
    "nearby_sales",
    "ownership_notes",
    "source_evidence",
    "summary_points",
    "key_details",
    "seller_intent_signals",
    "refinance_signals",
]
COTALITY_PROPERTY_INTELLIGENCE_FIELD_TO_LEAD_COLUMN = {
    "property_type": "property_type",
    "bedrooms": "bedrooms",
    "bathrooms": "bathrooms",
    "car_spaces": "car_spaces",
    "land_size_sqm": "land_size_sqm",
    "building_size_sqm": "floor_size_sqm",
    "year_built": "year_built",
    "ownership_duration_years": "ownership_duration_years",
    "tenure_bucket": "tenure_bucket",
    "owner_occupancy_status": "owner_occupancy_status",
    "absentee_owner": "absentee_owner",
    "likely_landlord": "likely_landlord",
    "likely_owner_occupier": "likely_owner_occupier",
    "owner_type": "owner_type",
    "estimated_value_low": "estimated_value_low",
    "estimated_value_mid": "estimated_value_mid",
    "estimated_value_high": "estimated_value_high",
    "valuation_confidence": "valuation_confidence",
    "valuation_date": "valuation_date",
    "rental_estimate_low": "rental_estimate_low",
    "rental_estimate_high": "rental_estimate_high",
    "yield_estimate": "yield_estimate",
    "last_sale_price": "sale_price",
    "last_sale_date": "sale_date",
    "sale_history": "sale_history",
    "last_listing_status": "last_listing_status",
    "last_listing_date": "last_listing_date",
    "listing_status_history": "listing_status_history",
    "nearby_sales": "nearby_sales",
    "ownership_notes": "ownership_notes",
    "source_evidence": "source_evidence",
    "summary_points": "summary_points",
    "key_details": "key_details",
    "seller_intent_signals": "seller_intent_signals",
    "refinance_signals": "refinance_signals",
}
COTALITY_PROPERTY_INTELLIGENCE_RAW_SECTION_NAMES = (
    "property_overview",
    "valuation",
    "sale_history",
    "listing_history",
    "nearby_sales",
    "mortgage_signals",
)

# Shared Models for JSON structures
class PotentialContact(SQLModel):
    type: str
    value: str
    probability: int
    source: str

class NextAction(SQLModel):
    title: str
    owner: str = "Shahid"
    due_at: Optional[str] = None
    channel: Optional[str] = None
    message_template_id: Optional[str] = None

class Lead(SQLModel, table=True):
    __tablename__ = "leads"

    id: str = Field(primary_key=True)
    address: str = Field(unique=True, index=True)
    suburb: Optional[str] = Field(default=None, index=True)
    postcode: Optional[str] = None
    owner_name: Optional[str] = None
    trigger_type: Optional[str] = None
    record_type: str = Field(default="property_record")
    heat_score: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0, index=True))
    scenario: Optional[str] = None
    strategic_value: Optional[str] = None
    contact_status: Optional[str] = None
    ownership_tenure: Optional[str] = None
    equity_estimate: Optional[str] = None
    confidence_score: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    
    # JSONB Columns
    contacts: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    potential_contacts: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    contact_emails: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    contact_phones: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    
    lat: float = Field(default=0.0, sa_column=SAColumn(Float, nullable=True, default=0.0))
    lng: float = Field(default=0.0, sa_column=SAColumn(Float, nullable=True, default=0.0))
    est_value: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    date_found: Optional[str] = None
    signal_status: Optional[str] = Field(default=None, index=True)
    
    key_details: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    main_image: Optional[str] = None
    property_images: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    description_deep: Optional[str] = None
    features: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    conversion_strategy: Optional[str] = None
    summary_points: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    horizon: Optional[str] = None
    last_checked: Optional[str] = None
    exhaustive_summary: Optional[str] = None
    likely_scenario: Optional[str] = None
    strategic_why: Optional[str] = None
    owner_age: Optional[int] = None
    date_of_birth: Optional[str] = None       # "YYYY-MM-DD"
    id4me_enriched: Optional[bool] = Field(default=None)
    id4me_enriched_at: Optional[str] = None   # ISO timestamp
    id4me_last_seen: Optional[str] = None     # raw "last seen" text from id4me
    suburb_average_tenure: Optional[int] = None
    propensity_score: Optional[int] = None
    recent_sales_velocity: Optional[str] = None
    est_net_profit: Optional[int] = None
    local_dominance: Optional[str] = None
    zoning_type: Optional[str] = None
    status: str = Field(default="captured", sa_column=SAColumn(String, nullable=True, default="captured", index=True))
    conversion_score: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    compliance_score: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    readiness_score: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    call_today_score: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0, index=True))
    evidence_score: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    lifecycle_stage: Optional[str] = Field(default=None, index=True)
    who_to_call: Optional[str] = None
    why_now: Optional[str] = None
    what_to_say: Optional[str] = None
    recommended_next_step: Optional[str] = None
    
    risk_flags: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    source_tags: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    external_link: Optional[str] = None
    next_actions: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    source_evidence: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    linked_files: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    
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
    canonical_address: Optional[str] = Field(default=None, index=True)
    address_unit: Optional[str] = None
    street_number: Optional[str] = None
    street_name: Optional[str] = None
    street_type: Optional[str] = None
    state: Optional[str] = None
    country_code: Optional[str] = None
    mailing_address: Optional[str] = None
    mailing_address_matches_property: Optional[bool] = Field(default=True)
    absentee_owner: Optional[bool] = Field(default=False, index=True)
    likely_landlord: Optional[bool] = Field(default=False, index=True)
    likely_owner_occupier: Optional[bool] = Field(default=False)
    owner_occupancy_status: Optional[str] = None
    owner_first_name: Optional[str] = None
    owner_last_name: Optional[str] = None
    owner_persona: Optional[str] = None
    alternate_phones: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    alternate_emails: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    phone_status: Optional[str] = None
    phone_line_type: Optional[str] = None
    email_status: Optional[str] = None
    do_not_call: Optional[bool] = Field(default=False, index=True)
    consent_status: Optional[str] = None
    contactability_tier: Optional[str] = Field(default=None, index=True)
    contactability_reasons: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    property_type: Optional[str] = None
    parcel_lot: Optional[str] = None
    parcel_plan: Optional[str] = None
    title_reference: Optional[str] = None
    ownership_duration_years: Optional[float] = None
    tenure_bucket: Optional[str] = Field(default=None, index=True)
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
    sale_history: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    listing_status_history: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    nearby_sales: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    deterministic_tags: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    seller_intent_signals: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    refinance_signals: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    ownership_notes: Optional[str] = None
    source_provenance: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    enrichment_status: Optional[str] = Field(default=None, index=True)
    enrichment_last_synced_at: Optional[str] = None
    research_status: Optional[str] = None
    
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None
    last_contacted_at: Optional[str] = None
    follow_up_due_at: Optional[str] = Field(default=None, index=True)
    preferred_contact_method: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    followup_frequency: str = Field(default="none", sa_column=SAColumn(String, nullable=True, default="none", index=True))
    market_updates_opt_in: bool = Field(default=False, sa_column=SAColumn(Boolean, nullable=True, default=False))
    next_followup_at: Optional[str] = Field(default=None, index=True)
    followup_status: str = Field(default="active", sa_column=SAColumn(String, nullable=True, default="active", index=True))
    followup_notes: Optional[str] = None
    last_inbound_at: Optional[str] = None
    last_outbound_at: Optional[str] = None
    last_called_date: Optional[str] = Field(default=None, index=True)
    
    queue_bucket: str = Field(default="", sa_column=SAColumn(String, nullable=True, default="", index=True))
    lead_archetype: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    contactability_status: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    owner_verified: bool = Field(default=False, sa_column=SAColumn(Boolean, nullable=True, default=False))
    contact_role: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    cadence_name: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    cadence_step: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    next_action_at: Optional[str] = None
    next_action_type: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    next_action_channel: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    next_action_title: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    next_action_reason: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    next_message_template: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    last_outcome: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    last_outcome_at: Optional[str] = None
    last_activity_type: Optional[str] = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    objection_reason: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    preferred_channel: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    strike_zone: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    touches_14d: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    touches_30d: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    do_not_contact_until: Optional[str] = None
    stage_note: Optional[str] = None
    notes: Optional[str] = None
    
    stage_note_history: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    activity_log: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))

    # Behavioral intelligence (Phase 4)
    best_open_hour: Optional[int] = None
    best_call_hour: Optional[int] = None
    email_engagement_score: float = Field(default=0.0, sa_column=SAColumn(Float, nullable=True, default=0.0))
    channel_preference: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    last_email_opened_at: Optional[str] = None
    email_open_count: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    email_click_count: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    ai_context_summary: Optional[str] = None

    # Intelligence Hub extensions
    h3index: Optional[str] = None
    cadid: Optional[str] = None
    route_queue: str = Field(default="", sa_column=SAColumn(String, nullable=True, default=""))
    last_settlement_date: Optional[str] = None

    # Domain API enrichment
    domain_listing_id: Optional[str] = Field(default=None, index=True)
    domain_enriched_date: Optional[str] = None
    days_on_market: Optional[int] = Field(default=0)
    listing_headline: Optional[str] = None
    price_drop_count: Optional[int] = Field(default=0)
    relisted: Optional[bool] = Field(default=False)
    list_date: Optional[str] = None
    estimated_completion: Optional[str] = None

    # REA listing management
    rea_listing_id: Optional[str] = Field(default=None, index=True)
    rea_upload_id: Optional[str] = None
    rea_upload_status: Optional[str] = None
    rea_last_upload_response: Optional[str] = None
    rea_title_variant: Optional[int] = None
    rea_desc_variant: Optional[int] = None
    rea_last_edit_at: Optional[str] = None
    rea_views: Optional[int] = Field(default=0)
    rea_enquiries: Optional[int] = Field(default=0)
    lot_number: Optional[str] = None
    lot_type: Optional[str] = None
    frontage: Optional[str] = None
    project_name: Optional[str] = None
    listing_description: Optional[str] = None


class EnrichmentJob(SQLModel, table=True):
    __tablename__ = "enrichment_jobs"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    provider: str = Field(default="cotality", index=True)
    status: str = Field(default="queued", index=True)
    requested_fields_json: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    matched_address: Optional[str] = None
    machine_id: Optional[str] = Field(default=None, index=True)
    attempt_count: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    error_message: Optional[str] = None
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = Field(default=None, index=True)
    completed_at: Optional[str] = Field(default=None, index=True)


class EnrichmentResult(SQLModel, table=True):
    __tablename__ = "enrichment_results"

    id: str = Field(primary_key=True)
    enrichment_job_id: str = Field(index=True)
    source: str = Field(default="cotality", index=True)
    raw_payload_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    proposed_updates_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    screenshot_path: Optional[str] = None
    confidence: Optional[float] = None
    created_at: Optional[str] = Field(default=None, index=True)


class SuburbMarketStat(SQLModel, table=True):
    __tablename__ = "suburb_market_stats"

    id: str = Field(primary_key=True)
    suburb: str = Field(index=True)
    state: Optional[str] = Field(default=None, index=True)
    postcode: Optional[str] = Field(default=None, index=True)
    segment: str = Field(default="houses", index=True)
    source: str = Field(default="cotality", index=True)
    median_value: Optional[int] = None
    properties_sold: Optional[int] = None
    median_asking_rent: Optional[int] = None
    median_value_change_12m_pct: Optional[float] = None
    days_on_market: Optional[int] = None
    average_tenure_years: Optional[float] = None
    median_value_change_5y_pct: Optional[float] = None
    new_listings_12m: Optional[int] = None
    rental_rate_observations: Optional[int] = None
    stats_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    refreshed_at: Optional[str] = Field(default=None, index=True)
    refresh_after: Optional[str] = Field(default=None, index=True)
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = Field(default=None, index=True)

class CallLog(SQLModel, table=True):
    __tablename__ = "call_log"
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: str = Field(default="", index=True)
    lead_address: str = Field(default="")
    user_id: str = Field(default="Shahid", index=True)
    outcome: str
    connected: bool = Field(default=False)
    timestamp: str = Field(default="", index=True)
    call_duration_seconds: int = Field(default=0)
    duration_seconds: int = Field(default=0)
    note: str = Field(default="")
    operator: str = Field(default="Shahid")
    logged_at: str = Field(default="")
    logged_date: str = Field(default="", index=True)
    next_action_due: Optional[str] = Field(default=None, index=True)
    provider: str = Field(default="manual", index=True)
    provider_call_id: Optional[str] = Field(default=None, index=True)
    direction: str = Field(default="")
    from_number: str = Field(default="")
    to_number: str = Field(default="")
    raw_payload: str = Field(default="{}")
    recording_url: Optional[str] = Field(default=None)
    recording_status: Optional[str] = Field(default=None, index=True)
    recording_duration_seconds: Optional[int] = Field(default=None)
    transcript: Optional[str] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    intent_signal: float = Field(default=0.0)
    booking_attempted: bool = Field(default=False)
    next_step_detected: bool = Field(default=False)
    objection_tags: str = Field(default="[]")


class SpeechCall(SQLModel, table=True):
    __tablename__ = "calls"

    id: str = Field(primary_key=True)
    external_call_id: str = Field(default="", index=True)
    source: str = Field(default="upload", index=True)
    lead_id: str = Field(default="", index=True)
    rep_id: str = Field(default="", index=True)
    call_type: str = Field(default="recorded_call")
    direction: str = Field(default="")
    outcome: str = Field(default="unknown", index=True)
    started_at: Optional[str] = Field(default=None, index=True)
    ended_at: Optional[str] = Field(default=None)
    duration_seconds: int = Field(default=0)
    recording_id: str = Field(default="")
    audio_uri: str = Field(default="")
    audio_storage_status: str = Field(default="pending")
    analysis_status: str = Field(default="pending", index=True)
    transcript_status: str = Field(default="pending")
    diarization_status: str = Field(default="pending")
    metadata_json: str = Field(default="{}")
    created_at: Optional[str] = Field(default=None)
    updated_at: Optional[str] = Field(default=None)


class Speaker(SQLModel, table=True):
    __tablename__ = "speakers"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    diarization_label: str = Field(default="")
    role: str = Field(default="unknown", index=True)
    display_name: str = Field(default="")
    linked_rep_id: str = Field(default="")
    linked_contact_id: str = Field(default="")
    confidence: float = Field(default=0.0)
    created_at: Optional[str] = Field(default=None)


class CallSegment(SQLModel, table=True):
    __tablename__ = "call_segments"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    speaker_id: str = Field(default="", index=True)
    turn_index: int = Field(default=0)
    start_ms: int = Field(default=0)
    end_ms: int = Field(default=0)
    text: str = Field(default="")
    overlap_flag: int = Field(default=0)
    segment_type: str = Field(default="turn")
    confidence: float = Field(default=0.0)
    created_at: Optional[str] = Field(default=None)


class Transcript(SQLModel, table=True):
    __tablename__ = "transcripts"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    provider: str = Field(default="stub")
    version_type: str = Field(default="canonical")
    language: str = Field(default="en-AU")
    full_text: str = Field(default="")
    confidence: float = Field(default=0.0)
    status: str = Field(default="pending")
    created_at: Optional[str] = Field(default=None)
    updated_at: Optional[str] = Field(default=None)


class WordTimestamp(SQLModel, table=True):
    __tablename__ = "word_timestamps"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    transcript_id: str = Field(index=True)
    segment_id: str = Field(default="", index=True)
    speaker_id: str = Field(default="", index=True)
    word: str = Field(default="")
    start_ms: int = Field(default=0)
    end_ms: int = Field(default=0)
    confidence: float = Field(default=0.0)
    phoneme_seq: str = Field(default="")


class PronunciationEvent(SQLModel, table=True):
    __tablename__ = "pronunciation_events"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    segment_id: str = Field(default="", index=True)
    word_timestamp_id: str = Field(default="", index=True)
    canonical: str = Field(default="")
    observed: str = Field(default="")
    deviation_type: str = Field(default="")
    severity: float = Field(default=0.0)
    notes: str = Field(default="")
    created_at: Optional[str] = Field(default=None)


class FluencyEvent(SQLModel, table=True):
    __tablename__ = "fluency_events"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    segment_id: str = Field(default="", index=True)
    event_type: str = Field(default="", index=True)
    start_ms: int = Field(default=0)
    duration_ms: int = Field(default=0)
    severity: float = Field(default=0.0)
    evidence: str = Field(default="")
    created_at: Optional[str] = Field(default=None)


class FillerEvent(SQLModel, table=True):
    __tablename__ = "filler_events"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    segment_id: str = Field(default="", index=True)
    token: str = Field(default="", index=True)
    family: str = Field(default="")
    count: int = Field(default=0)
    start_ms: int = Field(default=0)
    duration_ms: int = Field(default=0)
    created_at: Optional[str] = Field(default=None)


class TonalEvent(SQLModel, table=True):
    __tablename__ = "tonal_events"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    segment_id: str = Field(default="", index=True)
    contour_type: str = Field(default="")
    pitch_start_hz: float = Field(default=0.0)
    pitch_end_hz: float = Field(default=0.0)
    semitone_delta: float = Field(default=0.0)
    intensity_db: float = Field(default=0.0)
    tone_label: str = Field(default="", index=True)
    confidence: float = Field(default=0.0)
    created_at: Optional[str] = Field(default=None)


class Objection(SQLModel, table=True):
    __tablename__ = "objections"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    segment_id: str = Field(default="", index=True)
    objection_type: str = Field(default="", index=True)
    normalized_text: str = Field(default="")
    detected_at_ms: int = Field(default=0)
    response_quality_score: float = Field(default=0.0)
    resolved_flag: int = Field(default=0)
    created_at: Optional[str] = Field(default=None)


class CoachingReport(SQLModel, table=True):
    __tablename__ = "coaching_reports"

    id: str = Field(primary_key=True)
    call_id: str = Field(index=True)
    rep_id: str = Field(default="", index=True)
    report_version: str = Field(default="v0")
    brutal_summary: str = Field(default="")
    detailed_breakdown_json: str = Field(default="{}")
    rewrite_json: str = Field(default="{}")
    drills_json: str = Field(default="[]")
    live_task: str = Field(default="")
    generated_at: Optional[str] = Field(default=None, index=True)
    created_at: Optional[str] = Field(default=None)
    updated_at: Optional[str] = Field(default=None)


class ScoreSnapshot(SQLModel, table=True):
    __tablename__ = "score_snapshots"

    id: str = Field(primary_key=True)
    entity_type: str = Field(default="call", index=True)
    entity_id: str = Field(default="", index=True)
    call_id: str = Field(default="", index=True)
    rep_id: str = Field(default="", index=True)
    scenario_type: str = Field(default="")
    scoring_version: str = Field(default="v0")
    composite_score: float = Field(default=0.0)
    confidence: float = Field(default=0.0)
    computed_at: Optional[str] = Field(default=None, index=True)
    created_at: Optional[str] = Field(default=None)


class ScoreComponent(SQLModel, table=True):
    __tablename__ = "score_components"

    id: str = Field(primary_key=True)
    snapshot_id: str = Field(index=True)
    call_id: str = Field(default="", index=True)
    score_name: str = Field(default="", index=True)
    score_value: float = Field(default=0.0)
    raw_value: float = Field(default=0.0)
    normalized_value: float = Field(default=0.0)
    weight: float = Field(default=0.0)
    stable_flag: int = Field(default=0)
    evidence_json: str = Field(default="{}")
    created_at: Optional[str] = Field(default=None)


class RealtimeEvent(SQLModel, table=True):
    __tablename__ = "realtime_events"

    id: str = Field(primary_key=True)
    session_id: str = Field(default="", index=True)
    call_id: str = Field(default="", index=True)
    rep_id: str = Field(default="", index=True)
    event_type: str = Field(default="", index=True)
    severity: str = Field(default="info")
    payload_json: str = Field(default="{}")
    acknowledged_at: Optional[str] = Field(default=None)
    created_at: Optional[str] = Field(default=None, index=True)


class TickerEvent(SQLModel, table=True):
    """Dedicated table for real-time signal events shown in the live ticker bar."""
    __tablename__ = "ticker_events"
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    event_type: str = Field(index=True)      # WITHDRAWAL | PROBATE | DA_FILED | OBITUARY | INSOLVENCY | FIRE | NEWS | MARKET_SIGNAL
    source: str = Field(default="")          # domain_withdrawn | probate_gazette | da_portal | legacy_obit | asic | rfs | newsapi
    address: str = Field(default="")
    suburb: str = Field(default="", index=True)
    postcode: str = Field(default="")
    owner_name: str = Field(default="")
    heat_score: int = Field(default=0)
    lead_id: str = Field(default="", index=True)
    icon: str = Field(default="●")
    color: str = Field(default="rgba(255,255,255,0.5)")
    headline: str = Field(default="")        # short human-readable summary for ticker display
    extra: str = Field(default="{}")         # JSON blob for source-specific payload
    detected_at: str = Field(default="", index=True)  # ISO UTC


class BankDataHolder(SQLModel, table=True):
    __tablename__ = "bank_data_holders"

    id: str = Field(primary_key=True)
    name: str = Field(index=True)
    brand: str = Field(default="", index=True)
    base_url: str
    api_family: str = Field(default="cdr_banking_public", index=True)
    product_path: str = Field(default="/banking/products")
    category: str = Field(default="lender", index=True)
    supports_public_products: bool = Field(default=True)
    supports_consumer_data: bool = Field(default=False)
    active: bool = Field(default=True, index=True)
    notes: Optional[str] = None
    metadata_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = Field(default=None, index=True)


class MortgageOpportunity(SQLModel, table=True):
    __tablename__ = "mortgage_opportunities"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    opportunity_type: str = Field(index=True)
    status: str = Field(default="active", index=True)
    priority_score: float = Field(default=0.0, index=True)
    headline: str = Field(default="")
    reason_to_call: str = Field(default="")
    why_now: str = Field(default="")
    next_best_action: str = Field(default="")
    best_call_window: Optional[str] = None
    estimated_loan_amount: Optional[int] = None
    current_rate_estimate: Optional[float] = None
    market_rate_estimate: Optional[float] = None
    estimated_weekly_saving: Optional[int] = None
    estimated_monthly_saving: Optional[int] = None
    estimated_annual_saving: Optional[int] = None
    estimated_lifetime_saving: Optional[int] = None
    evidence_json: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    assumptions_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    source_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = Field(default=None, index=True)
    expires_at: Optional[str] = Field(default=None, index=True)


class LenderProductSnapshot(SQLModel, table=True):
    __tablename__ = "lender_product_snapshots"

    id: str = Field(primary_key=True)
    lender_id: str = Field(index=True)
    source_url: str
    status: str = Field(default="success", index=True)
    http_status: Optional[int] = None
    response_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    error_message: Optional[str] = None
    fetched_at: Optional[str] = Field(default=None, index=True)


class LenderProduct(SQLModel, table=True):
    __tablename__ = "lender_products"

    id: str = Field(primary_key=True)
    lender_id: str = Field(index=True)
    external_product_id: str = Field(index=True)
    name: str = Field(index=True)
    brand: str = Field(default="", index=True)
    product_kind: str = Field(default="mortgage", index=True)
    occupancy_target: str = Field(default="unknown", index=True)
    rate_type: str = Field(default="unknown", index=True)
    advertised_rate: Optional[float] = Field(default=None, index=True)
    comparison_rate: Optional[float] = None
    fixed_term_months: Optional[int] = None
    has_offset: bool = Field(default=False, index=True)
    has_redraw: bool = Field(default=False, index=True)
    interest_only_available: bool = Field(default=False, index=True)
    package_fee_annual: Optional[float] = None
    tags_json: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    constraints_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    raw_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    source_url: Optional[str] = None
    fetched_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = Field(default=None, index=True)


class LenderProductDelta(SQLModel, table=True):
    __tablename__ = "lender_product_deltas"

    id: str = Field(primary_key=True)
    lender_id: str = Field(index=True)
    external_product_id: str = Field(index=True)
    change_type: str = Field(index=True)
    headline: str = Field(default="")
    old_rate: Optional[float] = None
    new_rate: Optional[float] = None
    old_comparison_rate: Optional[float] = None
    new_comparison_rate: Optional[float] = None
    payload_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    detected_at: Optional[str] = Field(default=None, index=True)


class LeadMortgageProfile(SQLModel, table=True):
    __tablename__ = "lead_mortgage_profiles"

    lead_id: str = Field(primary_key=True)
    current_lender: Optional[str] = Field(default=None, index=True)
    current_rate: Optional[float] = None
    current_rate_source: Optional[str] = None
    repayment_type: Optional[str] = Field(default=None, index=True)
    loan_balance_estimate: Optional[int] = None
    loan_balance_band: Optional[str] = None
    fixed_or_variable: Optional[str] = Field(default=None, index=True)
    fixed_expiry: Optional[str] = None
    offset_account: Optional[bool] = Field(default=None)
    owner_occupancy_confirmed: Optional[str] = None
    refinance_interest: Optional[str] = None
    serviceability_notes: Optional[str] = None
    extracted_facts_json: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    provenance_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    updated_by: Optional[str] = None
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = Field(default=None, index=True)


class MortgageOpportunityFeedback(SQLModel, table=True):
    __tablename__ = "mortgage_opportunity_feedback"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    opportunity_type: Optional[str] = Field(default=None, index=True)
    feedback_type: str = Field(index=True)
    note: Optional[str] = None
    created_by: Optional[str] = None
    active: bool = Field(default=True, index=True)
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = Field(default=None, index=True)


class Task(SQLModel, table=True):
    __tablename__ = "tasks"
    id: str = Field(primary_key=True)
    lead_id: Optional[str] = Field(default=None, index=True)
    title: str
    task_type: str = Field(default="follow_up")
    action_type: str = Field(default="")
    channel: Optional[str] = None
    due_at: Optional[str] = Field(default=None, index=True)
    status: str = Field(default="pending", index=True)
    notes: Optional[str] = None
    related_report_id: Optional[str] = None
    approval_status: str = Field(default="not_required")
    message_subject: str = Field(default="")
    message_preview: str = Field(default="")
    rewrite_reason: str = Field(default="")
    superseded_by: str = Field(default="")
    cadence_name: str = Field(default="")
    cadence_step: int = Field(default=0)
    auto_generated: int = Field(default=0)
    priority_bucket: str = Field(default="")
    payload_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    attempt_count: int = Field(default=0, sa_column=SAColumn(Integer, nullable=True, default=0))
    last_error: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class LeadInteraction(SQLModel, table=True):
    __tablename__ = "lead_interactions"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    event_type: str = Field(index=True)
    direction: Optional[str] = None
    summary: str = Field(default="")
    payload_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    actor: Optional[str] = None
    source: Optional[str] = None
    created_at: Optional[str] = Field(default=None, index=True)

class Appointment(SQLModel, table=True):
    __tablename__ = "appointments"
    id: str = Field(primary_key=True)
    lead_id: Optional[str] = Field(default=None, index=True)
    title: str
    starts_at: Optional[str] = Field(default=None, index=True)
    status: str = Field(default="scheduled")
    location: Optional[str] = None
    notes: Optional[str] = None
    cadence_name: str = Field(default="")
    auto_generated: int = Field(default=0)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class SoldEvent(SQLModel, table=True):
    __tablename__ = "sold_events"
    id: str = Field(primary_key=True)
    address: str = Field(index=True)
    suburb: Optional[str] = Field(default=None, index=True)
    postcode: Optional[str] = None
    sale_date: Optional[str] = None
    sale_price: Optional[str] = None
    lat: float = Field(default=0.0)
    lng: float = Field(default=0.0)
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    match_reason: str = Field(default="")
    matched_lead_ids: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class CommunicationAccount(SQLModel, table=True):
    __tablename__ = "communication_accounts"
    id: str = Field(primary_key=True)
    label: str
    provider: str = Field(index=True)
    api_base: Optional[str] = None
    access_token: Optional[str] = None
    send_path: Optional[str] = None
    from_number: Optional[str] = None
    webhook_secret: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    account_id: Optional[str] = None
    token_url: Optional[str] = None
    webhook_url: Optional[str] = None
    use_account_credentials: int = Field(default=1)
    send_enabled: int = Field(default=0)
    call_enabled: int = Field(default=1)
    text_enabled: int = Field(default=1)
    verify_ssl: int = Field(default=1)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class Agent(SQLModel, table=True):
    __tablename__ = "agents"
    id: str = Field(primary_key=True)
    name: str
    description: Optional[str] = None
    status: str
    last_run: Optional[str] = None
    health: int
    activity: Optional[str] = None

class LeadNote(SQLModel, table=True):
    __tablename__ = "notes"
    id: Optional[int] = Field(default=None, primary_key=True)
    lead_id: str = Field(index=True)
    note_type: str
    content: str
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class ListingWorkflow(SQLModel, table=True):
    __tablename__ = "listing_workflows"

    lead_id: str = Field(primary_key=True)
    authority_type: str = Field(default="exclusive")
    stage: str = Field(default="documents")
    inspection_required: int = Field(default=1)
    inspection_complete: int = Field(default=0)
    price_guidance_required: int = Field(default=1)
    price_guidance_status: str = Field(default="draft_missing")
    authority_pack_status: str = Field(default="draft_missing")
    market_ready: int = Field(default=0)
    lawyer_signoff_status: str = Field(default="pending")
    marketing_payment_status: str = Field(default="not_requested")
    workflow_notes: Optional[str] = None
    inspection_report_id: Optional[str] = None
    approved_price_guidance_id: Optional[str] = None
    latest_signing_session_id: Optional[str] = None
    pack_document_id: Optional[str] = None
    pack_sent_at: Optional[str] = None
    pack_signed_at: Optional[str] = None
    market_ready_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ListingDocument(SQLModel, table=True):
    __tablename__ = "listing_documents"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    kind: str = Field(index=True)
    label: str
    original_name: str
    stored_name: str
    relative_path: str
    mime_type: Optional[str] = None
    version: int = Field(default=1)
    source: str = Field(default="upload")
    generated: int = Field(default=0)
    uploaded_by: str = Field(default="operator")
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class InspectionReport(SQLModel, table=True):
    __tablename__ = "inspection_reports"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    inspected_by: str
    inspection_at: str = Field(index=True)
    occupancy: str = Field(default="owner_occupied")
    condition_rating: str = Field(default="sound")
    summary: str
    notes: Optional[str] = None
    approved: int = Field(default=1)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PriceGuidanceLog(SQLModel, table=True):
    __tablename__ = "price_guidance_logs"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    kind: str = Field(index=True)
    status: str = Field(default="draft")
    version: int = Field(default=1)
    estimate_low: Optional[int] = None
    estimate_high: Optional[int] = None
    rationale: Optional[str] = None
    comparables: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    quoted_channel: Optional[str] = None
    quoted_to: Optional[str] = None
    quoted_at: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class OfferEvent(SQLModel, table=True):
    __tablename__ = "offer_events"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    amount: int
    buyer_name: Optional[str] = None
    conditions: Optional[str] = None
    channel: str = Field(default="manual")
    status: str = Field(default="received")
    received_at: str = Field(index=True)
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class SigningSession(SQLModel, table=True):
    __tablename__ = "signing_sessions"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    token: str = Field(index=True)
    status: str = Field(default="drafted")
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
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class LeadBrief(SQLModel, table=True):
    __tablename__ = "lead_briefs"

    id: int = Field(default=None, primary_key=True)
    lead_id: str = Field(index=True)
    input_hash: str = Field(default="")
    operator_brief: Optional[str] = None
    call_opening: Optional[str] = None
    objection_handling: Optional[str] = None
    urgency_reason: Optional[str] = None
    evidence_bullets: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    risk_flags: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    missing_data: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    assumptions: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    confidence: str = Field(default="medium")
    sms_draft: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    next_action: Optional[str] = None
    next_action_channel: Optional[str] = None
    model_used: str = Field(default="")
    generated_at: Optional[str] = None
    expires_at: Optional[str] = None


# ─── Phase 2: REA Listing Metrics + Developers ──────────────────────


class ReaListingMetric(SQLModel, table=True):
    __tablename__ = "rea_listing_metrics"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    listing_id: str = Field(index=True)
    lead_id: Optional[str] = Field(default=None, index=True)
    snapshot_date: str = Field(index=True)
    views_24h: int = Field(default=0)
    views_7d: int = Field(default=0)
    views_30d: int = Field(default=0)
    inquiries_24h: int = Field(default=0)
    inquiries_7d: int = Field(default=0)
    search_position: Optional[int] = None
    search_page: Optional[int] = None
    days_listed: int = Field(default=0)
    days_since_edit: int = Field(default=0)
    freshness_score: int = Field(default=50)
    rotation_recommended: bool = Field(default=False)
    rotation_strategy: Optional[str] = None
    created_at: Optional[str] = None


class Developer(SQLModel, table=True):
    __tablename__ = "developers"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    company_name: str = Field(index=True)
    contact_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    specialization: str = Field(default="residential")
    preferred_suburbs: List[str] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    budget_range: Optional[str] = None
    land_size_range: Optional[str] = None
    active_projects: int = Field(default=0)
    status: str = Field(default="active", index=True)
    notes: Optional[str] = None
    last_contacted_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ─── Phase 3: Outreach Templates ────────────────────────────────────


class OutreachTemplate(SQLModel, table=True):
    __tablename__ = "outreach_templates"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    name: str = Field(index=True)
    channel: str = Field(index=True)           # sms | email
    stage: str = Field(index=True)             # hot | warm | cold | doorknock | nurture
    trigger_match: Optional[str] = None        # regex or keyword match for auto-selection
    subject: Optional[str] = None              # email subject
    body: str = Field(default="")              # template body with {placeholders}
    style: str = Field(default="standard")     # standard | handwritten | newsletter | data_led | story_led
    variant: str = Field(default="A")
    send_count: int = Field(default=0)
    open_count: int = Field(default=0)
    reply_count: int = Field(default=0)
    booking_count: int = Field(default=0)
    ai_generated: bool = Field(default=False)
    active: bool = Field(default=True, index=True)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ─── Phase 4: Email Tracking ────────────────────────────────────────


class EmailEvent(SQLModel, table=True):
    __tablename__ = "email_events"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    outreach_log_id: Optional[str] = Field(default=None, index=True)
    lead_id: Optional[str] = Field(default=None, index=True)
    tracking_id: str = Field(index=True)
    event_type: str = Field(index=True)        # open | click | bounce | complaint
    link_url: Optional[str] = None
    opened_at: Optional[str] = Field(default=None, index=True)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    created_at: Optional[str] = None


# ─── Phase 5: Lead Memory + Chat ────────────────────────────────────


class LeadMemory(SQLModel, table=True):
    __tablename__ = "lead_memories"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: str = Field(index=True)
    memory_type: str = Field(index=True)       # call_insight | objection | preference | context | relationship | timing | behavioral
    content: str
    source: str = Field(default="operator")    # operator | ai_extraction | system
    importance: float = Field(default=0.5)
    source_event_id: Optional[str] = None
    created_at: Optional[str] = None
    expires_at: Optional[str] = None


class ChatSession(SQLModel, table=True):
    __tablename__ = "chat_sessions"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    user_id: str = Field(default="operator", index=True)
    title: Optional[str] = None
    messages_json: List[Dict[str, Any]] = Field(default=[], sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ─── Phase 6: Learning Loop + KPIs ──────────────────────────────────


class KpiTarget(SQLModel, table=True):
    __tablename__ = "kpi_targets"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    metric_name: str = Field(index=True)
    target_value: float = Field(default=0)
    current_value: float = Field(default=0)
    period: str = Field(default="daily", index=True)  # daily | weekly | monthly
    computed_at: Optional[str] = None
    source: str = Field(default="system")       # system | operator
    status: str = Field(default="on_track")     # on_track | at_risk | behind


class ScoringFeedback(SQLModel, table=True):
    __tablename__ = "scoring_feedback"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    trigger_type: str = Field(index=True)
    calls_analyzed: int = Field(default=0)
    connected_rate: float = Field(default=0.0)
    booking_rate: float = Field(default=0.0)
    current_weight: int = Field(default=0)
    recommended_weight: int = Field(default=0)
    applied: bool = Field(default=False)
    computed_at: Optional[str] = None


class AgentHealth(SQLModel, table=True):
    __tablename__ = "agent_health"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    agent_name: str = Field(index=True, unique=True)
    last_heartbeat: Optional[str] = None
    status: str = Field(default="healthy", index=True)  # healthy | warning | dead
    last_error: Optional[str] = None
    items_processed_24h: int = Field(default=0)
    items_failed_24h: int = Field(default=0)


class SystemConfig(SQLModel, table=True):
    __tablename__ = "system_config"

    key: str = Field(primary_key=True)
    value_json: Dict[str, Any] = Field(default={}, sa_column=SAColumn(JSON_FIELD_TYPE, nullable=True))
    updated_at: Optional[str] = None


# ─── Phase 7: Users + Roles ─────────────────────────────────────────


class User(SQLModel, table=True):
    __tablename__ = "users"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    identifier: str = Field(index=True, unique=True)  # email or username
    display_name: Optional[str] = None
    role: str = Field(default="admin", index=True)      # admin | lab | sales
    avatar_url: Optional[str] = None
    created_at: Optional[str] = None
    last_login_at: Optional[str] = None


class DoorKnockVisit(SQLModel, table=True):
    __tablename__ = "door_knock_visits"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: str = Field(index=True)
    user_id: str = Field(default="Shahid", index=True)
    visited_at: str = Field(index=True)
    lat: Optional[float] = None
    lng: Optional[float] = None
    notes: Optional[str] = None
    photo_url: Optional[str] = None
    outcome: str = Field(default="no_answer")  # spoke_to_owner | no_answer | left_card | refused
    follow_up_scheduled: bool = Field(default=False)
    created_at: Optional[str] = None
