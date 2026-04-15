from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SyncRunStartRequest(BaseModel):
    source_root: str
    requested_by: str = "operator"
    worker_host: Optional[str] = None
    total_files: int = 0
    total_bytes: int = 0


class SyncRunCompleteRequest(BaseModel):
    status: str = "completed"
    error_summary: Optional[str] = None


class SyncRunSummary(BaseModel):
    id: str
    source_root: str
    requested_by: str = "operator"
    worker_host: Optional[str] = None
    status: str
    total_files: int = 0
    total_bytes: int = 0
    scanned_files: int = 0
    uploaded_files: int = 0
    skipped_files: int = 0
    failed_files: int = 0
    uploaded_bytes: int = 0
    last_heartbeat_at: Optional[str] = None
    error_summary: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: Optional[str] = None
    stats: Dict[str, Any] = Field(default_factory=dict)


class SyncStatusResponse(BaseModel):
    runs: List[SyncRunSummary] = Field(default_factory=list)


class MirroredAssetManifest(BaseModel):
    relative_path: str
    original_name: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: int = 0
    sha256: str
    modified_at: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)


class AssetBatchUpsertRequest(BaseModel):
    sync_run_id: str
    assets: List[MirroredAssetManifest] = Field(default_factory=list)


class AssetBatchUpsertItem(BaseModel):
    asset_id: str
    relative_path: str
    upload_required: bool
    upload_reason: str
    upload_status: str
    sensitivity: str
    is_sensitive: bool


class AssetBatchUpsertResponse(BaseModel):
    sync_run_id: str
    accepted: int = 0
    upload_required_count: int = 0
    assets: List[AssetBatchUpsertItem] = Field(default_factory=list)


class AssetDerivativePayload(BaseModel):
    id: str
    asset_id: str
    derivative_type: str
    storage_path: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: int = 0
    status: str = "ready"
    content_text: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class EvidenceLinkPayload(BaseModel):
    id: str
    lead_id: str
    asset_id: str
    link_type: str = "archive_file"
    confidence_score: int = 70
    rationale: Optional[str] = None
    include_on_lead: bool = True
    linked_by: str = "operator"
    address: Optional[str] = None
    suburb: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class EvidenceAssetSummary(BaseModel):
    id: str
    sync_run_id: Optional[str] = None
    relative_path: str
    directory_path: str = ""
    original_name: str
    storage_path: Optional[str] = None
    extension: str = ""
    mime_type: Optional[str] = None
    size_bytes: int = 0
    sha256: Optional[str] = None
    modified_at: Optional[str] = None
    category: str = "other"
    preview_kind: str = "none"
    sensitivity: str = "standard"
    is_sensitive: bool = False
    upload_status: str = "pending"
    text_extract_status: str = "not_started"
    text_extract_excerpt: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)
    last_seen_at: Optional[str] = None
    uploaded_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    download_url: str = ""


class EvidenceAssetDetail(EvidenceAssetSummary):
    derivatives: List[AssetDerivativePayload] = Field(default_factory=list)
    linked_leads: List[EvidenceLinkPayload] = Field(default_factory=list)


class LeadEvidenceLinkRequest(BaseModel):
    lead_id: str
    link_type: str = "archive_file"
    confidence_score: int = 70
    rationale: Optional[str] = None
    include_on_lead: bool = True
    linked_by: str = "operator"
