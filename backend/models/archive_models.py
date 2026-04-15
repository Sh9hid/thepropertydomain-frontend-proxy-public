from typing import Any, Dict, Optional

from sqlalchemy import JSON as SAJSON
from sqlalchemy import Column as SAColumn
from sqlmodel import Field, SQLModel


class SyncRun(SQLModel, table=True):
    __tablename__ = "sync_runs"

    id: str = Field(primary_key=True)
    source_root: str
    requested_by: str = Field(default="operator")
    worker_host: Optional[str] = Field(default=None, index=True)
    status: str = Field(default="started", index=True)
    total_files: int = Field(default=0)
    total_bytes: int = Field(default=0)
    scanned_files: int = Field(default=0)
    uploaded_files: int = Field(default=0)
    skipped_files: int = Field(default=0)
    failed_files: int = Field(default=0)
    uploaded_bytes: int = Field(default=0)
    last_heartbeat_at: Optional[str] = Field(default=None, index=True)
    error_summary: Optional[str] = None
    stats: Dict[str, Any] = Field(default={}, sa_column=SAColumn(SAJSON))
    started_at: Optional[str] = Field(default=None, index=True)
    completed_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class MirroredAsset(SQLModel, table=True):
    __tablename__ = "mirrored_assets"

    id: str = Field(primary_key=True)
    sync_run_id: Optional[str] = Field(default=None, index=True)
    relative_path: str = Field(unique=True, index=True)
    directory_path: str = Field(default="")
    original_name: str
    storage_path: Optional[str] = None
    extension: str = Field(default="", index=True)
    mime_type: Optional[str] = None
    size_bytes: int = Field(default=0)
    sha256: Optional[str] = Field(default=None, index=True)
    modified_at: Optional[str] = Field(default=None, index=True)
    category: str = Field(default="other", index=True)
    preview_kind: str = Field(default="none", index=True)
    sensitivity: str = Field(default="standard", index=True)
    is_sensitive: bool = Field(default=False, index=True)
    upload_status: str = Field(default="pending", index=True)
    text_extract_status: str = Field(default="not_started", index=True)
    text_extract_excerpt: Optional[str] = None
    attributes: Dict[str, Any] = Field(default={}, sa_column=SAColumn(SAJSON))
    last_seen_at: Optional[str] = Field(default=None, index=True)
    uploaded_at: Optional[str] = Field(default=None, index=True)
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class AssetDerivative(SQLModel, table=True):
    __tablename__ = "asset_derivatives"

    id: str = Field(primary_key=True)
    asset_id: str = Field(index=True)
    derivative_type: str = Field(index=True)
    storage_path: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: int = Field(default=0)
    status: str = Field(default="ready", index=True)
    content_text: Optional[str] = None
    attributes: Dict[str, Any] = Field(default={}, sa_column=SAColumn(SAJSON))
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None


class LeadEvidenceLink(SQLModel, table=True):
    __tablename__ = "lead_evidence_links"

    id: str = Field(primary_key=True)
    lead_id: str = Field(index=True)
    asset_id: str = Field(index=True)
    link_type: str = Field(default="archive_file", index=True)
    confidence_score: int = Field(default=70)
    rationale: Optional[str] = None
    include_on_lead: bool = Field(default=True)
    linked_by: str = Field(default="operator")
    created_at: Optional[str] = Field(default=None, index=True)
    updated_at: Optional[str] = None
