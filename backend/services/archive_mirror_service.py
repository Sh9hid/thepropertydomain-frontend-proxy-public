from __future__ import annotations

import mimetypes
import re
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional

from fastapi import HTTPException
from sqlalchemy import func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.config import MIRROR_STORAGE_ROOT
from core.utils import now_iso
from models.archive_models import MirroredAsset, SyncRun
from models.archive_schemas import (
    AssetBatchUpsertItem,
    MirroredAssetManifest,
    SyncRunCompleteRequest,
    SyncRunStartRequest,
)

TEXT_EXTENSIONS = {".txt", ".md", ".csv", ".json", ".html", ".htm", ".xml", ".log"}
SPREADSHEET_EXTENSIONS = {".xlsx", ".xls"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tif", ".tiff"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".m4v"}
SENSITIVE_KEYWORDS = (
    "passport",
    "licence",
    "license",
    "trust receipt",
    "kyc",
    "identity",
    "bank",
    "2fa",
    "recovery",
    "drivers",
    "driver licence",
    "medicare",
    "salary",
    "payslip",
    "statement",
    "tax return",
)
INTERNAL_KEYWORDS = (
    "important documents",
    "authority",
    "contract",
    "listing agreement",
    "agency agreement",
    "mortgage",
    "loan",
    "valuation",
    "appraisal",
)


def ensure_mirror_storage() -> tuple[Path, Path]:
    raw_root = MIRROR_STORAGE_ROOT / "raw"
    derived_root = MIRROR_STORAGE_ROOT / "derived"
    raw_root.mkdir(parents=True, exist_ok=True)
    derived_root.mkdir(parents=True, exist_ok=True)
    return raw_root, derived_root


def normalize_relative_path(relative_path: str) -> str:
    candidate = re.sub(r"^[A-Za-z]:/+", "", str(relative_path or "").replace("\\", "/").strip())
    candidate = candidate.lstrip("/")
    parts = [part for part in PurePosixPath(candidate).parts if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        raise HTTPException(status_code=422, detail="relative_path must be a safe relative path")
    return "/".join(parts)


def build_raw_absolute_path(relative_path: str) -> Path:
    raw_root, _ = ensure_mirror_storage()
    posix_path = PurePosixPath(normalize_relative_path(relative_path))
    return raw_root.joinpath(*posix_path.parts)


def build_relative_storage_path(relative_path: str) -> str:
    return f"raw/{normalize_relative_path(relative_path)}"


def build_derived_absolute_path(relative_path: str, derivative_type: str, suffix: str = ".txt") -> Path:
    _, derived_root = ensure_mirror_storage()
    posix_path = PurePosixPath(normalize_relative_path(relative_path))
    filename = f"{posix_path.name}.{derivative_type}{suffix}"
    base_parts = list(posix_path.parts[:-1])
    return derived_root.joinpath(*base_parts, filename)


def build_relative_derived_path(relative_path: str, derivative_type: str, suffix: str = ".txt") -> str:
    posix_path = PurePosixPath(normalize_relative_path(relative_path))
    filename = f"{posix_path.name}.{derivative_type}{suffix}"
    parent = "/".join(posix_path.parts[:-1])
    return f"derived/{parent + '/' if parent else ''}{filename}"


def _asset_category(extension: str, mime_type: Optional[str]) -> tuple[str, str]:
    mime_lower = (mime_type or "").lower()
    if extension in IMAGE_EXTENSIONS or mime_lower.startswith("image/"):
        return "image", "image"
    if extension in VIDEO_EXTENSIONS or mime_lower.startswith("video/"):
        return "video", "video"
    if extension in TEXT_EXTENSIONS or extension in SPREADSHEET_EXTENSIONS:
        return "document", "text"
    if extension == ".pdf" or mime_lower == "application/pdf":
        return "document", "pdf"
    return "other", "none"


def _asset_sensitivity(relative_path: str) -> tuple[str, bool]:
    haystack = normalize_relative_path(relative_path).lower()
    if any(token in haystack for token in SENSITIVE_KEYWORDS):
        return "restricted", True
    if any(token in haystack for token in INTERNAL_KEYWORDS):
        return "internal", True
    return "standard", False


def _guess_mime_type(relative_path: str, mime_type: Optional[str]) -> str:
    return mime_type or mimetypes.guess_type(relative_path)[0] or "application/octet-stream"


def build_asset_summary(asset: MirroredAsset) -> Dict[str, Any]:
    return {
        "id": asset.id,
        "sync_run_id": asset.sync_run_id,
        "relative_path": asset.relative_path,
        "directory_path": asset.directory_path,
        "original_name": asset.original_name,
        "storage_path": asset.storage_path,
        "extension": asset.extension,
        "mime_type": asset.mime_type,
        "size_bytes": asset.size_bytes,
        "sha256": asset.sha256,
        "modified_at": asset.modified_at,
        "category": asset.category,
        "preview_kind": asset.preview_kind,
        "sensitivity": asset.sensitivity,
        "is_sensitive": asset.is_sensitive,
        "upload_status": asset.upload_status,
        "text_extract_status": asset.text_extract_status,
        "text_extract_excerpt": asset.text_extract_excerpt,
        "attributes": asset.attributes or {},
        "last_seen_at": asset.last_seen_at,
        "uploaded_at": asset.uploaded_at,
        "created_at": asset.created_at,
        "updated_at": asset.updated_at,
        "download_url": f"/api/evidence/assets/{asset.id}/download",
    }


async def get_sync_run_or_404(session: AsyncSession, sync_run_id: str) -> SyncRun:
    run = await session.get(SyncRun, sync_run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Sync run not found")
    return run


async def get_asset_or_404(session: AsyncSession, asset_id: str) -> MirroredAsset:
    asset = await session.get(MirroredAsset, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset


async def create_sync_run(session: AsyncSession, body: SyncRunStartRequest) -> SyncRun:
    now = now_iso()
    run = SyncRun(
        id=str(uuid.uuid4()),
        source_root=body.source_root,
        requested_by=body.requested_by,
        worker_host=body.worker_host,
        status="running",
        total_files=max(0, body.total_files),
        total_bytes=max(0, body.total_bytes),
        scanned_files=0,
        uploaded_files=0,
        skipped_files=0,
        failed_files=0,
        uploaded_bytes=0,
        last_heartbeat_at=now,
        started_at=now,
        updated_at=now,
        stats={},
    )
    session.add(run)
    await session.commit()
    await session.refresh(run)
    return run


async def summarize_sync_run(session: AsyncSession, run: SyncRun) -> SyncRun:
    result = await session.execute(
        text(
            """
            SELECT
                COUNT(*) AS scanned_files,
                COALESCE(SUM(CASE WHEN upload_status = 'completed' THEN 1 ELSE 0 END), 0) AS uploaded_files,
                COALESCE(SUM(CASE WHEN upload_status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped_files,
                COALESCE(SUM(CASE WHEN upload_status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_files,
                COALESCE(SUM(CASE WHEN upload_status = 'completed' THEN size_bytes ELSE 0 END), 0) AS uploaded_bytes
            FROM mirrored_assets
            WHERE sync_run_id = :sync_run_id
            """
        ),
        {"sync_run_id": run.id},
    )
    counts = result.mappings().first() or {}
    run.scanned_files = int(counts.get("scanned_files") or 0)
    run.uploaded_files = int(counts.get("uploaded_files") or 0)
    run.skipped_files = int(counts.get("skipped_files") or 0)
    run.failed_files = int(counts.get("failed_files") or 0)
    run.uploaded_bytes = int(counts.get("uploaded_bytes") or 0)
    return run


async def list_sync_runs(session: AsyncSession, run_id: Optional[str] = None, limit: int = 10) -> List[SyncRun]:
    if run_id:
        run = await get_sync_run_or_404(session, run_id)
        await summarize_sync_run(session, run)
        return [run]
    result = await session.execute(select(SyncRun).order_by(SyncRun.started_at.desc()).limit(limit))
    runs = list(result.scalars().all())
    for run in runs:
        await summarize_sync_run(session, run)
    return runs


def _manifest_core_fields(manifest: MirroredAssetManifest) -> Dict[str, Any]:
    relative_path = normalize_relative_path(manifest.relative_path)
    directory_path = str(PurePosixPath(relative_path).parent).replace("\\", "/")
    directory_path = "" if directory_path == "." else directory_path
    extension = Path(relative_path).suffix.lower()
    mime_type = _guess_mime_type(relative_path, manifest.mime_type)
    category, preview_kind = _asset_category(extension, mime_type)
    sensitivity, is_sensitive = _asset_sensitivity(relative_path)
    return {
        "relative_path": relative_path,
        "directory_path": directory_path,
        "original_name": manifest.original_name or PurePosixPath(relative_path).name,
        "extension": extension,
        "mime_type": mime_type,
        "category": category,
        "preview_kind": preview_kind,
        "sensitivity": sensitivity,
        "is_sensitive": is_sensitive,
    }


async def upsert_asset_manifests(
    session: AsyncSession,
    sync_run_id: str,
    manifests: Iterable[MirroredAssetManifest],
) -> List[AssetBatchUpsertItem]:
    run = await get_sync_run_or_404(session, sync_run_id)
    manifest_list = list(manifests)
    if not manifest_list:
        return []

    normalized_paths = [normalize_relative_path(item.relative_path) for item in manifest_list]
    existing_result = await session.execute(
        select(MirroredAsset).where(MirroredAsset.relative_path.in_(normalized_paths))
    )
    existing_by_path = {asset.relative_path: asset for asset in existing_result.scalars().all()}
    now = now_iso()
    results: List[AssetBatchUpsertItem] = []

    for manifest in manifest_list:
        core = _manifest_core_fields(manifest)
        asset = existing_by_path.get(core["relative_path"])
        existing_storage_ok = False
        if asset and asset.storage_path:
            existing_storage_ok = (MIRROR_STORAGE_ROOT / asset.storage_path).exists()

        upload_required = True
        upload_reason = "new_asset"

        if asset:
            sha_changed = (asset.sha256 or "") != manifest.sha256
            size_changed = int(asset.size_bytes or 0) != int(manifest.size_bytes or 0)
            if existing_storage_ok and not sha_changed and not size_changed:
                upload_required = False
                upload_reason = "unchanged"
                asset.upload_status = "skipped"
            elif not existing_storage_ok:
                upload_reason = "missing_storage"
                asset.upload_status = "pending"
            elif sha_changed:
                upload_reason = "sha_changed"
                asset.upload_status = "pending"
            elif size_changed:
                upload_reason = "size_changed"
                asset.upload_status = "pending"

            asset.sync_run_id = sync_run_id
            asset.directory_path = core["directory_path"]
            asset.original_name = core["original_name"]
            asset.extension = core["extension"]
            asset.mime_type = core["mime_type"]
            asset.size_bytes = manifest.size_bytes
            asset.sha256 = manifest.sha256
            asset.modified_at = manifest.modified_at
            asset.category = core["category"]
            asset.preview_kind = core["preview_kind"]
            asset.sensitivity = core["sensitivity"]
            asset.is_sensitive = core["is_sensitive"]
            asset.attributes = {**(asset.attributes or {}), **(manifest.attributes or {})}
            asset.last_seen_at = now
            asset.updated_at = now
        else:
            asset = MirroredAsset(
                id=str(uuid.uuid4()),
                sync_run_id=sync_run_id,
                relative_path=core["relative_path"],
                directory_path=core["directory_path"],
                original_name=core["original_name"],
                extension=core["extension"],
                mime_type=core["mime_type"],
                size_bytes=manifest.size_bytes,
                sha256=manifest.sha256,
                modified_at=manifest.modified_at,
                category=core["category"],
                preview_kind=core["preview_kind"],
                sensitivity=core["sensitivity"],
                is_sensitive=core["is_sensitive"],
                upload_status="pending",
                text_extract_status="not_started",
                attributes=manifest.attributes or {},
                last_seen_at=now,
                created_at=now,
                updated_at=now,
            )
            session.add(asset)
            existing_by_path[asset.relative_path] = asset

        results.append(
            AssetBatchUpsertItem(
                asset_id=asset.id,
                relative_path=asset.relative_path,
                upload_required=upload_required,
                upload_reason=upload_reason,
                upload_status=asset.upload_status,
                sensitivity=asset.sensitivity,
                is_sensitive=asset.is_sensitive,
            )
        )

    run.last_heartbeat_at = now
    run.scanned_files += len(manifest_list)
    run.updated_at = now
    await session.commit()
    return results


def _read_text_file(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(errors="ignore")


def _extract_pdf_text(path: Path) -> str:
    from pypdf import PdfReader

    reader = PdfReader(str(path))
    parts: List[str] = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(part for part in parts if part.strip())


def _extract_spreadsheet_text(path: Path) -> str:
    import pandas as pd

    workbook = pd.ExcelFile(path)
    chunks: List[str] = []
    for sheet_name in workbook.sheet_names[:5]:
        frame = workbook.parse(sheet_name=sheet_name, nrows=40)
        if frame.empty:
            continue
        chunks.append(f"[Sheet: {sheet_name}]")
        chunks.append(frame.fillna("").astype(str).to_csv(index=False))
    return "\n".join(chunks)


def _extract_image_metadata(path: Path) -> Dict[str, Any]:
    from PIL import Image

    with Image.open(path) as image:
        width, height = image.size
        return {"width": width, "height": height, "mode": image.mode}


def _extract_text_for_asset(asset: MirroredAsset, absolute_path: Path) -> tuple[Optional[str], Dict[str, Any]]:
    metadata: Dict[str, Any] = {}
    extension = (asset.extension or "").lower()
    if extension in TEXT_EXTENSIONS:
        return _read_text_file(absolute_path), metadata
    if extension in SPREADSHEET_EXTENSIONS:
        return _extract_spreadsheet_text(absolute_path), metadata
    if extension == ".pdf":
        return _extract_pdf_text(absolute_path), metadata
    if extension in IMAGE_EXTENSIONS:
        metadata.update(_extract_image_metadata(absolute_path))
        return None, metadata
    return None, metadata


async def _upsert_derivative(
    session: AsyncSession,
    asset_id: str,
    derivative_type: str,
    storage_path: Optional[str],
    mime_type: Optional[str],
    content_text: Optional[str],
    attributes: Dict[str, Any],
):
    from models.archive_models import AssetDerivative

    result = await session.execute(
        select(AssetDerivative).where(
            AssetDerivative.asset_id == asset_id,
            AssetDerivative.derivative_type == derivative_type,
        )
    )
    derivative = result.scalars().first()
    now = now_iso()
    size_bytes = len(content_text.encode("utf-8")) if content_text else 0
    if derivative:
        derivative.storage_path = storage_path
        derivative.mime_type = mime_type
        derivative.size_bytes = size_bytes
        derivative.status = "ready"
        derivative.content_text = content_text
        derivative.attributes = attributes
        derivative.updated_at = now
    else:
        derivative = AssetDerivative(
            id=str(uuid.uuid4()),
            asset_id=asset_id,
            derivative_type=derivative_type,
            storage_path=storage_path,
            mime_type=mime_type,
            size_bytes=size_bytes,
            status="ready",
            content_text=content_text,
            attributes=attributes,
            created_at=now,
            updated_at=now,
        )
        session.add(derivative)
    return derivative


async def refresh_asset_derivatives(session: AsyncSession, asset: MirroredAsset):
    absolute_path = build_raw_absolute_path(asset.relative_path)
    if not absolute_path.exists():
        raise HTTPException(status_code=404, detail="Asset file is missing from mirror storage")

    now = now_iso()
    attributes = dict(asset.attributes or {})
    try:
        extracted_text, metadata = _extract_text_for_asset(asset, absolute_path)
        attributes.update(metadata)
        asset.attributes = attributes
        asset.updated_at = now

        derivatives = []
        if extracted_text:
            from core.config import MIRROR_TEXT_MAX_CHARS

            normalized_text = extracted_text[:MIRROR_TEXT_MAX_CHARS]
            excerpt = normalized_text[:1000]
            derived_path = build_derived_absolute_path(asset.relative_path, "text_extract")
            derived_path.parent.mkdir(parents=True, exist_ok=True)
            derived_path.write_text(normalized_text, encoding="utf-8")
            derivative = await _upsert_derivative(
                session=session,
                asset_id=asset.id,
                derivative_type="text_extract",
                storage_path=build_relative_derived_path(asset.relative_path, "text_extract"),
                mime_type="text/plain",
                content_text=normalized_text,
                attributes={"excerpt": excerpt},
            )
            derivatives.append(derivative)
            asset.text_extract_status = "available"
            asset.text_extract_excerpt = excerpt
        else:
            asset.text_extract_status = "not_applicable"
            asset.text_extract_excerpt = None

        await session.flush()
        return derivatives
    except Exception as exc:
        asset.text_extract_status = "failed"
        asset.text_extract_excerpt = str(exc)[:240]
        asset.updated_at = now
        await session.flush()
        return []


async def save_uploaded_asset(
    session: AsyncSession,
    asset_id: str,
    sync_run_id: str,
    upload,
    sha256: Optional[str] = None,
    modified_at: Optional[str] = None,
) -> MirroredAsset:
    import aiofiles
    import hashlib

    run = await get_sync_run_or_404(session, sync_run_id)
    asset = await get_asset_or_404(session, asset_id)

    if asset.sync_run_id and asset.sync_run_id != sync_run_id:
        asset.sync_run_id = sync_run_id

    absolute_path = build_raw_absolute_path(asset.relative_path)
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    hasher = hashlib.sha256()
    bytes_written = 0
    previous_status = asset.upload_status
    previous_size = int(asset.size_bytes or 0)

    try:
        async with aiofiles.open(absolute_path, "wb") as handle:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
                bytes_written += len(chunk)
                await handle.write(chunk)
    except Exception:
        if absolute_path.exists():
            absolute_path.unlink()
        raise
    finally:
        await upload.close()

    digest = hasher.hexdigest()
    if sha256 and digest.lower() != sha256.lower():
        if absolute_path.exists():
            absolute_path.unlink()
        raise HTTPException(status_code=400, detail="Uploaded file hash does not match manifest")

    now = now_iso()
    asset.storage_path = build_relative_storage_path(asset.relative_path)
    asset.sha256 = digest
    asset.size_bytes = bytes_written
    asset.modified_at = modified_at or asset.modified_at
    asset.mime_type = _guess_mime_type(asset.relative_path, getattr(upload, "content_type", None) or asset.mime_type)
    asset.upload_status = "completed"
    asset.uploaded_at = now
    asset.last_seen_at = now
    asset.updated_at = now

    await refresh_asset_derivatives(session, asset)

    run.last_heartbeat_at = now
    run.updated_at = now
    if previous_status != "completed":
        run.uploaded_files += 1
        run.uploaded_bytes += bytes_written
    else:
        run.uploaded_bytes = max(0, run.uploaded_bytes - previous_size + bytes_written)

    await session.commit()
    await session.refresh(asset)
    return asset


async def complete_sync_run(session: AsyncSession, run_id: str, body: SyncRunCompleteRequest):
    run = await get_sync_run_or_404(session, run_id)
    now = now_iso()
    await summarize_sync_run(session, run)
    run.status = body.status
    run.error_summary = body.error_summary
    run.last_heartbeat_at = now
    run.completed_at = now
    run.updated_at = now
    await session.commit()
    await session.refresh(run)
    return run


async def get_asset_detail(session: AsyncSession, asset_id: str) -> Dict[str, Any]:
    from models.archive_models import AssetDerivative

    asset = await get_asset_or_404(session, asset_id)
    derivatives_result = await session.execute(
        select(AssetDerivative).where(AssetDerivative.asset_id == asset_id).order_by(AssetDerivative.derivative_type)
    )
    derivatives = list(derivatives_result.scalars().all())

    links_result = await session.execute(
        text(
            """
            SELECT
                l.id,
                l.lead_id,
                l.asset_id,
                l.link_type,
                l.confidence_score,
                l.rationale,
                l.include_on_lead,
                l.linked_by,
                l.created_at,
                l.updated_at,
                leads.address,
                leads.suburb
            FROM lead_evidence_links l
            LEFT JOIN leads ON leads.id = l.lead_id
            WHERE l.asset_id = :asset_id
            ORDER BY l.created_at DESC
            """
        ),
        {"asset_id": asset_id},
    )
    detail = build_asset_summary(asset)
    detail["derivatives"] = [derivative.model_dump() for derivative in derivatives]
    detail["linked_leads"] = [dict(row) for row in links_result.mappings().all()]
    return detail


async def list_assets(
    session: AsyncSession,
    search: Optional[str] = None,
    lead_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    stmt = select(MirroredAsset)
    count_stmt = select(func.count()).select_from(MirroredAsset)

    if lead_id:
        from models.archive_models import LeadEvidenceLink

        link_result = await session.execute(
            select(LeadEvidenceLink.asset_id).where(LeadEvidenceLink.lead_id == lead_id)
        )
        asset_ids = list(link_result.scalars().all())
        if not asset_ids:
            return {"assets": [], "total": 0}
        stmt = stmt.where(MirroredAsset.id.in_(asset_ids))
        count_stmt = count_stmt.where(MirroredAsset.id.in_(asset_ids))

    if search and search.strip():
        token = f"%{search.strip().lower()}%"
        clause = (
            func.lower(MirroredAsset.relative_path).like(token)
            | func.lower(MirroredAsset.original_name).like(token)
        )
        stmt = stmt.where(clause)
        count_stmt = count_stmt.where(clause)

    stmt = stmt.order_by(
        MirroredAsset.is_sensitive.desc(),
        MirroredAsset.uploaded_at.desc(),
        MirroredAsset.relative_path.asc(),
    ).offset(offset).limit(limit)
    total_result = await session.execute(count_stmt)
    result = await session.execute(stmt)
    assets = [build_asset_summary(asset) for asset in result.scalars().all()]
    return {"assets": assets, "total": int(total_result.scalar_one() or 0)}


async def get_asset_download_path(session: AsyncSession, asset_id: str) -> tuple[MirroredAsset, Path]:
    asset = await get_asset_or_404(session, asset_id)
    absolute_path = build_raw_absolute_path(asset.relative_path)
    if not absolute_path.exists():
        raise HTTPException(status_code=404, detail="Asset file is missing from mirror storage")
    return asset, absolute_path


async def link_asset_to_lead(
    session: AsyncSession,
    asset_id: str,
    lead_id: str,
    link_type: str,
    confidence_score: int,
    rationale: Optional[str],
    include_on_lead: bool,
    linked_by: str,
):
    from models.archive_models import LeadEvidenceLink
    from models.sql_models import Lead
    from services.scoring import _score_lead

    asset = await get_asset_or_404(session, asset_id)
    lead = await session.get(Lead, lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    existing_result = await session.execute(
        select(LeadEvidenceLink).where(
            LeadEvidenceLink.lead_id == lead_id,
            LeadEvidenceLink.asset_id == asset_id,
            LeadEvidenceLink.link_type == link_type,
        )
    )
    link = existing_result.scalars().first()
    now = now_iso()
    if link:
        link.confidence_score = confidence_score
        link.rationale = rationale
        link.include_on_lead = include_on_lead
        link.linked_by = linked_by
        link.updated_at = now
    else:
        link = LeadEvidenceLink(
            id=str(uuid.uuid4()),
            lead_id=lead_id,
            asset_id=asset_id,
            link_type=link_type,
            confidence_score=confidence_score,
            rationale=rationale,
            include_on_lead=include_on_lead,
            linked_by=linked_by,
            created_at=now,
            updated_at=now,
        )
        session.add(link)

    if include_on_lead:
        linked_files = [str(item) for item in (lead.linked_files or []) if str(item).strip()]
        if asset.relative_path not in linked_files:
            linked_files.append(asset.relative_path)
        source_evidence = [str(item) for item in (lead.source_evidence or []) if str(item).strip()]
        note = rationale or f"Archive evidence linked: {asset.relative_path}"
        if note not in source_evidence:
            source_evidence.append(note)

        lead.linked_files = linked_files
        lead.source_evidence = source_evidence
        lead.updated_at = now
        scored = _score_lead({**lead.model_dump(), "linked_files": linked_files, "source_evidence": source_evidence})
        lead.evidence_score = scored["evidence_score"]
        lead.call_today_score = scored["call_today_score"]

    await session.commit()
    await session.refresh(link)
    return link
