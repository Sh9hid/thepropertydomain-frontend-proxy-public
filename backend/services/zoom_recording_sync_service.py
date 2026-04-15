import hashlib
import hmac
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.logic import _resolve_zoom_account
from core.utils import now_iso
from services.integrations import _zoom_request
from services.recording_service import download_recording
from services.speech_pipeline_service import ensure_speech_schema, shadow_write_call_log_row
from services.zoom_call_sync_service import ensure_call_log_schema, find_best_matching_lead, normalize_zoom_call_entry

_logger = logging.getLogger(__name__)
_PAGE_SIZE = 100
_VALID_PRODUCTS = {"phone", "meetings", "mixed"}


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def validate_zoom_runtime_config() -> None:
    client_id = os.getenv("ZOOM_CLIENT_ID", "").strip()
    client_secret = os.getenv("ZOOM_CLIENT_SECRET", "").strip()
    account_id = os.getenv("ZOOM_ACCOUNT_ID", "").strip()
    webhook_enabled = _boolish(os.getenv("ZOOM_WEBHOOK_ENABLED", "0"))
    webhook_secret = os.getenv("ZOOM_WEBHOOK_SECRET", "").strip()

    if any((client_id, client_secret, account_id)):
        if not (client_id and client_secret and account_id):
            raise RuntimeError("Incomplete Zoom config: ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET, and ZOOM_ACCOUNT_ID are required together")
    if webhook_enabled and not webhook_secret:
        raise RuntimeError("Incomplete Zoom config: ZOOM_WEBHOOK_SECRET is required when ZOOM_WEBHOOK_ENABLED=1")


def infer_zoom_product(account: Optional[Dict[str, Any]]) -> str:
    account = account or {}
    explicit = str(account.get("zoom_product") or os.getenv("ZOOM_PRODUCT", "")).strip().lower()
    if explicit in _VALID_PRODUCTS:
        return explicit
    label = str(account.get("label") or "").lower()
    send_path = str(account.get("send_path") or "").lower()
    api_base = str(account.get("api_base") or "").lower()
    if "/phone/" in send_path or _boolish(account.get("text_enabled")) or _boolish(account.get("call_enabled")):
        return "phone"
    if "meeting" in label:
        return "meetings"
    if "/meetings" in api_base or "/recordings" in api_base:
        return "meetings"
    return "phone"


def get_zoom_capabilities(account: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    product = infer_zoom_product(account)
    if product == "meetings":
        return {
            "product": "meetings",
            "recordings_supported": True,
            "ai_summary_supported": False,
            "reason": "Zoom Meetings recordings are available through recording APIs; AI Companion summaries are separate.",
        }
    if product == "phone":
        return {
            "product": "phone",
            "recordings_supported": True,
            "ai_summary_supported": False,
            "reason": "Zoom Phone recordings are available through phone call-log recording APIs; no AI summary endpoint is enabled in this integration path.",
        }
    return {
        "product": "mixed",
        "recordings_supported": False,
        "ai_summary_supported": False,
        "reason": "Zoom product path is mixed or unclear. Recordings sync requires an explicit product path.",
    }


def log_zoom_runtime_status(account: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    capabilities = get_zoom_capabilities(account)
    payload = {
        "product": capabilities["product"],
        "recordings_enabled": capabilities["recordings_supported"],
        "ai_summary_enabled": capabilities["ai_summary_supported"],
        "reason": capabilities["reason"],
    }
    _logger.info("zoom_runtime_status=%s", json.dumps(payload, sort_keys=True))
    return payload


def extract_meeting_recording_artifacts(meeting: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts: List[Dict[str, Any]] = []
    for recording in meeting.get("recording_files", []) or []:
        if not isinstance(recording, dict):
            continue
        recording_state = str(recording.get("status") or "").strip().lower()
        downloadable = bool(recording.get("download_url") or recording.get("file_url"))
        artifacts.append(
            {
                "product": "meetings",
                "artifact_type": "recording",
                "external_id": str(recording.get("id") or ""),
                "external_parent_id": str(meeting.get("uuid") or meeting.get("id") or ""),
                "meeting_uuid": str(meeting.get("uuid") or ""),
                "meeting_id": str(meeting.get("id") or ""),
                "call_id": "",
                "host_id": str(meeting.get("host_id") or ""),
                "recording_file_id": str(recording.get("id") or ""),
                "download_url": str(recording.get("download_url") or ""),
                "file_url": str(recording.get("file_url") or ""),
                "file_type": str(recording.get("file_type") or "").lower(),
                "recording_state": recording_state,
                "status": "discovered" if recording_state != "completed" or not downloadable else "processing",
                "raw_payload": json.dumps({"meeting": meeting, "recording": recording}, ensure_ascii=True),
            }
        )
    return artifacts


def verify_zoom_webhook_request(secret: str, body: bytes, timestamp: str, signature: str) -> bool:
    if not secret or not body or not timestamp or not signature:
        return False
    expected = "v0=" + hmac.new(
        secret.encode("utf-8"),
        f"v0:{timestamp}:{body.decode('utf-8')}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def build_zoom_endpoint_validation(payload: Dict[str, Any], secret: str) -> Dict[str, str]:
    plain_token = str(payload.get("payload", {}).get("plainToken") or payload.get("plainToken") or "")
    return {
        "plainToken": plain_token,
        "encryptedToken": hmac.new(secret.encode("utf-8"), plain_token.encode("utf-8"), hashlib.sha256).hexdigest(),
    }


async def ensure_zoom_recording_schema(session: AsyncSession) -> None:
    await ensure_call_log_schema(session)
    await ensure_speech_schema(session)
    ddls = (
        "ALTER TABLE communication_accounts ADD COLUMN zoom_product TEXT DEFAULT ''",
        "ALTER TABLE communication_accounts ADD COLUMN recordings_enabled INTEGER DEFAULT 1",
        "ALTER TABLE communication_accounts ADD COLUMN ai_summary_enabled INTEGER DEFAULT 0",
        "ALTER TABLE communication_accounts ADD COLUMN webhook_enabled INTEGER DEFAULT 0",
        "ALTER TABLE communication_accounts ADD COLUMN feature_flags_json TEXT DEFAULT '{}'",
        """
        CREATE TABLE IF NOT EXISTS zoom_recording_artifacts (
            id TEXT PRIMARY KEY,
            integration_id TEXT DEFAULT '',
            product TEXT NOT NULL,
            artifact_type TEXT NOT NULL DEFAULT 'recording',
            external_id TEXT NOT NULL,
            external_parent_id TEXT DEFAULT '',
            meeting_uuid TEXT DEFAULT '',
            meeting_id TEXT DEFAULT '',
            call_id TEXT DEFAULT '',
            recording_file_id TEXT DEFAULT '',
            lead_id TEXT DEFAULT '',
            linked_entity_type TEXT DEFAULT '',
            linked_entity_id TEXT DEFAULT '',
            download_url TEXT DEFAULT '',
            file_url TEXT DEFAULT '',
            file_type TEXT DEFAULT '',
            status TEXT DEFAULT 'discovered',
            ai_summary_status TEXT DEFAULT 'unsupported',
            unmatched_reason TEXT DEFAULT '',
            error_message TEXT DEFAULT '',
            storage_uri TEXT DEFAULT '',
            raw_payload TEXT DEFAULT '{}',
            discovered_at TEXT,
            processed_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_zoom_recording_unique ON zoom_recording_artifacts(product, artifact_type, external_id)",
        "CREATE INDEX IF NOT EXISTS idx_zoom_recording_lead ON zoom_recording_artifacts(lead_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_zoom_recording_call ON zoom_recording_artifacts(call_id)",
        "CREATE INDEX IF NOT EXISTS idx_zoom_recording_status ON zoom_recording_artifacts(status, updated_at DESC)",
    )
    for ddl in ddls:
        try:
            await session.execute(text(ddl))
        except Exception:
            continue
    await session.commit()


async def _find_matching_lead_id(session: AsyncSession, *numbers: str) -> tuple[str, str]:
    matched_lead, unmatched_reason = await find_best_matching_lead(session, None, *numbers)
    if not matched_lead:
        return "", unmatched_reason
    return str(matched_lead.get("id") or ""), ""


async def _upsert_call_log_from_phone_recording(
    session: AsyncSession,
    raw_call: Dict[str, Any],
    recording: Dict[str, Any],
    lead_id: str,
) -> None:
    normalized = normalize_zoom_call_entry(raw_call)
    if not normalized:
        return
    raw_payload = normalized.get("raw_payload")
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except Exception:
            raw_payload = {}
    if not isinstance(raw_payload, dict):
        raw_payload = {}
    raw_payload["recording"] = dict(recording)
    row_payload = {
        "id": hashlib.md5(f"zoom-call:{normalized['provider_call_id']}".encode("utf-8")).hexdigest(),
        "lead_id": lead_id,
        "lead_address": "",
        "outcome": normalized["outcome"],
        "connected": 1 if normalized["connected"] else 0,
        "user_id": normalized["user_id"],
        "timestamp": normalized["timestamp"],
        "call_duration_seconds": normalized["duration_seconds"],
        "duration_seconds": normalized["duration_seconds"],
        "note": normalized["note"],
        "operator": normalized["operator"],
        "logged_at": normalized["logged_at"],
        "logged_date": normalized["logged_date"],
        "next_action_due": None,
        "provider": "zoom",
        "provider_call_id": normalized["provider_call_id"],
        "direction": normalized["direction"],
        "from_number": normalized["from_number"],
        "to_number": normalized["to_number"],
        "raw_payload": json.dumps(raw_payload, ensure_ascii=True),
        "recording_url": str(recording.get("file_url") or recording.get("download_url") or ""),
        "recording_status": "stored" if recording.get("storage_uri") else "available",
        "recording_duration_seconds": int(recording.get("duration") or normalized["duration_seconds"] or 0),
        "recording_id": str(recording.get("id") or ""),
        "audio_uri": str(recording.get("storage_uri") or ""),
        "audio_storage_status": "stored" if recording.get("storage_uri") else "not_downloaded",
    }
    existing = await session.execute(
        text("SELECT id FROM call_log WHERE provider = 'zoom' AND provider_call_id = :provider_call_id"),
        {"provider_call_id": normalized["provider_call_id"]},
    )
    if existing.mappings().first():
        await session.execute(
            text(
                """
                UPDATE call_log
                SET lead_id = :lead_id,
                    outcome = :outcome,
                    connected = :connected,
                    user_id = :user_id,
                    timestamp = :timestamp,
                    call_duration_seconds = :call_duration_seconds,
                    duration_seconds = :duration_seconds,
                    note = :note,
                    operator = :operator,
                    logged_at = :logged_at,
                    logged_date = :logged_date,
                    next_action_due = :next_action_due,
                    direction = :direction,
                    from_number = :from_number,
                    to_number = :to_number,
                    raw_payload = :raw_payload,
                    recording_url = :recording_url,
                    recording_status = :recording_status,
                    recording_duration_seconds = :recording_duration_seconds
                WHERE provider = :provider AND provider_call_id = :provider_call_id
                """
            ),
            row_payload,
        )
    else:
        await session.execute(
            text(
                """
                INSERT INTO call_log (
                    id, lead_id, lead_address, user_id, outcome, connected, timestamp, call_duration_seconds,
                    duration_seconds, note, operator, logged_at, logged_date, next_action_due, provider,
                    provider_call_id, direction, from_number, to_number, raw_payload, recording_url,
                    recording_status, recording_duration_seconds
                ) VALUES (
                    :id, :lead_id, :lead_address, :user_id, :outcome, :connected, :timestamp, :call_duration_seconds,
                    :duration_seconds, :note, :operator, :logged_at, :logged_date, :next_action_due, :provider,
                    :provider_call_id, :direction, :from_number, :to_number, :raw_payload, :recording_url,
                    :recording_status, :recording_duration_seconds
                )
                """
            ),
            row_payload,
        )
    await shadow_write_call_log_row(session, row_payload)


async def _upsert_artifact(session: AsyncSession, artifact: Dict[str, Any]) -> bool:
    now = now_iso()
    artifact_id = artifact.get("id") or hashlib.md5(
        f"{artifact.get('product')}:{artifact.get('artifact_type')}:{artifact.get('external_id')}".encode("utf-8")
    ).hexdigest()
    artifact["id"] = artifact_id
    artifact.setdefault("created_at", now)
    artifact["updated_at"] = now
    existing = await session.execute(
        text("SELECT id, status FROM zoom_recording_artifacts WHERE product = :product AND artifact_type = :artifact_type AND external_id = :external_id"),
        {
            "product": artifact["product"],
            "artifact_type": artifact["artifact_type"],
            "external_id": artifact["external_id"],
        },
    )
    row = existing.mappings().first()
    if row:
        await session.execute(
            text(
                """
                UPDATE zoom_recording_artifacts
                SET integration_id = :integration_id,
                    external_parent_id = :external_parent_id,
                    meeting_uuid = :meeting_uuid,
                    meeting_id = :meeting_id,
                    call_id = :call_id,
                    recording_file_id = :recording_file_id,
                    lead_id = :lead_id,
                    linked_entity_type = :linked_entity_type,
                    linked_entity_id = :linked_entity_id,
                    download_url = :download_url,
                    file_url = :file_url,
                    file_type = :file_type,
                    status = :status,
                    ai_summary_status = :ai_summary_status,
                    unmatched_reason = :unmatched_reason,
                    error_message = :error_message,
                    storage_uri = :storage_uri,
                    raw_payload = :raw_payload,
                    discovered_at = :discovered_at,
                    processed_at = :processed_at,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            artifact,
        )
        return False
    await session.execute(
        text(
            """
            INSERT INTO zoom_recording_artifacts (
                id, integration_id, product, artifact_type, external_id, external_parent_id, meeting_uuid,
                meeting_id, call_id, recording_file_id, lead_id, linked_entity_type, linked_entity_id,
                download_url, file_url, file_type, status, ai_summary_status, unmatched_reason, error_message,
                storage_uri, raw_payload, discovered_at, processed_at, created_at, updated_at
            ) VALUES (
                :id, :integration_id, :product, :artifact_type, :external_id, :external_parent_id, :meeting_uuid,
                :meeting_id, :call_id, :recording_file_id, :lead_id, :linked_entity_type, :linked_entity_id,
                :download_url, :file_url, :file_type, :status, :ai_summary_status, :unmatched_reason, :error_message,
                :storage_uri, :raw_payload, :discovered_at, :processed_at, :created_at, :updated_at
            )
            """
        ),
        artifact,
    )
    return True


def _build_phone_call_log_paths(filters: Dict[str, Any]) -> List[str]:
    params = {"page_size": _PAGE_SIZE}
    if filters.get("from"):
        params["from"] = str(filters["from"])
    if filters.get("to"):
        params["to"] = str(filters["to"])
    if filters.get("user"):
        params["extension_number"] = str(filters["user"])
    return [f"/phone/call_logs?{urlencode(params)}"]


def _build_meeting_recording_paths(filters: Dict[str, Any]) -> List[str]:
    if filters.get("meeting_uuid"):
        return [f"/meetings/{filters['meeting_uuid']}/recordings"]
    if filters.get("meeting_id"):
        return [f"/meetings/{filters['meeting_id']}/recordings"]
    user = str(filters.get("user") or "me")
    params = {"page_size": _PAGE_SIZE}
    if filters.get("from"):
        params["from"] = str(filters["from"])
    if filters.get("to"):
        params["to"] = str(filters["to"])
    return [f"/users/{user}/recordings?{urlencode(params)}"]


async def _sync_phone_recordings(session: AsyncSession, account: Dict[str, Any], filters: Dict[str, Any]) -> Dict[str, Any]:
    stats = {"meetings_found": 0, "files_discovered": 0, "files_stored": 0, "duplicates_skipped": 0, "unsupported_skipped": 0, "failures": 0, "pending": 0}
    for path in _build_phone_call_log_paths(filters):
        response = _zoom_request(account, "GET", path)
        if not response.get("ok"):
            raise HTTPException(status_code=400, detail=response.get("error") or "Zoom Phone call-log fetch failed")
        call_logs = (response.get("data") or {}).get("call_logs") or []
        stats["meetings_found"] += len(call_logs)
        for raw_call in call_logs:
            if isinstance(raw_call, dict):
                raw_call = dict(raw_call)
                raw_call.setdefault("__zoom_source_endpoint", "/phone/call_logs")
            call_id = str(raw_call.get("id") or "")
            if filters.get("call_id") and call_id != str(filters["call_id"]):
                continue
            rec = _zoom_request(account, "GET", f"/phone/call_logs/{call_id}/recordings")
            if not rec.get("ok"):
                stats["failures"] += 1
                continue
            recordings = (rec.get("data") or {}).get("recordings") or []
            if not recordings and isinstance(rec.get("data"), dict) and rec.get("data", {}).get("id"):
                recordings = [rec["data"]]
            for item in recordings:
                if not isinstance(item, dict):
                    continue
                stats["files_discovered"] += 1
                existing = await session.execute(
                    text("SELECT id FROM zoom_recording_artifacts WHERE product = 'phone' AND artifact_type = 'recording' AND external_id = :external_id"),
                    {"external_id": str(item.get("id") or f"{call_id}:recording")},
                )
                if existing.mappings().first():
                    stats["duplicates_skipped"] += 1
                    continue
                matched_lead, unmatched_reason = await find_best_matching_lead(session, raw_call)
                lead_id = str((matched_lead or {}).get("id") or "")
                artifact = {
                    "integration_id": str(account.get("id") or ""),
                    "product": "phone",
                    "artifact_type": "recording",
                    "external_id": str(item.get("id") or f"{call_id}:recording"),
                    "external_parent_id": call_id,
                    "meeting_uuid": "",
                    "meeting_id": "",
                    "call_id": call_id,
                    "recording_file_id": str(item.get("id") or ""),
                    "lead_id": lead_id,
                    "linked_entity_type": "lead" if lead_id else "",
                    "linked_entity_id": lead_id,
                    "download_url": str(item.get("download_url") or ""),
                    "file_url": str(item.get("file_url") or item.get("download_url") or ""),
                    "file_type": str(item.get("file_type") or "mp3").lower(),
                    "status": "discovered",
                    "ai_summary_status": "unsupported",
                    "unmatched_reason": unmatched_reason,
                    "error_message": "",
                    "storage_uri": "",
                    "raw_payload": json.dumps({"call": raw_call, "recording": item}, ensure_ascii=True),
                    "discovered_at": now_iso(),
                    "processed_at": None,
                }
                recording_url = artifact["file_url"]
                if not recording_url:
                    stats["pending"] += 1
                    await _upsert_artifact(session, artifact)
                    continue
                await _upsert_call_log_from_phone_recording(session, raw_call, {"storage_uri": "", **item}, lead_id)
                if not filters.get("dry_run"):
                    try:
                        local_path = await download_recording(recording_url, artifact["external_id"], account=account)
                    except Exception as exc:
                        local_path = None
                        artifact["error_message"] = str(exc)
                    if local_path:
                        artifact["storage_uri"] = str(local_path).replace("\\", "/").split("/backend/", 1)[-1]
                        artifact["status"] = "stored"
                        artifact["processed_at"] = now_iso()
                        item["storage_uri"] = artifact["storage_uri"]
                        await _upsert_call_log_from_phone_recording(session, raw_call, {"storage_uri": artifact["storage_uri"], **item}, lead_id)
                        stats["files_stored"] += 1
                    else:
                        artifact["status"] = "failed"
                        stats["failures"] += 1
                await _upsert_artifact(session, artifact)
    await session.commit()
    return stats


async def _sync_meeting_recordings(session: AsyncSession, account: Dict[str, Any], filters: Dict[str, Any]) -> Dict[str, Any]:
    stats = {"meetings_found": 0, "files_discovered": 0, "files_stored": 0, "duplicates_skipped": 0, "unsupported_skipped": 0, "failures": 0, "pending": 0}
    for path in _build_meeting_recording_paths(filters):
        response = _zoom_request(account, "GET", path)
        if not response.get("ok"):
            raise HTTPException(status_code=400, detail=response.get("error") or "Zoom Meetings recording fetch failed")
        payload = response.get("data") or {}
        meetings = payload.get("meetings") if isinstance(payload, dict) else None
        if not meetings and isinstance(payload, dict) and payload.get("uuid"):
            meetings = [payload]
        for meeting in meetings or []:
            stats["meetings_found"] += 1
            for artifact in extract_meeting_recording_artifacts(meeting):
                stats["files_discovered"] += 1
                artifact.update(
                    {
                        "integration_id": str(account.get("id") or ""),
                        "lead_id": "",
                        "linked_entity_type": "",
                        "linked_entity_id": "",
                        "ai_summary_status": "unsupported",
                        "unmatched_reason": "meeting_recording_not_linked_to_lead",
                        "error_message": "",
                        "storage_uri": "",
                        "discovered_at": now_iso(),
                        "processed_at": None,
                    }
                )
                created = await _upsert_artifact(session, artifact)
                if not created:
                    stats["duplicates_skipped"] += 1
                    continue
                if artifact["recording_state"] != "completed":
                    stats["pending"] += 1
                    await _upsert_artifact(session, artifact)
                    continue
                if filters.get("dry_run"):
                    await _upsert_artifact(session, artifact)
                    continue
                local_path = await download_recording(
                    artifact.get("file_url") or artifact.get("download_url") or "",
                    artifact["external_id"],
                    account=account,
                )
                if local_path:
                    artifact["storage_uri"] = str(local_path).replace("\\", "/").split("/backend/", 1)[-1]
                    artifact["status"] = "stored"
                    artifact["processed_at"] = now_iso()
                    stats["files_stored"] += 1
                else:
                    artifact["status"] = "failed"
                    stats["failures"] += 1
                await _upsert_artifact(session, artifact)
    await session.commit()
    return stats


async def sync_zoom_recordings(session: AsyncSession, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    filters = dict(filters or {})
    await ensure_zoom_recording_schema(session)
    account = await _resolve_zoom_account(session)
    capabilities = get_zoom_capabilities(account)
    log_zoom_runtime_status(account)
    if not capabilities["recordings_supported"]:
        return {
            "product": capabilities["product"],
            "recordings_supported": False,
            "ai_summary_supported": capabilities["ai_summary_supported"],
            "reason": capabilities["reason"],
            "meetings_found": 0,
            "files_discovered": 0,
            "files_stored": 0,
            "duplicates_skipped": 0,
            "unsupported_skipped": 1,
            "failures": 0,
            "pending": 0,
        }
    started = time.monotonic()
    if capabilities["product"] == "meetings":
        stats = await _sync_meeting_recordings(session, account, filters)
    else:
        stats = await _sync_phone_recordings(session, account, filters)
    return {
        "product": capabilities["product"],
        "recordings_supported": capabilities["recordings_supported"],
        "ai_summary_supported": capabilities["ai_summary_supported"],
        "reason": capabilities["reason"],
        "dry_run": bool(filters.get("dry_run")),
        "verbose": bool(filters.get("verbose")),
        "elapsed_ms": round((time.monotonic() - started) * 1000),
        **stats,
    }


async def get_zoom_recording_artifacts_for_lead(session: AsyncSession, lead_id: str) -> Dict[str, Dict[str, Any]]:
    await ensure_zoom_recording_schema(session)
    rows = (
        await session.execute(
            text(
                """
                SELECT *
                FROM zoom_recording_artifacts
                WHERE lead_id = :lead_id
                ORDER BY updated_at DESC, created_at DESC
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()
    artifacts: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        call_id = str(item.get("call_id") or item.get("external_parent_id") or item.get("external_id") or "")
        if call_id and call_id not in artifacts:
            artifacts[call_id] = item
    return artifacts


__all__ = [
    "build_zoom_endpoint_validation",
    "ensure_zoom_recording_schema",
    "extract_meeting_recording_artifacts",
    "get_zoom_capabilities",
    "get_zoom_recording_artifacts_for_lead",
    "infer_zoom_product",
    "log_zoom_runtime_status",
    "sync_zoom_recordings",
    "validate_zoom_runtime_config",
    "verify_zoom_webhook_request",
]
