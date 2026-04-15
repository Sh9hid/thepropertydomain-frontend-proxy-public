"""
Zoom Phone recording and SMS session endpoints.
"""
import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.routes._deps import APIKeyDep, SessionDep
from api.routes.recordings_support import (
    CallTranscriptIngestRequest,
    call_to_recording_payload,
    json_dict,
    lead_phone_suffixes,
    match_sms_session,
    phone_suffix,
    recording_source_url,
    remote_number as resolve_remote_number,
    session_preview,
)
from core.config import RECORDINGS_ROOT, SPEECH_AUDIO_STORAGE_ROOT
from core.database import get_session, _get_lead_or_404
from core.logic import _hydrate_lead, _resolve_zoom_account
from core.security import get_api_key
from core.utils import _normalize_phone
from services.integrations import _zoom_request
from services.recording_service import download_recording as download_recording_asset
from services.zoom_recording_sync_service import ensure_zoom_recording_schema, get_zoom_recording_artifacts_for_lead

router = APIRouter()


@router.get("/api/leads/{lead_id}/recordings")
async def get_lead_recordings(
    lead_id: str,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """DB-first recording list. Zoom API is a silent enrichment fallback only."""
    artifact_by_call_id = await get_zoom_recording_artifacts_for_lead(session, lead_id)
    # 1. call_log rows (manual + Zoom-synced)
    log_rows = (
        await session.execute(
            text(
                """
                SELECT id, outcome, connected, timestamp, duration_seconds,
                       call_duration_seconds, direction, from_number, to_number,
                       provider, provider_call_id, operator, user_id, raw_payload, recording_url
                FROM call_log WHERE lead_id = :lead_id
                ORDER BY timestamp DESC LIMIT 50
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()

    # 2. SpeechCall rows for local audio
    speech_rows = (
        await session.execute(
            text(
                """
                SELECT id, external_call_id, direction, outcome, started_at,
                       duration_seconds, audio_uri, audio_storage_status
                FROM calls WHERE lead_id = :lead_id
                ORDER BY started_at DESC LIMIT 50
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()
    speech_by_ext: Dict[str, Dict[str, Any]] = {
        str(r["external_call_id"]): dict(r) for r in speech_rows if r.get("external_call_id")
    }
    speech_by_id: Dict[str, Dict[str, Any]] = {str(r["id"]): dict(r) for r in speech_rows}

    # 3. Analysis keyed by call_id
    analysis_rows = (
        await session.execute(
            text("SELECT * FROM call_analysis WHERE lead_id = :lead_id"),
            {"lead_id": lead_id},
        )
    ).mappings().all()
    analysis_by_call_id: Dict[str, Dict[str, Any]] = {}
    for row in analysis_rows:
        cid = str(row["call_id"])
        try:
            key_topics = json.loads(row.get("key_topics") or "[]")
        except (json.JSONDecodeError, TypeError):
            key_topics = []
        try:
            objections = json.loads(row.get("objections") or "[]")
        except (json.JSONDecodeError, TypeError):
            objections = []
        analysis_by_call_id[cid] = {
            "summary": row.get("summary") or "",
            "outcome": row.get("outcome") or "unknown",
            "key_topics": key_topics,
            "objections": objections,
            "next_step": row.get("next_step") or "",
            "suggested_follow_up_task": row.get("suggested_follow_up_task") or "",
            "sentiment_label": row.get("sentiment_label") or "",
            "sentiment_confidence": float(row.get("sentiment_confidence") or 0),
            "sentiment_reason": row.get("sentiment_reason") or "",
            "overall_confidence": float(row.get("overall_confidence") or 0),
            "analyzed_at": row.get("analyzed_at"),
        }

    recordings: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()

    # Entries from call_log
    for row in log_rows:
        call_id = str(row.get("provider_call_id") or row["id"])
        if call_id in seen_ids:
            continue
        seen_ids.add(call_id)

        speech = speech_by_ext.get(call_id) or speech_by_id.get(call_id)
        stored = bool(speech and speech.get("audio_storage_status") == "stored" and speech.get("audio_uri"))
        file_url: Optional[str] = f"/api/recordings/{speech['id']}/stream" if stored else None
        raw_payload = json_dict(row.get("raw_payload"))
        remote_file_url = recording_source_url(row, raw_payload)
        direction = str(row.get("direction") or "")
        from_number = str(row.get("from_number") or "")
        to_number = str(row.get("to_number") or "")
        remote_number = resolve_remote_number(direction, from_number, to_number)

        analysis = analysis_by_call_id.get(call_id)
        recordings.append(
            {
                "id": str(row["id"]),
                "call_id": call_id,
                "call_history_id": "",
                "zoom_call_id": call_id if str(row.get("provider") or "").lower().startswith("zoom") else "",
                "date": str(row.get("timestamp") or ""),
                "duration": int(row.get("duration_seconds") or row.get("call_duration_seconds") or 0),
                "direction": direction,
                "from": from_number,
                "to": to_number,
                "remote_number": remote_number,
                "result": str(row.get("outcome") or ""),
                "recording_type": "automatic",
                "has_voicemail": str(row.get("outcome") or "").lower().startswith("voicemail"),
                "ai_call_summary_id": str(raw_payload.get("ai_call_summary_id") or ""),
                "download_url": str(raw_payload.get("download_url") or ""),
                "file_url": file_url or remote_file_url or None,
                "owner": str(raw_payload.get("zoom_owner_name") or row.get("operator") or row.get("user_id") or ""),
                "owner_extension": str(raw_payload.get("zoom_owner_extension") or ""),
                "source_endpoint": str(raw_payload.get("zoom_source_endpoint") or ""),
                "zoom_object_type": str(raw_payload.get("zoom_object_type") or ""),
                "recording_available": bool(remote_file_url or file_url),
                "playback_url": file_url or remote_file_url or None,
                "is_analyzed": bool(analysis),
                "analysis": analysis,
                "artifact_status": str((artifact_by_call_id.get(call_id) or {}).get("status") or ""),
                "ai_summary_status": str((artifact_by_call_id.get(call_id) or {}).get("ai_summary_status") or "unsupported"),
                "transcript_status": "unavailable",
                "ai_summary_body_status": "unavailable",
                "ai_summary_reference_only": bool(raw_payload.get("ai_call_summary_id")),
                "unmatched_reason": str(raw_payload.get("zoom_unmatched_reason") or ""),
            }
        )

    # SpeechCall entries not covered by call_log
    for row in speech_rows:
        call_id = str(row["id"])
        ext_id = str(row.get("external_call_id") or "")
        if call_id in seen_ids or (ext_id and ext_id in seen_ids):
            continue
        seen_ids.add(call_id)

        stored = bool(row.get("audio_storage_status") == "stored" and row.get("audio_uri"))
        file_url = f"/api/recordings/{call_id}/stream" if stored else None
        analysis = analysis_by_call_id.get(call_id) or (analysis_by_call_id.get(ext_id) if ext_id else None)
        recordings.append(
            {
                "id": call_id,
                "call_id": call_id,
                "call_history_id": "",
                "zoom_call_id": ext_id,
                "date": str(row.get("started_at") or ""),
                "duration": int(row.get("duration_seconds") or 0),
                "direction": str(row.get("direction") or ""),
                "from": "",
                "to": "",
                "remote_number": "",
                "result": str(row.get("outcome") or ""),
                "recording_type": "upload",
                "has_voicemail": False,
                "ai_call_summary_id": None,
                "download_url": None,
                "file_url": file_url,
                "owner": "",
                "owner_extension": "",
                "source_endpoint": "",
                "zoom_object_type": "",
                "recording_available": bool(file_url),
                "playback_url": file_url,
                "is_analyzed": bool(analysis),
                "analysis": analysis,
                "artifact_status": str((artifact_by_call_id.get(ext_id or call_id) or {}).get("status") or ""),
                "ai_summary_status": str((artifact_by_call_id.get(ext_id or call_id) or {}).get("ai_summary_status") or "unsupported"),
                "transcript_status": "unavailable",
                "ai_summary_body_status": "unavailable",
                "ai_summary_reference_only": False,
                "unmatched_reason": "",
            }
        )

    for call_id, artifact in artifact_by_call_id.items():
        if call_id in seen_ids:
            continue
        seen_ids.add(call_id)
        recordings.append(
            {
                "id": str(artifact.get("id") or artifact.get("external_id") or call_id),
                "call_id": call_id,
                "call_history_id": "",
                "zoom_call_id": call_id,
                "date": str(artifact.get("created_at") or artifact.get("discovered_at") or ""),
                "duration": 0,
                "direction": "",
                "from": "",
                "to": "",
                "remote_number": "",
                "result": "",
                "recording_type": str(artifact.get("artifact_type") or "recording"),
                "has_voicemail": False,
                "ai_call_summary_id": None,
                "download_url": artifact.get("download_url"),
                "file_url": (f"/api/recordings/{call_id}/stream" if artifact.get("storage_uri") else (artifact.get("file_url") or artifact.get("download_url"))),
                "owner": "",
                "owner_extension": "",
                "source_endpoint": "/phone/call_logs",
                "zoom_object_type": "phone_call_log",
                "recording_available": bool(artifact.get("download_url") or artifact.get("file_url") or artifact.get("storage_uri")),
                "playback_url": (f"/api/recordings/{call_id}/stream" if artifact.get("storage_uri") else (artifact.get("file_url") or artifact.get("download_url"))),
                "is_analyzed": False,
                "analysis": None,
                "artifact_status": str(artifact.get("status") or ""),
                "ai_summary_status": str(artifact.get("ai_summary_status") or "unsupported"),
                "transcript_status": "unavailable",
                "ai_summary_body_status": "unavailable",
                "ai_summary_reference_only": False,
                "unmatched_reason": str(artifact.get("unmatched_reason") or ""),
            }
        )

    # Zoom as silent enrichment — adds calls not yet in DB; never fails the endpoint
    try:
        phones_row = (
            await session.execute(
                text("SELECT contact_phones FROM leads WHERE id = :id LIMIT 1"),
                {"id": lead_id},
            )
        ).mappings().first()
        if phones_row:
            phones_raw = phones_row.get("contact_phones")
            phones: List[str] = (
                json.loads(phones_raw) if isinstance(phones_raw, str) else list(phones_raw or [])
            )
            if phones:
                account = await _resolve_zoom_account(session)
                for phone in phones[:2]:
                    normalized = _normalize_phone(phone)
                    res = _zoom_request(
                        account, "GET", f"/phone/call_logs?page_size=10&phone_number={normalized}"
                    )
                    if not res.get("ok"):
                        continue
                    for call in res["data"].get("call_logs", []):
                        zoom_call_id = str(call.get("id") or "")
                        if not zoom_call_id or zoom_call_id in seen_ids:
                            continue
                        seen_ids.add(zoom_call_id)
                        recording_meta: Optional[Dict[str, Any]] = None
                        if call.get("recording_id"):
                            rec_res = _zoom_request(
                                account, "GET", f"/phone/call_logs/{zoom_call_id}/recordings"
                            )
                            if rec_res.get("ok"):
                                recording_meta = rec_res.get("data") or {}
                        payload = call_to_recording_payload(call, recording_meta)
                        payload["is_analyzed"] = bool(analysis_by_call_id.get(zoom_call_id))
                        payload["analysis"] = analysis_by_call_id.get(zoom_call_id)
                        payload["artifact_status"] = str((artifact_by_call_id.get(zoom_call_id) or {}).get("status") or "")
                        payload["ai_summary_status"] = str((artifact_by_call_id.get(zoom_call_id) or {}).get("ai_summary_status") or "unsupported")
                        recordings.append(payload)
    except Exception:
        pass  # Zoom unavailable — DB data is sufficient

    recordings.sort(key=lambda item: str(item.get("date") or ""), reverse=True)
    return recordings


@router.get("/api/zoom/unmatched-calls")
async def get_unmatched_zoom_calls(
    days: int = 14,
    limit: int = 50,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    await ensure_zoom_recording_schema(session)
    day_count = max(1, min(int(days or 14), 30))
    row_limit = max(1, min(int(limit or 50), 200))
    cutoff = (date.today() - timedelta(days=day_count - 1)).isoformat()

    rows = (
        await session.execute(
            text(
                """
                SELECT id, provider_call_id, logged_at, direction, from_number, to_number, recording_url, raw_payload
                FROM call_log
                WHERE provider = 'zoom'
                  AND COALESCE(lead_id, '') = ''
                  AND logged_date >= :cutoff
                ORDER BY logged_at DESC
                LIMIT :limit
                """
            ),
            {"cutoff": cutoff, "limit": row_limit},
        )
    ).mappings().all()

    payload: List[Dict[str, Any]] = []
    for row in rows:
        raw_payload = json_dict(row.get("raw_payload"))
        direction = str(row.get("direction") or "")
        from_number = str(row.get("from_number") or "")
        to_number = str(row.get("to_number") or "")
        payload.append(
            {
                "id": str(row.get("id") or ""),
                "call_id": str(row.get("provider_call_id") or row.get("id") or ""),
                "timestamp": str(row.get("logged_at") or ""),
                "direction": direction,
                "remote_number": resolve_remote_number(direction, from_number, to_number),
                "owner": str(raw_payload.get("zoom_owner_name") or ""),
                "source_endpoint": str(raw_payload.get("zoom_source_endpoint") or ""),
                "recording_available": bool(row.get("recording_url") or raw_payload.get("download_url") or raw_payload.get("file_url")),
                "download_url": str(raw_payload.get("download_url") or ""),
                "playback_url": str(raw_payload.get("file_url") or raw_payload.get("download_url") or f"/api/recordings/{str(row.get('provider_call_id') or row.get('id') or '')}/stream"),
                "ai_call_summary_id": str(raw_payload.get("ai_call_summary_id") or ""),
                "ai_summary_body_status": "unavailable",
                "transcript_status": "unavailable",
                "unmatched_reason": str(raw_payload.get("zoom_unmatched_reason") or "phone_match_not_found"),
            }
        )

    return {"items": payload, "count": len(payload), "cutoff_date": cutoff}


@router.get("/api/recordings/{call_id}/stream")
async def stream_recording(
    call_id: str,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    from fastapi.responses import FileResponse
    
    # Try local first
    local_path = RECORDINGS_ROOT / f"call_{call_id}.mp3"
    legacy_path = RECORDINGS_ROOT / f"{call_id}.mp3"
    if local_path.exists():
        return FileResponse(local_path, media_type="audio/mpeg")
    if legacy_path.exists():
        return FileResponse(legacy_path, media_type="audio/mpeg")

    speech_row = (
        await session.execute(
            text("SELECT audio_uri FROM calls WHERE id = :call_id OR external_call_id = :call_id ORDER BY CASE WHEN id = :call_id THEN 0 ELSE 1 END LIMIT 1"),
            {"call_id": call_id},
        )
    ).mappings().first()
    if speech_row and speech_row.get("audio_uri"):
        speech_path = SPEECH_AUDIO_STORAGE_ROOT.parent / str(speech_row["audio_uri"])
        if speech_path.exists():
            return FileResponse(speech_path)

    call_row = (
        await session.execute(
            text(
                """
                SELECT id, provider_call_id, recording_url, raw_payload
                FROM call_log
                WHERE id = :call_id OR provider_call_id = :call_id
                ORDER BY CASE WHEN provider_call_id = :call_id THEN 0 ELSE 1 END
                LIMIT 1
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().first()
    provider_call_id = str((call_row or {}).get("provider_call_id") or call_id)
    call_raw_payload = json_dict((call_row or {}).get("raw_payload"))
    recording_url = recording_source_url(call_row, call_raw_payload)

    await ensure_zoom_recording_schema(session)
    artifact_row = (
        await session.execute(
            text(
                """
                SELECT storage_uri, file_url, download_url
                FROM zoom_recording_artifacts
                WHERE call_id = :call_id OR external_parent_id = :call_id OR external_id = :call_id
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """
            ),
            {"call_id": provider_call_id},
        )
    ).mappings().first()
    artifact_storage_uri = str((artifact_row or {}).get("storage_uri") or "").strip()
    if artifact_storage_uri:
        artifact_path = RECORDINGS_ROOT.parent / artifact_storage_uri
        if artifact_path.exists():
            return FileResponse(artifact_path, media_type="audio/mpeg")
    if not recording_url:
        recording_url = str(
            (artifact_row or {}).get("file_url") or (artifact_row or {}).get("download_url") or ""
        ).strip()

    try:
        account = await _resolve_zoom_account(session)
    except Exception:
        account = None

    if not recording_url and account:
        recording_res = _zoom_request(account, "GET", f"/phone/call_logs/{provider_call_id}/recordings")
        if recording_res.get("ok"):
            recording_payload = recording_res.get("data") or {}
            if isinstance(recording_payload, dict):
                recordings = recording_payload.get("recordings")
                if isinstance(recordings, list) and recordings:
                    recording_payload = recordings[0] if isinstance(recordings[0], dict) else {}
            elif isinstance(recording_payload, list):
                recording_payload = recording_payload[0] if recording_payload and isinstance(recording_payload[0], dict) else {}
            else:
                recording_payload = {}
            recording_url = str(
                recording_payload.get("file_url") or recording_payload.get("download_url") or recording_payload.get("recording_url") or ""
            ).strip()
            if recording_url and call_row:
                await session.execute(
                    text("UPDATE call_log SET recording_url = :recording_url WHERE id = :id"),
                    {"recording_url": recording_url, "id": str(call_row["id"])},
                )
                await session.commit()

    if recording_url and account:
        local_download = await download_recording_asset(recording_url, provider_call_id, account=account)
        if local_download:
            return FileResponse(local_download, media_type="audio/mpeg")

    raise HTTPException(status_code=404, detail="Local recording not found. Syncing may be required.")


@router.get("/api/recordings/{call_id}/download")
async def download_recording(
    call_id: str,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    account = await _resolve_zoom_account(session)
    recording_res = _zoom_request(account, "GET", f"/phone/call_logs/{call_id}/recordings")
    if not recording_res.get("ok"):
        raise HTTPException(
            status_code=recording_res.get("status") or 404,
            detail=recording_res.get("error") or "Recording not found",
        )
    file_url = str((recording_res.get("data") or {}).get("file_url") or "").strip()
    if not file_url:
        raise HTTPException(status_code=404, detail="Recording file URL is unavailable")
    return RedirectResponse(file_url)


@router.get("/api/leads/{lead_id}/sms-sessions")
async def get_lead_sms_sessions(
    lead_id: str,
    days: int = 30,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    lead = _hydrate_lead(await _get_lead_or_404(session, lead_id))
    phone_suffixes = lead_phone_suffixes(lead)
    if not phone_suffixes:
        return []

    account = await _resolve_zoom_account(session)
    today = date.today()
    start = today - timedelta(days=max(1, min(days, 90)))
    sessions_res = _zoom_request(
        account,
        "GET",
        f"/phone/sms/sessions?page_size=50&from={start.isoformat()}&to={today.isoformat()}",
    )
    if not sessions_res.get("ok"):
        return []

    matched_sessions: List[Dict[str, Any]] = []
    seen_session_ids: Set[str] = set()
    for session_summary in sessions_res.get("data", {}).get("sms_sessions", []) or []:
        summary_matches = any(
            phone_suffix(participant.get("phone_number")) in phone_suffixes
            for participant in session_summary.get("participants", []) or []
        )
        if not summary_matches:
            continue

        session_id = str(session_summary.get("session_id") or "")
        if not session_id or session_id in seen_session_ids:
            continue
        seen_session_ids.add(session_id)

        detail_res = _zoom_request(account, "GET", f"/phone/sms/sessions/{session_id}")
        if not detail_res.get("ok"):
            continue
        detail = detail_res.get("data") or {}
        detail["session_id"] = session_id
        detail["session_type"] = session_summary.get("session_type")
        detail["last_access_time"] = session_summary.get("last_access_time")
        detail["participants"] = session_summary.get("participants", [])
        if match_sms_session(detail, phone_suffixes):
            matched_sessions.append(session_preview(detail))

    matched_sessions.sort(key=lambda item: str(item.get("last_message_at") or item.get("last_access_time") or ""), reverse=True)
    return matched_sessions


@router.get("/api/leads/{lead_id}/call-history")
async def get_lead_call_history(
    lead_id: str,
    limit: int = 50,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """DB-backed call history for a lead.

    Joins call_log (manual + Zoom-synced logs) with call_analysis (Gemini understanding)
    and calls (SpeechCall records with transcript/pipeline status). Does not hit Zoom API.
    Returns entries sorted newest-first.
    """
    # 1. Call log rows (manual logs + Zoom sync)
    log_rows = (
        await session.execute(
            text(
                """
                SELECT
                    id, lead_id, user_id, outcome, connected, timestamp, call_duration_seconds,
                    duration_seconds, note, operator, logged_at, logged_date,
                    next_action_due, provider, provider_call_id, direction,
                    from_number, to_number
                FROM call_log
                WHERE lead_id = :lead_id
                ORDER BY timestamp DESC, logged_at DESC
                LIMIT :limit
                """
            ),
            {"lead_id": lead_id, "limit": limit},
        )
    ).mappings().all()

    # 2. SpeechCall rows for this lead (may exist from upload or process_zoom path)
    call_rows = (
        await session.execute(
            text(
                """
                SELECT
                    id, external_call_id, source, lead_id, rep_id, call_type,
                    direction, outcome, started_at, ended_at, duration_seconds,
                    recording_id, audio_uri, audio_storage_status,
                    analysis_status, transcript_status, diarization_status
                FROM calls
                WHERE lead_id = :lead_id
                ORDER BY started_at DESC
                LIMIT :limit
                """
            ),
            {"lead_id": lead_id, "limit": limit},
        )
    ).mappings().all()

    # 3. Analysis rows — keyed by call_id. Fetch all at once for the lead.
    analysis_rows = (
        await session.execute(
            text(
                """
                SELECT call_id, summary, outcome, key_topics, objections,
                       next_step, suggested_follow_up_task,
                       sentiment_label, sentiment_confidence, sentiment_reason,
                       overall_confidence, analyzed_at, provider
                FROM call_analysis
                WHERE lead_id = :lead_id
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().all()
    analysis_by_call_id: Dict[str, Dict[str, Any]] = {}
    for row in analysis_rows:
        call_id_key = str(row["call_id"])
        try:
            key_topics = json.loads(row.get("key_topics") or "[]")
        except (json.JSONDecodeError, TypeError):
            key_topics = []
        try:
            objections = json.loads(row.get("objections") or "[]")
        except (json.JSONDecodeError, TypeError):
            objections = []
        analysis_by_call_id[call_id_key] = {
            "summary": row.get("summary") or "",
            "outcome": row.get("outcome") or "unknown",
            "key_topics": key_topics,
            "objections": objections,
            "next_step": row.get("next_step") or "",
            "suggested_follow_up_task": row.get("suggested_follow_up_task") or "",
            "sentiment_label": row.get("sentiment_label") or "",
            "sentiment_confidence": float(row.get("sentiment_confidence") or 0),
            "sentiment_reason": row.get("sentiment_reason") or "",
            "overall_confidence": float(row.get("overall_confidence") or 0),
            "analyzed_at": row.get("analyzed_at"),
            "provider": row.get("provider") or "",
        }

    # 4. Transcript availability — keyed by call_id
    transcript_call_ids = list({str(r["id"]) for r in call_rows})
    transcript_status_by_call_id: Dict[str, bool] = {}
    if transcript_call_ids:
        placeholders = ", ".join(f":cid_{i}" for i in range(len(transcript_call_ids)))
        params = {f"cid_{i}": cid for i, cid in enumerate(transcript_call_ids)}
        transcript_rows = (
            await session.execute(
                text(f"SELECT call_id FROM transcripts WHERE call_id IN ({placeholders}) AND status = 'completed'"),
                params,
            )
        ).mappings().all()
        for row in transcript_rows:
            transcript_status_by_call_id[str(row["call_id"])] = True

    # 5. Build combined entries
    entries: List[Dict[str, Any]] = []

    # From call_log — include provider_call_id so analysis can be looked up
    for row in log_rows:
        call_id_key = str(row.get("provider_call_id") or row["id"])
        analysis = analysis_by_call_id.get(call_id_key)
        entries.append(
            {
                "entry_type": "call_log",
                "id": str(row["id"]),
                "call_id": call_id_key,
                "source": str(row.get("provider") or "manual"),
                "direction": str(row.get("direction") or ""),
                "outcome": str(row.get("outcome") or ""),
                "connected": bool(row.get("connected")),
                "duration_seconds": int(row.get("duration_seconds") or row.get("call_duration_seconds") or 0),
                "timestamp": str(row.get("timestamp") or row.get("logged_at") or ""),
                "note": str(row.get("note") or ""),
                "operator": str(row.get("operator") or row.get("user_id") or ""),
                "next_action_due": row.get("next_action_due"),
                "has_analysis": bool(analysis),
                "analysis": analysis,
                "has_transcript": transcript_status_by_call_id.get(call_id_key, False),
            }
        )

    # From SpeechCall — add entries not already covered by call_log (dedup by external_call_id)
    log_call_ids = {str(r.get("provider_call_id") or r["id"]) for r in log_rows}
    for row in call_rows:
        call_id_key = str(row["id"])
        if call_id_key in log_call_ids or str(row.get("external_call_id") or "") in log_call_ids:
            continue
        analysis = analysis_by_call_id.get(call_id_key) or analysis_by_call_id.get(str(row.get("external_call_id") or ""))
        entries.append(
            {
                "entry_type": "speech_call",
                "id": call_id_key,
                "call_id": call_id_key,
                "source": str(row.get("source") or ""),
                "direction": str(row.get("direction") or ""),
                "outcome": str(row.get("outcome") or ""),
                "connected": row.get("outcome") not in ("missed", "voicemail", "no_answer"),
                "duration_seconds": int(row.get("duration_seconds") or 0),
                "timestamp": str(row.get("started_at") or ""),
                "note": "",
                "operator": str(row.get("rep_id") or ""),
                "next_action_due": None,
                "has_analysis": bool(analysis),
                "analysis": analysis,
                "has_transcript": transcript_status_by_call_id.get(call_id_key, False),
                "audio_uri": str(row.get("audio_uri") or ""),
                "audio_storage_status": str(row.get("audio_storage_status") or ""),
                "file_url": (
                    f"/api/recordings/{call_id_key}/stream"
                    if row.get("audio_storage_status") == "stored" and row.get("audio_uri")
                    else None
                ),
                "analysis_status": str(row.get("analysis_status") or ""),
                "transcript_status": str(row.get("transcript_status") or ""),
            }
        )

    entries.sort(key=lambda e: str(e.get("timestamp") or ""), reverse=True)
    return entries[:limit]


@router.get("/api/calls/lead-summaries")
async def get_call_lead_summaries(
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """Batch call stats per lead for CommandLedger display.

    Returns a dict keyed by lead_id with total_calls, has_transcript, has_recording,
    latest_analysis_snippet, last_objection, booking_attempted, next_step_detected.
    Uses batch queries — no N+1.
    """
    log_counts = (
        await session.execute(
            text("SELECT lead_id, COUNT(*) as total_calls FROM call_log GROUP BY lead_id")
        )
    ).mappings().all()

    transcript_leads = (
        await session.execute(
            text(
                """
                SELECT DISTINCT c.lead_id
                FROM calls c
                JOIN transcripts t ON t.call_id = c.id AND t.status = 'completed'
                """
            )
        )
    ).mappings().all()

    recording_leads = (
        await session.execute(
            text(
                """
                SELECT DISTINCT lead_id FROM calls
                WHERE audio_uri IS NOT NULL AND audio_uri != ''
                  AND audio_storage_status = 'stored'
                """
            )
        )
    ).mappings().all()

    # Latest call_log row per lead — for booking_attempted, next_step_detected, objection_tags
    latest_call_log_res = await session.execute(
        text(
            """
            SELECT cl.lead_id, cl.booking_attempted, cl.next_step_detected, cl.objection_tags
            FROM call_log cl
            INNER JOIN (
                SELECT lead_id, MAX(logged_at) AS max_logged_at
                FROM call_log
                GROUP BY lead_id
            ) latest ON latest.lead_id = cl.lead_id AND latest.max_logged_at = cl.logged_at
            """
        )
    )
    latest_call_by_lead: Dict[str, Any] = {}
    for r in latest_call_log_res.mappings().all():
        latest_call_by_lead[str(r["lead_id"])] = dict(r)

    # Latest call_analysis summary per lead — use call_analysis table (raw DDL, no SQLModel)
    # Join via call_log.provider_call_id → call_analysis.call_id
    analysis_snippet_by_lead: Dict[str, str] = {}
    try:
        analysis_res = await session.execute(
            text(
                """
                SELECT cl.lead_id, ca.summary
                FROM call_analysis ca
                INNER JOIN (
                    SELECT lead_id, MAX(logged_at) AS max_logged_at
                    FROM call_log
                    WHERE provider_call_id IS NOT NULL AND provider_call_id != ''
                    GROUP BY lead_id
                ) latest_cl ON 1=1
                INNER JOIN call_log cl
                    ON cl.lead_id = latest_cl.lead_id
                   AND cl.logged_at = latest_cl.max_logged_at
                   AND cl.provider_call_id IS NOT NULL
                WHERE ca.call_id = cl.provider_call_id
                  AND ca.summary IS NOT NULL
                """
            )
        )
        for r in analysis_res.mappings().all():
            lid = str(r["lead_id"])
            summary = str(r.get("summary") or "").strip()
            if summary and lid not in analysis_snippet_by_lead:
                analysis_snippet_by_lead[lid] = summary[:120]
    except Exception:
        # call_analysis table may not exist in all environments
        pass

    transcript_set = {str(r["lead_id"]) for r in transcript_leads}
    recording_set = {str(r["lead_id"]) for r in recording_leads}

    result: Dict[str, Any] = {}
    for row in log_counts:
        lid = str(row["lead_id"])
        latest_cl = latest_call_by_lead.get(lid, {})

        # Parse objection_tags safely
        raw_objections = latest_cl.get("objection_tags")
        last_objection: Optional[str] = None
        if raw_objections:
            if isinstance(raw_objections, list):
                tags = raw_objections
            else:
                try:
                    tags = json.loads(str(raw_objections))
                except Exception:
                    tags = []
            if tags and isinstance(tags, list):
                first = tags[0]
                if isinstance(first, dict):
                    last_objection = str(first.get("normalized_text") or first.get("text") or "").strip() or None
                elif isinstance(first, str):
                    last_objection = first.strip() or None

        result[lid] = {
            "total_calls": int(row["total_calls"]),
            "has_transcript": lid in transcript_set,
            "has_recording": lid in recording_set,
            "latest_analysis_snippet": analysis_snippet_by_lead.get(lid),
            "last_objection": last_objection,
            "booking_attempted": bool(latest_cl.get("booking_attempted")),
            "next_step_detected": bool(latest_cl.get("next_step_detected")),
        }
    return result


@router.get("/api/calls/{call_id}/voice-analysis")
async def get_voice_analysis(
    call_id: str,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """Fetch existing voice trainer report for a call."""
    row = (
        await session.execute(
            text("SELECT * FROM voice_trainer_reports WHERE call_id = :call_id LIMIT 1"),
            {"call_id": call_id},
        )
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="No voice analysis — POST to generate")

    from services.voice_trainer_service import _row_to_payload
    return _row_to_payload(dict(row))


@router.post("/api/calls/{call_id}/voice-analysis")
async def trigger_voice_analysis(
    call_id: str,
    force: bool = False,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """Run voice trainer analysis for a call. Cached — use force=true to re-run."""
    from services.voice_trainer_service import run_voice_trainer
    result = await run_voice_trainer(session, call_id, force=force)
    if result.get("error") == "no_transcript":
        raise HTTPException(status_code=404, detail="No transcript found for this call")
    return result


@router.get("/api/calls/{call_id}/transcript")
async def get_call_transcript(
    call_id: str,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """Lightweight transcript + segments for a call. Does not load scores or coaching."""
    transcript_row = (
        await session.execute(
            text(
                """
                SELECT id, call_id, provider, version_type, language,
                       full_text, confidence, status, created_at
                FROM transcripts
                WHERE call_id = :call_id AND status = 'completed'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().first()

    if not transcript_row:
        raise HTTPException(status_code=404, detail="No completed transcript for this call")

    transcript_id = str(transcript_row["id"])
    segment_rows = (
        await session.execute(
            text(
                """
                SELECT id, call_id, speaker_id, turn_index, start_ms, end_ms,
                       text, overlap_flag, segment_type, confidence
                FROM call_segments
                WHERE call_id = :call_id
                ORDER BY start_ms ASC, turn_index ASC
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().all()

    speaker_rows = (
        await session.execute(
            text(
                """
                SELECT id, diarization_label, role, display_name, linked_rep_id, confidence
                FROM speakers
                WHERE call_id = :call_id
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().all()
    speaker_map = {str(r["id"]): dict(r) for r in speaker_rows}

    segments = []
    for row in segment_rows:
        speaker = speaker_map.get(str(row.get("speaker_id") or ""), {})
        segments.append(
            {
                "id": str(row["id"]),
                "turn_index": int(row.get("turn_index") or 0),
                "start_ms": int(row.get("start_ms") or 0),
                "end_ms": int(row.get("end_ms") or 0),
                "text": str(row.get("text") or ""),
                "confidence": float(row.get("confidence") or 0.0),
                "overlap_flag": bool(row.get("overlap_flag")),
                "segment_type": str(row.get("segment_type") or "turn"),
                "speaker_role": str(speaker.get("role") or "unknown"),
                "speaker_label": str(speaker.get("diarization_label") or ""),
            }
        )

    return {
        "call_id": call_id,
        "transcript": {
            "id": transcript_id,
            "provider": str(transcript_row.get("provider") or ""),
            "version_type": str(transcript_row.get("version_type") or ""),
            "language": str(transcript_row.get("language") or ""),
            "full_text": str(transcript_row.get("full_text") or ""),
            "confidence": float(transcript_row.get("confidence") or 0.0),
            "created_at": transcript_row.get("created_at"),
        },
        "segments": segments,
        "speakers": [dict(r) for r in speaker_rows],
    }


@router.post("/api/calls/{call_id}/transcript")
async def post_call_transcript(
    call_id: str,
    body: CallTranscriptIngestRequest,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    from services.transcript_service import ingest_transcript_with_session

    try:
        result = await ingest_transcript_with_session(session, call_id, body.transcript)
        await session.commit()
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.get("/api/leads/{lead_id}/call-summary")
async def get_lead_call_summary(
    lead_id: str,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """Lightweight per-lead aggregation from call_log only. No Zoom dependency."""
    from services.zoom_call_sync_service import ensure_call_log_schema
    await ensure_call_log_schema(session)

    agg = (
        await session.execute(
            text(
                """
                SELECT
                    COUNT(*)                              AS total_calls,
                    SUM(CASE WHEN COALESCE(connected, FALSE) THEN 1 ELSE 0 END) AS connect_count,
                    SUM(CASE WHEN COALESCE(connected, FALSE) AND COALESCE(duration_seconds, call_duration_seconds, 0) >= 10
                             THEN 1 ELSE 0 END)          AS conversation_count,
                    SUM(COALESCE(duration_seconds, call_duration_seconds, 0)) AS total_talk_time_seconds,
                    MAX(COALESCE(timestamp, logged_at))  AS last_call_at,
                    MAX(outcome)                         AS last_outcome_agg
                FROM call_log
                WHERE lead_id = :lead_id
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().first()

    # last_outcome is from the most-recent row, not MAX(outcome)
    last_row = (
        await session.execute(
            text(
                """
                SELECT outcome, COALESCE(timestamp, logged_at) AS ts
                FROM call_log
                WHERE lead_id = :lead_id
                ORDER BY COALESCE(timestamp, logged_at) DESC, logged_at DESC
                LIMIT 1
                """
            ),
            {"lead_id": lead_id},
        )
    ).mappings().first()

    # transcript availability — any call_log row with a matching transcript
    has_transcript = False
    if agg and int(agg["total_calls"] or 0) > 0:
        tr = (
            await session.execute(
                text(
                    """
                    SELECT 1 FROM transcripts t
                    JOIN call_log cl ON cl.provider_call_id = t.call_id OR cl.id = t.call_id
                    WHERE cl.lead_id = :lead_id AND t.status = 'completed'
                    LIMIT 1
                    """
                ),
                {"lead_id": lead_id},
            )
        ).first()
        has_transcript = tr is not None

    return {
        "lead_id": lead_id,
        "total_calls": int(agg["total_calls"] or 0) if agg else 0,
        "connect_count": int(agg["connect_count"] or 0) if agg else 0,
        "conversation_count": int(agg["conversation_count"] or 0) if agg else 0,
        "total_talk_time_seconds": int(agg["total_talk_time_seconds"] or 0) if agg else 0,
        "last_call_at": str(last_row["ts"] or "") if last_row else None,
        "last_outcome": str(last_row["outcome"] or "") if last_row else None,
        "has_transcript": has_transcript,
    }


@router.patch("/api/calls/{log_id}/attach")
async def attach_call_data(
    log_id: str,
    body: Dict[str, Any],
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """Attach recording_url, transcript, and/or summary to an existing call_log row."""
    from services.zoom_call_sync_service import ensure_call_log_schema
    await ensure_call_log_schema(session)

    row = (
        await session.execute(text("SELECT id FROM call_log WHERE id = :id"), {"id": log_id})
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Call log entry not found")

    updates: Dict[str, Any] = {}
    for field in ("recording_url", "transcript", "summary"):
        if field in body and body[field] is not None:
            updates[field] = str(body[field])
    if "intent_signal" in body and body["intent_signal"] is not None:
        updates["intent_signal"] = float(body["intent_signal"])
    if "booking_attempted" in body:
        updates["booking_attempted"] = 1 if body["booking_attempted"] else 0
    if "next_step_detected" in body:
        updates["next_step_detected"] = 1 if body["next_step_detected"] else 0
    if "objection_tags" in body and body["objection_tags"] is not None:
        updates["objection_tags"] = json.dumps(body["objection_tags"])

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["id"] = log_id
    await session.execute(
        text(f"UPDATE call_log SET {set_clause} WHERE id = :id"),
        updates,
    )
    await session.commit()

    updated = (
        await session.execute(
            text(
                "SELECT id, outcome, recording_url, summary, intent_signal, booking_attempted, objection_tags, next_step_detected FROM call_log WHERE id = :id"
            ),
            {"log_id": log_id, "id": log_id},
        )
    ).mappings().first()
    return {"status": "ok", "row": dict(updated) if updated else {}}
