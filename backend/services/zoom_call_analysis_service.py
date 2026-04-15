import base64
import json
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional

import httpx
from sqlalchemy import text

from services.call_intelligence_service import derive_signals_from_analysis
from services.metrics_service import build_call_log_row, insert_call_log_row
from services.zoom_call_sync_service import ensure_call_log_schema, normalize_zoom_call_entry

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
_MAX_AUDIO_BYTES = 18 * 1024 * 1024

_ANALYSIS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "outcome": {"type": "string"},
        "key_topics": {"type": "array", "items": {"type": "string"}},
        "objections": {"type": "array", "items": {"type": "string"}},
        "next_step": {"type": "string"},
        "suggested_follow_up_task": {"type": "string"},
        "overall_confidence": {"type": "number"},
        "sentiment_label": {"type": "string"},
        "sentiment_confidence": {"type": "number"},
        "sentiment_reason": {"type": "string"},
    },
    "required": [
        "summary",
        "outcome",
        "key_topics",
        "objections",
        "next_step",
        "suggested_follow_up_task",
        "overall_confidence",
        "sentiment_label",
        "sentiment_confidence",
        "sentiment_reason",
    ],
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_gemini_text(data: Dict[str, Any]) -> str:
    candidates = data.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return "".join(part.get("text", "") for part in parts if part.get("text")).strip()


def _strip_json_fences(payload: str) -> str:
    cleaned = payload.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:]
    return cleaned.strip()


def _guess_audio_mime_type(recording_meta: Dict[str, Any], content_type: str) -> str:
    if content_type and content_type != "application/octet-stream":
        return content_type
    file_url = str(recording_meta.get("file_url") or "").lower()
    if file_url.endswith(".wav"):
        return "audio/wav"
    return "audio/mpeg"


async def _download_recording_bytes(recording_meta: Dict[str, Any]) -> tuple[bytes, str]:
    file_url = str(recording_meta.get("file_url") or "").strip()
    if not file_url:
        raise ValueError("Recording file URL is unavailable")

    async with httpx.AsyncClient(timeout=90.0, follow_redirects=True) as client:
        response = await client.get(file_url)
        response.raise_for_status()
        audio_bytes = response.content
        if not audio_bytes:
            raise ValueError("Recording file is empty")
        if len(audio_bytes) > _MAX_AUDIO_BYTES:
            raise ValueError("Recording is too large for inline Gemini analysis")
        mime_type = _guess_audio_mime_type(recording_meta, response.headers.get("content-type", ""))
        return audio_bytes, mime_type


async def _call_gemini_audio_json(
    *,
    audio_bytes: bytes,
    mime_type: str,
    prompt: str,
    system: str,
) -> Dict[str, Any]:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("GEMINI_API_KEY is not configured")

    audio_b64 = base64.b64encode(audio_bytes).decode("ascii")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{_DEFAULT_MODEL}:generateContent"
    payload = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": prompt},
                    {"inlineData": {"mimeType": mime_type, "data": audio_b64}},
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1200,
            "responseMimeType": "application/json",
            "responseJsonSchema": _ANALYSIS_SCHEMA,
        },
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        ],
    }
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            url,
            headers={
                "x-goog-api-key": api_key,
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        text_payload = _extract_gemini_text(response.json())
    if not text_payload:
        raise ValueError("Gemini returned an empty analysis")
    parsed = json.loads(_strip_json_fences(text_payload))
    return parsed if isinstance(parsed, dict) else {}


def _sanitize_sentiment(payload: Dict[str, Any]) -> Dict[str, Any]:
    sentiment_confidence = float(payload.get("sentiment_confidence") or 0)
    if sentiment_confidence < 0.7:
        payload["sentiment_label"] = ""
        payload["sentiment_confidence"] = 0
        payload["sentiment_reason"] = ""
    return payload


def _build_analysis_prompt(lead: Dict[str, Any], call_meta: Dict[str, Any]) -> tuple[str, str]:
    system = (
        "You analyze sales and service calls for CRM use. "
        "Return concise, factual JSON only. "
        "Do not invent names, prices, or commitments that are not clearly present. "
        "Sentiment must only be labeled when confidence is strong; otherwise use an empty string and 0 confidence."
    )
    prompt = (
        "Analyze this Zoom Phone call recording for a real-estate/mortgage CRM.\n\n"
        f"Lead owner: {lead.get('owner_name') or 'Unknown'}\n"
        f"Lead address: {lead.get('address') or 'Unknown'}\n"
        f"Lead suburb: {lead.get('suburb') or 'Unknown'}\n"
        f"Call direction: {call_meta.get('direction') or 'Unknown'}\n"
        f"Call result: {call_meta.get('result') or 'Unknown'}\n"
        f"Call duration seconds: {call_meta.get('duration') or 0}\n\n"
        "Populate:\n"
        "- summary: 2-4 sentence factual recap\n"
        "- outcome: one of answered, voicemail, missed, qualified, follow_up_required, not_interested, unknown\n"
        "- key_topics: short list\n"
        "- objections: short list, empty if none\n"
        "- next_step: best next action\n"
        "- suggested_follow_up_task: operator task phrased as one action\n"
        "- overall_confidence: 0 to 1\n"
        "- sentiment_label: positive, neutral, negative, or empty string if uncertain\n"
        "- sentiment_confidence: 0 to 1\n"
        "- sentiment_reason: brief evidence for the sentiment if confident, else empty string"
    )
    return system, prompt


def _analysis_row(call_id: str, lead_id: str, call_meta: Dict[str, Any], recording_meta: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
    now = _now_iso()
    return {
        "call_id": call_id,
        "lead_id": lead_id,
        "provider": "zoom_phone",
        "recording_id": str(recording_meta.get("id") or call_meta.get("recording_id") or ""),
        "ai_call_summary_id": str(call_meta.get("ai_call_summary_id") or ""),
        "status": "completed",
        "summary": str(analysis.get("summary") or ""),
        "outcome": str(analysis.get("outcome") or "unknown"),
        "key_topics": json.dumps(analysis.get("key_topics") or []),
        "objections": json.dumps(analysis.get("objections") or []),
        "next_step": str(analysis.get("next_step") or ""),
        "suggested_follow_up_task": str(analysis.get("suggested_follow_up_task") or ""),
        "sentiment_label": str(analysis.get("sentiment_label") or ""),
        "sentiment_confidence": float(analysis.get("sentiment_confidence") or 0),
        "sentiment_reason": str(analysis.get("sentiment_reason") or ""),
        "overall_confidence": float(analysis.get("overall_confidence") or 0),
        "error_message": "",
        "raw_payload": json.dumps(
            {
                "call_meta": call_meta,
                "recording_meta": {
                    "id": recording_meta.get("id"),
                    "call_log_id": recording_meta.get("call_log_id"),
                    "recording_type": recording_meta.get("recording_type"),
                    "duration": recording_meta.get("duration"),
                    "date_time": recording_meta.get("date_time"),
                },
                "analysis": analysis,
            }
        ),
        "analyzed_at": now,
        "created_at": now,
        "updated_at": now,
    }


async def _ensure_zoom_call_log_row(
    session: "AsyncSession",
    *,
    lead: Dict[str, Any],
    call_id: str,
    call_meta: Dict[str, Any],
    recording_meta: Dict[str, Any],
) -> str:
    await ensure_call_log_schema(session)
    normalized = normalize_zoom_call_entry(call_meta, "") or {}
    recording_url = str(recording_meta.get("file_url") or recording_meta.get("download_url") or "").strip() or None
    existing_row = (
        await session.execute(
            text(
                """
                SELECT id
                FROM call_log
                WHERE provider = 'zoom' AND provider_call_id = :provider_call_id
                LIMIT 1
                """
            ),
            {"provider_call_id": call_id},
        )
    ).mappings().first()

    row_payload = build_call_log_row(
        row_id=str((existing_row or {}).get("id") or ""),
        lead_id=str(lead.get("id") or ""),
        lead_address=str(lead.get("address") or ""),
        outcome=str(normalized.get("outcome") or "unknown"),
        connected=bool(normalized.get("connected")),
        call_duration_seconds=int(normalized.get("duration_seconds") or call_meta.get("duration") or recording_meta.get("duration") or 0),
        note=str(normalized.get("note") or call_meta.get("result") or ""),
        user_id=str(normalized.get("user_id") or "Zoom"),
        timestamp=str(normalized.get("timestamp") or call_meta.get("date_time") or call_meta.get("start_time") or _now_iso()),
        next_action_due=None,
        provider="zoom",
        provider_call_id=call_id,
        direction=str(normalized.get("direction") or call_meta.get("direction") or "outbound"),
        from_number=str(normalized.get("from_number") or call_meta.get("caller_did_number") or call_meta.get("caller_number") or ""),
        to_number=str(normalized.get("to_number") or call_meta.get("callee_did_number") or call_meta.get("callee_number") or ""),
        raw_payload=json.dumps(call_meta, ensure_ascii=True),
        recording_url=recording_url,
    )
    if existing_row:
        await session.execute(
            text(
                """
                UPDATE call_log
                SET lead_id = :lead_id,
                    lead_address = :lead_address,
                    user_id = :user_id,
                    outcome = :outcome,
                    connected = :connected,
                    timestamp = :timestamp,
                    call_duration_seconds = :call_duration_seconds,
                    duration_seconds = :duration_seconds,
                    note = :note,
                    operator = :operator,
                    logged_at = :logged_at,
                    logged_date = :logged_date,
                    provider = :provider,
                    provider_call_id = :provider_call_id,
                    direction = :direction,
                    from_number = :from_number,
                    to_number = :to_number,
                    raw_payload = :raw_payload,
                    recording_url = :recording_url
                WHERE id = :id
                """
            ),
            row_payload,
        )
        return str(existing_row["id"])

    await insert_call_log_row(session, row_payload)
    return str(row_payload["id"])


async def _sync_call_log_analysis_fields(
    session: "AsyncSession",
    *,
    call_log_id: str,
    call_id: str,
    recording_meta: Dict[str, Any],
) -> None:
    analysis_row = (
        await session.execute(
            text(
                """
                SELECT summary, outcome, objections, next_step, raw_payload
                FROM call_analysis
                WHERE call_id = :call_id
                LIMIT 1
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().first()
    if not analysis_row:
        return

    transcript_row = (
        await session.execute(
            text(
                """
                SELECT full_text
                FROM transcripts
                WHERE call_id = :call_id
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().first()
    try:
        raw_payload = json.loads(analysis_row.get("raw_payload") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        raw_payload = {}
    try:
        objections = json.loads(analysis_row.get("objections") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        objections = []
    signals = derive_signals_from_analysis(
        summary=str(analysis_row.get("summary") or ""),
        outcome=str(analysis_row.get("outcome") or ""),
        next_step=str(analysis_row.get("next_step") or ""),
        objections=objections,
        sales_analysis=raw_payload.get("sales_analysis") if isinstance(raw_payload, dict) else {},
    )
    await session.execute(
        text(
            """
            UPDATE call_log
            SET transcript = COALESCE(:transcript, transcript),
                summary = :summary,
                intent_signal = :intent_signal,
                booking_attempted = :booking_attempted,
                objection_tags = :objection_tags,
                next_step_detected = :next_step_detected,
                recording_url = COALESCE(:recording_url, recording_url)
            WHERE id = :id OR provider_call_id = :provider_call_id
            """
        ),
        {
            "id": call_log_id,
            "provider_call_id": call_id,
            "transcript": str((transcript_row or {}).get("full_text") or "").strip() or None,
            "summary": str(signals["summary"]),
            "intent_signal": float(signals["intent_signal"]),
            "booking_attempted": 1 if signals["booking_attempted"] else 0,
            "objection_tags": json.dumps(signals["objection_tags"]),
            "next_step_detected": 1 if signals["next_step_detected"] else 0,
            "recording_url": str(recording_meta.get("file_url") or recording_meta.get("download_url") or "").strip() or None,
        },
    )


async def analyze_zoom_call_for_lead(session: "AsyncSession", lead_id: str, call_id: str) -> Dict[str, Any]:
    from core.logic import (
        _append_activity,
        _append_stage_note,
        _build_activity_entry,
        _hydrate_lead,
        _resolve_zoom_account,
    )
    from core.utils import _decode_row, now_iso
    from services.integrations import _zoom_request
    from services.speech_pipeline_service import process_zoom_recorded_call

    lead_row = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    ).mappings().first()
    if not lead_row:
        raise ValueError("Lead not found")
    lead = _decode_row(lead_row)

    account = await _resolve_zoom_account(session)
    call_res = _zoom_request(account, "GET", f"/phone/call_logs/{call_id}")
    if not call_res.get("ok"):
        raise ValueError("Zoom call log lookup failed")
    call_meta = call_res.get("data") or {}

    recording_res = _zoom_request(account, "GET", f"/phone/call_logs/{call_id}/recordings")
    if not recording_res.get("ok"):
        raise ValueError("Zoom recording lookup failed")
    recording_meta = recording_res.get("data") or {}

    call_log_id = await _ensure_zoom_call_log_row(
        session,
        lead=lead,
        call_id=call_id,
        call_meta=call_meta,
        recording_meta=recording_meta,
    )

    audio_bytes, mime_type = await _download_recording_bytes(recording_meta)
    system, prompt = _build_analysis_prompt(lead, call_meta)
    analysis = _sanitize_sentiment(await _call_gemini_audio_json(audio_bytes=audio_bytes, mime_type=mime_type, prompt=prompt, system=system))

    row = _analysis_row(call_id, lead_id, call_meta, recording_meta, analysis)
    await session.execute(
        text(
            """
            INSERT INTO call_analysis (
                call_id, lead_id, provider, recording_id, ai_call_summary_id, status,
                summary, outcome, key_topics, objections, next_step, suggested_follow_up_task,
                sentiment_label, sentiment_confidence, sentiment_reason, overall_confidence,
                error_message, raw_payload, analyzed_at, created_at, updated_at
            ) VALUES (
                :call_id, :lead_id, :provider, :recording_id, :ai_call_summary_id, :status,
                :summary, :outcome, :key_topics, :objections, :next_step, :suggested_follow_up_task,
                :sentiment_label, :sentiment_confidence, :sentiment_reason, :overall_confidence,
                :error_message, :raw_payload, :analyzed_at, :created_at, :updated_at
            )
            ON CONFLICT(call_id) DO UPDATE SET
                lead_id=excluded.lead_id,
                provider=excluded.provider,
                recording_id=excluded.recording_id,
                ai_call_summary_id=excluded.ai_call_summary_id,
                status=excluded.status,
                summary=excluded.summary,
                outcome=excluded.outcome,
                key_topics=excluded.key_topics,
                objections=excluded.objections,
                next_step=excluded.next_step,
                suggested_follow_up_task=excluded.suggested_follow_up_task,
                sentiment_label=excluded.sentiment_label,
                sentiment_confidence=excluded.sentiment_confidence,
                sentiment_reason=excluded.sentiment_reason,
                overall_confidence=excluded.overall_confidence,
                error_message=excluded.error_message,
                raw_payload=excluded.raw_payload,
                analyzed_at=excluded.analyzed_at,
                updated_at=excluded.updated_at
            """
        ),
        row,
    )
    await process_zoom_recorded_call(
        session,
        lead_id=lead_id,
        call_id=call_id,
        call_meta=call_meta,
        recording_meta=recording_meta,
        legacy_analysis=analysis,
        audio_bytes=audio_bytes,
        mime_type=mime_type,
    )
    await _sync_call_log_analysis_fields(
        session,
        call_log_id=call_log_id,
        call_id=call_id,
        recording_meta=recording_meta,
    )

    analysis_summary = str(analysis.get("summary") or "")
    stage_note_history = _append_stage_note(
        lead.get("stage_note_history"),
        analysis_summary,
        lead.get("status") or "captured",
        "zoom_call_analysis",
        "Zoom call analysis",
        call_meta.get("callee_did_number") or call_meta.get("callee_number") or "",
    )
    activity_log = _append_activity(
        lead.get("activity_log"),
        _build_activity_entry(
            "call_analyzed",
            analysis_summary,
            lead.get("status"),
            "zoom",
            "Zoom call analysis",
            call_meta.get("callee_did_number") or call_meta.get("callee_number") or "",
        ),
    )
    await session.execute(
        text(
            """
            UPDATE leads
            SET stage_note_history = :stage_note_history,
                activity_log = :activity_log,
                updated_at = :updated_at
            WHERE id = :lead_id
            """
        ),
        {
            "stage_note_history": json.dumps(stage_note_history),
            "activity_log": json.dumps(activity_log),
            "updated_at": now_iso(),
            "lead_id": lead_id,
        },
    )
    await session.commit()

    refreshed_lead_row = (
        await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    ).mappings().first()
    refreshed_lead = _hydrate_lead(refreshed_lead_row) if refreshed_lead_row else None
    parsed_key_topics = analysis.get("key_topics") or []
    parsed_objections = analysis.get("objections") or []
    return {
        "call_id": call_id,
        "lead_id": lead_id,
        "recording_id": row["recording_id"],
        "status": "completed",
        "analysis": {
            "summary": row["summary"],
            "outcome": row["outcome"],
            "key_topics": parsed_key_topics,
            "objections": parsed_objections,
            "next_step": row["next_step"],
            "suggested_follow_up_task": row["suggested_follow_up_task"],
            "sentiment_label": row["sentiment_label"],
            "sentiment_confidence": row["sentiment_confidence"],
            "sentiment_reason": row["sentiment_reason"],
            "overall_confidence": row["overall_confidence"],
            "analyzed_at": row["analyzed_at"],
        },
        "lead": refreshed_lead,
    }
