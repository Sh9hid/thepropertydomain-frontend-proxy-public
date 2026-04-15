from __future__ import annotations

import json
import logging
import mimetypes
import re
import uuid
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional

from fastapi import UploadFile
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import DEFAULT_OPERATOR_NAME, SPEECH_AUDIO_STORAGE_ROOT, SPEECH_ORCHESTRATOR_BACKEND
from core.utils import now_iso
from models.sql_models import (
    CallSegment,
    CoachingReport,
    FillerEvent,
    FluencyEvent,
    Objection,
    PronunciationEvent,
    RealtimeEvent,
    ScoreComponent,
    ScoreSnapshot,
    Speaker,
    SpeechCall,
    TonalEvent,
    Transcript,
    WordTimestamp,
)
from services.coaching_service import get_coaching_service
from services.conversation_model import build_structured_conversation, compute_conversation_metrics
from services.diarization_provider import get_diarization_provider
from services.sales_analysis_service import get_sales_analysis_service
from services.scoring_engine import score_recorded_call_v1
from services.speech_feature_extractor import get_speech_feature_extractor
from services.transcription_provider import get_transcription_provider

_logger = logging.getLogger(__name__)
_DEFAULT_COMPONENT_WEIGHT = 0.25


def _safe_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True)


def _safe_json_loads(payload: Any, default: Any) -> Any:
    if payload in (None, ""):
        return default
    if isinstance(payload, (dict, list)):
        return payload
    try:
        return json.loads(str(payload))
    except json.JSONDecodeError:
        return default


def _sanitize_file_stem(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", value).strip("-")
    return cleaned or uuid.uuid4().hex


def _audio_extension(mime_type: str, filename: str = "") -> str:
    guessed = mimetypes.guess_extension(mime_type or "") or Path(filename).suffix
    return guessed if guessed else ".bin"


def _derive_rep_id(operator: str) -> str:
    raw = re.sub(r"[^a-zA-Z0-9]+", "_", str(operator or "").strip().lower()).strip("_")
    if raw in {"", "zoom", "system"}:
        fallback = re.sub(r"[^a-zA-Z0-9]+", "_", DEFAULT_OPERATOR_NAME.strip().lower()).strip("_")
        return fallback
    return raw


def _build_transcript_hint(legacy_analysis: Dict[str, Any], transcript_hint: str = "") -> str:
    if transcript_hint.strip():
        return transcript_hint.strip()
    parts = [
        str(legacy_analysis.get("summary") or "").strip(),
        " ".join(_safe_json_loads(legacy_analysis.get("key_topics"), [])) if legacy_analysis.get("key_topics") else "",
        str(legacy_analysis.get("next_step") or "").strip(),
    ]
    return " ".join(part for part in parts if part).strip()


def _component_sort_key(item: Dict[str, Any]) -> float:
    return float(item.get("score_value") or item.get("score") or 0.0)


def _normalize_score_component_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(row)
    evidence_payload = _safe_json_loads(normalized.get("evidence_json"), {})
    if isinstance(evidence_payload, dict):
        normalized["metric"] = evidence_payload.get("metric", normalized.get("metric", ""))
        normalized["reason"] = evidence_payload.get("reason", normalized.get("reason", ""))
        normalized["evidence"] = evidence_payload.get("evidence", normalized.get("evidence", []))
        if evidence_payload.get("normalized_score") is not None:
            normalized["normalized_score"] = evidence_payload.get("normalized_score")
        if evidence_payload.get("score") is not None:
            normalized["score"] = evidence_payload.get("score")
    else:
        normalized["evidence"] = evidence_payload
    return normalized


def _build_generated_analysis_payload(
    *,
    call_id: str,
    lead_id: str,
    call_row: Dict[str, Any],
    legacy_analysis: Dict[str, Any],
    features: Dict[str, Any],
    sales_analysis: Dict[str, Any],
    score_result: Dict[str, Any],
    transcript_payload: Dict[str, Any],
) -> Dict[str, Any]:
    objections = [item.get("text", "") for item in (features.get("objections") or []) if item.get("text")]
    keyword_events = features.get("keyword_events") or {}
    key_topics: List[str] = []
    if keyword_events.get("pricing"):
        key_topics.append("pricing")
    if keyword_events.get("booking_intent"):
        key_topics.append("booking")
    if features.get("questions"):
        key_topics.append("discovery")
    if not key_topics:
        key_topics.append("conversation")

    sentiment = features.get("sentiment") or {}
    summary = str(legacy_analysis.get("summary") or "").strip()
    if not summary:
        question_count = len(features.get("questions") or [])
        objection_count = len(features.get("objections") or [])
        booking_attempted = bool(keyword_events.get("booking_intent"))
        summary = (
            f"Recorded call analyzed with {question_count} discovery question(s), "
            f"{objection_count} objection(s), and "
            f"{'a clear' if booking_attempted else 'no clear'} booking attempt."
        )

    next_step = str(legacy_analysis.get("next_step") or "").strip()
    if not next_step:
        next_step = (
            "Send confirmation for the agreed next step."
            if sales_analysis.get("booking_attempted")
            else "Follow up with a direct next-step ask."
        )

    follow_up_task = str(legacy_analysis.get("suggested_follow_up_task") or "").strip()
    if not follow_up_task:
        follow_up_task = "Review the call, tighten the close, and send the next-step follow-up."

    now = now_iso()
    return {
        "call_id": call_id,
        "lead_id": lead_id,
        "provider": "speech_pipeline_v1",
        "recording_id": str(call_row.get("recording_id") or ""),
        "ai_call_summary_id": "",
        "status": "completed",
        "summary": summary,
        "outcome": str(legacy_analysis.get("outcome") or sales_analysis.get("outcome") or call_row.get("outcome") or "unknown"),
        "key_topics": _safe_json_dumps(key_topics),
        "objections": _safe_json_dumps(objections),
        "next_step": next_step,
        "suggested_follow_up_task": follow_up_task,
        "sentiment_label": str(legacy_analysis.get("sentiment_label") or sentiment.get("label") or ""),
        "sentiment_confidence": float(legacy_analysis.get("sentiment_confidence") or min(1.0, abs(float(sentiment.get("score") or 0.0)) + 0.45)),
        "sentiment_reason": str(legacy_analysis.get("sentiment_reason") or "Derived from objection pressure and customer response language."),
        "overall_confidence": float(legacy_analysis.get("overall_confidence") or score_result.get("confidence") or 0.0),
        "error_message": "",
        "raw_payload": _safe_json_dumps(
            {
                "call_row": call_row,
                "sales_analysis": sales_analysis,
                "features": features,
                "score_result": score_result,
                "transcript_provider": transcript_payload.get("provider"),
            }
        ),
        "analyzed_at": now,
        "created_at": now,
        "updated_at": now,
    }


def _build_segment_records(
    transcript_payload: Dict[str, Any],
    diarization_payload: Dict[str, Any],
    duration_seconds: int,
) -> List[Dict[str, Any]]:
    raw_segments = transcript_payload.get("segments") or diarization_payload.get("segments") or []
    segment_records: List[Dict[str, Any]] = []
    for index, segment in enumerate(raw_segments):
        segment_records.append(
            {
                "id": uuid.uuid4().hex,
                "turn_index": index,
                "speaker_label": str(segment.get("speaker_label") or segment.get("diarization_label") or ""),
                "speaker_role": str(segment.get("speaker_role") or segment.get("role") or "unknown"),
                "start_ms": int(segment.get("start_ms") or 0),
                "end_ms": int(segment.get("end_ms") or 0),
                "text": str(segment.get("text") or "").strip(),
                "confidence": float(segment.get("confidence") or 0.0),
                "overlap_flag": int(segment.get("overlap_flag") or 0),
                "segment_type": str(segment.get("segment_type") or "turn"),
            }
        )

    if segment_records or not str(transcript_payload.get("full_text") or "").strip():
        return segment_records

    return [
        {
            "id": uuid.uuid4().hex,
            "turn_index": 0,
            "speaker_label": "",
            "speaker_role": "unknown",
            "start_ms": 0,
            "end_ms": max(0, duration_seconds * 1000),
            "text": str(transcript_payload.get("full_text") or "").strip(),
            "confidence": float(transcript_payload.get("confidence") or 0.0),
            "overlap_flag": 0,
            "segment_type": "summary_stub",
        }
    ]


def _resolve_segment_id_for_word(segment_records: List[Dict[str, Any]], word_row: Dict[str, Any]) -> str:
    speaker_label = str(word_row.get("speaker_label") or "")
    start_ms = int(word_row.get("start_ms") or 0)
    end_ms = int(word_row.get("end_ms") or start_ms)

    for segment in segment_records:
        if speaker_label and speaker_label != segment.get("speaker_label"):
            continue
        if start_ms >= int(segment.get("start_ms") or 0) - 250 and end_ms <= int(segment.get("end_ms") or 0) + 250:
            return str(segment["id"])

    nearest_segment_id = ""
    nearest_distance: Optional[int] = None
    for segment in segment_records:
        midpoint = (int(segment.get("start_ms") or 0) + int(segment.get("end_ms") or 0)) // 2
        word_midpoint = (start_ms + end_ms) // 2
        distance = abs(midpoint - word_midpoint)
        if nearest_distance is None or distance < nearest_distance:
            nearest_distance = distance
            nearest_segment_id = str(segment["id"])
    return nearest_segment_id


def _build_fluency_events(conversation: List[Dict[str, Any]], features: Dict[str, Any]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    keyword_events = features.get("keyword_events") or {}
    conversation_metrics = features.get("conversation_metrics") or {}

    for hesitation in keyword_events.get("hesitation") or []:
        events.append(
            {
                "event_type": "hesitation",
                "timestamp_ms": int(hesitation.get("timestamp_ms") or 0),
                "duration_ms": 0,
                "severity": 0.55,
                "evidence": str(hesitation.get("text") or ""),
                "speaker": str(hesitation.get("speaker") or ""),
            }
        )

    for index in range(1, len(conversation)):
        previous = conversation[index - 1]
        current = conversation[index]
        if previous["speaker"] == current["speaker"]:
            continue
        if int(current["start_ms"]) < int(previous["end_ms"]):
            events.append(
                {
                    "event_type": "interruption",
                    "timestamp_ms": int(current["start_ms"]),
                    "duration_ms": int(previous["end_ms"]) - int(current["start_ms"]),
                    "severity": 0.7,
                    "evidence": f"{current['speaker']} started before {previous['speaker']} finished.",
                    "speaker": current["speaker"],
                }
            )

    longest_agent_monologue_ms = int(conversation_metrics.get("longest_agent_monologue_ms") or 0)
    if longest_agent_monologue_ms > 4500:
        for turn in conversation:
            if turn["speaker"] == "agent" and int(turn["end_ms"]) - int(turn["start_ms"]) == longest_agent_monologue_ms:
                events.append(
                    {
                        "event_type": "long_monologue",
                        "timestamp_ms": int(turn["start_ms"]),
                        "duration_ms": longest_agent_monologue_ms,
                        "severity": 0.6,
                        "evidence": turn["text"],
                        "speaker": "agent",
                    }
                )
                break

    average_latency_ms = int(conversation_metrics.get("average_response_latency_ms") or 0)
    if average_latency_ms > 1200:
        events.append(
            {
                "event_type": "slow_response",
                "timestamp_ms": 0,
                "duration_ms": average_latency_ms,
                "severity": 0.45,
                "evidence": f"Average response latency was {average_latency_ms}ms.",
                "speaker": "system",
            }
        )

    return events


async def ensure_speech_schema(session: AsyncSession) -> None:
    conn = await session.connection()
    for model in (
        SpeechCall,
        Speaker,
        CallSegment,
        Transcript,
        WordTimestamp,
        PronunciationEvent,
        FluencyEvent,
        FillerEvent,
        TonalEvent,
        Objection,
        CoachingReport,
        ScoreSnapshot,
        ScoreComponent,
        RealtimeEvent,
    ):
        await conn.run_sync(lambda sync_conn, table=model.__table__: table.create(sync_conn, checkfirst=True))
    await session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS call_analysis (
                call_id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                provider TEXT DEFAULT 'speech_pipeline_v1',
                recording_id TEXT DEFAULT '',
                ai_call_summary_id TEXT DEFAULT '',
                status TEXT DEFAULT 'completed',
                summary TEXT DEFAULT '',
                outcome TEXT DEFAULT 'unknown',
                key_topics TEXT DEFAULT '[]',
                objections TEXT DEFAULT '[]',
                next_step TEXT DEFAULT '',
                suggested_follow_up_task TEXT DEFAULT '',
                sentiment_label TEXT DEFAULT '',
                sentiment_confidence REAL DEFAULT 0,
                sentiment_reason TEXT DEFAULT '',
                overall_confidence REAL DEFAULT 0,
                error_message TEXT DEFAULT '',
                raw_payload TEXT DEFAULT '{}',
                analyzed_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
    )


async def _persist_audio_bytes(*, source: str, call_id: str, audio_bytes: bytes, mime_type: str, filename: str = "") -> str:
    call_root = SPEECH_AUDIO_STORAGE_ROOT / source
    call_root.mkdir(parents=True, exist_ok=True)
    extension = _audio_extension(mime_type, filename)
    stored_name = f"{_sanitize_file_stem(call_id)}{extension}"
    stored_path = call_root / stored_name
    stored_path.write_bytes(audio_bytes)
    return str(stored_path.relative_to(SPEECH_AUDIO_STORAGE_ROOT.parent)).replace("\\", "/")


async def shadow_write_call_log_row(session: AsyncSession, row_payload: Dict[str, Any]) -> str:
    await ensure_speech_schema(session)
    call_id = str(row_payload.get("provider_call_id") or row_payload.get("id") or uuid.uuid4().hex)
    now = now_iso()
    started_at = str(row_payload.get("logged_at") or now)
    existing_metadata = {
        "call_log": row_payload,
    }
    if row_payload.get("recording_url"):
        existing_metadata["recording_url"] = str(row_payload.get("recording_url") or "")
    if row_payload.get("audio_uri"):
        existing_metadata["audio_uri"] = str(row_payload.get("audio_uri") or "")
    await session.execute(
        text(
            """
            INSERT INTO calls (
                id, external_call_id, source, lead_id, rep_id, call_type, direction, outcome,
                started_at, ended_at, duration_seconds, recording_id, audio_uri, audio_storage_status,
                analysis_status, transcript_status, diarization_status, metadata_json, created_at, updated_at
            ) VALUES (
                :id, :external_call_id, :source, :lead_id, :rep_id, :call_type, :direction, :outcome,
                :started_at, :ended_at, :duration_seconds, :recording_id, :audio_uri, :audio_storage_status,
                :analysis_status, :transcript_status, :diarization_status, :metadata_json, :created_at, :updated_at
            )
            ON CONFLICT(id) DO UPDATE SET
                external_call_id = excluded.external_call_id,
                source = excluded.source,
                lead_id = excluded.lead_id,
                rep_id = excluded.rep_id,
                call_type = excluded.call_type,
                direction = excluded.direction,
                outcome = excluded.outcome,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                duration_seconds = excluded.duration_seconds,
                recording_id = excluded.recording_id,
                audio_uri = excluded.audio_uri,
                audio_storage_status = excluded.audio_storage_status,
                analysis_status = excluded.analysis_status,
                transcript_status = excluded.transcript_status,
                diarization_status = excluded.diarization_status,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """
        ),
        {
            "id": call_id,
            "external_call_id": str(row_payload.get("provider_call_id") or row_payload.get("id") or ""),
            "source": str(row_payload.get("provider") or "manual"),
            "lead_id": str(row_payload.get("lead_id") or ""),
            "rep_id": _derive_rep_id(str(row_payload.get("operator") or "")),
            "call_type": "recorded_call",
            "direction": str(row_payload.get("direction") or ""),
            "outcome": str(row_payload.get("outcome") or "unknown"),
            "started_at": started_at,
            "ended_at": started_at,
            "duration_seconds": int(row_payload.get("duration_seconds") or 0),
            "recording_id": str(row_payload.get("recording_id") or ""),
            "audio_uri": str(row_payload.get("audio_uri") or ""),
            "audio_storage_status": str(row_payload.get("audio_storage_status") or "not_downloaded"),
            "analysis_status": "mirrored",
            "transcript_status": "pending",
            "diarization_status": "pending",
            "metadata_json": _safe_json_dumps(existing_metadata),
            "created_at": now,
            "updated_at": now,
        },
    )
    return call_id


def build_call_analysis_payload(
    *,
    call_row: Dict[str, Any],
    transcript_rows: List[Dict[str, Any]],
    objection_rows: List[Dict[str, Any]],
    snapshot_row: Optional[Dict[str, Any]],
    component_rows: List[Dict[str, Any]],
    legacy_analysis_row: Optional[Dict[str, Any]],
    speaker_rows: Optional[List[Dict[str, Any]]] = None,
    segment_rows: Optional[List[Dict[str, Any]]] = None,
    word_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    transcript_row = transcript_rows[0] if transcript_rows else {}
    legacy = legacy_analysis_row or {}
    metadata = _safe_json_loads(call_row.get("metadata_json"), {})
    cached_analysis = (metadata or {}).get("analysis_cache") or {}
    speaker_payload = [dict(row) for row in (speaker_rows or [])]
    speaker_lookup = {str(row.get("id")): dict(row) for row in speaker_payload}

    structured_conversation: List[Dict[str, Any]] = []
    for row in segment_rows or []:
        segment = dict(row)
        speaker = speaker_lookup.get(str(segment.get("speaker_id") or ""), {})
        structured_conversation.append(
            {
                "speaker": str(speaker.get("role") or "unknown"),
                "speaker_label": str(speaker.get("diarization_label") or ""),
                "text": str(segment.get("text") or ""),
                "start_ms": int(segment.get("start_ms") or 0),
                "end_ms": int(segment.get("end_ms") or 0),
                "confidence": float(segment.get("confidence") or 0.0),
                "turn_index": int(segment.get("turn_index") or 0),
            }
        )

    conversation_metrics = cached_analysis.get("conversation_metrics") or compute_conversation_metrics(structured_conversation)
    components = sorted(
        [_normalize_score_component_row(dict(row)) for row in component_rows],
        key=_component_sort_key,
        reverse=True,
    )
    analyzed_at = legacy.get("analyzed_at") or (snapshot_row or {}).get("computed_at")

    return {
        "call": call_row,
        "analysis": {
            "summary": legacy.get("summary") or "",
            "outcome": legacy.get("outcome") or call_row.get("outcome") or "unknown",
            "key_topics": _safe_json_loads(legacy.get("key_topics"), []),
            "objections": objection_rows,
            "next_step": legacy.get("next_step") or "",
            "suggested_follow_up_task": legacy.get("suggested_follow_up_task") or "",
            "sentiment": {
                "label": legacy.get("sentiment_label") or "",
                "confidence": legacy.get("sentiment_confidence") or 0,
                "reason": legacy.get("sentiment_reason") or "",
            },
            "overall_confidence": legacy.get("overall_confidence") or 0,
            "transcript": {
                "id": transcript_row.get("id"),
                "provider": transcript_row.get("provider"),
                "version_type": transcript_row.get("version_type"),
                "language": transcript_row.get("language"),
                "full_text": transcript_row.get("full_text") or "",
                "confidence": transcript_row.get("confidence") or 0,
                "created_at": transcript_row.get("created_at"),
            },
            "structured_conversation": structured_conversation,
            "conversation_metrics": conversation_metrics,
            "speakers": speaker_payload,
            "words": [dict(row) for row in (word_rows or [])],
            "features": cached_analysis.get("features") or {},
            "sales_analysis": cached_analysis.get("sales_analysis") or {},
            "pipeline_timings_ms": cached_analysis.get("pipeline_timings_ms") or {},
            "analyzed_at": analyzed_at,
        },
        "scores": {
            "snapshot_id": snapshot_row.get("id") if snapshot_row else None,
            "scoring_version": snapshot_row.get("scoring_version") if snapshot_row else "v0",
            "composite_score": snapshot_row.get("composite_score") if snapshot_row else None,
            "confidence": snapshot_row.get("confidence") if snapshot_row else None,
            "computed_at": snapshot_row.get("computed_at") if snapshot_row else None,
            "components": components,
        },
    }


def build_coaching_report_payload(*, report_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": report_row.get("id"),
        "call_id": report_row.get("call_id"),
        "rep_id": report_row.get("rep_id"),
        "report_version": report_row.get("report_version") or "v0",
        "brutal_summary": report_row.get("brutal_summary") or "",
        "detailed_breakdown": _safe_json_loads(report_row.get("detailed_breakdown_json"), {}),
        "rewrite": _safe_json_loads(report_row.get("rewrite_json"), {}),
        "drills": _safe_json_loads(report_row.get("drills_json"), []),
        "live_task": report_row.get("live_task") or "",
        "generated_at": report_row.get("generated_at"),
    }


async def get_call_analysis_payload(session: AsyncSession, call_id: str) -> Dict[str, Any]:
    await ensure_speech_schema(session)
    call_row = (
        await session.execute(text("SELECT * FROM calls WHERE id = :call_id"), {"call_id": call_id})
    ).mappings().first()
    if not call_row:
        raise ValueError("Call not found")

    transcript_rows = (
        await session.execute(text("SELECT * FROM transcripts WHERE call_id = :call_id ORDER BY created_at DESC"), {"call_id": call_id})
    ).mappings().all()
    objection_rows = (
        await session.execute(text("SELECT * FROM objections WHERE call_id = :call_id ORDER BY detected_at_ms ASC, created_at ASC"), {"call_id": call_id})
    ).mappings().all()
    speaker_rows = (
        await session.execute(text("SELECT * FROM speakers WHERE call_id = :call_id ORDER BY created_at ASC"), {"call_id": call_id})
    ).mappings().all()
    segment_rows = (
        await session.execute(text("SELECT * FROM call_segments WHERE call_id = :call_id ORDER BY start_ms ASC, turn_index ASC"), {"call_id": call_id})
    ).mappings().all()
    word_rows = (
        await session.execute(text("SELECT * FROM word_timestamps WHERE call_id = :call_id ORDER BY start_ms ASC"), {"call_id": call_id})
    ).mappings().all()
    snapshot_row = (
        await session.execute(
            text(
                """
                SELECT * FROM score_snapshots
                WHERE entity_type = 'call' AND entity_id = :call_id
                ORDER BY computed_at DESC
                LIMIT 1
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().first()
    component_rows: List[Dict[str, Any]] = []
    if snapshot_row:
        component_rows = (
            await session.execute(
                text("SELECT * FROM score_components WHERE snapshot_id = :snapshot_id ORDER BY score_value DESC"),
                {"snapshot_id": snapshot_row["id"]},
            )
        ).mappings().all()
    legacy_analysis_row = (
        await session.execute(text("SELECT * FROM call_analysis WHERE call_id = :call_id"), {"call_id": call_id})
    ).mappings().first()
    return build_call_analysis_payload(
        call_row=dict(call_row),
        transcript_rows=[dict(row) for row in transcript_rows],
        objection_rows=[dict(row) for row in objection_rows],
        snapshot_row=dict(snapshot_row) if snapshot_row else None,
        component_rows=[dict(row) for row in component_rows],
        legacy_analysis_row=dict(legacy_analysis_row) if legacy_analysis_row else None,
        speaker_rows=[dict(row) for row in speaker_rows],
        segment_rows=[dict(row) for row in segment_rows],
        word_rows=[dict(row) for row in word_rows],
    )


async def get_call_coaching_payload(session: AsyncSession, call_id: str) -> Dict[str, Any]:
    await ensure_speech_schema(session)
    report_row = (
        await session.execute(
            text("SELECT * FROM coaching_reports WHERE call_id = :call_id ORDER BY generated_at DESC LIMIT 1"),
            {"call_id": call_id},
        )
    ).mappings().first()
    if not report_row:
        raise ValueError("Coaching report not found")
    return build_coaching_report_payload(report_row=dict(report_row))


async def get_rep_score_summary(session: AsyncSession, rep_id: str) -> Dict[str, Any]:
    await ensure_speech_schema(session)
    snapshot_rows = (
        await session.execute(
            text(
                """
                SELECT * FROM score_snapshots
                WHERE rep_id = :rep_id
                ORDER BY computed_at DESC
                LIMIT 20
                """
            ),
            {"rep_id": rep_id},
        )
    ).mappings().all()
    if not snapshot_rows:
        return {"rep_id": rep_id, "latest": {}, "moving_average": {}, "snapshots": []}

    latest_snapshot = dict(snapshot_rows[0])
    latest_components = (
        await session.execute(
            text("SELECT * FROM score_components WHERE snapshot_id = :snapshot_id ORDER BY score_name ASC"),
            {"snapshot_id": latest_snapshot["id"]},
        )
    ).mappings().all()
    component_rows = (
        await session.execute(
            text(
                """
                SELECT sc.score_name, AVG(sc.score_value) AS avg_score
                FROM score_components sc
                JOIN score_snapshots ss ON ss.id = sc.snapshot_id
                WHERE ss.rep_id = :rep_id
                GROUP BY sc.score_name
                """
            ),
            {"rep_id": rep_id},
        )
    ).mappings().all()
    return {
        "rep_id": rep_id,
        "latest": {
            "snapshot_id": latest_snapshot["id"],
            "composite_score": latest_snapshot["composite_score"],
            "confidence": latest_snapshot["confidence"],
            "computed_at": latest_snapshot["computed_at"],
            "components": [_normalize_score_component_row(dict(row)) for row in latest_components],
        },
        "moving_average": {row["score_name"]: round(float(row["avg_score"] or 0), 2) for row in component_rows},
        "snapshots": [dict(row) for row in snapshot_rows],
    }


async def _delete_existing_analysis_artifacts(session: AsyncSession, call_id: str) -> None:
    snapshot_ids = [
        row["id"]
        for row in (
            await session.execute(
                text("SELECT id FROM score_snapshots WHERE entity_type = 'call' AND entity_id = :call_id"),
                {"call_id": call_id},
            )
        ).mappings().all()
    ]
    for snapshot_id in snapshot_ids:
        await session.execute(text("DELETE FROM score_components WHERE snapshot_id = :snapshot_id"), {"snapshot_id": snapshot_id})

    for table_name in (
        "score_snapshots",
        "coaching_reports",
        "objections",
        "tonal_events",
        "filler_events",
        "fluency_events",
        "pronunciation_events",
        "word_timestamps",
        "transcripts",
        "call_segments",
        "speakers",
    ):
        await session.execute(text(f"DELETE FROM {table_name} WHERE call_id = :call_id"), {"call_id": call_id})


async def _get_cached_pipeline_result(session: AsyncSession, call_id: str) -> Optional[Dict[str, Any]]:
    call_row = (
        await session.execute(
            text("SELECT id, analysis_status FROM calls WHERE id = :call_id"),
            {"call_id": call_id},
        )
    ).mappings().first()
    if not call_row or str(call_row.get("analysis_status") or "") != "completed":
        return None

    snapshot_row = (
        await session.execute(
            text(
                """
                SELECT * FROM score_snapshots
                WHERE entity_type = 'call' AND entity_id = :call_id
                ORDER BY computed_at DESC
                LIMIT 1
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().first()
    report_row = (
        await session.execute(
            text("SELECT * FROM coaching_reports WHERE call_id = :call_id ORDER BY generated_at DESC LIMIT 1"),
            {"call_id": call_id},
        )
    ).mappings().first()
    if not snapshot_row or not report_row:
        return None

    return {
        "call_id": call_id,
        "execution_mode": "cached_v1",
        "orchestrator": SPEECH_ORCHESTRATOR_BACKEND,
        "scoring_version": str(snapshot_row.get("scoring_version") or "v1"),
        "score_result": {
            "scoring_version": str(snapshot_row.get("scoring_version") or "v1"),
            "composite_score": float(snapshot_row.get("composite_score") or 0.0),
            "confidence": float(snapshot_row.get("confidence") or 0.0),
        },
        "coaching_result": {
            "report_version": str(report_row.get("report_version") or "v1"),
            "brutal_summary": str(report_row.get("brutal_summary") or ""),
        },
    }


async def _upsert_analysis_ready_call(
    session: AsyncSession,
    *,
    call_id: str,
    source: str,
    lead_id: str,
    rep_id: str,
    direction: str,
    outcome: str,
    started_at: str,
    duration_seconds: int,
    recording_id: str,
    audio_uri: str,
    audio_storage_status: str,
    metadata_json: str,
) -> None:
    now = now_iso()
    await session.execute(
        text(
            """
            INSERT INTO calls (
                id, external_call_id, source, lead_id, rep_id, call_type, direction, outcome,
                started_at, ended_at, duration_seconds, recording_id, audio_uri, audio_storage_status,
                analysis_status, transcript_status, diarization_status, metadata_json, created_at, updated_at
            ) VALUES (
                :id, :external_call_id, :source, :lead_id, :rep_id, 'recorded_call', :direction, :outcome,
                :started_at, :ended_at, :duration_seconds, :recording_id, :audio_uri, :audio_storage_status,
                'completed', 'completed', 'completed', :metadata_json, :created_at, :updated_at
            )
            ON CONFLICT(id) DO UPDATE SET
                external_call_id = excluded.external_call_id,
                source = excluded.source,
                lead_id = excluded.lead_id,
                rep_id = excluded.rep_id,
                direction = excluded.direction,
                outcome = excluded.outcome,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                duration_seconds = excluded.duration_seconds,
                recording_id = excluded.recording_id,
                audio_uri = excluded.audio_uri,
                audio_storage_status = excluded.audio_storage_status,
                analysis_status = excluded.analysis_status,
                transcript_status = excluded.transcript_status,
                diarization_status = excluded.diarization_status,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """
        ),
        {
            "id": call_id,
            "external_call_id": call_id,
            "source": source,
            "lead_id": lead_id,
            "rep_id": rep_id,
            "direction": direction,
            "outcome": outcome,
            "started_at": started_at,
            "ended_at": started_at,
            "duration_seconds": duration_seconds,
            "recording_id": recording_id,
            "audio_uri": audio_uri,
            "audio_storage_status": audio_storage_status,
            "metadata_json": metadata_json,
            "created_at": now,
            "updated_at": now,
        },
    )


async def _store_analysis_outputs(
    session: AsyncSession,
    *,
    call_id: str,
    lead_id: str,
    rep_id: str,
    call_row: Dict[str, Any],
    transcript_payload: Dict[str, Any],
    diarization_payload: Dict[str, Any],
    features: Dict[str, Any],
    legacy_analysis: Dict[str, Any],
    sales_analysis: Dict[str, Any],
    score_result: Dict[str, Any],
    coaching_result: Dict[str, Any],
    conversation: List[Dict[str, Any]],
) -> None:
    now = now_iso()
    speakers = diarization_payload.get("speakers") or transcript_payload.get("speakers") or []
    speaker_ids: Dict[str, str] = {}
    for speaker in speakers:
        speaker_id = uuid.uuid4().hex
        label = str(speaker.get("label") or "")
        speaker_ids[label] = speaker_id
        await session.execute(
            text(
                """
                INSERT INTO speakers (id, call_id, diarization_label, role, display_name, linked_rep_id, linked_contact_id, confidence, created_at)
                VALUES (:id, :call_id, :diarization_label, :role, :display_name, :linked_rep_id, :linked_contact_id, :confidence, :created_at)
                """
            ),
            {
                "id": speaker_id,
                "call_id": call_id,
                "diarization_label": label,
                "role": str(speaker.get("role") or "unknown"),
                "display_name": str(speaker.get("role") or "unknown").title(),
                "linked_rep_id": rep_id if str(speaker.get("role") or "") == "agent" else "",
                "linked_contact_id": lead_id if str(speaker.get("role") or "") == "customer" else "",
                "confidence": float(speaker.get("confidence") or 0.0),
                "created_at": now,
            },
        )

    transcript_id = uuid.uuid4().hex
    transcript_text = str(transcript_payload.get("full_text") or "")
    await session.execute(
        text(
            """
            INSERT INTO transcripts (id, call_id, provider, version_type, language, full_text, confidence, status, created_at, updated_at)
            VALUES (:id, :call_id, :provider, :version_type, :language, :full_text, :confidence, :status, :created_at, :updated_at)
            """
        ),
        {
            "id": transcript_id,
            "call_id": call_id,
            "provider": transcript_payload.get("provider") or "stub",
            "version_type": transcript_payload.get("version_type") or "canonical",
            "language": transcript_payload.get("language") or "en-AU",
            "full_text": transcript_text,
            "confidence": float(transcript_payload.get("confidence") or 0.0),
            "status": transcript_payload.get("status") or "completed",
            "created_at": now,
            "updated_at": now,
        },
    )

    segment_records = _build_segment_records(transcript_payload, diarization_payload, int(call_row.get("duration_seconds") or 0))
    for segment in segment_records:
        speaker_id = speaker_ids.get(str(segment.get("speaker_label") or ""), "")
        await session.execute(
            text(
                """
                INSERT INTO call_segments (id, call_id, speaker_id, turn_index, start_ms, end_ms, text, overlap_flag, segment_type, confidence, created_at)
                VALUES (:id, :call_id, :speaker_id, :turn_index, :start_ms, :end_ms, :text, :overlap_flag, :segment_type, :confidence, :created_at)
                """
            ),
            {
                "id": segment["id"],
                "call_id": call_id,
                "speaker_id": speaker_id,
                "turn_index": int(segment["turn_index"]),
                "start_ms": int(segment["start_ms"]),
                "end_ms": int(segment["end_ms"]),
                "text": segment["text"],
                "overlap_flag": int(segment["overlap_flag"]),
                "segment_type": segment["segment_type"],
                "confidence": float(segment["confidence"]),
                "created_at": now,
            },
        )

    for word in transcript_payload.get("words") or []:
        speaker_label = str(word.get("speaker_label") or "")
        segment_id = _resolve_segment_id_for_word(segment_records, word)
        await session.execute(
            text(
                """
                INSERT INTO word_timestamps (id, call_id, transcript_id, segment_id, speaker_id, word, start_ms, end_ms, confidence, phoneme_seq)
                VALUES (:id, :call_id, :transcript_id, :segment_id, :speaker_id, :word, :start_ms, :end_ms, :confidence, :phoneme_seq)
                """
            ),
            {
                "id": uuid.uuid4().hex,
                "call_id": call_id,
                "transcript_id": transcript_id,
                "segment_id": segment_id,
                "speaker_id": speaker_ids.get(speaker_label, ""),
                "word": str(word.get("word") or ""),
                "start_ms": int(word.get("start_ms") or 0),
                "end_ms": int(word.get("end_ms") or 0),
                "confidence": float(word.get("confidence") or 0.0),
                "phoneme_seq": "",
            },
        )

    for filler_event in features.get("filler_events") or []:
        segment_id = _resolve_segment_id_for_word(
            segment_records,
            {
                "speaker_label": "",
                "start_ms": int(filler_event.get("timestamp_ms") or 0),
                "end_ms": int(filler_event.get("timestamp_ms") or 0),
            },
        )
        await session.execute(
            text(
                """
                INSERT INTO filler_events (id, call_id, segment_id, token, family, count, start_ms, duration_ms, created_at)
                VALUES (:id, :call_id, :segment_id, :token, :family, :count, :start_ms, :duration_ms, :created_at)
                """
            ),
            {
                "id": uuid.uuid4().hex,
                "call_id": call_id,
                "segment_id": segment_id,
                "token": str(filler_event.get("text") or ""),
                "family": "filler",
                "count": 1,
                "start_ms": int(filler_event.get("timestamp_ms") or 0),
                "duration_ms": 0,
                "created_at": now,
            },
        )

    for fluency_event in _build_fluency_events(conversation, features):
        segment_id = _resolve_segment_id_for_word(
            segment_records,
            {
                "speaker_label": "",
                "start_ms": int(fluency_event.get("timestamp_ms") or 0),
                "end_ms": int(fluency_event.get("timestamp_ms") or 0),
            },
        )
        await session.execute(
            text(
                """
                INSERT INTO fluency_events (id, call_id, segment_id, event_type, start_ms, duration_ms, severity, evidence, created_at)
                VALUES (:id, :call_id, :segment_id, :event_type, :start_ms, :duration_ms, :severity, :evidence, :created_at)
                """
            ),
            {
                "id": uuid.uuid4().hex,
                "call_id": call_id,
                "segment_id": segment_id,
                "event_type": str(fluency_event.get("event_type") or ""),
                "start_ms": int(fluency_event.get("timestamp_ms") or 0),
                "duration_ms": int(fluency_event.get("duration_ms") or 0),
                "severity": float(fluency_event.get("severity") or 0.0),
                "evidence": str(fluency_event.get("evidence") or ""),
                "created_at": now,
            },
        )

    sentiment = features.get("sentiment") or {}
    if sentiment.get("label"):
        await session.execute(
            text(
                """
                INSERT INTO tonal_events (
                    id, call_id, segment_id, contour_type, pitch_start_hz, pitch_end_hz,
                    semitone_delta, intensity_db, tone_label, confidence, created_at
                ) VALUES (
                    :id, :call_id, :segment_id, :contour_type, :pitch_start_hz, :pitch_end_hz,
                    :semitone_delta, :intensity_db, :tone_label, :confidence, :created_at
                )
                """
            ),
            {
                "id": uuid.uuid4().hex,
                "call_id": call_id,
                "segment_id": segment_records[0]["id"] if segment_records else "",
                "contour_type": "sentiment_proxy",
                "pitch_start_hz": 0.0,
                "pitch_end_hz": 0.0,
                "semitone_delta": 0.0,
                "intensity_db": 0.0,
                "tone_label": str(sentiment.get("label") or ""),
                "confidence": float(min(1.0, abs(float(sentiment.get("score") or 0.0)) + 0.3)),
                "created_at": now,
            },
        )

    for objection in features.get("objections") or []:
        await session.execute(
            text(
                """
                INSERT INTO objections (id, call_id, segment_id, objection_type, normalized_text, detected_at_ms, response_quality_score, resolved_flag, created_at)
                VALUES (:id, :call_id, :segment_id, :objection_type, :normalized_text, :detected_at_ms, :response_quality_score, :resolved_flag, :created_at)
                """
            ),
            {
                "id": uuid.uuid4().hex,
                "call_id": call_id,
                "segment_id": _resolve_segment_id_for_word(
                    segment_records,
                    {
                        "speaker_label": "",
                        "start_ms": int(objection.get("timestamp_ms") or 0),
                        "end_ms": int(objection.get("timestamp_ms") or 0),
                    },
                ),
                "objection_type": str(objection.get("label") or "general"),
                "normalized_text": str(objection.get("text") or ""),
                "detected_at_ms": int(objection.get("timestamp_ms") or 0),
                "response_quality_score": 0.85 if objection.get("resolved") else 0.45,
                "resolved_flag": 1 if objection.get("resolved") else 0,
                "created_at": now,
            },
        )

    snapshot_id = uuid.uuid4().hex
    await session.execute(
        text(
            """
            INSERT INTO score_snapshots (id, entity_type, entity_id, call_id, rep_id, scenario_type, scoring_version, composite_score, confidence, computed_at, created_at)
            VALUES (:id, 'call', :entity_id, :call_id, :rep_id, 'recorded_call', :scoring_version, :composite_score, :confidence, :computed_at, :created_at)
            """
        ),
        {
            "id": snapshot_id,
            "entity_id": call_id,
            "call_id": call_id,
            "rep_id": rep_id,
            "scoring_version": str(score_result.get("scoring_version") or "v1"),
            "composite_score": float(score_result.get("composite_score") or 0.0),
            "confidence": float(score_result.get("confidence") or 0.0),
            "computed_at": now,
            "created_at": now,
        },
    )

    for score_name, component in (score_result.get("components") or {}).items():
        await session.execute(
            text(
                """
                INSERT INTO score_components (
                    id, snapshot_id, call_id, score_name, score_value, raw_value, normalized_value,
                    weight, stable_flag, evidence_json, created_at
                ) VALUES (
                    :id, :snapshot_id, :call_id, :score_name, :score_value, :raw_value, :normalized_value,
                    :weight, :stable_flag, :evidence_json, :created_at
                )
                """
            ),
            {
                "id": uuid.uuid4().hex,
                "snapshot_id": snapshot_id,
                "call_id": call_id,
                "score_name": score_name,
                "score_value": float(component.get("score") or 0.0),
                "raw_value": float(component.get("normalized_score") or 0.0),
                "normalized_value": float(component.get("normalized_score") or 0.0),
                "weight": _DEFAULT_COMPONENT_WEIGHT,
                "stable_flag": 1 if len(component.get("evidence") or []) >= 1 else 0,
                "evidence_json": _safe_json_dumps(component),
                "created_at": now,
            },
        )

    await session.execute(
        text(
            """
            INSERT INTO coaching_reports (
                id, call_id, rep_id, report_version, brutal_summary, detailed_breakdown_json,
                rewrite_json, drills_json, live_task, generated_at, created_at, updated_at
            ) VALUES (
                :id, :call_id, :rep_id, :report_version, :brutal_summary, :detailed_breakdown_json,
                :rewrite_json, :drills_json, :live_task, :generated_at, :created_at, :updated_at
            )
            """
        ),
        {
            "id": uuid.uuid4().hex,
            "call_id": call_id,
            "rep_id": rep_id,
            "report_version": str(coaching_result.get("report_version") or "v1"),
            "brutal_summary": str(coaching_result.get("brutal_summary") or ""),
            "detailed_breakdown_json": _safe_json_dumps(coaching_result.get("detailed_breakdown") or {}),
            "rewrite_json": _safe_json_dumps(coaching_result.get("rewrite") or {}),
            "drills_json": _safe_json_dumps(coaching_result.get("drills") or []),
            "live_task": str(coaching_result.get("live_task") or ""),
            "generated_at": now,
            "created_at": now,
            "updated_at": now,
        },
    )

    generated_analysis_row = _build_generated_analysis_payload(
        call_id=call_id,
        lead_id=lead_id,
        call_row=call_row,
        legacy_analysis=legacy_analysis,
        features=features,
        sales_analysis=sales_analysis,
        score_result=score_result,
        transcript_payload=transcript_payload,
    )
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
                lead_id = excluded.lead_id,
                provider = excluded.provider,
                recording_id = excluded.recording_id,
                ai_call_summary_id = excluded.ai_call_summary_id,
                status = excluded.status,
                summary = excluded.summary,
                outcome = excluded.outcome,
                key_topics = excluded.key_topics,
                objections = excluded.objections,
                next_step = excluded.next_step,
                suggested_follow_up_task = excluded.suggested_follow_up_task,
                sentiment_label = excluded.sentiment_label,
                sentiment_confidence = excluded.sentiment_confidence,
                sentiment_reason = excluded.sentiment_reason,
                overall_confidence = excluded.overall_confidence,
                error_message = excluded.error_message,
                raw_payload = excluded.raw_payload,
                analyzed_at = excluded.analyzed_at,
                updated_at = excluded.updated_at
            """
        ),
        generated_analysis_row,
    )


async def _run_recorded_call_pipeline(
    session: AsyncSession,
    *,
    call_id: str,
    source: str,
    lead_id: str,
    rep_id: str,
    direction: str,
    outcome: str,
    started_at: str,
    duration_seconds: int,
    recording_id: str,
    audio_path: Path,
    metadata: Dict[str, Any],
    legacy_analysis: Dict[str, Any],
    transcript_hint: str = "",
) -> Dict[str, Any]:
    await ensure_speech_schema(session)
    if not bool(metadata.get("force_reprocess")):
        cached = await _get_cached_pipeline_result(session, call_id)
        if cached:
            _logger.info("speech_pipeline cache_hit call_id=%s", call_id)
            return cached

    await _delete_existing_analysis_artifacts(session, call_id)

    transcription_provider = get_transcription_provider()
    diarization_provider = get_diarization_provider()
    feature_extractor = get_speech_feature_extractor()
    sales_analysis_service = get_sales_analysis_service()
    coaching_service = get_coaching_service()
    stage_timings_ms: Dict[str, int] = {}

    _logger.info("speech_pipeline start call_id=%s source=%s", call_id, source)

    started = perf_counter()
    transcript_payload = await transcription_provider.transcribe(
        call_id=call_id,
        audio_path=audio_path,
        context={
            "transcript_hint": _build_transcript_hint(legacy_analysis, transcript_hint),
            "direction": direction,
        },
    )
    stage_timings_ms["transcription"] = round((perf_counter() - started) * 1000)
    _logger.info(
        "speech_pipeline stage=transcription call_id=%s provider=%s status=%s duration_ms=%s",
        call_id,
        transcript_payload.get("provider"),
        transcript_payload.get("status"),
        stage_timings_ms["transcription"],
    )

    started = perf_counter()
    diarization_payload = await diarization_provider.diarize(
        call_id=call_id,
        audio_path=audio_path,
        context={
            "direction": direction,
            "transcription": transcript_payload,
        },
    )
    stage_timings_ms["diarization"] = round((perf_counter() - started) * 1000)
    _logger.info(
        "speech_pipeline stage=diarization call_id=%s provider=%s status=%s duration_ms=%s",
        call_id,
        diarization_payload.get("provider"),
        diarization_payload.get("status"),
        stage_timings_ms["diarization"],
    )

    started = perf_counter()
    structured_conversation = build_structured_conversation(diarization_payload.get("segments") or transcript_payload.get("segments") or [])
    conversation_metrics = compute_conversation_metrics(structured_conversation)
    features = feature_extractor.extract(
        conversation=structured_conversation,
        conversation_metrics=conversation_metrics,
        legacy_analysis=legacy_analysis,
    )
    stage_timings_ms["feature_extraction"] = round((perf_counter() - started) * 1000)

    started = perf_counter()
    transcript_text = str(transcript_payload.get("full_text") or "")
    sales_analysis = sales_analysis_service.analyze(
        call_row={"id": call_id, "duration_seconds": duration_seconds, "outcome": outcome, "recording_id": recording_id},
        transcript_text=transcript_text,
        legacy_analysis=legacy_analysis,
        features=features,
    )
    stage_timings_ms["sales_analysis"] = round((perf_counter() - started) * 1000)

    started = perf_counter()
    score_result = score_recorded_call_v1(features)
    stage_timings_ms["scoring"] = round((perf_counter() - started) * 1000)

    started = perf_counter()
    coaching_result = coaching_service.generate(
        call_row={"id": call_id, "outcome": outcome},
        features=features,
        sales_analysis=sales_analysis,
        score_result=score_result,
    )
    stage_timings_ms["coaching"] = round((perf_counter() - started) * 1000)

    call_metadata = {
        **metadata,
        "transcription_provider": transcript_payload.get("provider"),
        "diarization_provider": diarization_payload.get("provider"),
        "analysis_cache": {
            "conversation_metrics": conversation_metrics,
            "features": features,
            "sales_analysis": sales_analysis,
            "pipeline_timings_ms": stage_timings_ms,
        },
    }

    call_row = {
        "id": call_id,
        "source": source,
        "lead_id": lead_id,
        "rep_id": rep_id,
        "direction": direction,
        "outcome": outcome,
        "started_at": started_at,
        "duration_seconds": duration_seconds,
        "recording_id": recording_id,
        "audio_uri": str(metadata.get("audio_uri") or ""),
    }

    await _upsert_analysis_ready_call(
        session,
        call_id=call_id,
        source=source,
        lead_id=lead_id,
        rep_id=rep_id,
        direction=direction,
        outcome=outcome,
        started_at=started_at,
        duration_seconds=duration_seconds,
        recording_id=recording_id,
        audio_uri=str(metadata.get("audio_uri") or ""),
        audio_storage_status=str(metadata.get("audio_storage_status") or "stored"),
        metadata_json=_safe_json_dumps(call_metadata),
    )
    await _store_analysis_outputs(
        session,
        call_id=call_id,
        lead_id=lead_id,
        rep_id=rep_id,
        call_row=call_row,
        transcript_payload=transcript_payload,
        diarization_payload=diarization_payload,
        features=features,
        legacy_analysis=legacy_analysis,
        sales_analysis=sales_analysis,
        score_result=score_result,
        coaching_result=coaching_result,
        conversation=structured_conversation,
    )

    _logger.info(
        "speech_pipeline complete call_id=%s scoring_version=%s composite_score=%s timings_ms=%s",
        call_id,
        score_result.get("scoring_version"),
        score_result.get("composite_score"),
        stage_timings_ms,
    )
    return {
        "call_id": call_id,
        "execution_mode": "inline_v1",
        "orchestrator": SPEECH_ORCHESTRATOR_BACKEND,
        "scoring_version": str(score_result.get("scoring_version") or "v1"),
        "score_result": score_result,
        "coaching_result": coaching_result,
    }


async def process_zoom_recorded_call(
    session: AsyncSession,
    *,
    lead_id: str,
    call_id: str,
    call_meta: Dict[str, Any],
    recording_meta: Dict[str, Any],
    legacy_analysis: Dict[str, Any],
    audio_bytes: bytes,
    mime_type: str,
) -> Dict[str, Any]:
    audio_uri = await _persist_audio_bytes(
        source="zoom",
        call_id=call_id,
        audio_bytes=audio_bytes,
        mime_type=mime_type,
        filename=str(recording_meta.get("file_url") or ""),
    )
    audio_path = SPEECH_AUDIO_STORAGE_ROOT.parent / audio_uri
    return await _run_recorded_call_pipeline(
        session,
        call_id=call_id,
        source="zoom",
        lead_id=lead_id,
        rep_id=_derive_rep_id("zoom"),
        direction=str(call_meta.get("direction") or "outbound"),
        outcome=str(legacy_analysis.get("outcome") or call_meta.get("result") or "unknown"),
        started_at=str(call_meta.get("date_time") or call_meta.get("start_time") or now_iso()),
        duration_seconds=int(call_meta.get("duration") or recording_meta.get("duration") or 0),
        recording_id=str(recording_meta.get("id") or call_meta.get("recording_id") or ""),
        audio_path=audio_path,
        metadata={
            "audio_uri": audio_uri,
            "audio_storage_status": "stored",
            "call_meta": call_meta,
            "recording_meta": recording_meta,
        },
        legacy_analysis=legacy_analysis,
    )


async def ingest_uploaded_recorded_call(
    session: AsyncSession,
    *,
    upload: UploadFile,
    lead_id: str = "",
    rep_id: str = "",
    source: str = "upload",
    call_type: str = "recorded_call",
    outcome: str = "uploaded",
    started_at: Optional[str] = None,
    duration_seconds: int = 0,
    transcript_hint: str = "",
) -> Dict[str, Any]:
    call_id = uuid.uuid4().hex
    audio_bytes = await upload.read()
    mime_type = upload.content_type or "application/octet-stream"
    audio_uri = await _persist_audio_bytes(
        source=source,
        call_id=call_id,
        audio_bytes=audio_bytes,
        mime_type=mime_type,
        filename=upload.filename or "",
    )
    audio_path = SPEECH_AUDIO_STORAGE_ROOT.parent / audio_uri
    pipeline_result = await _run_recorded_call_pipeline(
        session,
        call_id=call_id,
        source=source,
        lead_id=lead_id,
        rep_id=rep_id,
        direction="unknown",
        outcome=outcome,
        started_at=started_at or now_iso(),
        duration_seconds=duration_seconds,
        recording_id="",
        audio_path=audio_path,
        metadata={
            "audio_uri": audio_uri,
            "audio_storage_status": "stored",
            "upload_filename": upload.filename or "",
            "call_type": call_type,
        },
        legacy_analysis={
            "summary": "",
            "outcome": outcome,
            "key_topics": [],
            "objections": [],
            "next_step": "",
            "suggested_follow_up_task": "",
            "sentiment_label": "",
            "sentiment_confidence": 0,
            "sentiment_reason": "",
            "overall_confidence": 0,
        },
        transcript_hint=transcript_hint,
    )
    return {
        "call_id": call_id,
        "source": source,
        "status": "processed",
        "audio_uri": audio_uri,
        "pipeline": {
            "execution_mode": pipeline_result["execution_mode"],
            "orchestrator": pipeline_result["orchestrator"],
            "scoring_version": pipeline_result["scoring_version"],
        },
    }
