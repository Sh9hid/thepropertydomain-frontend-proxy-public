from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Protocol

import httpx

from core.config import SPEECH_TRANSCRIPTION_PROVIDER

_DEEPGRAM_URL = os.getenv("DEEPGRAM_BASE_URL", "https://api.deepgram.com/v1/listen")
_DEEPGRAM_MODEL = os.getenv("DEEPGRAM_MODEL", "nova-2")
_DEEPGRAM_LANGUAGE = os.getenv("DEEPGRAM_LANGUAGE", "en-AU")
_logger = logging.getLogger(__name__)


class TranscriptionProvider(Protocol):
    async def transcribe(self, *, call_id: str, audio_path: Path, context: Dict[str, Any]) -> Dict[str, Any]:
        ...


def _build_stub_response(transcript_hint: str) -> Dict[str, Any]:
    return {
        "provider": "stub",
        "version_type": "canonical",
        "language": _DEEPGRAM_LANGUAGE,
        "full_text": transcript_hint,
        "confidence": 0.0,
        "segments": [],
        "words": [],
        "speakers": [],
        "status": "stubbed",
    }


def _resolve_speaker_roles(utterances: List[Dict[str, Any]], direction: str) -> Dict[int, str]:
    ordered_speakers: List[int] = []
    for utterance in utterances:
        speaker_id = int(utterance.get("speaker", 0))
        if speaker_id not in ordered_speakers:
            ordered_speakers.append(speaker_id)

    roles: Dict[int, str] = {}
    if not ordered_speakers:
        return roles

    first_role = "agent" if direction == "outbound" else "customer"
    second_role = "customer" if first_role == "agent" else "agent"

    roles[ordered_speakers[0]] = first_role
    if len(ordered_speakers) > 1:
        roles[ordered_speakers[1]] = second_role
    for speaker_id in ordered_speakers[2:]:
        roles[speaker_id] = f"speaker_{speaker_id}"
    return roles


def map_deepgram_payload_to_transcript(payload: Dict[str, Any], direction: str = "outbound") -> Dict[str, Any]:
    results = payload.get("results") or {}
    channels = results.get("channels") or []
    alternatives = ((channels[0] or {}).get("alternatives") or [{}]) if channels else [{}]
    alternative = alternatives[0] or {}
    utterances = results.get("utterances") or []
    roles_by_speaker = _resolve_speaker_roles(utterances, direction)

    speakers: List[Dict[str, Any]] = []
    seen_labels = set()
    for utterance in utterances:
        speaker_id = int(utterance.get("speaker", 0))
        label = f"speaker_{speaker_id}"
        if label in seen_labels:
            continue
        seen_labels.add(label)
        speakers.append(
            {
                "label": label,
                "role": roles_by_speaker.get(speaker_id, label),
                "confidence": float(utterance.get("confidence") or alternative.get("confidence") or 0.0),
            }
        )

    segments = [
        {
            "speaker_label": f"speaker_{int(utterance.get('speaker', 0))}",
            "speaker_role": roles_by_speaker.get(int(utterance.get("speaker", 0)), "unknown"),
            "text": str(utterance.get("transcript") or "").strip(),
            "start_ms": int(float(utterance.get("start") or 0.0) * 1000),
            "end_ms": int(float(utterance.get("end") or 0.0) * 1000),
            "confidence": float(utterance.get("confidence") or alternative.get("confidence") or 0.0),
        }
        for utterance in utterances
        if str(utterance.get("transcript") or "").strip()
    ]

    words = [
        {
            "speaker_label": f"speaker_{int(word.get('speaker', 0))}",
            "speaker_role": roles_by_speaker.get(int(word.get("speaker", 0)), "unknown"),
            "word": str(word.get("punctuated_word") or word.get("word") or "").strip(),
            "start_ms": int(float(word.get("start") or 0.0) * 1000),
            "end_ms": int(float(word.get("end") or 0.0) * 1000),
            "confidence": float(word.get("confidence") or 0.0),
        }
        for word in alternative.get("words") or []
        if str(word.get("word") or "").strip()
    ]

    return {
        "provider": "deepgram",
        "version_type": "canonical",
        "language": _DEEPGRAM_LANGUAGE,
        "full_text": str(alternative.get("transcript") or "").strip(),
        "confidence": float(alternative.get("confidence") or 0.0),
        "segments": segments,
        "words": words,
        "speakers": speakers,
        "status": "completed",
        "raw_payload": payload,
    }


class StubTranscriptionProvider:
    async def transcribe(self, *, call_id: str, audio_path: Path, context: Dict[str, Any]) -> Dict[str, Any]:
        return _build_stub_response(str(context.get("transcript_hint") or "").strip())


class DeepgramTranscriptionProvider:
    async def transcribe(self, *, call_id: str, audio_path: Path, context: Dict[str, Any]) -> Dict[str, Any]:
        api_key = os.getenv("DEEPGRAM_API_KEY", "").strip()
        transcript_hint = str(context.get("transcript_hint") or "").strip()
        if not api_key or not audio_path.exists() or audio_path.stat().st_size == 0:
            _logger.info(
                "transcription_provider fallback_to_stub call_id=%s reason=%s",
                call_id,
                "missing_api_key_or_audio",
            )
            return _build_stub_response(transcript_hint)

        params = {
            "model": _DEEPGRAM_MODEL,
            "language": _DEEPGRAM_LANGUAGE,
            "punctuate": "true",
            "smart_format": "true",
            "diarize": "true",
            "utterances": "true",
        }
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    _DEEPGRAM_URL,
                    params=params,
                    headers={
                        "Authorization": f"Token {api_key}",
                        "Content-Type": "application/octet-stream",
                    },
                    content=audio_path.read_bytes(),
                )
                response.raise_for_status()
            mapped = map_deepgram_payload_to_transcript(response.json(), direction=str(context.get("direction") or "outbound"))
            _logger.info(
                "transcription_provider deepgram_success call_id=%s words=%s segments=%s",
                call_id,
                len(mapped.get("words") or []),
                len(mapped.get("segments") or []),
            )
            return mapped
        except Exception as exc:
            _logger.warning(
                "transcription_provider deepgram_failed call_id=%s error=%s",
                call_id,
                exc,
            )
            return _build_stub_response(transcript_hint)


def get_transcription_provider() -> TranscriptionProvider:
    provider_name = (SPEECH_TRANSCRIPTION_PROVIDER or "").strip().lower()
    if provider_name in {"deepgram", "deepgram_api"}:
        return DeepgramTranscriptionProvider()
    return StubTranscriptionProvider()
