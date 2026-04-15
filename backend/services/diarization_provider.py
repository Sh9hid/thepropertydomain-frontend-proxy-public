from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Protocol

from core.config import SPEECH_DIARIZATION_PROVIDER


class DiarizationProvider(Protocol):
    async def diarize(self, *, call_id: str, audio_path: Path, context: Dict[str, Any]) -> Dict[str, Any]:
        ...


class StubDiarizationProvider:
    async def diarize(self, *, call_id: str, audio_path: Path, context: Dict[str, Any]) -> Dict[str, Any]:
        transcription = context.get("transcription") or {}
        speakers = transcription.get("speakers") or [
            {"label": "speaker_rep", "role": "agent", "confidence": 0.5},
            {"label": "speaker_customer", "role": "customer", "confidence": 0.5},
        ]
        return {
            "provider": "stub",
            "status": "completed" if transcription.get("segments") else "stubbed",
            "speakers": speakers,
            "segments": transcription.get("segments") or [],
            "direction": str(context.get("direction") or "outbound").lower(),
        }


class ProviderDiarizationProvider:
    async def diarize(self, *, call_id: str, audio_path: Path, context: Dict[str, Any]) -> Dict[str, Any]:
        transcription = context.get("transcription") or {}
        if transcription.get("segments") and transcription.get("speakers"):
            return {
                "provider": SPEECH_DIARIZATION_PROVIDER,
                "status": "completed",
                "speakers": transcription.get("speakers") or [],
                "segments": transcription.get("segments") or [],
                "direction": str(context.get("direction") or "outbound").lower(),
            }
        return await StubDiarizationProvider().diarize(call_id=call_id, audio_path=audio_path, context=context)


def get_diarization_provider() -> DiarizationProvider:
    provider_name = (SPEECH_DIARIZATION_PROVIDER or "").strip().lower()
    if provider_name in {"provider", "deepgram", "transcription"}:
        return ProviderDiarizationProvider()
    return StubDiarizationProvider()
