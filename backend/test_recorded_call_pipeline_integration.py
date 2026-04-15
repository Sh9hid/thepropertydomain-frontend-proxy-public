import sys
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from services.speech_pipeline_service import _run_recorded_call_pipeline, ensure_speech_schema

_VENDORED_SITE_PACKAGES = Path(__file__).resolve().parent / "stealth_venv" / "Lib" / "site-packages"
if _VENDORED_SITE_PACKAGES.exists() and str(_VENDORED_SITE_PACKAGES) not in sys.path:
    sys.path.append(str(_VENDORED_SITE_PACKAGES))


class _FakeTranscriptionProvider:
    async def transcribe(self, *, call_id: str, audio_path: Path, context):
        return {
            "provider": "fake",
            "version_type": "canonical",
            "language": "en-AU",
            "full_text": "Would Thursday work for you? I need to think about the price first. Let's lock in Thursday afternoon.",
            "confidence": 0.93,
            "status": "completed",
            "speakers": [
                {"label": "speaker_0", "role": "agent", "confidence": 0.93},
                {"label": "speaker_1", "role": "customer", "confidence": 0.91},
            ],
            "segments": [
                {"speaker_label": "speaker_0", "speaker_role": "agent", "text": "Would Thursday work for you?", "start_ms": 0, "end_ms": 2200, "confidence": 0.94},
                {"speaker_label": "speaker_1", "speaker_role": "customer", "text": "I need to think about the price first.", "start_ms": 2600, "end_ms": 4500, "confidence": 0.9},
                {"speaker_label": "speaker_0", "speaker_role": "agent", "text": "Let's lock in Thursday afternoon.", "start_ms": 5000, "end_ms": 7200, "confidence": 0.95},
            ],
            "words": [
                {"speaker_label": "speaker_0", "speaker_role": "agent", "word": "Would", "start_ms": 0, "end_ms": 300, "confidence": 0.95},
                {"speaker_label": "speaker_0", "speaker_role": "agent", "word": "Thursday", "start_ms": 301, "end_ms": 650, "confidence": 0.95},
                {"speaker_label": "speaker_0", "speaker_role": "agent", "word": "work", "start_ms": 651, "end_ms": 850, "confidence": 0.95},
                {"speaker_label": "speaker_1", "speaker_role": "customer", "word": "price", "start_ms": 3500, "end_ms": 3900, "confidence": 0.91},
                {"speaker_label": "speaker_0", "speaker_role": "agent", "word": "lock", "start_ms": 5400, "end_ms": 5600, "confidence": 0.94},
                {"speaker_label": "speaker_0", "speaker_role": "agent", "word": "in", "start_ms": 5601, "end_ms": 5700, "confidence": 0.94},
            ],
        }


class _FakeDiarizationProvider:
    async def diarize(self, *, call_id: str, audio_path: Path, context):
        transcription = context["transcription"]
        return {
            "provider": "fake",
            "status": "completed",
            "speakers": transcription["speakers"],
            "segments": transcription["segments"],
        }


@pytest.mark.asyncio
async def test_run_recorded_call_pipeline_persists_segments_words_scores_and_coaching(tmp_path, monkeypatch):
    audio_path = tmp_path / "call.wav"
    audio_path.write_bytes(b"fake audio")

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    import services.speech_pipeline_service as pipeline_service

    monkeypatch.setattr(pipeline_service, "get_transcription_provider", lambda: _FakeTranscriptionProvider())
    monkeypatch.setattr(pipeline_service, "get_diarization_provider", lambda: _FakeDiarizationProvider())

    async with session_factory() as session:
        await ensure_speech_schema(session)
        result = await _run_recorded_call_pipeline(
            session,
            call_id="call-v1-1",
            source="upload",
            lead_id="lead-1",
            rep_id="rep-1",
            direction="outbound",
            outcome="qualified",
            started_at="2026-03-24T10:00:00Z",
            duration_seconds=120,
            recording_id="",
            audio_path=audio_path,
            metadata={"audio_uri": "speech_audio/upload/call-v1-1.wav", "audio_storage_status": "stored"},
            legacy_analysis={"summary": "", "outcome": "qualified", "key_topics": [], "objections": [], "next_step": ""},
        )
        await session.commit()

        segment_count = (await session.execute(text("SELECT COUNT(*) FROM call_segments WHERE call_id = 'call-v1-1'"))).scalar_one()
        word_count = (await session.execute(text("SELECT COUNT(*) FROM word_timestamps WHERE call_id = 'call-v1-1'"))).scalar_one()
        evidence_json = (
            await session.execute(text("SELECT evidence_json FROM score_components WHERE call_id = 'call-v1-1' ORDER BY score_value DESC LIMIT 1"))
        ).scalar_one()
        report_json = (
            await session.execute(text("SELECT detailed_breakdown_json FROM coaching_reports WHERE call_id = 'call-v1-1'"))
        ).scalar_one()

    await engine.dispose()

    assert result["scoring_version"] == "v1"
    assert segment_count == 3
    assert word_count == 6
    assert "\"evidence\"" in evidence_json
    assert "\"strengths\"" in report_json
