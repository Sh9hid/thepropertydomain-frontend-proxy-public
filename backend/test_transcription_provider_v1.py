import pytest

from services.transcription_provider import (
    DeepgramTranscriptionProvider,
    map_deepgram_payload_to_transcript,
)


def test_map_deepgram_payload_to_transcript_builds_segments_words_and_roles():
    payload = {
        "results": {
            "channels": [
                {
                    "alternatives": [
                        {
                            "transcript": "Hello there. Would Thursday work for you? Thursday works.",
                            "confidence": 0.94,
                            "words": [
                                {"word": "Hello", "start": 0.0, "end": 0.4, "confidence": 0.98, "speaker": 0},
                                {"word": "there", "start": 0.41, "end": 0.8, "confidence": 0.97, "speaker": 0},
                                {"word": "Would", "start": 1.0, "end": 1.2, "confidence": 0.94, "speaker": 0},
                                {"word": "Thursday", "start": 1.21, "end": 1.7, "confidence": 0.94, "speaker": 0},
                                {"word": "work", "start": 1.71, "end": 2.0, "confidence": 0.94, "speaker": 0},
                                {"word": "for", "start": 2.01, "end": 2.1, "confidence": 0.94, "speaker": 0},
                                {"word": "you", "start": 2.11, "end": 2.3, "confidence": 0.94, "speaker": 0},
                                {"word": "Thursday", "start": 2.8, "end": 3.2, "confidence": 0.96, "speaker": 1},
                                {"word": "works", "start": 3.21, "end": 3.6, "confidence": 0.96, "speaker": 1},
                            ],
                        }
                    ]
                }
            ],
            "utterances": [
                {
                    "speaker": 0,
                    "start": 0.0,
                    "end": 2.3,
                    "confidence": 0.95,
                    "transcript": "Hello there. Would Thursday work for you?",
                },
                {
                    "speaker": 1,
                    "start": 2.8,
                    "end": 3.6,
                    "confidence": 0.96,
                    "transcript": "Thursday works.",
                },
            ],
        }
    }

    result = map_deepgram_payload_to_transcript(payload, direction="outbound")

    assert result["provider"] == "deepgram"
    assert result["full_text"].startswith("Hello there.")
    assert len(result["segments"]) == 2
    assert result["segments"][0]["speaker_role"] == "agent"
    assert result["segments"][1]["speaker_role"] == "customer"
    assert result["segments"][0]["start_ms"] == 0
    assert result["words"][7]["speaker_role"] == "customer"
    assert result["speakers"][0]["label"] == "speaker_0"


def test_map_deepgram_payload_to_transcript_handles_single_speaker_payload():
    payload = {
        "results": {
            "channels": [
                {
                    "alternatives": [
                        {
                            "transcript": "Hello there.",
                            "confidence": 0.91,
                            "words": [
                                {"word": "Hello", "start": 0.0, "end": 0.4, "confidence": 0.94, "speaker": 0},
                                {"word": "there", "start": 0.41, "end": 0.8, "confidence": 0.93, "speaker": 0},
                            ],
                        }
                    ]
                }
            ],
            "utterances": [
                {
                    "speaker": 0,
                    "start": 0.0,
                    "end": 0.8,
                    "confidence": 0.92,
                    "transcript": "Hello there.",
                }
            ],
        }
    }

    result = map_deepgram_payload_to_transcript(payload, direction="outbound")

    assert len(result["segments"]) == 1
    assert result["segments"][0]["speaker_role"] == "agent"
    assert result["words"][0]["speaker_role"] == "agent"


@pytest.mark.asyncio
async def test_deepgram_provider_falls_back_to_stub_without_api_key(tmp_path, monkeypatch):
    audio_path = tmp_path / "call.wav"
    audio_path.write_bytes(b"not-real-audio")
    monkeypatch.delenv("DEEPGRAM_API_KEY", raising=False)

    provider = DeepgramTranscriptionProvider()
    result = await provider.transcribe(
        call_id="call-123",
        audio_path=audio_path,
        context={"transcript_hint": "Fallback transcript hint"},
    )

    assert result["status"] == "stubbed"
    assert result["full_text"] == "Fallback transcript hint"


@pytest.mark.asyncio
async def test_deepgram_provider_falls_back_to_stub_for_empty_audio(tmp_path, monkeypatch):
    audio_path = tmp_path / "empty.wav"
    audio_path.write_bytes(b"")
    monkeypatch.setenv("DEEPGRAM_API_KEY", "test-key")

    provider = DeepgramTranscriptionProvider()
    result = await provider.transcribe(
        call_id="call-empty",
        audio_path=audio_path,
        context={"transcript_hint": "Empty audio fallback"},
    )

    assert result["status"] == "stubbed"
    assert result["full_text"] == "Empty audio fallback"
