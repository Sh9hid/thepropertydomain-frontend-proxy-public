from services.speech_pipeline_service import (
    build_call_analysis_payload,
    build_coaching_report_payload,
)


def test_build_call_analysis_payload_shapes_components_and_objections():
    payload = build_call_analysis_payload(
        call_row={
            "id": "call-123",
            "source": "zoom",
            "lead_id": "lead-1",
            "rep_id": "rep-7",
            "outcome": "booked_appraisal",
            "duration_seconds": 180,
            "analysis_status": "completed",
            "audio_uri": "/speech/zoom/call-123.mp3",
            "started_at": "2026-03-23T10:00:00Z",
            "ended_at": "2026-03-23T10:03:00Z",
            "created_at": "2026-03-23T10:03:05Z",
            "updated_at": "2026-03-23T10:03:05Z",
        },
        transcript_rows=[
            {
                "id": "tx-1",
                "provider": "stub",
                "version_type": "canonical",
                "language": "en-AU",
                "full_text": "Thanks for taking the call. Let's book a time on Thursday.",
                "confidence": 0.0,
                "created_at": "2026-03-23T10:03:05Z",
            }
        ],
        objection_rows=[
            {
                "id": "obj-1",
                "objection_type": "timing",
                "normalized_text": "Need to think about timing",
                "detected_at_ms": 92000,
                "response_quality_score": 0.8,
                "resolved_flag": 1,
            }
        ],
        snapshot_row={
            "id": "snap-1",
            "scoring_version": "v0",
            "composite_score": 74.2,
            "confidence": 0.82,
            "computed_at": "2026-03-23T10:03:06Z",
        },
        component_rows=[
            {
                "score_name": "Confidence",
                "score_value": 68.46,
                "raw_value": 0.63,
                "normalized_value": 0.71,
                "weight": 0.25,
                "evidence_json": "{\"hedge_count\": 3}",
            },
            {
                "score_name": "Booking/Closing",
                "score_value": 75.9,
                "raw_value": 0.78,
                "normalized_value": 0.76,
                "weight": 0.25,
                "evidence_json": "{\"booking_attempted\": true}",
            },
        ],
        legacy_analysis_row={
            "summary": "Strong consultative call with a booked appraisal.",
            "outcome": "qualified",
            "key_topics": "[\"appraisal\", \"timing\"]",
            "objections": "[\"timing\"]",
            "next_step": "Confirm Thursday appraisal time by SMS.",
            "suggested_follow_up_task": "Send SMS confirmation.",
            "sentiment_label": "positive",
            "sentiment_confidence": 0.91,
            "sentiment_reason": "Prospect agreed to a next step.",
            "overall_confidence": 0.88,
            "analyzed_at": "2026-03-23T10:03:05Z",
        },
    )

    assert payload["call"]["id"] == "call-123"
    assert payload["analysis"]["summary"] == "Strong consultative call with a booked appraisal."
    assert payload["analysis"]["transcript"]["full_text"].startswith("Thanks for taking the call.")
    assert payload["analysis"]["objections"][0]["objection_type"] == "timing"
    assert payload["scores"]["composite_score"] == 74.2
    assert payload["scores"]["components"][0]["score_name"] == "Booking/Closing"


def test_build_coaching_report_payload_shapes_rewrites_and_drills():
    payload = build_coaching_report_payload(
        report_row={
            "id": "report-1",
            "call_id": "call-123",
            "rep_id": "rep-7",
            "report_version": "v0",
            "brutal_summary": "Clear intent, but too hedgey before the close.",
            "detailed_breakdown_json": "{\"priorities\": [\"tighten the close\"]}",
            "rewrite_json": "{\"before\": \"maybe we can\", \"after\": \"let's lock in\"}",
            "drills_json": "[\"Say the close in one breath\", \"Ask one direct booking question\"]",
            "live_task": "Use one direct booking question on the next live call.",
            "generated_at": "2026-03-23T10:05:00Z",
        }
    )

    assert payload["id"] == "report-1"
    assert payload["call_id"] == "call-123"
    assert payload["rewrite"]["after"] == "let's lock in"
    assert payload["drills"][0] == "Say the close in one breath"


def test_build_call_analysis_payload_includes_structured_conversation_and_evidence():
    payload = build_call_analysis_payload(
        call_row={
            "id": "call-v1",
            "metadata_json": "{\"analysis_cache\": {\"conversation_metrics\": {\"agent_talk_ratio\": 0.6}, \"features\": {\"questions\": []}, \"sales_analysis\": {\"booking_attempted\": true}}}",
            "outcome": "qualified",
        },
        transcript_rows=[
            {
                "id": "tx-v1",
                "provider": "deepgram",
                "version_type": "canonical",
                "language": "en-AU",
                "full_text": "Would Thursday work for you? Let's lock in Thursday afternoon.",
                "confidence": 0.91,
                "created_at": "2026-03-24T11:00:00Z",
            }
        ],
        objection_rows=[],
        snapshot_row={
            "id": "snap-v1",
            "scoring_version": "v1",
            "composite_score": 69.5,
            "confidence": 0.74,
            "computed_at": "2026-03-24T11:01:00Z",
        },
        component_rows=[
            {
                "score_name": "Sales Control",
                "score_value": 72.0,
                "evidence_json": "{\"metric\": \"sales_control\", \"reason\": \"Strong question flow.\", \"evidence\": [{\"text\": \"Would Thursday work for you?\", \"timestamp_ms\": 0}], \"score\": 72.0}",
            }
        ],
        legacy_analysis_row={
            "summary": "Consultative call with a direct next step.",
            "outcome": "qualified",
            "key_topics": "[\"booking\"]",
            "next_step": "Confirm Thursday.",
            "suggested_follow_up_task": "Send confirmation.",
            "sentiment_label": "positive",
            "sentiment_confidence": 0.8,
            "sentiment_reason": "Clear agreement",
            "overall_confidence": 0.74,
            "analyzed_at": "2026-03-24T11:01:00Z",
        },
        speaker_rows=[
            {"id": "sp-1", "diarization_label": "speaker_0", "role": "agent"},
        ],
        segment_rows=[
            {"speaker_id": "sp-1", "text": "Would Thursday work for you?", "start_ms": 0, "end_ms": 2200, "confidence": 0.9, "turn_index": 0},
        ],
        word_rows=[
            {"word": "Would", "start_ms": 0, "end_ms": 200},
        ],
    )

    assert payload["analysis"]["structured_conversation"][0]["speaker"] == "agent"
    assert payload["analysis"]["conversation_metrics"]["agent_talk_ratio"] == 0.6
    assert payload["scores"]["components"][0]["reason"] == "Strong question flow."
    assert payload["scores"]["components"][0]["evidence"][0]["text"] == "Would Thursday work for you?"
