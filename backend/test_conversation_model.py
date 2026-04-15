from services.conversation_model import build_structured_conversation, compute_conversation_metrics


def test_compute_conversation_metrics_tracks_talk_ratio_interruptions_and_latency():
    segments = [
        {"speaker_role": "agent", "text": "Opening line", "start_ms": 0, "end_ms": 2000, "confidence": 0.9},
        {"speaker_role": "customer", "text": "Quick reply", "start_ms": 2200, "end_ms": 3000, "confidence": 0.9},
        {"speaker_role": "agent", "text": "Long explanation", "start_ms": 3100, "end_ms": 7000, "confidence": 0.9},
        {"speaker_role": "customer", "text": "Interruption", "start_ms": 6800, "end_ms": 9000, "confidence": 0.9},
    ]

    conversation = build_structured_conversation(segments)
    metrics = compute_conversation_metrics(conversation)

    assert conversation[0]["speaker"] == "agent"
    assert round(metrics["agent_talk_ratio"], 2) == 0.66
    assert metrics["interruptions"] == 1
    assert metrics["longest_agent_monologue_ms"] == 3900
    assert metrics["average_response_latency_ms"] == 100
