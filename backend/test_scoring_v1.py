from services.scoring_engine import score_recorded_call_v1


def test_score_recorded_call_v1_returns_evidence_and_reasoning():
    features = {
        "questions": [
            {"text": "Would Thursday work for you?", "timestamp_ms": 1000, "speaker": "agent"},
        ],
        "objections": [
            {
                "label": "price",
                "text": "I need to think about the price first.",
                "timestamp_ms": 2600,
                "speaker": "customer",
                "resolved": True,
            }
        ],
        "keyword_events": {
            "booking_intent": [
                {"text": "Let's lock in Thursday afternoon.", "timestamp_ms": 5200, "speaker": "agent"},
            ],
            "hesitation": [
                {"text": "I need to think about the price first.", "timestamp_ms": 2600, "speaker": "customer"},
            ],
            "pricing": [
                {"text": "I need to think about the price first.", "timestamp_ms": 2600, "speaker": "customer"},
            ],
        },
        "conversation_metrics": {
            "agent_talk_ratio": 0.61,
            "interruptions": 1,
            "longest_agent_monologue_ms": 3000,
            "average_response_latency_ms": 180,
        },
        "filler_events": [
            {"text": "um", "timestamp_ms": 600, "speaker": "agent"},
        ],
        "sentiment": {"label": "mixed", "score": 0.15},
    }

    result = score_recorded_call_v1(features)

    sales_control = result["components"]["Sales Control"]
    assert result["scoring_version"] == "v1"
    assert sales_control["evidence"][0]["text"] == "Would Thursday work for you?"
    assert "question" in sales_control["reason"].lower()
    assert result["components"]["Booking/Closing"]["score"] > result["components"]["Confidence"]["score"]
