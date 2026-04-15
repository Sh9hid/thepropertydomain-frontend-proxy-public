from services.scoring_engine import score_recorded_call_v0


def test_score_recorded_call_v0_is_deterministic_and_smoothed():
    result = score_recorded_call_v0(
        {
            "duration_seconds": 180,
            "word_count": 252,
            "filler_count": 4,
            "long_pause_count": 2,
            "hedge_count": 3,
            "question_count": 5,
            "booking_attempted": True,
            "next_step_defined": True,
            "objection_count": 1,
            "objection_resolved": True,
            "control_phrase_count": 3,
            "confidence_phrase_count": 4,
            "evidence_count": 12,
        }
    )

    assert result["scoring_version"] == "v0"
    assert round(result["composite_score"], 2) == 73.66
    assert round(result["components"]["Fluency"]["score"], 2) == 78.57
    assert round(result["components"]["Confidence"]["score"], 2) == 68.46
    assert round(result["components"]["Sales Control"]["score"], 2) == 71.73
    assert round(result["components"]["Booking/Closing"]["score"], 2) == 75.90
    assert result["components"]["Booking/Closing"]["evidence_count"] == 12

