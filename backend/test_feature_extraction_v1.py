from services.speech_feature_extractor import extract_features_v1


def test_extract_features_v1_detects_questions_objections_keywords_and_sentiment():
    conversation = [
        {"speaker": "agent", "text": "Would Thursday work for you if we keep the review tight?", "start_ms": 0, "end_ms": 2200},
        {"speaker": "customer", "text": "I need to think about the price first.", "start_ms": 2500, "end_ms": 4500},
        {"speaker": "agent", "text": "That's fair. We can compare options and lock in a time.", "start_ms": 5000, "end_ms": 8000},
        {"speaker": "customer", "text": "Okay, Thursday afternoon works.", "start_ms": 8500, "end_ms": 10000},
    ]

    metrics = {
        "agent_talk_ratio": 0.61,
        "interruptions": 0,
        "longest_agent_monologue_ms": 3000,
        "average_response_latency_ms": 250,
    }

    features = extract_features_v1(conversation=conversation, conversation_metrics=metrics, legacy_analysis={})

    assert features["questions"][0]["text"].startswith("Would Thursday work")
    assert features["objections"][0]["label"] == "price"
    assert features["keyword_events"]["booking_intent"][0]["text"] == "lock in a time"
    assert features["keyword_events"]["hesitation"][0]["text"] == "I need to think about the price first."
    assert features["sentiment"]["label"] == "mixed"
