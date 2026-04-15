from services.zoom_call_analysis_service import _sanitize_sentiment


def test_sanitize_sentiment_omits_low_confidence_sentiment():
    payload = _sanitize_sentiment(
        {
            "summary": "Call summary",
            "sentiment_label": "negative",
            "sentiment_confidence": 0.42,
            "sentiment_reason": "Weak signal",
        }
    )
    assert payload["sentiment_label"] == ""
    assert payload["sentiment_confidence"] == 0
    assert payload["sentiment_reason"] == ""


def test_sanitize_sentiment_keeps_high_confidence_sentiment():
    payload = _sanitize_sentiment(
        {
            "summary": "Call summary",
            "sentiment_label": "positive",
            "sentiment_confidence": 0.91,
            "sentiment_reason": "Caller was clearly receptive",
        }
    )
    assert payload["sentiment_label"] == "positive"
    assert payload["sentiment_confidence"] == 0.91
    assert payload["sentiment_reason"] == "Caller was clearly receptive"
