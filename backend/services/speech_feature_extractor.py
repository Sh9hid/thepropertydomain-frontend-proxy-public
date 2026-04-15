from __future__ import annotations

import re
from typing import Any, Dict, List, Protocol

from core.config import SPEECH_FEATURE_EXTRACTOR_PROVIDER

_FILLER_TERMS = ("um", "uh", "like", "you know", "sort of", "kind of")
_OBJECTION_RULES = {
    "price": ("price", "expensive", "cost"),
    "timing": ("think about", "not ready", "later", "timing"),
    "trust": ("not sure", "uncertain", "concerned"),
}
_BOOKING_TERMS = ("book", "schedule", "lock in", "confirm a time")
_HESITATION_TERMS = ("think about", "maybe", "not sure", "later", "just")
_PRICING_TERMS = ("price", "rate", "cost", "repayment", "fee")
_POSITIVE_TERMS = ("works", "sounds good", "let's do it", "okay", "great")
_NEGATIVE_TERMS = ("too expensive", "not ready", "not interested", "concerned", "issue")


class SpeechFeatureExtractor(Protocol):
    def extract(self, *, conversation: List[Dict[str, Any]], conversation_metrics: Dict[str, Any], legacy_analysis: Dict[str, Any]) -> Dict[str, Any]:
        ...


def _contains_question(text: str) -> bool:
    lowered = text.lower()
    return "?" in text or lowered.startswith(("would ", "could ", "can ", "are ", "is ", "do ", "does ", "did ", "what ", "when ", "why ", "how "))


def _extract_questions(conversation: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {"text": turn["text"], "timestamp_ms": turn["start_ms"], "speaker": turn["speaker"]}
        for turn in conversation
        if turn["speaker"] == "agent" and _contains_question(turn["text"])
    ]


def _extract_objections(conversation: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    objections: List[Dict[str, Any]] = []
    for index, turn in enumerate(conversation):
        if turn["speaker"] != "customer":
            continue
        lowered = turn["text"].lower()
        for label, terms in _OBJECTION_RULES.items():
            if any(term in lowered for term in terms):
                resolved = False
                if index + 1 < len(conversation):
                    next_turn = conversation[index + 1]
                    if next_turn["speaker"] == "agent":
                        resolved = any(term in next_turn["text"].lower() for term in _BOOKING_TERMS + ("compare", "options"))
                objections.append(
                    {
                        "label": label,
                        "text": turn["text"],
                        "timestamp_ms": turn["start_ms"],
                        "speaker": turn["speaker"],
                        "resolved": resolved,
                    }
                )
                break
    return objections


def _extract_keyword_events(conversation: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    events = {"booking_intent": [], "hesitation": [], "pricing": []}
    for turn in conversation:
        lowered = turn["text"].lower()
        if turn["speaker"] == "agent" and any(term in lowered for term in _BOOKING_TERMS):
            text = "lock in a time" if "lock in" in lowered else turn["text"]
            events["booking_intent"].append({"text": text, "timestamp_ms": turn["start_ms"], "speaker": turn["speaker"]})
        if any(term in lowered for term in _HESITATION_TERMS):
            events["hesitation"].append({"text": turn["text"], "timestamp_ms": turn["start_ms"], "speaker": turn["speaker"]})
        if any(term in lowered for term in _PRICING_TERMS):
            events["pricing"].append({"text": turn["text"], "timestamp_ms": turn["start_ms"], "speaker": turn["speaker"]})
    return events


def _extract_filler_events(conversation: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for turn in conversation:
        lowered = turn["text"].lower()
        for token in _FILLER_TERMS:
            if token in lowered:
                events.append({"text": token, "timestamp_ms": turn["start_ms"], "speaker": turn["speaker"]})
    return events


def _score_sentiment(conversation: List[Dict[str, Any]], objections: List[Dict[str, Any]]) -> Dict[str, Any]:
    positive_hits = 0
    negative_hits = 0
    for turn in conversation:
        lowered = turn["text"].lower()
        positive_hits += sum(lowered.count(term) for term in _POSITIVE_TERMS)
        negative_hits += sum(lowered.count(term) for term in _NEGATIVE_TERMS)
    if objections:
        negative_hits += len(objections)

    if positive_hits and negative_hits:
        return {"label": "mixed", "score": 0.15}
    if positive_hits:
        return {"label": "positive", "score": 0.55}
    if negative_hits:
        return {"label": "negative", "score": -0.55}
    return {"label": "neutral", "score": 0.0}


def extract_features_v1(
    *,
    conversation: List[Dict[str, Any]],
    conversation_metrics: Dict[str, Any],
    legacy_analysis: Dict[str, Any],
) -> Dict[str, Any]:
    questions = _extract_questions(conversation)
    objections = _extract_objections(conversation)
    keyword_events = _extract_keyword_events(conversation)
    filler_events = _extract_filler_events(conversation)
    sentiment = _score_sentiment(conversation, objections)

    return {
        "provider": SPEECH_FEATURE_EXTRACTOR_PROVIDER,
        "questions": questions,
        "objections": objections,
        "keyword_events": keyword_events,
        "filler_events": filler_events,
        "sentiment": sentiment,
        "conversation_metrics": conversation_metrics,
        "legacy_outcome": str(legacy_analysis.get("outcome") or ""),
    }


class V1SpeechFeatureExtractor:
    def extract(self, *, conversation: List[Dict[str, Any]], conversation_metrics: Dict[str, Any], legacy_analysis: Dict[str, Any]) -> Dict[str, Any]:
        return extract_features_v1(
            conversation=conversation,
            conversation_metrics=conversation_metrics,
            legacy_analysis=legacy_analysis,
        )


def get_speech_feature_extractor() -> SpeechFeatureExtractor:
    return V1SpeechFeatureExtractor()
