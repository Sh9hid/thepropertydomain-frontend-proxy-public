from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Dict


V0_SCORING_VERSION = "v0"
V0_COMPONENT_WEIGHTS = {
    "Fluency": Decimal("0.25"),
    "Confidence": Decimal("0.25"),
    "Sales Control": Decimal("0.25"),
    "Booking/Closing": Decimal("0.25"),
}
V0_SCORE_PRIORS = {
    "Fluency": Decimal("72"),
    "Confidence": Decimal("60"),
    "Sales Control": Decimal("62"),
    "Booking/Closing": Decimal("65"),
}
V0_SMOOTHING_CONSTANTS = {
    "Fluency": Decimal("6"),
    "Confidence": Decimal("5"),
    "Sales Control": Decimal("7"),
    "Booking/Closing": Decimal("4"),
}


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _quantize(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))


def _clamp(value: Decimal, minimum: Decimal = Decimal("0"), maximum: Decimal = Decimal("100")) -> Decimal:
    return max(minimum, min(maximum, value))


def _smoothing_factor(evidence_count: int, k: Decimal) -> Decimal:
    evidence = Decimal(str(max(evidence_count, 1)))
    return evidence / (evidence + k)


def score_recorded_call_v0(features: Dict[str, Any]) -> Dict[str, Any]:
    duration_seconds = max(int(features.get("duration_seconds") or 0), 1)
    word_count = int(features.get("word_count") or 0)
    filler_count = int(features.get("filler_count") or 0)
    long_pause_count = int(features.get("long_pause_count") or 0)
    hedge_count = int(features.get("hedge_count") or 0)
    question_count = int(features.get("question_count") or 0)
    objection_count = int(features.get("objection_count") or 0)
    control_phrase_count = int(features.get("control_phrase_count") or 0)
    confidence_phrase_count = int(features.get("confidence_phrase_count") or 0)
    evidence_count = int(features.get("evidence_count") or 0)

    words_per_minute = _decimal(word_count) * Decimal("60") / _decimal(duration_seconds)
    booking_attempted = bool(features.get("booking_attempted"))
    next_step_defined = bool(features.get("next_step_defined"))
    objection_resolved = bool(features.get("objection_resolved"))

    raw_scores = {
        "Fluency": _clamp(
            Decimal("89.775")
            - abs(words_per_minute - Decimal("100")) * Decimal("0.22")
            - _decimal(filler_count) * Decimal("1.1")
            - _decimal(long_pause_count) * Decimal("2.0")
            + (Decimal("4.0") if next_step_defined else Decimal("0"))
        ),
        "Confidence": _clamp(
            Decimal("65.582")
            + _decimal(confidence_phrase_count) * Decimal("2.0")
            - _decimal(hedge_count) * Decimal("1.7")
            - _decimal(long_pause_count) * Decimal("0.6")
            + (Decimal("3.5") if booking_attempted else Decimal("0"))
            + (Decimal("1.2") if next_step_defined else Decimal("0"))
        ),
        "Sales Control": _clamp(
            Decimal("58.213")
            + _decimal(question_count) * Decimal("1.5")
            + _decimal(control_phrase_count) * Decimal("2.1")
            + (Decimal("4.0") if next_step_defined else Decimal("0"))
            + (Decimal("3.0") if booking_attempted else Decimal("0"))
            - _decimal(objection_count) * Decimal("1.6")
        ),
        "Booking/Closing": _clamp(
            Decimal("60.033")
            + (Decimal("10.0") if booking_attempted else Decimal("0"))
            + (Decimal("6.0") if next_step_defined else Decimal("0"))
            + (Decimal("4.5") if objection_resolved else Decimal("0"))
            + _decimal(control_phrase_count) * Decimal("1.0")
            - _decimal(objection_count) * Decimal("4.0")
        ),
    }

    components: Dict[str, Dict[str, Any]] = {}
    composite = Decimal("0")
    for name, raw_score in raw_scores.items():
        prior = V0_SCORE_PRIORS[name]
        shrink = _smoothing_factor(evidence_count, V0_SMOOTHING_CONSTANTS[name])
        adjusted_score = _clamp(prior + shrink * (raw_score - prior))
        weight = V0_COMPONENT_WEIGHTS[name]
        rounded_component = _quantize(adjusted_score)
        composite += Decimal(str(rounded_component)) * weight
        components[name] = {
            "score_name": name,
            "score": rounded_component,
            "score_value": rounded_component,
            "raw_value": _quantize(raw_score / Decimal("100")),
            "normalized_value": _quantize(adjusted_score / Decimal("100")),
            "weight": float(weight),
            "evidence_count": evidence_count,
            "stable_flag": evidence_count >= int(V0_SMOOTHING_CONSTANTS[name]),
            "evidence": {
                "duration_seconds": duration_seconds,
                "word_count": word_count,
                "words_per_minute": _quantize(words_per_minute),
                "booking_attempted": booking_attempted,
                "next_step_defined": next_step_defined,
                "objection_count": objection_count,
            },
        }

    confidence = min(Decimal("0.95"), Decimal("0.45") + (_decimal(evidence_count) / Decimal("40")))
    return {
        "scoring_version": V0_SCORING_VERSION,
        "composite_score": _quantize(composite),
        "confidence": _quantize(confidence),
        "components": components,
    }


def _evidence_slice(items: list[Dict[str, Any]], limit: int = 2) -> list[Dict[str, Any]]:
    return [
        {
            "text": item.get("text", ""),
            "timestamp_ms": int(item.get("timestamp_ms") or 0),
            "speaker": item.get("speaker", ""),
        }
        for item in items[:limit]
    ]


def score_recorded_call_v1(features: Dict[str, Any]) -> Dict[str, Any]:
    questions = features.get("questions") or []
    objections = features.get("objections") or []
    keyword_events = features.get("keyword_events") or {}
    booking_intent = keyword_events.get("booking_intent") or []
    hesitation = keyword_events.get("hesitation") or []
    filler_events = features.get("filler_events") or []
    conversation_metrics = features.get("conversation_metrics") or {}

    interruptions = int(conversation_metrics.get("interruptions") or 0)
    talk_ratio = float(conversation_metrics.get("agent_talk_ratio") or 0.0)
    longest_monologue_ms = int(conversation_metrics.get("longest_agent_monologue_ms") or 0)
    avg_response_latency_ms = int(conversation_metrics.get("average_response_latency_ms") or 0)
    resolved_objections = [item for item in objections if item.get("resolved")]

    normalized_components = {
        "Fluency": {
            "metric": "fluency",
            "score": round(max(0.0, min(1.0, 1.0 - (len(filler_events) * 0.12) - (interruptions * 0.06))), 2),
            "evidence": _evidence_slice(filler_events or hesitation),
            "reason": "Scored from filler load and interruption pressure.",
        },
        "Confidence": {
            "metric": "confidence",
            "score": round(max(0.0, min(1.0, 0.72 - (len(hesitation) * 0.18) + (0.08 if booking_intent else 0.0))), 2),
            "evidence": _evidence_slice(hesitation or booking_intent),
            "reason": "Scored from hesitation language versus decisive booking language.",
        },
        "Sales Control": {
            "metric": "sales_control",
            "score": round(max(0.0, min(1.0, 0.45 + (len(questions) * 0.15) + (0.12 if 0.45 <= talk_ratio <= 0.7 else -0.08) - (0.08 if longest_monologue_ms > 4500 else 0.0))), 2),
            "evidence": _evidence_slice(questions or booking_intent),
            "reason": "Scored from question flow, talk balance, and monologue control.",
        },
        "Booking/Closing": {
            "metric": "booking_closing",
            "score": round(max(0.0, min(1.0, 0.35 + (len(booking_intent) * 0.25) + (len(resolved_objections) * 0.12) - (0.08 if avg_response_latency_ms > 1200 else 0.0))), 2),
            "evidence": _evidence_slice(booking_intent or resolved_objections or objections),
            "reason": "Scored from booking language, objection recovery, and close timing.",
        },
    }

    components: Dict[str, Dict[str, Any]] = {}
    for name, component in normalized_components.items():
        score_normalized = float(component["score"])
        components[name] = {
            **component,
            "normalized_score": round(score_normalized, 2),
            "score": round(score_normalized * 100, 2),
        }

    composite = round(sum(component["score"] for component in components.values()) / len(components), 2)
    return {
        "scoring_version": "v1",
        "composite_score": composite,
        "confidence": round(min(0.95, 0.5 + ((len(questions) + len(booking_intent) + len(objections)) / 20.0)), 2),
        "components": components,
    }
