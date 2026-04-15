from __future__ import annotations

import re
from typing import Iterable


_FILLER_PATTERNS = [
    r"\b(?:um+|uh+|erm+|ah+|mm+)\b",
    r"\byou know\b",
    r"\bkind of\b",
    r"\bsort of\b",
]

_INTENT_RULES: list[tuple[float, tuple[str, ...]]] = [
    (0.9, (r"\bready to sell\b", r"\bwe(?:'| a)?re ready to sell\b")),
    (0.7, (r"\bthinking of selling\b", r"\bconsidering selling\b")),
    (0.3, (r"\bjust looking\b", r"\bjust researching\b", r"\bexploring options\b")),
    (0.1, (r"\bnot interested\b",)),
]

_BOOKING_PATTERNS = (
    r"\blet(?:'|’)s meet\b",
    r"\bbook(?: the)? appraisal\b",
    r"\bcome see\b",
    r"\bcome and see\b",
    r"\bmeet next\b",
)

_NEXT_STEP_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("call me back", (r"\bcall me back\b",)),
    ("next week", (r"\bnext week\b",)),
    ("after x", (r"\bafter [a-z0-9][a-z0-9\s-]{0,20}\b",)),
]

_OBJECTION_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("not now", (r"\bnot now\b", r"\bnot right now\b")),
    ("already have agent", (r"\balready have (?:an? )?agent\b", r"\bworking with (?:an? )?agent\b", r"\balready listed with\b")),
    ("price too low", (r"\bprice(?: is| feels| seems| was)? too low\b", r"\btoo low\b", r"\bnot enough\b")),
    ("just researching", (r"\bjust researching\b", r"\bjust looking\b", r"\bdoing research\b", r"\blooking around\b")),
]


def clean_transcript(raw_text: str) -> str:
    text = str(raw_text or "").replace("\r", " ").replace("\n", " ")
    for pattern in _FILLER_PATTERNS:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s+([.,!?])", r"\1", text)
    return text.strip()


def _collect_matches(text: str, patterns: Iterable[str]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _extract_next_step_text(text: str) -> str:
    for label, patterns in _NEXT_STEP_PATTERNS:
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            if label == "after x":
                return re.sub(r"\s+", " ", match.group(0)).strip().lower()
            return label
    return ""


def _dedupe_tags(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = re.sub(r"\s+", " ", str(value or "")).strip().lower()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


def _intent_label(intent_signal: float) -> str:
    if intent_signal >= 0.85:
        return "high-intent"
    if intent_signal >= 0.6:
        return "interested"
    if intent_signal >= 0.25:
        return "low-intent"
    return "disengaged"


def extract_signals(transcript: str) -> dict:
    cleaned = clean_transcript(transcript)
    lowered = cleaned.lower()

    intent_signal = 0.2
    for score, patterns in _INTENT_RULES:
        if _collect_matches(lowered, patterns):
            intent_signal = max(intent_signal, score)

    booking_attempted = _collect_matches(lowered, _BOOKING_PATTERNS)
    next_step_text = _extract_next_step_text(lowered)
    next_step_detected = bool(next_step_text)

    objection_tags: list[str] = []
    for tag, patterns in _OBJECTION_RULES:
        if _collect_matches(lowered, patterns) and tag not in objection_tags:
            objection_tags.append(tag)

    if booking_attempted:
        intent_signal = max(intent_signal, 0.75)
    if next_step_detected:
        intent_signal = max(intent_signal, 0.6)

    summary_parts = [f"{_intent_label(intent_signal)} seller"]
    if objection_tags:
        summary_parts.append(objection_tags[0])
    if next_step_text:
        summary_parts.append(next_step_text)

    return {
        "intent_signal": round(min(max(intent_signal, 0.0), 1.0), 2),
        "booking_attempted": booking_attempted,
        "next_step_detected": next_step_detected,
        "next_step_text": next_step_text,
        "objection_tags": objection_tags,
        "summary": ", ".join(summary_parts),
        "cleaned_transcript": cleaned,
    }


def derive_signals_from_analysis(
    *,
    summary: str = "",
    outcome: str = "",
    next_step: str = "",
    objections: Iterable[str] | None = None,
    sales_analysis: dict | None = None,
) -> dict:
    normalized_outcome = str(outcome or "").strip().lower().replace("-", "_").replace(" ", "_")
    structured_sales_analysis = sales_analysis if isinstance(sales_analysis, dict) else {}
    booking_attempted = bool(structured_sales_analysis.get("booking_attempted"))
    next_step_detected = bool(next_step) or bool(structured_sales_analysis.get("next_step_defined"))
    objection_tags = _dedupe_tags(objections or [])

    intent_signal = {
        "qualified": 0.82,
        "follow_up_required": 0.72,
        "answered": 0.58,
        "connected": 0.58,
        "not_interested": 0.16,
        "missed": 0.08,
        "voicemail": 0.08,
        "unknown": 0.22,
    }.get(normalized_outcome, 0.22)

    if booking_attempted:
        intent_signal = max(intent_signal, 0.78)
    if next_step_detected:
        intent_signal = max(intent_signal, 0.66)
    if objection_tags and intent_signal < 0.35:
        intent_signal = 0.35

    resolved_summary = re.sub(r"\s+", " ", str(summary or "")).strip()
    if not resolved_summary:
        summary_parts = ["call analyzed"]
        if booking_attempted:
            summary_parts.append("booking attempted")
        if next_step_detected:
            summary_parts.append("next step detected")
        if objection_tags:
            summary_parts.append(objection_tags[0])
        resolved_summary = ", ".join(summary_parts)

    return {
        "intent_signal": round(min(max(intent_signal, 0.0), 1.0), 2),
        "booking_attempted": booking_attempted,
        "next_step_detected": next_step_detected,
        "next_step_text": str(next_step or "").strip(),
        "objection_tags": objection_tags,
        "summary": resolved_summary,
    }
