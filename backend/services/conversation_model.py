from __future__ import annotations

from typing import Any, Dict, List


def build_structured_conversation(segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(segments, key=lambda item: (int(item.get("start_ms") or 0), int(item.get("end_ms") or 0)))
    conversation: List[Dict[str, Any]] = []
    for segment in ordered:
        conversation.append(
            {
                "speaker": str(segment.get("speaker_role") or segment.get("role") or "unknown"),
                "speaker_label": str(segment.get("speaker_label") or segment.get("diarization_label") or ""),
                "text": str(segment.get("text") or ""),
                "start_ms": int(segment.get("start_ms") or 0),
                "end_ms": int(segment.get("end_ms") or 0),
                "confidence": float(segment.get("confidence") or 0.0),
            }
        )
    return conversation


def compute_conversation_metrics(conversation: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not conversation:
        return {
            "agent_talk_ratio": 0.0,
            "customer_talk_ratio": 0.0,
            "interruptions": 0,
            "longest_agent_monologue_ms": 0,
            "longest_customer_monologue_ms": 0,
            "average_response_latency_ms": 0,
        }

    total_talk = 0
    agent_talk = 0
    customer_talk = 0
    longest_agent = 0
    longest_customer = 0
    interruptions = 0
    latencies: List[int] = []

    for index, turn in enumerate(conversation):
        duration = max(0, int(turn["end_ms"]) - int(turn["start_ms"]))
        total_talk += duration
        if turn["speaker"] == "agent":
            agent_talk += duration
            longest_agent = max(longest_agent, duration)
        elif turn["speaker"] == "customer":
            customer_talk += duration
            longest_customer = max(longest_customer, duration)

        if index == 0:
            continue

        previous = conversation[index - 1]
        if previous["speaker"] != turn["speaker"]:
            latency = max(0, int(turn["start_ms"]) - int(previous["end_ms"]))
            latencies.append(latency)
            if int(turn["start_ms"]) < int(previous["end_ms"]):
                interruptions += 1

    return {
        "agent_talk_ratio": round((agent_talk / total_talk), 4) if total_talk else 0.0,
        "customer_talk_ratio": round((customer_talk / total_talk), 4) if total_talk else 0.0,
        "interruptions": interruptions,
        "longest_agent_monologue_ms": longest_agent,
        "longest_customer_monologue_ms": longest_customer,
        "average_response_latency_ms": round(sum(latencies) / len(latencies)) if latencies else 0,
    }
