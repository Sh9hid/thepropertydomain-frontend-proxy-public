from __future__ import annotations

from typing import Any, Dict, List, Protocol

from core.config import SPEECH_COACHING_PROVIDER


class CoachingService(Protocol):
    def generate(self, *, call_row: Dict[str, Any], features: Dict[str, Any], sales_analysis: Dict[str, Any], score_result: Dict[str, Any]) -> Dict[str, Any]:
        ...


def _priority_message(score_name: str, features: Dict[str, Any], sales_analysis: Dict[str, Any]) -> Dict[str, str]:
    filler_count = len(features.get("filler_events") or [])
    hesitation_count = len(((features.get("keyword_events") or {}).get("hesitation") or []))
    question_count = len(features.get("questions") or [])
    booking_count = len(((features.get("keyword_events") or {}).get("booking_intent") or []))
    objection_count = len(features.get("objections") or [])

    if score_name == "Fluency":
        return {
            "title": "Tighten pacing",
            "why": f"Detected {filler_count} filler moments and hesitation pressure in the call flow.",
            "replacement": "Use one clean sentence, then stop.",
            "drill": "Record a 20-second answer without fillers.",
        }
    if score_name == "Confidence":
        return {
            "title": "Cut the hedge",
            "why": f"Hesitation language appeared {hesitation_count} times while decisive booking language appeared {booking_count} times.",
            "replacement": "Say the recommendation directly, then ask for the next step.",
            "drill": "Repeat the close with 'let's' and no 'maybe'.",
        }
    if score_name == "Sales Control":
        return {
            "title": "Lead the next step",
            "why": f"Question count is {question_count} with {sales_analysis.get('control_phrase_count', 0)} control signals.",
            "replacement": "Ask one diagnostic question, then propose the next action.",
            "drill": "Practice: question, answer, booking question.",
        }
    return {
        "title": "Make the booking ask",
        "why": f"The call needs a clearer next-step commitment. Objections raised: {objection_count}.",
        "replacement": "Let's lock in a time now rather than leaving it open.",
        "drill": "End three practice reps with a direct calendar ask.",
    }


class HeuristicCoachingService:
    def generate(self, *, call_row: Dict[str, Any], features: Dict[str, Any], sales_analysis: Dict[str, Any], score_result: Dict[str, Any]) -> Dict[str, Any]:
        ranked = sorted(score_result["components"].items(), key=lambda item: item[1]["score"])
        weakest = ranked[:3]
        strongest = ranked[-2:] if len(ranked) > 1 else ranked

        weaknesses: List[Dict[str, str]] = []
        prioritized: List[Dict[str, str]] = []
        drills: List[str] = []
        for score_name, component in weakest:
            priority = _priority_message(score_name, features, sales_analysis)
            weaknesses.append(
                {
                    "metric": score_name,
                    "score": str(component["score"]),
                    "reason": component.get("reason", priority["why"]),
                    "evidence": component.get("evidence", []),
                }
            )
            prioritized.append(
                {
                    "metric": score_name,
                    "title": priority["title"],
                    "action": priority["replacement"],
                    "why": priority["why"],
                }
            )
            drills.append(priority["drill"])

        strengths = [
            {
                "metric": score_name,
                "score": str(component["score"]),
                "reason": component.get("reason", ""),
                "evidence": component.get("evidence", []),
            }
            for score_name, component in strongest
        ]

        strongest_line = f"Best signal: {strengths[0]['metric']} at {strengths[0]['score']}."
        weakest_line = f"Main drag: {weaknesses[0]['metric']} at {weaknesses[0]['score']}."
        brutal_summary = f"{strongest_line} {weakest_line} Fix the weakest moment first, not the whole call."

        rewrite_before = "I was just calling to see if maybe this week could work."
        rewrite_after = "Let's lock in a time this week so we can move this forward properly."
        if sales_analysis.get("booking_attempted"):
            rewrite_before = "Maybe we can touch base again soon."
            rewrite_after = "Let's confirm the time now so the next step is locked in."

        live_task = "Use one direct booking question on the next live call."
        if not sales_analysis.get("booking_attempted"):
            live_task = "On the next call, make an explicit booking attempt before you hang up."

        return {
            "provider": SPEECH_COACHING_PROVIDER,
            "report_version": "v1",
            "brutal_summary": brutal_summary,
            "detailed_breakdown": {
                "strengths": strengths,
                "weaknesses": weaknesses,
                "actionable_coaching": prioritized,
                "prioritized_improvements": prioritized,
                "sales_analysis": sales_analysis,
            },
            "rewrite": {
                "before": rewrite_before,
                "after": rewrite_after,
            },
            "drills": drills[:3],
            "live_task": live_task,
        }


def get_coaching_service() -> CoachingService:
    return HeuristicCoachingService()
