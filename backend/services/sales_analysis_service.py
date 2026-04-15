from __future__ import annotations

import json
from typing import Any, Dict, Protocol

from core.config import SPEECH_SALES_ANALYSIS_PROVIDER
from services.ai_router import _call_nim


class SalesAnalysisService(Protocol):
    def analyze(self, *, call_row: Dict[str, Any], transcript_text: str, legacy_analysis: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
        ...


class NimSalesAnalysisService:
    def analyze(self, *, call_row: Dict[str, Any], transcript_text: str, legacy_analysis: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
        if not transcript_text:
            return HeuristicSalesAnalysisService().analyze(call_row=call_row, transcript_text=transcript_text, legacy_analysis=legacy_analysis, features=features)

        import os
        import httpx
        
        api_key = os.getenv("NVIDIA_API_KEY", "")
        if not api_key:
            return HeuristicSalesAnalysisService().analyze(call_row=call_row, transcript_text=transcript_text, legacy_analysis=legacy_analysis, features=features)

        system_prompt = (
            "You are a sales coaching assistant analyzing a phone transcript. "
            "Output valid JSON only with keys: 'opener_quality' (strong/developing), 'booking_attempted' (boolean), "
            "'next_step_defined' (boolean), 'question_count' (integer), 'objection_count' (integer), "
            "'objection_resolved' (boolean), 'conversation_control' (balanced/loose), 'outcome' (string)."
        )
        prompt = f"Transcript:\n{transcript_text}\n\nAnalyze the call and output the JSON."
        
        selected_model = os.getenv("NVIDIA_NIM_MODEL", "meta/llama3-70b-instruct")
        base_url = os.getenv("NVIDIA_NIM_BASE_URL", "https://integrate.api.nvidia.com/v1")

        try:
            with httpx.Client(timeout=60.0) as client:
                resp = client.post(
                    f"{base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": selected_model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.1,
                        "max_tokens": 1024,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
                if choice:
                    cleaned = choice.strip()
                    if cleaned.startswith("```"):
                        cleaned = cleaned.strip("`")
                        if cleaned.lower().startswith("json"):
                            cleaned = cleaned[4:]
                    parsed = json.loads(cleaned.strip())
                    parsed["provider"] = "nim_sales_analysis"
                    parsed["status"] = "completed"
                    return parsed
        except Exception:
            pass

        return HeuristicSalesAnalysisService().analyze(call_row=call_row, transcript_text=transcript_text, legacy_analysis=legacy_analysis, features=features)


class HeuristicSalesAnalysisService:
    def analyze(self, *, call_row: Dict[str, Any], transcript_text: str, legacy_analysis: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
        outcome = str(legacy_analysis.get("outcome") or call_row.get("outcome") or "unknown")
        questions = features.get("questions") or []
        objections = features.get("objections") or []
        booking_events = (features.get("keyword_events") or {}).get("booking_intent") or []
        hesitation_events = (features.get("keyword_events") or {}).get("hesitation") or []
        conversation_metrics = features.get("conversation_metrics") or {}
        talk_ratio = float(conversation_metrics.get("agent_talk_ratio") or 0.0)
        interruptions = int(conversation_metrics.get("interruptions") or 0)
        longest_monologue_ms = int(conversation_metrics.get("longest_agent_monologue_ms") or 0)
        resolved_objections = [item for item in objections if item.get("resolved")]

        return {
            "provider": SPEECH_SALES_ANALYSIS_PROVIDER,
            "opener_quality": "strong" if questions else "developing",
            "booking_attempted": bool(booking_events),
            "next_step_defined": bool(booking_events or legacy_analysis.get("next_step")),
            "question_count": len(questions),
            "objection_count": len(objections),
            "objection_resolved": bool(resolved_objections),
            "control_phrase_count": len(booking_events) + len(questions),
            "confidence_phrase_count": len(booking_events) - len(hesitation_events),
            "conversation_control": "balanced" if 0.45 <= talk_ratio <= 0.7 and longest_monologue_ms <= 4500 else "loose",
            "interruptions": interruptions,
            "talk_ratio": talk_ratio,
            "sentiment_label": (features.get("sentiment") or {}).get("label") or "",
            "booking_language": booking_events,
            "outcome": outcome,
            "status": "completed",
        }


def get_sales_analysis_service() -> SalesAnalysisService:
    if SPEECH_SALES_ANALYSIS_PROVIDER == "nim_sales_analysis":
        return NimSalesAnalysisService()
    return HeuristicSalesAnalysisService()
