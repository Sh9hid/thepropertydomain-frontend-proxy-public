"""
Daily Voice/Accent Training Plan service.

Generates a grounded 10-minute daily training plan from:
  - voice_trainer_reports (last 7 days)
  - filler_events (token counts from recent calls)
  - pronunciation_events (if any)
  - call_log (outcome context)
  - transcripts (agent text snippets)

Never invents acoustic details not present in the data.
Only derives what the stored analysis actually supports.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.config import SYDNEY_TZ
from models.org_models import VoiceTrainingPlan
from services.orchestration_engine import route_completion

log = logging.getLogger(__name__)

PLAN_WINDOW_DAYS = 7        # look-back for source calls
MAX_CALLS_TO_ANALYSE = 10  # cap input volume


# ─── Data loader ──────────────────────────────────────────────────────────────

async def _load_voice_data(session: AsyncSession, rep_id: str) -> Dict[str, Any]:
    cutoff = (datetime.utcnow() - timedelta(days=PLAN_WINDOW_DAYS)).isoformat()

    # Voice trainer reports (last 7 days)
    vtr_rows = (await session.execute(
        text("""
            SELECT id, call_id, pace_wpm, filler_count, filler_words,
                   avg_pause_ms, long_pause_count, agent_talk_ratio,
                   intro_detected, reason_stated, value_prop_detected,
                   next_step_attempted, highlights_json, issues_json,
                   improved_opener, coaching_status, generated_at
            FROM voice_trainer_reports
            WHERE rep_id = :rep_id
              AND created_at >= :cutoff
            ORDER BY created_at DESC
            LIMIT :lim
        """),
        {"rep_id": rep_id, "cutoff": cutoff, "lim": MAX_CALLS_TO_ANALYSE},
    )).mappings().all()

    # Filler token frequency across those calls
    if vtr_rows:
        call_ids = [r["call_id"] for r in vtr_rows]
        # Use parameterised IN the safe way for sqlite/pg
        placeholders = ",".join(f":cid{i}" for i in range(len(call_ids)))
        params = {f"cid{i}": cid for i, cid in enumerate(call_ids)}
        filler_rows = (await session.execute(
            text(f"""
                SELECT token, family, sum(count) as total_count
                FROM filler_events
                WHERE call_id IN ({placeholders})
                GROUP BY token, family
                ORDER BY total_count DESC
                LIMIT 10
            """),
            params,
        )).mappings().all()
    else:
        filler_rows = []

    # Pronunciation issues (if any)
    if vtr_rows:
        prn_rows = (await session.execute(
            text(f"""
                SELECT canonical, observed, deviation_type, severity, count(*) as cnt
                FROM pronunciation_events
                WHERE call_id IN ({placeholders})
                GROUP BY canonical, observed, deviation_type
                ORDER BY cnt DESC, severity DESC
                LIMIT 8
            """),
            params,
        )).mappings().all()
    else:
        prn_rows = []

    # Recent call outcomes for context
    outcome_rows = (await session.execute(
        text("""
            SELECT outcome, count(*) as cnt
            FROM call_log
            WHERE user_id = :rep_id
              AND logged_at >= :cutoff
            GROUP BY outcome
            ORDER BY cnt DESC
            LIMIT 8
        """),
        {"rep_id": rep_id, "cutoff": cutoff},
    )).mappings().all()

    return {
        "reports": [dict(r) for r in vtr_rows],
        "filler_frequency": [dict(r) for r in filler_rows],
        "pronunciation_issues": [dict(r) for r in prn_rows],
        "call_outcomes": [dict(r) for r in outcome_rows],
        "calls_analysed": len(vtr_rows),
        "source_call_ids": [r["call_id"] for r in vtr_rows],
    }


# ─── Plan generator ───────────────────────────────────────────────────────────

_VOICE_SYSTEM = (
    "You are an expert speech coach for Australian real estate sales calls. "
    "You analyse real call data and create practical, targeted daily training plans. "
    "Never invent pronunciation details, timing, or acoustic data not present in the input. "
    "Keep plans concise, actionable, and specific to Australian context. "
    "Return valid JSON only."
)

_PLAN_SCHEMA = """{
  "key_focus": "one-sentence summary of today's main focus",
  "mistakes": [
    {
      "pattern": "...",
      "count": 0,
      "example_quote": "direct quote from call data or null",
      "recommendation": "..."
    }
  ],
  "drills": [
    {
      "drill_name": "...",
      "instruction": "...",
      "example_before": "...",
      "example_after": "...",
      "duration_minutes": 3
    }
  ],
  "improved_phrases": [
    {
      "original": "...",
      "improved": "...",
      "context": "..."
    }
  ],
  "session_structure": "2-sentence 10-minute plan outline",
  "overall_score": 0.0
}"""


def _build_plan_prompt(data: Dict) -> str:
    reports = data["reports"]
    if not reports:
        return ""

    # Aggregate key metrics
    avg_pace = sum(r.get("pace_wpm") or 0 for r in reports) / len(reports)
    avg_fillers = sum(r.get("filler_count") or 0 for r in reports) / len(reports)
    avg_pause = sum(r.get("avg_pause_ms") or 0 for r in reports) / len(reports)
    long_pauses = sum(r.get("long_pause_count") or 0 for r in reports)
    intro_rate = sum(1 for r in reports if r.get("intro_detected")) / len(reports)
    next_step_rate = sum(1 for r in reports if r.get("next_step_attempted")) / len(reports)

    # Collect all issues from coaching reports
    all_issues = []
    for r in reports:
        try:
            issues = json.loads(r.get("issues_json") or "[]")
            all_issues.extend(issues[:3])
        except Exception:
            pass

    data_summary = {
        "calls_analysed": data["calls_analysed"],
        "avg_pace_wpm": round(avg_pace, 1),
        "avg_filler_count_per_call": round(avg_fillers, 1),
        "avg_pause_ms": round(avg_pause, 0),
        "long_pause_total": long_pauses,
        "intro_stated_rate": round(intro_rate, 2),
        "next_step_attempted_rate": round(next_step_rate, 2),
        "top_filler_words": data["filler_frequency"][:6],
        "pronunciation_issues": data["pronunciation_issues"][:4],
        "call_outcomes": data["call_outcomes"],
        "common_issues_from_coaching": list(set(all_issues))[:8],
    }

    return f"""Based on REAL call analysis data from the past 7 days, generate a 10-minute daily voice training plan.

DATA:
{json.dumps(data_summary, indent=2)[:2000]}

Generate 3-5 mistakes, 3 drills (total ~10 minutes), 3 improved phrases.
Focus on Australian accent, pacing, filler reduction, and sales phrasing.
Only reference specific issues that appear in the data.

Return JSON matching this schema:
{_PLAN_SCHEMA}"""


# ─── Public entry point ───────────────────────────────────────────────────────

async def generate_daily_plan(
    session: AsyncSession,
    rep_id: str = "Shahid",
) -> Optional[VoiceTrainingPlan]:
    """
    Generate today's training plan. If a plan already exists today, return it.
    """
    from core.config import SYDNEY_TZ
    today = datetime.now(SYDNEY_TZ).strftime("%Y-%m-%d")

    # Check for existing today plan
    existing = (await session.execute(
        select(VoiceTrainingPlan).where(
            VoiceTrainingPlan.plan_date == today,
            VoiceTrainingPlan.rep_id == rep_id,
        )
    )).scalars().first()
    if existing:
        return existing

    data = await _load_voice_data(session, rep_id)

    if data["calls_analysed"] == 0:
        # No data — create a skeleton plan
        plan = VoiceTrainingPlan(
            plan_date=today,
            rep_id=rep_id,
            calls_analysed=0,
            source_call_ids=[],
            status="no_data",
            key_focus="No call data available for this period.",
            session_structure="No recent call data found. Record calls to enable personalised training.",
        )
        session.add(plan)
        await session.commit()
        await session.refresh(plan)
        return plan

    prompt = _build_plan_prompt(data)
    if not prompt:
        return None

    messages = [
        {"role": "system", "content": _VOICE_SYSTEM},
        {"role": "user", "content": prompt},
    ]

    try:
        result = await route_completion(
            work_type="summarization",
            messages=messages,
            max_tokens=1200,
        )
        text_raw = result.text.strip()
        if "```" in text_raw:
            text_raw = text_raw.split("```")[1]
            if text_raw.startswith("json"):
                text_raw = text_raw[4:]
        parsed = json.loads(text_raw)

        plan = VoiceTrainingPlan(
            plan_date=today,
            rep_id=rep_id,
            calls_analysed=data["calls_analysed"],
            source_call_ids=data["source_call_ids"],
            status="generated",
            key_focus=str(parsed.get("key_focus", ""))[:300],
            mistakes=parsed.get("mistakes") or [],
            drills=parsed.get("drills") or [],
            improved_phrases=parsed.get("improved_phrases") or [],
            session_structure=str(parsed.get("session_structure", ""))[:1000],
            overall_score=float(parsed.get("overall_score") or 0) or None,
            provider_used=result.provider,
            tokens_used=result.input_tokens + result.output_tokens,
        )
    except Exception as exc:
        log.warning("[voice_plan] generation failed: %s", exc)
        plan = VoiceTrainingPlan(
            plan_date=today,
            rep_id=rep_id,
            calls_analysed=data["calls_analysed"],
            source_call_ids=data["source_call_ids"],
            status="failed",
            key_focus="Plan generation failed. Check provider status.",
        )

    session.add(plan)
    await session.commit()
    await session.refresh(plan)

    from core.events import event_manager
    await event_manager.broadcast({
        "type": "VOICE_PLAN_GENERATED",
        "data": {
            "plan_id": plan.id,
            "plan_date": today,
            "rep_id": rep_id,
            "calls_analysed": data["calls_analysed"],
            "ts": datetime.utcnow().isoformat(),
        }
    })
    return plan


async def get_plan_history(
    session: AsyncSession,
    rep_id: str = "Shahid",
    limit: int = 14,
) -> List[VoiceTrainingPlan]:
    return (await session.execute(
        select(VoiceTrainingPlan)
        .where(VoiceTrainingPlan.rep_id == rep_id)
        .order_by(VoiceTrainingPlan.plan_date.desc())
        .limit(limit)
    )).scalars().all()
