"""
Scoring Feedback — learn from call outcomes to adjust scoring weights.

Queries the last 90 days of call_log grouped by trigger_type, computes
connected_rate and booking_rate, compares against current scoring weights,
and recommends adjustments (max +/- 5 pts, minimum 10 calls per trigger).
Feedback is stored in ScoringFeedback and applied via SystemConfig.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select, func, case, and_
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from models.sql_models import CallLog, ScoringFeedback, SystemConfig

logger = logging.getLogger(__name__)

# ─── Current baseline weights (from scoring.py _trigger_bonus) ────────────────

_BASELINE_WEIGHTS: Dict[str, int] = {
    "rescinded": 34,
    "withdrawn": 28,
    "construction": 26,
    "contract": 24,
    "mortgage_cliff": 22,
    "marketing": 20,
    "subdivision": 18,
    "probate": 12,
    "default": 8,
}

# Minimum calls required before we recommend a weight change
_MIN_CALLS = 10

# Maximum adjustment per feedback cycle
_MAX_ADJUSTMENT = 5

# Lookback window in days
_LOOKBACK_DAYS = 90


# ─── Compute feedback ────────────────────────────────────────────────────────


async def compute_scoring_feedback(session: AsyncSession) -> list[dict]:
    """Query call_log from the last 90 days grouped by trigger type.

    For each trigger type with >= 10 calls, compute:
      - connected_rate: fraction of calls where connected == True
      - booking_rate: fraction of calls with booking_attempted == True

    Compare against current scoring weights.  Recommend adjustments
    (max +/- 5 pts).  Store results in ScoringFeedback table.

    Returns list of feedback dicts.
    """
    cutoff = (datetime.utcnow() - timedelta(days=_LOOKBACK_DAYS)).isoformat()

    # Load current weights from SystemConfig, fall back to baseline
    current_weights = await _load_current_weights(session)

    # Query call_log aggregated by trigger_type (derived from lead)
    # We join call_log -> leads to get trigger_type
    # However CallLog doesn't have trigger_type, so we use a subquery approach
    # Actually, let's query calls and join to leads for trigger_type
    from models.sql_models import Lead

    stmt = (
        select(
            Lead.trigger_type,
            func.count(CallLog.id).label("total_calls"),
            func.sum(case((CallLog.connected == True, 1), else_=0)).label("connected_count"),  # noqa: E712
            func.sum(case((CallLog.booking_attempted == True, 1), else_=0)).label("booking_count"),  # noqa: E712
        )
        .join(Lead, CallLog.lead_id == Lead.id)
        .where(CallLog.timestamp >= cutoff)
        .group_by(Lead.trigger_type)
    )

    result = await session.execute(stmt)
    rows = result.all()

    feedback_list: list[dict] = []
    now = now_iso()

    for row in rows:
        trigger_type = row[0] or "unknown"
        total_calls = row[1] or 0
        connected_count = row[2] or 0
        booking_count = row[3] or 0

        if total_calls < _MIN_CALLS:
            continue

        connected_rate = round(connected_count / total_calls, 4)
        booking_rate = round(booking_count / total_calls, 4)

        # Determine current weight for this trigger type
        trigger_key = _normalize_trigger_key(trigger_type)
        current_weight = current_weights.get(trigger_key, _BASELINE_WEIGHTS.get("default", 8))

        # Recommend adjustment based on booking_rate relative to average
        # High booking rate -> increase weight; low -> decrease
        recommended = _compute_recommended_weight(
            current_weight, connected_rate, booking_rate, total_calls,
        )

        fb = ScoringFeedback(
            id=str(uuid.uuid4()),
            trigger_type=trigger_type,
            calls_analyzed=total_calls,
            connected_rate=connected_rate,
            booking_rate=booking_rate,
            current_weight=current_weight,
            recommended_weight=recommended,
            applied=False,
            computed_at=now,
        )
        session.add(fb)
        feedback_list.append(_feedback_to_dict(fb))

    await session.commit()
    logger.info("Computed scoring feedback for %d trigger types.", len(feedback_list))
    return feedback_list


# ─── Get pending feedback ─────────────────────────────────────────────────────


async def get_pending_feedback(session: AsyncSession) -> list[dict]:
    """Return all unapplied feedback entries, newest first."""
    result = await session.execute(
        select(ScoringFeedback)
        .where(ScoringFeedback.applied == False)  # noqa: E712
        .order_by(ScoringFeedback.computed_at.desc())
    )
    rows = result.scalars().all()
    return [_feedback_to_dict(r) for r in rows]


# ─── Apply feedback ──────────────────────────────────────────────────────────


async def apply_feedback(session: AsyncSession, feedback_id: str) -> dict:
    """Mark feedback as applied and store the new weight in SystemConfig.

    Returns the updated feedback dict, or {"error": ...} if not found.
    """
    result = await session.execute(
        select(ScoringFeedback).where(ScoringFeedback.id == feedback_id)
    )
    fb = result.scalars().first()
    if not fb:
        return {"error": "feedback_not_found", "id": feedback_id}

    if fb.applied:
        return {"error": "already_applied", "id": feedback_id}

    # Mark as applied
    fb.applied = True
    now = now_iso()

    # Update SystemConfig with the new weight
    trigger_key = _normalize_trigger_key(fb.trigger_type)
    config_key = "scoring_weights"

    config_result = await session.execute(
        select(SystemConfig).where(SystemConfig.key == config_key)
    )
    config = config_result.scalars().first()

    if not config:
        config = SystemConfig(
            key=config_key,
            value_json={},
            updated_at=now,
        )
        session.add(config)

    weights = dict(config.value_json or {})
    weights[trigger_key] = fb.recommended_weight
    config.value_json = weights
    config.updated_at = now

    await session.commit()
    logger.info(
        "Applied scoring feedback %s: %s weight %d -> %d",
        feedback_id, fb.trigger_type, fb.current_weight, fb.recommended_weight,
    )
    return _feedback_to_dict(fb)


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _normalize_trigger_key(trigger_type: str) -> str:
    """Normalize a trigger_type string to match _BASELINE_WEIGHTS keys."""
    t = (trigger_type or "").lower().strip()
    if "rescinded" in t:
        return "rescinded"
    if "withdrawn" in t:
        return "withdrawn"
    if "construction" in t:
        return "construction"
    if "contract" in t:
        return "contract"
    if "mortgage" in t and "cliff" in t:
        return "mortgage_cliff"
    if "marketing" in t:
        return "marketing"
    if "subdivision" in t or "lot" in t:
        return "subdivision"
    if "probate" in t:
        return "probate"
    return "default"


def _compute_recommended_weight(
    current: int,
    connected_rate: float,
    booking_rate: float,
    total_calls: int,
) -> int:
    """Compute a recommended weight adjustment.

    Strategy:
      - booking_rate > 0.15 and connected_rate > 0.3 -> increase by up to 5
      - booking_rate > 0.05 and connected_rate > 0.2 -> increase by up to 3
      - booking_rate == 0 and connected_rate < 0.1   -> decrease by up to 5
      - booking_rate < 0.02 and connected_rate < 0.15 -> decrease by up to 3
      - Otherwise keep current weight

    Adjustments are capped at _MAX_ADJUSTMENT.
    """
    adjustment = 0

    if booking_rate > 0.15 and connected_rate > 0.3:
        adjustment = min(_MAX_ADJUSTMENT, 5)
    elif booking_rate > 0.05 and connected_rate > 0.2:
        adjustment = min(_MAX_ADJUSTMENT, 3)
    elif booking_rate == 0 and connected_rate < 0.1:
        adjustment = -min(_MAX_ADJUSTMENT, 5)
    elif booking_rate < 0.02 and connected_rate < 0.15:
        adjustment = -min(_MAX_ADJUSTMENT, 3)

    # Scale down adjustment if sample size is small (< 25 calls)
    if total_calls < 25:
        adjustment = adjustment // 2 if abs(adjustment) > 1 else adjustment

    recommended = current + adjustment
    # Keep weight in a sane range [0, 50]
    return max(0, min(50, recommended))


async def _load_current_weights(session: AsyncSession) -> Dict[str, int]:
    """Load scoring weights from SystemConfig, falling back to baseline."""
    try:
        result = await session.execute(
            select(SystemConfig).where(SystemConfig.key == "scoring_weights")
        )
        config = result.scalars().first()
        if config and config.value_json:
            # Merge with baseline so we have defaults for missing keys
            merged = dict(_BASELINE_WEIGHTS)
            for k, v in config.value_json.items():
                try:
                    merged[k] = int(v)
                except (ValueError, TypeError):
                    pass
            return merged
    except Exception as exc:
        logger.warning("Could not load scoring_weights from SystemConfig: %s", exc)

    return dict(_BASELINE_WEIGHTS)


def _feedback_to_dict(fb: ScoringFeedback) -> dict:
    """Convert a ScoringFeedback ORM row to a plain dict."""
    return {
        "id": fb.id,
        "trigger_type": fb.trigger_type,
        "calls_analyzed": fb.calls_analyzed,
        "connected_rate": fb.connected_rate,
        "booking_rate": fb.booking_rate,
        "current_weight": fb.current_weight,
        "recommended_weight": fb.recommended_weight,
        "weight_change": fb.recommended_weight - fb.current_weight,
        "applied": fb.applied,
        "computed_at": fb.computed_at,
    }
