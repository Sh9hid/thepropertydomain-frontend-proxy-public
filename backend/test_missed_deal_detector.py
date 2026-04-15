"""
Tests for the missed deal detector.

All tests use pure function run_detectors() — no database required.
These verify that the deterministic rules fire correctly.
"""
import pytest
from datetime import datetime, timedelta, timezone

from services.missed_deal_detector import (
    _CallStats,
    _compute_urgency,
    _compute_score,
    _days_since,
    _parse_iso,
    run_detectors,
)

SYDNEY_TZ_OFFSET = timezone(timedelta(hours=10))


def _now():
    return datetime.now(SYDNEY_TZ_OFFSET)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _lead(overrides: dict = None) -> dict:
    """Default active lead with no signals."""
    base = {
        "id": "abc123",
        "address": "10 Test St, Woonona",
        "suburb": "Woonona",
        "postcode": "2517",
        "owner_name": "John Smith",
        "status": "captured",
        "heat_score": 30,
        "call_today_score": 30,
        "evidence_score": 40,
        "next_action_at": None,
        "last_contacted_at": None,
        "updated_at": _iso(_now() - timedelta(days=3)),
        "objection_reason": "",
        "trigger_type": "marketing_list",
        "est_value": 800_000,
        "touches_14d": 2,
    }
    if overrides:
        base.update(overrides)
    return base


def _stats(overrides: dict = None) -> _CallStats:
    s = _CallStats()
    if overrides:
        for k, v in overrides.items():
            setattr(s, k, v)
    return s


# ── WARM_THEN_ABANDONED ───────────────────────────────────────────────────────

class TestWarmThenAbandoned:
    def test_triggers_when_connected_and_stale(self):
        now = _now()
        lead = _lead({"status": "contacted", "last_contacted_at": _iso(now - timedelta(days=10))})
        stats = _stats({
            "connected_calls": 1,
            "last_connected_at": now - timedelta(days=10),
            "talk_time_total": 120,
        })
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "WARM_THEN_ABANDONED" in keys

    def test_not_triggered_when_connected_recently(self):
        now = _now()
        lead = _lead({"status": "contacted"})
        stats = _stats({
            "connected_calls": 1,
            "last_connected_at": now - timedelta(days=2),
        })
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "WARM_THEN_ABANDONED" not in keys

    def test_not_triggered_for_terminal_status(self):
        now = _now()
        lead = _lead({"status": "converted"})
        stats = _stats({
            "connected_calls": 1,
            "last_connected_at": now - timedelta(days=20),
        })
        results = run_detectors(lead, stats, False, now)
        assert len(results) == 0

    def test_not_triggered_with_future_next_action(self):
        now = _now()
        lead = _lead({
            "status": "contacted",
            "next_action_at": _iso(now + timedelta(days=2)),
        })
        stats = _stats({
            "connected_calls": 1,
            "last_connected_at": now - timedelta(days=8),
        })
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "WARM_THEN_ABANDONED" not in keys


# ── OVERDUE_CALLBACK ──────────────────────────────────────────────────────────

class TestOverdueCallback:
    def test_triggers_when_next_action_overdue(self):
        now = _now()
        lead = _lead({"next_action_at": _iso(now - timedelta(days=3))})
        results = run_detectors(lead, _stats(), False, now)
        keys = [r.key for r in results]
        assert "OVERDUE_CALLBACK" in keys

    def test_not_triggered_when_future(self):
        now = _now()
        lead = _lead({"next_action_at": _iso(now + timedelta(days=2))})
        results = run_detectors(lead, _stats(), False, now)
        keys = [r.key for r in results]
        assert "OVERDUE_CALLBACK" not in keys

    def test_triggers_from_call_log_next_action(self):
        now = _now()
        lead = _lead()
        stats = _stats({"last_call_next_action_due": _iso(now - timedelta(days=5))})
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "OVERDUE_CALLBACK" in keys

    def test_critical_when_overdue_7_plus_days(self):
        now = _now()
        lead = _lead({"next_action_at": _iso(now - timedelta(days=8))})
        results = run_detectors(lead, _stats(), False, now)
        overdue = [r for r in results if r.key == "OVERDUE_CALLBACK"]
        assert overdue and overdue[0].severity == 3


# ── LONG_TALK_NO_BOOKING ─────────────────────────────────────────────────────

class TestLongTalkNoBooking:
    def test_triggers_when_talk_time_high_and_no_appointment(self):
        now = _now()
        lead = _lead({"status": "contacted"})
        stats = _stats(
            {
                "connected_calls": 2,
                "talk_time_total": 300,
                "booking_attempted": True,
                "max_intent_signal": 0.8,
            }
        )
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "LONG_TALK_NO_BOOKING" in keys

    def test_not_triggered_below_threshold(self):
        now = _now()
        lead = _lead({"status": "contacted"})
        stats = _stats({"connected_calls": 1, "talk_time_total": 90})
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "LONG_TALK_NO_BOOKING" not in keys

    def test_not_triggered_when_appointment_booked(self):
        now = _now()
        lead = _lead({"status": "contacted"})
        stats = _stats({"connected_calls": 2, "talk_time_total": 300})
        results = run_detectors(lead, stats, has_appointment=True, now=now)
        keys = [r.key for r in results]
        assert "LONG_TALK_NO_BOOKING" not in keys

    def test_not_triggered_when_status_appt_booked(self):
        now = _now()
        lead = _lead({"status": "appt_booked"})
        stats = _stats({"connected_calls": 2, "talk_time_total": 300})
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "LONG_TALK_NO_BOOKING" not in keys

    def test_not_triggered_when_call_had_no_real_seller_signal(self):
        now = _now()
        lead = _lead({"status": "contacted"})
        stats = _stats(
            {
                "connected_calls": 2,
                "talk_time_total": 300,
                "booking_attempted": False,
                "next_step_detected": False,
                "max_intent_signal": 0.2,
            }
        )
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "LONG_TALK_NO_BOOKING" not in keys


# ── REPEATED_ATTEMPTS_NO_PROGRESS ────────────────────────────────────────────

class TestRepeatedAttemptsNoProgress:
    def test_triggers_when_attempts_no_connection(self):
        now = _now()
        lead = _lead({"status": "captured"})
        stats = _stats({"total_attempts": 4, "connected_calls": 0})
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "REPEATED_ATTEMPTS_NO_PROGRESS" in keys

    def test_not_triggered_when_connected(self):
        now = _now()
        lead = _lead({"status": "captured"})
        stats = _stats({"total_attempts": 4, "connected_calls": 1})
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "REPEATED_ATTEMPTS_NO_PROGRESS" not in keys

    def test_not_triggered_below_min_attempts(self):
        now = _now()
        lead = _lead({"status": "captured"})
        stats = _stats({"total_attempts": 2, "connected_calls": 0})
        results = run_detectors(lead, stats, False, now)
        keys = [r.key for r in results]
        assert "REPEATED_ATTEMPTS_NO_PROGRESS" not in keys


# ── DISMISS/SNOOZE (summary aggregation logic) ───────────────────────────────

class TestUrgencyAndScoreFormulas:
    def test_urgency_critical_when_three_detectors(self):
        from services.missed_deal_detector import _DetectorResult
        detectors = [
            _DetectorResult("A", "A", "a", 1),
            _DetectorResult("B", "B", "b", 1),
            _DetectorResult("C", "C", "c", 1),
        ]
        assert _compute_urgency(detectors, overdue_days=0, days_since_connected=3) == "critical"

    def test_urgency_high_for_warm_abandoned(self):
        from services.missed_deal_detector import _DetectorResult
        detectors = [_DetectorResult("WARM_THEN_ABANDONED", "Warm", "basis", 2)]
        assert _compute_urgency(detectors, overdue_days=0, days_since_connected=7) == "high"

    def test_urgency_critical_for_overdue_plus_7_days(self):
        from services.missed_deal_detector import _DetectorResult
        detectors = [_DetectorResult("OVERDUE_CALLBACK", "Overdue", "basis", 2)]
        assert _compute_urgency(detectors, overdue_days=8, days_since_connected=None) == "critical"

    def test_score_increases_with_severity(self):
        from services.missed_deal_detector import _DetectorResult
        low = [_DetectorResult("A", "A", "a", 1)]
        high = [_DetectorResult("A", "A", "a", 3)]
        low_score = _compute_score(low, 0, 0, 0, None, 0)
        high_score = _compute_score(high, 0, 0, 0, None, 0)
        assert high_score > low_score

    def test_score_increases_with_overdue_days(self):
        from services.missed_deal_detector import _DetectorResult
        d = [_DetectorResult("A", "A", "a", 1)]
        score_0 = _compute_score(d, 0, 0, 0, None, 0)
        score_7 = _compute_score(d, 7, 0, 0, None, 0)
        assert score_7 > score_0

    def test_score_bounded_at_100(self):
        from services.missed_deal_detector import _DetectorResult
        d = [
            _DetectorResult("A", "A", "a", 3),
            _DetectorResult("B", "B", "b", 3),
        ]
        score = _compute_score(d, 14, 30, 100, 2_000_000, 600)
        assert score <= 100
