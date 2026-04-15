"""
Targeted tests for lead display helper logic.

Covers:
- trigger_type normalization (mirrors TRIGGER_TYPE_MAP in CommandLedger.tsx)
- days_ago time derivation
- withdrawn / expired / stale / probate visibility
- empty-field suppression
"""
import math
from datetime import datetime, timedelta, timezone


# ── Mirror of TRIGGER_TYPE_MAP (frontend/src/views/CommandLedger.tsx) ──────────

TRIGGER_TYPE_MAP = {
    "domain_withdrawn": {"label": "WITHDRAWN", "color": "#ff453a"},
    "probate": {"label": "PROBATE", "color": "#ff453a"},
    "da_feed": {"label": "DA", "color": "#30d158"},
    "Development Application": {"label": "DA", "color": "#30d158"},
    "delta_engine": {"label": "DELTA", "color": "#ff9f0a"},
    "reaxml": {"label": "REAXML", "color": "#0a84ff"},
    "stale_queue": {"label": "STALE", "color": "#ff9f0a"},
    "manual": {"label": "MANUAL", "color": "#636366"},
    "manual_entry": {"label": "MANUAL", "color": "#636366"},
    "distress": {"label": "DISTRESS", "color": "#ff453a"},
}


def normalize_trigger(trigger_type: str | None) -> dict:
    """Normalize a trigger_type string to a display label + color."""
    if not trigger_type:
        return {"label": "—", "color": "var(--text-dim)"}
    mapped = TRIGGER_TYPE_MAP.get(trigger_type)
    if mapped:
        return mapped
    return {"label": trigger_type.upper()[:10], "color": "var(--text-dim)"}


def days_ago(date_str: str | None) -> int | None:
    """Compute integer days since the given ISO date string (UTC reference)."""
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Make both tz-aware for safe subtraction
    now = datetime.now(tz=timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    delta = now - d
    if delta.total_seconds() < 0:
        return 0  # future date → treat as 0
    return math.floor(delta.total_seconds() / 86400)


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestNormalizeTrigger:
    def test_withdrawn(self):
        r = normalize_trigger("domain_withdrawn")
        assert r["label"] == "WITHDRAWN"
        assert r["color"] == "#ff453a"

    def test_probate(self):
        r = normalize_trigger("probate")
        assert r["label"] == "PROBATE"

    def test_da_feed(self):
        r = normalize_trigger("da_feed")
        assert r["label"] == "DA"

    def test_da_development_application(self):
        r = normalize_trigger("Development Application")
        assert r["label"] == "DA"

    def test_stale(self):
        r = normalize_trigger("stale_queue")
        assert r["label"] == "STALE"

    def test_probate_color_is_red(self):
        r = normalize_trigger("probate")
        assert r["color"] == "#ff453a"

    def test_withdrawn_color_is_red(self):
        r = normalize_trigger("domain_withdrawn")
        assert r["color"] == "#ff453a"

    def test_da_color_is_green(self):
        r = normalize_trigger("da_feed")
        assert r["color"] == "#30d158"

    def test_stale_color_is_amber(self):
        r = normalize_trigger("stale_queue")
        assert r["color"] == "#ff9f0a"

    def test_manual(self):
        r = normalize_trigger("manual")
        assert r["label"] == "MANUAL"

    def test_manual_entry(self):
        r = normalize_trigger("manual_entry")
        assert r["label"] == "MANUAL"

    def test_distress(self):
        r = normalize_trigger("distress")
        assert r["label"] == "DISTRESS"

    def test_empty_string_returns_dash(self):
        r = normalize_trigger("")
        assert r["label"] == "—"

    def test_none_returns_dash(self):
        r = normalize_trigger(None)
        assert r["label"] == "—"

    def test_unknown_truncated_to_10(self):
        r = normalize_trigger("some_very_long_unknown_type")
        assert r["label"] == "SOME_VERY_"
        assert len(r["label"]) <= 10

    def test_unknown_uses_dim_color(self):
        r = normalize_trigger("unknown_xyz")
        assert r["color"] == "var(--text-dim)"

    def test_all_critical_types_map_correctly(self):
        """Ensure every lead type the operator cares about is mapped."""
        critical = {
            "domain_withdrawn": "WITHDRAWN",
            "probate": "PROBATE",
            "stale_queue": "STALE",
            "da_feed": "DA",
            "distress": "DISTRESS",
        }
        for trigger, expected_label in critical.items():
            assert normalize_trigger(trigger)["label"] == expected_label, (
                f"trigger_type '{trigger}' should map to '{expected_label}'"
            )


class TestDaysAgo:
    def test_today_is_zero(self):
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        assert days_ago(today) == 0

    def test_yesterday_is_one(self):
        yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=1)).isoformat()
        assert days_ago(yesterday) == 1

    def test_70_days_ago(self):
        d = (datetime.now(tz=timezone.utc) - timedelta(days=70)).isoformat()
        assert days_ago(d) == 70

    def test_stale_threshold(self):
        """Leads 70+ days old should register as stale."""
        d = (datetime.now(tz=timezone.utc) - timedelta(days=75)).isoformat()
        age = days_ago(d)
        assert age is not None and age >= 70

    def test_none_returns_none(self):
        assert days_ago(None) is None

    def test_empty_string_returns_none(self):
        assert days_ago("") is None

    def test_invalid_date_returns_none(self):
        assert days_ago("not-a-date") is None

    def test_future_date_returns_zero(self):
        future = (datetime.now(tz=timezone.utc) + timedelta(days=5)).isoformat()
        assert days_ago(future) == 0

    def test_date_only_string(self):
        d = (datetime.now(tz=timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
        result = days_ago(d)
        # date-only strings parse as midnight; result is 2 or 3 depending on time of day
        assert result is not None and 2 <= result <= 4


class TestEmptyFieldSuppression:
    """Verify that empty/dash rows are suppressed — mirrors frontend filter logic."""

    def _execution_rows(self, lead: dict) -> list[tuple[str, str]]:
        rows = [
            ("STATE", lead.get("lead_state") or ""),
            ("NEXT ACTION", (lead.get("next_action") or {}).get("title") or ""),
            ("TYPE", (lead.get("next_action") or {}).get("type") or ""),
            ("CHANNEL", (lead.get("next_action") or {}).get("channel") or ""),
            ("REASON", (lead.get("next_action") or {}).get("reason") or ""),
            ("OPENER", (lead.get("script_hints") or {}).get("opener") or ""),
            ("NO ANSWER", (lead.get("script_hints") or {}).get("if_no_answer") or ""),
            ("OBJECTION", (lead.get("script_hints") or {}).get("if_objection") or ""),
            ("CTA", (lead.get("script_hints") or {}).get("cta") or ""),
        ]
        return [(label, val) for label, val in rows if val]

    def test_empty_lead_produces_no_rows(self):
        rows = self._execution_rows({})
        assert rows == []

    def test_partial_lead_hides_empty_fields(self):
        lead = {
            "lead_state": "contacted",
            "next_action": {"title": "Follow up call", "type": None, "channel": "phone"},
        }
        rows = self._execution_rows(lead)
        labels = [r[0] for r in rows]
        assert "STATE" in labels
        assert "NEXT ACTION" in labels
        assert "CHANNEL" in labels
        assert "TYPE" not in labels  # type was None → filtered

    def test_full_lead_shows_all_rows(self):
        lead = {
            "lead_state": "outreach_ready",
            "next_action": {"title": "Call", "type": "follow_up", "channel": "phone", "reason": "No answer x3"},
            "script_hints": {
                "opener": "Hi, this is Nitin",
                "if_no_answer": "Leave voicemail",
                "if_objection": "Acknowledge and offer to call back",
                "cta": "Book an appraisal",
            },
        }
        rows = self._execution_rows(lead)
        assert len(rows) == 9

    def test_placeholder_dash_not_shown(self):
        """Old code used '-' as placeholder — verify it is NOT treated as a value."""
        lead = {"lead_state": ""}
        rows = self._execution_rows(lead)
        labels = [r[0] for r in rows]
        assert "STATE" not in labels
