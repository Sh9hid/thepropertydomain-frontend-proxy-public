import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from workflow_trace import sanitize_dom_candidate, sanitize_trace_event  # noqa: E402


def test_sanitize_trace_event_redacts_password_values():
    event = {
        "type": "input",
        "element": {
            "tag": "input",
            "type": "password",
            "name": "password",
            "value": "super-secret",
            "nearbyText": ["Password"],
        },
        "extra": {"value": "super-secret"},
    }

    sanitized = sanitize_trace_event(event)

    assert sanitized["element"]["value"] == "[REDACTED]"
    assert sanitized["extra"]["value"] == "[REDACTED]"


def test_sanitize_dom_candidate_drops_sensitive_password_controls():
    candidate = {
        "tag": "input",
        "type": "password",
        "name": "password",
        "placeholder": "Password",
        "value": "super-secret",
    }

    assert sanitize_dom_candidate(candidate) is None
