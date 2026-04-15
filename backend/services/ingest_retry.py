"""Ingest retry — circuit-breaker for external data pulls."""
from typing import Any

_failures: dict[str, int] = {}
_successes: dict[str, int] = {}


def can_attempt(source_key: str, max_failures: int = 5) -> bool:
    return _failures.get(source_key, 0) < max_failures


def mark_failure(source_key: str) -> None:
    _failures[source_key] = _failures.get(source_key, 0) + 1


def mark_success(source_key: str) -> None:
    _failures[source_key] = 0
    _successes[source_key] = _successes.get(source_key, 0) + 1
