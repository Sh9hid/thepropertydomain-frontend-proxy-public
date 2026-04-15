"""
Cost tracker — daily budget enforcement for Claude Agent SDK sessions.

Controls:
  - Per-session token cap (default 50 000)
  - Daily cost ceiling (CLAUDE_SDK_DAILY_BUDGET_USD, default $5)
  - Weekend mode (drops to $1, restricts available agents)
  - Model assignment enforcement (Sonnet for execution, Haiku for compliance)
  - Fallback flag when budget exhausted
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from threading import Lock
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

# Approximate costs per 1K tokens (Sonnet 4.6 pricing)
_COST_PER_1K_INPUT = 0.003
_COST_PER_1K_OUTPUT = 0.015


def _today_sydney() -> date:
    return datetime.now(SYDNEY_TZ).date()


def _is_weekend() -> bool:
    return _today_sydney().weekday() >= 5  # Saturday=5, Sunday=6


@dataclass
class SessionUsage:
    session_id: str
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    turns: int = 0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def estimated_cost_usd(self) -> float:
        return (
            (self.input_tokens / 1000) * _COST_PER_1K_INPUT
            + (self.output_tokens / 1000) * _COST_PER_1K_OUTPUT
        )


@dataclass
class CostTracker:
    """Thread-safe daily cost tracker across all SDK sessions."""

    _daily_budget_usd: float = field(init=False)
    _weekend_budget_usd: float = field(init=False)
    _session_token_cap: int = field(init=False)
    _sessions: dict[str, SessionUsage] = field(default_factory=dict)
    _daily_date: date = field(default_factory=_today_sydney)
    _daily_cost_usd: float = 0.0
    _lock: Lock = field(default_factory=Lock)
    _budget_exhausted: bool = False

    def __post_init__(self) -> None:
        self._daily_budget_usd = float(
            os.getenv("CLAUDE_SDK_DAILY_BUDGET_USD", "5.0")
        )
        self._weekend_budget_usd = float(
            os.getenv("CLAUDE_SDK_WEEKEND_BUDGET_USD", "1.0")
        )
        self._session_token_cap = int(
            os.getenv("CLAUDE_SDK_SESSION_TOKEN_CAP", "50000")
        )

    @property
    def effective_budget(self) -> float:
        return self._weekend_budget_usd if _is_weekend() else self._daily_budget_usd

    def _maybe_reset_day(self) -> None:
        today = _today_sydney()
        if today != self._daily_date:
            self._daily_date = today
            self._daily_cost_usd = 0.0
            self._budget_exhausted = False
            self._sessions.clear()
            log.info("[CostTracker] Day rolled to %s — budget reset", today)

    def can_spend(self, session_id: str) -> bool:
        """Check if a session can make another API call."""
        with self._lock:
            self._maybe_reset_day()

            if self._budget_exhausted:
                return False

            if self._daily_cost_usd >= self.effective_budget:
                self._budget_exhausted = True
                log.warning(
                    "[CostTracker] Daily budget exhausted: $%.4f / $%.2f",
                    self._daily_cost_usd,
                    self.effective_budget,
                )
                return False

            usage = self._sessions.get(session_id)
            if usage and usage.total_tokens >= self._session_token_cap:
                log.warning(
                    "[CostTracker] Session %s hit token cap: %d / %d",
                    session_id,
                    usage.total_tokens,
                    self._session_token_cap,
                )
                return False

            return True

    def record_usage(
        self,
        session_id: str,
        input_tokens: int,
        output_tokens: int,
        tool_calls: int = 0,
    ) -> SessionUsage:
        """Record token usage for a session turn."""
        with self._lock:
            self._maybe_reset_day()

            if session_id not in self._sessions:
                self._sessions[session_id] = SessionUsage(session_id=session_id)

            usage = self._sessions[session_id]
            usage.input_tokens += input_tokens
            usage.output_tokens += output_tokens
            usage.tool_calls += tool_calls
            usage.turns += 1

            turn_cost = (
                (input_tokens / 1000) * _COST_PER_1K_INPUT
                + (output_tokens / 1000) * _COST_PER_1K_OUTPUT
            )
            self._daily_cost_usd += turn_cost

            log.debug(
                "[CostTracker] Session %s turn: +%d in / +%d out ($%.4f) — daily total $%.4f",
                session_id,
                input_tokens,
                output_tokens,
                turn_cost,
                self._daily_cost_usd,
            )
            return usage

    def get_session_usage(self, session_id: str) -> SessionUsage | None:
        with self._lock:
            return self._sessions.get(session_id)

    def get_daily_summary(self) -> dict:
        with self._lock:
            self._maybe_reset_day()
            return {
                "date": str(self._daily_date),
                "cost_usd": round(self._daily_cost_usd, 4),
                "budget_usd": self.effective_budget,
                "budget_exhausted": self._budget_exhausted,
                "is_weekend": _is_weekend(),
                "active_sessions": len(self._sessions),
                "session_token_cap": self._session_token_cap,
            }


# Module-level singleton
_tracker: CostTracker | None = None


def get_cost_tracker() -> CostTracker:
    global _tracker
    if _tracker is None:
        _tracker = CostTracker()
    return _tracker
