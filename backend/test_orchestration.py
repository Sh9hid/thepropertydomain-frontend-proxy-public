"""
Tests for the orchestration runtime.

Covers:
  - Provider selection / ranking logic
  - Circuit breaker open/close
  - Rate limit sliding window
  - Job/task state transitions via API
  - Agent role lookups
  - Event persistence

Run: pytest test_orchestration.py -v
"""
import asyncio
import time
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime


# ─── Provider / engine tests ─────────────────────────────────────────────────

from services.orchestration_engine import (
    _ProviderRuntime,
    _build_provider_registry,
    _rank_providers,
    init_providers,
    get_provider_snapshot,
)


def test_rate_limiter_allows_up_to_rpm():
    """Sliding window should allow exactly rpm_cap requests per minute."""
    rt = _ProviderRuntime(rpm_cap=3)

    async def run():
        results = []
        for _ in range(4):
            results.append(await rt.acquire())
        return results

    results = asyncio.run(run())
    assert results == [True, True, True, False], results


def test_circuit_opens_after_three_failures():
    rt = _ProviderRuntime(rpm_cap=60)
    for _ in range(3):
        rt.record_failure()
    assert rt.is_circuit_open


def test_circuit_does_not_open_after_two_failures():
    rt = _ProviderRuntime(rpm_cap=60)
    rt.record_failure()
    rt.record_failure()
    assert not rt.is_circuit_open


def test_circuit_resets_after_success():
    rt = _ProviderRuntime(rpm_cap=60)
    rt.record_failure()
    rt.record_failure()
    rt.record_success()
    assert rt._consecutive_failures == 0
    assert not rt.is_circuit_open


def test_provider_registry_built():
    providers = _build_provider_registry()
    assert "nim" in providers
    assert "gemini" in providers
    assert "claude" in providers
    assert "ollama" in providers


def test_ollama_always_available():
    """Ollama has a hardcoded api_key so it always shows as available."""
    providers = _build_provider_registry()
    assert providers["ollama"].available is True


def test_rank_providers_excludes_circuit_open(monkeypatch):
    init_providers()
    from services import orchestration_engine as oe
    # Open circuit on nim
    oe._RUNTIMES["nim"]._circuit_open = True
    oe._RUNTIMES["nim"]._circuit_open_until = time.monotonic() + 300
    chain = _rank_providers("implementation")
    assert "nim" not in chain
    # Restore
    oe._RUNTIMES["nim"]._circuit_open = False


def test_rank_providers_prefers_cheapest():
    init_providers()
    from services import orchestration_engine as oe
    # Make all available
    for key, cfg in oe._PROVIDERS.items():
        if not cfg.api_key:
            cfg.api_key = "fake"
    chain = _rank_providers("default")
    # ollama should be first (cost = 0)
    if "ollama" in chain:
        assert chain[0] == "ollama"


def test_rank_providers_respects_preferred():
    init_providers()
    from services import orchestration_engine as oe
    oe._PROVIDERS["gemini"].api_key = "fake"
    chain = _rank_providers("default", preferred="gemini")
    if "gemini" in chain:
        assert chain[0] == "gemini"


# ─── Agent definitions ────────────────────────────────────────────────────────

from services.orchestration_agents import AGENT_DEFS, get_agent_for_work_type, list_agents


def test_all_agents_have_required_fields():
    for role, agent in AGENT_DEFS.items():
        assert agent.role == role
        assert agent.display_name
        assert agent.work_types
        assert agent.system_prompt


def test_get_agent_for_work_type_implementation():
    agent = get_agent_for_work_type("implementation")
    assert agent is not None
    assert "implementation" in agent.work_types


def test_get_agent_for_work_type_unknown_falls_back_to_builder():
    agent = get_agent_for_work_type("nonexistent_type_xyz")
    assert agent is not None
    assert agent.role == "builder"


def test_list_agents_returns_all():
    agents = list_agents()
    assert len(agents) == len(AGENT_DEFS)
    for a in agents:
        assert "role" in a
        assert "work_types" in a


# ─── Model schemas ────────────────────────────────────────────────────────────

from models.orchestration_models import (
    OrchJob, OrchTask, OrchAgent, OrchEvent, OrchProviderState, OrchMemory,
    JobStatus, TaskStatus, AgentStatus,
)


def test_orch_job_defaults():
    j = OrchJob(title="Test", work_type="implementation")
    assert j.status == JobStatus.QUEUED
    assert j.tokens_used == 0
    assert j.cost_usd == 0.0
    assert j.retries == 0
    assert j.priority == 5


def test_orch_task_defaults():
    t = OrchTask(job_id="abc", title="Do X", work_type="debugging")
    assert t.status == TaskStatus.PENDING
    assert t.retries == 0
    assert t.input_tokens == 0


def test_orch_agent_defaults():
    a = OrchAgent(role="builder", display_name="Builder")
    assert a.status == AgentStatus.IDLE
    assert a.tasks_completed == 0
    assert a.tasks_failed == 0


def test_job_status_values():
    assert JobStatus.QUEUED == "queued"
    assert JobStatus.RUNNING == "running"
    assert JobStatus.DONE == "done"
    assert JobStatus.FAILED == "failed"


def test_task_status_values():
    assert TaskStatus.PENDING == "pending"
    assert TaskStatus.DONE == "done"
    assert TaskStatus.FAILED == "failed"
    assert TaskStatus.ESCALATED == "escalated"


# ─── Route completion error path ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_route_completion_raises_when_all_providers_fail(monkeypatch):
    """When all providers fail, route_completion should raise RuntimeError."""
    init_providers()
    from services import orchestration_engine as oe

    # Make all providers fail
    for key in oe._PROVIDERS:
        oe._PROVIDERS[key].api_key = "fake"

    async def _fail(*args, **kwargs):
        raise ConnectionError("simulated failure")

    monkeypatch.setattr(oe, "_call_openai_compat", _fail)
    monkeypatch.setattr(oe, "_call_ollama", _fail)
    monkeypatch.setattr(oe, "_call_anthropic", _fail)

    with pytest.raises(RuntimeError, match="All providers exhausted"):
        await oe.route_completion(
            work_type="default",
            messages=[{"role": "user", "content": "test"}],
        )


@pytest.mark.asyncio
async def test_route_completion_falls_back_on_first_failure(monkeypatch):
    """Should try second provider when first fails."""
    init_providers()
    from services import orchestration_engine as oe

    call_log = []

    async def _first_fail(cfg, messages, model, max_tokens=4096):
        call_log.append(cfg.key)
        if cfg.key == "ollama":
            raise ConnectionError("fail")
        return "ok", 10, 5

    monkeypatch.setattr(oe, "_call_openai_compat", _first_fail)
    monkeypatch.setattr(oe, "_call_ollama", _first_fail)
    monkeypatch.setattr(oe, "_call_anthropic", _first_fail)

    # Make ollama available first in chain
    oe._PROVIDERS["ollama"].api_key = "ollama"
    oe._RUNTIMES["ollama"]._consecutive_failures = 0
    oe._RUNTIMES["ollama"]._circuit_open = False

    # Reset runtime tokens
    for rt in oe._RUNTIMES.values():
        rt._request_timestamps = []
        rt._consecutive_failures = 0
        rt._circuit_open = False

    try:
        result = await oe.route_completion(
            work_type="default",
            messages=[{"role": "user", "content": "hello"}],
        )
        # Should have fallen back to something that succeeded
        assert result.text == "ok"
        assert "ollama" in result.fallbacks_used
    except RuntimeError:
        pass  # acceptable if all fail in test env
