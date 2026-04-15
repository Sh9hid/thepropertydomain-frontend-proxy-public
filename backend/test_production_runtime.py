import importlib

import httpx
import pytest

import main
import workers.job_registry as job_registry
from core.database import _fix_asyncpg_url
from core.settings import Settings

pytestmark = pytest.mark.reliability


def test_settings_require_frontend_url_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@db.example.com:5432/leads")
    monkeypatch.setenv("REDIS_URL", "redis://cache.example.com:6379/0")
    monkeypatch.setenv("BASE_URL", "https://api.example.com")
    monkeypatch.delenv("FRONTEND_URL", raising=False)
    monkeypatch.setenv("API_KEY", "production-secret")

    with pytest.raises(ValueError, match="FRONTEND_URL"):
        Settings()


def test_settings_reject_local_redis_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@db.example.com:5432/leads")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("BASE_URL", "https://api.example.com")
    monkeypatch.setenv("FRONTEND_URL", "https://app.example.com")
    monkeypatch.setenv("API_KEY", "production-secret")

    with pytest.raises(ValueError, match="REDIS_URL"):
        Settings()


def test_core_config_rejects_sqlite_database_url_in_production(monkeypatch):
    import core.config as config_module

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///tmp/runtime.db")
    monkeypatch.setenv("REDIS_URL", "redis://cache.example.com:6379/0")
    monkeypatch.setenv("BASE_URL", "https://api.example.com")
    monkeypatch.setenv("FRONTEND_URL", "https://app.example.com")
    monkeypatch.setenv("API_KEY", "production-secret")

    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        importlib.reload(config_module)


@pytest.mark.asyncio
async def test_healthcheck_reports_dependency_success(monkeypatch):
    async def _db_ok():
        return "ok", None

    async def _redis_ok():
        return "ok", None

    monkeypatch.setattr(main.app.state, "database_health_check", _db_ok)
    monkeypatch.setattr(main.app.state, "redis_health_check", _redis_ok)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as ac:
        response = await ac.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "app_env": main.core_config.APP_ENV,
        "services": {
            "database": "ok",
            "redis": "ok",
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("dependency_name", ["database", "redis"])
async def test_healthcheck_returns_503_when_dependency_unavailable(monkeypatch, dependency_name):
    async def _db_status():
        if dependency_name == "database":
            return "error", "database unavailable"
        return "ok", None

    async def _redis_status():
        if dependency_name == "redis":
            return "error", "redis unavailable"
        return "ok", None

    monkeypatch.setattr(main.app.state, "database_health_check", _db_status)
    monkeypatch.setattr(main.app.state, "redis_health_check", _redis_status)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as ac:
        response = await ac.get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "error"
    assert body["services"][dependency_name] == "error"


def test_runtime_task_functions_are_not_owned_by_main_module():
    runtime_loops = importlib.import_module("runtime.loops")

    for task_name in job_registry.get_runtime_task_names("scheduler"):
        loop_fn = getattr(runtime_loops, task_name)
        assert callable(loop_fn)


def test_fix_asyncpg_url_converts_sslmode_to_ssl():
    raw = "postgresql+asyncpg://u:p@db.example.com:5432/leads?sslmode=require&channel_binding=require"
    normalized = _fix_asyncpg_url(raw)
    assert "ssl=require" in normalized
    assert "sslmode=" not in normalized
    assert "channel_binding=" not in normalized


def test_fix_asyncpg_url_defaults_ssl_for_managed_hosts():
    raw = "postgresql+asyncpg://u:p@db.example.com:5432/leads"
    normalized = _fix_asyncpg_url(raw)
    assert normalized.endswith("?ssl=require")


def test_fix_asyncpg_url_does_not_force_ssl_for_local_hosts():
    raw = "postgresql+asyncpg://u:p@localhost:5432/leads"
    normalized = _fix_asyncpg_url(raw)
    assert normalized == raw
