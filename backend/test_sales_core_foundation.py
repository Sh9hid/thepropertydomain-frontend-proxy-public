import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

import core.config
import core.database as db_module
from models.sales_core_models import (
    BusinessContext,
    ContactAttempt,
    EnrichmentState,
    LeadContact,
    LeadState,
    TaskQueue,
)
from models.sql_models import Lead
from services.provider_routing import load_provider_routing_policy, resolve_provider_for_feature
from services.sales_core.dialing_service import log_contact_attempt, sync_lead_state
from services.sales_core.enrichment_service import enqueue_enrichment_job
from services.sales_core.state_engine import build_lead_state_snapshot


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def test_callable_now_requires_phone_not_dnc_and_not_cooled_down() -> None:
    now = _now()
    snapshot = build_lead_state_snapshot(
        {
            "lead_contact_id": "contact-1",
            "business_context_key": "real_estate",
            "lead_status": "captured",
            "primary_phone": "+61412345678",
            "phone_verified": True,
            "do_not_call": False,
            "attempts": [
                {
                    "attempted_at": (now - timedelta(days=3)).isoformat(),
                    "outcome": "no_answer",
                    "connected": False,
                }
            ],
            "lead_est_value": 1250000,
            "lead_heat_score": 88,
            "lead_evidence_score": 72,
            "next_action_due_at": (now - timedelta(minutes=5)).isoformat(),
        },
        now=now,
    )

    assert snapshot["callable_now"] is True
    assert snapshot["next_action"] == "call"
    assert snapshot["fatigue_band"] == "low"


def test_cooldown_blocks_repeated_calls_and_sets_follow_up_due() -> None:
    now = _now()
    snapshot = build_lead_state_snapshot(
        {
            "lead_contact_id": "contact-1",
            "business_context_key": "real_estate",
            "lead_status": "contacted",
            "primary_phone": "+61412345678",
            "phone_verified": True,
            "do_not_call": False,
            "attempts": [
                {
                    "attempted_at": (now - timedelta(hours=4)).isoformat(),
                    "outcome": "voicemail_left",
                    "connected": False,
                    "next_action_due_at": (now + timedelta(hours=20)).isoformat(),
                },
                {
                    "attempted_at": (now - timedelta(days=1)).isoformat(),
                    "outcome": "no_answer",
                    "connected": False,
                },
            ],
            "lead_est_value": 970000,
            "lead_heat_score": 61,
            "lead_evidence_score": 45,
        },
        now=now,
    )

    assert snapshot["callable_now"] is False
    assert snapshot["fatigue_band"] == "medium"
    assert snapshot["next_action"] == "follow_up_due"
    assert snapshot["next_action_due_at"] == (now + timedelta(hours=20)).isoformat()


def test_enrich_first_prioritizes_hot_records_missing_contactability() -> None:
    now = _now()
    snapshot = build_lead_state_snapshot(
        {
            "lead_contact_id": "contact-1",
            "business_context_key": "mortgage",
            "lead_status": "captured",
            "primary_phone": "",
            "phone_verified": False,
            "primary_email": "",
            "do_not_call": False,
            "attempts": [],
            "lead_est_value": 1650000,
            "lead_heat_score": 91,
            "lead_evidence_score": 79,
            "enrichment_status": "stale",
        },
        now=now,
    )

    assert snapshot["callable_now"] is False
    assert snapshot["needs_enrichment"] is True
    assert snapshot["next_action"] == "enrich_first"
    assert snapshot["queue_score"] >= 100


WORKSPACE_TMP = Path(r"D:\woonona-lead-machine\.backend-test-tmp")
WORKSPACE_TMP.mkdir(parents=True, exist_ok=True)


@pytest.mark.asyncio
async def test_enrichment_queue_avoids_duplicate_hammering(monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = WORKSPACE_TMP / f"sales_core_enrichment_{uuid.uuid4().hex}.db"
    database_url = f"sqlite+aiosqlite:///{db_path.as_posix()}"

    engine = create_async_engine(database_url, echo=False, future=True)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(core.config, "DATABASE_URL", database_url)
    monkeypatch.setattr(db_module, "async_engine", engine)
    monkeypatch.setattr(db_module, "_async_session_factory", session_factory)

    async with engine.begin() as conn:
        await conn.run_sync(
            SQLModel.metadata.create_all,
            tables=[
                BusinessContext.__table__,
                Lead.__table__,
                LeadContact.__table__,
                LeadState.__table__,
                EnrichmentState.__table__,
            ],
        )

    try:
        async with session_factory() as session:
            session.add(BusinessContext(key="real_estate", label="Real Estate"))
            session.add(
                Lead(
                    id="lead-1",
                    address="1 Queue Street, Test NSW 2000",
                    suburb="Test",
                    postcode="2000",
                    est_value=1300000,
                    heat_score=85,
                    evidence_score=70,
                    status="captured",
                )
            )
            session.add(
                LeadContact(
                    id="contact-1",
                    business_context_key="real_estate",
                    lead_id="lead-1",
                    full_name="Owner One",
                )
            )
            await session.commit()

            first = await enqueue_enrichment_job(
                session,
                business_context_key="real_estate",
                lead_contact_id="contact-1",
                source="rp_data",
                reason="missing_contactability",
                now=_now(),
            )
            second = await enqueue_enrichment_job(
                session,
                business_context_key="real_estate",
                lead_contact_id="contact-1",
                source="rp_data",
                reason="missing_contactability",
                now=_now(),
            )

            assert first["enqueued"] is True
            assert second["enqueued"] is False
            assert second["reason"] == "cooldown_active"
    finally:
        await engine.dispose()
        db_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_attempt_logging_creates_follow_up_task_and_updates_state(monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = WORKSPACE_TMP / f"sales_core_dialing_{uuid.uuid4().hex}.db"
    database_url = f"sqlite+aiosqlite:///{db_path.as_posix()}"

    engine = create_async_engine(database_url, echo=False, future=True)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(core.config, "DATABASE_URL", database_url)
    monkeypatch.setattr(db_module, "async_engine", engine)
    monkeypatch.setattr(db_module, "_async_session_factory", session_factory)

    async with engine.begin() as conn:
        await conn.run_sync(
            SQLModel.metadata.create_all,
            tables=[
                BusinessContext.__table__,
                Lead.__table__,
                LeadContact.__table__,
                ContactAttempt.__table__,
                LeadState.__table__,
                TaskQueue.__table__,
            ],
        )

    now = _now()
    try:
        async with session_factory() as session:
            session.add_all(
                [
                    BusinessContext(key="real_estate", label="Real Estate"),
                    Lead(
                        id="lead-1",
                        address="9 Dial Street, Test NSW 2000",
                        suburb="Test",
                        postcode="2000",
                        est_value=1180000,
                        heat_score=76,
                        evidence_score=68,
                        status="captured",
                    ),
                    LeadContact(
                        id="contact-1",
                        business_context_key="real_estate",
                        lead_id="lead-1",
                        full_name="Owner Caller",
                        primary_phone="+61411111111",
                        phone_verification_status="verified",
                    ),
                ]
            )
            await session.commit()

            await sync_lead_state(session, "contact-1", now=now)
            result = await log_contact_attempt(
                session,
                {
                    "business_context_key": "real_estate",
                    "lead_contact_id": "contact-1",
                    "channel": "call",
                    "outcome": "call_back",
                    "connected": True,
                    "duration_seconds": 180,
                    "voicemail_left": False,
                    "note": "Asked for a call back tomorrow morning",
                    "created_by": "operator",
                    "next_action_due_at": now + timedelta(days=1),
                },
                now=now,
            )

            assert result["task"] is not None
            assert result["task"].task_type == "follow_up_call"
            assert result["state"].next_action == "follow_up_due"
            assert result["state"].callable_now is False
    finally:
        await engine.dispose()
        db_path.unlink(missing_ok=True)


def test_provider_routing_uses_fallback_when_primary_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("KIMI_API_KEY", "kimi-token")
    monkeypatch.setenv("AI_PROVIDER_GEMINI_ENABLED", "true")
    monkeypatch.setenv("AI_PROVIDER_KIMI_ENABLED", "true")
    monkeypatch.setenv("AI_FEATURE_LIGHT_DRAFTING_ENABLED", "true")

    policy = load_provider_routing_policy()
    decision = resolve_provider_for_feature(policy, feature="light_drafting", task_class="cheap")

    assert decision.provider == "kimi"
    assert decision.allowed is True
    assert decision.reason == "selected"


def test_provider_routing_gracefully_disables_feature_when_no_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("KIMI_API_KEY", raising=False)
    monkeypatch.setenv("AI_PROVIDER_GEMINI_ENABLED", "true")
    monkeypatch.setenv("AI_PROVIDER_KIMI_ENABLED", "true")
    monkeypatch.setenv("AI_FEATURE_CALL_SUMMARY_ENABLED", "true")

    policy = load_provider_routing_policy()
    decision = resolve_provider_for_feature(policy, feature="call_summary", task_class="expensive")

    assert decision.allowed is False
    assert decision.provider is None
    assert decision.reason == "no_provider_available"
