import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

import core.config
import core.database as db_module
from models.sales_core_models import BusinessContext, ContactAttempt, ContentAsset, LeadContact, LeadState, TaskQueue
from models.sql_models import CallLog, Lead
from services.revenue_growth_service import (
    BASIC_SEQUENCE_KEY,
    generate_daily_content_bundle,
    get_business_context_strategy,
    record_email_event,
    send_outreach_email,
    summarize_email_performance,
)


WORKSPACE_TMP = Path(r"D:\woonona-lead-machine\.backend-test-tmp")
WORKSPACE_TMP.mkdir(parents=True, exist_ok=True)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@pytest.mark.asyncio
async def test_send_outreach_email_logs_contact_attempt_and_schedules_sequence(monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = WORKSPACE_TMP / f"revenue_growth_{uuid.uuid4().hex}.db"
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

    sent_messages: list[dict] = []

    def _fake_send(account_data, body) -> None:
        sent_messages.append({"recipient": body.recipient, "subject": body.subject, "body": body.body})

    try:
        async with session_factory() as session:
            session.add_all(
                [
                    BusinessContext(key="real_estate", label="Real Estate"),
                    Lead(
                        id="lead-1",
                        address="11 Market Street, Windsor NSW 2756",
                        suburb="Windsor",
                        postcode="2756",
                        estimated_value_low=1150000,
                        estimated_value_high=1220000,
                        status="captured",
                    ),
                    LeadContact(
                        id="contact-1",
                        business_context_key="real_estate",
                        lead_id="lead-1",
                        full_name="Olivia Seller",
                        primary_email="olivia@example.com",
                    ),
                ]
            )
            await session.commit()

            result = await send_outreach_email(
                session,
                business_context_key="real_estate",
                lead_contact_id="contact-1",
                created_by="pytest",
                send_fn=_fake_send,
                now=_now(),
            )

            assert sent_messages
            assert result["attempt"].channel == "email"
            assert result["attempt"].sequence_key == BASIC_SEQUENCE_KEY
            assert result["attempt"].recipient_email == "olivia@example.com"
            assert len(result["tasks"]) == 3
            assert {task.task_type for task in result["tasks"]} == {"sequence_email", "sequence_call"}
            assert result["state"].next_action in {"review", "cooldown", "follow_up_due"}
    finally:
        await engine.dispose()
        db_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_record_email_event_updates_attempt_metrics_and_performance_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = WORKSPACE_TMP / f"revenue_growth_perf_{uuid.uuid4().hex}.db"
    database_url = f"sqlite+aiosqlite:///{db_path.as_posix()}"

    engine = create_async_engine(database_url, echo=False, future=True)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(core.config, "DATABASE_URL", database_url)
    monkeypatch.setattr(db_module, "async_engine", engine)
    monkeypatch.setattr(db_module, "_async_session_factory", session_factory)

    async with engine.begin() as conn:
        await conn.run_sync(
            SQLModel.metadata.create_all,
            tables=[BusinessContext.__table__, ContactAttempt.__table__],
        )

    try:
        async with session_factory() as session:
            session.add(BusinessContext(key="app_saas", label="App / SaaS"))
            session.add_all(
                [
                    ContactAttempt(
                        id="attempt-1",
                        business_context_key="app_saas",
                        lead_contact_id="contact-1",
                        channel="email",
                        outcome="sent",
                        variant_key="direct_offer",
                    ),
                    ContactAttempt(
                        id="attempt-2",
                        business_context_key="app_saas",
                        lead_contact_id="contact-2",
                        channel="email",
                        outcome="sent",
                        variant_key="market_intel",
                        opened_at=_now(),
                    ),
                ]
            )
            await session.commit()

            updated = await record_email_event(session, attempt_id="attempt-1", event_type="open", now=_now())
            updated = await record_email_event(session, attempt_id="attempt-1", event_type="reply", now=_now() + timedelta(minutes=5))
            summary = await summarize_email_performance(session, business_context_key="app_saas")

            assert updated.opened_at is not None
            assert updated.replied_at is not None
            assert updated.performance_json["open_count"] == 1
            assert updated.performance_json["reply_count"] == 1
            assert summary["send_count"] == 2
            assert summary["reply_count"] == 1
            assert summary["best_variant"] == "direct_offer"
    finally:
        await engine.dispose()
        db_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_generate_daily_content_bundle_creates_requested_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = WORKSPACE_TMP / f"revenue_growth_content_{uuid.uuid4().hex}.db"
    database_url = f"sqlite+aiosqlite:///{db_path.as_posix()}"

    engine = create_async_engine(database_url, echo=False, future=True)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(core.config, "DATABASE_URL", database_url)
    monkeypatch.setattr(db_module, "async_engine", engine)
    monkeypatch.setattr(db_module, "_async_session_factory", session_factory)

    async with engine.begin() as conn:
        await conn.run_sync(
            SQLModel.metadata.create_all,
            tables=[BusinessContext.__table__, Lead.__table__, CallLog.__table__, ContentAsset.__table__],
        )

    try:
        async with session_factory() as session:
            session.add(BusinessContext(key="mortgage", label="Mortgage"))
            session.add(
                Lead(
                    id="lead-1",
                    address="44 Refinance Road, Sydney NSW 2000",
                    suburb="Sydney",
                    postcode="2000",
                    route_queue="mortgage",
                )
            )
            session.add(
                CallLog(
                    id="call-1",
                    lead_id="lead-1",
                    outcome="follow_up_required",
                    transcript="Client is worried about rate increases and timing.",
                    summary="Asked for refinance options",
                    objection_tags='["timing","rate"]',
                )
            )
            await session.commit()

            result = await generate_daily_content_bundle(
                session,
                business_context_key="mortgage",
                posts_per_day=5,
                blog_count=1,
                newsletter_count=1,
                created_by="pytest",
            )

            assert result["counts"]["linkedin_post"] == 5
            assert result["counts"]["blog"] == 1
            assert result["counts"]["newsletter"] == 1
            assert len(result["assets"]) == 7
            assert any("timing" in asset.content_text.lower() for asset in result["assets"])
    finally:
        await engine.dispose()
        db_path.unlink(missing_ok=True)


def test_business_context_strategy_reflects_requested_bias() -> None:
    real_estate = get_business_context_strategy("real_estate")
    mortgage = get_business_context_strategy("mortgage")
    app_saas = get_business_context_strategy("app_saas")

    assert real_estate["channel_bias"] == "call_heavy"
    assert real_estate["sms_supported"] is True
    assert mortgage["channel_bias"] == "mixed"
    assert app_saas["channel_bias"] == "email_first"
