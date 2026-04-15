import asyncio
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module

from api.routes import missed_deals
from core.config import API_KEY
from models.sql_models import CallLog, Lead
from services.followup_engine import generate_follow_up
from services.missed_deals_service import get_missed_deals


app = FastAPI()
app.include_router(missed_deals.router)
SYDNEY_TZ = ZoneInfo("Australia/Sydney")


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"missed_deals_{uuid.uuid4().hex}.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(
        db_module,
        "_async_session_factory",
        sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False),
    )
    db_module.init_db()
    yield test_db
    asyncio.run(test_engine.dispose())
    if test_db.exists():
        test_db.unlink()


async def seed_lead(
    session: AsyncSession,
    *,
    lead_id: str,
    owner_name: str,
    address: str,
    status: str = "contacted",
    last_contacted_at: str | None = None,
    follow_up_due_at: str | None = None,
    price_drop_count: int = 0,
    last_activity_type: str = "call",
    suburb: str = "Woonona",
    postcode: str = "2517",
    heat_score: int = 61,
    evidence_score: int = 46,
    est_value: int = 950000,
):
    now = datetime.now(SYDNEY_TZ).replace(microsecond=0)
    session.add(
        Lead(
            id=lead_id,
            address=address,
            suburb=suburb,
            postcode=postcode,
            owner_name=owner_name,
            record_type="property_record",
            status=status,
            heat_score=heat_score,
            evidence_score=evidence_score,
            est_value=est_value,
            created_at=(now - timedelta(days=6)).isoformat(),
            updated_at=now.isoformat(),
            last_contacted_at=last_contacted_at,
            follow_up_due_at=follow_up_due_at,
            price_drop_count=price_drop_count,
            last_activity_type=last_activity_type,
        )
    )


async def seed_call(
    session: AsyncSession,
    *,
    row_id: str,
    lead_id: str,
    timestamp: str,
    connected: int,
    outcome: str,
    intent_signal: float,
    booking_attempted: int,
    next_step_detected: int,
    call_duration_seconds: int = 180,
    summary: str = "",
    transcript: str = "",
    objection_tags: str = "[]",
    user_id: str = "Shahid",
):
    session.add(
        CallLog(
            id=row_id,
            lead_id=lead_id,
            lead_address=f"{lead_id} Example Street, Woonona NSW 2517",
            user_id=user_id,
            outcome=outcome,
            connected=bool(connected),
            timestamp=timestamp,
            logged_at=timestamp,
            logged_date=timestamp[:10],
            call_duration_seconds=call_duration_seconds,
            duration_seconds=call_duration_seconds,
            note="",
            operator=user_id,
            provider="manual",
            provider_call_id=f"provider-{row_id}",
            direction="outbound",
            from_number="",
            to_number="",
            raw_payload="{}",
            summary=summary,
            transcript=transcript,
            intent_signal=intent_signal,
            booking_attempted=bool(booking_attempted),
            objection_tags=objection_tags,
            next_step_detected=bool(next_step_detected),
        )
    )


def iso_hours_ago(hours: int) -> str:
    return (datetime.now(SYDNEY_TZ) - timedelta(hours=hours)).replace(microsecond=0).isoformat()


def iso_hours_ahead(hours: int) -> str:
    return (datetime.now(SYDNEY_TZ) + timedelta(hours=hours)).replace(microsecond=0).isoformat()


def _card_by_lead(cards, lead_id: str):
    return next(card for card in cards if card["lead_id"] == lead_id)


@pytest.mark.asyncio
async def test_high_intent_no_booking_detected_with_premium_contract(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(
            session,
            lead_id="lead-booking",
            owner_name="Lead Booking",
            address="1 Booking Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(2),
        )
        await seed_call(
            session,
            row_id="call-booking",
            lead_id="lead-booking",
            timestamp=iso_hours_ago(2),
            connected=1,
            outcome="connected_interested",
            intent_signal=0.85,
            booking_attempted=0,
            next_step_detected=1,
            summary="Owner sounded ready but no appraisal was asked for.",
            objection_tags='["bad timing"]',
        )
        await session.commit()
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")

    card = _card_by_lead(cards, "lead-booking")
    assert card["reason"] == "MISSED_BOOKING"
    assert card["reason_label"] == "Missed Booking"
    assert card["lead_name"] == "Lead Booking"
    assert card["address"] == "1 Booking Street, Woonona NSW 2517"
    assert card["action_type"] == "CALL_NOW"
    assert card["urgency_band"] == "NOW"
    assert card["estimated_recovery_class"] == "HIGH"
    assert card["last_call_outcome"] == "connected_interested"
    assert card["last_call_summary"] == "Owner sounded ready but no appraisal was asked for."
    assert card["intent_signal"] == pytest.approx(0.85)
    assert card["hours_since_contact"] >= 1.9
    assert card["follow_up_due_at"]
    assert "booking_attempted=no" in card["reason_detail"]
    assert card["last_objection_tags"] == ["bad timing"]
    assert card["booking_attempted"] is False
    assert card["next_step_detected"] is True


@pytest.mark.asyncio
async def test_connected_no_follow_up_detected(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(
            session,
            lead_id="lead-follow-up",
            owner_name="Lead Follow Up",
            address="2 Follow Up Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(3),
            follow_up_due_at=None,
        )
        await seed_call(
            session,
            row_id="call-follow-up",
            lead_id="lead-follow-up",
            timestamp=iso_hours_ago(3),
            connected=1,
            outcome="connected_follow_up",
            intent_signal=0.72,
            booking_attempted=1,
            next_step_detected=1,
            summary="Seller asked for a callback tomorrow.",
        )
        await session.commit()
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")

    card = _card_by_lead(cards, "lead-follow-up")
    assert card["reason"] == "NO_FOLLOW_UP"
    assert card["action_type"] == "SET_FOLLOW_UP"
    assert card["estimated_recovery_class"] in {"HIGH", "MEDIUM"}
    assert "follow_up=missing" in card["reason_detail"]


@pytest.mark.asyncio
async def test_stale_high_intent_detected(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(
            session,
            lead_id="lead-stale",
            owner_name="Lead Stale",
            address="3 Stale Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(60),
        )
        await seed_call(
            session,
            row_id="call-stale",
            lead_id="lead-stale",
            timestamp=iso_hours_ago(60),
            connected=1,
            outcome="connected_interested",
            intent_signal=0.63,
            booking_attempted=1,
            next_step_detected=0,
            summary="Lead asked questions and showed interest.",
        )
        await session.commit()
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")

    card = _card_by_lead(cards, "lead-stale")
    assert card["reason"] == "STALE_HIGH_INTENT"
    assert card["action_type"] == "REENGAGE"
    assert card["hours_since_contact"] >= 59
    assert "hours_since_contact=" in card["reason_detail"]


@pytest.mark.asyncio
async def test_price_drop_trigger_detected(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(
            session,
            lead_id="lead-price-drop",
            owner_name="Lead Price Drop",
            address="4 Price Drop Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(120),
            price_drop_count=2,
        )
        await seed_call(
            session,
            row_id="call-price-drop",
            lead_id="lead-price-drop",
            timestamp=iso_hours_ago(80),
            connected=0,
            outcome="no_answer",
            intent_signal=0.1,
            booking_attempted=0,
            next_step_detected=0,
            call_duration_seconds=0,
        )
        await session.commit()
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")

    card = _card_by_lead(cards, "lead-price-drop")
    assert card["reason"] == "PRICE_DROP_OPPORTUNITY"
    assert card["action_type"] == "PRICE_DROP_REACHOUT"
    assert "price_drops=2" in card["reason_detail"]


@pytest.mark.asyncio
async def test_no_false_positives_for_recently_contacted_leads(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(
            session,
            lead_id="lead-clean",
            owner_name="Lead Clean",
            address="5 Clean Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(1),
            follow_up_due_at=iso_hours_ahead(24),
            price_drop_count=0,
        )
        await seed_call(
            session,
            row_id="call-clean",
            lead_id="lead-clean",
            timestamp=iso_hours_ago(1),
            connected=1,
            outcome="connected",
            intent_signal=0.45,
            booking_attempted=1,
            next_step_detected=0,
        )
        await session.commit()
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")

    assert not any(card["lead_id"] == "lead-clean" for card in cards)


@pytest.mark.asyncio
async def test_null_and_missing_fields_do_not_break_engine(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(
            session,
            lead_id="lead-null",
            owner_name="Lead Null",
            address="6 Null Street, Woonona NSW 2517",
            last_contacted_at=None,
            price_drop_count=1,
            last_activity_type="",
        )
        await seed_call(
            session,
            row_id="call-null",
            lead_id="lead-null",
            timestamp=iso_hours_ago(90),
            connected=0,
            outcome="no_answer",
            intent_signal=0.0,
            booking_attempted=0,
            next_step_detected=0,
            summary="",
            transcript="",
            objection_tags="[]",
        )
        await session.commit()
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")

    card = _card_by_lead(cards, "lead-null")
    assert card["reason"] == "PRICE_DROP_OPPORTUNITY"
    assert card["last_call_summary"] == ""
    assert card["last_call_outcome"] == "no_answer"


@pytest.mark.asyncio
async def test_priority_sort_and_api_contract_shape(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_lead(
            session,
            lead_id="lead-low",
            owner_name="Lead Low",
            address="7 Low Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(72),
        )
        await seed_call(
            session,
            row_id="call-low",
            lead_id="lead-low",
            timestamp=iso_hours_ago(72),
            connected=1,
            outcome="connected_interested",
            intent_signal=0.62,
            booking_attempted=0,
            next_step_detected=0,
        )
        await seed_lead(
            session,
            lead_id="lead-high",
            owner_name="Lead High",
            address="8 High Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(2),
        )
        await seed_call(
            session,
            row_id="call-high",
            lead_id="lead-high",
            timestamp=iso_hours_ago(2),
            connected=1,
            outcome="connected_interested",
            intent_signal=0.93,
            booking_attempted=0,
            next_step_detected=1,
            summary="Asked about next steps quickly.",
            objection_tags='["price too low"]',
        )
        await session.commit()

    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/missed-deals?range=today", headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert data[0]["lead_id"] == "lead-high"
    assert data[0]["priority_score"] >= data[1]["priority_score"]
    assert {
        "lead_id",
        "lead_name",
        "address",
        "reason",
        "reason_label",
        "priority_score",
        "last_contacted_at",
        "last_call_outcome",
        "last_call_summary",
        "suggested_action",
        "follow_up_due_at",
        "hours_since_contact",
        "intent_signal",
        "urgency_band",
        "action_type",
        "estimated_recovery_class",
        "reason_detail",
    }.issubset(data[0].keys())


@pytest.mark.asyncio
async def test_follow_up_generation_and_route_persist_updates(isolated_db):
    async with db_module._async_session_factory() as session:
        lead = {
            "id": "lead-follow-up-save",
            "owner_name": "Lead Follow Up Save",
            "address": "9 Follow Up Save Street, Woonona NSW 2517",
        }
        preview = generate_follow_up(lead, "MISSED_BOOKING")
        assert preview["action_type"] == "CALL_NOW"
        assert preview["follow_up_due_at"]

        await seed_lead(
            session,
            lead_id="lead-follow-up-save",
            owner_name="Lead Follow Up Save",
            address="9 Follow Up Save Street, Woonona NSW 2517",
            last_contacted_at=iso_hours_ago(3),
        )
        await seed_call(
            session,
            row_id="call-follow-up-save",
            lead_id="lead-follow-up-save",
            timestamp=iso_hours_ago(3),
            connected=1,
            outcome="connected_interested",
            intent_signal=0.88,
            booking_attempted=0,
            next_step_detected=1,
        )
        await session.commit()

    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/api/missed-deals/lead-follow-up-save/follow-up",
            headers=headers,
            json={"reason": "MISSED_BOOKING"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["lead_id"] == "lead-follow-up-save"
    assert payload["last_activity_type"] == "follow_up"
    assert payload["follow_up_due_at"]
    assert payload["lead"]["follow_up_due_at"] == payload["follow_up_due_at"]
    assert payload["lead"]["last_activity_type"] == "follow_up"
    assert payload["removed_from_queue"] is True

    async with db_module._async_session_factory() as session:
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")
        lead_row = (
            await session.execute(
                text(
                    "SELECT follow_up_due_at, last_activity_type FROM leads WHERE id = 'lead-follow-up-save'"
                )
            )
        ).mappings().first()

    assert not any(card["lead_id"] == "lead-follow-up-save" for card in cards)
    assert lead_row["follow_up_due_at"]
    assert lead_row["last_activity_type"] == "follow_up"


@pytest.mark.asyncio
async def test_route_auth_required(isolated_db):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/missed-deals?range=today")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_scale_behavior_stays_reasonable(isolated_db):
    async with db_module._async_session_factory() as session:
        for idx in range(160):
            lead_id = f"lead-scale-{idx}"
            await seed_lead(
                session,
                lead_id=lead_id,
                owner_name=f"Lead Scale {idx}",
                address=f"{idx} Scale Street, Woonona NSW 2517",
                last_contacted_at=iso_hours_ago(60 if idx % 4 == 0 else 6),
                price_drop_count=1 if idx % 5 == 0 else 0,
            )
            await seed_call(
                session,
                row_id=f"call-scale-{idx}-0",
                lead_id=lead_id,
                timestamp=iso_hours_ago(60 if idx % 4 == 0 else 6),
                connected=1,
                outcome="connected_interested",
                intent_signal=0.8 if idx % 4 == 0 else 0.4,
                booking_attempted=0 if idx % 4 == 0 else 1,
                next_step_detected=1 if idx % 6 == 0 else 0,
                summary="Scale summary",
            )
            await seed_call(
                session,
                row_id=f"call-scale-{idx}-1",
                lead_id=lead_id,
                timestamp=iso_hours_ago(82),
                connected=0,
                outcome="no_answer",
                intent_signal=0.1,
                booking_attempted=0,
                next_step_detected=0,
                call_duration_seconds=0,
            )
        await session.commit()

        started = time.perf_counter()
        cards = await get_missed_deals(session, date_range="today", user_id="Shahid")
        elapsed = time.perf_counter() - started

    assert cards
    assert elapsed < 2.5


def test_migration_safety_adds_columns_once(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"migration_safety_{uuid.uuid4().hex}.db"
    sqlite = sqlite3.connect(test_db)
    sqlite.execute(
        """
        CREATE TABLE leads (
            id TEXT PRIMARY KEY,
            address TEXT,
            owner_name TEXT,
            status TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    sqlite.execute(
        """
        CREATE TABLE call_log (
            id TEXT PRIMARY KEY,
            lead_id TEXT,
            outcome TEXT,
            logged_at TEXT
        )
        """
    )
    sqlite.commit()
    sqlite.close()

    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(
        db_module,
        "_async_session_factory",
        sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False),
    )

    db_module.init_db()
    db_module.init_db()

    sqlite = sqlite3.connect(test_db)
    lead_columns = [row[1] for row in sqlite.execute("PRAGMA table_info(leads)").fetchall()]
    call_columns = [row[1] for row in sqlite.execute("PRAGMA table_info(call_log)").fetchall()]
    sqlite.close()
    asyncio.run(test_engine.dispose())

    assert lead_columns.count("last_contacted_at") == 1
    assert lead_columns.count("follow_up_due_at") == 1
    assert lead_columns.count("price_drop_count") == 1
    assert lead_columns.count("last_activity_type") == 1
    assert call_columns.count("timestamp") == 1
    assert call_columns.count("call_duration_seconds") == 1
    assert call_columns.count("connected") == 1
    assert call_columns.count("summary") == 1
    assert call_columns.count("intent_signal") == 1
    assert call_columns.count("booking_attempted") == 1
    assert call_columns.count("next_step_detected") == 1
    assert call_columns.count("objection_tags") == 1
    if test_db.exists():
        test_db.unlink()
