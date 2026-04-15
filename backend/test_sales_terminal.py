import asyncio
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
from api.routes import growth, signals, waitlist
from core.config import API_KEY


SYDNEY_TZ = ZoneInfo("Australia/Sydney")

app = FastAPI()
app.include_router(signals.router)
app.include_router(growth.router)
app.include_router(waitlist.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine") / f"sales_terminal_{uuid.uuid4().hex}.db"
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


async def seed_terminal_data(session: AsyncSession):
    now = datetime.now(SYDNEY_TZ).replace(microsecond=0)
    lead_rows = [
        {
            "id": "lead-stale",
            "address": "1 Terminal Street, Woonona NSW 2517",
            "suburb": "Woonona",
            "postcode": "2517",
            "owner_name": "Lead Stale",
            "status": "contacted",
            "signal_status": "LIVE",
            "days_on_market": 74,
            "price_drop_count": 2,
            "heat_score": 88,
            "call_today_score": 84,
            "est_value": 1125000,
            "last_contacted_at": (now - timedelta(hours=96)).isoformat(),
            "created_at": (now - timedelta(days=8)).isoformat(),
            "updated_at": now.isoformat(),
            "id4me_enriched": 1,
            "last_activity_type": "call",
            "record_type": "property_record",
            "preferred_contact_method": "", "confidence_score": 70,
        },
        {
            "id": "lead-drop",
            "address": "2 Delta Avenue, Woonona NSW 2517",
            "suburb": "Woonona",
            "postcode": "2517",
            "owner_name": "Lead Drop",
            "status": "qualified",
            "signal_status": "DELTA",
            "days_on_market": 38,
            "price_drop_count": 1,
            "heat_score": 73,
            "call_today_score": 41,
            "est_value": 925000,
            "last_contacted_at": (now - timedelta(hours=30)).isoformat(),
            "created_at": (now - timedelta(days=5)).isoformat(),
            "updated_at": now.isoformat(),
            "id4me_enriched": 1,
            "last_activity_type": "follow_up",
            "record_type": "property_record",
            "preferred_contact_method": "", "confidence_score": 70,
        },
        {
            "id": "lead-zone",
            "address": "3 Activity Parade, Woonona NSW 2517",
            "suburb": "Woonona",
            "postcode": "2517",
            "owner_name": "Lead Zone",
            "status": "captured",
            "signal_status": "WITHDRAWN",
            "days_on_market": 29,
            "price_drop_count": 0,
            "heat_score": 69,
            "call_today_score": 35,
            "est_value": 875000,
            "last_contacted_at": "",
            "created_at": (now - timedelta(days=3)).isoformat(),
            "updated_at": now.isoformat(),
            "id4me_enriched": 0,
            "last_activity_type": "",
            "record_type": "property_record",
            "preferred_contact_method": "", "confidence_score": 70,
        },
    ]

    for row in lead_rows:
        await session.execute(
            text(
                """
                INSERT INTO leads (
                    id, address, suburb, postcode, owner_name, status, signal_status,
                    days_on_market, price_drop_count, heat_score, call_today_score,
                    est_value, last_contacted_at, created_at, updated_at, id4me_enriched, last_activity_type,
                    record_type, preferred_contact_method, confidence_score,
                    lat, lng, followup_frequency, conversion_score, compliance_score,
                    readiness_score, evidence_score, queue_bucket, lead_archetype,
                    contactability_status, owner_verified, contact_role, cadence_name,
                    cadence_step, next_action_type, next_action_channel, next_action_title,
                    next_action_reason, next_message_template, last_outcome, objection_reason,
                    preferred_channel, strike_zone, touches_14d, touches_30d, route_queue,
                    followup_status
                ) VALUES (
                    :id, :address, :suburb, :postcode, :owner_name, :status, :signal_status,
                    :days_on_market, :price_drop_count, :heat_score, :call_today_score,
                    :est_value, :last_contacted_at, :created_at, :updated_at, :id4me_enriched, :last_activity_type,
                    :record_type, :preferred_contact_method, :confidence_score,
                    0.0, 0.0, 'none', 0, 0,
                    0, 0, '', '',
                    '', 0, '', '',
                    0, '', '', '',
                    '', '', '', '',
                    '', '', 0, 0, '',
                    'active'
                )
                """
            ),
            row,
        )

    await session.execute(
        text(
            """
            INSERT INTO call_log (
                id, lead_id, lead_address, user_id, outcome, connected, timestamp, logged_at, logged_date,
                call_duration_seconds, duration_seconds, note, operator, provider, provider_call_id, direction,
                from_number, to_number, raw_payload, summary, transcript, intent_signal, booking_attempted,
                next_step_detected, objection_tags
            ) VALUES (
                'call-stale', 'lead-stale', '1 Terminal Street, Woonona NSW 2517', 'Shahid',
                'connected_interested', 1, :call_at, :call_at, :logged_date,
                420, 420, '', 'Shahid', 'manual', 'provider-call-stale', 'outbound',
                '', '', '{}', 'Owner engaged but timing objection blocked movement.', 'Need to think about changing agents.',
                0.79, 0, 1, '["bad timing"]'
            )
            """
        ),
        {
            "call_at": (now - timedelta(hours=96)).isoformat(),
            "logged_date": (now - timedelta(hours=96)).date().isoformat(),
        },
    )

    await session.execute(
        text(
            """
            INSERT INTO sold_events (
                id, address, suburb, postcode, sale_date, sale_price, lat, lng, source_name, match_reason, matched_lead_ids, created_at, updated_at
            ) VALUES (
                'sold-nearby', '9 Sold Lane, Woonona NSW 2517', 'Woonona', '2517', :sale_date, '1210000', 0, 0,
                'terminal_seed', 'same_suburb_recent_sale', '[]', :created_at, :updated_at
            )
            """
        ),
        {
            "sale_date": (now - timedelta(days=6)).date().isoformat(),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        },
    )

    await session.execute(
        text(
            """
            INSERT INTO distress_signals (
                id, source_key, signal_type, external_ref, title, owner_name, address, suburb, postcode,
                description, occurred_at, source_name, source_url, confidence_score, severity_score, status,
                lead_ids, inferred_owner_matches, inferred_property_matches, payload, created_at, updated_at
            ) VALUES (
                'distress-news', 'newsapi_distress', 'news', 'external-ref-1', 'Agency closure pressure rising',
                'Lead Stale', '1 Terminal Street, Woonona NSW 2517', 'Woonona', '2517',
                'Local market stress mentioned in recent reporting.', :occurred_at, 'NewsAPI', 'https://example.com/news/1',
                82, 67, 'captured', '["lead-stale"]', '[]', '[]', '{}', :created_at, :updated_at
            )
            """
        ),
        {
            "occurred_at": (now - timedelta(hours=18)).isoformat(),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        },
    )

    await session.commit()


@pytest.mark.asyncio
async def test_live_signals_return_ranked_sales_terminal_contract(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_terminal_data(session)

    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/signals/live?limit=20", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 5
    signals = payload["signals"]
    assert signals[0]["score"] >= signals[-1]["score"]
    signal_types = {item["type"] for item in signals}
    assert {
        "STALE_LISTING",
        "PRICE_DROP",
        "NEARBY_SOLD",
        "HIGH_ACTIVITY_ZONE",
        "OWNER_LIKELY_TO_CHURN",
        "NEWS_DISTRESS",
    }.issubset(signal_types)
    assert {
        "lead_id",
        "type",
        "headline",
        "detail",
        "suggested_action",
        "urgency_band",
        "score",
        "reason_detail",
        "detected_at",
    }.issubset(signals[0].keys())


@pytest.mark.asyncio
async def test_lead_signals_growth_digest_and_waitlist_minimal_capture(isolated_db):
    async with db_module._async_session_factory() as session:
        await seed_terminal_data(session)

    headers = {"X-API-KEY": API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        lead_response = await ac.get("/api/signals/lead/lead-stale", headers=headers)
        growth_response = await ac.get("/api/growth/digest", headers=headers)
        waitlist_response = await ac.post("/api/waitlist", json={"email": "operator@example.com"})

    assert lead_response.status_code == 200
    lead_payload = lead_response.json()
    assert lead_payload["lead_id"] == "lead-stale"
    assert lead_payload["signals"]
    assert all(signal["lead_id"] == "lead-stale" for signal in lead_payload["signals"])

    assert growth_response.status_code == 200
    growth_payload = growth_response.json()
    assert {"brand", "x_posts", "reports", "share_cards"}.issubset(growth_payload.keys())
    assert len(growth_payload["x_posts"]) >= 2
    assert len(growth_payload["reports"]) >= 3

    assert waitlist_response.status_code == 200
    assert waitlist_response.json()["ok"] is True

    async with db_module._async_session_factory() as session:
        row = (
            await session.execute(
                text("SELECT name, email FROM propella_waitlist ORDER BY submitted_at DESC LIMIT 1")
            )
        ).mappings().first()
    assert row["email"] == "operator@example.com"
    assert row["name"]
