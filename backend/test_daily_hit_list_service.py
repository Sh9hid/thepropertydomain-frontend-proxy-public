import asyncio
import sqlite3
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from api.routes import leads

import core.config
import core.database as db_module


app = FastAPI()
app.include_router(leads.router)

TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"daily-hit-list-{uuid.uuid4().hex}.db"
    db_path = str(test_db)
    database_url = f"sqlite+aiosqlite:///{test_db}"
    monkeypatch.setattr(core.config, "DB_PATH", db_path)
    monkeypatch.setattr(core.config, "DATABASE_URL", database_url)

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
    try:
        test_db.unlink(missing_ok=True)
    except PermissionError:
        pass


def insert_lead(
    conn: sqlite3.Connection,
    *,
    lead_id: str,
    address: str,
    suburb: str,
    owner_name: str,
    trigger_type: str,
    record_type: str,
    heat_score: int,
    confidence_score: int,
    contact_phones: str = "[]",
    call_today_score: int = 0,
    sale_date: str | None = None,
    settlement_date: str | None = None,
    signal_status: str = "",
):
    columns = [
        "id", "address", "suburb", "postcode", "owner_name", "trigger_type", "record_type",
        "heat_score", "confidence_score", "contact_emails", "contact_phones", "lat", "lng", "est_value",
        "created_at", "updated_at", "status", "conversion_score", "compliance_score", "readiness_score",
        "call_today_score", "evidence_score", "queue_bucket", "lead_archetype", "contactability_status",
        "owner_verified", "contact_role", "cadence_name", "cadence_step", "next_action_type", "next_action_channel",
        "next_action_title", "next_action_reason", "next_message_template", "last_outcome", "last_activity_type",
        "objection_reason", "preferred_channel", "strike_zone", "touches_14d", "touches_30d", "route_queue",
        "days_on_market", "price_drop_count", "relisted", "sale_date", "settlement_date",
        "preferred_contact_method", "followup_frequency", "followup_status", "signal_status",
    ]
    values = [
        lead_id,
        address,
        suburb,
        "2756",
        owner_name,
        trigger_type,
        record_type,
        heat_score,
        confidence_score,
        '["owner@example.com"]',
        contact_phones,
        -34.3430,
        150.9130,
        950000,
        "2026-03-26T08:00:00+11:00",
        "2026-03-26T08:00:00+11:00",
        "captured",
        0,
        0,
        0,
        call_today_score,
        0,
        "",
        "",
        "",
        0,
        "",
        "",
        0,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        0,
        0,
        "",
        0,
        0,
        0,
        sale_date,
        settlement_date,
        "",
        "none",
        "active",
        signal_status,
    ]
    assert len(columns) == len(values) == 51
    placeholders = ", ".join(["?"] * len(values))
    conn.execute(
        f"INSERT INTO leads ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(values),
    )


@pytest.mark.asyncio
async def test_api_leads_promotes_daily_hit_list_ranked_records_and_exposes_summary(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    insert_lead(
        conn,
        lead_id="rank-50",
        address="1 Mary Pl, Bligh Park NSW",
        suburb="Bligh Park",
        owner_name="Owner rank-50",
        trigger_type="marketing_list",
        record_type="marketing_contact",
        heat_score=10,
        confidence_score=60,
        contact_phones='["0403900844"]',
        call_today_score=10,
    )
    insert_lead(
        conn,
        lead_id="rank-40",
        address="1/1 Samuel St, Bligh Park NSW",
        suburb="Bligh Park",
        owner_name="Owner rank-40",
        trigger_type="marketing_list",
        record_type="marketing_contact",
        heat_score=90,
        confidence_score=60,
        contact_phones='["0434680667"]',
        call_today_score=90,
    )
    insert_lead(
        conn,
        lead_id="duplicate-should-not-rank",
        address="Unit 1 1 Samuel St",
        suburb="Bligh Park",
        owner_name="Owner duplicate",
        trigger_type="Marketing Report Import",
        record_type="marketing_contact",
        heat_score=95,
        confidence_score=60,
        contact_phones='["0434680667"]',
        call_today_score=95,
    )
    insert_lead(
        conn,
        lead_id="recent-sale-should-drop",
        address="1 Ann Pl, Bligh Park NSW",
        suburb="Bligh Park",
        owner_name="Owner recent",
        trigger_type="marketing_list",
        record_type="marketing_contact",
        heat_score=99,
        confidence_score=60,
        contact_phones='["0448984282"]',
        call_today_score=99,
    )
    insert_lead(
        conn,
        lead_id="sale-50",
        address="1 MARY PLACE, Bligh Park NSW 2756",
        suburb="Bligh Park",
        owner_name="Owner sale-50",
        trigger_type="cotality_import",
        record_type="property_report",
        heat_score=20,
        confidence_score=80,
        sale_date="01 Aug 2022",
        settlement_date="29 Aug 2022",
    )
    insert_lead(
        conn,
        lead_id="sale-40",
        address="1/1 SAMUEL STREET, Bligh Park NSW 2756",
        suburb="Bligh Park",
        owner_name="Owner sale-40",
        trigger_type="cotality_import",
        record_type="property_report",
        heat_score=20,
        confidence_score=80,
        sale_date="27 Mar 2017",
        settlement_date="08 May 2017",
    )
    insert_lead(
        conn,
        lead_id="sale-recent",
        address="1 ANN PLACE, Bligh Park NSW 2756",
        suburb="Bligh Park",
        owner_name="Owner sale-recent",
        trigger_type="cotality_import",
        record_type="property_report",
        heat_score=20,
        confidence_score=80,
        sale_date="03 Mar 2026",
        settlement_date="03 Mar 2026",
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/leads?limit=10", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert [lead["id"] for lead in body["leads"][:2]] == ["rank-50", "rank-40"]

    top = body["leads"][0]
    assert top["intent_score"] == 50
    assert top["daily_hit_list_name"] == "Daily_Hit_List_Ranked"
    assert top["daily_hit_list_rank"] == 1
    assert "2021-2022" in top["intent_summary"]

    second = body["leads"][1]
    assert second["intent_score"] == 40
    assert second["daily_hit_list_rank"] == 2
    assert "2016-2019" in second["intent_summary"]

    duplicate = next(lead for lead in body["leads"] if lead["id"] == "duplicate-should-not-rank")
    assert duplicate.get("intent_score", 0) == 0
    assert not duplicate.get("daily_hit_list_rank")

    recent = next(lead for lead in body["leads"] if lead["id"] == "recent-sale-should-drop")
    assert recent.get("intent_score", 0) == 0
    assert not recent.get("daily_hit_list_rank")


@pytest.mark.asyncio
async def test_api_leads_filters_withdrawn_server_side_and_exposes_non_ranked_intent_summary(isolated_db):
    conn = sqlite3.connect(core.config.DB_PATH)
    insert_lead(
        conn,
        lead_id="withdrawn-lead",
        address="99 Example St, Bligh Park NSW",
        suburb="Bligh Park",
        owner_name="Withdrawn Owner",
        trigger_type="domain_withdrawn",
        record_type="listing_signal",
        heat_score=70,
        confidence_score=85,
        signal_status="WITHDRAWN",
    )
    insert_lead(
        conn,
        lead_id="da-no-phone",
        address="12 Planner Ave, Bligh Park NSW",
        suburb="Bligh Park",
        owner_name="DA Owner",
        trigger_type="da_feed",
        record_type="planning_signal",
        heat_score=55,
        confidence_score=78,
    )
    insert_lead(
        conn,
        lead_id="plain-marketing",
        address="15 Caller Ct, Bligh Park NSW",
        suburb="Bligh Park",
        owner_name="Phone Owner",
        trigger_type="marketing_list",
        record_type="marketing_contact",
        heat_score=40,
        confidence_score=60,
        contact_phones='["0411222333"]',
    )
    conn.commit()
    conn.close()

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        withdrawn_response = await ac.get("/api/leads?limit=10&signal_status=WITHDRAWN", headers=headers)
        all_response = await ac.get("/api/leads?limit=10", headers=headers)

    assert withdrawn_response.status_code == 200
    withdrawn_body = withdrawn_response.json()
    withdrawn_ids = [lead["id"] for lead in withdrawn_body["leads"]]
    assert withdrawn_ids == ["withdrawn-lead"]

    assert all_response.status_code == 200
    all_body = all_response.json()
    da_lead = next(lead for lead in all_body["leads"] if lead["id"] == "da-no-phone")
    assert da_lead["intent_score"] == 0
    assert da_lead["intent_summary"]
    assert "No direct phone" in da_lead["intent_summary"]
    assert "DA" in da_lead["intent_summary"] or "development" in da_lead["intent_summary"].lower()
