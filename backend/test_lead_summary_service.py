import asyncio
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

import core.config
import core.database as db_module
from api.routes import leads as leads_routes
from models.sql_models import Lead
from services import lead_summary_service
from services.lead_summary_service import build_lead_detail, build_lead_summary


app = FastAPI()
app.include_router(leads_routes.router)


@pytest.fixture
def isolated_db(monkeypatch):
    test_db = Path(r"D:\woonona-lead-machine\backend\test_dbs") / f"lead-summary-{uuid.uuid4().hex}.db"
    intelligence_db = Path(r"D:\woonona-lead-machine\backend\test_dbs") / f"lead-summary-{uuid.uuid4().hex}.intelligence.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db.as_posix()}")

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    @event.listens_for(test_engine.sync_engine, "connect")
    def _attach_intelligence(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"ATTACH DATABASE '{intelligence_db.as_posix()}' AS intelligence")
        finally:
            cursor.close()

    test_factory = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(db_module, "_async_session_factory", test_factory)

    async def override_get_session():
        async with test_factory() as session:
            yield session

    app.dependency_overrides[db_module.get_session] = override_get_session
    asyncio.run(_create_schema(test_engine))
    try:
        yield test_factory
    finally:
        app.dependency_overrides.pop(db_module.get_session, None)
        asyncio.run(test_engine.dispose())
        test_db.unlink(missing_ok=True)
        intelligence_db.unlink(missing_ok=True)


async def _create_schema(engine):
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def _seed_lead(session: AsyncSession) -> None:
    session.add(
        Lead(
            id="lead-summary-1",
            address="10 Example Street, Windsor NSW 2756",
            suburb="Windsor",
            postcode="2756",
            owner_name="Jane Example",
            status="qualified",
            call_today_score=88,
            heat_score=92,
            confidence_score=77,
            conversion_score=66,
            compliance_score=55,
            readiness_score=44,
            contact_emails=["jane@example.com"],
            contact_phones=["0411222333"],
            activity_log=[{"type": "call", "note": "Touched base"}],
            stage_note_history=[{"status": "qualified", "note": "Updated"}],
            summary_points=["High intent"],
            source_evidence=["Documented in lead sheet"],
            description_deep="Long notes that should stay out of summary rows.",
            exhaustive_summary="Even longer internal summary that should stay in detail.",
            nearby_sales=[{"address": "8 Example Street"}],
            sale_history=[{"date": "2016-02-03", "price": 875000}],
            listing_status_history=[{"status": "withdrawn"}],
            seller_intent_signals=[{"key": "absentee_owner"}],
            refinance_signals=[{"key": "equity_position"}],
        )
    )
    await session.commit()


def test_build_lead_summary_strips_heavy_fields():
    payload = build_lead_summary(
        {
            "id": "lead-1",
            "address": "10 Example Street, Windsor NSW 2756",
            "owner_name": "Jane Example",
            "status": "qualified",
            "activity_log": [{"type": "note"}],
            "stage_note_history": [{"type": "note"}],
            "source_evidence": ["one"],
            "summary_points": ["two"],
            "description_deep": "long text",
            "exhaustive_summary": "long text",
            "nearby_sales": [{"address": "8 Example Street"}],
            "sale_history": [{"date": "2016-02-03"}],
            "listing_status_history": [{"status": "withdrawn"}],
            "seller_intent_signals": [{"key": "absentee_owner"}],
            "refinance_signals": [{"key": "equity_position"}],
            "timeline": [{"kind": "activity"}],
        }
    )

    assert payload["id"] == "lead-1"
    assert payload["address"] == "10 Example Street, Windsor NSW 2756"
    assert payload["status"] == "qualified"
    assert "activity_log" not in payload
    assert "stage_note_history" not in payload
    assert "source_evidence" not in payload
    assert "summary_points" not in payload
    assert "description_deep" not in payload
    assert "exhaustive_summary" not in payload
    assert "nearby_sales" not in payload
    assert "sale_history" not in payload
    assert "listing_status_history" not in payload
    assert "seller_intent_signals" not in payload
    assert "refinance_signals" not in payload
    assert "timeline" not in payload


def test_build_lead_detail_keeps_heavy_fields():
    payload = build_lead_detail(
        {
            "id": "lead-1",
            "address": "10 Example Street, Windsor NSW 2756",
            "activity_log": [{"type": "note"}],
            "source_evidence": ["one"],
            "timeline": [{"kind": "activity"}],
        }
    )

    assert payload["id"] == "lead-1"
    assert payload["activity_log"] == [{"type": "note"}]
    assert payload["source_evidence"] == ["one"]
    assert payload["timeline"] == [{"kind": "activity"}]


@pytest.mark.asyncio
async def test_summary_and_detail_endpoints_use_separate_payloads(isolated_db, monkeypatch):
    async with isolated_db() as session:
        await _seed_lead(session)

    async def _timeline_stub(*args, **kwargs):
        return [{"kind": "activity", "detail": "Called owner"}]

    monkeypatch.setattr(lead_summary_service, "get_lead_timeline", _timeline_stub)

    headers = {"X-API-KEY": core.config.API_KEY}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        summary_response = await ac.get("/api/leads/summary", headers=headers)
        existing_response = await ac.get("/api/leads/lead-summary-1", headers=headers)
        detail_response = await ac.get("/api/leads/lead-summary-1/detail", headers=headers)

    assert summary_response.status_code == 200
    summary_body = summary_response.json()
    assert summary_body["total"] == 1
    summary_row = summary_body["leads"][0]
    assert summary_row["id"] == "lead-summary-1"
    assert "activity_log" not in summary_row
    assert "source_evidence" not in summary_row
    assert "timeline" not in summary_row

    assert existing_response.status_code == 200
    existing_body = existing_response.json()
    assert existing_body["id"] == "lead-summary-1"

    assert detail_response.status_code == 200
    detail_body = detail_response.json()
    assert detail_body["id"] == "lead-summary-1"
    assert detail_body["activity_log"] == [{"type": "call", "note": "Touched base"}]
    assert detail_body["timeline"] == [{"kind": "activity", "detail": "Called owner"}]
