import asyncio
import json
import sqlite3
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module
from api.routes import rea
from services import rea_service


app = FastAPI()
app.include_router(rea.router)

TEST_ROOT = Path("D:/woonona-lead-machine/backend/test_dbs")


@pytest.fixture
def isolated_db(monkeypatch):
    TEST_ROOT.mkdir(parents=True, exist_ok=True)
    test_db = TEST_ROOT / f"rea-phase1-{uuid.uuid4().hex}.db"
    db_path = str(test_db)
    database_url = f"sqlite+aiosqlite:///{test_db}"
    monkeypatch.setattr(core.config, "DB_PATH", db_path)
    monkeypatch.setattr(core.config, "DATABASE_URL", database_url)
    monkeypatch.setattr(core.config, "REA_CLIENT_ID", "rea-client")
    monkeypatch.setattr(core.config, "REA_CLIENT_SECRET", "rea-secret")
    monkeypatch.setattr(core.config, "REA_AGENCY_ID", "LSOAKV")
    monkeypatch.setattr(rea_service, "REA_CLIENT_ID", "rea-client")
    monkeypatch.setattr(rea_service, "REA_CLIENT_SECRET", "rea-secret")
    monkeypatch.setattr(rea_service, "REA_AGENCY_ID", "LSOAKV")

    test_engine = create_async_engine(core.config.DATABASE_URL, echo=False, future=True)
    monkeypatch.setattr(db_module, "async_engine", test_engine)
    monkeypatch.setattr(
        db_module,
        "_async_session_factory",
        sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False),
    )
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE leads (
            id TEXT PRIMARY KEY,
            address TEXT,
            suburb TEXT,
            postcode TEXT,
            owner_name TEXT,
            trigger_type TEXT,
            record_type TEXT,
            heat_score INTEGER,
            confidence_score INTEGER,
            contact_emails TEXT,
            contact_phones TEXT,
            created_at TEXT,
            updated_at TEXT,
            status TEXT,
            conversion_score INTEGER,
            compliance_score INTEGER,
            readiness_score INTEGER,
            call_today_score INTEGER,
            evidence_score INTEGER,
            queue_bucket TEXT,
            lead_archetype TEXT,
            contactability_status TEXT,
            owner_verified INTEGER,
            contact_role TEXT,
            cadence_name TEXT,
            cadence_step INTEGER,
            next_action_type TEXT,
            next_action_channel TEXT,
            next_action_title TEXT,
            next_action_reason TEXT,
            next_message_template TEXT,
            last_outcome TEXT,
            last_activity_type TEXT,
            objection_reason TEXT,
            preferred_channel TEXT,
            strike_zone TEXT,
            touches_14d INTEGER,
            touches_30d INTEGER,
            route_queue TEXT,
            listing_headline TEXT,
            property_images TEXT,
            price_guide_low INTEGER,
            price_guide_high INTEGER,
            property_type TEXT,
            bedrooms INTEGER,
            bathrooms INTEGER,
            car_spaces INTEGER,
            land_size_sqm REAL,
            rea_upload_id TEXT,
            rea_upload_status TEXT,
            rea_listing_id TEXT,
            rea_last_upload_response TEXT,
            rea_last_upload_report TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE rea_api_logs (
            id TEXT PRIMARY KEY,
            lead_id TEXT,
            rea_upload_id TEXT,
            rea_listing_id TEXT,
            action TEXT NOT NULL,
            request_method TEXT,
            request_path TEXT,
            request_payload TEXT,
            response_status_code INTEGER,
            response_body TEXT,
            ok INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()
    yield test_db
    asyncio.run(test_engine.dispose())
    try:
        test_db.unlink(missing_ok=True)
    except PermissionError:
        pass


def _seed_lead(db_path: str, lead_id: str = "lead-rea-1") -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO leads (
            id, address, suburb, postcode, owner_name, trigger_type, record_type,
            heat_score, confidence_score, contact_emails, contact_phones,
            created_at, updated_at, status, conversion_score, compliance_score,
            readiness_score, call_today_score, evidence_score, queue_bucket,
            lead_archetype, contactability_status, owner_verified, contact_role,
            cadence_name, cadence_step, next_action_type, next_action_channel,
            next_action_title, next_action_reason, next_message_template, last_outcome,
            last_activity_type, objection_reason, preferred_channel, strike_zone,
            touches_14d, touches_30d, route_queue, listing_headline,
            property_images, price_guide_low, price_guide_high, property_type,
            bedrooms, bathrooms, car_spaces, land_size_sqm
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            lead_id,
            "10 Example Street",
            "Windsor",
            "2756",
            "Example Owner",
            "manual",
            "listing",
            50,
            80,
            "[]",
            "[]",
            "2026-04-02T10:00:00Z",
            "2026-04-02T10:00:00Z",
            "captured",
            0,
            0,
            0,
            0,
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
            "Prime Windsor address",
            json.dumps(["https://img.example.com/1.jpg"]),
            1000000,
            1100000,
            "House",
            4,
            2,
            2,
            550,
        ),
    )
    conn.commit()
    conn.close()


def _fetch_lead_state(db_path: str, lead_id: str = "lead-rea-1") -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT rea_upload_id, rea_upload_status, rea_listing_id,
               rea_last_upload_response, rea_last_upload_report
        FROM leads WHERE id = ?
        """,
        (lead_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


def _fetch_rea_logs(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT action, request_method, request_path, response_status_code, ok
        FROM rea_api_logs ORDER BY created_at ASC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def test_build_lot_cotality_accuracy_report_marks_static_claims_unverified_without_result():
    report = rea._build_lot_cotality_accuracy_report(
        {
            "address": "48 Futurity Street",
            "suburb": "Box Hill",
            "postcode": "2765",
            "land_size_sqm": 300,
            "frontage": "10",
        },
        None,
    )

    assert report["result_found"] is False
    assert report["is_100_percent_accurate"] is False
    assert report["matched_address"] is None
    assert report["hardcoded_location_source"] == "backend/assets/suburb_profiles.json"

    core_facts = {item["field"]: item for item in report["core_facts"]}
    assert core_facts["address"]["status"] == "unverified"
    assert core_facts["land_size_sqm"]["status"] == "unverified"
    assert core_facts["frontage"]["status"] == "not_available_in_cotality_workflow"

    location_claims = {item["claim"]: item for item in report["hardcoded_location_claims"]}
    assert location_claims["Box Hill Public School catchment"]["status"] == "unverified"
    assert location_claims["Zoned for Rouse Hill High School"]["status"] == "unverified"
    assert location_claims["Approx. 8 minutes to Rouse Hill Metro"]["status"] == "unverified"
    assert "No completed Cotality enrichment result exists" in report["summary"]


class _FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text or (json.dumps(payload) if payload is not None else "")

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class _FakeAsyncClient:
    calls: list[dict] = []
    queued: list[_FakeResponse] = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    @classmethod
    def queue(cls, *responses: _FakeResponse):
        cls.calls = []
        cls.queued = list(responses)

    async def post(self, url, **kwargs):
        return self._record("POST", url, kwargs)

    async def get(self, url, **kwargs):
        return self._record("GET", url, kwargs)

    async def put(self, url, **kwargs):
        return self._record("PUT", url, kwargs)

    def _record(self, method: str, url: str, kwargs: dict):
        self.__class__.calls.append({"method": method, "url": url, "kwargs": kwargs})
        if not self.__class__.queued:
            raise AssertionError(f"no queued fake response for {method} {url}")
        return self.__class__.queued.pop(0)


@pytest.mark.asyncio
async def test_publish_route_stores_upload_state_without_overwriting_listing_id(isolated_db, monkeypatch):
    _seed_lead(core.config.DB_PATH)
    monkeypatch.setattr(rea, "REA_PUBLISH_ENABLED", True)

    async def fake_publish(lead, agency_id="", session=None):
        return {
            "ok": True,
            "upload_id": "upload-123",
            "status": "IN_PROGRESS",
            "response": {"uploadId": "upload-123", "progress": "IN_PROGRESS"},
        }

    monkeypatch.setattr(rea, "publish_listing", fake_publish)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/rea/listings/lead-rea-1/publish", headers={"X-API-KEY": core.config.API_KEY})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "submitted"
    assert body["rea_upload_id"] == "upload-123"
    state = _fetch_lead_state(core.config.DB_PATH)
    assert state["rea_upload_id"] == "upload-123"
    assert state["rea_upload_status"] == "IN_PROGRESS"
    assert state["rea_listing_id"] in (None, "")
    assert json.loads(state["rea_last_upload_response"])["uploadId"] == "upload-123"


@pytest.mark.asyncio
async def test_upload_status_route_updates_listing_id_from_upload_report(isolated_db, monkeypatch):
    _seed_lead(core.config.DB_PATH)
    conn = sqlite3.connect(core.config.DB_PATH)
    conn.execute(
        "UPDATE leads SET rea_upload_id = ?, rea_upload_status = ? WHERE id = ?",
        ("upload-123", "IN_PROGRESS", "lead-rea-1"),
    )
    conn.commit()
    conn.close()

    async def fake_report(upload_id, session=None, lead_id=None):
        assert upload_id == "upload-123"
        return {
            "ok": True,
            "data": {
                "uploadId": "upload-123",
                "progress": "COMPLETED",
                "result": "NEW",
                "listingId": "131603746",
                "issues": {"warnings": [], "errors": []},
            },
        }

    monkeypatch.setattr(rea, "get_upload_report", fake_report)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/rea/upload/upload-123/status", headers={"X-API-KEY": core.config.API_KEY})

    assert response.status_code == 200
    payload = response.json()
    assert payload["progress"] == "COMPLETED"
    assert payload["listingId"] == "131603746"
    state = _fetch_lead_state(core.config.DB_PATH)
    assert state["rea_upload_status"] == "COMPLETED"
    assert state["rea_listing_id"] == "131603746"
    assert json.loads(state["rea_last_upload_report"])["result"] == "NEW"


@pytest.mark.asyncio
async def test_verification_endpoints_surface_service_data(isolated_db, monkeypatch):
    async def fake_integration(session=None):
        return {"connected": True, "scopes": ["lead:v1:enquiries", "campaign:v1:listing-performance"]}

    async def fake_export(session=None):
        return [{"rea_listing_id": "111", "address": "10 Example Street"}]

    async def fake_enquiries(since=None, session=None):
        return [{"id": "enq-1", "listingId": "111"}]

    async def fake_performance(listing_id, session=None):
        return {"listingId": listing_id, "metrics": [{"name": "pageView", "value": 15}]}

    monkeypatch.setattr(rea, "get_integration_status", fake_integration)
    monkeypatch.setattr(rea, "export_listings", fake_export)
    monkeypatch.setattr(rea, "get_enquiries", fake_enquiries)
    monkeypatch.setattr(rea, "get_listing_performance", fake_performance)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        integration = await ac.get("/api/rea/integration-status", headers={"X-API-KEY": core.config.API_KEY})
        export = await ac.get("/api/rea/listings/export", headers={"X-API-KEY": core.config.API_KEY})
        enquiries = await ac.get("/api/rea/enquiries", headers={"X-API-KEY": core.config.API_KEY})
        performance = await ac.get("/api/rea/performance/111", headers={"X-API-KEY": core.config.API_KEY})

    assert integration.status_code == 200
    assert integration.json()["connected"] is True
    assert export.json()["count"] == 1
    assert enquiries.json()["count"] == 1
    assert performance.json()["listingId"] == "111"


@pytest.mark.asyncio
async def test_service_calls_documented_paths_and_logs_responses(isolated_db, monkeypatch):
    monkeypatch.setattr(rea_service.httpx, "AsyncClient", _FakeAsyncClient)
    # Response queue:
    # 1. Token (consumed by check_credentials in get_integration_status; cached for all subsequent calls)
    # 2. Upload (consumed by publish_listing)
    # 3. Upload report (consumed by get_upload_report)
    # 4. Export (consumed by export_listings)
    # 5. Enquiries (consumed by get_enquiries → get_seller_leads)
    # 6. Performance (consumed by get_listing_performance)
    _FakeAsyncClient.queue(
        _FakeResponse(200, {"access_token": "token-1", "expires_in": 3600, "scope": "lead:v1:enquiries campaign:v1:listing-performance"}),
        _FakeResponse(200, {"uploadId": "upload-1", "progress": "IN_PROGRESS"}),
        _FakeResponse(200, {"uploadId": "upload-1", "progress": "COMPLETED", "result": "NEW", "listingId": "131603746"}),
        _FakeResponse(200, {"listings": [{"listingId": "131603746"}]}),
        _FakeResponse(200, [{"id": "enq-1", "listingId": "111"}]),
        _FakeResponse(200, {"listingId": "131603746", "metrics": [{"name": "pageView", "value": 15}]}),
    )

    async with db_module._async_session_factory() as session:
        integration = await rea_service.get_integration_status(session=session)
        publish = await rea_service.publish_listing(
            {
                "id": "lead-rea-1",
                "address": "10 Example Street",
                "suburb": "Windsor",
                "postcode": "2756",
                "property_images": json.dumps(["https://img.example.com/1.jpg"]),
                "listing_headline": "Prime Windsor address",
                "price_guide_low": 1000000,
                "price_guide_high": 1100000,
            },
            session=session,
            lead_id="lead-rea-1",
        )
        report = await rea_service.get_upload_report("upload-1", session=session, lead_id="lead-rea-1")
        export = await rea_service.export_listings(session=session)
        enquiries = await rea_service.get_enquiries(session=session)
        performance = await rea_service.get_listing_performance("131603746", session=session)

    assert integration["configured"] is True
    assert publish["upload_id"] == "upload-1"
    assert report["data"]["listingId"] == "131603746"
    # export_listings / get_enquiries may return data from DB fallback or API
    assert isinstance(export, list)
    assert isinstance(enquiries, list)
    assert performance.get("listingId") == "131603746" or performance.get("listing_id") == "131603746"

    urls = [call["url"] for call in _FakeAsyncClient.calls]
    assert "https://api.realestate.com.au/oauth/token" in urls[0]
    assert "https://api.realestate.com.au/listing/v1/upload" in urls[1]
    assert "https://api.realestate.com.au/listing/v1/upload/upload-1" in urls[2]

    logs = _fetch_rea_logs(core.config.DB_PATH)
    actions = [entry["action"] for entry in logs]
    assert "publish_listing" in actions


@pytest.mark.asyncio
async def test_lot127_sample_uses_staging_csv_row_and_infers_box_hill(isolated_db, monkeypatch, tmp_path):
    csv_path = tmp_path / "bathla_reaxml_staging.csv"
    csv_path.write_text(
        "\n".join(
            [
                "address,all_lot_numbers_same_size_in_project,estimated_completion,frontage,group_key,land_area,lot_number,lot_type,price,project_name,project_slug,project_status,property_type_summary,ready_for_reaxml,same_size_count_in_project,,stage,status,suburb",
                "48 Futurity Street,127,,10,124_old_pitt_town_road_box_hill_land|300,300,127,,819990,124 old pitt town road box hill land,124_old_pitt_town_road_box_hill_land,,,YES,1,,,Available,",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(rea, "_CSV_PATH", csv_path)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/rea/studio/sample/lot127", headers={"X-API-KEY": core.config.API_KEY})

    assert response.status_code == 200
    payload = response.json()
    assert payload["inputs"]["address"] == "48 Futurity Street"
    assert payload["inputs"]["suburb"] == "Box Hill"
    assert payload["inputs"]["postcode"] == "2765"
    assert payload["inputs"]["frontage"] == "10"
    assert payload["lifestyle"]["headline"] == "Build-Ready 300sqm in Box Hill"
    assert "Nitin Puri from Laing+Simmons Oakville | Windsor is pleased to present Lot 127" in payload["lifestyle"]["body"]
    assert "— a 300 sqm registered block in the growth corridor of Box Hill." in payload["lifestyle"]["body"]
    assert "If you've been searching for the right block to build your first home, this one ticks every box." in payload["lifestyle"]["body"]
    assert "Land size: 300 sqm" in payload["lifestyle"]["body"]
    assert "Frontage: 10m" in payload["lifestyle"]["body"]
    assert "Registration: Registered and ready" in payload["lifestyle"]["body"]
    assert "Location:" in payload["lifestyle"]["body"]
    assert "Price:" not in payload["lifestyle"]["body"]
    assert "$819,990" not in payload["lifestyle"]["body"]
    assert "Build with your own team" not in payload["lifestyle"]["body"]
    assert "0430 042 041" in payload["lifestyle"]["body"]
    assert "0430 042 041" in payload["investor"]["body"]
    assert "04 85 85 7881" not in payload["lifestyle"]["body"]
    assert "04 85 85 7881" not in payload["investor"]["body"]
    assert payload["cotality_backed"] is False
    assert payload["cotality_verification"]["result_found"] is False
    assert payload["cotality_verification"]["is_100_percent_accurate"] is False
    assert payload["cotality_verification"]["hardcoded_location_source"] == "backend/assets/suburb_profiles.json"
    assert any(
        item["claim"] == "Box Hill Public School catchment" and item["status"] == "unverified"
        for item in payload["cotality_verification"]["hardcoded_location_claims"]
    )
