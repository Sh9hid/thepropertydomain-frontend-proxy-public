import asyncio
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config
import core.database as db_module
from services import auto_ticket_watcher, domain_withdrawn, reaxml_ingestor

pytestmark = pytest.mark.reliability


@pytest.fixture
def isolated_db(monkeypatch):
    Path("D:/woonona-lead-machine/backend/test_dbs").mkdir(parents=True, exist_ok=True)
    test_db = Path("D:/woonona-lead-machine/backend/test_dbs") / f"source-health-{uuid.uuid4().hex}.db"
    monkeypatch.setattr(core.config, "DB_PATH", str(test_db))
    monkeypatch.setattr(core.config, "DATABASE_URL", f"sqlite+aiosqlite:///{test_db}")

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
    for _ in range(10):
        try:
            test_db.unlink(missing_ok=True)
            break
        except PermissionError:
            asyncio.run(asyncio.sleep(0.05))


def _fetch_source_row(db_path: Path | str, source_key: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM source_health WHERE source_key = ?", (source_key,)).fetchone()
    conn.close()
    return dict(row) if row else None


@pytest.mark.asyncio
async def test_repeated_403_blocks_domain_source_and_skips_during_cooldown(isolated_db, monkeypatch):
    async def fake_get_token():
        return "token"

    call_count = {"post": 0}

    class FakeResponse:
        status_code = 403

        def json(self):
            return []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, *args, **kwargs):
            call_count["post"] += 1
            return FakeResponse()

    monkeypatch.setattr(domain_withdrawn, "_get_token", fake_get_token)
    monkeypatch.setattr(domain_withdrawn.httpx, "AsyncClient", FakeClient)

    for _ in range(3):
        assert await domain_withdrawn.fetch_withdrawn_listings(["Windsor"]) == []

    row = _fetch_source_row(isolated_db, "domain_withdrawn")
    assert row is not None
    assert row["status"] == "blocked"
    assert row["last_error_code"] == "403"
    assert row["consecutive_failures"] == 3
    assert row["blocked_until"]

    assert await domain_withdrawn.fetch_withdrawn_listings(["Windsor"]) == []
    assert call_count["post"] == 3


@pytest.mark.asyncio
async def test_reaxml_404_marks_source_misconfigured_and_skips_future_runs(isolated_db, monkeypatch):
    call_count = {"get": 0}

    class FakeResponse:
        status_code = 404
        headers = {}
        content = b""

        def raise_for_status(self):
            raise reaxml_ingestor.httpx.HTTPStatusError(
                "404",
                request=reaxml_ingestor.httpx.Request("GET", "https://example.com/feed.xml"),
                response=reaxml_ingestor.httpx.Response(404),
            )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            call_count["get"] += 1
            return FakeResponse()

    monkeypatch.setattr(reaxml_ingestor.httpx, "AsyncClient", FakeClient)

    ingestor = reaxml_ingestor.REAXMLIngestor()
    ingestor._all_feeds = [{"name": "Broken Feed", "reaxml": "https://example.com/feed.xml"}]

    assert await ingestor.poll_all_feeds() == []

    row = _fetch_source_row(isolated_db, "reaxml:Broken Feed")
    assert row is not None
    assert row["status"] == "misconfigured"
    assert row["last_error_code"] == "404"

    assert await ingestor.poll_all_feeds() == []
    assert call_count["get"] == 1


@pytest.mark.asyncio
async def test_reaxml_worker_continues_when_one_source_fails(isolated_db, monkeypatch):
    call_urls: list[str] = []

    class FakeResponse:
        def __init__(self, url: str):
            self.url = url
            self.headers = {}
            if "broken" in url:
                self.status_code = 404
                self.content = b""
            else:
                self.status_code = 200
                self.content = b"<root><residential status='withdrawn' uniqueID='abc'><address><street>1 Good St</street><suburb>Windsor</suburb></address></residential></root>"

        def raise_for_status(self):
            if self.status_code >= 400:
                raise reaxml_ingestor.httpx.HTTPStatusError(
                    str(self.status_code),
                    request=reaxml_ingestor.httpx.Request("GET", self.url),
                    response=reaxml_ingestor.httpx.Response(self.status_code),
                )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, *args, **kwargs):
            call_urls.append(url)
            return FakeResponse(url)

    monkeypatch.setattr(reaxml_ingestor.httpx, "AsyncClient", FakeClient)

    ingestor = reaxml_ingestor.REAXMLIngestor()
    ingestor._all_feeds = [
        {"name": "Broken Feed", "reaxml": "https://example.com/broken.xml"},
        {"name": "Good Feed", "reaxml": "https://example.com/good.xml"},
    ]

    withdrawn = await ingestor.poll_all_feeds()

    assert len(withdrawn) == 1
    assert withdrawn[0]["agency_name"] == "Good Feed"
    assert call_urls == ["https://example.com/broken.xml", "https://example.com/good.xml"]


@pytest.mark.asyncio
async def test_auto_watcher_uses_sqlite_json_extract_for_work_type(isolated_db, monkeypatch):
    conn = sqlite3.connect(isolated_db)
    conn.execute("DROP TABLE IF EXISTS orch_events")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orch_events (
            id TEXT PRIMARY KEY,
            event_type TEXT,
            provider TEXT,
            ts TEXT,
            data TEXT
        )
        """
    )
    conn.execute("DELETE FROM orch_events")
    now = datetime.now(timezone.utc).isoformat()
    for idx in range(3):
        conn.execute(
            "INSERT INTO orch_events (id, event_type, provider, ts, data) VALUES (?, 'task_failed', 'provider-x', ?, ?)",
            (f"evt-{idx}", now, '{"work_type":"debugging"}'),
        )
    conn.commit()
    conn.close()

    created = []

    async def fake_ticket_exists_for_pattern(*args, **kwargs):
        return False

    async def fake_create_ticket(session, **kwargs):
        created.append(kwargs)

    monkeypatch.setattr(auto_ticket_watcher, "ticket_exists_for_pattern", fake_ticket_exists_for_pattern)
    monkeypatch.setattr(auto_ticket_watcher, "create_ticket", fake_create_ticket)

    async with db_module._async_session_factory() as session:
        raised: list[str] = []
        await auto_ticket_watcher._check_repeated_orch_failures(session, raised)

    assert len(created) == 1
    assert created[0]["evidence_json"]["work_type"] == "debugging"
    assert created[0]["evidence_json"]["failure_count"] == 3
