import csv
import sqlite3
import asyncio
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import core.config as config
from api.routes.leads import _derive_import_source_tags, _normalize_source_tag
from services import door_knock_sync_service as sync_service


@pytest.fixture
def isolated_session(tmp_path: Path, monkeypatch):
    test_db = tmp_path / "door_knock_sync.db"
    db_url = f"sqlite+aiosqlite:///{test_db}"
    monkeypatch.setattr(config, "DB_PATH", str(test_db))
    monkeypatch.setattr(config, "DATABASE_URL", db_url)
    test_engine = create_async_engine(db_url, echo=False, future=True)
    factory = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    conn = sqlite3.connect(test_db)
    conn.execute(
        """
        CREATE TABLE leads (
            id TEXT PRIMARY KEY,
            address TEXT,
            suburb TEXT,
            postcode TEXT,
            owner_name TEXT,
            owner_type TEXT,
            trigger_type TEXT,
            record_type TEXT,
            status TEXT,
            queue_bucket TEXT,
            source_tags TEXT,
            notes TEXT,
            stage_note TEXT,
            contact_phones TEXT,
            contact_emails TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO leads (
            id, address, suburb, postcode, owner_name, trigger_type, record_type, status,
            queue_bucket, source_tags, notes, contact_phones, contact_emails, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """,
        (
            "lead-door-1",
            "10 Example St",
            "Oakville",
            "2765",
            "Owner One",
            "Door Knock",
            "manual_entry",
            "captured",
            "door_knock",
            '["door_knock","land"]',
            "Initial note",
            '["0412345678"]',
            '["owner@example.com"]',
        ),
    )
    conn.commit()
    conn.close()

    try:
        yield factory, tmp_path
    finally:
        asyncio.run(test_engine.dispose())


def test_source_normalization_defaults_to_rp_data():
    assert _normalize_source_tag("") == "rp_data"
    assert _normalize_source_tag("  ") == "rp_data"
    assert _normalize_source_tag("doorknock") == "door_knock"


def test_derive_import_tags_includes_builder_and_land():
    tags = _derive_import_source_tags("door_knock", "builder", "builder/land", "land owner - builder contact")
    assert "door_knock" in tags
    assert "builder" in tags
    assert "land" in tags


@pytest.mark.asyncio
async def test_door_knock_sheet_sync_exports_and_imports_updates(isolated_session, monkeypatch):
    factory, tmp_path = isolated_session
    sync_file = tmp_path / "door_knock_sync.csv"
    state_file = tmp_path / "door_knock_sync_state.json"
    monkeypatch.setattr(sync_service, "DOOR_KNOCK_SYNC_FILE", str(sync_file))
    monkeypatch.setattr(sync_service, "STATE_PATH", state_file)

    async with factory() as session:
        first = await sync_service.run_door_knock_sheet_sync_once(session)
    assert first["exported"] >= 1
    assert sync_file.exists()

    rows = []
    with sync_file.open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert rows
    rows[0]["owner_name"] = "Owner Updated"
    rows[0]["notes"] = "Updated from sheet"
    rows[0]["tags"] = "door_knock,builder,land"
    with sync_file.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=sync_service.SYNC_HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    async with factory() as session:
        second = await sync_service.run_door_knock_sheet_sync_once(session)
        assert second["imported"] >= 1
        verify = (
            await session.execute(
                text(
                    "SELECT owner_name, notes, source_tags, queue_bucket FROM leads WHERE id = :id"
                ),
                {"id": "lead-door-1"},
            )
        ).mappings().first()
    assert verify is not None
    assert verify["owner_name"] == "Owner Updated"
    assert verify["notes"] == "Updated from sheet"
    assert "door_knock" in str(verify["source_tags"])
    assert verify["queue_bucket"] == "door_knock"
