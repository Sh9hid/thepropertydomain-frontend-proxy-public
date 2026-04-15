import pytest

import core.config
from services.delta_engine import DeltaEngine


class _FakeDB:
    def __init__(self):
        self.calls = []
        self.committed = False

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), params or {}))

    async def commit(self):
        self.committed = True


@pytest.mark.asyncio
async def test_ingest_withdrawn_batch_uses_resolved_schema_prefix(monkeypatch):
    monkeypatch.setattr(core.config, "USE_POSTGRES", False)
    db = _FakeDB()
    engine = DeltaEngine()

    await engine.ingest_withdrawn_batch(
        db,
        [
            {
                "address": "1 Test Street",
                "suburb": "Windsor",
                "listing_id": "abc123",
                "agency_name": "Test Agency",
                "source": "reaxml",
            }
        ],
    )

    assert db.committed is True
    assert len(db.calls) == 2
    assert "INSERT INTO property" in db.calls[0][0]
    assert "INSERT INTO event" in db.calls[1][0]
    assert "::jsonb" not in db.calls[1][0]
