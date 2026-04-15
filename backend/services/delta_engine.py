"""
Delta Engine — Set Theory Off-Market Confirmation
Formula: Withdrawn(t) ∩ NOT(Sold_PSI ∪ Sold_LRS) = Confirmed Off-Market

Runs daily at midnight Sydney time.
Broadcasts confirmed leads via WebSocket pulse.
"""

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime

from core.config import SYDNEY_TZ
from core.events import event_manager

logger = logging.getLogger(__name__)


class DeltaEngine:
    """
    Set theory verification: withdrawn minus known sales = genuine off-market.
    """

    async def run_daily_delta(self, db):
        """
        Full daily delta cycle. Fetches today's withdrawn, removes known sales,
        routes and broadcasts confirmed off-market leads.
        """
        from core.utils import now_iso
        from services.routing_engine import routing_engine

        logger.info("[Delta] Starting daily delta run")
        try:
            withdrawn = await self._get_withdrawn_today(db)
            sold_psi = await self._get_sold_set(db, source="psi")
            sold_lrs = await self._get_sold_set(db, source="lrs")

            sold_all = sold_psi | sold_lrs
            confirmed = [w for w in withdrawn if w["property_id"] not in sold_all]

            logger.info(
                f"[Delta] withdrawn={len(withdrawn)}, sold_known={len(sold_all)}, "
                f"confirmed_off_market={len(confirmed)}"
            )

            for lead in confirmed:
                try:
                    queue = await routing_engine.route(db, lead)
                    await self._update_route_queue(db, lead["property_id"], queue)
                    await event_manager.broadcast({
                        "type": "CONFIRMED_OFF_MARKET",
                        "data": {**lead, "route_queue": queue},
                    })
                except Exception as e:
                    logger.warning(f"[Delta] Lead routing error for {lead.get('property_id')}: {e}")

        except Exception as e:
            logger.error(f"[Delta] Daily delta error: {e}")

    async def ingest_withdrawn_batch(self, db, withdrawn_listings: list[dict]):
        """
        Ingest a batch of withdrawn listings (from REAXML worker) into intelligence.event.
        Called by reaxml_ingestor after each poll.
        """
        from sqlalchemy import text
        from core.utils import now_iso

        now = now_iso()
        for listing in withdrawn_listings:
            address = listing.get("address", "").strip()
            if not address:
                continue
            prop_id = hashlib.md5(address.encode()).hexdigest()

            # Upsert property stub
            try:
                await db.execute(
                    text(f"""
                        INSERT INTO {schema_prefix}property
                            (id, address, suburb, status, route_queue, created_at, updated_at)
                        VALUES (:id, :address, :suburb, 'withdrawn', '', :now, :now)
                        ON CONFLICT (id) DO UPDATE SET
                            status = 'withdrawn',
                            updated_at = :now
                    """),
                    {
                        "id": prop_id,
                        "address": address,
                        "suburb": listing.get("suburb", ""),
                        "now": now,
                    }
                )
            except Exception:
                pass  # table may not exist in SQLite mode

            # Insert WITHDRAWN event
            try:
                import json as _json
                await db.execute(
                    text("""
                        INSERT INTO intelligence.event
                            (id, property_id, event_type, source, raw_payload, occurred_at, created_at)
                        VALUES (:id, :property_id, 'WITHDRAWN', :source, :payload::jsonb, :now, :now)
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {
                        "id": str(uuid.uuid4()),
                        "property_id": prop_id,
                        "source": listing.get("source", "reaxml"),
                        "payload": _json.dumps({
                            "listing_id": listing.get("listing_id", ""),
                            "agency_name": listing.get("agency_name", ""),
                        }),
                        "now": now,
                    }
                )
            except Exception:
                pass
        try:
            await db.commit()
        except Exception:
            pass

    async def _get_withdrawn_today(self, db) -> list[dict]:
        """Fetch WITHDRAWN events from intelligence.event filed today."""
        from sqlalchemy import text
        from core.config import USE_POSTGRES

        schema_prefix = "intelligence." if USE_POSTGRES else ""
        today = datetime.now(SYDNEY_TZ).date().isoformat()
        try:
            result = await db.execute(
                text(f"""
                    SELECT e.property_id, p.address, p.h3index, p.last_settlement_date,
                           p.lat, p.lng
                    FROM {schema_prefix}event e
                    JOIN {schema_prefix}property p ON p.id = e.property_id
                    WHERE e.event_type = 'WITHDRAWN'
                      AND e.created_at >= :today
                """),
                {"today": today}
            )
            rows = result.fetchall()
            return [
                {
                    "property_id": r[0],
                    "address": r[1],
                    "h3index": r[2],
                    "last_settlement_date": r[3],
                    "lat": r[4],
                    "lng": r[5],
                }
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"[Delta] _get_withdrawn_today error: {e}")
            return []

    async def _get_sold_set(self, db, source: str) -> set[str]:
        """Return set of property_ids confirmed sold from the given source."""
        from sqlalchemy import text
        from core.config import USE_POSTGRES

        schema_prefix = "intelligence." if USE_POSTGRES else ""
        try:
            result = await db.execute(
                text(f"""
                    SELECT DISTINCT property_id
                    FROM {schema_prefix}event
                    WHERE event_type = 'SOLD' AND source = :source
                """),
                {"source": source}
            )
            return {r[0] for r in result.fetchall()}
        except Exception as e:
            logger.warning(f"[Delta] _get_sold_set({source}) error: {e}")
            return set()

    async def _update_route_queue(self, db, property_id: str, queue: str):
        """Persist the routing decision back to intelligence.property."""
        from sqlalchemy import text
        from core.utils import now_iso
        from core.config import USE_POSTGRES

        schema_prefix = "intelligence." if USE_POSTGRES else ""
        try:
            await db.execute(
                text(f"""
                    UPDATE {schema_prefix}property
                    SET route_queue = :queue, updated_at = :now
                    WHERE id = :id
                """),
                {"queue": queue, "now": now_iso(), "id": property_id}
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"[Delta] _update_route_queue error: {e}")


# Module-level singleton
_delta_engine = DeltaEngine()


# ─── Background loop ─────────────────────────────────────────────────────────

async def _daily_delta_loop():
    """
    Runs once per day at midnight Sydney time.
    Performs set-theory delta: Withdrawn - (Sold_PSI ∪ Sold_LRS).
    """
    logger.info("[Delta] Daily delta loop started")
    while True:
        now = datetime.now(SYDNEY_TZ)
        # Sleep until next midnight Sydney
        tomorrow_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now.hour >= 0:
            from datetime import timedelta
            tomorrow_midnight = tomorrow_midnight + timedelta(days=1)
        sleep_secs = (tomorrow_midnight - now).total_seconds()
        if sleep_secs < 60:
            sleep_secs = 86400

        logger.info(f"[Delta] Next delta run in {sleep_secs/3600:.1f}h")
        await asyncio.sleep(sleep_secs)

        try:
            from core.database import get_session
            async for db in get_session():
                await _delta_engine.run_daily_delta(db)
        except Exception as e:
            logger.error(f"[Delta] Loop error: {e}")
