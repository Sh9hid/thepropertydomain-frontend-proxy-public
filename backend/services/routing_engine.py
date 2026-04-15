"""
Dual-Opportunity Routing Engine
Routes confirmed off-market properties to the correct team queue:
  - MORTGAGE  → Ownit1st Loans (fixed-rate cliff window)
  - DEVELOPMENT → Commercial JV / Developer Acquisition (subdivision DA)
  - RE         → Standard real estate pipeline (default)
"""

import logging
from datetime import datetime

import httpx

from core.config import (
    QUEUE_RE, QUEUE_MORTGAGE, QUEUE_DEVELOPMENT,
    MORTGAGE_CLIFF_MIN_YEARS, MORTGAGE_CLIFF_MAX_YEARS,
    NSW_EPLANNING_DA_URL,
)

logger = logging.getLogger(__name__)


class RoutingEngine:
    """
    Dual-opportunity routing for confirmed off-market properties.
    Priority: MORTGAGE > DEVELOPMENT > RE (default).
    """

    async def route(self, db, property_data: dict) -> str:
        """
        Returns the appropriate QUEUE_* constant for the given property.

        Args:
            db: AsyncSession (may be None if Postgres unavailable)
            property_data: dict with at least property_id, h3index, last_settlement_date
        """
        # --- Mortgage Cliff Trigger ---
        last_settlement = property_data.get("last_settlement_date")
        if last_settlement:
            try:
                settled = datetime.fromisoformat(last_settlement)
                years_ago = (datetime.now() - settled).days / 365.25
                if MORTGAGE_CLIFF_MIN_YEARS <= years_ago <= MORTGAGE_CLIFF_MAX_YEARS:
                    logger.info(
                        f"[Router] MORTGAGE queue: settled {years_ago:.1f}y ago — "
                        f"{property_data.get('address', '')}"
                    )
                    return QUEUE_MORTGAGE
            except (ValueError, TypeError) as e:
                logger.debug(f"[Router] Settlement date parse error: {e}")

        # --- Development Acquisition Trigger ---
        h3idx = property_data.get("h3index")
        if h3idx and await self._has_subdivision_da(h3idx):
            logger.info(
                f"[Router] DEVELOPMENT queue: DA found near H3 {h3idx} — "
                f"{property_data.get('address', '')}"
            )
            return QUEUE_DEVELOPMENT

        return QUEUE_RE

    async def _has_subdivision_da(self, h3idx: str) -> bool:
        """
        Query NSW ePlanning public API for subdivision DAs near the given H3 hex.
        Uses CouncilName filter for both Wollongong (Locus 2517) and
        The Hills / Hawkesbury (Locus 2765).  No API key required.
        """
        if not NSW_EPLANNING_DA_URL:
            return False

        # Map H3 prefix to council — first 4 chars distinguish rough area
        council_map = {
            "8b6a": "The Hills Shire Council",   # Locus 2765
            "8b6b": "Hawkesbury City Council",
            "8b65": "Wollongong City Council",    # Locus 2517
        }
        council = next(
            (v for k, v in council_map.items() if (h3idx or "").startswith(k)),
            "Wollongong City Council",
        )

        try:
            params = {
                "filters": f"CouncilName={council}",
                "pageSize": 10,
                "pageNumber": 1,
            }
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(NSW_EPLANNING_DA_URL, params=params)
            if resp.status_code != 200:
                logger.warning(f"[Router] NSW ePlanning returned {resp.status_code}")
                return False
            data = resp.json()
            # Response shape: {"Application": {"TotalCount": N, "Application": [...]}}
            app_wrapper = data.get("Application", {})
            if isinstance(app_wrapper, dict):
                total = app_wrapper.get("TotalCount", 0)
            else:
                total = len(data.get("Application", []))
            logger.info(f"[Router] NSW ePlanning: {total} DAs for {council}")
            return total > 0
        except Exception as exc:
            logger.debug(f"[Router] ePlanning DA check error: {exc}")
            return False


# Module-level singleton
routing_engine = RoutingEngine()
