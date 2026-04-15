"""
Cadastral Resolution Service
Maps unregistered Lot/DP numbers → gazetted street address via NSW public APIs.
Required for Box Hill subdivision ghost properties (pre-registration parcels).
"""

import logging
import re
from typing import Optional

import httpx

from core.config import NSW_SPATIAL_FEATURESERVER_URL, NSW_GURAS_API_URL

logger = logging.getLogger(__name__)

LOT_DP_RE = re.compile(r"Lot\s+(\d+[A-Za-z]?)[,\s]+DP\s*(\d+)", re.IGNORECASE)


class CadastralResolver:
    """
    Maps unregistered Lot/DP numbers → gazetted street address via NSW APIs.
    Required for Box Hill subdivision ghost properties.

    Step 1: NSW Spatial Services FeatureServer Layer 8 → cadid + geometry polygon
    Step 2: Centroid of polygon
    Step 3: NSW GURAS Address Location Service → gazetted street address
    """

    async def resolve(self, raw_text: str) -> Optional[dict]:
        """
        Parse a Lot/DP reference from raw_text and resolve to a gazetted address.
        Returns dict with keys: cadid, address, lat, lng — or None if not resolvable.
        """
        match = LOT_DP_RE.search(raw_text)
        if not match:
            return None
        lot, dp = match.group(1), match.group(2)

        cadastral = await self._query_featureserver(lot, dp)
        if not cadastral:
            logger.debug(f"[Cadastral] No FeatureServer result for Lot {lot} DP {dp}")
            return None

        centroid = self._polygon_centroid(cadastral["geometry"]["rings"])
        address = await self._query_guras(centroid["lat"], centroid["lng"])

        return {
            "cadid": cadastral["cadid"],
            "address": address,
            "lat": centroid["lat"],
            "lng": centroid["lng"],
        }

    async def _query_featureserver(self, lot: str, dp: str) -> Optional[dict]:
        """Query NSW Spatial Services FeatureServer Layer 8 for Lot/DP geometry."""
        params = {
            "where": f"lotnumber='{lot}' AND plannumber='DP{dp}'",
            "outFields": "cadid,lotnumber,plannumber",
            "returnGeometry": "true",
            "geometryType": "esriGeometryPolygon",
            "f": "json",
        }
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(NSW_SPATIAL_FEATURESERVER_URL, params=params)
                resp.raise_for_status()
                data = resp.json()

            features = data.get("features", [])
            if not features:
                return None

            feat = features[0]
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {})
            return {
                "cadid": attrs.get("cadid", ""),
                "geometry": geom,
            }
        except Exception as e:
            logger.warning(f"[Cadastral] FeatureServer query error: {e}")
            return None

    async def _query_guras(self, lat: float, lng: float) -> str:
        """
        Query NSW GURAS (Address Location Service) with centroid to get gazetted address.
        Returns address string, or lat/lng stub if API call fails.
        """
        if not NSW_GURAS_API_URL or NSW_GURAS_API_URL.endswith("/"):
            # Stub not configured — return coordinate placeholder
            return f"Lot near ({lat:.6f}, {lng:.6f})"

        params = {
            "geometry": f"{lng},{lat}",
            "geometryType": "esriGeometryPoint",
            "inSR": "4326",
            "outFields": "address",
            "returnGeometry": "false",
            "f": "json",
        }
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(NSW_GURAS_API_URL, params=params)
                resp.raise_for_status()
                data = resp.json()

            features = data.get("features", [])
            if features:
                return features[0].get("attributes", {}).get("address", "")
            return f"Lot near ({lat:.6f}, {lng:.6f})"
        except Exception as e:
            logger.warning(f"[Cadastral] GURAS query error: {e}")
            return f"Lot near ({lat:.6f}, {lng:.6f})"

    def _polygon_centroid(self, rings: list) -> dict:
        """Average of all vertices in the outer ring of an esri polygon."""
        if not rings:
            return {"lat": 0.0, "lng": 0.0}
        coords = rings[0]
        if not coords:
            return {"lat": 0.0, "lng": 0.0}
        lat = sum(c[1] for c in coords) / len(coords)
        lng = sum(c[0] for c in coords) / len(coords)
        return {"lat": lat, "lng": lng}


# Module-level singleton
cadastral_resolver = CadastralResolver()
