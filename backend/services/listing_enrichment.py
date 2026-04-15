"""
Land listing enrichment service.

Combines:
  - Static suburb profile (schools, transport, amenities) from assets/suburb_profiles.json
  - Dynamic suburb intel (median price, land size) from suburb_intel_service
  - Recent comparables from domain_enrichment
  - Computed metrics (price per sqm, price vs median, buyer fit score)

Used by the REA Studio populate/ticket pipeline so each listing ships with
full context for templates and operator review.  Zero external API dependencies
for the core path — everything falls back gracefully if services are offline.
"""

from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.config import BRAND_NAME, PRINCIPAL_EMAIL, PRINCIPAL_NAME, PRINCIPAL_PHONE

logger = logging.getLogger(__name__)

_PROFILES_PATH = Path(__file__).resolve().parents[1] / "assets" / "suburb_profiles.json"


@lru_cache(maxsize=1)
def _load_profiles() -> Dict[str, Any]:
    try:
        return json.loads(_PROFILES_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.warning("suburb profiles not loaded: %s", exc)
        return {"_default": {}, "_compliance_footer": ""}


def _profile_key(suburb: str, postcode: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (suburb or "").strip().lower()).strip("_")
    pc = (postcode or "").strip()
    return f"{slug}_nsw_{pc}" if pc else slug


def get_suburb_profile(suburb: str, postcode: str = "") -> Dict[str, Any]:
    """Return the static suburb profile or a default scaffold if unknown."""
    profiles = _load_profiles()
    key = _profile_key(suburb, postcode)
    if key in profiles:
        return profiles[key]
    # Try without postcode
    alt_key = _profile_key(suburb, "")
    for k, v in profiles.items():
        if k.startswith(alt_key + "_"):
            return v
    return profiles.get("_default", {})


def get_compliance_footer() -> str:
    raw_footer = str(_load_profiles().get("_compliance_footer", "") or "").strip()
    disclaimer = raw_footer.split("\n\n", 1)[0].strip() if raw_footer else ""
    signature = f"{PRINCIPAL_NAME}  |  {BRAND_NAME}  |  {PRINCIPAL_EMAIL}  |  {PRINCIPAL_PHONE}"
    return f"{disclaimer}\n\n{signature}" if disclaimer else signature


def _safe_int(value: Any) -> int:
    try:
        return int(float(re.sub(r"[^0-9.]", "", str(value)))) if value else 0
    except (ValueError, TypeError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(re.sub(r"[^0-9.]", "", str(value))) if value else 0.0
    except (ValueError, TypeError):
        return 0.0


def compute_price_metrics(price: int, land_sqm: float, median_price: int = 0) -> Dict[str, Any]:
    """Return price_per_sqm and comparison to suburb median."""
    out: Dict[str, Any] = {
        "price": price,
        "price_display": f"${price:,}" if price else "Contact Agent",
        "price_per_sqm": 0,
        "price_per_sqm_display": "—",
        "vs_median": "",
        "vs_median_pct": 0,
    }
    if price and land_sqm > 0:
        per_sqm = int(round(price / land_sqm))
        out["price_per_sqm"] = per_sqm
        out["price_per_sqm_display"] = f"${per_sqm:,}/sqm"
    if price and median_price:
        diff_pct = round((price - median_price) / median_price * 100)
        out["vs_median_pct"] = diff_pct
        if diff_pct <= -5:
            out["vs_median"] = f"{abs(diff_pct)}% below suburb median"
        elif diff_pct >= 5:
            out["vs_median"] = f"{diff_pct}% above suburb median"
        else:
            out["vs_median"] = "in line with suburb median"
    return out


def size_bucket(land_sqm: float) -> str:
    """Classify land size for template selection."""
    if land_sqm <= 0:
        return "unknown"
    if land_sqm < 300:
        return "compact"
    if land_sqm < 400:
        return "mid"
    if land_sqm < 550:
        return "generous"
    return "premium"


def price_bucket(price: int) -> str:
    if price <= 0:
        return "unknown"
    if price < 800_000:
        return "entry"
    if price < 1_000_000:
        return "mid"
    if price < 1_300_000:
        return "upper"
    return "prestige"


def infer_archetype(size_b: str, lot_type: str, price_b: str) -> str:
    """Pick the best narrative archetype for this specific lot."""
    lot = (lot_type or "").strip().lower()
    if "corner" in lot:
        return "corner_block"
    if "cul-de-sac" in lot or "cul de sac" in lot:
        return "cul_de_sac"
    if size_b in {"premium", "generous"} and price_b in {"upper", "prestige"}:
        return "upgrader_family"
    if size_b == "compact" and price_b == "entry":
        return "first_home"
    if price_b in {"entry", "mid"}:
        return "investor_yield"
    return "family_build"


def enrich_land_listing(
    address: str,
    suburb: str,
    postcode: str,
    land_sqm: float,
    price: int,
    lot_number: str = "",
    lot_type: str = "",
    frontage: str = "",
    include_comparables: bool = False,
) -> Dict[str, Any]:
    """Build a full enrichment dict for a single land listing.

    The dict is consumed by the template renderer.  Optional comparables pull
    uses a Domain API call — pass ``include_comparables=True`` only when you
    can afford the quota hit (default off for bulk populate).
    """
    profile = get_suburb_profile(suburb, postcode)

    # ── Cotality-only policy ───────────────────────────────────────────────
    # Market-stat claims (median price, growth %, yield ranges) must come
    # from the authoritative Cotality xlsx exports — never from the static
    # suburb_profiles.json (which holds marketing narrative only).  If the
    # Cotality intel is missing for this suburb, market-anchored copy is
    # suppressed downstream by the renderer.
    median_price = 0
    median_land = 0
    cotality_present = False
    cotality_source: str = ""
    try:
        from services.suburb_intel_service import get_suburb_intel
        intel = get_suburb_intel(suburb) or {}
        median_price = _safe_int(intel.get("median_price") or intel.get("median_price_recent"))
        median_land = _safe_int(intel.get("median_land_size"))
        if intel:
            cotality_present = True
            cotality_source = str(intel.get("source_file") or "")
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("suburb intel lookup failed: %s", exc)

    # Only pass median_price into compute_price_metrics when Cotality supplied it.
    price_metrics = compute_price_metrics(
        price,
        land_sqm,
        median_price if cotality_present else 0,
    )
    size_b = size_bucket(land_sqm)
    price_b = price_bucket(price)
    archetype = infer_archetype(size_b, lot_type, price_b)

    transport = profile.get("transport", {}) or {}
    metro = transport.get("metro", {}) or {}
    schools = profile.get("schools", {}) or {}
    amenities = profile.get("amenities", []) or []
    personas = profile.get("buyer_personas", {}) or {}

    # Frontage line variants
    frontage_val = _safe_float(frontage)
    frontage_line = ""
    frontage_bullet = ""
    if frontage_val > 0:
        frontage_line = f"Boasting a {frontage_val:g}m frontage, this lot offers excellent street presence and design flexibility. "
        frontage_bullet = f"- Frontage: {frontage_val:g}m\n"

    # Lot type line variants
    lot_type_line = ""
    lot_type_bullet = ""
    lot_clean = (lot_type or "").strip()
    if lot_clean:
        lot_type_line = f"As a {lot_clean.lower()}, this block offers added character and design opportunity. "
        lot_type_bullet = f"- Lot type: {lot_clean}\n"

    # Pick a hook opener deterministically based on lot_number so adjacent lots differ
    openers = profile.get("hook_openers") or ["Secure your build-ready block in one of Sydney's most sought-after corridors."]
    opener_idx = (int(re.sub(r"[^0-9]", "", lot_number) or 0) + int(land_sqm)) % len(openers)
    hook_opener = openers[opener_idx]

    # Build school blurb
    school_blurb = ""
    if schools.get("catchment_primary") or schools.get("catchment_secondary"):
        bits = []
        if schools.get("catchment_primary"):
            bits.append(f"catchment for {schools['catchment_primary']}")
        if schools.get("catchment_secondary"):
            bits.append(f"zoned for {schools['catchment_secondary']}")
        school_blurb = "In " + " and ".join(bits) + "."

    # Build transport blurb
    transport_parts: List[str] = []
    if metro.get("station") and metro.get("minutes_drive"):
        transport_parts.append(f"{metro['minutes_drive']} min drive to {metro['station']}")
    if transport.get("norwest_drive_min"):
        transport_parts.append(f"{transport['norwest_drive_min']} min to Norwest business park")
    if transport.get("parramatta_drive_min"):
        transport_parts.append(f"{transport['parramatta_drive_min']} min to Parramatta")
    if transport.get("cbd_drive_min"):
        transport_parts.append(f"{transport['cbd_drive_min']} min to Sydney CBD")
    transport_blurb = " | ".join(transport_parts[:4])

    # Build amenities short list
    top_amenities: List[str] = []
    for amen in amenities[:3]:
        name = amen.get("name", "")
        dist = amen.get("distance_km")
        if name:
            top_amenities.append(f"{name} ({dist} km)" if dist else name)

    # Optional comparables (off by default to avoid API quota)
    comparables: List[Dict[str, Any]] = []
    if include_comparables:
        try:
            from services.domain_enrichment import get_suburb_comparables
            comparables = get_suburb_comparables(suburb, postcode)[:3]
        except Exception as exc:  # pragma: no cover
            logger.debug("comparables lookup failed: %s", exc)

    # Buyer persona — pick one based on archetype
    persona_map = {
        "first_home": personas.get("first_home_buyer", ""),
        "investor_yield": personas.get("investor", ""),
        "upgrader_family": personas.get("upgrader") or personas.get("downsizer", ""),
        "corner_block": personas.get("upgrader") or personas.get("first_home_buyer", ""),
        "cul_de_sac": personas.get("first_home_buyer", ""),
        "family_build": personas.get("first_home_buyer", ""),
    }
    persona_blurb = persona_map.get(archetype, "")

    # Market-narrative fields are ONLY rendered when Cotality is present.
    # suburb_profiles.json may still supply hook_opener, schools, transport,
    # amenities, infrastructure factual summary, and compliance footer — all
    # non-market facts — but "growth_story" and "stock_context" contain
    # implicit market claims (e.g. "42% rise", "selling within 45-60 days")
    # so we suppress them when unbacked.
    if cotality_present:
        growth_story = profile.get("growth_story", "")
        stock_context = profile.get("stock_context", "")
    else:
        growth_story = ""
        stock_context = ""

    # Persona blurbs mention yield ranges (e.g. "3.8-4.2% gross") for the
    # investor archetype — those are market claims and must be gated too.
    if not cotality_present and archetype == "investor_yield":
        persona_blurb = ""

    data_sources: List[str] = []
    if cotality_present:
        data_sources.append("cotality_xlsx")
    if profile and profile.get("name"):
        data_sources.append("suburb_profile_public_facts")
    if not data_sources:
        data_sources.append("none")

    return {
        # Core
        "address": address,
        "suburb": suburb,
        "postcode": postcode,
        "land_sqm": land_sqm,
        "lot_number": lot_number,
        "lot_type": lot_clean,
        "frontage_val": frontage_val,
        "price": price,
        # Classification
        "size_bucket": size_b,
        "price_bucket": price_b,
        "archetype": archetype,
        # Price context
        **price_metrics,
        "median_price": median_price,
        "median_land": median_land,
        # Cotality gating flags (renderers check these)
        "cotality_data_present": cotality_present,
        "cotality_source_file": cotality_source,
        "data_sources": data_sources,
        # Suburb narrative
        "suburb_tagline": profile.get("tagline", ""),
        "hook_opener": hook_opener,
        "infrastructure_story": profile.get("infrastructure_story", ""),
        "growth_story": growth_story,
        "stock_context": stock_context,
        # Schools / transport / amenities
        "school_blurb": school_blurb,
        "schools_primary": schools.get("catchment_primary", ""),
        "schools_secondary": schools.get("catchment_secondary", ""),
        "transport_blurb": transport_blurb,
        "metro_station": metro.get("station", ""),
        "metro_minutes": metro.get("minutes_drive", 0),
        "cbd_drive_min": transport.get("cbd_drive_min", 0),
        "norwest_drive_min": transport.get("norwest_drive_min", 0),
        "parramatta_drive_min": transport.get("parramatta_drive_min", 0),
        "amenities_top": top_amenities,
        # Template line helpers
        "frontage_line": frontage_line,
        "frontage_bullet": frontage_bullet,
        "lot_type_line": lot_type_line,
        "lot_type_bullet": lot_type_bullet,
        # Persona
        "persona_blurb": persona_blurb,
        # Data
        "comparables": comparables,
        # Compliance
        "compliance_footer": get_compliance_footer(),
    }
