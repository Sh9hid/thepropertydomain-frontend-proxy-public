"""
Suburb Intelligence Service — reads Cotality xlsx reports from D:\\L+S Stock\\Suburb reports\\

Provides median price, recent sales count, property type breakdown, and land size
stats per suburb, sourced from Shahid's own Cotality suburb reports (not external API).

Cache is in-memory (files don't change between runs). If no matching file is found,
returns an empty dict — no error raised, caller renders fallback.
"""

import logging
import statistics
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from functools import lru_cache

from core.config import STOCK_ROOT

logger = logging.getLogger(__name__)

SUBURB_REPORTS_DIR = Path(STOCK_ROOT) / "Suburb reports"

# Column indices in the xlsx data rows (row index 2 = header, 3+ = data)
COL_ADDRESS = 1
COL_SUBURB = 2
COL_PROPERTY_TYPE = 6
COL_BEDS = 7
COL_LAND_SIZE = 10
COL_YEAR_BUILT = 12
COL_SALE_PRICE = 13
COL_SALE_DATE = 14
COL_ZONE = 20


def _find_xlsx_for_suburb(suburb: str) -> Optional[Path]:
    """
    Find the xlsx file that matches a suburb name, case-insensitively.
    'BLIGH PARK' matches 'Bligh Park report.xlsx'.
    Returns None if no match found.
    """
    if not SUBURB_REPORTS_DIR.exists():
        return None
    clean = suburb.strip().lower()
    for f in SUBURB_REPORTS_DIR.glob("*.xlsx"):
        stem = f.stem.lower().replace(" report", "").strip()
        if stem == clean:
            return f
    # Partial match fallback
    for f in SUBURB_REPORTS_DIR.glob("*.xlsx"):
        stem = f.stem.lower().replace(" report", "").strip()
        if clean in stem or stem in clean:
            return f
    return None


def _parse_price(val) -> Optional[int]:
    """Parse a price string like '$480,000' or '480000' into an int."""
    if not val or str(val).strip() in ("-", "", "None", "$0", "0"):
        return None
    try:
        return int(str(val).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _parse_date(val) -> Optional[date]:
    """Parse a date string like '15 May 1991' or '2023-05-15' into a date."""
    if not val or str(val).strip() in ("-", "", "None"):
        return None
    s = str(val).strip()
    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_int(val) -> Optional[int]:
    if not val or str(val).strip() in ("-", "", "None"):
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def _read_xlsx_rows(path: Path) -> list[dict]:
    """
    Read all data rows from the suburb xlsx (openpyxl, read-only).
    Returns list of dicts with keys: address, type, beds, land_size,
    year_built, sale_price, sale_date, zone.
    """
    try:
        import openpyxl
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        ws = wb.active
        rows = []
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i < 3:  # skip search string row, blank row, header row
                continue
            if not row or not row[COL_ADDRESS]:
                continue
            rows.append({
                "address": str(row[COL_ADDRESS] or "").strip(),
                "type": str(row[COL_PROPERTY_TYPE] or "").strip(),
                "beds": _parse_int(row[COL_BEDS]),
                "land_size": _parse_int(row[COL_LAND_SIZE]),
                "year_built": _parse_int(row[COL_YEAR_BUILT]),
                "sale_price": _parse_price(row[COL_SALE_PRICE]),
                "sale_date": _parse_date(row[COL_SALE_DATE]),
                "zone": str(row[COL_ZONE] or "").strip() if len(row) > COL_ZONE else "",
            })
        wb.close()
        return rows
    except Exception as exc:
        logger.warning("suburb_intel_service: failed to read %s: %s", path, exc)
        return []


# Simple in-memory cache keyed by suburb name (lower)
_cache: dict[str, dict] = {}


def get_suburb_intel(suburb: str) -> dict:
    """
    Return suburb stats for the given suburb name.

    Returns dict with:
        suburb, total_records, recent_5y_count, median_price, median_price_recent,
        median_land_size, house_pct, unit_pct, top_zone, source_file

    Returns {} if no matching xlsx file found.
    """
    key = suburb.strip().lower()
    if key in _cache:
        return _cache[key]

    path = _find_xlsx_for_suburb(suburb)
    if not path:
        return {}

    rows = _read_xlsx_rows(path)
    if not rows:
        return {}

    cutoff_5y = date(date.today().year - 5, 1, 1)
    cutoff_3y = date(date.today().year - 3, 1, 1)

    all_prices = [r["sale_price"] for r in rows if r["sale_price"] and r["sale_price"] > 50_000]
    recent_5y = [r for r in rows if r["sale_date"] and r["sale_date"] >= cutoff_5y]
    recent_5y_prices = [r["sale_price"] for r in recent_5y if r["sale_price"] and r["sale_price"] > 50_000]
    recent_3y_prices = [
        r["sale_price"] for r in rows
        if r["sale_price"] and r["sale_price"] > 50_000
        and r["sale_date"] and r["sale_date"] >= cutoff_3y
    ]

    land_sizes = [r["land_size"] for r in rows if r["land_size"] and r["land_size"] > 0]

    types = [r["type"].lower() for r in rows if r["type"] and r["type"] != "-"]
    house_count = sum(1 for t in types if "house" in t or "villa" in t or "townhouse" in t)
    unit_count = sum(1 for t in types if "unit" in t or "apartment" in t or "flat" in t)
    total_typed = len(types) or 1

    zones = [r["zone"] for r in rows if r["zone"] and r["zone"] not in ("-", "", "None")]
    zone_counts: dict[str, int] = {}
    for z in zones:
        zone_counts[z] = zone_counts.get(z, 0) + 1
    top_zone = max(zone_counts, key=lambda z: zone_counts[z]) if zone_counts else None

    result = {
        "suburb": suburb.title(),
        "total_records": len(rows),
        "recent_5y_count": len(recent_5y),
        "median_price": int(statistics.median(all_prices)) if all_prices else None,
        "median_price_recent": int(statistics.median(recent_3y_prices)) if recent_3y_prices else (
            int(statistics.median(recent_5y_prices)) if recent_5y_prices else None
        ),
        "median_land_size": int(statistics.median(land_sizes)) if land_sizes else None,
        "house_pct": round(house_count / total_typed * 100) if types else None,
        "unit_pct": round(unit_count / total_typed * 100) if types else None,
        "top_zone": top_zone,
        "source_file": path.name,
        "source": "L+S Cotality Report",
    }
    _cache[key] = result
    return result
