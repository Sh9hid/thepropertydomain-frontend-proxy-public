"""Cadastral identity — address normalization and lot/plan extraction."""
from typing import Optional, Tuple


def build_storage_address(address: str, suburb: str = "", postcode: str = "") -> str:
    """Normalize an address for storage/dedup."""
    parts = [p.strip() for p in [address, suburb, postcode] if p and p.strip()]
    return ", ".join(parts)


def extract_lot_plan(description: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract lot and plan numbers from a description string."""
    return None, None


def is_subdivision_signal(description: str) -> bool:
    """Check if a DA description suggests subdivision activity."""
    lower = description.lower() if description else ""
    keywords = ["subdivis", "strata", "torrens", "lot", "consolidat"]
    return any(k in lower for k in keywords)
