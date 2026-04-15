"""
Lead query service — thin re-export layer.
Route handlers import from here rather than directly from core.logic,
establishing a clean service boundary for future extraction.
"""
from models.schemas import LEDGER_COLUMNS  # noqa: F401
from core.logic import _derive_intelligence, _hydrate_lead  # noqa: F401

__all__ = ["LEDGER_COLUMNS", "_derive_intelligence", "_hydrate_lead"]
