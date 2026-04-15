from __future__ import annotations

from typing import Any


_TERMINAL_CACHE: dict[str, dict[str, Any]] = {}


def get_terminal_cache_entry(cache_key: str) -> dict[str, Any] | None:
    return _TERMINAL_CACHE.get(cache_key)


def set_terminal_cache_entry(cache_key: str, payload: dict[str, Any]) -> None:
    _TERMINAL_CACHE[cache_key] = payload


def invalidate_terminal_cache(lead_ids: list[str]) -> None:
    if not lead_ids:
        return
    lead_id_set = {str(lead_id) for lead_id in lead_ids if str(lead_id).strip()}
    doomed = [
        cache_key
        for cache_key in _TERMINAL_CACHE
        if cache_key.split(":", 1)[0] in lead_id_set
    ]
    for cache_key in doomed:
        _TERMINAL_CACHE.pop(cache_key, None)


def invalidate_lead_read_models(lead_ids: list[str]) -> None:
    invalidate_terminal_cache(lead_ids)
    from services.underwriter_service import invalidate_brief

    invalidate_brief(lead_ids)


def clear_all_read_caches() -> None:
    _TERMINAL_CACHE.clear()
    from services.underwriter_service import _BRIEF_CACHE

    _BRIEF_CACHE.clear()
