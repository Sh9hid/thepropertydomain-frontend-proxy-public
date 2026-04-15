"""Inbound email sync — stub for boot safety."""
from typing import Any, Dict, List


async def _load_imap_accounts(*args, **kwargs) -> List[Dict[str, Any]]:
    return []


def _parse_aliases(*args, **kwargs) -> List[str]:
    return []


async def poll_inbound_email_imap(*args, **kwargs) -> Dict[str, Any]:
    return {"polled": 0, "new_messages": 0}
