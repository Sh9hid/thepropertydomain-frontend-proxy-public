"""
System health pulse service.
Extracted from main.py to keep the entry point thin.
"""
import asyncio
import datetime
from sqlalchemy import text
from core.config import SYDNEY_TZ
from core.events import event_manager


async def _system_health_pulse() -> None:
    """
    Background loop: checks DB health every 60 s and broadcasts via WebSocket.
    """
    while True:
        try:
            from core.database import async_engine
            async with async_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            health = {
                "type": "SYSTEM_HEALTH",
                "data": {
                    "database": "online",
                    "status": "active",
                    "timestamp": datetime.datetime.now(SYDNEY_TZ).isoformat(),
                },
            }
            await event_manager.broadcast(health)
        except Exception as exc:
            await event_manager.broadcast({
                "type": "SYSTEM_HEALTH",
                "data": {"status": "error", "error": str(exc)},
            })
        await asyncio.sleep(60)
