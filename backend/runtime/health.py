from __future__ import annotations

from sqlalchemy import text

from core.database import _async_session_factory, get_redis


async def _check_database_health() -> tuple[str, str | None]:
    try:
        async with _async_session_factory() as session:
            await session.execute(text("SELECT 1"))
        return "ok", None
    except Exception as exc:
        return "error", str(exc)


async def _check_redis_health() -> tuple[str, str | None]:
    try:
        redis_client = await get_redis()
        if not await redis_client.ping():
            raise RuntimeError("Redis ping returned a falsey response")
        return "ok", None
    except Exception as exc:
        return "error", str(exc)
