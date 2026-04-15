from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("RUNTIME_ROLE", "scheduler")

from app_factory import create_app


app = create_app(runtime_role="scheduler")


async def _run_forever() -> None:
    async with app.router.lifespan_context(app):
        await asyncio.Event().wait()


def main() -> None:
    asyncio.run(_run_forever())


if __name__ == "__main__":
    main()
