from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from core.database import _async_session_factory  # noqa: E402
from models.sales_core_models import ContentAsset  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export latest LinkedIn post drafts to a dated text file.")
    parser.add_argument("--business-context", default="real_estate", help="Business context key to export from.")
    parser.add_argument("--limit", type=int, default=5, help="Number of posts to export.")
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> Path:
    output_dir = REPO_ROOT / "content"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{datetime.utcnow().date().isoformat()}.txt"

    async with _async_session_factory() as session:  # type: AsyncSession
        rows = (
            await session.execute(
                select(ContentAsset)
                .where(ContentAsset.business_context_key == args.business_context)
                .where(ContentAsset.asset_type == "linkedin_post")
                .order_by(ContentAsset.created_at.desc())
                .limit(args.limit)
            )
        ).scalars().all()

    lines: list[str] = []
    for index, row in enumerate(rows, start=1):
        lines.append(f"Post {index}: {row.title or 'LinkedIn post'}")
        lines.append(row.content_text.strip())
        lines.append("")

    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return output_path


def main() -> int:
    args = parse_args()
    output_path = asyncio.run(_run(args))
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
