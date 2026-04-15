from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from core.database import _async_session_factory  # noqa: E402
from services.revenue_growth_service import generate_daily_content_bundle  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily revenue-engine content from call transcripts and objections.")
    parser.add_argument("--business-context", default="real_estate", help="Business context key: real_estate | mortgage | app_saas")
    parser.add_argument("--posts-per-day", type=int, default=5, help="Number of LinkedIn posts to generate.")
    parser.add_argument("--blog-count", type=int, default=1, help="Number of blog drafts to generate.")
    parser.add_argument("--newsletter-count", type=int, default=1, help="Number of newsletter drafts to generate.")
    parser.add_argument("--created-by", default="content_generator_cli", help="Audit label stored with generated content.")
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict:
    async with _async_session_factory() as session:  # type: AsyncSession
        result = await generate_daily_content_bundle(
            session,
            business_context_key=args.business_context,
            posts_per_day=args.posts_per_day,
            blog_count=args.blog_count,
            newsletter_count=args.newsletter_count,
            created_by=args.created_by,
        )
    return {
        "run_date": result["run_date"],
        "counts": result["counts"],
        "assets": [
            {
                "id": asset.id,
                "asset_type": asset.asset_type,
                "title": asset.title,
                "content_text": asset.content_text,
            }
            for asset in result["assets"]
        ],
        "source": result["source"],
    }


def main() -> int:
    args = parse_args()
    payload = asyncio.run(_run(args))
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
