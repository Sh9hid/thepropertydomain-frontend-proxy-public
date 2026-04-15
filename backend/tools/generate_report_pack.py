import argparse
import asyncio
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from core.database import async_engine
from core.db_adapter import db_adapter
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import main  # noqa: E402


async def _pick_random_complete_lead(session: AsyncSession):
    from core.logic import _hydrate_lead
    row = await session.execute(
        text("""
        SELECT *
        FROM leads
        WHERE trim(COALESCE(owner_name, '')) != ''
          AND trim(COALESCE(address, '')) != ''
          AND trim(COALESCE(suburb, '')) != ''
        ORDER BY
            (CASE WHEN bedrooms IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN bathrooms IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN car_spaces IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN land_size_sqm IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN floor_size_sqm IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN trim(COALESCE(sale_date, '')) != '' THEN 1 ELSE 0 END +
             CASE WHEN trim(COALESCE(sale_price, '')) != '' AND sale_price != '$0' THEN 1 ELSE 0 END) DESC,
            RANDOM()
        LIMIT 1
        """)
    )
    res = row.mappings().first()
    if not res:
        raise RuntimeError("No suitable lead found for report pack generation.")
    return _hydrate_lead(res)


async def _run(args):
    async_session = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        if args.lead_id:
            lead = await db_adapter.get_lead(args.lead_id, db=session)
            if not lead:
                raise RuntimeError("Lead not found")
        else:
            lead = await _pick_random_complete_lead(session)
        
        manifest = await main._generate_report_pack_for_lead(
            session,
            lead,
            include_existing_briefs=not args.skip_existing_briefs,
            output_root=args.output_root,
        )
    print(f"lead_id={lead['id']}")
    print(f"address={lead['address']}")
    print(f"pack_root={manifest['pack_root']}")


def main_cli():
    parser = argparse.ArgumentParser(description="Generate a Cotality-style report pack for a lead.")
    parser.add_argument("--lead-id", help="Specific lead ID to use")
    parser.add_argument("--output-root", help="Override output root")
    parser.add_argument("--skip-existing-briefs", action="store_true", help="Do not generate generic_seller_brief and ai_appraisal_brief PDFs")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main_cli()
