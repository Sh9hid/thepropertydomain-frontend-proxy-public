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
import main  # noqa: E402
from customer_pack_engine import create_customer_pack  # noqa: E402
from pdf_generator import html_to_pdf  # noqa: E402


DEFAULT_LEAD_ID = "f1bd665bd2d1b45f961e18d6ffc24c9d"


async def _run(args):
    async_session = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        lead = await db_adapter.get_lead(args.lead_id or DEFAULT_LEAD_ID, db=session)
        if not lead:
            raise RuntimeError("Lead not found")
        manifest = await create_customer_pack(
            session,
            lead,
            stock_root=main.STOCK_ROOT,
            principal_name="Nitin Puri",
            principal_email=args.email,
            principal_phone=args.phone,
            html_to_pdf=html_to_pdf,
            output_root=Path(args.output_root) if args.output_root else None,
        )
    print(f"lead_id={lead['id']}")
    print(f"address={lead['address']}")
    print(f"pack_root={manifest['pack_root']}")


def main_cli():
    parser = argparse.ArgumentParser(description="Generate the customer-ready property pack for a lead.")
    parser.add_argument("--lead-id", help="Specific lead ID to use. Defaults to the selected high-probability owner-occupied lead.")
    parser.add_argument("--output-root", default="D:/", help="Root folder where the property-address folder will be created.")
    parser.add_argument("--phone", default="+61 430 042 041", help="Principal phone number for the pack.")
    parser.add_argument("--email", default="oakville@lsre.com.au", help="Principal email for the pack.")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main_cli()
