import argparse
import asyncio
import json

from core.database import _async_session_factory, init_db
from services.zoom_recording_sync_service import sync_zoom_recordings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill Zoom recordings")
    parser.add_argument("--meeting-id")
    parser.add_argument("--meeting-uuid")
    parser.add_argument("--call-id")
    parser.add_argument("--user")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


async def _main() -> None:
    args = build_parser().parse_args()
    init_db()
    async with _async_session_factory() as session:
        result = await sync_zoom_recordings(
            session,
            {
                "meeting_id": args.meeting_id,
                "meeting_uuid": args.meeting_uuid,
                "call_id": args.call_id,
                "user": args.user,
                "from": args.from_date,
                "to": args.to_date,
                "dry_run": args.dry_run,
                "verbose": args.verbose,
            },
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
