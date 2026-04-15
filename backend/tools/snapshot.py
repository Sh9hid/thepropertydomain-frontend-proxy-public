from __future__ import annotations

import argparse
import shutil
from datetime import UTC, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TARGETS = [
    "frontend/src/features/lead/LeadWorkspace.tsx",
    "frontend/src/features/lead/DialMode.tsx",
    "frontend/src/views/MarketPulse.tsx",
    "frontend/src/views/AgentFloor.tsx",
    "frontend/src/components/CadenceCoach.tsx",
    "backend/main.py",
    "backend/core/database.py",
    "backend/api/routes/leads.py",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Snapshot critical repo files before refactors.")
    parser.add_argument("--files", nargs="*", help="Optional explicit file list.")
    parser.add_argument("--label", default="", help="Optional snapshot label.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    label = f"-{args.label.strip().replace(' ', '-')} " if args.label.strip() else ""
    snapshot_root = PROJECT_ROOT / "snapshots" / f"{timestamp}{label}".strip()
    snapshot_root.mkdir(parents=True, exist_ok=True)

    targets = args.files or DEFAULT_TARGETS
    copied = []
    for raw_target in targets:
        source = (PROJECT_ROOT / raw_target).resolve()
        if not source.exists() or not source.is_file():
            print(f"skip={raw_target}")
            continue
        destination = snapshot_root / source.relative_to(PROJECT_ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        copied.append(destination)
        print(f"copied={destination.relative_to(PROJECT_ROOT)}")

    manifest = snapshot_root / "manifest.txt"
    manifest.write_text(
        "\n".join([f"snapshot={snapshot_root.relative_to(PROJECT_ROOT)}", *[str(path.relative_to(PROJECT_ROOT)) for path in copied]]),
        encoding="utf-8",
    )
    print(f"manifest={manifest.relative_to(PROJECT_ROOT)}")
    print(f"count={len(copied)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
