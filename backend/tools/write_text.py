"""Simple writer that copies bytes from a temporary file to target path."""
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Write bytes from a temp file to target path")
    parser.add_argument("--file", required=True)
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    target = Path(args.file)
    target.parent.mkdir(parents=True, exist_ok=True)
    content = Path(args.input).read_bytes()
    target.write_bytes(content)
    print(f"written {args.file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
