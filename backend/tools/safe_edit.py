from __future__ import annotations

import argparse
import difflib
import shutil
import sys
import tempfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safely overwrite a file or replace a marked block.")
    parser.add_argument("--file", required=True, help="Target file to edit.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=("overwrite", "replace_block"),
        help="Edit mode.",
    )
    parser.add_argument("--start", help="Start marker for replace_block mode.")
    parser.add_argument("--end", help="End marker for replace_block mode.")
    parser.add_argument("--with-file", dest="with_file", required=True, help="Path to replacement content.")
    parser.add_argument("--dry-run", action="store_true", help="Show diff summary without writing.")
    return parser.parse_args()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8-sig")


def build_replacement(original: str, mode: str, replacement: str, start: str | None, end: str | None) -> str:
    if mode == "overwrite":
        return replacement

    if not start or not end:
        raise ValueError("--start and --end are required for replace_block mode")

    start_count = original.count(start)
    end_count = original.count(end)
    if start_count != 1 or end_count != 1:
        raise ValueError(f"Expected exactly one start and one end marker, found start={start_count}, end={end_count}")

    start_index = original.index(start)
    end_index = original.index(end, start_index + len(start))
    block_start = start_index + len(start)

    before = original[:block_start]
    after = original[end_index:]
    middle = replacement

    if block_start > 0 and not before.endswith("\n"):
        middle = "\n" + middle
    if after and not after.startswith("\n") and not middle.endswith("\n"):
        middle = middle + "\n"

    return before + middle + after


def summarize_diff(original: str, updated: str, target_path: Path) -> str:
    original_lines = original.splitlines()
    updated_lines = updated.splitlines()
    diff = list(
        difflib.unified_diff(
            original_lines,
            updated_lines,
            fromfile=str(target_path),
            tofile=str(target_path),
            lineterm="",
        )
    )
    changed = sum(1 for line in diff if line.startswith("+") or line.startswith("-")) - 2
    changed = max(changed, 0)
    return "\n".join(
        [
            f"target={target_path}",
            f"lines_before={len(original_lines)}",
            f"lines_after={len(updated_lines)}",
            f"changed_lines={changed}",
            *diff[:40],
        ]
    )


def emit(text: str, *, stream=sys.stdout) -> None:
    safe_text = text
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        safe_text.encode(encoding)
    except UnicodeEncodeError:
        safe_text = safe_text.encode(encoding, errors="replace").decode(encoding)
    stream.write(safe_text + ("\n" if not safe_text.endswith("\n") else ""))


def safe_write(target_path: Path, updated: str) -> None:
    backup_path = target_path.with_suffix(target_path.suffix + ".bak")
    shutil.copy2(target_path, backup_path)

    fd, temp_name = tempfile.mkstemp(prefix=target_path.name + ".", suffix=".tmp", dir=str(target_path.parent))
    temp_path = Path(temp_name)
    try:
        with open(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(updated)
            handle.flush()
        temp_path.replace(target_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        shutil.copy2(backup_path, target_path)
        raise


def main() -> int:
    args = parse_args()
    target_path = Path(args.file).resolve()
    replacement_path = Path(args.with_file).resolve()

    if not target_path.exists():
        raise FileNotFoundError(f"Target file not found: {target_path}")
    if not replacement_path.exists():
        raise FileNotFoundError(f"Replacement file not found: {replacement_path}")
    if not target_path.is_file() or not replacement_path.is_file():
        raise ValueError("Both --file and --with-file must point to files")

    original = read_text(target_path)
    replacement = read_text(replacement_path)
    updated = build_replacement(original, args.mode, replacement, args.start, args.end)

    if original == updated:
        emit(summarize_diff(original, updated, target_path))
        emit("status=no_changes")
        return 0

    emit(summarize_diff(original, updated, target_path))
    if args.dry_run:
        emit("status=dry_run")
        return 0

    safe_write(target_path, updated)
    emit(f"backup={target_path.with_suffix(target_path.suffix + '.bak')}")
    emit("status=written")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        emit(f"error={exc}", stream=sys.stderr)
        raise SystemExit(1)
