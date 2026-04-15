import argparse
import hashlib
import mimetypes
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push a full D:\\L+S Stock mirror to the cloud evidence API.")
    parser.add_argument("--server", default="http://127.0.0.1:8001/api", help="Backend API base URL")
    parser.add_argument("--api-key", required=True, help="X-API-KEY value")
    parser.add_argument("--source-root", default=r"D:\L+S Stock", help="Local source root to mirror")
    parser.add_argument("--batch-size", type=int, default=100, help="Manifest batch size")
    parser.add_argument("--limit", type=int, default=0, help="Optional max file count for testing")
    parser.add_argument("--manifest-only", action="store_true", help="Register manifests without uploading files")
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def inventory_files(root: Path, limit: int = 0) -> List[Dict[str, object]]:
    manifests: List[Dict[str, object]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        stat = path.stat()
        manifests.append(
            {
                "local_path": path,
                "relative_path": path.relative_to(root).as_posix(),
                "original_name": path.name,
                "mime_type": mimetypes.guess_type(str(path))[0],
                "size_bytes": stat.st_size,
                "sha256": sha256_file(path),
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "attributes": {"local_parent": path.parent.name},
            }
        )
        if limit and len(manifests) >= limit:
            break
    return manifests


def batched(items: List[Dict[str, object]], size: int) -> Iterable[List[Dict[str, object]]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def main() -> int:
    args = parse_args()
    root = Path(args.source_root)
    if not root.exists():
        raise SystemExit(f"Source root does not exist: {root}")

    manifests = inventory_files(root, limit=args.limit)
    total_bytes = sum(int(item["size_bytes"]) for item in manifests)
    print(f"Indexed {len(manifests)} files from {root} ({total_bytes:,} bytes)")

    session = requests.Session()
    session.headers.update({"X-API-KEY": args.api_key})

    start_res = session.post(
        f"{args.server}/system/sync/full-mirror/start",
        json={
            "source_root": str(root),
            "requested_by": "push_stock_mirror",
            "worker_host": socket.gethostname(),
            "total_files": len(manifests),
            "total_bytes": total_bytes,
        },
        timeout=120,
    )
    start_res.raise_for_status()
    run = start_res.json()
    run_id = run["id"]
    print(f"Started sync run {run_id}")

    failures = 0
    uploaded = 0
    for batch_number, batch in enumerate(batched(manifests, args.batch_size), start=1):
        manifest_payload = []
        local_by_relative = {}
        for item in batch:
            local_by_relative[str(item["relative_path"])] = item
            manifest_payload.append(
                {
                    "relative_path": item["relative_path"],
                    "original_name": item["original_name"],
                    "mime_type": item["mime_type"],
                    "size_bytes": item["size_bytes"],
                    "sha256": item["sha256"],
                    "modified_at": item["modified_at"],
                    "attributes": item["attributes"],
                }
            )

        batch_res = session.post(
            f"{args.server}/system/sync/assets/batch",
            json={"sync_run_id": run_id, "assets": manifest_payload},
            timeout=180,
        )
        batch_res.raise_for_status()
        batch_result = batch_res.json()
        print(
            f"Batch {batch_number}: accepted={batch_result['accepted']} "
            f"upload_required={batch_result['upload_required_count']}"
        )

        if args.manifest_only:
            continue

        for item in batch_result["assets"]:
            if not item["upload_required"]:
                continue
            local_item = local_by_relative[item["relative_path"]]
            local_path = Path(local_item["local_path"])
            with local_path.open("rb") as handle:
                upload_res = session.post(
                    f"{args.server}/system/sync/assets/{item['asset_id']}/upload",
                    data={
                        "sync_run_id": run_id,
                        "sha256": local_item["sha256"],
                        "modified_at": local_item["modified_at"],
                    },
                    files={
                        "file": (
                            local_path.name,
                            handle,
                            local_item["mime_type"] or "application/octet-stream",
                        )
                    },
                    timeout=1800,
                )
            if upload_res.ok:
                uploaded += 1
                print(f"Uploaded {item['relative_path']}")
            else:
                failures += 1
                print(f"Upload failed for {item['relative_path']}: {upload_res.status_code} {upload_res.text}")

    status = "completed" if failures == 0 else "completed_with_errors"
    complete_res = session.post(
        f"{args.server}/system/sync/runs/{run_id}/complete",
        json={"status": status, "error_summary": None if failures == 0 else f"{failures} upload(s) failed"},
        timeout=120,
    )
    complete_res.raise_for_status()
    print(f"Finished sync run {run_id}. Uploaded {uploaded} files. Failures: {failures}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
