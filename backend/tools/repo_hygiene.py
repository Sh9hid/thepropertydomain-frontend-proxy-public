from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import subprocess
import sys


@dataclass(frozen=True)
class HygieneFinding:
    path: str
    reason: str


def _normalize(path: str) -> str:
    return path.replace("\\", "/").strip()


def _reason_for_path(path: str) -> str | None:
    normalized = _normalize(path)
    parts = PurePosixPath(normalized).parts
    name = parts[-1] if parts else normalized

    if name in {".env", ".env.local", ".env.production", ".env.development"}:
        return "tracked environment file"
    if name == "token_cache.bin":
        return "tracked token cache"
    if name.endswith(".log") or name.startswith("startup_log") or name.startswith("backend_startup"):
        return "tracked runtime log"
    if normalized.startswith("backend/recordings/"):
        return "tracked call recording artifact"
    if normalized.startswith("backend/artifacts/"):
        return "tracked generated artifact"
    if normalized.startswith("backend/generated_reports/"):
        return "tracked generated report"
    if normalized.startswith("backend/generated_brochures/"):
        return "tracked generated brochure"
    if normalized.startswith("backend/report_packs/"):
        return "tracked report pack output"
    if normalized.startswith("backend/test_dbs/"):
        return "tracked test database artifact"
    if name in {"database.db", "local.db", "intelligence.db", "leads.db"}:
        return "tracked local database artifact"
    return None


def audit_tracked_paths(paths: list[str]) -> list[HygieneFinding]:
    findings: list[HygieneFinding] = []
    for path in paths:
        reason = _reason_for_path(path)
        if reason:
            findings.append(HygieneFinding(path=_normalize(path), reason=reason))
    return findings


def get_tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def main() -> int:
    findings = audit_tracked_paths(get_tracked_files())
    if not findings:
        print("repo hygiene audit passed")
        return 0

    print("repo hygiene audit found tracked production-hostile files:")
    for finding in findings:
        print(f"- {finding.path}: {finding.reason}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
