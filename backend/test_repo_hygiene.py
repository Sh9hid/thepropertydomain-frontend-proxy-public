from tools.repo_hygiene import audit_tracked_paths


def test_audit_tracked_paths_flags_runtime_artifacts_and_secrets():
    findings = audit_tracked_paths(
        [
            "backend/.env",
            "backend/token_cache.bin",
            "backend/startup_log_3.txt",
            "backend/recordings/call_123.mp3",
            "backend/artifacts/cotality/demo.png",
        ]
    )

    finding_paths = {finding.path for finding in findings}

    assert "backend/.env" in finding_paths
    assert "backend/token_cache.bin" in finding_paths
    assert "backend/startup_log_3.txt" in finding_paths
    assert "backend/recordings/call_123.mp3" in finding_paths
    assert "backend/artifacts/cotality/demo.png" in finding_paths


def test_audit_tracked_paths_allows_runtime_source_files():
    findings = audit_tracked_paths(
        [
            "backend/main.py",
            "backend/runtime/app.py",
            "backend/services/lead_service.py",
            "backend/test_production_runtime.py",
        ]
    )

    assert findings == []
